use std::collections::BTreeMap;
use std::ops::Add;
use std::sync::Arc;

use super::error::{resolver_error, GqlError};
#[cfg(debug_assertions)]
use super::ext::ValidatorExt;
use crate::dbs::Session;
use crate::gql::cursor::{make_field_value_resolver, PageInfo};
use crate::gql::error::{internal_error, schema_error, type_error};
use crate::gql::ext::{NamedContainer, TryFromExt, TryIntoExt};
use crate::gql::functions::process_fns;
use crate::gql::geometry;
use crate::gql::tables::process_tbs;
use crate::gql::utils::{GQLTx, GqlValueUtils};
use crate::kvs::Datastore;
use crate::kvs::LockType;
use crate::kvs::TransactionType;
use crate::sql;
use crate::sql::kind::Literal;
use crate::sql::statements::define::config::graphql::{FunctionsConfig, TablesConfig};
use crate::sql::Kind;
use crate::sql::Value as SqlValue;
use crate::sql::{Geometry, Ident};
use async_graphql::dynamic::Schema;
use async_graphql::dynamic::{Enum, Type, Union};
use async_graphql::dynamic::{EnumItem, Interface};
use async_graphql::dynamic::{Field, InterfaceField, ResolverContext};
use async_graphql::dynamic::{FieldFuture, Object};
use async_graphql::dynamic::{Scalar, TypeRef};
use async_graphql::indexmap::IndexMap;
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use geo::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use inflector::Inflector;
use js::Type::Object;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::Number;

pub async fn generate_schema(
    datastore: &Arc<Datastore>,
    session: &Session,
) -> Result<Schema, GqlError> {
    let kvs = datastore;
    let tx = kvs.transaction(TransactionType::Read, LockType::Optimistic).await?;
    let ns = session.ns.as_ref().ok_or(GqlError::UnspecifiedNamespace)?;
    let db = session.db.as_ref().ok_or(GqlError::UnspecifiedDatabase)?;
    let gtx = GQLTx::new(datastore, session).await?;

    let cg = tx.get_db_config(ns, db, "graphql").await.map_err(|e| match e {
        crate::err::Error::CgNotFound {
            ..
        } => GqlError::NotConfigured,
        e => e.into(),
    })?;
    let config = cg.inner.clone().try_into_graphql()?;

    let tbs = tx.all_tb(ns, db, None).await?;

    let tbs = match config.tables {
        TablesConfig::None => None,
        TablesConfig::Auto => Some(tbs),
        TablesConfig::Include(inc) => {
            Some(tbs.iter().filter(|t| inc.contains_name(&t.name)).cloned().collect())
        }
        TablesConfig::Exclude(exc) => {
            Some(tbs.iter().filter(|t| !exc.contains_name(&t.name)).cloned().collect())
        }
    };

    let fns = tx.all_db_functions(ns, db).await?;

    let fns = match config.functions {
        FunctionsConfig::None => None,
        FunctionsConfig::Auto => Some(fns),
        FunctionsConfig::Include(inc) => {
            Some(fns.iter().filter(|f| inc.contains(&f.name)).cloned().collect())
        }
        FunctionsConfig::Exclude(exc) => {
            Some(fns.iter().filter(|f| !exc.contains(&f.name)).cloned().collect())
        }
    };

    match (&tbs, &fns) {
        (None, None) => return Err(GqlError::NotConfigured),
        (None, Some(fs)) if fs.is_empty() => {
            return Err(schema_error("no functions found in database"))
        }
        (Some(ts), None) if ts.is_empty() => {
            return Err(schema_error("no tables found in database"))
        }
        (Some(ts), Some(fs)) if ts.is_empty() && fs.is_empty() => {
            return Err(schema_error("no items found in database"));
        }
        _ => {}
    }

    let mut query = Object::new("Query");
    let mut types: Vec<Type> = Vec::new();
    let cursor = config.cursor; //TODO: use cursor config here

    trace!(ns, db, ?tbs, ?fns, "generating schema");

    match tbs {
        Some(tbs) if !tbs.is_empty() => {
            query = process_tbs(tbs, query, &mut types, &tx, ns, db, session, datastore, cursor).await?;
        }
        _ => {}
    }

    if let Some(fns) = fns {
        query = process_fns(fns, query, &mut types, session, datastore).await?;
    }

    // trace!("current Query object for schema: {:?}", query);

    let mut schema = Schema::build("Query", None, None).register(query);
    for ty in types {
        // trace!("adding type: {ty:?}");
        schema = schema.register(ty);
    }

    // =======================================================
    // Custom Scalar types
    // =======================================================

    #[macro_export]
    macro_rules! scalar_debug_validated {
		($schema:ident, $name:expr, $kind:expr) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				::std::option::Option::<&str>::None,
				::std::option::Option::<&str>::None
			)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:literal) => {
			scalar_debug_validated!($schema, $name, $kind, std::option::Option::Some($desc), None)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:literal, $url:literal) => {
			scalar_debug_validated!(
				$schema,
				$name,
				$kind,
				std::option::Option::Some($desc),
				Some($url)
			)
		};
		($schema:ident, $name:expr, $kind:expr, $desc:expr, $url:expr) => {{
			let new_type = Type::Scalar({
				let mut tmp = Scalar::new($name);
				if let Some(desc) = $desc {
					tmp = tmp.description(desc);
				}
				if let Some(url) = $url {
					tmp = tmp.specified_by_url(url);
				}
				#[cfg(debug_assertions)]
				tmp.add_validator(|v| gql_to_sql_kind(v, $kind).is_ok());
				tmp
			});
			$schema = $schema.register(new_type);
		}};
	}

    scalar_debug_validated!(
		schema,
		"UUID",
		Kind::Uuid,
		"String encoded UUID",
		"https://datatracker.ietf.org/doc/html/rfc4122"
	);
    scalar_debug_validated!(
        schema,
        "Bytes", 
        Kind::Bytes,
        "A scalar that represents binary data, serialized as a Base64 string.",
        "https://datatracker.ietf.org/doc/html/rfc4648#section-4"
    );
    scalar_debug_validated!(schema, "Decimal", Kind::Decimal);
    scalar_debug_validated!(schema, "Number", Kind::Number);
    scalar_debug_validated!(schema, "Null", Kind::Null);
    scalar_debug_validated!(schema, "DateTime", Kind::Datetime);
    scalar_debug_validated!(schema, "Duration", Kind::Duration);
    scalar_debug_validated!(schema, "Object", Kind::Object);
    scalar_debug_validated!(schema, "Any", Kind::Any);

    // =======================================================
    // Cursor/Custom types
    // =======================================================

    schema = schema.register(Enum::new("OrderDirection")
        .items([
            EnumItem::new("ASC")
                .description("Specifies an ascending order for a given `orderBy` argument."),
            EnumItem::new("DESC")
                .description("Specifies a descending order for a given `orderBy` argument."),
        ])
        .description("Possible directions in which to order a list of items when provided and `orderBy` argument."));

    schema = schema.register(Object::new("PageInfo")
        .field(
            Field::new(
                "hasNextPage",
                TypeRef::named_nn(TypeRef::BOOLEAN),
                make_field_value_resolver(|pi: &PageInfo| pi.has_next_page),
            ).description("When paginating forwards, are there more items?")
        )
        .field(
            Field::new(
                "hasPreviousPage",
                TypeRef::named_nn(TypeRef::BOOLEAN),
                make_field_value_resolver(|pi: &PageInfo| pi.has_previous_page),
            ).description("When paginating backwards, are there more items?")
        )
        .field(
            Field::new(
                "startCursor",
                TypeRef::named(TypeRef::STRING),
                // asyncGQL Value doesnt implement from for Option<T>
                make_field_value_resolver(|pi: &PageInfo|
                    match &pi.start_cursor {
                        Some(s) => GqlValue::from(s.clone()),
                        None => GqlValue::Null,
                    }
                ),
            ).description("When paginating backwards, the cursor to continue.")
        )
        .field(
            Field::new(
                "endCursor",
                TypeRef::named(TypeRef::STRING),
                make_field_value_resolver(|pi: &PageInfo|
                    match &pi.end_cursor {
                        Some(s) => GqlValue::from(s.clone()),
                        None => GqlValue::Null,
                    }
                ),
            ).description("When paginating forwards, the cursor to continue.")
        )
        .description("Information about pagination in a connection.")
    );

    // =======================================================
    // Geometry types
    // =======================================================

    macro_rules! geometry_type {
		($schema:ident, $name:expr, $type:expr) => {
			$schema = $schema.register(
				Object::new(format!("Geometry{}", $name))
					.field(Field::new(
						"type",
						TypeRef::named("GeometryType"),
						|_: ResolverContext| {
							async_graphql::dynamic::FieldFuture::Value(Some(
								GqlValue::String($name.to_string().to_screaming_snake_case()).into(),
							))
						},
					))
					.field(Field::new("coordinates", $type.clone(), |ctx: ResolverContext| {
						FieldFuture::new({
							async move {
								let val = ctx
									.parent_value
									.try_to_value()?
									.as_object()
									.ok_or_else(|| internal_error("Expected object"))?
									.get("coordinates");
								Ok(val.cloned())
							}
						})
					})),
			);
			// $schema = $schema.register(
			// 	InputObject::new(format!("{}_input", $name))
			// 		.field(InputValue::new("type", TypeRef::named("GeometryType")))
			// 		// .field(InputValue::new("geotype", TypeRef::named(TypeRef::STRING)))
			// 		.field(InputValue::new("coordinates", $type.clone())),
			// );
		};
	}

    schema = schema.register(Enum::new("GeometryType")
        .items([
            EnumItem::new("POINT")
                .description("A geolocation point with longitude and latitude."),
            EnumItem::new("LINE_STRING")
                .description("A GeoJSON LineString value for storing a geometric path."),
            EnumItem::new("POLYGON")
                .description("A GeoJSON Polygon value for storing a geometric area."),
            EnumItem::new("MULTI_POINT")
                .description("A value which contains multiple geometry points"),
            EnumItem::new("MULTI_LINE_STRING")
                .description("A value which contains multiple geometry lines."),
            EnumItem::new("MULTI_POLYGON")
                .description("A value which contains multiple geometry polygons."),
            EnumItem::new("COLLECTION")
                .description("A value which contains multiple different geometry types."),
        ])
        .description("A type of geometry value."));

    let coordinate_type = TypeRef::named_nn_list_nn(TypeRef::FLOAT);
    let coordinate_list_type = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(
        TypeRef::NonNull(Box::new(coordinate_type.clone())),
    ))));
    let coordinate_list_list_type = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(
        TypeRef::NonNull(Box::new(coordinate_list_type.clone())),
    ))));
    let coordinate_list_list_list_type = TypeRef::NonNull(Box::new(TypeRef::List(Box::new(
        TypeRef::NonNull(Box::new(coordinate_list_type.clone())),
    ))));

    //FIXME: adapt macro to fit collection case as well
    geometry_type!(schema, "Point", coordinate_type);
    geometry_type!(schema, "LineString", coordinate_list_type);
    geometry_type!(schema, "Polygon", coordinate_list_list_type);
    geometry_type!(schema, "MultiPoint", coordinate_list_type);
    geometry_type!(schema, "MultiLineString", coordinate_list_list_type);
    geometry_type!(schema, "MultiPolygon", coordinate_list_list_list_type);
    schema = schema.register(
        Object::new(format!("Geometry{}", "Collection"))
            .field(Field::new("type", TypeRef::named("GeometryType"), |_: ResolverContext| {
                FieldFuture::Value(Some(
                    GqlValue::String("COLLECTION".to_string()).into(),
                ))
            }))
            .field(Field::new(
                "geometries",
                TypeRef::named_nn_list_nn("Geometry"),
                |ctx: ResolverContext| {
                    FieldFuture::new({
                        async move {
                            let val = ctx
                                .parent_value
                                .try_to_value()?
                                .as_object()
                                .ok_or_else(|| internal_error("Expected Object"))?
                                .get("geometries");
                            Ok(val.cloned())
                        }
                    })
                },
            )),
    );

    let mut geometry_union = Union::new("Geometry");
    geometry_union = geometry_union.possible_type("GeometryPoint");
    geometry_union = geometry_union.possible_type("GeometryLineString");
    geometry_union = geometry_union.possible_type("GeometryPolygon");
    geometry_union = geometry_union.possible_type("GeometryMultiPoint");
    geometry_union = geometry_union.possible_type("GeometryMultiLineString");
    geometry_union = geometry_union.possible_type("GeometryMultiPolygon");
    geometry_union = geometry_union.possible_type("GeometryCollection");
    schema = schema.register(geometry_union);


    // =======================================================
    // Interfaces
    // =======================================================

    let id_interface =
        Interface::new("Record").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)));
    schema = schema.register(id_interface);

    //FIXME: remove
    // TODO: when used get: `Result::unwrap()` on an `Err` value: SchemaError("Field \"like.in\" is not sub-type of \"relation.in\"")
    let relation_interface = Interface::new("relation")
        .field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID)))
        .field(InterfaceField::new("in", TypeRef::named_nn("Record")))
        .field(InterfaceField::new("out", TypeRef::named_nn("Record")))
        .implement("Record");
    schema = schema.register(relation_interface);


    schema
        .data(gtx)
        .finish()
        .map_err(|e| schema_error(format!("there was an error generating schema: {e:?}")))
}

pub fn sql_value_to_gql_value(v: SqlValue) -> Result<GqlValue, GqlError> {
    let out = match v {
        SqlValue::None => GqlValue::Null,
        SqlValue::Null => GqlValue::Null,
        SqlValue::Bool(b) => GqlValue::Boolean(b),
        SqlValue::Number(n) => match n {
            crate::sql::Number::Int(i) => GqlValue::Number(i.into()),
            crate::sql::Number::Float(f) => GqlValue::Number(
                Number::from_f64(f)
                    .ok_or(resolver_error("unimplemented: graceful NaN and Inf handling"))?,
            ),
            num @ crate::sql::Number::Decimal(_) => GqlValue::String(num.to_string()),
        },
        SqlValue::Strand(s) => GqlValue::String(s.0),
        d @ SqlValue::Duration(_) => GqlValue::String(d.to_string()),
        SqlValue::Datetime(d) => GqlValue::String(d.to_rfc3339()),
        SqlValue::Uuid(uuid) => GqlValue::String(uuid.to_string()),
        SqlValue::Array(a) => {
            GqlValue::List(a.into_iter().map(|v| sql_value_to_gql_value(v).unwrap()).collect())
        }
        SqlValue::Object(o) => GqlValue::Object(
            o.0.into_iter()
                .map(|(k, v)| (Name::new(k), sql_value_to_gql_value(v).unwrap()))
                .collect(),
        ),
        SqlValue::Geometry(kind) => match kind {
            Geometry::Point(point) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("POINT".to_string())),
                    (
                        Name::new("coordinates"),
                        GqlValue::List(vec![point.x().try_intox()?, point.y().try_intox()?]),
                    ),
                ]
                    .into(),
            ),
            Geometry::Line(line) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("LINE_STRING".to_string())),
                    (Name::new("coordinates"), geometry::coord_collection_to_list(line)?),
                ]
                    .into(),
            ),
            Geometry::MultiLine(multiline) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("MULTI_LINE".to_string())),
                    (
                        Name::new("coordinates"),
                        GqlValue::List(
                            multiline
                                .into_iter()
                                .map(geometry::coord_collection_to_list)
                                .collect::<Result<Vec<_>, GqlError>>()?,
                        ),
                    ),
                ]
                    .into(),
            ),
            Geometry::MultiPoint(multipoint) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("MULTI_POINT".to_string())),
                    (
                        Name::new("coordinates"),
                        geometry::coord_collection_to_list(multipoint.into_iter().map(|p| p.0))?,
                    ),
                ]
                    .into(),
            ),
            Geometry::Polygon(polygon) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("POLYGON".to_string())),
                    (Name::new("coordinates"), geometry::polygon_to_list(&polygon)?),
                ]
                    .into(),
            ),
            Geometry::MultiPolygon(multipolygon) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("MULTI_POLYGON".to_string())),
                    (
                        Name::new("coordinates"),
                        GqlValue::List(
                            multipolygon
                                .iter()
                                .map(geometry::polygon_to_list)
                                .collect::<Result<Vec<_>, _>>()?,
                        ),
                    ),
                ]
                    .into(),
            ),
            Geometry::Collection(collection) => GqlValue::Object(
                [
                    (Name::new("type"), GqlValue::String("COLLECTION".to_string())),
                    (
                        Name::new("geometries"),
                        GqlValue::List(
                            collection
                                .into_iter()
                                .map(|g| sql_value_to_gql_value(SqlValue::Geometry(g)).unwrap())
                                .collect(),
                        ),
                    ),
                ]
                    .into(),
            ),
        },
        SqlValue::Bytes(b) => GqlValue::Binary(b.into_inner().into()),
        SqlValue::Thing(t) => GqlValue::String(t.to_string()),
        v => return Err(internal_error(format!("found unsupported value variant: {v:?}"))),
    };
    Ok(out)
}

pub fn kind_to_type(kind: Kind, types: &mut Vec<Type>, path: &[&Ident]) ->
Result<TypeRef,
    GqlError> {
    let ty_name = path.iter().map(|i| i.0.to_string())
        .collect::<Vec<_>>().join(".").to_pascal_case();
    let (optional, match_kind) = match kind {
        Kind::Option(op_ty) => (true, *op_ty),
        _ => (false, kind),
    };
    let out_ty = match match_kind {
        Kind::Any => TypeRef::named("Any"),
        Kind::Null => TypeRef::named("Null"),
        Kind::Bool => TypeRef::named(TypeRef::BOOLEAN),
        Kind::Bytes => TypeRef::named("Bytes"),
        Kind::Datetime => TypeRef::named("DateTime"),
        Kind::Decimal => TypeRef::named("Decimal"),
        Kind::Duration => TypeRef::named("Duration"),
        Kind::Float => TypeRef::named(TypeRef::FLOAT),
        Kind::Int => TypeRef::named(TypeRef::INT),
        Kind::Number => TypeRef::named("Number"),
        Kind::Object => TypeRef::named(format!("{}Object", ty_name)),
        Kind::Point => TypeRef::named("GeometryPoint"),
        Kind::Regex => return Err(schema_error("Kind::Regex is not yet supported")),
        Kind::String => TypeRef::named(TypeRef::STRING),
        Kind::Uuid => TypeRef::named("UUID"),
        Kind::Record(mut r) => match r.len() {
            0 => TypeRef::named("Record"),
            1 => TypeRef::named(r.pop().unwrap().0.to_pascal_case()),
            _ => {
                let names: Vec<String> = r.into_iter().map(|t| t.0.to_pascal_case()).collect();

                let union_name = format!("{}RecordUnion", ty_name);
                let mut tmp_union = Union::new(&union_name)
                    .description(format!("A record which is one of: {}", names.join(", ")));
                for n in names {
                    tmp_union = tmp_union.possible_type(n);
                }

                types.push(Type::Union(tmp_union));
                TypeRef::named(union_name)
            }
        },
        Kind::Geometry(g) => match g.len() {
            0 => TypeRef::named("Geometry"),
            1 => {
                let name = g.into_iter().next().expect("checked that length is 1");
                TypeRef::named(geometry::geometry_kind_name_to_type_name(&name)?)
            }
            _ => {
                let geo_types = g
                    .iter()
                    .map(|n| geometry::geometry_kind_name_to_type_name(n).map(TypeRef::named))
                    .collect::<Result<Vec<_>, GqlError>>()?;

                let union_name = format!("{}Union", ty_name);
                let mut tmp_union = Union::new(&union_name)
                    .description(format!("A geometry which is one of: {}",
                                         geo_types.iter().map(|t| t.to_string()).collect::<Vec<_>>().join(", ")));
                for geo_type in geo_types {
                    tmp_union = tmp_union.possible_type(geo_type.type_name())
                }
                types.push(Type::Union(tmp_union));
                TypeRef::named(union_name)
            }
        },
        Kind::Option(t) => {
            let mut non_op_ty = *t;
            while let Kind::Option(inner) = non_op_ty {
                non_op_ty = *inner;
            }
            kind_to_type(non_op_ty, types, path)?
        }
        Kind::Either(ks) => {
            let (ls, others): (Vec<Kind>, Vec<Kind>) =
                ks.into_iter().partition(|k| matches!(k, Kind::Literal(Literal::String(_))));

            let enum_ty = if !ls.is_empty() {
                let vals: Vec<String> = ls
                    .into_iter()
                    .map(|l| {
                        let Kind::Literal(Literal::String(out)) = l else {
                            unreachable!(
                                "just checked that this is a Kind::Literal(Literal::String(_))"
                            );
                        };
                        out.0.to_string().to_screaming_snake_case()
                    })
                    .collect();

                let enum_name = format!("{}Enum", ty_name);
                let tmp_enum = Enum::new(&enum_name)
                    .description(format!("Represents one of the following states: {}", vals.join(", ")))
                    .items(vals);

                types.push(Type::Enum(tmp_enum));
                if others.is_empty() {
                    return Ok(TypeRef::named(enum_name));
                }
                Some(enum_name)
            } else {
                None
            };

            let pos_names: Result<Vec<TypeRef>, GqlError> =
                others.into_iter().map(|k| kind_to_type(k, types, path)).collect();
            let pos_names: Vec<String> = pos_names?.into_iter().map(|tr| tr.to_string()).collect();

            let union_name = format!("{}Union", ty_name);
            let mut tmp_union = Union::new(&union_name)
                .description(format!("A union of: {}", pos_names.join(", ")));
            for n in pos_names {
                tmp_union = tmp_union.possible_type(n);
            }

            if let Some(ty) = enum_ty {
                tmp_union = tmp_union.possible_type(ty);
            }

            types.push(Type::Union(tmp_union));
            TypeRef::named(union_name)
        }
        Kind::Set(_, _) => return Err(schema_error("Kind::Set is not yet supported")),
        Kind::Array(k, _) => TypeRef::List(Box::new(kind_to_type(*k, types, path)?)),
        Kind::Function(_, _) => return Err(schema_error("Kind::Function is not yet supported")),
        Kind::Range => return Err(schema_error("Kind::Range is not yet supported")),
        //TODO(raphaeldarley): check if union is of literals and generate enum
        // generate custom scalar from other literals?
        Kind::Literal(_) => return Err(schema_error("Kind::Literal is not yet supported")),
        Kind::References(ft, _) => {
            let inner = match ft.to_owned() {
                Some(ft) => Kind::Record(vec![ft]),
                None => Kind::Record(vec![]),
            };
            TypeRef::List(Box::new(kind_to_type(inner, types, path)?))
        }
        Kind::File(_) => return Err(schema_error("Kind::File is not yet supported")),
    };

    let out = match optional {
        true => out_ty,
        false => TypeRef::NonNull(Box::new(out_ty)),
    };
    Ok(out)
}

pub fn unwrap_type(ty: TypeRef) -> TypeRef {
    match ty {
        TypeRef::NonNull(t) => unwrap_type(*t),
        _ => ty,
    }
}

macro_rules! either_try_kind {
	($ks:ident, $val:expr, Kind::Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, Array) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, Record) => {
		for arr_kind in $ks.iter().filter(|k| matches!(k, Kind::Array(_, _))).cloned() {
			either_try_kind!($ks, $val, arr_kind);
		}
	};
	($ks:ident, $val:expr, AllNumbers) => {
		either_try_kind!($ks, $val, Kind::Int);
		either_try_kind!($ks, $val, Kind::Float);
		either_try_kind!($ks, $val, Kind::Decimal);
		either_try_kind!($ks, $val, Kind::Number);
	};
	($ks:ident, $val:expr, $kind:expr) => {
		if $ks.contains(&$kind) {
			if let Ok(out) = gql_to_sql_kind($val, $kind) {
				return Ok(out);
			}
		}
	};
}

macro_rules! either_try_kinds {
	($ks:ident, $val:expr, $($kind:tt),+) => {
		$(either_try_kind!($ks, $val, $kind));+
	};
}

macro_rules! any_try_kind {
	($val:expr, $kind:expr) => {
		if let Ok(out) = gql_to_sql_kind($val, $kind) {
			return Ok(out);
		}
	};
}
macro_rules! any_try_kinds {
	($val:expr, $($kind:tt),+) => {
		$(any_try_kind!($val, $kind));+
	};
}

pub fn gql_to_sql_kind(val: &GqlValue, kind: Kind) -> Result<SqlValue, GqlError> {
    use crate::gql::geometry;
    use crate::syn;
    match kind {
        Kind::Any => match val {
            GqlValue::String(s) => {
                use Kind::*;
                any_try_kinds!(val, Datetime, Duration, Uuid);
                Ok(SqlValue::Strand(s.as_str().into()))
            }
            GqlValue::Null => Ok(SqlValue::Null),
            obj @ GqlValue::Object(_) => gql_to_sql_kind(obj, Kind::Object),
            num @ GqlValue::Number(_) => gql_to_sql_kind(num, Kind::Number),
            GqlValue::Boolean(b) => Ok(SqlValue::Bool(*b)),
            bin @ GqlValue::Binary(_) => gql_to_sql_kind(bin, Kind::Bytes),
            GqlValue::Enum(s) => Ok(SqlValue::Strand(s.as_str().into())),
            arr @ GqlValue::List(_) => gql_to_sql_kind(arr, Kind::Array(Box::new(Kind::Any), None)),
        },
        Kind::Null => match val {
            GqlValue::Null => Ok(SqlValue::Null),
            _ => Err(type_error(kind, val)),
        },
        Kind::Bool => match val {
            GqlValue::Boolean(b) => Ok(SqlValue::Bool(*b)),
            _ => Err(type_error(kind, val)),
        },
        Kind::Bytes => match val {
            GqlValue::Binary(b) => Ok(SqlValue::Bytes(b.to_owned().to_vec().into())),
            _ => Err(type_error(kind, val)),
        },
        Kind::Datetime => match val {
            GqlValue::String(s) => match syn::datetime(s) {
                Ok(dt) => Ok(dt.into()),
                Err(_) => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Decimal => match val {
            GqlValue::Number(n) => {
                if let Some(int) = n.as_i64() {
                    Ok(SqlValue::Number(sql::Number::Decimal(int.into())))
                } else if let Some(d) = n.as_f64().and_then(Decimal::from_f64) {
                    Ok(SqlValue::Number(sql::Number::Decimal(d)))
                } else if let Some(uint) = n.as_u64() {
                    Ok(SqlValue::Number(sql::Number::Decimal(uint.into())))
                } else {
                    Err(type_error(kind, val))
                }
            }
            GqlValue::String(s) => match syn::value(s) {
                Ok(SqlValue::Number(n)) => match n {
                    sql::Number::Int(i) => Ok(SqlValue::Number(sql::Number::Decimal(i.into()))),
                    sql::Number::Float(f) => match Decimal::from_f64(f) {
                        Some(d) => Ok(SqlValue::Number(sql::Number::Decimal(d))),
                        None => Err(type_error(kind, val)),
                    },
                    sql::Number::Decimal(d) => Ok(SqlValue::Number(sql::Number::Decimal(d))),
                },
                _ => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Duration => match val {
            GqlValue::String(s) => match syn::duration(s) {
                Ok(d) => Ok(d.into()),
                Err(_) => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Float => match val {
            GqlValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(SqlValue::Number(sql::Number::Float(i as f64)))
                } else if let Some(f) = n.as_f64() {
                    Ok(SqlValue::Number(sql::Number::Float(f)))
                } else if let Some(uint) = n.as_u64() {
                    Ok(SqlValue::Number(sql::Number::Float(uint as f64)))
                } else {
                    unreachable!("serde_json::Number must be either i64, u64 or f64")
                }
            }
            GqlValue::String(s) => match syn::value(s) {
                Ok(SqlValue::Number(n)) => match n {
                    sql::Number::Int(int) => Ok(SqlValue::Number(sql::Number::Float(int as f64))),
                    sql::Number::Float(float) => Ok(SqlValue::Number(sql::Number::Float(float))),
                    sql::Number::Decimal(d) => match d.try_into() {
                        Ok(f) => Ok(SqlValue::Number(sql::Number::Float(f))),
                        _ => Err(type_error(kind, val)),
                    },
                },
                _ => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Int => match val {
            GqlValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(SqlValue::Number(sql::Number::Int(i)))
                } else {
                    Err(type_error(kind, val))
                }
            }
            GqlValue::String(s) => match syn::value(s) {
                Ok(SqlValue::Number(n)) => match n {
                    sql::Number::Int(int) => Ok(SqlValue::Number(sql::Number::Int(int))),
                    sql::Number::Float(float) => {
                        if float.fract() == 0.0 {
                            Ok(SqlValue::Number(sql::Number::Int(float as i64)))
                        } else {
                            Err(type_error(kind, val))
                        }
                    }
                    sql::Number::Decimal(d) => match d.try_into() {
                        Ok(i) => Ok(SqlValue::Number(sql::Number::Int(i))),
                        _ => Err(type_error(kind, val)),
                    },
                },
                _ => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Number => match val {
            GqlValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(SqlValue::Number(sql::Number::Int(i)))
                } else if let Some(f) = n.as_f64() {
                    Ok(SqlValue::Number(sql::Number::Float(f)))
                } else if let Some(uint) = n.as_u64() {
                    Ok(SqlValue::Number(sql::Number::Decimal(uint.into())))
                } else {
                    unreachable!("serde_json::Number must be either i64, u64 or f64")
                }
            }
            GqlValue::String(s) => match syn::value(s) {
                Ok(SqlValue::Number(n)) => Ok(SqlValue::Number(n)),
                _ => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Object => match val {
            GqlValue::Object(o) => {
                let out: Result<BTreeMap<String, SqlValue>, GqlError> = o
                    .iter()
                    .map(|(k, v)| gql_to_sql_kind(v, Kind::Any).map(|sqlv| (k.to_string(), sqlv)))
                    .collect();
                Ok(SqlValue::Object(out?.into()))
            }
            GqlValue::String(s) => match syn::value_legacy_strand(s.as_str()) {
                Ok(obj @ SqlValue::Object(_)) => Ok(obj),
                _ => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Point => match val {
            GqlValue::List(l) => match l.as_slice() {
                [GqlValue::Number(x), GqlValue::Number(y)] => match (x.as_f64(), y.as_f64()) {
                    (Some(x), Some(y)) => Ok(SqlValue::Geometry(Geometry::Point((x, y).into()))),
                    _ => Err(type_error(kind, val)),
                },
                _ => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::String => match val {
            GqlValue::String(s) => Ok(SqlValue::Strand(s.to_owned().into())),
            GqlValue::Enum(s) => Ok(SqlValue::Strand(s.as_str().into())),
            _ => Err(type_error(kind, val)),
        },
        Kind::Uuid => match val {
            GqlValue::String(s) => match s.parse::<uuid::Uuid>() {
                Ok(u) => Ok(SqlValue::Uuid(u.into())),
                Err(_) => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Record(ref ts) => match val {
            GqlValue::String(s) => match syn::thing(s) {
                Ok(t) => match ts.contains(&t.tb.as_str().into()) {
                    true => Ok(SqlValue::Thing(t)),
                    false => Err(type_error(kind, val)),
                },
                Err(_) => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Geometry(ref ts) => match &val {
            GqlValue::Object(map) => match map.get("type") {
                Some(t) => match t {
                    GqlValue::String(acutal_t) => {
                        let mut included = false;
                        for ty in ts {
                            if geometry::geometry_kind_name_to_type_name(ty)? == acutal_t {
                                included = true;
                                break;
                            }
                        }
                        if included {
                            geometry::extract_geometry(map)
                                .map(SqlValue::Geometry)
                                .ok_or_else(|| type_error(kind, val))
                        } else {
                            Err(type_error(kind, val))
                        }
                    }
                    _ => Err(type_error(kind, val)),
                },
                None => Err(type_error(kind, val)),
            },
            _ => Err(type_error(kind, val)),
        },
        Kind::Option(k) => match val {
            GqlValue::Null => Ok(SqlValue::None),
            v => gql_to_sql_kind(v, *k),
        },
        // TODO: handle nested eithers
        Kind::Either(ref ks) => {
            use Kind::*;

            match val {
                GqlValue::Null => {
                    if ks.iter().any(|k| matches!(k, Kind::Option(_))) {
                        Ok(SqlValue::None)
                    } else if ks.contains(&Kind::Null) {
                        Ok(SqlValue::Null)
                    } else {
                        Err(type_error(kind, val))
                    }
                }
                num @ GqlValue::Number(_) => {
                    either_try_kind!(ks, num, AllNumbers);
                    Err(type_error(kind, val))
                }
                string @ GqlValue::String(_) => {
                    either_try_kinds!(
						ks, string, Datetime, Duration, AllNumbers, Object, Uuid, Array, Any,
						String
					);
                    Err(type_error(kind, val))
                }
                bool @ GqlValue::Boolean(_) => {
                    either_try_kind!(ks, bool, Kind::Bool);
                    Err(type_error(kind, val))
                }
                GqlValue::Binary(_) => {
                    Err(resolver_error("binary input for Either is not yet supported"))
                }
                GqlValue::Enum(n) => {
                    either_try_kind!(ks, &GqlValue::String(n.to_string()), Kind::String);
                    Err(type_error(kind, val))
                }
                list @ GqlValue::List(_) => {
                    either_try_kind!(ks, list, Kind::Array);
                    Err(type_error(kind, val))
                }
                // TODO: consider geometry and other types that can come from objects
                obj @ GqlValue::Object(_) => {
                    either_try_kind!(ks, obj, Object);
                    Err(type_error(kind, val))
                }
            }
        }
        Kind::Set(_k, _n) => Err(resolver_error("Sets are not yet supported")),
        Kind::Array(ref k, n) => match val {
            GqlValue::List(l) => {
                let list_iter = l.iter().map(|v| gql_to_sql_kind(v, *k.to_owned()));
                let list: Result<Vec<SqlValue>, GqlError> = list_iter.collect();

                match (list, n) {
                    (Ok(l), Some(n)) => {
                        if l.len() as u64 == n {
                            Ok(l.into())
                        } else {
                            Err(type_error(kind, val))
                        }
                    }
                    (Ok(l), None) => Ok(l.into()),
                    (Err(e), _) => Err(e),
                }
            }
            _ => Err(type_error(kind, val)),
        },
        Kind::Function(_, _) => Err(resolver_error("Sets are not yet supported")),
        Kind::Range => Err(resolver_error("Ranges are not yet supported")),
        Kind::Literal(_) => Err(resolver_error("Literals are not yet supported")),
        Kind::Regex => Err(resolver_error("Regexes are not yet supported")),
        Kind::References(_, _) => Err(resolver_error("Cannot convert value into references")),
        Kind::File(_) => Err(resolver_error("Files are not yet supported")),
    }
}
