use crate::gql::error::internal_error;
use crate::gql::ext::TryFromExt;
use crate::gql::GqlError;
use crate::sql::Geometry;
use async_graphql::indexmap::IndexMap;
use async_graphql::Name;
use async_graphql::Value as GqlValue;
use geo_types::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

fn coord_to_list(coord: Coord<f64>) -> Result<GqlValue, GqlError> {
    Ok(GqlValue::List(
        <[f64; 2]>::from(coord)
            .into_iter()
            .map(GqlValue::try_fromx)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

pub fn coord_collection_to_list(
    coord_collection: impl IntoIterator<Item=Coord<f64>>,
) -> Result<GqlValue, GqlError> {
    Ok(GqlValue::List(
        coord_collection.into_iter().map(coord_to_list).collect::<Result<Vec<_>, _>>()?,
    ))
}

pub fn polygon_to_list(polygon: &Polygon) -> Result<GqlValue, GqlError> {
    Ok(GqlValue::List(
        [polygon.exterior()]
            .into_iter()
            .chain(polygon.interiors().iter())
            .cloned()
            .map(coord_collection_to_list)
            .collect::<Result<_, _>>()?,
    ))
}

pub fn geometry_kind_name_to_type_name(name: &str) -> Result<&'static str, GqlError> {
    match name {
        "point" => Ok("GeometryPoint"),
        "line" => Ok("GeometryLineString"),
        "polygon" => Ok("GeometryPolygon"),
        "multipoint" => Ok("GeometryMultiPoint"),
        "multiline" => Ok("GeometryMultiLineString"),
        "multipolygon" => Ok("GeometryMultiPolygon"),
        "collection" => Ok("GeometryCollection"),
        _ => Err(internal_error("expected valid geometry name")),
    }
}

fn extract_coord(arr: &[GqlValue]) -> Option<Coord> {
    match arr {
        [GqlValue::Number(y), GqlValue::Number(x)] => Some(Coord {
            x: x.as_f64()?,
            y: y.as_f64()?,
        }),
        _ => None,
    }
}

fn extract_coord_list(arr: &[GqlValue]) -> Option<Vec<Coord>> {
    arr.iter()
        .map(|c| match c {
            GqlValue::List(c) => extract_coord(c),
            _ => None,
        })
        .collect()
}

fn extract_coord_list_list(arr: &[GqlValue]) -> Option<Vec<Vec<Coord>>> {
    arr.iter()
        .map(|c| match c {
            GqlValue::List(c) => extract_coord_list(c),
            _ => None,
        })
        .collect()
}

fn extract_polygon(arr: &[GqlValue]) -> Option<Polygon> {
    let mut line_strings = extract_coord_list_list(arr)?.into_iter().map(LineString);
    let exterior = line_strings.next()?;
    let interior = line_strings.collect();
    Some(Polygon::new(exterior, interior))
}

fn extract_polygon_list(arr: &[GqlValue]) -> Option<Vec<Polygon>> {
    arr.iter()
        .map(|c| match c {
            GqlValue::List(c) => extract_polygon(c),
            _ => None,
        })
        .collect()
}

pub fn extract_geometry(map: &IndexMap<Name, GqlValue>) -> Option<Geometry> {
    let ty = match map.get("type") {
        Some(GqlValue::String(ty)) => Some(ty),
        _ => None,
    };

    let coordinates = match map.get("coordinates") {
        Some(GqlValue::List(cs)) => Some(cs.as_slice()),
        _ => None,
    };

    let geometries = match map.get("geometries") {
        Some(GqlValue::List(cs)) => Some(cs.as_slice()),
        _ => None,
    };

    match ty?.as_str() {
        "GeometryPoint" => Some(Geometry::Point(Point(extract_coord(coordinates?).unwrap()))),
        "GeometryLineString" => Some(Geometry::Line(LineString(extract_coord_list(coordinates?)?))),
        "GeometryPolygon" => Some(Geometry::Polygon(extract_polygon(coordinates?)?)),
        "GeometryMultiPoint" => Some(Geometry::MultiPoint(MultiPoint(
            extract_coord_list(&coordinates?)?.into_iter().map(Point).collect(),
        ))),
        "GeometryMultiLineString" => Some(Geometry::MultiLine(MultiLineString(
            extract_coord_list_list(&coordinates?)?.into_iter().map(LineString).collect(),
        ))),
        "GeometryMultiPolygon" => {
            Some(Geometry::MultiPolygon(MultiPolygon(extract_polygon_list(&coordinates?)?)))
        }
        "GeometryCollection" => Some(Geometry::Collection(
            geometries?
                .iter()
                .map(|g| match g {
                    GqlValue::Object(inner_map) => extract_geometry(inner_map),
                    _ => None,
                })
                .collect::<Option<_>>()?,
        )),
        _ => None,
    }
}