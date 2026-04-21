use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Field, FieldFuture, FieldValue, InputValue, Object, Subscription, SubscriptionField,
	SubscriptionFieldFuture, Type, TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use async_stream::try_stream;
use surrealdb_types::{Action as PublicAction, Notification as PublicNotification, ToSql};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::error::{GqlError, resolver_error};
use super::naming;
use super::schema::{record_id_to_raw, semantic_non_null_directive};
use super::tables::{CachedRecord, TableFilterRegistry, filter_name_from_table, parse_filter_arg};
use super::utils::{GqlValueUtils, execute_plan};
use crate::catalog::{FieldDefinition, TableDefinition, TableType};
use crate::dbs::Session;
use crate::expr::field::Selector;
use crate::expr::plan::TopLevelExpr;
use crate::expr::statements::{KillStatement, LiveFields, LiveStatement};
use crate::expr::{
	BinaryOperator, Cond, Expr, Field as QueryField, Fields, Idiom, Literal, LogicalPlan, Part,
};
use crate::kvs::Datastore;
use crate::val::{RecordId, TableName, Value};

/// Routes LIVE query notifications to their specific GraphQL subscribers.
///
/// Each GraphQL subscription registers its live query UUID and receives a
/// dedicated bounded mpsc channel. Notifications are dispatched in O(1) via
/// HashMap lookup. When a subscriber's channel is full the notification is
/// dropped (analogous to the "lagged" behaviour of broadcast channels).
pub struct NotificationRouter {
	routes: RwLock<HashMap<Uuid, mpsc::Sender<PublicNotification>>>,
	channel_capacity: usize,
}

impl NotificationRouter {
	pub fn new(channel_capacity: usize) -> Self {
		Self {
			routes: RwLock::new(HashMap::new()),
			channel_capacity: channel_capacity.max(1),
		}
	}

	fn subscribe(&self, live_id: Uuid) -> mpsc::Receiver<PublicNotification> {
		let (tx, rx) = mpsc::channel(self.channel_capacity);
		self.routes.write().unwrap_or_else(|e| e.into_inner()).insert(live_id, tx);
		rx
	}

	fn unsubscribe(&self, live_id: &Uuid) {
		self.routes.write().unwrap_or_else(|e| e.into_inner()).remove(live_id);
	}

	/// Route a notification to the matching subscriber, if any.
	///
	/// Clones the notification only when a matching subscriber exists.
	/// If the subscriber's channel is full the notification is dropped
	/// rather than blocking the dispatch loop.
	pub fn dispatch(&self, notification: &PublicNotification) {
		let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
		if let Some(sender) = routes.get(&notification.id) {
			match sender.try_send(notification.clone()) {
				Ok(()) => {}
				Err(mpsc::error::TrySendError::Full(_)) => {
					warn!(
						live_id = %notification.id,
						"GraphQL subscription channel full, notification dropped"
					);
				}
				Err(mpsc::error::TrySendError::Closed(_)) => {
					trace!(
						live_id = %notification.id,
						"GraphQL subscription channel closed, stale route"
					);
				}
			}
		}
	}

	pub fn has_subscribers(&self) -> bool {
		!self.routes.read().unwrap_or_else(|e| e.into_inner()).is_empty()
	}
}

pub(crate) fn process_subscriptions(
	tbs: &[TableDefinition],
	table_fields: &HashMap<String, Arc<[FieldDefinition]>>,
	table_filter_registry: &TableFilterRegistry,
	types: &mut Vec<Type>,
) -> Option<Subscription> {
	if tbs.is_empty() {
		return None;
	}

	let mut subscription = Subscription::new("Subscription");
	for tb in tbs {
		let fds = table_fields
			.get(tb.name.as_str())
			.cloned()
			.unwrap_or_else(|| Arc::<[FieldDefinition]>::from([]));
		let entity = subscription_entity_context(tb);
		let filter_spec =
			table_filter_registry.get(tb.name.as_str()).cloned().unwrap_or_else(|| {
				super::tables::FilterObjectSpec {
					type_name: entity.filter_type_name.clone(),
					description: format!(
						"Filter input for `{}` connections.",
						entity.entity_type_name
					),
					fields: Vec::new(),
				}
			});
		for event in subscription_events(tb) {
			let payload_type_name = register_subscription_payload_type(&entity, *event, types);
			subscription = subscription.field(make_table_subscription_field(
				tb,
				fds.clone(),
				filter_spec.clone(),
				entity.clone(),
				*event,
				payload_type_name,
			));
		}
	}

	Some(subscription)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SubscriptionEventKind {
	Created,
	Updated,
	Deleted,
	Related,
}

impl SubscriptionEventKind {
	fn field_suffix(self) -> &'static str {
		match self {
			Self::Created => "Created",
			Self::Updated => "Updated",
			Self::Deleted => "Deleted",
			Self::Related => "Related",
		}
	}

	fn payload_suffix(self) -> &'static str {
		self.field_suffix()
	}

	fn action_description(self) -> &'static str {
		match self {
			Self::Created => "created",
			Self::Updated => "updated",
			Self::Deleted => "deleted",
			Self::Related => "related",
		}
	}

	fn from_notification_action(action: PublicAction, is_relation: bool) -> Option<Self> {
		match action {
			PublicAction::Create => Some(if is_relation {
				Self::Related
			} else {
				Self::Created
			}),
			PublicAction::Update => Some(Self::Updated),
			PublicAction::Delete => Some(Self::Deleted),
			PublicAction::Killed => None,
		}
	}

	fn entity_is_nullable(self) -> bool {
		matches!(self, Self::Deleted)
	}
}

#[derive(Clone)]
struct SubscriptionEntityContext {
	field_name: String,
	entity_field_name: String,
	entity_type_name: String,
	filter_type_name: String,
	payload_type_prefix: String,
	description_name: String,
	is_relation: bool,
}

#[derive(Clone)]
struct SubscriptionPayloadValue {
	id: String,
	entity: Option<CachedRecord>,
}

fn subscription_events(tb: &TableDefinition) -> &'static [SubscriptionEventKind] {
	if matches!(&tb.table_type, TableType::Relation(_)) {
		&[
			SubscriptionEventKind::Related,
			SubscriptionEventKind::Updated,
			SubscriptionEventKind::Deleted,
		]
	} else {
		&[
			SubscriptionEventKind::Created,
			SubscriptionEventKind::Updated,
			SubscriptionEventKind::Deleted,
		]
	}
}

fn subscription_entity_context(tb: &TableDefinition) -> SubscriptionEntityContext {
	let tb_name = tb.name.as_str();
	match &tb.table_type {
		TableType::Relation(_) => SubscriptionEntityContext {
			field_name: naming::to_camel_case(tb_name),
			entity_field_name: naming::relation_payload_entity_field_name(tb_name),
			entity_type_name: naming::relation_type_name(tb_name),
			filter_type_name: naming::relation_filter_input_name(tb_name),
			payload_type_prefix: naming::to_pascal_case(tb_name),
			description_name: naming::relation_type_name(tb_name),
			is_relation: true,
		},
		_ => SubscriptionEntityContext {
			field_name: naming::singular_query_name(tb_name),
			entity_field_name: naming::payload_entity_field_name(tb_name),
			entity_type_name: naming::table_type_name(tb_name),
			filter_type_name: filter_name_from_table(&tb.name),
			payload_type_prefix: naming::table_type_name(tb_name),
			description_name: naming::table_type_name(tb_name),
			is_relation: false,
		},
	}
}

fn register_subscription_payload_type(
	entity: &SubscriptionEntityContext,
	event: SubscriptionEventKind,
	types: &mut Vec<Type>,
) -> String {
	let payload_type_name =
		format!("{}{}Payload", entity.payload_type_prefix, event.payload_suffix());
	if types
		.iter()
		.any(|ty| matches!(ty, Type::Object(obj) if obj.type_name() == payload_type_name))
	{
		return payload_type_name;
	}

	let entity_field_name = entity.entity_field_name.clone();
	let entity_type_name = entity.entity_type_name.clone();
	let mut entity_field = Field::new(
		&entity_field_name,
		if event.entity_is_nullable() {
			TypeRef::named(&entity_type_name)
		} else {
			TypeRef::named_nn(&entity_type_name)
		},
		move |ctx| {
			FieldFuture::new(async move {
				let payload = ctx.parent_value.try_downcast_ref::<SubscriptionPayloadValue>()?;
				Ok(payload.entity.as_ref().map(|entity| FieldValue::owned_any(entity.clone())))
			})
		},
	)
	.description(format!("The {} entity snapshot for this event.", entity.description_name));
	if !event.entity_is_nullable() {
		entity_field = entity_field.directive(semantic_non_null_directive());
	}

	let payload = Object::new(&payload_type_name)
		.description(format!(
			"Payload emitted when a `{}` record is {}.",
			entity.description_name,
			event.action_description()
		))
		.field(
			Field::new("id", TypeRef::named_nn(TypeRef::ID), |ctx| {
				FieldFuture::new(async move {
					let payload =
						ctx.parent_value.try_downcast_ref::<SubscriptionPayloadValue>()?;
					Ok(Some(FieldValue::value(payload.id.clone())))
				})
			})
			.description("The record id for this event.")
			.directive(semantic_non_null_directive()),
		)
		.field(entity_field);
	types.push(Type::Object(payload));
	payload_type_name
}

fn make_table_subscription_field(
	tb: &TableDefinition,
	fds: Arc<[FieldDefinition]>,
	filter_spec: super::tables::FilterObjectSpec,
	entity: SubscriptionEntityContext,
	event: SubscriptionEventKind,
	payload_type_name: String,
) -> SubscriptionField {
	let tb_name = tb.name.clone();
	let field_name = format!("{}{}", entity.field_name, event.field_suffix());
	let table_filter_name = entity.filter_type_name.clone();
	let description_name = entity.description_name.clone();
	let selectable_fields = selectable_top_level_fields(&fds);

	SubscriptionField::new(&field_name, TypeRef::named_nn(&payload_type_name), move |ctx| {
		let tb_name = tb_name.clone();
		let selectable_fields = selectable_fields.clone();
		let filter_spec = filter_spec.clone();
		let entity = entity.clone();
		SubscriptionFieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;
			let router = ctx.data::<Arc<NotificationRouter>>().map_err(|_| {
				async_graphql::Error::new(
					"GraphQL subscriptions are not enabled on this server node",
				)
			})?;
			let args = ctx.args.as_index_map();

			let live_sess = sess.as_ref().clone().with_rt(true);
			let fields = projected_live_fields(&ctx, &selectable_fields, &entity.entity_field_name);
			let table_filter_registry = ctx.data::<Arc<TableFilterRegistry>>()?;
			let cond = parse_subscription_cond(
				args,
				&filter_spec,
				&tb_name,
				table_filter_registry.as_ref(),
			)?;
			let live_id = start_table_live_query(ds, &live_sess, &tb_name, fields, cond).await?;
			let mut receiver = router.subscribe(live_id);
			let cleanup = LiveQueryCleanup::new(ds.clone(), live_sess, live_id, router.clone());

			Ok(try_stream! {
				let _cleanup = cleanup;
				loop {
					let Some(notification) = receiver.recv().await else {
						break;
					};
					let Some(notification_event) = SubscriptionEventKind::from_notification_action(
						notification.action,
						entity.is_relation,
					) else {
						continue;
					};
					if notification_event == event
						&& let Some(value) = notification_to_field_value(notification, notification_event)
					{
						yield value;
					}
				}
			})
		})
	})
	.description(format!(
		"LIVE query notifications for {} `{}` records.",
		event.action_description(),
		description_name
	))
	.argument(InputValue::new("id", TypeRef::named(TypeRef::ID)))
	.argument(InputValue::new("filterBy", TypeRef::named(&table_filter_name)))
}

fn selectable_top_level_fields(fds: &[FieldDefinition]) -> HashSet<String> {
	let mut out = HashSet::new();
	out.insert("id".to_string());
	for fd in fds {
		if fd.name.0.len() != 1 {
			continue;
		}
		if let Some(Part::Field(name)) = fd.name.0.first() {
			out.insert(name.clone());
		}
	}
	out
}

fn projected_live_fields(
	ctx: &async_graphql::dynamic::ResolverContext<'_>,
	selectable_fields: &HashSet<String>,
	entity_field_name: &str,
) -> LiveFields {
	let mut selected = vec!["id".to_string()];
	for field in ctx.field().selection_set() {
		let name = field.name();
		if name.starts_with("__") {
			continue;
		}
		if name != entity_field_name {
			continue;
		}
		for entity_field in field.selection_set() {
			let entity_field_name = entity_field.name();
			if entity_field_name.starts_with("__") {
				continue;
			}
			if selectable_fields.contains(entity_field_name) {
				selected.push(entity_field_name.to_string());
			}
		}
	}
	selected.sort_unstable();
	selected.dedup();
	let projected = selected
		.into_iter()
		.map(|name| {
			QueryField::Single(Selector {
				expr: Expr::Idiom(Idiom::field(name)),
				alias: None,
			})
		})
		.collect();
	LiveFields::Select(Fields::Select(projected))
}

fn parse_subscription_cond(
	args: &IndexMap<Name, GqlValue>,
	filter_spec: &super::tables::FilterObjectSpec,
	tb_name: &TableName,
	table_filter_registry: &TableFilterRegistry,
) -> Result<Option<Cond>, async_graphql::Error> {
	let id_cond = parse_id_cond(args, tb_name)?;
	let where_cond = parse_filter_arg(args, filter_spec, table_filter_registry)
		.map_err(|e| async_graphql::Error::new(e.to_string()))?;
	Ok(combine_cond(id_cond, where_cond))
}

fn parse_id_cond(
	args: &IndexMap<Name, GqlValue>,
	tb_name: &TableName,
) -> Result<Option<Cond>, async_graphql::Error> {
	let Some(id_val) = args.get("id") else {
		return Ok(None);
	};
	if matches!(id_val, GqlValue::Null) {
		return Ok(None);
	}
	let Some(id_str) = id_val.as_string() else {
		return Err(async_graphql::Error::new("id must be a record ID string"));
	};
	let rid: RecordId = crate::syn::record_id(&id_str)
		.map_err(|_| async_graphql::Error::new(format!("Invalid record ID format: {id_str}")))?
		.into();
	if &rid.table != tb_name {
		return Err(async_graphql::Error::new(format!(
			"Record ID `{id_str}` does not belong to table `{tb_name}`"
		)));
	}
	Ok(Some(Cond(Expr::Binary {
		left: Box::new(Expr::Idiom(Idiom::field("id".to_string()))),
		op: BinaryOperator::Equal,
		right: Box::new(Value::RecordId(rid).into_literal()),
	})))
}

fn combine_cond(left: Option<Cond>, right: Option<Cond>) -> Option<Cond> {
	match (left, right) {
		(Some(left), Some(right)) => Some(Cond(Expr::Binary {
			left: Box::new(left.0),
			op: BinaryOperator::And,
			right: Box::new(right.0),
		})),
		(Some(left), None) => Some(left),
		(None, Some(right)) => Some(right),
		(None, None) => None,
	}
}

fn notification_to_field_value(
	notification: PublicNotification,
	event: SubscriptionEventKind,
) -> Option<FieldValue<'static>> {
	let record: Value = notification.record.into();
	let result: Value = notification.result.into();
	let rid = match &result {
		Value::Object(obj) => extract_record_id(obj, &record)?,
		_ => extract_record_id_from_value(&record)?,
	};
	let entity = match result {
		Value::Object(obj) => Some(CachedRecord {
			rid: rid.clone(),
			version: None,
			data: obj,
		}),
		_ if event.entity_is_nullable() => None,
		_ => return None,
	};

	Some(FieldValue::owned_any(SubscriptionPayloadValue {
		id: record_id_to_raw(&rid),
		entity,
	}))
}

fn extract_record_id(obj: &crate::val::Object, fallback: &Value) -> Option<RecordId> {
	match obj.get("id") {
		Some(Value::RecordId(rid)) => Some(rid.clone().into()),
		_ => extract_record_id_from_value(fallback),
	}
}

fn extract_record_id_from_value(value: &Value) -> Option<RecordId> {
	match value {
		Value::RecordId(rid) => Some(rid.clone()),
		_ => None,
	}
}

async fn start_table_live_query(
	ds: &Datastore,
	sess: &Session,
	table: &TableName,
	fields: LiveFields,
	cond: Option<Cond>,
) -> Result<Uuid, async_graphql::Error> {
	let stmt = LiveStatement {
		id: Uuid::new_v4(),
		node: Uuid::new_v4(),
		fields,
		what: Expr::Table(table.clone()),
		cond,
		fetch: None,
	};
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Live(Box::new(stmt))],
	};
	let res = execute_plan(ds, sess, plan).await?;

	match res {
		Value::Uuid(id) => Ok(id.into()),
		value => {
			Err(resolver_error(format!("LIVE query did not return a UUID, got {}", value.to_sql()))
				.into())
		}
	}
}

async fn kill_live_query(ds: &Datastore, sess: &Session, live_id: Uuid) -> Result<(), GqlError> {
	let stmt = KillStatement {
		id: Expr::Literal(Literal::Uuid(live_id.into())),
	};
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Kill(stmt)],
	};
	let _ = execute_plan(ds, sess, plan).await?;
	Ok(())
}

struct LiveQueryCleanup {
	ds: Arc<Datastore>,
	sess: Session,
	live_id: Uuid,
	router: Arc<NotificationRouter>,
}

impl LiveQueryCleanup {
	fn new(
		ds: Arc<Datastore>,
		sess: Session,
		live_id: Uuid,
		router: Arc<NotificationRouter>,
	) -> Self {
		Self {
			ds,
			sess,
			live_id,
			router,
		}
	}
}

impl Drop for LiveQueryCleanup {
	fn drop(&mut self) {
		self.router.unsubscribe(&self.live_id);
		let Ok(handle) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let ds = self.ds.clone();
		let sess = self.sess.clone();
		let live_id = self.live_id;
		handle.spawn(async move {
			if let Err(err) = kill_live_query(&ds, &sess, live_id).await {
				trace!(?err, ?live_id, "failed to cleanup GraphQL live query");
			}
		});
	}
}
