use async_graphql::Value as GqlValue;
use async_graphql::dynamic::{FieldFuture, FieldValue, ResolverContext};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

use super::error::{GqlError, internal_error, resolver_error};
use super::tables::{CachedRecord, VersionedRecord};
use crate::val::Object as SurObject;

#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectionArgs {
	pub first: Option<usize>,
	pub after: Option<String>,
	pub last: Option<usize>,
	pub before: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PageInfo {
	pub has_next_page: bool,
	pub has_previous_page: bool,
	pub start_cursor: Option<String>,
	pub end_cursor: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum ConnectionNode {
	Record {
		record: CachedRecord,
		runtime_type_name: Option<String>,
	},
	VersionedRecord {
		record: VersionedRecord,
		runtime_type_name: Option<String>,
	},
	Object {
		object: SurObject,
		runtime_type_name: Option<String>,
	},
	Value(GqlValue),
}

impl ConnectionNode {
	pub(crate) fn to_field_value(&self) -> FieldValue<'static> {
		match self {
			ConnectionNode::Record {
				record,
				runtime_type_name,
			} => {
				let field = FieldValue::owned_any(record.clone());
				if let Some(type_name) = runtime_type_name {
					field.with_type(type_name.clone())
				} else {
					field
				}
			}
			ConnectionNode::VersionedRecord {
				record,
				runtime_type_name,
			} => {
				let field = FieldValue::owned_any(record.clone());
				if let Some(type_name) = runtime_type_name {
					field.with_type(type_name.clone())
				} else {
					field
				}
			}
			ConnectionNode::Object {
				object,
				runtime_type_name,
			} => {
				let field = FieldValue::owned_any(object.clone());
				if let Some(type_name) = runtime_type_name {
					field.with_type(type_name.clone())
				} else {
					field
				}
			}
			ConnectionNode::Value(value) => FieldValue::value(value.clone()),
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionEdge {
	pub cursor: String,
	pub node: ConnectionNode,
	pub relation_record: Option<CachedRecord>,
}

#[derive(Clone, Debug)]
pub(crate) struct Connection {
	pub edges: Vec<ConnectionEdge>,
	pub page_info: PageInfo,
	pub total_count: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct PaginationWindow<'a, T> {
	pub selected: Vec<&'a T>,
	pub page_info: PageInfo,
	pub total_count: i64,
}

pub(crate) fn encode_cursor(value: &str) -> String {
	URL_SAFE_NO_PAD.encode(value.as_bytes())
}

pub(crate) fn decode_cursor(cursor: &str) -> Result<String, GqlError> {
	let bytes =
		URL_SAFE_NO_PAD.decode(cursor).map_err(|_| resolver_error("Invalid cursor value"))?;
	String::from_utf8(bytes).map_err(|_| resolver_error("Invalid cursor value"))
}

pub(crate) fn paginate<'a, T, F>(
	items: &'a [T],
	args: &ConnectionArgs,
	cursor_for: F,
) -> Result<PaginationWindow<'a, T>, GqlError>
where
	F: Fn(&T) -> String,
{
	if args.first.is_some() && args.last.is_some() {
		return Err(resolver_error("Cannot use both `first` and `last` in the same connection"));
	}

	let all_cursors: Vec<String> = items.iter().map(&cursor_for).collect();
	let total_count = items.len() as i64;

	let mut start = 0usize;
	let mut end = items.len();

	if let Some(after) = args.after.as_deref() {
		let after = decode_cursor(after)?;
		let idx = all_cursors
			.iter()
			.position(|cursor| cursor == &after)
			.ok_or_else(|| resolver_error("Invalid `after` cursor"))?;
		start = idx.saturating_add(1);
	}

	if let Some(before) = args.before.as_deref() {
		let before = decode_cursor(before)?;
		let idx = all_cursors
			.iter()
			.position(|cursor| cursor == &before)
			.ok_or_else(|| resolver_error("Invalid `before` cursor"))?;
		end = idx;
	}

	if start > end || end > items.len() {
		return Ok(PaginationWindow {
			selected: Vec::new(),
			page_info: PageInfo {
				has_next_page: false,
				has_previous_page: start > 0,
				start_cursor: None,
				end_cursor: None,
			},
			total_count,
		});
	}

	let window = &items[start..end];
	let mut selected: Vec<&T> = window.iter().collect();
	let mut has_previous_page = start > 0;
	let mut has_next_page = end < items.len();

	if let Some(first) = args.first {
		if selected.len() > first {
			selected.truncate(first);
			has_next_page = true;
		}
	}

	if let Some(last) = args.last {
		if selected.len() > last {
			let trim = selected.len() - last;
			selected.drain(0..trim);
			has_previous_page = true;
		}
	}

	let start_cursor = selected.first().map(|item| encode_cursor(&cursor_for(item)));
	let end_cursor = selected.last().map(|item| encode_cursor(&cursor_for(item)));

	Ok(PaginationWindow {
		selected,
		page_info: PageInfo {
			has_next_page,
			has_previous_page,
			start_cursor,
			end_cursor,
		},
		total_count,
	})
}

pub(crate) fn value_resolver<P, F, V>(
	extractor: F,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static
where
	P: Clone + Send + Sync + 'static,
	F: Fn(&P) -> V + Clone + Send + Sync + 'static,
	V: Into<GqlValue> + Send,
{
	move |ctx: ResolverContext| {
		let extractor = extractor.clone();
		FieldFuture::new(async move {
			let parent = ctx.parent_value.try_downcast_ref::<P>().map_err(|_| {
				internal_error(format!(
					"Unexpected connection parent type: {}",
					std::any::type_name::<P>()
				))
			})?;
			Ok(Some(FieldValue::value(extractor(parent).into())))
		})
	}
}

pub(crate) fn object_resolver<P, F, V>(
	extractor: F,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static
where
	P: Clone + Send + Sync + 'static,
	F: Fn(&P) -> V + Clone + Send + Sync + 'static,
	V: Clone + Send + Sync + 'static,
{
	move |ctx: ResolverContext| {
		let extractor = extractor.clone();
		FieldFuture::new(async move {
			let parent = ctx.parent_value.try_downcast_ref::<P>().map_err(|_| {
				internal_error(format!(
					"Unexpected connection parent type: {}",
					std::any::type_name::<P>()
				))
			})?;
			Ok(Some(FieldValue::owned_any(extractor(parent))))
		})
	}
}
