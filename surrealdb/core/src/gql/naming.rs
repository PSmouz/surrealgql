use std::borrow::Cow;

/// Returns true when a database identifier is safe to project into the GraphQL
/// schema using the generated naming rules.
pub(crate) fn is_valid_db_name(name: &str) -> bool {
	if name.is_empty() {
		return false;
	}

	let mut chars = name.chars();
	match chars.next() {
		Some(c) if c.is_ascii_lowercase() => {}
		_ => return false,
	}

	chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn split_db_name(name: &str) -> impl Iterator<Item = Cow<'_, str>> {
	name.split('_').filter(|part| !part.is_empty()).map(Cow::Borrowed)
}

pub(crate) fn to_pascal_case(name: &str) -> String {
	let mut out = String::new();
	for part in split_db_name(name) {
		let mut chars = part.chars();
		if let Some(first) = chars.next() {
			out.push(first.to_ascii_uppercase());
			out.extend(chars);
		}
	}
	out
}

pub(crate) fn to_camel_case(name: &str) -> String {
	let mut parts = split_db_name(name);
	let Some(first) = parts.next() else {
		return String::new();
	};

	let mut out = first.into_owned();
	for part in parts {
		let mut chars = part.chars();
		if let Some(first) = chars.next() {
			out.push(first.to_ascii_uppercase());
			out.extend(chars);
		}
	}
	out
}

pub(crate) fn to_screaming_snake_case(name: &str) -> String {
	let mut out = String::new();
	let mut last_was_underscore = false;
	let chars = name.chars().collect::<Vec<_>>();

	for (idx, c) in chars.iter().copied().enumerate() {
		if c.is_ascii_alphanumeric() {
			let prev = idx.checked_sub(1).and_then(|i| chars.get(i)).copied();
			let next = chars.get(idx + 1).copied();
			let should_split_before_upper = c.is_ascii_uppercase()
				&& !out.is_empty()
				&& !last_was_underscore
				&& prev.is_some_and(|prev| {
					prev.is_ascii_lowercase()
						|| prev.is_ascii_digit()
						|| (prev.is_ascii_uppercase()
							&& next.is_some_and(|next| next.is_ascii_lowercase()))
				});
			if should_split_before_upper {
				out.push('_');
			}
			out.push(c.to_ascii_uppercase());
			last_was_underscore = false;
		} else if !out.is_empty() && !last_was_underscore {
			out.push('_');
			last_was_underscore = true;
		}
	}

	while out.ends_with('_') {
		out.pop();
	}

	if out.is_empty() {
		"VALUE".to_string()
	} else if out.as_bytes()[0].is_ascii_digit() {
		format!("VALUE_{out}")
	} else {
		out
	}
}

pub(crate) fn pluralize(name: &str) -> String {
	if let Some(stem) = name.strip_suffix('y') {
		let ends_with_consonant =
			stem.chars().last().is_some_and(|c| !matches!(c, 'a' | 'e' | 'i' | 'o' | 'u'));
		if ends_with_consonant {
			return format!("{stem}ies");
		}
	}

	if name.ends_with('s')
		|| name.ends_with('x')
		|| name.ends_with('z')
		|| name.ends_with("ch")
		|| name.ends_with("sh")
	{
		return format!("{name}es");
	}

	format!("{name}s")
}

pub(crate) fn singular_query_name(table_name: &str) -> String {
	to_camel_case(table_name)
}

pub(crate) fn plural_query_name(table_name: &str) -> String {
	to_camel_case(&pluralize(table_name))
}

pub(crate) fn table_type_name(table_name: &str) -> String {
	to_pascal_case(table_name)
}

pub(crate) fn relation_root_base_name(table_name: &str) -> String {
	format!("{table_name}_relation")
}

pub(crate) fn relation_type_name(table_name: &str) -> String {
	table_type_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_connection_type_name(table_name: &str) -> String {
	connection_type_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_edge_type_name(table_name: &str) -> String {
	edge_type_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_order_input_name(table_name: &str) -> String {
	order_input_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_order_field_enum_name(table_name: &str) -> String {
	order_field_enum_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_filter_input_name(table_name: &str) -> String {
	filter_input_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_singular_query_name(table_name: &str) -> String {
	singular_query_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_plural_query_name(table_name: &str) -> String {
	plural_query_name(&relation_root_base_name(table_name))
}

pub(crate) fn relation_payload_entity_field_name(table_name: &str) -> String {
	to_camel_case(&relation_root_base_name(table_name))
}

pub(crate) fn nested_type_name(path: &[&str]) -> String {
	path.iter().map(|part| to_pascal_case(part)).collect()
}

pub(crate) fn connection_type_name(base_name: &str) -> String {
	format!("{}Connection", to_pascal_case(base_name))
}

pub(crate) fn edge_type_name(base_name: &str) -> String {
	format!("{}Edge", to_pascal_case(base_name))
}

pub(crate) fn field_connection_type_name(type_name: &str, field_name: &str) -> String {
	format!("{}{}Connection", to_pascal_case(type_name), to_pascal_case(field_name))
}

pub(crate) fn field_edge_type_name(type_name: &str, field_name: &str) -> String {
	format!("{}{}Edge", to_pascal_case(type_name), to_pascal_case(field_name))
}

pub(crate) fn order_input_name(table_name: &str) -> String {
	format!("{}Order", to_pascal_case(table_name))
}

pub(crate) fn order_field_enum_name(table_name: &str) -> String {
	format!("{}OrderField", to_pascal_case(table_name))
}

pub(crate) fn filter_input_name(table_name: &str) -> String {
	format!("{}FilterInput", to_pascal_case(table_name))
}

pub(crate) fn scalar_filter_input_name(type_name: &str) -> String {
	format!("{}FilterInput", to_pascal_case(type_name))
}

pub(crate) fn enum_type_name(scope: &str) -> String {
	format!("{}Enum", to_pascal_case(scope))
}

pub(crate) fn mutation_input_name(mutation_type: &str, table_name: &str) -> String {
	format!("{}{}Input", to_pascal_case(mutation_type), to_pascal_case(table_name))
}

pub(crate) fn mutation_payload_name(mutation_type: &str, table_name: &str) -> String {
	format!("{}{}Payload", to_pascal_case(mutation_type), to_pascal_case(table_name))
}

pub(crate) fn payload_entity_field_name(table_name: &str) -> String {
	to_camel_case(table_name)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn validates_db_names() {
		assert!(is_valid_db_name("person"));
		assert!(is_valid_db_name("person_profile_2"));
		assert!(!is_valid_db_name(""));
		assert!(!is_valid_db_name("Person"));
		assert!(!is_valid_db_name("2person"));
		assert!(!is_valid_db_name("person-profile"));
	}

	#[test]
	fn converts_identifier_case() {
		assert_eq!(to_pascal_case("person_profile"), "PersonProfile");
		assert_eq!(to_camel_case("person_profile"), "personProfile");
		assert_eq!(to_screaming_snake_case("personProfile"), "PERSON_PROFILE");
		assert_eq!(to_screaming_snake_case("VILLA"), "VILLA");
		assert_eq!(to_screaming_snake_case("favorite-id"), "FAVORITE_ID");
		assert_eq!(to_screaming_snake_case("enum-1"), "ENUM_1");
		assert_eq!(to_screaming_snake_case("check_in"), "CHECK_IN");
	}

	#[test]
	fn pluralizes_query_names() {
		assert_eq!(pluralize("person"), "persons");
		assert_eq!(pluralize("company"), "companies");
		assert_eq!(plural_query_name("person"), "persons");
		assert_eq!(singular_query_name("person_profile"), "personProfile");
	}

	#[test]
	fn builds_graphql_type_names() {
		assert_eq!(table_type_name("person"), "Person");
		assert_eq!(
			nested_type_name(&["home", "information", "items", "items"]),
			"HomeInformationItemsItems"
		);
		assert_eq!(relation_type_name("likes"), "LikesRelation");
		assert_eq!(relation_connection_type_name("likes"), "LikesRelationConnection");
		assert_eq!(relation_edge_type_name("likes"), "LikesRelationEdge");
		assert_eq!(relation_order_input_name("likes"), "LikesRelationOrder");
		assert_eq!(relation_order_field_enum_name("likes"), "LikesRelationOrderField");
		assert_eq!(relation_filter_input_name("likes"), "LikesRelationFilterInput");
		assert_eq!(relation_singular_query_name("likes"), "likesRelation");
		assert_eq!(relation_plural_query_name("likes"), "likesRelations");
		assert_eq!(connection_type_name("likes"), "LikesConnection");
		assert_eq!(edge_type_name("likes"), "LikesEdge");
		assert_eq!(field_connection_type_name("Person", "likes"), "PersonLikesConnection");
		assert_eq!(field_edge_type_name("Person", "likes"), "PersonLikesEdge");
		assert_eq!(order_input_name("likes"), "LikesOrder");
		assert_eq!(order_field_enum_name("likes"), "LikesOrderField");
		assert_eq!(filter_input_name("likes"), "LikesFilterInput");
		assert_eq!(scalar_filter_input_name("datetime"), "DatetimeFilterInput");
		assert_eq!(enum_type_name("likes_rating"), "LikesRatingEnum");
	}

	#[test]
	fn builds_mutation_names() {
		assert_eq!(mutation_input_name("create", "person"), "CreatePersonInput");
		assert_eq!(mutation_input_name("relate", "likes"), "RelateLikesInput");
		assert_eq!(mutation_payload_name("delete", "person"), "DeletePersonPayload");
		assert_eq!(payload_entity_field_name("person_profile"), "personProfile");
		assert_eq!(relation_payload_entity_field_name("likes"), "likesRelation");
	}
}
