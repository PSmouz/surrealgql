/// This macro is used to add a field to an object, typically a mutation or query object.
/// It allows for adding fields with a specific operation type (like "create", "update", or "delete")
/// and an object that implements the `type_name` method.
/// It can also be used to add a field without an operation type.
/// # Parameters
/// - `$root`: The root object to which the field is added (e.g., mutation or query).
/// - `$op`: (optional) The operation type, such as "create", "update", or "delete".
/// - `$obj`: The object to add, which should implement the `type_name` method.
/// - `$tb_name`: (optional) The table name, used for naming conventions and descriptions.
#[macro_export]
macro_rules! add_to_obj {
    (
        $root:ident,
        $op:literal,
        $obj:expr,
        $tb_name:expr
    ) => {
        $root = $root.field(
            Field::new(
                format!("{}{}", $op, $tb_name.to_pascal_case()),
                TypeRef::named($obj.type_name()),
                dummy_resolver(), //TODO: implement resolver
            )
            .description(format!("{}s a record of the `{}` table.", $op.to_pascal_case(), $tb_name))
            .argument(input_input!($op.to_pascal_case(), $tb_name))
        );
    };
    (
        $root:ident,
        $obj:expr
    ) => {
        $root = $root.field($obj);
    };
}

/// Generates a description string for a field definition.
///
/// This macro checks if the field definition has a comment and returns it as a formatted string.
/// If the comment is not present, it returns an empty string or a provided default description.
///
/// # Parameters
/// - `$fd`: The field definition to check for a comment.
/// - `$desc`: (optional) A default description to return if the field definition does not have a comment.
/// # Returns
/// - A formatted string containing the comment or the default description.
#[macro_export]
macro_rules! description {
    ($fd:ident) => {
        if let Some(ref c) = $fd.comment {
            format!("{c}")
        } else {
            "".to_string()
        }
    };
    ($fd:ident, $desc:expr) => {
        if let Some(ref c) = $fd.comment {
            format!("{c}")
        } else {
            $desc.to_string()
        }
    };
}