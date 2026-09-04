/**
[env]
namespace = true
database = true

[test]
reason = "ALTER FIELD enforces the same `id`-field clause restrictions as DEFINE FIELD (issue #7382 follow-up): VALUE, REFERENCE, COMPUTED, DEFAULT ALWAYS, READONLY, FLEXIBLE and non-key TYPEs are rejected, while ASSERT, a plain DEFAULT, a valid TYPE, and COMMENT are allowed. Previously ALTER bypassed these checks entirely."

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot use the `READONLY` keyword on the `id` field."

[[test.results]]
error = "Cannot use the `FLEXIBLE` keyword on the `id` field."

[[test.results]]
error = "Cannot use the `VALUE` keyword on the `id` field."

[[test.results]]
error = "Cannot use the `DEFAULT ALWAYS` keyword on the `id` field."

[[test.results]]
error = "Cannot use the `bool` type on the `id` field, as that's not a valid record id key."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ events: {  }, fields: { id: "DEFINE FIELD id ON t TYPE uuid DEFAULT `rand`::uuid() ASSERT id.id().is_uuid() COMMENT 'the primary key' PERMISSIONS FULL" }, indexes: {  }, lives: {  }, tables: {  } }"""
*/

-- A valid typed id field to alter.
DEFINE FIELD id ON t TYPE string;

-- Forbidden clauses are rejected on ALTER, just as on DEFINE.
ALTER FIELD id ON t READONLY;
ALTER FIELD id ON t FLEXIBLE;
ALTER FIELD id ON t VALUE 'x';
ALTER FIELD id ON t DEFAULT ALWAYS 'x';
ALTER FIELD id ON t TYPE bool;

-- Allowed clauses still succeed on ALTER.
ALTER FIELD id ON t TYPE uuid;
ALTER FIELD id ON t DEFAULT rand::uuid();
ALTER FIELD id ON t ASSERT id.id().is_uuid();
ALTER FIELD id ON t COMMENT 'the primary key';

-- The forbidden ALTERs did not take effect: the field reflects only the
-- allowed changes above.
INFO FOR TABLE t;
