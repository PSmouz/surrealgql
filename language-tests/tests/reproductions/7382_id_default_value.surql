/**
[env]
namespace = true
database = true

[test]
reason = "Issue #7382: DEFAULT on the `id` field (with or without TYPE) is allowed and supplies the record id; VALUE is rejected by design because the id is immutable."
issue = 7382

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
error = "Cannot use the `VALUE` keyword on the `id` field."

*/

-- Exactly as written in the issue: DEFAULT with no TYPE, using OVERWRITE
DEFINE FIELD OVERWRITE id ON tbl DEFAULT rand::ulid();
-- The default produces a ulid string as the record id key
type::is_string(record::id((CREATE ONLY tbl).id));

-- VALUE is rejected on purpose (id is immutable, VALUE recomputes on update)
DEFINE FIELD OVERWRITE id ON tbl VALUE rand::ulid();
