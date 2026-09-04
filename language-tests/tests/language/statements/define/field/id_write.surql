/**
[env]
namespace = true
database = true

[test]
reason = "A typed `id` field drives record-id generation and coerces supplied keys to the declared type at write time across CREATE/UPSERT/INSERT, synthesises singleton literal ids, rejects ranges, and forbids meaningless clauses at define time."

[[test.results]]
value = "NONE"

[[test.results]]
value = "'abc'"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
error = "Couldn't coerce value for field `id` of `uid_t:notauuid`: Expected `uuid` but found `'notauuid'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
error = "Cannot generate a record id of type `int` for the `int_t` table; specify an explicit record id, or declare the `id` field as `uuid` or `string` to auto-generate one."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
error = "Couldn't coerce value for field `id` of `arr_t:['a', 'b']`: Expected `int` but found `'a'` when coercing element at index 0 of `array<int>`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "123"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'singleton'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `ups_t:notauuid`: Expected `uuid` but found `'notauuid'`"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `ins_t:notauuid`: Expected `uuid` but found `'notauuid'`"

[[test.results]]
error = "Cannot execute CREATE statement using value: uid_t:1..5"

[[test.results]]
error = "Cannot execute CREATE statement using value: plain_t:1..5"

[[test.results]]
error = "Cannot use the `READONLY` keyword on the `id` field."

[[test.results]]
error = "Cannot use the `FLEXIBLE` keyword on the `id` field."

*/

-- ------------------------------------------------------------
-- TYPE string: an explicit key is kept; a missing key auto-generates a string
-- ------------------------------------------------------------
DEFINE FIELD id ON str_t TYPE string;
record::id((CREATE ONLY str_t:abc).id);
type::is_string(record::id((CREATE ONLY str_t).id));

-- ------------------------------------------------------------
-- TYPE uuid: a missing key auto-generates a UUID, which is then selectable;
-- a non-uuid key is rejected
-- ------------------------------------------------------------
DEFINE FIELD id ON uid_t TYPE uuid;
LET $auto = (CREATE ONLY uid_t).id;
type::is_uuid(record::id($auto));
$auto IN (SELECT VALUE id FROM uid_t);
CREATE uid_t:notauuid;

-- ------------------------------------------------------------
-- TYPE int: an explicit int is kept; a missing key cannot be synthesised
-- ------------------------------------------------------------
DEFINE FIELD id ON int_t TYPE int;
record::id((CREATE ONLY int_t:5).id);
CREATE int_t;

-- ------------------------------------------------------------
-- TYPE array<int>: a conforming key is kept; element types are enforced
-- ------------------------------------------------------------
DEFINE FIELD id ON arr_t TYPE array<int>;
record::id((CREATE ONLY arr_t:[1, 2, 3]).id);
CREATE arr_t:['a', 'b'];

-- ------------------------------------------------------------
-- Singleton literal ids are synthesised from the declared value
-- ------------------------------------------------------------
DEFINE FIELD id ON lit_int TYPE 123;
record::id((CREATE ONLY lit_int).id);
DEFINE FIELD id ON lit_str TYPE 'singleton';
record::id((CREATE ONLY lit_str).id);

-- ------------------------------------------------------------
-- Coercion applies to UPSERT and INSERT, not just CREATE
-- ------------------------------------------------------------
DEFINE FIELD id ON ups_t TYPE uuid;
UPSERT ups_t:notauuid;
DEFINE FIELD id ON ins_t TYPE uuid;
INSERT INTO ins_t { id: 'notauuid' };

-- ------------------------------------------------------------
-- Range keys are rejected as ids on both typed and untyped tables
-- ------------------------------------------------------------
CREATE uid_t:1..5;
CREATE plain_t:1..5;

-- ------------------------------------------------------------
-- Forbidden clauses on the `id` field
-- ------------------------------------------------------------
DEFINE FIELD id ON bad_t READONLY;
DEFINE FIELD id ON bad_t TYPE object FLEXIBLE;
