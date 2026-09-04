/**
[env]
namespace = true
database = true

[test]
reason = "ASSERT on the `id` field is processed by the regular field pipeline like any other field's ASSERT. `$value` is the whole record id; extract the key with `id.id()` / `$value.id()` (the method form of `record::id`). It runs on creation for generated, DEFAULT-supplied, explicitly-supplied, and INSERT keys; is skipped on update and under OPTION IMPORT. Issue #7382."
issue = 7382

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "'01ARZ3NDEKTSV4RRFFQ69G5FAV'"

[[test.results]]
error = "Found user:notaulid for field `id`, with record `user:notaulid`, but field must conform to: id.id().is_ulid()"

[[test.results]]
error = "Found user:stillnotaulid for field `id`, with record `user:stillnotaulid`, but field must conform to: id.id().is_ulid()"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
error = "Found counter:0 for field `id`, with record `counter:0`, but field must conform to: id.id() > 0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'London'"

[[test.results]]
error = "Found reading:['London', d'1999-01-01T00:00:00Z'] for field `id`, with record `reading:['London', d'1999-01-01T00:00:00Z']`, but field must conform to: id.id()[1] > d'2020-01-01T00:00:00Z'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'abcd'"

[[test.results]]
error = "Found acct:ab for field `id`, with record `acct:ab`, but field must conform to: id.id().len() >= 3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'ok'"

[[test.results]]
error = "Found tag:forbidden for field `id`, with record `tag:forbidden`, but field must conform to: $value.id() = id.id() AND $value.id() != 'forbidden'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

-- ------------------------------------------------------------
-- Issue #7382: a string id with a ULID DEFAULT and a ULID assertion.
-- The generated default passes; an explicitly supplied valid ULID passes;
-- a non-ULID key is rejected whether given as a target or via SET — a DEFAULT
-- clause does not cause the ASSERT to be skipped.
-- ------------------------------------------------------------
DEFINE FIELD id ON user TYPE string DEFAULT rand::ulid() ASSERT id.id().is_ulid();
record::id((CREATE ONLY user).id).is_ulid();
record::id((CREATE ONLY user SET id = '01ARZ3NDEKTSV4RRFFQ69G5FAV').id);
CREATE user:notaulid;
CREATE user SET id = 'stillnotaulid';

-- ------------------------------------------------------------
-- A typed int id constrained to a positive range.
-- ------------------------------------------------------------
DEFINE FIELD id ON counter TYPE int ASSERT id.id() > 0;
record::id((CREATE ONLY counter:5).id);
CREATE counter:0;

-- ------------------------------------------------------------
-- A composite (tuple) id: assert on a single key element.
-- ------------------------------------------------------------
DEFINE FIELD id ON reading TYPE [string, datetime] DEFAULT ['London', time::now()] ASSERT id.id()[1] > d'2020-01-01T00:00:00Z';
record::id((CREATE ONLY reading).id)[0];
CREATE reading:['London', d'1999-01-01T00:00:00Z'];

-- ------------------------------------------------------------
-- The ASSERT applies to INSERT as well, against the supplied key.
-- ------------------------------------------------------------
DEFINE FIELD id ON acct TYPE string ASSERT id.id().len() >= 3;
record::id((INSERT INTO acct { id: 'abcd' })[0].id);
INSERT INTO acct { id: 'ab' };

-- ------------------------------------------------------------
-- `$value` is the whole record id for the id field; `$value.id()` is the key,
-- equal to `id.id()`. Works on an untyped id too (explicit target, no TYPE).
-- ------------------------------------------------------------
DEFINE FIELD id ON tag ASSERT $value.id() = id.id() AND $value.id() != 'forbidden';
record::id((CREATE ONLY tag:ok).id);
CREATE tag:forbidden;

-- ------------------------------------------------------------
-- Under OPTION IMPORT the id ASSERT is skipped (import restores already-
-- validated data verbatim, bypassing all field validation), so a key that
-- would otherwise fail the assertion is accepted. Kept last because OPTION
-- applies to every subsequent statement in the query.
-- ------------------------------------------------------------
DEFINE FIELD id ON imp TYPE int ASSERT id.id() > 100;
OPTION IMPORT;
CREATE imp:5;
