/**
[env]
namespace = true
database = true

[test]
reason = "A DEFAULT on the `id` field supplies the record id when none is given, evaluated against the session and coerced to the declared kind; DEFAULT ALWAYS is forbidden and an explicit id always wins."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'London'"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'fromdefault'"

[[test.results]]
value = "'explicit'"

[[test.results]]
value = "'viaset'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'alice'"

[[test.results]]
value = "'explicit'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `id` of `badtype:notanint`: Expected `int` but found `'notanint'`"

[[test.results]]
error = "Cannot use the `DEFAULT ALWAYS` keyword on the `id` field."

*/

-- ------------------------------------------------------------
-- Composite (tuple) DEFAULT, evaluated per record (mirrors a
-- `[$auth.city, time::now()]` shape with a deterministic city)
-- ------------------------------------------------------------
DEFINE FIELD id ON reading TYPE [string, datetime] DEFAULT ['London', time::now()];
LET $r = (CREATE ONLY reading SET value = 20.1).id;
record::id($r)[0];
type::is_datetime(record::id($r)[1]);

-- ------------------------------------------------------------
-- A dynamic DEFAULT is evaluated freshly for each record (not a singleton)
-- ------------------------------------------------------------
DEFINE FIELD id ON ev TYPE uuid DEFAULT rand::uuid();
type::is_uuid(record::id((CREATE ONLY ev).id));
(CREATE ONLY ev).id != (CREATE ONLY ev).id;

-- ------------------------------------------------------------
-- An explicit id (target or data clause) always wins over the DEFAULT
-- ------------------------------------------------------------
DEFINE FIELD id ON ov TYPE string DEFAULT 'fromdefault';
record::id((CREATE ONLY ov).id);
record::id((CREATE ONLY ov:explicit).id);
record::id((CREATE ONLY ov SET id = 'viaset').id);

-- ------------------------------------------------------------
-- DEFAULT applies to INSERT too, evaluated against the inserted row: a
-- field-referencing DEFAULT resolves to the inserted value, and an explicit
-- id in the row still wins
-- ------------------------------------------------------------
DEFINE FIELD id ON ins TYPE string DEFAULT name;
record::id((INSERT INTO ins { name: 'alice' })[0].id);
record::id((INSERT INTO ins { id: 'explicit', name: 'bob' })[0].id);

-- ------------------------------------------------------------
-- A DEFAULT that does not conform to the declared kind is rejected
-- ------------------------------------------------------------
DEFINE FIELD id ON badtype TYPE int DEFAULT 'notanint';
CREATE badtype;

-- ------------------------------------------------------------
-- DEFAULT ALWAYS is forbidden on the id field (id is immutable)
-- ------------------------------------------------------------
DEFINE FIELD id ON alwaystbl TYPE uuid DEFAULT ALWAYS rand::uuid();
