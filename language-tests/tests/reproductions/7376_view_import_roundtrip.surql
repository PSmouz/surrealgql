/**
[env]
namespace = true
database = true

[test]
reason = "A database export emits the stored rows of a materialised view as INSERT statements. Re-importing them (under OPTION IMPORT) must still be allowed even though manual writes to a view are otherwise rejected, so the export -> import round-trip keeps working. This guards the `opt.import` exemption in the read-only view check."
issue = 7376

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: adults:ada, name: 'Ada' }]"
*/

-- Replaying an export: OPTION IMPORT skips view maintenance, and the
-- exported view rows are re-inserted directly.
OPTION IMPORT;
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE adults TYPE ANY AS SELECT name FROM person WHERE age >= 18;
INSERT INTO person { id: person:ada, name: 'Ada', age: 36 };

-- Under OPTION IMPORT this write to the view table is allowed.
INSERT INTO adults { id: adults:ada, name: 'Ada' };

-- The imported view row is present.
SELECT * FROM adults;
