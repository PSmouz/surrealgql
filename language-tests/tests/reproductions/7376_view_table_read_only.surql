/**
[env]
namespace = true
database = true

[test]
reason = "View tables (DEFINE TABLE ... AS SELECT) are read-only. Their records are computed from the source query and maintained automatically, so manual CREATE / INSERT / UPSERT / UPDATE / DELETE / RELATE against the view must be rejected. Otherwise the view ends up holding a mix of computed and hand-inserted rows."
issue = 7376

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 36, id: person:ada, name: 'Ada' }]"

[[test.results]]
value = "[{ age: 12, id: person:bob, name: 'Bob' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: adults:ada, name: 'Ada' }]"

[[test.results]]
error = "Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "Cannot write to the `adults` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
value = "[{ id: adults:ada, name: 'Ada' }]"
*/

-- Source table with a couple of rows.
DEFINE TABLE person SCHEMALESS;
CREATE person:ada SET name = 'Ada', age = 36;
CREATE person:bob SET name = 'Bob', age = 12;

-- A view projecting only adults from the source table.
DEFINE TABLE adults TYPE ANY AS SELECT name FROM person WHERE age >= 18;

-- The view is computed from the source: only Ada qualifies.
SELECT * FROM adults;

-- Every manual write against the view table must be rejected.
CREATE adults:manual SET name = 'Manual';
INSERT INTO adults { id: adults:manual2, name: 'Manual2' };
UPSERT adults:manual SET name = 'Manual';
UPDATE adults SET name = 'changed';
DELETE adults;
RELATE person:ada->adults->person:bob;

-- The view still contains only the computed row.
SELECT * FROM adults;
