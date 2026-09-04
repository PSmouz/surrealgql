/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "TopK threshold pushdown with field-level SELECT permissions on the ORDER BY field: plan time cannot know the session's permission level, so EXPLAIN shows `deferred (runtime field state)`; the execute-time re-check arms the probe only when permission checks are bypassed (owner), and owner results reflect the raw field values."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, pub: 1, secret: 3 }]"

[[test.results]]
value = "[{ id: p:2, pub: 2, secret: 1 }]"

[[test.results]]
value = "[{ id: p:3, pub: 3, secret: 2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Limit [ctx: Db] [limit: 2]\n        SortTopKByKey [ctx: Db] [sort_keys: secret DESC, limit: 2]\n            TableScan [ctx: Db] [table: p, direction: Forward, topk_pushdown: deferred (runtime field state)]\n''''

[[test.results]]
value = "[p:1, p:3]"

[[test.results]]
value = "[p:2, p:3]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    Limit [ctx: Db] [limit: 2]\n        SortTopKByKey [ctx: Db] [sort_keys: pub DESC, limit: 2]\n            TableScan [ctx: Db] [table: p, direction: Forward, topk_pushdown: yes]\n''''
*/

DEFINE TABLE p SCHEMALESS;
DEFINE FIELD secret ON p TYPE int PERMISSIONS FOR select WHERE false;
CREATE p:1 SET secret = 3, pub = 1;
CREATE p:2 SET secret = 1, pub = 2;
CREATE p:3 SET secret = 2, pub = 3;

-- Non-Allow SELECT permission on the ORDER BY field defers the field check
-- to execute time, where the session's permission level is known.
EXPLAIN SELECT * FROM p ORDER BY secret DESC LIMIT 2;

-- Owner session bypasses permission checks, so the probe arms and the order
-- reflects the stored values.
SELECT VALUE id FROM p ORDER BY secret DESC LIMIT 2;
SELECT VALUE id FROM p ORDER BY secret ASC LIMIT 2;

-- An unrestricted field on the same table stays fully eligible.
EXPLAIN SELECT * FROM p ORDER BY pub DESC LIMIT 2;
