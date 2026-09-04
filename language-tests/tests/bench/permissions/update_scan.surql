/**
[env]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bench" }

[bench]
reason = "UPDATE half the table as a record user — measures per-record table UPDATE permission cost (row-level WHERE predicate vs. FULL)"
run = true
rebuild = true
datasets = { full = "bench/util/records-10000-perms-full.surql", where = "bench/util/records-10000-perms-where.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true WHERE age <= 50;
