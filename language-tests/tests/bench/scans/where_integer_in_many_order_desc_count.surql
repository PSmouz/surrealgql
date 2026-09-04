/**
[bench]
reason = "Count records matching a status-code IN list"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-10000.surql", indexed = "bench/util/records-10000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE http_status IN [200, 201] GROUP ALL;
