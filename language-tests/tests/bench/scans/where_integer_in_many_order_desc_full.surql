/**
[bench]
reason = "Select up to 1000 records matching a status-code IN list, ordered by datetime descending"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-10000.surql", indexed = "bench/util/records-10000-indexed.surql", correlated = "bench/util/records-10000-created-seq.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record WHERE http_status IN [200, 201] ORDER BY created_at DESC LIMIT 1000;
