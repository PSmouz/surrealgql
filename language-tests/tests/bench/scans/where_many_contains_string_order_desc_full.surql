/**
[bench]
reason = "Select up to 1000 records whose tag array contains a value, ordered by datetime descending"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-10000.surql", indexed = "bench/util/records-10000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record WHERE tags CONTAINS 'omicron' ORDER BY created_at DESC LIMIT 1000;
