/**
[bench]
reason = "Count records matching an integer IN list"
run = true
rebuild = false
datasets = { unindexed = "bench/util/records-10000.surql", indexed = "bench/util/records-10000-indexed.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record WHERE number IN [18, 21, 30, 40, 50, 60] GROUP ALL;
