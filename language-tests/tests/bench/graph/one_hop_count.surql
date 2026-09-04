/**
[bench]
reason = "Count outgoing edges for 1000 nodes"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count(->knows) AS degree FROM person LIMIT 1000;
