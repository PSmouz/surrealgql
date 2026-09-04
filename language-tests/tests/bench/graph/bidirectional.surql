/**
[bench]
reason = "Bidirectional one-hop traversal for 1000 nodes"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT <->knows<->person AS network FROM person LIMIT 1000;
