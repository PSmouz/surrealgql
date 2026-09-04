/**
[bench]
reason = "One-hop graph traversal from 1000 nodes"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT ->knows->person AS friends FROM person LIMIT 1000;
