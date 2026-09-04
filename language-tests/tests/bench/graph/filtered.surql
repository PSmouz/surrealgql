/**
[bench]
reason = "One-hop traversal filtered on the neighbour's age"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT ->knows->(person WHERE age > 30) AS friends FROM person LIMIT 1000;
