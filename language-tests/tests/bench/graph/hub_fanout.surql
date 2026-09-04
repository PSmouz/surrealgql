/**
[bench]
reason = "One-hop fan-out from a high-degree hub node (250 edges), isolating per-edge traversal cost at real-world-skewed degree instead of the uniform low-degree neighbourhoods in graph-sparse/dense"
run = true
rebuild = false
datasets = { hub = "bench/util/graph-hub-50000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT ->knows->person AS friends, count(->knows) AS degree FROM person:1;
