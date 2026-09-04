/**
[bench]
reason = "Bounded recursive traversal (up to 3 hops) from one node"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

person:1.{..3}->knows->person;
