/**
[bench]
reason = "Shortest path (<=8 hops) between two nodes"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql", large = "bench/util/graph-sparse-50000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

person:1.{..8+shortest=person:5000}->knows->person;
