/**
[bench]
reason = "Deeper bounded recursive traversal (up to 6 hops) from one node — the existing `recursive`/`path_collect` benches cap at 3 hops, well short of where BFS frontier growth (especially on the dense graph) starts to dominate"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

person:1.{..6+collect}->knows->person;
