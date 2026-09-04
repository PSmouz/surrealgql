/**
[bench]
reason = "One-hop traversal feeding an aggregate function (math::mean) over the neighbour set, rather than just counting or projecting it"
run = true
rebuild = false
datasets = { sparse = "bench/util/graph-sparse-5000.surql", dense = "bench/util/graph-dense-5000.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count(->knows) AS degree, math::mean(->knows->person.age) AS avg_friend_age FROM person LIMIT 1000;
