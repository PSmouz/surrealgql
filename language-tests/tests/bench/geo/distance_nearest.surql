/**
[env]
imports = ["bench/util/geo-points-100000.surql"]

[bench]
reason = "Geo nearest-neighbour full scan: geo::distance per row then full sort to the 25 closest (no spatial index — see #219)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, geo::distance(location, (-0.1276, 51.5072)) AS dist FROM place ORDER BY dist ASC LIMIT 25;
