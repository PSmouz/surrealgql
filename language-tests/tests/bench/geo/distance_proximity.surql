/**
[env]
imports = ["bench/util/geo-points-100000.surql"]

[bench]
reason = "Geo proximity full scan: geo::distance from a fixed point evaluated per row, filtered by radius (no spatial index — see #219)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM place WHERE geo::distance(location, (-0.1276, 51.5072)) < 1000000;
