/**
[env]
imports = ["bench/util/geo-points-100000.surql"]

[bench]
reason = "Geo containment full scan: point-in-polygon (INSIDE) test evaluated per row against a fixed bounding polygon (no spatial index — see #219)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM place WHERE location INSIDE {
	type: 'Polygon',
	coordinates: [[[-10.0, 35.0], [40.0, 35.0], [40.0, 70.0], [-10.0, 70.0], [-10.0, 35.0]]]
};
