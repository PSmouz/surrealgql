/**
# Geospatial dataset: 100k `place` rows each carrying a `location` point spread
# uniformly across the whole globe (lng -180..180, lat -90..90). Used by the geo
# benches to time `geo::distance` proximity / nearest-neighbour queries and
# polygon containment as FULL SCANS — spatial indexing does not exist yet (see
# issue #219), so every row's geometry function is evaluated. This is the baseline
# that becomes the before/after harness once spatial indexing lands. Points are
# built with `type::point([lng, lat])` (GeoJSON axis order). `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE place SCHEMALESS;

	CREATE |place:100000|
		SET location = type::point([rand::float(-180.0, 180.0), rand::float(-90.0, 90.0)]),
			category = rand::enum('cafe', 'shop', 'park', 'office', 'home', 'school', 'hospital')
		RETURN NONE;

	RETURN 'OK';
}
