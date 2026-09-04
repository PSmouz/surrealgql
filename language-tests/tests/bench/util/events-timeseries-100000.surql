/**
# Time-series dataset: 100k `event` rows whose `time` is spread uniformly across a
# single calendar year (2024). Unlike `records-100000.created_at` — an unbounded
# `rand::time()` spanning ~260k years, which would bucket into ~one row per group —
# these timestamps live in a bounded window so `GROUP BY time::group(time, ...)`
# produces meaningful buckets (≈366 days / ≈8784 hours) with thousands of rows
# each. Each row also carries a numeric `value` (for per-bucket stats) and a
# `sensor` / `region` category. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE event SCHEMALESS;

	CREATE |event:100000|
		SET time = rand::time(d'2024-01-01T00:00:00Z', d'2024-12-31T23:59:59Z'),
			sensor = rand::enum('s01', 's02', 's03', 's04', 's05', 's06', 's07', 's08'),
			region = rand::enum('us-east', 'us-west', 'eu-west', 'eu-central', 'ap-south'),
			value = rand::float(0.0, 1000.0)
		RETURN NONE;

	RETURN 'OK';
}
