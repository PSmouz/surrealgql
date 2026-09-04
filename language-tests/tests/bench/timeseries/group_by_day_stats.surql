/**
[env]
imports = ["bench/util/events-timeseries-100000.surql"]

[bench]
reason = "Temporal bucketing with per-bucket aggregates: mean/min/max/count of value per day via time::group"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT
	time::group(time, "day") AS day,
	count(),
	math::mean(value) AS avg,
	math::min(value) AS min,
	math::max(value) AS max
FROM event
GROUP BY day;
