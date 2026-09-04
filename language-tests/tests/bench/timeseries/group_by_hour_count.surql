/**
[env]
imports = ["bench/util/events-timeseries-100000.surql"]

[bench]
reason = "Temporal bucketing at finer granularity: count events per hour via time::group (≈8784 buckets over 100k rows)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT time::group(time, "hour") AS hour, count() FROM event GROUP BY hour;
