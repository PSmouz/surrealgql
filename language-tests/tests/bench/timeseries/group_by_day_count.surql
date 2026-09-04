/**
[env]
imports = ["bench/util/events-timeseries-100000.surql"]

[bench]
reason = "Temporal bucketing: count events per day via time::group (≈366 buckets over 100k rows)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT time::group(time, "day") AS day, count() FROM event GROUP BY day;
