/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Group all records by city with a count"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT city, count() FROM record GROUP BY city;
