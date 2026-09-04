/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Group all records by status with a count"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT status, count() FROM record GROUP BY status;
