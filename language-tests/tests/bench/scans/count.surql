/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Count all records"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count() FROM record GROUP ALL;
