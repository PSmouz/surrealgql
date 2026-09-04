/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Select all fields from the first 100 records"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record LIMIT 100;
