/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Select all fields from 100 records at offset 5000"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record START 5000 LIMIT 100;
