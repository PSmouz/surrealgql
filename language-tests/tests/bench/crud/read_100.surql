/**
[env]
imports = ["bench/util/records-100.surql"]

[bench]
reason = "Select a batch of 100 records"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record;
