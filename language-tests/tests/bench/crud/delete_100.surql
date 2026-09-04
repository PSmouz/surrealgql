/**
[env]
imports = ["bench/util/records-100.surql"]

[bench]
reason = "Delete a batch of 100 records"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

DELETE record;
