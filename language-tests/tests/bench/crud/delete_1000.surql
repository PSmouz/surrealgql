/**
[env]
imports = ["bench/util/records-1000.surql"]

[bench]
reason = "Delete a batch of 1000 records"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

DELETE record;
