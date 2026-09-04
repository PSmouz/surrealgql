/**
[env]
imports = ["bench/util/records-1000.surql"]

[bench]
reason = "Update a batch of 1000 records"
run = true
rebuild = true
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true, score = rand::float(0.0, 100.0);
