/**
[env]
imports = ["bench/util/references-5000.surql"]

[bench]
reason = "FETCH an array of record-link fields for 1000 users"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM user LIMIT 1000 FETCH friends;
