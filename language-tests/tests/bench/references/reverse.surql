/**
[env]
imports = ["bench/util/references-5000.surql"]

[bench]
reason = "Reverse references: comments pointing at each of 1000 users"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT <~comment AS comments FROM user LIMIT 1000;
