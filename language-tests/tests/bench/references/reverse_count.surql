/**
[env]
imports = ["bench/util/references-5000.surql"]

[bench]
reason = "Count reverse references (back-links) for 1000 users"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count(<~comment) AS refs FROM user LIMIT 1000;
