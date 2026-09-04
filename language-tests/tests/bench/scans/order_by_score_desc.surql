/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Full sort of all records by score, top 100"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record ORDER BY score DESC LIMIT 100;
