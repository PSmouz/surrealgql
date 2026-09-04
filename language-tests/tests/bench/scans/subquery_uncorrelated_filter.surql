/**
[env]
imports = ["bench/util/records-1000.surql"]

[bench]
reason = "Filter records against an uncorrelated aggregate subquery"
run = true
rebuild = false
warmup = "2s"
sample-size = 20
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record WHERE score > (SELECT VALUE math::mean(score) FROM record GROUP ALL)[0] LIMIT 1000;
