/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Project an uncorrelated aggregate subquery per row"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT name, (SELECT VALUE count() FROM record GROUP ALL)[0] AS total FROM record LIMIT 100;
