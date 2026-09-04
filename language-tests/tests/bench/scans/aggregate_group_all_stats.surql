/**
[env]
imports = ["bench/util/records-10000.surql"]

[bench]
reason = "Aggregate stats over all records (mean/min/max/sum)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT count(), math::mean(score), math::min(score), math::max(score), math::sum(number) FROM record GROUP ALL;
