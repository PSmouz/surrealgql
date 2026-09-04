/**
[bench]
reason = "UPDATE ... WHERE over 10k records with the changefeed off vs on vs INCLUDE ORIGINAL — measures changefeed write-amplification (and the pre-image/reverse-diff cost) on the update path"
run = true
rebuild = true
datasets = { off = "bench/util/records-10000.surql", on = "bench/util/records-10000-changefeed.surql", original = "bench/util/records-10000-changefeed-original.surql" }
warmup = "2s"
sample-size = 10
measurement-time = "20s"

[test]
run = false
*/

UPDATE record SET active = true WHERE number = 21;
