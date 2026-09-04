/**
[env]
imports = ["bench/util/records-10000-fulltext.surql"]

[bench]
reason = "Full-text search for two terms combined with OR (BM25)"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM record WHERE words @@ 'foo' OR words @@ 'bar' LIMIT 1000;
