/**
[env]
imports = ["bench/util/references-5000.surql"]

[bench]
reason = "FETCH a single record-link field for 1000 comments"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT * FROM comment LIMIT 1000 FETCH author;
