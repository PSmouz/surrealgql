/**
[env]
imports = ["bench/util/changefeed-populated.surql"]

[bench]
reason = "Read a populated changefeed via SHOW CHANGES — measures the cf scan + decode + into_value path, including INCLUDE ORIGINAL pre-images"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SHOW CHANGES FOR TABLE record SINCE 0 LIMIT 1000;
