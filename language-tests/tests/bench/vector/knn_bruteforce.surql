/**
[env]
imports = ["bench/util/embeddings-flat-5000.surql"]

[bench]
reason = "Top-10 nearest neighbours by brute-force distance scan"
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

LET $q = array::map(array::range(0, 8), |$i| rand::float(0.0, 1.0));
SELECT id, vector::distance::euclidean(vec, $q) AS dist FROM embeddings ORDER BY dist LIMIT 10;
