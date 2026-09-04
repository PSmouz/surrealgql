/**
[env]
imports = ["bench/util/embeddings-diskann-filtered-5000.surql"]

[bench]
reason = "Top-10 nearest neighbours via the DiskANN index with a SELECTIVE residual filter (category = 7, 2% of rows). The filter is pushed into the DiskANN search, so the traversal visits many candidates to find 10 matching rows and fetches each candidate's record to evaluate the condition. This is the primary target of the record-fetch batching and the must-not-regress case for the windowed prefetch: the result builder rarely fills, so ~all candidates are needed and the prefetch stays one (widened) batch. The dataset and query vector are deterministic and DiskANN graph construction has no RNG, so a before/after isolates the code change. Compare against knn_diskann.surql (unfiltered, unchanged) as the noise floor."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND category = 7 ORDER BY dist;
