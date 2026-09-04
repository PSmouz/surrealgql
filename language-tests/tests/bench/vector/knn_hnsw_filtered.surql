/**
[env]
imports = ["bench/util/embeddings-hnsw-filtered-5000.surql"]

[bench]
reason = "Top-10 nearest neighbours via the HNSW index with a selective residual filter (category = 7, 2% of rows). The filter is pushed into the HNSW search, so the traversal visits many candidates to find 10 matching rows and fetches each candidate's record to evaluate the condition; a high EF (400) maximises that candidate count, exercising the per-neighbourhood record-fetch batching. The dataset and the query vector are deterministic, so running both sides of a before/after with SURREAL_HNSW_BUILD_SEED set builds an identical graph and isolates the code change. Compare against knn_hnsw.surql (unfiltered, unchanged) as the noise floor."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|10,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND category = 7 ORDER BY dist;
