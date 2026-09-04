/**
[env]
imports = ["bench/util/embeddings-diskann-filtered-5000.surql"]

[bench]
reason = "Top-5 nearest neighbours via the DiskANN index with a NON-selective residual filter (category < 25, ~50% of rows) and small k. The result builder fills almost immediately, so the committed-graph search's up-front record prefetch over-fetches: it warms records for candidates the tightening distance gate then skips. This bench exercises the windowed prefetch that bounds that over-fetch (fewer total record bytes read) while keeping round-trips low. The dataset and query vector are deterministic and DiskANN graph construction has no RNG, so a before/after isolates the code change."
run = true
rebuild = false
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

SELECT id, vector::distance::knn() AS dist FROM embeddings WHERE vec <|5,400|> [0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f, 0.5f] AND category < 25 ORDER BY dist;
