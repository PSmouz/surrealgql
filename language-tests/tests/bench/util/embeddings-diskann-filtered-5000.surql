/**
# 5000 8-dimensional embeddings with a DiskANN index plus a low-cardinality
# `category` field (0..=49, 2% selectivity each), for *filtered* indexed
# nearest-neighbour benches. A selective residual filter forces the DiskANN
# search to visit many graph candidates to find K matching rows, exercising the
# committed-graph record prefetch (the bench harness compacts pending updates
# before measuring).
#
# Vectors and categories are DETERMINISTIC (derived from the record index, no
# `rand`). DiskANN graph construction is deterministic (no RNG), so the built
# graph is identical across runs and a before/after wall-clock comparison is not
# confounded by graph variance — no build seed is needed (unlike HNSW).
# `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	FOR $i IN array::range(0, 5000) {
		CREATE type::record("embeddings", $i) SET
			vec = array::map(array::range(0, 8), |$j| (($i * 7 + $j * 131) % 1000) / 1000.0),
			category = $i % 50
		RETURN NONE;
	};
	DEFINE INDEX emb_diskann ON embeddings FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;
	RETURN 'OK';
}
