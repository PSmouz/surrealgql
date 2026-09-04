/**
# 5000 8-dimensional embeddings with an HNSW index plus a low-cardinality
# `category` field (0..=49, 2% selectivity each), for *filtered* indexed
# nearest-neighbour benches. A selective residual filter forces the HNSW search
# to visit many graph candidates to find K matching rows, exercising the
# per-neighbourhood record batching in the committed-graph search path (the
# bench harness compacts pending updates before measuring).
#
# Vectors and categories are DETERMINISTIC (derived from the record index, no
# `rand`). Combined with `SURREAL_HNSW_BUILD_SEED`, the built graph is identical
# across runs, so a before/after wall-clock comparison is not confounded by
# graph variance. `run = false`.

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
	DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 200 M 12;
	RETURN 'OK';
}
