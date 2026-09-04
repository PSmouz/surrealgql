/**
# 5000 8-dimensional random embeddings with an HNSW index, for indexed
# nearest-neighbour benches. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:5000| SET vec = array::map(array::range(0, 8), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	DEFINE INDEX emb_hnsw ON embeddings FIELDS vec HNSW DIMENSION 8 DIST EUCLIDEAN TYPE F32 EFC 200 M 12;
	RETURN 'OK';
}
