/**
# 5000 8-dimensional random embeddings with a DiskANN index, for indexed
# nearest-neighbour benches. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:5000| SET vec = array::map(array::range(0, 8), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	DEFINE INDEX emb_diskann ON embeddings FIELDS vec DISKANN DIMENSION 8 DIST EUCLIDEAN TYPE F32;
	RETURN 'OK';
}
