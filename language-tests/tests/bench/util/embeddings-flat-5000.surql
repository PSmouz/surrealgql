/**
# 5000 8-dimensional random embeddings with NO index, for brute-force
# nearest-neighbour benches (full-scan + distance sort). `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE embeddings SCHEMALESS;
	CREATE |embeddings:5000| SET vec = array::map(array::range(0, 8), |$i| rand::float(0.0, 1.0)) RETURN NONE;
	RETURN 'OK';
}
