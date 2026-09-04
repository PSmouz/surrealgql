/**
# 100k records identical in shape to records-100000, except `created_at` is
# MONOTONIC with insertion order (sequential numeric ids record:1..100000,
# created_at = epoch + id minutes). Models production tables where creation
# timestamps correlate with record ids: a forward scan sees ascending
# created_at, which is the worst case for `ORDER BY created_at DESC LIMIT n`
# TopK threshold pushdown (every row beats the heap-worst, so the threshold
# never rejects). Pairs with the `unindexed` (random created_at, near best
# case) dataset to keep the bench honest across both regimes.
# `run = false` keeps it out of the regular suite.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMALESS;

	CREATE |record:1..100000|
		SET text = rand::string(50),
			name = rand::string(20),
			age = rand::int(1, 100),
			city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York', 'Sydney', 'Madrid', 'Rome', 'Beijing'),
			score = rand::float(0.0, 100.0),
			number = rand::int(1, 5000),
			integer = rand::int(),
			active = rand::bool(),
			http_status = rand::enum(200, 201, 400, 404, 500),
			request_id = rand::uuid(),
			status = rand::enum('draft', 'published', 'archived'),
			description = rand::string(100, 400),
			tier = rand::enum(0, 1, 2, 3),
			words = array::join(array::map(array::range(0, 24), |$i| rand::enum('hello', 'world', 'foo', 'bar', 'test', 'search', 'data', 'query', 'index', 'document', 'database', 'performance')), ' '),
			tags = [
				rand::enum('alpha', 'beta', 'gamma', 'delta', 'epsilon'),
				rand::enum('theta', 'iota', 'kappa', 'lambda', 'mu'),
				rand::enum('nu', 'xi', 'omicron', 'pi', 'rho')
			],
			geography = {
				code: rand::string(3),
				text: rand::string(15),
				location: rand::enum('Africa', 'Antarctica', 'Asia', 'Europe', 'North America', 'South America', 'Oceania'),
				description: rand::string(80)
			}
		RETURN NONE;

	-- `id` is not yet visible during CREATE's SET phase, so the
	-- insertion-correlated timestamp is assigned in a second bulk pass.
	UPDATE record SET created_at = d'2024-01-01T00:00:00Z' + duration::from_mins(record::id(id)) RETURN NONE;

	RETURN 'OK';
}
