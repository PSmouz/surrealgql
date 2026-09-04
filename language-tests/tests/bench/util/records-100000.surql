/**
[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMALESS;

	CREATE |record:100000|
		SET text = rand::string(50),
			name = rand::string(20),
			age = rand::int(1, 100),
			city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York', 'Sydney', 'Madrid', 'Rome', 'Beijing'),
			score = rand::float(0.0, 100.0),
			number = rand::int(1, 5000),
			integer = rand::int(),
			active = rand::bool(),
			created_at = rand::time(),
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
				description: rand::string(80),
				region: {
					name: rand::string(10),
					details: {
						continent: rand::enum('Africa', 'Antarctica', 'Asia', 'Europe', 'North America', 'South America', 'Oceania'),
						timezone: rand::enum('UTC', 'GMT', 'PST', 'EST', 'CET', 'JST')
					}
				}
			}
		RETURN NONE;

	RETURN 'OK';
}
