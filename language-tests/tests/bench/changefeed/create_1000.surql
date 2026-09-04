/**
[bench]
reason = "Create 1000 records with the changefeed off vs on — measures changefeed write-amplification on the create path"
run = true
rebuild = true
datasets = { off = "bench/util/changefeed-off.surql", on = "bench/util/changefeed-on.surql" }
warmup = "2s"
sample-size = 50
measurement-time = "20s"

[test]
run = false
*/

CREATE |record:1000| SET
	text = rand::string(50),
	name = rand::string(20),
	number = rand::int(1, 5000),
	active = rand::bool(),
	city = rand::enum('London', 'Paris', 'Berlin', 'Tokyo', 'New York'),
	status = rand::enum('draft', 'published', 'archived');
