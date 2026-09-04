/**
# Empty `record` table with NO changefeed — the baseline (`off`) arm of the
# changefeed write-amplification matrix, paired with `changefeed-on`. Isolates
# the cost of capturing changes on the write path. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMALESS;
	RETURN 'OK';
}
