/**
# Empty `record` table WITH a changefeed enabled — the `on` arm of the
# changefeed write-amplification matrix, paired with `changefeed-off`. Each
# captured write adds one changefeed KV entry per modified table per
# transaction. `run = false`.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE record SCHEMALESS CHANGEFEED 1h;
	RETURN 'OK';
}
