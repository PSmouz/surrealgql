/**
# `records-100000` with a changefeed enabled on the `record` table. The `on` arm
# of the changefeed update matrix — paired with the base `records-100000` (`off`)
# and `records-100000-changefeed-original` (INCLUDE ORIGINAL). `run = false`.

[env]
imports = ["./records-100000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	ALTER TABLE record CHANGEFEED 1h;
	RETURN 'OK';
}
