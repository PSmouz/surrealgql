/**
# Permissioned variant of `records-100000` with a row-level `WHERE` predicate on
# the `record` table. Imports the 100k-row base, defines a RECORD access method +
# the authenticated record the bench session runs as (`user:bench`), then ALTERs
# the `record` table so select/update/delete each carry a per-record predicate.
# The predicate (`age < 200`) is satisfied by every row (`age` is 1..100), so the
# permissioned and unpermissioned (`full`) arms return the SAME rows — the only
# difference being the per-record predicate evaluation we want to measure. Paired
# with `records-100000-perms-full` in the permissions matrix. `run = false`.

[env]
imports = ["./records-100000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE user SCHEMALESS;
	CREATE user:bench SET name = 'bench';

	DEFINE ACCESS user ON DATABASE TYPE RECORD
		SIGNIN ( SELECT * FROM type::record('user', $id) )
		DURATION FOR TOKEN 1h, FOR SESSION 1h;

	-- Row-level predicate evaluated against every record on each select/update/
	-- delete. Always true (every `age` is < 200) so the result set matches the
	-- `full` variant; the cost difference is the predicate evaluation itself.
	ALTER TABLE record PERMISSIONS
		FOR select, update, delete WHERE age < 200
		FOR create FULL;

	RETURN 'OK';
}
