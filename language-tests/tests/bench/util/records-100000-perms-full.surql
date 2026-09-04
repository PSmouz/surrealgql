/**
# Permissioned variant of `records-100000` with table permissions set to FULL.
# Imports the 100k-row base, defines a RECORD access method + the authenticated
# record the bench session runs as (`user:bench`), then ALTERs the `record` table
# so every action is permitted with NO per-row predicate. Paired with
# `records-100000-perms-where` as the baseline arm of the permissions matrix: both
# run as the same record user (so the per-record permission machinery is engaged),
# isolating the cost of evaluating a row-level `WHERE` predicate (the `where`
# variant) from the permission machinery itself (this `full` variant). Owners
# bypass the per-row check entirely — that path is already covered by `scans/` and
# `mutate/`. `run = false`.

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

	-- Baseline: all actions allowed, no per-row predicate to evaluate.
	ALTER TABLE record PERMISSIONS FOR select, create, update, delete FULL;

	RETURN 'OK';
}
