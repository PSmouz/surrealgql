/**
# `records-100000` with a changefeed enabled in INCLUDE ORIGINAL mode (stores the
# before-image / reverse diff on every update and delete). The heaviest arm of
# the changefeed update matrix — isolates the cost of computing and storing the
# pre-image on top of plain changefeed capture. `run = false`.

[env]
imports = ["./records-100000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	ALTER TABLE record CHANGEFEED 1h INCLUDE ORIGINAL;
	RETURN 'OK';
}
