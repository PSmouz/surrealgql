/**
# Builds an INCLUDE ORIGINAL changefeed on `record` for the SHOW CHANGES read
# bench. The bulk create, the update sweep, and the delete sweep run as separate
# top-level transactions, so the feed contains Set, SetWithDiff and
# DelWithOriginal mutations across several changesets (versionstamps). Built once
# and only read by the bench (`rebuild = false`). `run = false`.

[test]
run = false
*/

DEFINE TABLE record SCHEMALESS CHANGEFEED 1h INCLUDE ORIGINAL;
CREATE |record:10000| SET n = rand::int(1, 100), text = rand::string(50) RETURN NONE;
UPDATE record SET n = n + 1 RETURN NONE;
DELETE record WHERE n > 50 RETURN NONE;
