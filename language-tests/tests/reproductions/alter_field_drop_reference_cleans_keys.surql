/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ALTER FIELD ... DROP REFERENCE must purge the reference keys the field wrote. Otherwise the keys are stranded (the DELETE purge gate skips the scan because no current field targets the table), and re-adding the reference would let a later DELETE find the stale key and fire the wrong ON DELETE CASCADE. Here comment:one must survive deleting person:one."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

*/

DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
CREATE person:one;
CREATE comment:one SET author = person:one;

-- Drop the REFERENCE clause. This must purge the reference key written under
-- person:one; the field itself (record<person>) remains.
ALTER FIELD author ON comment DROP REFERENCE;

-- Re-add the reference. Existing records are not re-scanned, so no reference
-- key is recreated for comment:one -> person:one.
DEFINE FIELD OVERWRITE author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;

-- With the stale key purged at DROP time, deleting person:one must NOT cascade
-- into comment:one. (Without the cleanup the stale key would survive and drive
-- a wrong cascade here, deleting comment:one.)
DELETE person:one;
SELECT * FROM comment;
