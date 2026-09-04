/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "REMOVE FIELD purges the reference keys a REFERENCE field wrote. After re-defining the same field, a previously-referenced record can be deleted without acting on a stale reference. Without the cleanup the orphaned key would survive REMOVE FIELD and, once the field is re-defined, wrongly trigger its ON DELETE CASCADE."

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

-- Remove the reference field: the reference key it wrote under person:one is
-- purged as part of REMOVE FIELD.
REMOVE FIELD author ON comment;

-- Re-define the same reference field. No referrer has been (re-)created since
-- the removal, so person:one must have no incoming references: deleting it must
-- NOT cascade to comment:one. Without the cleanup, the stale key would still
-- resolve and wrongly delete comment:one.
DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;
DELETE person:one;
SELECT * FROM comment;
