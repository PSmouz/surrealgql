/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ALTER FIELD that NARROWS a multi-table record kind (record<person | robot> -> record<person>) must purge only the reference keys for tables that are no longer targeted. Deleting a record of the dropped table (robot) must NOT cascade (its key was purged and the purge gate skips the scan), while deleting a record of a retained table (person) MUST still cascade (its key is preserved)."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
value = "[{ id: robot:one }]"

[[test.results]]
value = "[{ author: person:one, id: comment:p }]"

[[test.results]]
value = "[{ author: robot:one, id: comment:r }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:one, id: comment:p }, { author: robot:one, id: comment:r }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: robot:one, id: comment:r }]"

*/

DEFINE FIELD author ON comment TYPE record<person | robot> REFERENCE ON DELETE CASCADE;
CREATE person:one;
CREATE robot:one;
CREATE comment:p SET author = person:one;
CREATE comment:r SET author = robot:one;

-- Narrow the kind so `robot` is no longer a referenceable target; the key the
-- field wrote under `robot:one` is purged, while `person:one`'s key is kept.
ALTER FIELD author ON comment TYPE record<person>;

-- `robot` is no longer targeted by any REFERENCE field, so deleting it takes
-- the skip path and must NOT cascade — comment:r survives.
DELETE robot:one;
SELECT * FROM comment;

-- `person` is still targeted with ON DELETE CASCADE, so deleting it must still
-- cascade and remove the referencing comment:p.
DELETE person:one;
SELECT * FROM comment;
