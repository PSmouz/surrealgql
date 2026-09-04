/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DELETE skips the per-record reference-cleanup scan for tables that no DEFINE FIELD REFERENCE can target, while still applying ON DELETE strategies for tables that are targeted (purge_references catalog gate)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:one }]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

[[test.results]]
value = "[{ id: widget:one, name: 'gadget' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ author: person:one, id: comment:one }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE CASCADE;

CREATE person:one;
CREATE comment:one SET author = person:one;
CREATE widget:one SET name = "gadget";

-- `widget` is targeted by no REFERENCE field, so deleting it takes the skip
-- path. It must succeed and leave every other record untouched.
DELETE widget:one;
SELECT * FROM comment;

-- `person` IS targeted by `comment.author` with ON DELETE CASCADE, so deleting
-- it must still cascade and remove the referencing comment.
DELETE person:one;
SELECT * FROM comment;
SELECT * FROM widget;
