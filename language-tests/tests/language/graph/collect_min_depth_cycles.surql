/**
[test]
reason = "Test +collect with min depth > 1 in cyclic graphs: a node visited below the minimum depth must still be collected (and traversed through) when re-reached at a depth within the [min, max] range"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:1, reach: [person:1, person:3] }, { id: person:2, reach: [person:2, person:1] }, { id: person:3, reach: [person:2] }]"

[[test.results]]
value = "[person:1, person:3]"

[[test.results]]
value = "[person:2, person:1, person:3]"

[[test.results]]
value = "[person:2, person:1, person:3]"

[[test.results]]
value = "[person:1, person:3, person:2]"
*/

-- Cyclic graph: 1 -> 2, 2 -> 1, 2 -> 3, 3 -> 1
CREATE person:1, person:2, person:3 RETURN NONE;
INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2 },
	{ id: knows:k21, in: person:2, out: person:1 },
	{ id: knows:k23, in: person:2, out: person:3 },
	{ id: knows:k31, in: person:3, out: person:1 }
] RETURN NONE;

-- 2: Exactly depth 2. person:2 reaches person:1 only via 2->3->1, which
-- revisits a node (person:1) already seen at depth 1 via 2->1->...
SELECT id, id.{2+collect}(->knows->person) AS reach FROM person ORDER BY id;

-- 3: Exactly depth 3. person:1 is visited at depth 1 (below min) and must
-- still be traversed through at depth 1 to reach depth-3 nodes via the cycle.
person:3.{3+collect}->knows->person;

-- 4: Depth range with min > 1
person:2.{2..3+collect}->knows->person;

-- 5: Inclusive start with min > 1: person:2 is collected up-front but must
-- still be traversed through when re-reached at depth 2 via the cycle.
person:2.{2..3+collect+inclusive}->knows->person;

-- 6: min depth 1 is unaffected (sanity check)
person:2.{1..2+collect}->knows->person;
