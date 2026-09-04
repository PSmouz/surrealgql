/**
[env]
namespace = true
database = true

[test]
reason = "Pins the engine behaviors the GQL lowering design relies on: lookup-with-projection row shape, SPLIT unnest + empty-array guard, $parent scope in lookup conditions, dotted-name aliases in ORDER/GROUP BY, recursion collect, and null-comparison guard semantics. If any of these change, the gql lowering must be revisited."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ __a: { age: 30, id: person:1, name: 'A' }, __m: [{ id: knows:k12, in: person:1, out: person:2, since: 2021 }], id: person:1 }, { __a: { age: 20, id: person:2, name: 'B' }, __m: [{ id: knows:k21, in: person:2, out: person:1, since: 2018 }], id: person:2 }, { __a: { id: person:3, name: 'C' }, __m: [], id: person:3 }]"

[[test.results]]
value = "[{ __a: { age: 30, id: person:1, name: 'A' }, __m: { id: knows:k12, in: person:1, out: person:2, since: 2021 }, id: person:1 }, { __a: { age: 20, id: person:2, name: 'B' }, __m: { id: knows:k21, in: person:2, out: person:1, since: 2018 }, id: person:2 }]"

[[test.results]]
value = """[{ b: { age: 30, id: person:1, name: 'A' }, "b.name": 'A' }, { b: { age: 20, id: person:2, name: 'B' }, "b.name": 'B' }]"""

[[test.results]]
value = "[{ __m: [{ id: knows:k12, in: person:1, out: person:2, since: 2021 }, { id: knows:k1c, in: person:1, out: city:1, since: 2019 }], id: person:1 }, { __m: [], id: person:2 }, { __m: [], id: person:3 }]"

[[test.results]]
value = """[{ "a.name": 'A' }, { "a.name": 'B' }]"""

[[test.results]]
value = """[{ "b.name": 'A' }, { "b.name": 'B' }]"""

[[test.results]]
value = "[{ id: person:1, reach: [person:2, person:1] }, { id: person:2, reach: [person:1, person:2] }, { id: person:3, reach: [] }]"

[[test.results]]
value = "[{ id: person:2 }]"

[[test.results]]
value = "[{ id: person:2 }, { id: person:3 }]"

[[test.results]]
value = "[true, true, false, true]"
*/

-- Seed: people (one without age), a non-person edge target, deterministic edge ids
CREATE person:1 SET name = 'A', age = 30 RETURN NONE;
CREATE person:2 SET name = 'B', age = 20 RETURN NONE;
CREATE person:3 SET name = 'C' RETURN NONE;
CREATE city:1 SET name = 'London' RETURN NONE;
INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2, since: 2021 },
	{ id: knows:k1c, in: person:1, out: city:1, since: 2019 },
	{ id: knows:k21, in: person:2, out: person:1, since: 2018 }
] RETURN NONE;

-- E1: L1 bind — lookup with inline projection must yield FULL EDGE OBJECTS,
-- with non-person targets filtered out by the record::tb(out) condition
SELECT id, $this AS __a, ->(SELECT * FROM knows WHERE record::tb(out) = 'person') AS __m FROM person ORDER BY id;

-- E2: L2 unnest — anchors with no qualifying edges must vanish (guard),
-- and each surviving row carries exactly one edge object in __m
SELECT * FROM (SELECT id, $this AS __a, ->(SELECT * FROM knows WHERE record::tb(out) = 'person') AS __m FROM person) WHERE __m != [] SPLIT __m ORDER BY id;

-- E3: far-end derivation from the edge via out (RecordId auto-fetch)
SELECT __m.out.name AS `b.name`, __m.out.* AS b FROM (SELECT * FROM (SELECT id, $this AS __a, ->(SELECT * FROM knows WHERE record::tb(out) = 'person') AS __m FROM person) WHERE __m != [] SPLIT __m) ORDER BY `b.name`;

-- E4: $parent in a lookup condition (cross-variable predicate pushed to edge scope);
-- also pins the unguarded NONE-comparison hazard (city:1 has no age)
SELECT id, ->(SELECT * FROM knows WHERE out.age < $parent.age) AS __m FROM person ORDER BY id;

-- E5: ORDER BY a dotted-name alias (single escaped identifier containing a dot)
SELECT __a.name AS `a.name` FROM (SELECT * FROM (SELECT $this AS __a, ->(SELECT * FROM knows WHERE record::tb(out) = 'person') AS __m FROM person) WHERE __m != [] SPLIT __m) ORDER BY `a.name`;

-- E6: DISTINCT via GROUP BY on a dotted-name alias
SELECT __m.out.name AS `b.name` FROM (SELECT * FROM (SELECT $this AS __a, ->(SELECT * FROM knows WHERE record::tb(out) = 'person') AS __m FROM person) WHERE __m != [] SPLIT __m) GROUP BY `b.name` ORDER BY `b.name`;

-- E7: variable-length reach via recursion collect (depths 1..3, dedup expected)
SELECT id, id.{1..3+collect}(->knows->person) AS reach FROM person ORDER BY id;

-- E8a: guarded comparison excludes records with a missing field
SELECT id FROM person WHERE age != NONE AND age != NULL AND age < 25 ORDER BY id;

-- E8b: pin the hazard — UNGUARDED comparison includes records with a missing field
SELECT id FROM person WHERE age < 25 ORDER BY id;

-- E8c: equality/order pins for NULL and NONE under SurrealQL's total order
RETURN [NULL = NULL, NONE = NONE, NULL = NONE, NONE < 1];
