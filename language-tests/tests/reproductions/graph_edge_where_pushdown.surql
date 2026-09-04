/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Graph traversal WHERE conditions are pushed into GraphEdgeScan (Tier-1): edge-property equality/range filters, paren vs bracket forms, bidirectional and multi-edge-table filters, and filter+LIMIT early-termination fusion."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:a }, { id: person:b }, { id: person:c }, { id: person:d }, { id: person:e }]"

[[test.results]]
value = "[{ close: true, id: knows:ab, in: person:a, out: person:b, since: 2019 }]"

[[test.results]]
value = "[{ close: false, id: knows:ac, in: person:a, out: person:c, since: 2021 }]"

[[test.results]]
value = "[{ close: true, id: knows:ad, in: person:a, out: person:d, since: 2023 }]"

[[test.results]]
value = "[{ id: follows:ae, in: person:a, out: person:e, weight: 10 }]"

[[test.results]]
value = "[{ close: false, id: knows:ba, in: person:b, out: person:a, since: 2020 }]"

[[test.results]]
value = "[person:b, person:d]"

[[test.results]]
value = "[person:c, person:d]"

[[test.results]]
value = "[person:c, person:d]"

[[test.results]]
value = "[2020, 2019]"

[[test.results]]
value = "[knows:ac, knows:ad, follows:ae]"

[[test.results]]
value = "2"
*/

DEFINE TABLE OVERWRITE person SCHEMALESS;
DEFINE TABLE OVERWRITE knows SCHEMAFULL TYPE RELATION IN person OUT person;
DEFINE FIELD OVERWRITE since ON knows TYPE int;
DEFINE FIELD OVERWRITE close ON knows TYPE bool DEFAULT false;
DEFINE TABLE OVERWRITE follows SCHEMAFULL TYPE RELATION IN person OUT person;
DEFINE FIELD OVERWRITE weight ON follows TYPE int;

CREATE person:a, person:b, person:c, person:d, person:e;
-- Explicit edge ids keep results deterministic across runs.
RELATE person:a->knows:ab->person:b SET since = 2019, close = true;
RELATE person:a->knows:ac->person:c SET since = 2021, close = false;
RELATE person:a->knows:ad->person:d SET since = 2023, close = true;
RELATE person:a->follows:ae->person:e SET weight = 10;
RELATE person:b->knows:ba->person:a SET since = 2020, close = false;

-- edge-property equality filter (record-referencing pushdown) -> b, d
SELECT VALUE ->knows[WHERE close = true]->person FROM ONLY person:a;

-- edge-property range filter -> c, d
SELECT VALUE ->knows[WHERE since > 2020]->person FROM ONLY person:a;

-- paren form, filter then target -> c, d
SELECT VALUE ->(knows WHERE since >= 2021)->person FROM ONLY person:a;

-- bidirectional filter over edges, since<=2020: in b->a(2020), out a->b(2019)
SELECT VALUE <->knows[WHERE since <= 2020].since FROM ONLY person:a;

-- multi-edge-table filter across knows + follows -> knows:ac, knows:ad, follows:ae
SELECT VALUE ->(knows, follows WHERE since > 2020 OR weight > 5) FROM ONLY person:a;

-- filter + LIMIT early-termination fusion (2 of the 3 matching knows edges)
SELECT VALUE array::len(->(knows WHERE since > 2000 LIMIT 2)->person) FROM ONLY person:a;
