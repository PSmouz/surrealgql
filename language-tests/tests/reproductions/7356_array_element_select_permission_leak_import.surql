/**
[test]

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:guest }]"

[[test.results]]
value = "[{ id: doc:1, items: [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }] }]"

[[test.results]]
value = "[{ id: docp:1, items: [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }] }]"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM type::record('person', $id) )
    DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;

-- Table A: element-level SELECT permission that always denies. Every
-- element of `items` must be hidden, leaving an empty array.
DEFINE TABLE doc SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD items ON doc TYPE array<object>;
DEFINE FIELD OVERWRITE items[*] ON doc TYPE object PERMISSIONS FOR select WHERE false;
DEFINE FIELD items[*].n ON doc TYPE int;

-- Table B: a genuine per-element predicate. Only even `n` survive, by
-- element identity — NOT by (post-removal, shifted) index.
DEFINE TABLE docp SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD items ON docp TYPE array<object>;
DEFINE FIELD OVERWRITE items[*] ON docp TYPE object PERMISSIONS FOR select WHERE $value.n % 2 == 0;
DEFINE FIELD items[*].n ON docp TYPE int;

CREATE person:guest;
CREATE doc:1 SET items = [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }];
CREATE docp:1 SET items = [{ n: 0 }, { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 }, { n: 5 }];
