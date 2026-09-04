/**
[test]
run = false
*/

-- Security fixture for `perm_hidden_edge.gql`: a graph where the node table is
-- SELECT-able but the edge table is NOT. A `MATCH (a)-[:knows]->(b)` binds the
-- `knows` edge id-only (anonymous edge), so the edge SELECT permission must
-- still gate whether `b` can surface — otherwise a user denied SELECT on
-- `knows` could traverse it to enumerate otherwise-hidden relationships.
DEFINE TABLE person SCHEMALESS PERMISSIONS FOR select FULL;

-- Edge table the record user may NOT select: traversal must be blocked.
DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS NONE;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:1 SET name = 'A' RETURN NONE;
CREATE person:2 SET name = 'B' RETURN NONE;
CREATE person:3 SET name = 'C' RETURN NONE;

-- New-format relation edges (the leak was on the new-format adjacency-key path,
-- where the target is decoded straight from the key and the edge record is
-- never read).
INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2 },
	{ id: knows:k23, in: person:2, out: person:3 }
] RETURN NONE;
