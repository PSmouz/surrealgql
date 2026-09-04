/**
[test]
run = false
*/

-- Security fixture for `perm_hidden_field.gql`: a SCHEMAFULL `person` table that
-- is table-level SELECT-able, but whose `secret` field is gated by a field-level
-- `PERMISSIONS FOR select WHERE id = $auth.id` (a record user sees the secret of
-- ITS OWN record only). `label` is a read-time `COMPUTED` field (always
-- selectable). The point: a `b` binding produced by a MATCH hop is resolved
-- through the same FieldState machinery as SELECT (table perm + computed fields +
-- field perms), so `b.secret` must be NONE for other people's records and
-- `b.label` must be present — byte-identical to `SELECT secret, label FROM person`
-- under the same record session. (Field-level companion to perm_hidden_edge.gql,
-- which covers table-level edge gating.)
DEFINE TABLE person SCHEMAFULL PERMISSIONS FOR select FULL;

DEFINE FIELD name ON person TYPE string PERMISSIONS FULL;
-- Read-time computed field: re-derived on every read (and on every binding-row
-- fetch). Always selectable, so it must surface in the binding regardless of who
-- is asking.
DEFINE FIELD label ON person TYPE string COMPUTED string::uppercase(name) PERMISSIONS FULL;
-- Field-level SELECT permission: a record user may read `secret` only on its own
-- record. On every other person's record the field is cut from the document.
DEFINE FIELD secret ON person TYPE string PERMISSIONS FOR select WHERE id = $auth.id;

-- Edge table is fully selectable: we are testing field perms on the TARGET node
-- `b`, not edge gating (that is perm_hidden_edge.gql's job).
DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS FOR select FULL;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('person', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

CREATE person:1 SET name = 'alice', secret = 's1' RETURN NONE;
CREATE person:2 SET name = 'bob',   secret = 's2' RETURN NONE;
CREATE person:3 SET name = 'carol', secret = 's3' RETURN NONE;

INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2 },
	{ id: knows:k13, in: person:1, out: person:3 },
	{ id: knows:k21, in: person:2, out: person:1 }
] RETURN NONE;
