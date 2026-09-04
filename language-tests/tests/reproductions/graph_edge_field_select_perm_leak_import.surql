/**
[test]
reason = "Setup: an edge table (`knows`) and a referencing table (`comment`) whose records are visible at the TABLE level (`PERMISSIONS FOR select FULL`) but each carry a `secret` field gated by a field-level SELECT permission (`NONE`, so always hidden from a record user) and a read-time `COMPUTED` field. A record user materialising these records via a graph traversal (`->(SELECT * FROM knows)`) or a reference traversal (`<~(SELECT * FROM comment)`) must get the same field-permission + computed-field processing as a direct `SELECT *`."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
-- A restricted field on the vertex table, to confirm a traversal that
-- materialises the *target vertex* (`->knows->(SELECT * FROM person)`) also
-- applies field-level permissions through the same code path.
DEFINE FIELD pin ON person TYPE option<string> PERMISSIONS FOR select NONE;

-- Edge table. The edge records themselves are visible (table-level select
-- FULL), but `secret` is hidden by a field-level SELECT permission and
-- `label` is a read-time COMPUTED field. A graph traversal that yields full
-- edge objects must apply BOTH (hide `secret`, compute `label`).
DEFINE TABLE knows SCHEMAFULL TYPE RELATION FROM person TO person
	PERMISSIONS FOR select FULL;
DEFINE FIELD public ON knows TYPE string;
DEFINE FIELD secret ON knows TYPE string PERMISSIONS FOR select NONE;
-- Conditional field permission: visible only to the edge's `in` endpoint.
-- Exercises the Conditional branch of field-permission filtering (with the
-- predicate evaluated against the materialised edge) through the graph path.
DEFINE FIELD owner_only ON knows TYPE string PERMISSIONS FOR select WHERE in = $auth;
DEFINE FIELD label ON knows TYPE string COMPUTED 'lbl_' + public;

-- Referencing table. Same shape: visible record, hidden `secret`, computed
-- `tag`. A reverse-reference traversal that yields full records must apply
-- the same processing.
DEFINE TABLE comment SCHEMAFULL PERMISSIONS FOR select FULL;
DEFINE FIELD author ON comment TYPE record<person> REFERENCE;
DEFINE FIELD body ON comment TYPE string;
DEFINE FIELD secret ON comment TYPE string PERMISSIONS FOR select NONE;
DEFINE FIELD tag ON comment TYPE string COMPUTED 'tag_' + body;

CREATE person:alice SET pin = 'secret_pin';
CREATE person:bob;

RELATE person:bob->knows:e1->person:alice SET public = 'pub', secret = 'classified', owner_only = 'mine';

CREATE comment:c1 SET author = person:bob, body = 'hi', secret = 'classified';
