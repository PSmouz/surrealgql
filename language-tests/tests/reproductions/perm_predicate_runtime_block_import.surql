/**
[test]
reason = "Setup for perm_predicate_runtime_block: a function whose body performs a write, referenced from a table's SELECT/UPDATE permission predicate. The DEFINE is accepted because the function call is opaque to the definition-time check, so the runtime guard must block the write when a record user triggers the predicate (GHSA-66r2-5gwj-gxm2)."
run = false
*/

-- `audit` is readable so the test can assert that no rows were written; the
-- permission predicate must not be permitted to write to it.
DEFINE TABLE audit SCHEMALESS PERMISSIONS FOR select FULL;

-- A read-only-looking predicate: it calls a function whose body performs a
-- write. The DEFINE is accepted (the call is opaque to `has_direct_write`); the
-- write must be blocked at runtime by the permission-predicate guard.
DEFINE FUNCTION fn::side_effect() {
	CREATE audit SET marker = true;
	RETURN true;
} PERMISSIONS FULL;

DEFINE TABLE victim SCHEMALESS
	PERMISSIONS FOR select, update WHERE fn::side_effect();

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:test;
CREATE victim:1 SET visible = true;
