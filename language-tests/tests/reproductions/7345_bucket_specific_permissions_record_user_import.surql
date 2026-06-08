/**
[test]
reason = "Setup for issue 7345: buckets with specific permissions used by a record user reproduction."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( $rid );

CREATE user:bob;

DEFINE BUCKET deny_where BACKEND "memory"
	PERMISSIONS WHERE false;
f"deny_where:/secret.txt".put("DENY-ME");

DEFINE BUCKET file_gate BACKEND "memory"
	PERMISSIONS WHERE $action = "get" AND $file = f"file_gate:/allowed.txt";
f"file_gate:/allowed.txt".put("ALLOWED");
f"file_gate:/denied.txt".put("DENIED");

DEFINE BUCKET target_gate BACKEND "memory"
	PERMISSIONS WHERE $action = "copy"
		AND $file = f"target_gate:/source.txt"
		AND $target = f"target_gate:/allowed-copy.txt";
f"target_gate:/source.txt".put("SOURCE");
