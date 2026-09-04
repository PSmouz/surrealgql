/**
[test]
reason = "Setup: a record access plus an owner-defined function that calls eval, for the eval subject-escalation guard test."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:bob;

DEFINE FUNCTION fn::run_eval() {
	RETURN eval::surql("RETURN 1");
} PERMISSIONS FULL;
