/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Streaming TopK (SortTopKByKey) must break ties stably: rows with equal sort keys are returned in scan (insertion) order, including when LIMIT cuts inside a tie group. Tie order is not pinned across planner strategies (the legacy compute path differs), so this locks the streaming path only."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ grp: 1, id: item:2, sub: 20 }, { grp: 1, id: item:4, sub: 40 }, { grp: 1, id: item:6, sub: 60 }]"

[[test.results]]
value = "[{ grp: 2, id: item:1, sub: 10 }, { grp: 2, id: item:3, sub: 30 }, { grp: 2, id: item:5, sub: 50 }]"

[[test.results]]
value = "[{ grp: 1, id: item:2, sub: 20 }, { grp: 1, id: item:4, sub: 40 }]"

[[test.results]]
value = "[{ grp: 2, id: item:1, sub: 10 }, { grp: 2, id: item:3, sub: 30 }, { grp: 2, id: item:5, sub: 50 }, { grp: 1, id: item:2, sub: 20 }]"

[[test.results]]
value = "[{ grp: 1, id: item:6, sub: 60 }, { grp: 1, id: item:4, sub: 40 }, { grp: 1, id: item:2, sub: 20 }]"

[[test.results]]
value = "[{ grp: 1, id: item:4, sub: 40 }, { grp: 1, id: item:6, sub: 60 }]"
*/

-- Setup: duplicate sort keys across interleaved ids
{
	CREATE item:1 SET grp = 2, sub = 10;
	CREATE item:2 SET grp = 1, sub = 20;
	CREATE item:3 SET grp = 2, sub = 30;
	CREATE item:4 SET grp = 1, sub = 40;
	CREATE item:5 SET grp = 2, sub = 50;
	CREATE item:6 SET grp = 1, sub = 60;
	RETURN "OK";
};

-- Ties under ASC: all grp=1 rows tie; stable order is id order
SELECT * FROM item ORDER BY grp ASC LIMIT 3;

-- Ties under DESC: all grp=2 rows tie; stable order is id order
SELECT * FROM item ORDER BY grp DESC LIMIT 3;

-- LIMIT cuts inside a tie group: earlier rows win the contested slots
SELECT * FROM item ORDER BY grp ASC LIMIT 2;
SELECT * FROM item ORDER BY grp DESC LIMIT 4;

-- Multi-key: ties on first key resolved by second key, not insertion order
SELECT * FROM item ORDER BY grp ASC, sub DESC LIMIT 3;

-- START offset within a tie group keeps the same stable sequence
SELECT * FROM item ORDER BY grp ASC START 1 LIMIT 2;
