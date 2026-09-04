/**
# GQL bench dataset for OPTIONAL MATCH null-fill cost: 5000 `person` nodes,
# of which exactly 10% (every 10th id) have a single outgoing `bought` edge to a
# `item`; the other 90% have none. So an OPTIONAL MATCH against `bought`
# misses for ~90% of the driving rows, each of which the LeftJoin must preserve
# and null-fill — isolating that null-fill path.
#
# 500 item nodes and 500 `bought` edges, all via RELATE so the adjacency keys
# exist. Deterministic by construction (no rand), so re-imports are
# byte-identical. `run = false` — imported only by benches.

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE TABLE item SCHEMALESS;

	CREATE |person:1..5000| SET name = 'p' RETURN NONE;
	CREATE |item:1..500| SET name = 'q' RETURN NONE;

	-- Every 10th person (10, 20, ... 5000) buys one item; the rest buy none,
	-- giving a ~90% OPTIONAL MATCH miss rate.
	FOR $k IN array::range(1, 500) {
		LET $person = type::record('person', $k * 10);
		LET $item = type::record('item', $k);
		RELATE $person->bought->$item;
	};

	RETURN 'OK';
}
