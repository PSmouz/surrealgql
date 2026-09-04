/**
[test]
run = false
*/

-- Isolated seed for the self-loop regression tests: `loop` nodes on their own
-- tables (disjoint from person/knows and hub/link) so existing expectations
-- stay byte-identical. ring:1 has a self-edge (self:s1), ring:2 has only an
-- outgoing edge to ring:1 (no self-edge), so a self-loop match returns exactly
-- ring:1.
CREATE ring:1 SET name = 'P' RETURN NONE;
CREATE ring:2 SET name = 'Q' RETURN NONE;
INSERT RELATION INTO selfedge [
	{ id: selfedge:s1, in: ring:1, out: ring:1 },
	{ id: selfedge:e21, in: ring:2, out: ring:1 }
] RETURN NONE;
