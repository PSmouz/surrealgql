/**
[test]
run = false
*/

-- Seed for the GQL (.gql) conformance corpus: people (one without age,
-- one without active), a non-person node, and deterministic knows edges —
-- including one person→city edge and one edge without a since property.
-- The person-only edge graph over 1/2/3 is 1→2, 2→1, 2→3, 3→1, giving 2-hop
-- chains and a 1↔2 cycle for the variable-length tests.
CREATE person:1 SET name = 'A', age = 30, active = true, city = 'London' RETURN NONE;
CREATE person:2 SET name = 'B', age = 20, active = false, city = 'Paris' RETURN NONE;
CREATE person:3 SET name = 'C', city = 'London' RETURN NONE;
CREATE city:1 SET name = 'London' RETURN NONE;
INSERT RELATION INTO knows [
	{ id: knows:k12, in: person:1, out: person:2, since: 2021 },
	{ id: knows:k21, in: person:2, out: person:1, since: 2018 },
	{ id: knows:k23, in: person:2, out: person:3, since: 2020 },
	{ id: knows:k1c, in: person:1, out: city:1, since: 2019 },
	{ id: knows:k31, in: person:3, out: person:1 }
] RETURN NONE;

-- A clean directed triangle cycle 4→5→6→4 over a SEPARATE `hub` node table
-- joined by `link` edges, fully disjoint from the person/knows/city component
-- above. Keeping it on its own tables means `(n:person)` scans in the existing
-- corpus are unaffected (their expectations stay byte-identical), while the
-- per-path quantifier tests get a single simple cycle whose path counts at each
-- depth are hand-derivable without interference from the denser 1/2/3 graph.
-- Deterministic `since` values let edge-group / path-variable tests pin exact
-- edge records.
CREATE hub:4 SET name = 'D', age = 40 RETURN NONE;
CREATE hub:5 SET name = 'E', age = 50 RETURN NONE;
CREATE hub:6 SET name = 'F', age = 60 RETURN NONE;
INSERT RELATION INTO link [
	{ id: link:l45, in: hub:4, out: hub:5, since: 2014 },
	{ id: link:l56, in: hub:5, out: hub:6, since: 2015 },
	{ id: link:l64, in: hub:6, out: hub:4, since: 2016 }
] RETURN NONE;

-- A directed acyclic "diamond" over a SEPARATE `wp` (waypoint) node table joined
-- by `road` edges, again fully disjoint from the other components so existing
-- expectations stay byte-identical. From X there are two shortest (length-2)
-- routes to Z — X→Y1→Z and X→Y2→Z — and one longer (length-3) route
-- X→Y3→Y4→Z. This is what distinguishes the path-search selectors: ALL vs ANY
-- (multiplicity), ALL SHORTEST ties (two length-2 paths to Z), SHORTEST k /
-- k GROUP (how many of the {2,2,3}-length paths to Z survive), and an
-- asymmetric (directed) graph for the partition / reverse-anchor checks.
CREATE wp:x SET name = 'X' RETURN NONE;
CREATE wp:y1 SET name = 'Y1' RETURN NONE;
CREATE wp:y2 SET name = 'Y2' RETURN NONE;
CREATE wp:y3 SET name = 'Y3' RETURN NONE;
CREATE wp:y4 SET name = 'Y4' RETURN NONE;
CREATE wp:z SET name = 'Z' RETURN NONE;
INSERT RELATION INTO road [
	{ id: road:xy1, in: wp:x, out: wp:y1 },
	{ id: road:xy2, in: wp:x, out: wp:y2 },
	{ id: road:xy3, in: wp:x, out: wp:y3 },
	{ id: road:y1z, in: wp:y1, out: wp:z },
	{ id: road:y2z, in: wp:y2, out: wp:z },
	{ id: road:y3y4, in: wp:y3, out: wp:y4 },
	{ id: road:y4z, in: wp:y4, out: wp:z }
] RETURN NONE;

-- A "converging" DAG over a SEPARATE `cv` node table joined by `cvedge`, again
-- disjoint from everything above. From S there are two equal-length routes that
-- RECONVERGE at J before continuing to T: S→P→J→T and S→Q→J→T. This is the
-- shape the diamond cannot express — J is reached by two shortest sub-paths
-- (depth 2) that then share the J→T suffix — so it exercises the ALL-SHORTEST
-- tie-extension through a shared intermediate (and the SHORTEST-k per-node
-- bound).
CREATE cv:s SET name = 'S' RETURN NONE;
CREATE cv:p SET name = 'P' RETURN NONE;
CREATE cv:q SET name = 'Q' RETURN NONE;
CREATE cv:j SET name = 'J' RETURN NONE;
CREATE cv:t SET name = 'T' RETURN NONE;
INSERT RELATION INTO cvedge [
	{ id: cvedge:sp, in: cv:s, out: cv:p },
	{ id: cvedge:sq, in: cv:s, out: cv:q },
	{ id: cvedge:pj, in: cv:p, out: cv:j },
	{ id: cvedge:qj, in: cv:q, out: cv:j },
	{ id: cvedge:jt, in: cv:j, out: cv:t }
] RETURN NONE;
