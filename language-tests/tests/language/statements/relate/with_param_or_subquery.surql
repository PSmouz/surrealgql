/**
[test]
reason = "Note: Some RELATE statemens currently succeed despite being the same edge ID but emits a warning and suggestion to opt in via OR RELATE. Should return a hard error beginning in 4.0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:foo, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "[{ id: knows:bar, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: knows:foo, in: person:tobie, out: person:jaime }]"

[[test.results]]
value = "[{ id: knows:bar, in: person:tobie, out: person:jaime }]"


*/

LET $tobie = person:tobie;
LET $jaime = person:jaime;
LET $relation = type::table("knows");
RELATE $tobie->$relation->$jaime CONTENT { id: "foo" };
RELATE $tobie->(type::table("knows"))->$jaime CONTENT { id: "bar" };
LET $relation = type::record("knows:foo");
RELATE $tobie->$relation->$jaime;
RELATE $tobie->(type::record("knows:bar"))->$jaime;
