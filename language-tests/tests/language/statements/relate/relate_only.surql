/**
[test]
reason = "Note: Second RELATE currently succeeds despite being the same edge ID but emits a warning and suggestion to opt in via OR RELATE. Should return a hard error beginning in 4.0"

[[test.results]]
value = "{ id: edge:1, in: a:1, out: a:2 }"

[[test.results]]
value = "{ id: edge:1, in: a:1, name: 'A', out: a:2 }"

*/

RELATE ONLY a:1->edge:1->a:2;
RELATE ONLY a:1->edge:1->a:2 SET name = 'A';
