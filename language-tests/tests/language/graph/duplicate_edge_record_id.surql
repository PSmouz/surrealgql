/**
[test]
reason = "RELATE updates duplicate explicit edge ids with a warning; INSERT RELATION must error unless ON DUPLICATE KEY UPDATE is set. Note: RELATE should return a hard error beginning in 4.0"

[[test.results]]
value = "[{ id: person:one }, { id: person:two }, { id: post:one }]"

[[test.results]]
value = "[{ id: likes:[person:one, post:one], in: person:one, out: post:one }]"

[[test.results]]
match = "$result[0].id = likes:[person:one, post:one] AND $result[0].liked_at != NONE"

[[test.results]]
value = "[{ id: likes:[person:two, post:one], in: person:two, note: 'first', out: post:one }]"

[[test.results]]
match = "$error = /Database record .*likes:\\[person:two, post:one\\].* already exists/"

[[test.results]]
match = "$result[0].note = 'explicit'"

[[test.results]]
match = "$result[0].id = likes:[person:two, post:one] AND $result[0].note = 'ondup'"

*/

CREATE person:one, person:two, post:one;

RELATE person:one->likes:[person:one, post:one]->post:one;

RELATE person:one->likes:[person:one, post:one]->post:one
  SET liked_at = time::now();

INSERT RELATION INTO likes {
  in: person:two,
  out: post:one,
  id: [person:two, post:one],
  note: 'first',
};

INSERT RELATION INTO likes {
  in: person:two,
  out: post:one,
  id: [person:two, post:one],
  note: 'second',
};

RELATE OR UPDATE person:one->likes:[person:one, post:one]->post:one
  SET note = 'explicit';

INSERT RELATION INTO likes {
  in: person:two,
  out: post:one,
  id: [person:two, post:one],
  note: 'third',
} ON DUPLICATE KEY UPDATE note = 'ondup';
