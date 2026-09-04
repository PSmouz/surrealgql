/**
[test]
reason = "SELECT on an undefined table errors even with LIMIT 0"

[[test.results]]
error = "The table 'doesnt_exist' does not exist"

[[test.results]]
error = "The table 'doesnt_exist' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

*/

SELECT * FROM doesnt_exist;
SELECT * FROM doesnt_exist LIMIT 0;
DEFINE TABLE person;
SELECT * FROM person LIMIT 0;
