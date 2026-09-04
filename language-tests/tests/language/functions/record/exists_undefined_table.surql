/**
[test]
reason = "record::exists returns false when the table is undefined"
issue = 0

[[test.results]]
value = "false"

*/

record::exists(r"person:tobie");
