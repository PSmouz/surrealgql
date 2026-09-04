/**
[env]
namespace = true
database = true

[test]
reason = "`[$]` on a set returns the greatest value in sorted order, matching `set::last()`"

[[test.results]]
value = "3"

[[test.results]]
value = "6"

[[test.results]]
value = "2"

[[test.results]]
value = "6"

*/

[1, 2, 3][$];
{4, 6, 2}[$];
{4, 6, 2}[0];
set::last({4, 6, 2});
