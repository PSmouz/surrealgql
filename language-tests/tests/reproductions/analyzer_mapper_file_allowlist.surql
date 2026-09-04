/**
[env]
namespace = true
database = true

[test]
reason = "DEFINE ANALYZER mapper() must enforce the configured file_allowlist. The test harness allows only the `../tests` tree, so a mapper path outside it must be denied rather than read off the host filesystem."

[[test.results]]
error = true
match = "$error = /File access denied/"

*/

-- A mapper() path outside the configured file_allowlist must be rejected at
-- definition time, before the file is opened. Reading it would otherwise leak
-- arbitrary host file content to the caller.
--
-- This deliberately targets a real file that exists but lies *outside* the
-- harness allowlist (`../tests`): the workspace-root `../Cargo.toml`. With a
-- non-empty allowlist, `check_is_path_allowed` canonicalizes the path before
-- the prefix check, so a non-existent path would fail with an unrelated
-- canonicalize IO error ("No such file or directory") instead of the
-- `File access denied` we assert. If this file is ever moved or renamed, point
-- the test at another real file outside `../tests` to keep the assertion valid.
DEFINE ANALYZER pwn TOKENIZERS blank FILTERS mapper('../Cargo.toml');
