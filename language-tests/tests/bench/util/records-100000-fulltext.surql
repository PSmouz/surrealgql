/**
[env]
imports = ["./records-100000.surql"]

[test]
run = false
*/

-- Separate statements (not a `{ }` block) so the analyzer is committed before the
-- fulltext index references it; in one transaction the index can't see it yet.
DEFINE ANALYZER text_analyzer TOKENIZERS blank, class FILTERS lowercase, ascii;
DEFINE INDEX words ON record FIELDS words FULLTEXT ANALYZER text_analyzer BM25;
