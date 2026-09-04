/**
[test]
reason = "Analyzer FUNCTION rejects SurrealML model functions"

[test.results]
parsing-error = """
Analyzers cannot use model functions
  --> [15:30]
   |
15 | DEFINE ANALYZER ana FUNCTION ml::insurance::prediction<1.0.0> TOKENIZERS class;
   |                              ^^ Model functions cannot be used in an analyzer `FUNCTION` clause
"""
*/

DEFINE ANALYZER ana FUNCTION ml::insurance::prediction<1.0.0> TOKENIZERS class;
