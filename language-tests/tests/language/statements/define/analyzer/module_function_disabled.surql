/**
[env.capabilities]
allow-experimental = []

[test]
reason = "Analyzer FUNCTION mod:: requires the surrealism experimental capability"

[test.results]
parsing-error = """
Experimental capability `surrealism` is not enabled
  --> [18:30]
   |
18 | DEFINE ANALYZER ana FUNCTION mod::demo::alter TOKENIZERS class;
   |                              ^^^ Use of `mod::` is still experimental
"""
*/

DEFINE ANALYZER ana FUNCTION mod::demo::alter TOKENIZERS class;
