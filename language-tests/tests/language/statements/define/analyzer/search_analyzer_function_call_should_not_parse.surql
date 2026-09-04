/**
[env.capabilities]
allow-experimental = []

[test.results]
parsing-error = """
Analyzer FUNCTION expects a function name, not a function call
  --> [15:66]
   |
15 | ...e_function();
   |              ^ Remove `()` from the analyzer FUNCTION clause (for example: `fn::some` or `mod::demo::some`)
"""
*/

DEFINE ANALYZER tries_to_call_function FUNCTION fn::some_function();