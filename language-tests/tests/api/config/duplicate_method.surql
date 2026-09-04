/**
[test]
reason = "DEFINE API must reject duplicate methods across or within FOR clauses"

# 0 — duplicate method across two FOR clauses
[[test.results]]
error = "The method 'get' is defined in more than one FOR clause on api '/dup_across'"

# 1 — duplicate method within a single FOR clause
[[test.results]]
error = "The method 'post' is defined in more than one FOR clause on api '/dup_within'"

# 2 — duplicate method in a comma list combined with another FOR clause
[[test.results]]
error = "The method 'put' is defined in more than one FOR clause on api '/dup_mixed'"

# 3 — disjoint methods across two FOR clauses are accepted
[[test.results]]
value = "NONE"

# 4 — invoking the disjoint definition succeeds
[[test.results]]
skip-api-request-id = true
value = "{ body: { handler: 'post' }, headers: {}, status: 200 }"

*/

# 0 — duplicate method across two FOR clauses
DEFINE API "/dup_across"
    FOR get
        THEN {
            { status: 200, body: { handler: "first" } };
        }
    FOR get
        THEN {
            { status: 200, body: { handler: "second" } };
        };

# 1 — duplicate method within a single FOR clause
DEFINE API "/dup_within"
    FOR post, post
        THEN {
            { status: 200, body: { handler: "duplicated" } };
        };

# 2 — duplicate method in a comma list combined with another FOR clause
DEFINE API "/dup_mixed"
    FOR get, put
        THEN {
            { status: 200, body: { handler: "first" } };
        }
    FOR put, delete
        THEN {
            { status: 200, body: { handler: "second" } };
        };

# 3 — disjoint methods across two FOR clauses are accepted
DEFINE API "/disjoint"
    FOR get
        THEN {
            { status: 200, body: { handler: "get" } };
        }
    FOR post
        THEN {
            { status: 200, body: { handler: "post" } };
        };

# 4 — invoking the disjoint definition succeeds
api::invoke("/disjoint", { method: "post" });
