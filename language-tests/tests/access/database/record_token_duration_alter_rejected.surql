/**
[test]
reason = "ALTER ACCESS on TYPE RECORD cannot leave DURATION FOR TOKEN as NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Tokens issued by record access methods can be consumed by third parties and must have an expiration; DURATION FOR TOKEN cannot be NONE on TYPE RECORD access"

*/

-- Define a record access method with a valid token duration.
DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN 15m, FOR SESSION 12h;

-- ALTER ACCESS ... DURATION FOR TOKEN NONE clears the token duration; for
-- record access that produces a non-expiring token, which must be rejected.
ALTER ACCESS account ON DATABASE DURATION FOR TOKEN NONE;
