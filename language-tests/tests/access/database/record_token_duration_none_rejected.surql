/**
[test]
reason = "DEFINE ACCESS TYPE RECORD with DURATION FOR TOKEN NONE must be rejected because record-access tokens authenticate end-users and must have an expiration"

[[test.results]]
error = "Tokens issued by record access methods can be consumed by third parties and must have an expiration; DURATION FOR TOKEN cannot be NONE on TYPE RECORD access"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Tokens issued by record access methods can be consumed by third parties and must have an expiration; DURATION FOR TOKEN cannot be NONE on TYPE RECORD access"

*/

-- Literal NONE in the DURATION FOR TOKEN clause: must be rejected.
DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN NONE;

-- Parameterized NONE (the case that broke the parser-time guarantee) must be
-- rejected at compute time too.
LET $dur = NONE;
DEFINE ACCESS account ON DATABASE TYPE RECORD WITH JWT ALGORITHM HS512 KEY 'secret' DURATION FOR TOKEN $dur;
