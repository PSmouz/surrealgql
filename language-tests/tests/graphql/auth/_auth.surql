/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Record access definition and user table imported by the GraphQL auth tests"
*/

-- Expose all tables; the access definition below also generates the
-- signIn/signUp GraphQL mutations.
DEFINE CONFIG GRAPHQL TABLES AUTO;

-- Users can only select themselves.
DEFINE TABLE user SCHEMAFULL PERMISSIONS FOR select WHERE id = $auth.id;
DEFINE FIELD email ON user TYPE string;
DEFINE FIELD pass ON user TYPE string PERMISSIONS NONE;
DEFINE INDEX email ON user FIELDS email UNIQUE;

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNUP ( CREATE user SET email = $email, pass = crypto::argon2::generate($pass) )
	SIGNIN ( SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass) )
	DURATION FOR TOKEN 15m, FOR SESSION 12h;

CREATE user:alice SET email = "alice@example.com", pass = crypto::argon2::generate("alice-secret");
CREATE user:bob SET email = "bob@example.com", pass = crypto::argon2::generate("bob-secret");
