/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "Shared schema, data, and GraphQL config imported by the GraphQL test suite"
*/

-- Enable GraphQL for this database, exposing all tables and functions.
DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;

-- A regular table with a variety of scalar field types and a nested object.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE int;
DEFINE FIELD active ON person TYPE bool;
DEFINE FIELD score ON person TYPE float;
DEFINE FIELD email ON person TYPE option<string>;
DEFINE FIELD tags ON person TYPE array<string>;
DEFINE FIELD address ON person TYPE object;
DEFINE FIELD address.city ON person TYPE string;
DEFINE FIELD address.country ON person TYPE string;

-- A table linking to person through a record field.
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD title ON post TYPE string;
DEFINE FIELD views ON post TYPE int;
DEFINE FIELD published ON post TYPE bool;
DEFINE FIELD author ON post TYPE record<person>;

-- A relation table between person and post.
DEFINE TABLE likes TYPE RELATION FROM person TO post SCHEMAFULL;
DEFINE FIELD rating ON likes TYPE int;

-- A table exercising the remaining scalar types.
DEFINE TABLE event SCHEMAFULL;
DEFINE FIELD label ON event TYPE string;
DEFINE FIELD at ON event TYPE datetime;
DEFINE FIELD took ON event TYPE duration;
DEFINE FIELD uid ON event TYPE uuid;
DEFINE FIELD loc ON event TYPE geometry<point>;
DEFINE FIELD amount ON event TYPE decimal;

-- Functions exposed through GraphQL (only functions with a return type are).
DEFINE FUNCTION fn::num() -> int { RETURN 42 };
DEFINE FUNCTION fn::double($x: int) -> int { RETURN $x * 2 };
DEFINE FUNCTION fn::greet($name: string) -> string { RETURN "Hello, " + $name + "!" };
DEFINE FUNCTION fn::alice() -> record<person> { RETURN person:alice };

-- Data. Record keys are chosen so that the default scan order (by record key)
-- is deterministic: alice < bob < carol, draft < hello < intro.
CREATE person:alice SET name = "Alice", age = 30, active = true, score = 4.5, email = "alice@example.com", tags = ["admin", "dev"], address = { city: "London", country: "UK" };
CREATE person:bob SET name = "Bob", age = 25, active = false, score = 3.5, tags = ["dev"], address = { city: "Berlin", country: "DE" };
CREATE person:carol SET name = "Carol", age = 35, active = true, score = 5.0, email = "carol@example.com", tags = [], address = { city: "Paris", country: "FR" };

CREATE post:draft SET title = "Draft post", views = 5, published = false, author = person:bob;
CREATE post:hello SET title = "Hello world", views = 100, published = true, author = person:alice;
CREATE post:intro SET title = "Intro to GraphQL", views = 42, published = true, author = person:alice;

RELATE person:alice->likes->post:hello SET rating = 5;
RELATE person:alice->likes->post:intro SET rating = 3;
RELATE person:bob->likes->post:hello SET rating = 4;

CREATE event:launch SET label = "Launch", at = d"2024-01-15T10:00:00Z", took = 2h30m, uid = u"018e3b1c-7c5d-7c5e-8f30-111111111111", loc = (51.5, -0.12), amount = 19.99dec;
