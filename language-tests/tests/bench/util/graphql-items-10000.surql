/**
# GraphQL bench dataset: 10000 `item` rows with an integer and a string field,
# plus the GraphQL config required to generate the dynamic schema. `run = false`.

[test]
run = false
*/

{
	DEFINE CONFIG GRAPHQL TABLES AUTO;

	DEFINE TABLE item SCHEMAFULL;
	DEFINE FIELD number ON item TYPE int;
	DEFINE FIELD label ON item TYPE string;

	CREATE |item:1..10000| SET
		number = rand::int(0, 100),
		label = rand::string(12)
	RETURN NONE;

	RETURN 'OK';
}
