/**
[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE INDEX number ON record FIELDS number;
	DEFINE INDEX status_created ON record FIELDS status, created_at;
	DEFINE INDEX http_created ON record FIELDS http_status, created_at;
	DEFINE INDEX tags_created ON record FIELDS tags.*, created_at;
	DEFINE INDEX geo_continent ON record FIELDS geography.region.details.continent;
	RETURN 'OK';
}
