#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &str| {
	// Don't crash, in the parser or in the lowering.
	_ = surrealdb_core::gql::parse_str(data).and_then(surrealdb_core::gql::lower);
});
