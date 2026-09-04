use clap::builder::EnumValueParser;
use clap::{Command, arg};

use crate::cli::Backend;

pub fn cmd() -> Command {
	Command::new("bench")
		.about("Commands related to the surrealdb language benchmark suite")
		.arg(arg!(--"store-path" <PATH> "Set the path to save the result of the benchmark to").default_value("./bench_results").global(true))
		.arg(arg!(--"store-url" <URL> "Set the websocket url to save the result of the benchmark to").global(true))
		.arg(arg!(--"store-db" <DATABASE> "Set the database in which the results are stored").default_value("main").global(true))
		.arg(arg!(--"store-ns" <NAMESPACE> "Set the namespace in which the results are stored").default_value("main").global(true))
		.arg(arg!(--"store-user" <USER> "Set the username to login to the benchmark result datastore").default_value("viewer").env("LANG_BENCH_USER").global(true))
		.arg(arg!(--"store-password" <PASSWORD> "Set the password to login to the benchmark result datastore").default_value("viewer").env("LANG_BENCH_PASSWORD").global(true))
		.subcommand(Command::new("run").about("Run the surrealdb benchmarking suite")
			.arg(arg!([filter] "Filter the benches by their path"))
			.arg(
				arg!(--dataset <DATASET> "For `[bench].datasets` matrix benches, only run the variant with this name (e.g. `unindexed` or `indexed`)")
			)
			.arg(
				arg!(--backend <BACKEND> "Specify the storage backend to use for the upgrade test")
					.value_parser(EnumValueParser::<Backend>::new()).default_value("mem")
			)
            .arg(arg!(--path <PATH> "The path to tests directory").default_value("./tests"))
			.arg(arg!(-s --save "Save the result to the comparison datastore"))
			.arg(arg!(-q --quick "Run a fast, low-sample comparison (coarse: catches large regressions, not small drift)"))
			.arg(arg!(--json <PATH> "Write per-bench results and the comparison verdict to a JSON file"))
		)
		.subcommand_required(true)
}
