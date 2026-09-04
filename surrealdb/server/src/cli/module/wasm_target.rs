use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use surrealism_runtime::PrefixErr;

pub const TARGET: &str = "wasm32-wasip2";

const CARGO_CONFIG: &str = r#"[build]
rustflags = ["--cfg", "tokio_unstable"]
"#;

const TOKIO_UNSTABLE_CFG: &str = "--cfg tokio_unstable";

/// Ensure the module project has `.cargo/config.toml` with WASI build flags.
///
/// The Surrealism SDK depends on Tokio's `net` feature for `wasm32-wasip2`.
/// Tokio currently requires `--cfg tokio_unstable` for that target. Existing
/// files are left untouched so operators can customise flags.
pub fn ensure_cargo_config(project_dir: &Path) -> Result<()> {
	let cargo_dir = project_dir.join(".cargo");
	let config_path = cargo_dir.join("config.toml");
	if config_path.exists() {
		return Ok(());
	}

	fs::create_dir_all(&cargo_dir)
		.prefix_err(|| format!("Failed to create {}", cargo_dir.display()))?;
	fs::write(&config_path, CARGO_CONFIG)
		.prefix_err(|| format!("Failed to write {}", config_path.display()))?;

	Ok(())
}

/// Set `RUSTFLAGS` on a `cargo build` invocation, preserving any existing value.
pub fn apply_rustflags(cmd: &mut Command) {
	cmd.env("RUSTFLAGS", merge_tokio_unstable_rustflags());
}

fn merge_tokio_unstable_rustflags() -> String {
	match std::env::var("RUSTFLAGS") {
		Ok(existing) if existing.contains(TOKIO_UNSTABLE_CFG) => existing,
		Ok(existing) => format!("{existing} {TOKIO_UNSTABLE_CFG}"),
		Err(_) => TOKIO_UNSTABLE_CFG.to_string(),
	}
}
