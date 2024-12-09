#![allow(clippy::upper_case_acronyms)]

use std::env;

use clap::Parser;
use log::debug;
use types::errors::ApplicationError;

mod engine;
mod types;

const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_NAME: &str = env!("CARGO_PKG_NAME");

#[derive(Parser, Debug)]
#[command(
    name = APP_NAME,
    version = APP_VERSION,
    about = "Engine/Tool to process transaction data",
)]
struct CLI {
    /// Path of input file in CSV format
    transaction_file_path: String,

    /// Optional log level
    #[arg(long, value_parser = ["error", "warn", "info", "debug", "trace"])]
    log_level: Option<String>,
}

fn main() -> Result<(), ApplicationError> {
    let cli = CLI::parse();
    debug!("CLI Arguments provided: {:?}", cli);
    if let Some(level) = cli.log_level {
        env::set_var("RUST_LOG", level);
    }
    env_logger::init();

    engine::run_transactions(cli.transaction_file_path.as_str())?;

    Ok(())
}
