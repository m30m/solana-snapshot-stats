use clap::{Parser, Subcommand};
use loader::{LoadProgressTracking, SupportedLoader};
use log::{error, info};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;

mod cmd_compression_benchmark;
mod cmd_debug;
mod cmd_dump_tokens;
mod cmd_stats;
mod compression_benchmark;
mod loader;
mod mpl_metadata;
mod stats;
mod token;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(help = "Snapshot source (unpacked snapshot, archive file, or HTTP link)")]
    source: String,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Collect and display account statistics by owner
    Stats,

    /// Benchmark zstd compression for accounts owned by a specific program
    CompressionBenchmark {
        #[clap(long, help = "Filter accounts by this owner pubkey")]
        owner: String,

        #[clap(long, default_value = "3", help = "Zstd compression level (1-22)")]
        level: i32,
    },

    /// Print a few sample accounts filtered by owner and exit
    Debug {
        #[clap(long, help = "Filter accounts by this owner pubkey")]
        owner: String,

        #[clap(long, default_value = "5", help = "Number of accounts to print")]
        count: usize,
    },

    /// Dump all token accounts to a DuckDB database
    DumpTokens {
        #[clap(long, help = "Path to the DuckDB database file")]
        db: String,
    },
}

fn main() {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );
    if let Err(e) = _main() {
        error!("{}", e);
        std::process::exit(1);
    }
}

fn _main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut loader = SupportedLoader::new(&args.source, Box::new(LoadProgressTracking {}))?;
    info!("Processing snapshot: {}", &args.source);

    let num_threads = num_cpus::get() / 2;
    info!("Using {} threads", num_threads);

    match args.command {
        Command::Stats => {
            cmd_stats::run(&mut loader, num_threads)?;
        }
        Command::CompressionBenchmark { owner, level } => {
            let owner_filter = if owner == "all" {
                None
            } else {
                Some(
                    Pubkey::from_str(&owner)
                        .map_err(|e| format!("Invalid owner pubkey '{}': {}", owner, e))?,
                )
            };
            cmd_compression_benchmark::run(&mut loader, owner_filter, level)?;
        }
        Command::Debug { owner, count } => {
            let owner_pubkey = Pubkey::from_str(&owner)
                .map_err(|e| format!("Invalid owner pubkey '{}': {}", owner, e))?;
            cmd_debug::run(&mut loader, owner_pubkey, count)?;
        }
        Command::DumpTokens { db } => {
            cmd_dump_tokens::run(&mut loader, &db)?;
        }
    }

    println!("Done!");
    Ok(())
}
