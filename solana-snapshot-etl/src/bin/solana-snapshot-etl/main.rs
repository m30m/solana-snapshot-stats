use crate::compression_benchmark::CompressionBenchmarkConsumer;
use crate::stats::{SharedStats, StatsConsumerFactory};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::{error, info};
use reqwest::blocking::Response;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::archived::ArchiveSnapshotExtractor;
use solana_snapshot_etl::parallel::par_iter_append_vecs;
use solana_snapshot_etl::unpacked::UnpackedSnapshotExtractor;
use solana_snapshot_etl::{AppendVecIterator, ReadProgressTracking, SnapshotExtractor};
use std::fs::File;
use std::io::{IoSliceMut, Read};
use std::path::Path;
use std::str::FromStr;

mod compression_benchmark;
mod mpl_metadata;
mod stats;

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
            run_stats(&mut loader, num_threads)?;
        }
        Command::CompressionBenchmark { owner, level } => {
            let owner_pubkey = Pubkey::from_str(&owner)
                .map_err(|e| format!("Invalid owner pubkey '{}': {}", owner, e))?;
            run_compression_benchmark(&mut loader, owner_pubkey, level)?;
        }
        Command::Debug { owner, count } => {
            let owner_pubkey = Pubkey::from_str(&owner)
                .map_err(|e| format!("Invalid owner pubkey '{}': {}", owner, e))?;
            run_debug(&mut loader, owner_pubkey, count)?;
        }
    }

    println!("Done!");
    Ok(())
}

fn run_stats(
    loader: &mut SupportedLoader,
    num_threads: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let shared_stats = SharedStats::new();
    let mut factory = StatsConsumerFactory::new(shared_stats.clone());

    par_iter_append_vecs(loader.iter(), &mut factory, num_threads)?;

    shared_stats.finish();
    shared_stats.print_stats(None);

    Ok(())
}

fn run_compression_benchmark(
    loader: &mut SupportedLoader,
    owner_filter: Pubkey,
    compression_level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    use solana_snapshot_etl::parallel::AppendVecConsumer;

    info!("Filtering accounts by owner: {}", owner_filter);
    info!("Compression level: {}", compression_level);

    let mut consumer = CompressionBenchmarkConsumer::new(owner_filter, compression_level);

    for append_vec in loader.iter() {
        match append_vec {
            Ok(v) => {
                consumer.on_append_vec(v).unwrap_or_else(|err| {
                    error!("on_append_vec: {:?}", err);
                });
            }
            Err(err) => error!("append_vec: {:?}", err),
        }
    }

    consumer.finish();

    Ok(())
}

const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_ACCOUNT_LEN: usize = 165;

fn run_debug(
    loader: &mut SupportedLoader,
    owner_filter: Pubkey,
    max_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use solana_snapshot_etl::append_vec_iter;
    use std::rc::Rc;

    info!("Looking for accounts owned by: {}", owner_filter);

    let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap();
    let mut found = 0;

    'outer: for append_vec in loader.iter() {
        let append_vec = append_vec?;
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();

            if account.account_meta.owner != owner_filter {
                continue;
            }

            found += 1;
            println!("\n--- Account {} ---", found);
            println!("Pubkey:      {}", account.meta.pubkey);
            println!("Owner:       {}", account.account_meta.owner);
            println!("Lamports:    {}", account.account_meta.lamports);
            println!("Data len:    {}", account.data.len());
            println!("Executable:  {}", account.account_meta.executable);
            println!("Rent epoch:  {}", account.account_meta.rent_epoch);

            // Try to parse as SPL Token Account
            if account.account_meta.owner == token_program && account.data.len() == TOKEN_ACCOUNT_LEN {
                print_token_account(account.data);
            } else {
                // Print first 64 bytes of data as hex
                let preview_len = account.data.len().min(64);
                if preview_len > 0 {
                    println!("Data (first {} bytes): {:02x?}", preview_len, &account.data[..preview_len]);
                }
            }

            if found >= max_count {
                break 'outer;
            }
        }
    }

    println!("\nFound {} accounts", found);
    Ok(())
}

fn print_token_account(data: &[u8]) {
    // Token Account layout (165 bytes):
    // - mint: Pubkey (32)
    // - owner: Pubkey (32)
    // - amount: u64 (8)
    // - delegate: COption<Pubkey> (4 + 32 = 36)
    // - state: u8 (1)
    // - is_native: COption<u64> (4 + 8 = 12)
    // - delegated_amount: u64 (8)
    // - close_authority: COption<Pubkey> (4 + 32 = 36)

    let mint = Pubkey::try_from(&data[0..32]).unwrap();
    let owner = Pubkey::try_from(&data[32..64]).unwrap();
    let amount = u64::from_le_bytes(data[64..72].try_into().unwrap());

    let delegate_tag = u32::from_le_bytes(data[72..76].try_into().unwrap());
    let delegate = if delegate_tag == 1 {
        Some(Pubkey::try_from(&data[76..108]).unwrap())
    } else {
        None
    };

    let state = data[108];
    let state_str = match state {
        0 => "Uninitialized",
        1 => "Initialized",
        2 => "Frozen",
        _ => "Unknown",
    };

    let is_native_tag = u32::from_le_bytes(data[109..113].try_into().unwrap());
    let is_native = if is_native_tag == 1 {
        Some(u64::from_le_bytes(data[113..121].try_into().unwrap()))
    } else {
        None
    };

    let delegated_amount = u64::from_le_bytes(data[121..129].try_into().unwrap());

    let close_authority_tag = u32::from_le_bytes(data[129..133].try_into().unwrap());
    let close_authority = if close_authority_tag == 1 {
        Some(Pubkey::try_from(&data[133..165]).unwrap())
    } else {
        None
    };

    println!("Token Account:");
    println!("  Mint:             {}", mint);
    println!("  Token Owner:      {}", owner);
    println!("  Amount:           {}", amount);
    println!("  Delegate:         {:?}", delegate);
    println!("  State:            {} ({})", state, state_str);
    println!("  Is Native:        {:?}", is_native);
    println!("  Delegated Amount: {}", delegated_amount);
    println!("  Close Authority:  {:?}", close_authority);
}

struct LoadProgressTracking {}

impl ReadProgressTracking for LoadProgressTracking {
    fn new_read_progress_tracker(
        &self,
        _: &Path,
        rd: Box<dyn Read>,
        file_len: u64,
    ) -> Box<dyn Read> {
        let progress_bar = ProgressBar::new(file_len).with_style(
            ProgressStyle::with_template(
                "{prefix:>10.bold.dim} {spinner:.green} [{bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        progress_bar.set_prefix("manifest");
        Box::new(LoadProgressTracker {
            rd: progress_bar.wrap_read(rd),
            progress_bar,
        })
    }
}

struct LoadProgressTracker {
    progress_bar: ProgressBar,
    rd: ProgressBarIter<Box<dyn Read>>,
}

impl Drop for LoadProgressTracker {
    fn drop(&mut self) {
        self.progress_bar.finish()
    }
}

impl Read for LoadProgressTracker {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rd.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.rd.read_vectored(bufs)
    }

    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.rd.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.rd.read_exact(buf)
    }
}

pub enum SupportedLoader {
    Unpacked(UnpackedSnapshotExtractor),
    ArchiveFile(ArchiveSnapshotExtractor<File>),
    ArchiveDownload(ArchiveSnapshotExtractor<Response>),
}

impl SupportedLoader {
    fn new(
        source: &str,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Self::new_download(source)
        } else {
            Self::new_file(source.as_ref(), progress_tracking).map_err(Into::into)
        }
    }

    fn new_download(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let resp = reqwest::blocking::get(url)?;
        let loader = ArchiveSnapshotExtractor::from_reader(resp)?;
        info!("Streaming snapshot from HTTP");
        Ok(Self::ArchiveDownload(loader))
    }

    fn new_file(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> solana_snapshot_etl::Result<Self> {
        Ok(if path.is_dir() {
            info!("Reading unpacked snapshot");
            Self::Unpacked(UnpackedSnapshotExtractor::open(path, progress_tracking)?)
        } else {
            info!("Reading snapshot archive");
            Self::ArchiveFile(ArchiveSnapshotExtractor::open(path)?)
        })
    }
}

impl SnapshotExtractor for SupportedLoader {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        match self {
            SupportedLoader::Unpacked(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveFile(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveDownload(loader) => Box::new(loader.iter()),
        }
    }
}
