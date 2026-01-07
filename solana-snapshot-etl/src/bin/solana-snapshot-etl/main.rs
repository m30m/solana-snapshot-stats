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
    consumer.print_stats();

    Ok(())
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
