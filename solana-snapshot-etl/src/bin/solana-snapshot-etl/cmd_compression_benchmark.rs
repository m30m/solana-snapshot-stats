use crate::compression_benchmark::CompressionBenchmarkConsumer;
use crate::loader::SupportedLoader;
use log::{error, info};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::parallel::AppendVecConsumer;
use solana_snapshot_etl::SnapshotExtractor;

pub fn run(
    loader: &mut SupportedLoader,
    owner_filter: Option<Pubkey>,
    compression_level: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    match owner_filter {
        Some(owner) => info!("Filtering accounts by owner: {}", owner),
        None => info!("Processing all accounts (no owner filter)"),
    }
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
