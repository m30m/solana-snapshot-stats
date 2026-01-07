use crate::loader::SupportedLoader;
use crate::stats::{SharedStats, StatsConsumerFactory};
use solana_snapshot_etl::parallel::par_iter_append_vecs;
use solana_snapshot_etl::SnapshotExtractor;

pub fn run(
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
