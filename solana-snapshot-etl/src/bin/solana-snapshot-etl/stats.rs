use indicatif::{ProgressBar, ProgressStyle};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::AppendVec;
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, AppendVecConsumerFactory, GenericResult};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct OwnerStats {
    pub count: u64,
    pub total_size: u64,
}

pub struct SharedStats {
    accounts_spinner: ProgressBar,
    accounts_count: AtomicU64,
    stats_by_owner: Mutex<HashMap<Pubkey, OwnerStats>>,
}

impl SharedStats {
    pub fn new() -> Arc<Self> {
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        Arc::new(Self {
            accounts_spinner,
            accounts_count: AtomicU64::new(0),
            stats_by_owner: Mutex::new(HashMap::new()),
        })
    }

    pub fn print_stats(&self, top_n: Option<usize>) {
        let top_n = top_n.unwrap_or(100);
        let accounts_count = self.accounts_count.load(Ordering::Relaxed);
        println!("\n--- Account Stats by Owner (Top {}) ---\n", top_n);

        let stats_map = self.stats_by_owner.lock().unwrap();
        let mut stats: Vec<_> = stats_map.iter().collect();
        stats.sort_by(|a, b| b.1.total_size.cmp(&a.1.total_size));

        println!(
            "{:<45} {:>15} {:>20} {:>15}",
            "Owner", "Count", "Total Size (bytes)", "Avg Size"
        );
        println!("{}", "-".repeat(97));

        for (owner, owner_stats) in stats.into_iter().take(top_n) {
            let avg_size = if owner_stats.count > 0 {
                owner_stats.total_size / owner_stats.count
            } else {
                0
            };
            println!(
                "{:<45} {:>15} {:>20} {:>15}",
                owner.to_string(),
                owner_stats.count,
                owner_stats.total_size,
                avg_size
            );
        }

        println!("\nTotal accounts processed: {}", accounts_count);
    }

    pub fn finish(&self) {
        self.accounts_spinner.finish();
    }
}

pub struct StatsConsumerFactory {
    shared: Arc<SharedStats>,
}

impl StatsConsumerFactory {
    pub fn new(shared: Arc<SharedStats>) -> Self {
        Self { shared }
    }
}

impl AppendVecConsumerFactory for StatsConsumerFactory {
    type Consumer = StatsConsumer;

    fn new_consumer(&mut self) -> GenericResult<Self::Consumer> {
        Ok(StatsConsumer {
            shared: Arc::clone(&self.shared),
            local_stats: HashMap::new(),
            local_count: 0,
        })
    }
}

const FLUSH_INTERVAL: u64 = 10_000_000;

pub struct StatsConsumer {
    shared: Arc<SharedStats>,
    local_stats: HashMap<Pubkey, OwnerStats>,
    local_count: u64,
}

impl StatsConsumer {
    fn flush(&mut self) {
        if self.local_count == 0 {
            return;
        }

        let mut shared_stats = self.shared.stats_by_owner.lock().unwrap();
        for (owner, local) in self.local_stats.drain() {
            let entry = shared_stats.entry(owner).or_insert(OwnerStats {
                count: 0,
                total_size: 0,
            });
            entry.count += local.count;
            entry.total_size += local.total_size;
        }
        drop(shared_stats);

        let new_count = self
            .shared
            .accounts_count
            .fetch_add(self.local_count, Ordering::Relaxed)
            + self.local_count;
        self.shared.accounts_spinner.set_position(new_count);

        // Print stats every million accounts
        let old_millions = (new_count - self.local_count) / 1_000_000;
        let new_millions = new_count / 1_000_000;
        if new_millions > old_millions {
            self.shared.print_stats(Some(10));
        }

        self.local_count = 0;
    }
}

impl AppendVecConsumer for StatsConsumer {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            let owner = account.account_meta.owner;
            let data_len = account.data.len() as u64;

            let entry = self.local_stats.entry(owner).or_insert(OwnerStats {
                count: 0,
                total_size: 0,
            });
            entry.count += 1;
            entry.total_size += data_len;

            self.local_count += 1;

            if self.local_count >= FLUSH_INTERVAL {
                self.flush();
            }
        }
        Ok(())
    }
}

impl Drop for StatsConsumer {
    fn drop(&mut self) {
        self.flush();
    }
}
