use indicatif::{ProgressBar, ProgressStyle};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::AppendVec;
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use std::collections::HashMap;
use std::rc::Rc;

pub struct OwnerStats {
    pub count: u64,
    pub total_size: u64,
}

pub struct StatsCollector {
    accounts_spinner: ProgressBar,
    accounts_count: u64,
    stats_by_owner: HashMap<Pubkey, OwnerStats>,
}

impl AppendVecConsumer for StatsCollector {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            let owner = account.account_meta.owner;
            let data_len = account.data.len() as u64;

            let entry = self.stats_by_owner.entry(owner).or_insert(OwnerStats {
                count: 0,
                total_size: 0,
            });
            entry.count += 1;
            entry.total_size += data_len;

            self.accounts_count += 1;
            if self.accounts_count % 1024 == 0 {
                self.accounts_spinner.set_position(self.accounts_count);
            }
        }
        Ok(())
    }
}

impl StatsCollector {
    pub fn new() -> Self {
        let spinner_style = ProgressStyle::with_template(
            "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
        )
        .unwrap();
        let accounts_spinner = ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("accs");

        Self {
            accounts_spinner,
            accounts_count: 0,
            stats_by_owner: HashMap::new(),
        }
    }

    pub fn print_stats(&self) {
        println!("\n--- Account Stats by Owner ---\n");

        let mut stats: Vec<_> = self.stats_by_owner.iter().collect();
        stats.sort_by(|a, b| b.1.total_size.cmp(&a.1.total_size));

        println!("{:<45} {:>15} {:>20}", "Owner", "Count", "Total Size (bytes)");
        println!("{}", "-".repeat(80));

        for (owner, owner_stats) in stats {
            println!(
                "{:<45} {:>15} {:>20}",
                owner.to_string(),
                owner_stats.count,
                owner_stats.total_size
            );
        }

        println!("\nTotal accounts processed: {}", self.accounts_count);
    }
}

impl Drop for StatsCollector {
    fn drop(&mut self) {
        self.accounts_spinner.finish();
    }
}
