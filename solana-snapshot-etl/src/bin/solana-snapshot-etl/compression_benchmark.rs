use indicatif::{ProgressBar, ProgressStyle};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::AppendVec;
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::parallel::{AppendVecConsumer, GenericResult};
use std::io::Write;
use std::rc::Rc;
use zstd::stream::Encoder;

/// A sink that counts bytes written but discards the data
struct CountingSink {
    bytes_written: u64,
}

impl CountingSink {
    fn new() -> Self {
        Self { bytes_written: 0 }
    }

    fn count(&self) -> u64 {
        self.bytes_written
    }
}

impl Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bytes_written += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BenchmarkStats {
    accounts_spinner: ProgressBar,
    accounts_count: u64,
    filtered_count: u64,
    total_uncompressed: u64,
}

impl BenchmarkStats {
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
            filtered_count: 0,
            total_uncompressed: 0,
        }
    }

    pub fn print_stats(&self, total_compressed: u64) {
        let ratio = if self.total_uncompressed > 0 {
            total_compressed as f64 / self.total_uncompressed as f64
        } else {
            0.0
        };

        println!("\n--- Compression Benchmark Stats ---\n");
        println!("Accounts scanned:     {:>15}", self.accounts_count);
        println!("Accounts matched:     {:>15}", self.filtered_count);
        println!("Total uncompressed:   {:>15} bytes", self.total_uncompressed);
        println!("Total compressed:     {:>15} bytes", total_compressed);
        println!("Compression ratio:    {:>15.4}", ratio);
        println!(
            "Space savings:        {:>14.2}%",
            (1.0 - ratio) * 100.0
        );
    }

    pub fn finish(&self) {
        self.accounts_spinner.finish();
    }
}

pub struct CompressionBenchmarkConsumer {
    stats: BenchmarkStats,
    owner_filter: Option<Pubkey>,
    encoder: Option<Encoder<'static, CountingSink>>,
}

impl CompressionBenchmarkConsumer {
    pub fn new(owner_filter: Option<Pubkey>, compression_level: i32) -> Self {
        let encoder = Encoder::new(CountingSink::new(), compression_level)
            .expect("Failed to create zstd encoder");

        Self {
            stats: BenchmarkStats::new(),
            owner_filter,
            encoder: Some(encoder),
        }
    }

    pub fn print_stats(&self) {
        let compressed = self
            .encoder
            .as_ref()
            .map(|e| e.get_ref().count())
            .unwrap_or(0);
        self.stats.print_stats(compressed);
    }

    pub fn finish(&mut self) {
        let compressed = if let Some(encoder) = self.encoder.take() {
            match encoder.finish() {
                Ok(sink) => sink.count(),
                Err(e) => {
                    eprintln!("Error finishing encoder: {}", e);
                    0
                }
            }
        } else {
            0
        };
        self.stats.finish();
        self.stats.print_stats(compressed);
    }
}

impl AppendVecConsumer for CompressionBenchmarkConsumer {
    fn on_append_vec(&mut self, append_vec: AppendVec) -> GenericResult<()> {
        let encoder = self.encoder.as_mut().expect("encoder already finished");

        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            self.stats.accounts_count += 1;

            if self.stats.accounts_count % 1024 == 0 {
                self.stats
                    .accounts_spinner
                    .set_position(self.stats.accounts_count);
            }

            // Filter by owner (if specified)
            if let Some(owner_filter) = self.owner_filter {
                if account.account_meta.owner != owner_filter {
                    continue;
                }
            }

            self.stats.filtered_count += 1;

            // Serialize account data for compression
            // We compress: pubkey (32) + lamports (8) + rent_epoch (8) + owner (32) + executable (1) + data
            let uncompressed_size = 32 + 8 + 8 + 32 + 1 + account.data.len();
            self.stats.total_uncompressed += uncompressed_size as u64;

            // Write to streaming encoder
            encoder.write_all(account.meta.pubkey.as_ref())?;
            encoder.write_all(&account.account_meta.lamports.to_le_bytes())?;
            encoder.write_all(&account.account_meta.rent_epoch.to_le_bytes())?;
            encoder.write_all(account.account_meta.owner.as_ref())?;
            encoder.write_all(&[account.account_meta.executable as u8])?;
            encoder.write_all(account.data)?;

            // Print stats every million accounts
            if self.stats.accounts_count % 1_000_000 == 0 {
                self.stats.print_stats(encoder.get_ref().count());
            }
        }
        Ok(())
    }
}
