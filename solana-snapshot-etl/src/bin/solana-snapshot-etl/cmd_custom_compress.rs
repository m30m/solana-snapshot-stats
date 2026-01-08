use crate::compressor::{Compressor, TokenAccountCompressor};
use crate::loader::SupportedLoader;
use crate::token::TOKEN_PROGRAM_ID;
use indicatif::{ProgressBar, ProgressStyle};
use log::info;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::SnapshotExtractor;
use std::rc::Rc;
use std::str::FromStr;

pub fn run(
    loader: &mut SupportedLoader,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap();

    let mut compressor = TokenAccountCompressor::new();

    let spinner_style = ProgressStyle::with_template(
        "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
    )
    .unwrap();
    let spinner = ProgressBar::new_spinner()
        .with_style(spinner_style)
        .with_prefix("compress");

    let mut total_accounts: u64 = 0;
    let mut accepted_accounts: u64 = 0;

    for append_vec in loader.iter() {
        let append_vec = append_vec?;
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            total_accounts += 1;

            if total_accounts % 10000 == 0 {
                spinner.set_position(accepted_accounts);
            }

            // Filter for token program accounts
            if account.account_meta.owner != token_program {
                continue;
            }

            // Pass to compressor for deserialization
            if compressor.add(&account) {
                accepted_accounts += 1;
            }
            if accepted_accounts > 10_000_000 {
                break;
            }
        }
    }

    spinner.finish();

    info!(
        "Processed {} accounts from {} total accounts",
        accepted_accounts, total_accounts
    );

    info!("Persisting to: {}", output_path);
    compressor.persist(output_path)?;

    info!("Done! Saved {} accounts", compressor.len());

    Ok(())
}
