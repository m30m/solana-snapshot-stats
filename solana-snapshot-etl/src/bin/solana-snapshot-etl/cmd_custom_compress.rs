use crate::compressor::{Compressor, TokenAccountCompressor, TokenAccountData};
use crate::loader::SupportedLoader;
use crate::token::{
    ASSOCIATED_TOKEN_PROGRAM_ID, TOKEN_ACCOUNT_LEN, TOKEN_PROGRAM_ID,
};
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
    let ata_program = Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).unwrap();

    let mut compressor = TokenAccountCompressor::new();

    let spinner_style = ProgressStyle::with_template(
        "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
    )
    .unwrap();
    let spinner = ProgressBar::new_spinner()
        .with_style(spinner_style)
        .with_prefix("compress");

    let mut total_accounts: u64 = 0;
    let mut token_accounts: u64 = 0;

    for append_vec in loader.iter() {
        let append_vec = append_vec?;
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            total_accounts += 1;

            if total_accounts % 10000 == 0 {
                spinner.set_position(token_accounts);
            }

            // Filter for token accounts
            if account.account_meta.owner != token_program {
                continue;
            }
            if account.data.len() != TOKEN_ACCOUNT_LEN {
                continue;
            }

            // Parse token account
            let mint = Pubkey::try_from(&account.data[0..32]).unwrap();
            let token_owner = Pubkey::try_from(&account.data[32..64]).unwrap();
            let amount = u64::from_le_bytes(account.data[64..72].try_into().unwrap());

            // Check if this is the canonical ATA PDA
            let (expected_ata, _bump) = Pubkey::find_program_address(
                &[
                    token_owner.as_ref(),
                    token_program.as_ref(),
                    mint.as_ref(),
                ],
                &ata_program,
            );
            let is_pda = account.meta.pubkey == expected_ata;

            compressor.add(TokenAccountData {
                pubkey: account.meta.pubkey.to_bytes(),
                owner: token_owner.to_bytes(),
                mint: mint.to_bytes(),
                amount,
                is_pda,
            });

            token_accounts += 1;
        }
    }

    spinner.finish();

    info!(
        "Processed {} token accounts from {} total accounts",
        token_accounts, total_accounts
    );

    info!("Persisting to: {}", output_path);
    compressor.persist(output_path)?;

    info!("Done! Saved {} token accounts", compressor.len());

    Ok(())
}
