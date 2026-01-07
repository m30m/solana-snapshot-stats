use crate::loader::SupportedLoader;
use crate::token::{
    ASSOCIATED_TOKEN_PROGRAM_ID, MINT_ACCOUNT_LEN, TOKEN_ACCOUNT_LEN, TOKEN_PROGRAM_ID,
};
use duckdb::{params, Connection};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::info;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::SnapshotExtractor;
use std::rc::Rc;
use std::str::FromStr;

pub fn run(loader: &mut SupportedLoader, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap();
    let ata_program = Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).unwrap();

    info!("Opening DuckDB database: {}", db_path);
    let conn = Connection::open(db_path)?;

    // Create tables
    conn.execute_batch(
        "DROP TABLE IF EXISTS token_accounts;
         DROP TABLE IF EXISTS mints;
         CREATE TABLE token_accounts (
             pubkey VARCHAR NOT NULL,
             owner VARCHAR NOT NULL,
             mint VARCHAR NOT NULL,
             amount UBIGINT NOT NULL,
             is_pda BOOLEAN NOT NULL
         );
         CREATE TABLE mints (
             pubkey VARCHAR NOT NULL,
             mint_authority VARCHAR,
             supply UBIGINT NOT NULL,
             decimals UTINYINT NOT NULL,
             is_initialized BOOLEAN NOT NULL,
             freeze_authority VARCHAR
         );",
    )?;

    let mut token_appender = conn.appender("token_accounts")?;
    let mut mint_appender = conn.appender("mints")?;

    let spinner_style = ProgressStyle::with_template(
        "{prefix:>10.bold.dim} {spinner} rate={per_sec}/s total={human_pos}",
    )
    .unwrap();

    let multi = MultiProgress::new();
    let token_spinner = multi.add(
        ProgressBar::new_spinner()
            .with_style(spinner_style.clone())
            .with_prefix("tokens"),
    );
    let mint_spinner = multi.add(
        ProgressBar::new_spinner()
            .with_style(spinner_style)
            .with_prefix("mints"),
    );

    let mut total_accounts: u64 = 0;
    let mut token_accounts: u64 = 0;
    let mut mint_accounts: u64 = 0;

    for append_vec in loader.iter() {
        let append_vec = append_vec?;
        for account in append_vec_iter(Rc::new(append_vec)) {
            let account = account.access().unwrap();
            total_accounts += 1;

            if total_accounts % 10000 == 0 {
                token_spinner.set_position(token_accounts);
                mint_spinner.set_position(mint_accounts);
            }

            // Filter for token program accounts
            if account.account_meta.owner != token_program {
                continue;
            }

            if account.data.len() == TOKEN_ACCOUNT_LEN {
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

                token_appender.append_row(params![
                    account.meta.pubkey.to_string(),
                    token_owner.to_string(),
                    mint.to_string(),
                    amount,
                    is_pda,
                ])?;

                token_accounts += 1;

                // Flush every million records
                if token_accounts % 1_000_000 == 0 {
                    token_appender.flush()?;
                    info!(
                        "Flushed {} token accounts ({} total scanned)",
                        token_accounts, total_accounts
                    );
                }
            } else if account.data.len() == MINT_ACCOUNT_LEN {
                // Parse mint account
                // Layout (82 bytes):
                // - mint_authority: COption<Pubkey> (4 + 32 = 36)
                // - supply: u64 (8)
                // - decimals: u8 (1)
                // - is_initialized: bool (1)
                // - freeze_authority: COption<Pubkey> (4 + 32 = 36)

                let mint_authority_tag = u32::from_le_bytes(account.data[0..4].try_into().unwrap());
                let mint_authority = if mint_authority_tag == 1 {
                    Some(Pubkey::try_from(&account.data[4..36]).unwrap().to_string())
                } else {
                    None
                };

                let supply = u64::from_le_bytes(account.data[36..44].try_into().unwrap());
                let decimals = account.data[44];
                let is_initialized = account.data[45] != 0;

                let freeze_authority_tag =
                    u32::from_le_bytes(account.data[46..50].try_into().unwrap());
                let freeze_authority = if freeze_authority_tag == 1 {
                    Some(Pubkey::try_from(&account.data[50..82]).unwrap().to_string())
                } else {
                    None
                };

                mint_appender.append_row(params![
                    account.meta.pubkey.to_string(),
                    mint_authority,
                    supply,
                    decimals,
                    is_initialized,
                    freeze_authority,
                ])?;

                mint_accounts += 1;

                // Flush every million records
                if mint_accounts % 1_000_000 == 0 {
                    mint_appender.flush()?;
                    info!(
                        "Flushed {} mint accounts ({} total scanned)",
                        mint_accounts, total_accounts
                    );
                }
            }
        }
    }

    token_appender.flush()?;
    mint_appender.flush()?;
    token_spinner.finish();
    mint_spinner.finish();

    info!(
        "Dumped {} token accounts and {} mints from {} total accounts",
        token_accounts, mint_accounts, total_accounts
    );

    Ok(())
}
