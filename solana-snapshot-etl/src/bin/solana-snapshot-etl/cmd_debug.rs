use crate::loader::SupportedLoader;
use crate::token::{TOKEN_ACCOUNT_LEN, TOKEN_PROGRAM_ID};
use log::info;
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec_iter;
use solana_snapshot_etl::SnapshotExtractor;
use std::rc::Rc;
use std::str::FromStr;

pub fn run(
    loader: &mut SupportedLoader,
    owner_filter: Pubkey,
    max_count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
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
            if account.account_meta.owner == token_program
                && account.data.len() == TOKEN_ACCOUNT_LEN
            {
                print_token_account(account.data);
            } else {
                // Print first 64 bytes of data as hex
                let preview_len = account.data.len().min(64);
                if preview_len > 0 {
                    println!(
                        "Data (first {} bytes): {:02x?}",
                        preview_len,
                        &account.data[..preview_len]
                    );
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
