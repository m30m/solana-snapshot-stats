use crate::token::{ASSOCIATED_TOKEN_PROGRAM_ID, TOKEN_ACCOUNT_LEN, TOKEN_PROGRAM_ID};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::StoredAccountMeta;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::str::FromStr;
use wincode::{SchemaRead, SchemaWrite};

/// COption for PubkeyBytes matching SPL Token's binary layout
#[repr(C)]
#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
pub enum COptionPubkey {
    None,
    Some(PubkeyBytes),
}

/// COption for u64 matching SPL Token's binary layout
#[repr(C)]
#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
pub enum COptionU64 {
    None,
    Some(u64),
}

/// COption for usize (compressed format)
#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub enum COptionUsize {
    None,
    Some(usize),
}

/// Account state matching SPL Token's binary layout
#[repr(u8)]
#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
pub enum AccountState {
    Uninitialized = 0,
    Initialized = 1,
    Frozen = 2,
}

/// Pubkey bytes for wincode serialization
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SchemaRead, SchemaWrite)]
pub struct PubkeyBytes(pub [u8; 32]);

impl Default for PubkeyBytes {
    fn default() -> Self {
        PubkeyBytes([0u8; 32])
    }
}

impl From<Pubkey> for PubkeyBytes {
    fn from(pubkey: Pubkey) -> Self {
        PubkeyBytes(pubkey.to_bytes())
    }
}

impl From<&Pubkey> for PubkeyBytes {
    fn from(pubkey: &Pubkey) -> Self {
        PubkeyBytes(pubkey.to_bytes())
    }
}

impl From<PubkeyBytes> for Pubkey {
    fn from(bytes: PubkeyBytes) -> Self {
        Pubkey::from(bytes.0)
    }
}

impl AsRef<[u8]> for PubkeyBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub trait Compressor: Sized {
    type Account;
    type State: for<'de> SchemaRead<'de, Dst = Self::State> + SchemaWrite<Src = Self::State>;

    fn new() -> Self;
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>>;
    fn persist<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>>;
    /// Add an account. Returns true if the account was accepted, false if skipped.
    fn add(&mut self, account: &StoredAccountMeta) -> bool;
    fn iter(&self) -> impl Iterator<Item = &Self::Account>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub enum TokenAccountPubkey {
    Pda,
    Custom(usize),
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct TokenAccountDataCompressed {
    pub pubkey: TokenAccountPubkey,
    pub mint: usize,
    pub owner: usize,
    pub amount: u64,
    pub delegate: COptionUsize,
    pub state: AccountState,
    pub is_native: COptionU64,
    pub delegated_amount: u64,
    pub close_authority: COptionUsize,
}

/// Token account data matching SPL Token's binary layout (165 bytes)
#[repr(C)]
#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct TokenAccountData {
    pub mint: PubkeyBytes,
    pub owner: PubkeyBytes,
    pub amount: u64,
    pub delegate: COptionPubkey,
    pub state: AccountState,
    pub is_native: COptionU64,
    pub delegated_amount: u64,
    pub close_authority: COptionPubkey,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Default)]
pub struct TokenAccountCompressorState {
    pub pubkey_list: Vec<PubkeyBytes>,
    pub accounts: Vec<TokenAccountDataCompressed>,
}

pub struct TokenAccountCompressor {
    state: TokenAccountCompressorState,
    pubkey_position: HashMap<PubkeyBytes, usize>,
    token_program: Pubkey,
    ata_program: Pubkey,
}

impl TokenAccountCompressor {
    fn get_or_insert_pubkey_position(&mut self, pubkey: PubkeyBytes) -> usize {
        if let Some(&position) = self.pubkey_position.get(&pubkey) {
            position
        } else {
            let position = self.state.pubkey_list.len();
            self.state.pubkey_list.push(pubkey);
            self.pubkey_position.insert(pubkey, position);
            position
        }
    }
}

impl Compressor for TokenAccountCompressor {
    type Account = TokenAccountDataCompressed;
    type State = TokenAccountCompressorState;

    fn new() -> Self {
        Self {
            state: TokenAccountCompressorState::default(),
            pubkey_position: HashMap::new(),
            token_program: Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(),
            ata_program: Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).unwrap(),
        }
    }

    fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let path_str = path.to_string_lossy();

        // Load pubkey_list
        let pubkey_path = format!("{}.pubkeys", path_str);
        let file = File::open(&pubkey_path)?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let pubkey_list: Vec<PubkeyBytes> = wincode::deserialize(&bytes)?;

        // Load accounts
        let accounts_path = format!("{}.accounts", path_str);
        let file = File::open(&accounts_path)?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let accounts: Vec<TokenAccountDataCompressed> = wincode::deserialize(&bytes)?;

        let pubkey_position: HashMap<PubkeyBytes, usize> = pubkey_list
            .iter()
            .enumerate()
            .map(|(i, pk)| (*pk, i))
            .collect();

        Ok(Self {
            state: TokenAccountCompressorState {
                pubkey_list,
                accounts,
            },
            pubkey_position,
            token_program: Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(),
            ata_program: Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).unwrap(),
        })
    }

    fn persist<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let path_str = path.to_string_lossy();

        // Persist pubkey_list
        let pubkey_path = format!("{}.pubkeys", path_str);
        let file = File::create(&pubkey_path)?;
        let mut writer = BufWriter::new(file);
        let bytes = wincode::serialize(&self.state.pubkey_list)?;
        writer.write_all(&bytes)?;
        drop(writer);

        // Persist accounts
        let accounts_path = format!("{}.accounts", path_str);
        let file = File::create(&accounts_path)?;
        let mut writer = BufWriter::new(file);
        let bytes = wincode::serialize(&self.state.accounts)?;
        writer.write_all(&bytes)?;

        println!("pubkey_list size: {}", self.state.pubkey_list.len());

        Ok(())
    }

    fn add(&mut self, account: &StoredAccountMeta) -> bool {
        // Only accept token accounts (165 bytes)
        if account.data.len() != TOKEN_ACCOUNT_LEN {
            return false;
        }

        // Deserialize token account using wincode
        let token_account: TokenAccountData = match wincode::deserialize(account.data) {
            Ok(data) => data,
            Err(_) => return false,
        };

        // Convert to Pubkey for PDA calculation
        let owner_pubkey: Pubkey = token_account.owner.into();
        let mint_pubkey: Pubkey = token_account.mint.into();

        // Check if this is the canonical ATA PDA
        let (expected_ata, _bump) = Pubkey::find_program_address(
            &[
                owner_pubkey.as_ref(),
                self.token_program.as_ref(),
                mint_pubkey.as_ref(),
            ],
            &self.ata_program,
        );
        let is_pda = account.meta.pubkey == expected_ata;

        // Extract all positions before pushing (to avoid borrow checker issues)
        let pubkey_bytes = PubkeyBytes::from(&account.meta.pubkey);
        let pubkey_field = if is_pda {
            TokenAccountPubkey::Pda
        } else {
            TokenAccountPubkey::Custom(self.get_or_insert_pubkey_position(pubkey_bytes))
        };
        let owner_pos = self.get_or_insert_pubkey_position(token_account.owner);
        let mint_pos = self.get_or_insert_pubkey_position(token_account.mint);
        let delegate_pos = match token_account.delegate {
            COptionPubkey::None => COptionUsize::None,
            COptionPubkey::Some(d) => COptionUsize::Some(self.get_or_insert_pubkey_position(d)),
        };
        let close_authority_pos = match token_account.close_authority {
            COptionPubkey::None => COptionUsize::None,
            COptionPubkey::Some(c) => COptionUsize::Some(self.get_or_insert_pubkey_position(c)),
        };

        self.state.accounts.push(TokenAccountDataCompressed {
            pubkey: pubkey_field,
            owner: owner_pos,
            mint: mint_pos,
            amount: token_account.amount,
            delegate: delegate_pos,
            state: token_account.state,
            is_native: token_account.is_native,
            delegated_amount: token_account.delegated_amount,
            close_authority: close_authority_pos,
        });

        true
    }

    fn iter(&self) -> impl Iterator<Item = &TokenAccountDataCompressed> {
        self.state.accounts.iter()
    }

    fn len(&self) -> usize {
        self.state.accounts.len()
    }
}
