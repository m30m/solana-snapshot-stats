use crate::token::{ASSOCIATED_TOKEN_PROGRAM_ID, TOKEN_ACCOUNT_LEN, TOKEN_PROGRAM_ID};
use solana_sdk::pubkey::Pubkey;
use solana_snapshot_etl::append_vec::StoredAccountMeta;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::str::FromStr;
use wincode::{SchemaRead, SchemaWrite};

#[repr(C)]
#[derive(Debug, Clone, Copy, SchemaRead, SchemaWrite)]
pub enum COption<T> {
    None,
    Some(T),
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

#[repr(C)]
#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct Pubkey(pub [u8; 32]);


enum TokenAccountPubkey {
    Pda,
    Custom(usize),
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct TokenAccountDataCompressed {
    pub pubkey: TokenAccountPubkey,
    pub mint: usize,
    pub owner: usize,
    pub amount: u64,
    pub delegate: COption<usize>,
    pub state: AccountState,
    pub is_native: COption<u64>,
    pub delegated_amount: u64,
    pub close_authority: COption<usize>,
}

/// Token account data matching SPL Token's binary layout (165 bytes)
#[repr(C)]
#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct TokenAccountData {
    pub mint: Pubkey,
    pub owner: Pubkey,
    pub amount: u64,
    pub delegate: COption<Pubkey>,
    pub state: AccountState,
    pub is_native: COption<u64>,
    pub delegated_amount: u64,
    pub close_authority: COption<Pubkey>,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Default)]
pub struct TokenAccountCompressorState {
    pub pubkey_list: Vec<Pubkey>,
    pub accounts: Vec<TokenAccountDataCompressed>,
}

pub struct TokenAccountCompressor {
    state: TokenAccountCompressorState,
    pubkey_position: HashMap<Pubkey, usize>,
    token_program: Pubkey,
    ata_program: Pubkey,
}


impl TokenAccountCompressor {
    fn get_pubkey_position(&self, pubkey: &Pubkey) -> usize {
        self.pubkey_position.get(pubkey).unwrap_or_else(|| {
            let position = self.state.pubkey_list.len();
            self.state.pubkey_list.push(pubkey.clone());
            self.pubkey_position.insert(pubkey.clone(), position);
            position
        })
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
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let state: TokenAccountCompressorState = wincode::deserialize(&bytes)?;
        Ok(Self {
            state,
            pubkey_position: state.pubkey_list.iter().enumerate().collect(),
            token_program: Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(),
            ata_program: Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID).unwrap(),
        })
    }

    fn persist<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let bytes = wincode::serialize(&self.state)?;
        writer.write_all(&bytes)?;
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

        // Convert byte arrays to Pubkey for PDA calculation
        let owner_pubkey = Pubkey::from(token_account.owner);
        let mint_pubkey = Pubkey::from(token_account.mint);

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

        self.state.accounts.push(TokenAccountDataCompressed {
            pubkey: if is_pda {
                TokenAccountPubkey::Pda
            } else {
                TokenAccountPubkey::Custom(self.get_pubkey_position(&account.meta.pubkey))
            },
            owner: self.get_pubkey_position(&token_account.owner),
            mint: self.get_pubkey_position(&token_account.mint),
            amount: token_account.amount,
            delegate: token_account.delegate.map(|delegate| self.get_pubkey_position(&delegate)),
            state: token_account.state,
            is_native: token_account.is_native,
            delegated_amount: token_account.delegated_amount,
            close_authority: token_account.close_authority.map(|close_authority| self.get_pubkey_position(&close_authority)),
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
