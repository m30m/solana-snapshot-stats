use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use wincode::{SchemaRead, SchemaWrite};

pub trait Compressor: Sized {
    type Account;
    type State: for<'de> SchemaRead<'de, Dst = Self::State> + SchemaWrite<Src = Self::State>;

    fn new() -> Self;
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>>;
    fn persist<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>>;
    fn add(&mut self, account: Self::Account);
    fn iter(&self) -> impl Iterator<Item = &Self::Account>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite)]
pub struct TokenAccountData {
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub mint: [u8; 32],
    pub amount: u64,
    pub is_pda: bool,
}

#[derive(Debug, Clone, SchemaRead, SchemaWrite, Default)]
pub struct TokenAccountCompressorState {
    pub accounts: Vec<TokenAccountData>,
}

pub struct TokenAccountCompressor {
    state: TokenAccountCompressorState,
}

impl Compressor for TokenAccountCompressor {
    type Account = TokenAccountData;
    type State = TokenAccountCompressorState;

    fn new() -> Self {
        Self {
            state: TokenAccountCompressorState::default(),
        }
    }

    fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        let state: TokenAccountCompressorState = wincode::deserialize(&bytes)?;
        Ok(Self { state })
    }

    fn persist<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let bytes = wincode::serialize(&self.state)?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    fn add(&mut self, account: TokenAccountData) {
        self.state.accounts.push(account);
    }

    fn iter(&self) -> impl Iterator<Item = &TokenAccountData> {
        self.state.accounts.iter()
    }

    fn len(&self) -> usize {
        self.state.accounts.len()
    }
}
