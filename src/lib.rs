#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::{
    self,
    fmt::Display,
    io::ErrorKind,
    fs::OpenOptions,
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use serde::{Serialize, Deserialize};
use serde_json;


pub mod error;
pub mod log;

pub use error::*;
use log::{Entry, Log};


type KvsEntry = Entry<String, String>;


const COMPACTION_FACTOR: usize = 2;


#[derive(Debug)]
pub struct KvStore {
    dirname: PathBuf,
    log: Log,
    index: HashMap<String, String>,
}


impl KvStore {

    pub fn open<P: AsRef<Path>>(dirname: P) -> Result<KvStore> {
        // eprintln!("KvsStore::open()");
        let mut store = KvStore {
            dirname: PathBuf::from(dirname.as_ref()),
            log: Log::open(dirname.as_ref())?,
            index: HashMap::new(),
        };
        store.load_index()?;
        // eprintln!("KvsStore::open() -> {:?}", store);
        Ok(store)
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // eprintln!("KvsStore::set()");
        let entry = KvsEntry::Set(key.clone(), value.clone());
        self.log.append(&entry)?;
        self.index.insert(key, value);
        self.maybe_compact()?;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.index.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.get(key.clone())? {
            Some(v) => {
                let entry = KvsEntry::Remove(key.clone());
                self.log.append(&entry)?;
                self.index.remove(&key);
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }

    fn load_index(&mut self) -> Result<()> {
        for item  in self.log.iter::<KvsEntry>() {
            match item {
                KvsEntry::Set(k, v) => { self.index.insert(k.to_owned(), v.to_owned()); },
                KvsEntry::Remove(k) => { self.index.remove(&k); },
            }
        }
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<()> {
        let orig_entries = self.log.len();
        if self.log.len() > COMPACTION_FACTOR * self.len() {
            self.log.compact(self.index.iter().map(|(key, value)| { KvsEntry::Set(key.clone(), value.clone()) }))?;
            println!("Compacted from {} entries to {}", orig_entries, self.log.len());
        }
        Ok(())
    }

}
