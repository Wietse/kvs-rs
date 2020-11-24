// #![allow(dead_code)]
// #![allow(unused_variables)]
// #![allow(unused_imports)]

use std::{
    self,
    collections::HashMap,
    path::Path,
};


pub mod error;
pub mod log;

pub use error::*;
use log::{Entry, Log, LogPointer};


type KvsEntry = Entry<String, String>;


const COMPACTION_FACTOR: usize = 2;


#[derive(Debug)]
pub struct KvStore {
    log: Log,
    index: HashMap<String, LogPointer>,
}


impl KvStore {

    pub fn open<P: AsRef<Path>>(dirname: P) -> Result<KvStore> {
        // eprintln!("KvsStore::open()");
        let mut store = KvStore {
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
        let log_pointer = self.log.append(&entry)?;
        self.index.insert(key, log_pointer);
        self.maybe_compact()?;
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(lp) => {
                match self.log.retrieve(lp)? {
                    KvsEntry::Set(_key, value) => Ok(Some(value)),
                    _ => Err(KvsError::KeyNotFound),
                }
            },
            None => Ok(None),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.get(key.clone())? {
            Some(_) => {
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
                (KvsEntry::Set(k, _v), lp) => { self.index.insert(k.to_owned(), lp); },
                (KvsEntry::Remove(k), _lp) => { self.index.remove(&k); },
            }
        }
        // eprintln!("loaded index: {:?}", self.index);
        Ok(())
    }

    fn maybe_compact(&mut self) -> Result<()> {
        if self.log.hist.len() > 2 {
            let orig_entries = self.log.len();
            if orig_entries > COMPACTION_FACTOR * self.len() {
                self.log.compact(self.index.iter())?;
                println!("Compacted from {} entries to {}", orig_entries, self.log.len());
            }
            // rebuild the index
            self.load_index()?;
        }
        Ok(())
    }

}
