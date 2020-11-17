#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::{
    self,
    mem,
    fmt::Display,
    io::{Write, Seek, SeekFrom, ErrorKind},
    fs::{self, File, OpenOptions},
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use serde::{Serialize, Deserialize};
use serde_json;

use crate::error::*;


// ~~~~~ Entry ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Serialize, Deserialize, Debug)]
pub enum Entry<K, V> {
    Set(K, V),
    Remove(K),
}


// ~~~~~ LogPartition ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogPartition {
    gen: u64,
    entry_count: u16,
    file_id: u128,
}


impl LogPartition {

    fn new(dirname: &Path, gen: u64) -> Result<(LogPartition, File)> {
        // TODO: this *could* turn into an endless loop, more defensive to limit the number of
        //       iterations?
        loop {
            let file_id = OffsetDateTime::now_utc().timestamp_nanos() as u128;
            let name = LogPartition::build_file_name(gen, file_id);
            let mut path = PathBuf::from(dirname);
            path.push(&name);
            let fh = OpenOptions::new().write(true).create_new(true).open(path);
            match fh {
                Ok(f) => {
                    return Ok((
                            LogPartition { gen, entry_count: 0, file_id, },
                            f,
                    ));
                }
                Err(err) => {
                    match err.kind() {
                        ErrorKind::AlreadyExists => {},
                        _ => { return Err(KvsError::from(err)); },
                    }
                }
            }
        }
    }

    fn build_file_name(gen: u64, file_id: u128) -> String {
        format!("{:x}-{:x}.dblog", gen, file_id)
    }

    fn file_name(&self) -> String {
        LogPartition::build_file_name(self.gen, self.file_id)
    }

    fn full_path(&self, dirname: &Path) -> PathBuf {
        let mut path = PathBuf::from(dirname);
        path.push(self.file_name());
        path
    }

    fn iter<'de, I: Deserialize<'de>>(&self, dirname: &Path) -> LogPartitionIter<'de, I> {
        LogPartitionIter::new(self.full_path(dirname))
    }

}


struct LogPartitionIter<'de, I> {
    iter: serde_json::StreamDeserializer<'de, serde_json::de::IoRead<File>, I>,
}


impl<'de, I: Deserialize<'de>> LogPartitionIter<'de, I> {

    fn new(path: PathBuf) -> LogPartitionIter<'de, I> {
        let fh = OpenOptions::new().read(true).create(false).open(&path).unwrap();
        let deserializer = serde_json::Deserializer::from_reader(fh);
        LogPartitionIter { iter: deserializer.into_iter::<I>() }
    }

}


impl<'de, I: Deserialize<'de>> Iterator for LogPartitionIter<'de, I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| item.unwrap())
    }

}


struct LogPartitionReverseIter<I> {
    iter: ::std::vec::IntoIter<I>,
}


impl<'de, I: Deserialize<'de>> LogPartitionReverseIter<I> {

    fn new(path: PathBuf) -> LogPartitionReverseIter<I> {
        let fh = OpenOptions::new().read(true).create(false).open(&path).unwrap();
        let deserializer = serde_json::Deserializer::from_reader(fh);
        let records: Vec<I> = deserializer.into_iter().map(|r| r.unwrap()).collect();
        LogPartitionReverseIter { iter: records.into_iter() }
    }

}


impl<'de, I: Deserialize<'de>> Iterator for LogPartitionReverseIter<I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }

}


// ~~~~~ Log ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Serialize, Deserialize, Debug)]
pub struct Log {
    pub dirname: PathBuf,
    pub gen: u64,
    pub active: LogPartition,
    pub hist: Vec<LogPartition>,
    #[serde(skip)]
    pub fh: Option<File>,
}


impl Log {

    pub fn open(dirname: &Path) -> Result<Log> {
        // load the meta data for the log
        let mut meta_path = PathBuf::from(dirname);
        meta_path.push("logparts");
        match &meta_path.exists() {
            true => {
                // deserialize the Log struct
                let fh = OpenOptions::new().read(true).create(false).open(meta_path)?;
                let mut log: Log = serde_json::from_reader(fh)?;
                // open the active partition file
                let mut path = PathBuf::from(dirname);
                path.push(log.active.file_name());
                log.fh = Some(OpenOptions::new().read(true).append(true).create(false).open(path)?);
                Ok(log)
            },
            false => {
                // initialize a new partition
                let (active_part, fh) = LogPartition::new(dirname, 1)?;
                // initialize the Log struct
                let log = Log {
                    dirname: PathBuf::from(dirname),
                    gen: 1,
                    active: active_part,
                    hist: vec![],
                    fh: Some(fh),
                };
                // write the Log struct's meta data to disk
                let fh = OpenOptions::new().write(true).create_new(true).open(meta_path)?;
                serde_json::to_writer(fh, &log)?;
                Ok(log)
            },
        }
    }

    pub fn len(&self) -> usize {
        let mut sum: usize = self.hist.iter().map(|p| p.entry_count as usize).sum();
        sum += self.active.entry_count as usize;
        sum
    }

    pub fn append<T: ?Sized + Serialize>(&mut self, entry: &T) -> Result<u64> {
        // eprintln!("Log::append()");
        if self.active.entry_count == u16::MAX {
            self.hist.push(self.active.clone());
            let (active, fh) = LogPartition::new(&self.dirname, self.gen)?;
            self.active = active;
            self.fh = Some(fh);
        }
        let mut fh = self.fh.as_ref().ok_or(KvsError::InvalidLogFileHandle)?;
        fh.seek(SeekFrom::End(0))?;
        let offset = fh.seek(SeekFrom::Current(0))?;
        serde_json::to_writer(fh, &entry)?;
        self.active.entry_count += 1;
        Ok(offset)
    }

    pub fn iter<'de, I: Deserialize<'de>>(&'de self) -> LogIter<'de, I> {
        LogIter::new(self)
    }

    pub fn compact<I: Iterator<Item = E>, E: Serialize>(&mut self, records: I) -> Result<()> {
        let compact_gen = self.gen + 1;
        let (compact_active, compact_fh) = LogPartition::new(&self.dirname, compact_gen)?;
        // backup the current state
        let current_gen = mem::replace(&mut self.gen, compact_gen);
        let current_active = mem::replace(&mut self.active, compact_active);
        let current_hist = mem::replace(&mut self.hist, vec![]);
        let current_fh = mem::replace(&mut self.fh, Some(compact_fh));
        let mut result = Ok(());
        for entry in records {
            if let Err(err) = self.append(&entry) {
                result = Err(err);
                break;
            }
        }
        // cleanup
        match result {
            Ok(_) => {
                for partition in &current_hist {
                    fs::remove_file(partition.full_path(&self.dirname))?;
                }
                fs::remove_file(current_active.full_path(&self.dirname))?;
            },
            Err(_) => {
                for partition in &self.hist {
                    fs::remove_file(partition.full_path(&self.dirname))?;
                }
                fs::remove_file(self.active.full_path(&self.dirname))?;
                // rollback to the backed up state
                self.gen = current_gen;
                self.active = current_active;
                self.hist = current_hist;
                self.fh = current_fh;
            },
        }
        result
    }

    fn meta_file_path(&self) -> PathBuf {
        let mut path = PathBuf::from(&self.dirname);
        path.push("logparts");
        path
    }

    fn dump_meta(&mut self) {
        // TODO: error handling
        let fh = OpenOptions::new().write(true).create(true).open(&self.meta_file_path()).unwrap();
        serde_json::to_writer(fh, self).unwrap();
    }

}


// Make sure the meta data for the Log is written to disk
impl Drop for Log {
    fn drop(&mut self) {
        self.dump_meta()
    }
}


pub struct LogIter<'de, I> {
    dirname: PathBuf,
    partitions: VecDeque<&'de LogPartition>,
    current_iterator: Option<LogPartitionIter<'de, I>>,
}


impl<'de, I: Deserialize<'de>> LogIter<'de, I> {
    fn new(log: &'de Log) -> LogIter<I> {
        let mut partitions: VecDeque<&LogPartition> = log.hist.iter().collect();
        partitions.push_back(&log.active);
        LogIter {
            dirname: log.dirname.clone(),
            partitions,
            current_iterator: None,
        }
    }
}


impl<'de, I: Deserialize<'de>> Iterator for LogIter<'de, I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_iterator.is_none() {
            match self.partitions.pop_front() {
                Some(partition) => {
                    let mut path = PathBuf::from(&self.dirname);
                    path.push(partition.file_name());
                    self.current_iterator = Some(LogPartitionIter::new(path));
                },
                None => {
                    return None;
                },
            }
        }
        // at this point self.current_iterator cannot be None
        let mut iterator = self.current_iterator.take();
        match iterator {
            Some(ref mut it) => {
                match it.next() {
                    Some(entry) => {
                        self.current_iterator = iterator;
                        Some(entry)
                    },
                    None => {
                        self.next()
                    }
                }
            },
            None => None,
        }
    }

    // TODO: implement the reverse iterator
}
