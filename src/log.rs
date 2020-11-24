// #![allow(dead_code)]
// #![allow(unused_variables)]
// #![allow(unused_imports)]

use std::{
    self,
    mem,
    io::{Read, Write, Seek, SeekFrom, ErrorKind},
    fs::{self, File, OpenOptions},
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
};
use time::OffsetDateTime;
use serde::{
    Serialize,
    Deserialize,
    de::DeserializeOwned
};
use serde_json;

use crate::error::*;


fn meta_file_path(dirname: &Path) -> PathBuf {
    let mut path = PathBuf::from(dirname);
    path.push("logparts");
    path
}


// ~~~~~ Entry ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Serialize, Deserialize, Debug)]
pub enum Entry<K, V> {
    Set(K, V),
    Remove(K),
}


// ~~~~~ LogPartition ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogPartition {
    entry_count: u16,
    file_id: u128,
}


impl LogPartition {

    fn new(dirname: &Path) -> Result<(LogPartition, File)> {
        // TODO: more defensive to limit the number of iterations?
        loop {
            let file_id = OffsetDateTime::now_utc().timestamp_nanos() as u128;
            let name = LogPartition::build_file_name(file_id);
            let mut path = PathBuf::from(dirname);
            path.push(&name);
            let fh = OpenOptions::new().write(true).create_new(true).open(path);
            match fh {
                Ok(f) => {
                    return Ok((LogPartition { entry_count: 0, file_id, }, f));
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

    fn build_file_name(file_id: u128) -> String {
        format!("{:x}.dblog", file_id)
    }

    fn file_name(&self) -> String {
        LogPartition::build_file_name(self.file_id)
    }

    fn full_path(&self, dirname: &Path) -> PathBuf {
        let mut path = PathBuf::from(dirname);
        path.push(self.file_name());
        path
    }

    // fn iter<'de, I: Deserialize<'de>>(&self, dirname: &Path) -> LogPartitionIter<'de, I> {
    //     LogPartitionIter::new(self.full_path(dirname))
    // }

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

    fn current_offset(&self) -> usize {
        self.iter.byte_offset()
    }

}


impl<'de, I: Deserialize<'de>> Iterator for LogPartitionIter<'de, I> {
    type Item = I;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|item| item.unwrap())
    }

}


// struct LogPartitionReverseIter<I> {
//     iter: ::std::vec::IntoIter<I>,
// }
//
//
// impl<'de, I: Deserialize<'de>> LogPartitionReverseIter<I> {
//
//     fn new(path: PathBuf) -> LogPartitionReverseIter<I> {
//         let fh = OpenOptions::new().read(true).create(false).open(&path).unwrap();
//         let deserializer = serde_json::Deserializer::from_reader(fh);
//         let records: Vec<I> = deserializer.into_iter().map(|r| r.unwrap()).collect();
//         LogPartitionReverseIter { iter: records.into_iter() }
//     }
//
// }
//
//
// impl<'de, I: Deserialize<'de>> Iterator for LogPartitionReverseIter<I> {
//     type Item = I;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next_back()
//     }
//
// }


// ~~~~~ LogPointer ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Debug)]
pub struct LogPointer {
    file_id: u128,
    offset: u64,
    len: u64,
}


impl LogPointer {
    pub fn len(&self) -> u64 { self.len }
    pub fn offset(&self) -> u64 { self.offset }
}


// ~~~~~ Log ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#[derive(Serialize, Deserialize, Debug)]
pub struct Log {
    pub dirname: PathBuf,
    pub active: LogPartition,
    pub hist: Vec<LogPartition>,
    #[serde(skip)]
    pub fh: Option<File>,
    #[serde(skip)]
    hist_map: HashMap<u128, usize>,
}


impl Log {

    pub fn open(dirname: &Path) -> Result<Log> {
        // load the meta data for the log
        let meta_path = meta_file_path(dirname);
        match &meta_path.exists() {
            true => {
                // deserialize the Log struct
                let fh = OpenOptions::new().read(true).create(false).open(meta_path)?;
                let mut log: Log = serde_json::from_reader(fh)?;
                // open the active partition file
                let path = log.active.full_path(&dirname);
                log.fh = Some(OpenOptions::new().read(true).append(true).create(false).open(path)?);
                for (i, p) in log.hist.iter().enumerate() {
                    log.hist_map.insert(p.file_id, i);
                }
                Ok(log)
            },
            false => {
                // initialize a new partition
                let (active_part, fh) = LogPartition::new(dirname)?;
                // initialize the Log struct
                let log = Log {
                    dirname: PathBuf::from(dirname),
                    active: active_part,
                    hist: vec![],
                    fh: Some(fh),
                    hist_map: HashMap::new(),
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

    pub fn append<K, V>(&mut self, entry: &Entry<K, V>) -> Result<LogPointer>
        where
            K: Sized + Serialize,
            V: Sized + Serialize,
    {
        if self.active.entry_count == u16::MAX {
            self.initialize_new_active()?;
        }
        let mut fh = self.fh.as_ref().ok_or(KvsError::InvalidLogFileHandle)?;
        let offset = fh.seek(SeekFrom::End(0))?;
        serde_json::to_writer(fh, &entry)?;
        let len = fh.seek(SeekFrom::Current(0))? - offset;
        self.active.entry_count += 1;
        Ok(LogPointer {
            file_id: self.active.file_id,
            offset,
            len,
        })
    }

    pub fn retrieve<K, V>(&self, lp: &LogPointer) -> Result<Entry<K, V>>
        where
            K: Sized + DeserializeOwned,
            V: Sized + DeserializeOwned,
    {
        let partition = self.hist_map.get(&lp.file_id).map_or(&self.active, |i| &self.hist[*i]);
        let mut fh = File::open(partition.full_path(&self.dirname))?;
        fh.seek(SeekFrom::Start(lp.offset))?;
        let handle = fh.take(lp.len);
        serde_json::from_reader(handle).map_err(|err| KvsError::from(err))
    }

    pub fn iter<'de, I: Deserialize<'de>>(&'de self) -> LogIter<'de, I> {
        LogIter::new(self)
    }

    pub fn compact<'a, I: Iterator<Item = (&'a K, &'a LogPointer)>, K: 'a>(&mut self, records: I) -> Result<()> {
        let (compact_active, compact_fh) = LogPartition::new(&self.dirname)?;
        // backup the current state
        let current_active = mem::replace(&mut self.active, compact_active);
        let current_hist = mem::replace(&mut self.hist, vec![]);
        let current_hist_map = mem::replace(&mut self.hist_map, HashMap::new());
        let current_fh = mem::replace(&mut self.fh, Some(compact_fh));
        let mut result = Ok(());
        let mut partition = &current_active;
        let mut fh = File::open(partition.full_path(&self.dirname))?;
        for (_key, lp) in records {
            if lp.file_id != partition.file_id {
                partition = current_hist_map.get(&lp.file_id).map_or(&current_active, |i| &current_hist[*i]);
                assert_eq!(lp.file_id, partition.file_id);
                fh = File::open(partition.full_path(&self.dirname))?;
            }
            fh.seek(SeekFrom::Start(lp.offset))?;
            let mut entry = vec![0_u8; lp.len() as usize];
            fh.read_exact(&mut entry[..])?;
            assert_eq!(entry.len(), lp.len as usize, "could not read from partition");

            if let Err(err) = self.append_bytes(&entry) {
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
                self.active = current_active;
                self.hist = current_hist;
                self.fh = current_fh;
                self.hist_map = current_hist_map;
            },
        }
        result
    }

    fn initialize_new_active(&mut self) -> Result<()> {
        let (active, fh) = LogPartition::new(&self.dirname)?;
        let i = self.hist.len();
        self.hist_map.insert(self.active.file_id, i);
        self.hist.push(mem::replace(&mut self.active, active));
        self.fh = Some(fh);
        Ok(())
    }

    fn append_bytes(&mut self, entry: &[u8]) -> Result<LogPointer> {
        // eprintln!("Log::append()");
        if self.active.entry_count == u16::MAX {
            self.initialize_new_active()?;
        }
        let mut fh = self.fh.as_ref().ok_or(KvsError::InvalidLogFileHandle)?;
        let offset = fh.seek(SeekFrom::End(0))?;
        fh.write_all(entry)?;
        let len = fh.seek(SeekFrom::Current(0))? - offset;
        self.active.entry_count += 1;
        Ok(LogPointer {
            file_id: self.active.file_id,
            offset,
            len,
        })
    }

    fn dump_meta(&mut self) -> Result<()> {
        let fh = OpenOptions::new().write(true).create(true).open(&meta_file_path(&self.dirname))?;
        serde_json::to_writer(fh, self)?;
        Ok(())
    }

}


// Make sure the meta data for the Log is written to disk
impl Drop for Log {
    fn drop(&mut self) {
        self.dump_meta().unwrap()
    }
}


pub struct LogIter<'de, I> {
    dirname: PathBuf,
    partitions: VecDeque<&'de LogPartition>,
    current_iterator: Option<LogPartitionIter<'de, I>>,
    current_file_id: u128,
}


impl<'de, I: Deserialize<'de>> LogIter<'de, I> {
    fn new(log: &'de Log) -> LogIter<'de, I> {
        let mut partitions: VecDeque<&LogPartition> = log.hist.iter().collect();
        partitions.push_back(&log.active);
        LogIter {
            dirname: log.dirname.clone(),
            partitions,
            current_iterator: None,
            current_file_id: 0,
        }
    }
}


impl<'de, I: Deserialize<'de>> Iterator for LogIter<'de, I> {
    type Item = (I, LogPointer);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_iterator.is_none() {
            match self.partitions.pop_front() {
                Some(partition) => {
                    self.current_file_id = partition.file_id;
                    self.current_iterator = Some(LogPartitionIter::new(partition.full_path(&self.dirname)));
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
                let offset = it.current_offset();
                match it.next() {
                    Some(entry) => {
                        let len =  it.current_offset() - offset;
                        self.current_iterator = iterator;
                        Some((entry, LogPointer {
                            file_id: self.current_file_id,
                            offset: offset as u64,
                            len: len as u64,
                        }))
                    },
                    None => {
                        self.next()
                    }
                }
            },
            None => None,
        }
    }

}
