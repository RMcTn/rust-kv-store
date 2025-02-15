use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufReader, BufWriter, Read, Write},
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

type FileOffset = usize;

const STORE_FILENAME_SUFFIX: &str = ".store.kv";

const WRITE_AHEAD_LOG_FILENAME: &str = "write_ahead_log.txt";

type StoreData = HashMap<Vec<u8>, StoreEntry>;
type StoreIndexes = HashMap<u64, StoreData>; // file id to store data index

pub struct Store {
    current_file_id: u64,
    dir: PathBuf,
    pub mem_table_size_limit_in_bytes: u64,
    // TODO: FIXME: Anything stored in the active mem table will be lost if the process crashes.
    //      Build up a write ahead log that gets cleared every time the mem table gets flushed to
    //      make fully durable
    active_mem_table: BTreeMap<Vec<u8>, TableEntry>,
    // TODO: Sparse index for keys in the store. Since the keys are
    //     sorted, we only need to keep a subset of keys indexed. We can scan for the key in the
    //     file if it isn't indexed already (keys are sorted, so we know at least what key the
    //     requested key comes AFTER)
    store_indexes: StoreIndexes,
    bytes_written_since_last_flush: u64,
    wal_writer: BufWriter<File>,
}

#[derive(Clone, Debug)]
enum TableEntry {
    Tombstone,
    Populated(Vec<u8>),
}

#[derive(Debug, PartialEq)]
struct StoreEntry {
    value_size: usize,
    key_size: usize,
    byte_offset: FileOffset,
    file_id: u64,
    // TODO: Checksum for key + value
}

#[derive(Debug, PartialEq)]
struct KeyValue {
    value_size: u32,
    key_size: u32,
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Store {
    pub fn new(dir_path: &Path, keep_existing_dir: bool) -> Self {
        if !keep_existing_dir {
            if let Err(e) = fs::remove_dir_all(dir_path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    panic!("{}", e);
                }
            }
        }
        fs::create_dir_all(dir_path).unwrap();

        let store_info = Store::build_store_from_dir(dir_path);

        // If the WAL exists, that is unpersisted data from most recent writes, so data from
        // from the WAL should overwrite existing store info
        // Remember - Those writes havent been persisted yet, so there is no associated file ID
        // TODO: Test for recovering from WAL
        let recovered_wal_kvs =
            Store::restore_from_wal(dir_path.join(WRITE_AHEAD_LOG_FILENAME)).unwrap();

        let write_ahead_log_file = fs::File::options()
            .write(true)
            .create(true)
            .open(dir_path.join(WRITE_AHEAD_LOG_FILENAME))
            .unwrap();

        let mut store = Store {
            current_file_id: store_info.1,
            dir: dir_path.to_path_buf(),
            mem_table_size_limit_in_bytes: 1024 * 1024 * 5,
            active_mem_table: BTreeMap::new(),
            store_indexes: store_info.0,
            bytes_written_since_last_flush: 0,
            wal_writer: BufWriter::new(write_ahead_log_file),
        };

        for kv in &recovered_wal_kvs {
            store.put_into_memory(&kv.key, &kv.value);
        }

        return store;
    }

    pub fn flush_pending_writes(&mut self) {
        self.write_mem_table_to_disk();
        self.truncate_wal().unwrap();
    }

    fn truncate_wal(&mut self) -> std::io::Result<()> {
        let temp_log_filename = self.dir.join(WRITE_AHEAD_LOG_FILENAME.to_owned() + ".temp");
        let _log_file = fs::File::options()
            .create(true)
            .write(true)
            .open(&temp_log_filename)
            .unwrap();

        std::fs::rename(temp_log_filename, self.dir.join(WRITE_AHEAD_LOG_FILENAME))
    }

    /// Write a key value pair into memory, with no durability
    fn put_into_memory(&mut self, key: &[u8], value: &[u8]) {
        let key_len = key.len();
        self.active_mem_table
            .insert(key.to_vec(), TableEntry::Populated(value.to_owned()));
        self.bytes_written_since_last_flush += (key_len + value.len()) as u64;
    }

    // Stores value with key. User is responsible for serializing/deserializing
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        Self::append_kv_to_file(
            &mut self.wal_writer,
            key.len() as u32,
            key,
            value.len() as u32,
            Some(value),
        );
        self.put_into_memory(key, value);
        if self.bytes_written_since_last_flush > self.mem_table_size_limit_in_bytes {
            // TODO: Handle ongoing writes as we persist the mem table in the background
            self.flush_pending_writes();
        }
    }

    /// Returns how many bytes were written in total
    fn append_kv_to_file(
        writer: &mut BufWriter<File>,
        key_size: u32,
        key: &[u8],
        value_size: u32,
        value: Option<&[u8]>,
    ) -> usize {
        // TODO: Make sure this ALWAYS appends and doesn't just write wherever
        let key_size_bytes = key_size.to_le_bytes();
        writer.write_all(&key_size_bytes).unwrap();
        writer.write_all(&key).unwrap();
        let value_size_bytes = value_size.to_le_bytes();
        writer.write_all(&value_size_bytes).unwrap();

        if let Some(value) = value {
            writer.write_all(value).unwrap();
        }
        writer.flush().unwrap();
        return key_size_bytes.len() + key.len() + value_size_bytes.len() + value_size as usize;
    }

    fn file_path_for_file_id(file_id: u64, dir_path: &Path) -> PathBuf {
        let filename = Self::filename_for_file_id(file_id);
        let file_path = dir_path.join(&filename);
        return file_path;
    }

    fn filename_for_file_id(file_id: u64) -> String {
        let filename = file_id.to_string() + STORE_FILENAME_SUFFIX;
        return filename;
    }

    fn create_store_file(file_id: u64, dir_path: &Path) -> File {
        // TODO: Return errors
        let file_path = Self::file_path_for_file_id(file_id, dir_path);

        // Always open the file like we want to keep it. If we're asked to wipe the dir, then there
        // should be no files here anyway
        let file = fs::File::options()
            .append(true)
            .create(true)
            .read(true)
            .open(&file_path)
            .unwrap();
        return file;
    }

    fn increment_file_id(&mut self) {
        self.current_file_id += 1;
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.active_mem_table.get(key).cloned() {
            Some(table_entry) => match table_entry {
                TableEntry::Tombstone => return None,
                TableEntry::Populated(v) => Some(v),
            },
            None => {
                for file_id in (0..=self.current_file_id).rev() {
                    // Check our store files for the value
                    if let Some(index) = self.store_indexes.get(&file_id) {
                        if let Some(entry) = index.get(key) {
                            let value_offset_in_file = entry.byte_offset + 4 + entry.key_size + 4; // Skip everything until the actual value
                            let mut buffer: Vec<u8> = vec![0; entry.value_size];
                            self.read_from_store_file(file_id, &mut buffer, value_offset_in_file);
                            if buffer.is_empty() {
                                // TODO: Is there a valid use case for having an empty value for a key? Assuming it is
                                // the tombstone for now
                                return None;
                            }
                            return Some(buffer);
                        }
                    } else {
                        // No index here means we've probably reached beyond our active store
                        // files
                        return None;
                    }
                }
                return None;
            }
        }
    }

    fn read_from_store_file(&self, file_id: u64, buffer: &mut [u8], offset: usize) {
        let path = Self::file_path_for_file_id(file_id, &self.dir);
        let file = File::open(path).unwrap();
        file.read_exact_at(buffer, offset as u64).unwrap();
    }

    fn create_fresh_store_file(&mut self) -> File {
        self.increment_file_id();
        Self::create_store_file(self.current_file_id, &self.dir)
    }

    /// Assumes mem table keys are sorted!
    fn write_mem_table_to_disk(&mut self) {
        let file = self.create_fresh_store_file();

        let mut writer = BufWriter::new(file);

        let mut file_offset = 0;
        self.store_indexes
            .insert(self.current_file_id, StoreData::new());
        let store_index = self.store_indexes.get_mut(&self.current_file_id).unwrap();
        for (key, value) in self.active_mem_table.iter() {
            let (value, value_size) = match value {
                TableEntry::Tombstone => (None, 0),
                TableEntry::Populated(v) => (Some(v.as_slice()), v.len() as u32),
            };
            let key_size = key.len();
            let bytes_written =
                Self::append_kv_to_file(&mut writer, key_size as u32, &key, value_size, value);
            let entry = StoreEntry {
                value_size: value_size as usize,
                key_size: key_size as usize,
                byte_offset: file_offset,
                file_id: self.current_file_id,
            };
            store_index.insert(key.clone(), entry);
            file_offset += bytes_written;
        }
        self.active_mem_table.clear();
        writer.flush().unwrap();
        self.bytes_written_since_last_flush = 0;
    }

    pub fn remove(&mut self, key: &[u8]) {
        // No value after key is our "tombstone" for now - Not a great idea if we ever wanted to
        // checksum rows for corruption/crash recovery. No value = No bytes = Nothing to use as a
        // tombstone checksum(?)
        // TODO: Cleanup duplication with regular "store" method"

        // TODO: FIXME: We'll don't persist the mem table with deletes
        // TODO: FIXME: REFACTOR: We don't increase the mem table bytes written on removes
        Self::append_kv_to_file(&mut self.wal_writer, key.len() as u32, key, 0, None);
        self.active_mem_table
            .insert(key.to_owned(), TableEntry::Tombstone);
    }

    fn is_store_file(path: &PathBuf) -> bool {
        path.file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(STORE_FILENAME_SUFFIX)
    }

    // TODO: This name feels a bit misleading since it's just the "data" we're building up
    fn build_store_from_dir(dir_path: &Path) -> (StoreIndexes, u64) {
        let mut entries = Self::get_store_files(dir_path);

        entries.sort();
        // TODO: Assuming that the directory only holds good data, and no bad files (such as
        // malformed file names).

        let mut highest_file_id = 1;
        let mut store_index = HashMap::new();
        for entry in entries {
            let filename = entry.strip_prefix(dir_path).unwrap();
            let current_file_id = Store::file_id_from_path(filename);

            if current_file_id > highest_file_id {
                // This feels like an unnecessary check since the files should be sorted, but
                // better safe than sorry
                highest_file_id = current_file_id;
            }

            store_index.insert(current_file_id, StoreData::new());
            let store_data = store_index.get_mut(&current_file_id).unwrap();

            Self::parse_store_file_into_store_data(&dir_path, current_file_id, store_data);
        }
        return (store_index, highest_file_id);
    }

    /// Will increment byte_offset by:
    ///     key_size value (4 bytes)
    ///     key (key_size bytes)
    ///     value_size value (4 bytes)
    ///     value (value_size bytes)
    fn parse_key_value_from_bytes(byte_offset: &mut FileOffset, bytes: &[u8]) -> KeyValue {
        let key_size =
            u32::from_le_bytes(bytes[*byte_offset..(*byte_offset + 4)].try_into().unwrap());
        *byte_offset += 4;

        let key = bytes[*byte_offset..(*byte_offset + key_size as usize)].to_vec();
        *byte_offset += key_size as usize;
        let value_size =
            u32::from_le_bytes(bytes[*byte_offset..(*byte_offset + 4)].try_into().unwrap());
        *byte_offset += 4;
        let value = bytes[*byte_offset..(*byte_offset + value_size as usize)].to_vec();
        *byte_offset += value_size as usize;

        let kv = KeyValue {
            value_size,
            key_size,
            key,
            value,
        };
        return kv;
    }

    fn parse_file_into_kv(
        dir_path: &Path,
        file_id: u64,
        key_values: &mut HashMap<Vec<u8>, Vec<u8>>,
    ) {
        let path_to_open = Self::file_path_for_file_id(file_id, dir_path);
        let mut file = File::open(path_to_open).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let mut byte_offset = 0;
        loop {
            let kv = Store::parse_key_value_from_bytes(&mut byte_offset, &buffer);
            key_values.insert(kv.key, kv.value);
            if byte_offset >= buffer.len() {
                break;
            }
        }
    }

    fn parse_store_file_into_store_data(dir_path: &Path, file_id: u64, store_data: &mut StoreData) {
        let path_to_open = Self::file_path_for_file_id(file_id, dir_path);
        let mut file = File::open(path_to_open).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let mut byte_offset = 0;

        loop {
            if byte_offset >= buffer.len() {
                break;
            }
            let byte_offset_for_key = byte_offset;
            let kv = Store::parse_key_value_from_bytes(&mut byte_offset, &buffer);
            let entry = StoreEntry {
                value_size: kv.value_size as usize,
                key_size: kv.key_size as usize,
                byte_offset: byte_offset_for_key,
                file_id,
            };

            store_data.insert(kv.key, entry);
        }
    }

    fn file_id_from_path(filename: &Path) -> u64 {
        let filename = filename.file_name().unwrap().to_string_lossy();
        let filename_sections: Vec<_> = filename.split(".").collect();
        let file_id = filename_sections[0].parse().unwrap();
        return file_id;
    }

    fn get_store_files(dir: &Path) -> Vec<PathBuf> {
        // TODO: NOTE: Assuming the filepaths here are all perfect for the program for now
        fs::read_dir(dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()).unwrap())
            .filter(|path| Self::is_store_file(&path))
            .collect::<Vec<_>>()
    }

    pub fn compact(&mut self) {
        // TODO: Background thread!

        let mut files_for_compaction = Self::get_store_files(&self.dir);

        // TODO: Is there a limit to compaction for files? i.e a certain length? Ignoring for
        // now!

        if files_for_compaction.is_empty() {
            return;
        }

        files_for_compaction
            .sort_by(|a, b| Self::file_id_from_path(a).cmp(&Self::file_id_from_path(b)));

        let mut compacted_kvs = HashMap::new();
        for entry in &files_for_compaction {
            let filename = entry.strip_prefix(&self.dir).unwrap();
            let file_id = Self::file_id_from_path(filename);
            Self::parse_file_into_kv(&self.dir, file_id, &mut compacted_kvs);
        }

        let compaction_file_id = self.current_file_id;
        let compaction_sacrifice_filename = Self::filename_for_file_id(compaction_file_id);
        let compaction_sacrifice_file_path = self.dir.join(&compaction_sacrifice_filename);
        let compaction_filename = "temp.".to_string() + &compaction_sacrifice_filename;
        let compaction_file_path = self.dir.join(&compaction_filename);
        let compaction_file = File::create(&compaction_file_path).unwrap();

        let mut compaction_file = BufWriter::new(compaction_file);

        let mut mapping_entries = HashMap::new();
        let mut file_offset = 0;
        for (k, v) in &compacted_kvs {
            let (entry, bytes_written) =
                Self::create_entry(&mut compaction_file, &k, v, file_offset, compaction_file_id);

            file_offset += bytes_written;

            mapping_entries.insert(k.to_owned(), entry);
        }

        std::fs::rename(compaction_file_path, compaction_sacrifice_file_path).unwrap();
        self.store_indexes
            .insert(compaction_file_id, mapping_entries);

        // Ensure we don't delete our newly compacted file as well!
        files_for_compaction.remove(files_for_compaction.len() - 1);
        for entry in &files_for_compaction {
            fs::remove_file(entry).unwrap();
        }
    }

    /// Returns the Entry created for key and value, and how many bytes were written to file
    fn create_entry(
        writer: &mut BufWriter<File>,
        key: &[u8],
        value: &[u8],
        file_offset: FileOffset,
        file_id: u64,
    ) -> (StoreEntry, usize) {
        let value_size = value.len() as u32;
        let key_size = key.len();
        let bytes_written =
            Store::append_kv_to_file(writer, key_size as u32, key, value_size, Some(value));
        let entry = StoreEntry {
            value_size: value_size as usize,
            key_size: key_size as usize, // TODO: Use the actual key's size once it's not just a u32
            byte_offset: file_offset,
            file_id,
        };
        return (entry, bytes_written);
    }

    fn restore_from_wal(write_ahead_log_path: PathBuf) -> Result<Vec<KeyValue>, std::io::Error> {
        let mut write_ahead_log_file =
            match fs::File::options().read(true).open(write_ahead_log_path) {
                Ok(file) => file,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        // No WAL, nothing to restore!
                        return Ok(vec![]);
                    }
                    _ => return Err(e),
                },
            };

        let mut buffer = Vec::new();
        write_ahead_log_file.read_to_end(&mut buffer).unwrap();
        let mut byte_offset = 0;

        let mut recovered_kvs = Vec::new();

        loop {
            if byte_offset >= buffer.len() {
                break;
            }
            let kv = Store::parse_key_value_from_bytes(&mut byte_offset, &buffer);
            recovered_kvs.push(kv);
        }

        return Ok(recovered_kvs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMP_TEST_FILE_DIR: &str = "./tmp_test_files/";

    #[test]
    fn it_stores_and_retreives() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "stores_and_retrieves";
        let mut store = Store::new(Path::new(&test_dir), false);
        let test_key = 50_u32.to_ne_bytes();
        assert_eq!(store.get(&test_key), None);

        store.put(&test_key, "100".as_bytes());
        assert_eq!(store.get(&test_key).unwrap(), 100.to_string().as_bytes());
        store.put(&test_key, "101".as_bytes());
        assert_eq!(store.get(&test_key).unwrap(), 101.to_string().as_bytes());
    }

    #[test]
    fn it_deletes() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "deletes";
        let mut store = Store::new(Path::new(&test_dir), false);
        let test_key = 50_u32.to_ne_bytes();
        store.put(&test_key, "100".as_bytes());

        store.remove(&test_key);
        assert_eq!(store.get(&test_key), None);
    }

    #[test]
    fn it_persists() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "persists";
        let mut store = Store::new(Path::new(&test_dir), false);
        let deleted_test_key = 50_u32.to_ne_bytes();
        let other_test_key = "Longer key".as_bytes();
        store.put(&deleted_test_key, "100".as_bytes());
        store.remove(&deleted_test_key);

        store.put(&other_test_key, "1000".as_bytes());
        store.remove(&other_test_key);
        store.put(&other_test_key, "2000".as_bytes());

        store.flush_pending_writes();

        let store = Store::new(Path::new(&test_dir), true);

        assert_eq!(store.get(&deleted_test_key), None);
        let val = store.get(&other_test_key).unwrap();
        let expected_val = 2000.to_string();
        let expected_val = expected_val.as_bytes();
        assert_eq!(val, expected_val);
    }

    #[test]
    fn it_stores_and_retrieves_using_entries() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "entries-store";
        let mut store = Store::new(Path::new(&test_dir), false);
        let key = 1_u32.to_ne_bytes();
        let value = "2".as_bytes();
        store.put(&key, value);

        let bytes = store.get(&key).unwrap();
        assert_eq!(bytes, value);
        let key = 500_u32.to_ne_bytes();
        let value = "5000000".as_bytes();
        store.put(&key, value);
        let bytes = store.get(&key).unwrap();
        assert_eq!(bytes, value);
    }

    #[test]
    fn it_creates_a_new_file_after_crossing_mem_table_size_limit() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "mutliple-files";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.mem_table_size_limit_in_bytes = 1;
        assert_eq!(store.current_file_id, 1);
        let key = 1_u32.to_ne_bytes();
        let value = "2".as_bytes();
        store.put(&key, value);

        let key = 500_u32.to_ne_bytes();
        let value = "5000000".as_bytes();
        store.put(&key, value);

        assert_eq!(store.current_file_id, 3);
        let store_files = Store::get_store_files(&PathBuf::from(&test_dir));
        assert_eq!(store_files.len(), 2);
    }

    #[test]
    fn it_reads_from_across_files() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "mutliple-files-reading";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.mem_table_size_limit_in_bytes = 1;
        assert_eq!(store.current_file_id, 1);

        let key = "Longer key".as_bytes();
        let test_value = "10".as_bytes();
        store.put(&key, test_value);
        store.put(&2_u32.to_ne_bytes(), "20".as_bytes());
        store.put(&3_u32.to_ne_bytes(), "30".as_bytes());

        let result = store.get(&key).unwrap();

        assert_eq!(result, test_value);
    }

    #[test]
    fn it_compacts_old_files_into_a_merged_file() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "file-compaction/untouched-active-file";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.put(&1_u32.to_ne_bytes(), "10".as_bytes());
        store.put(&1_u32.to_ne_bytes(), "1010".as_bytes());
        store.put(&2_u32.to_ne_bytes(), "20".as_bytes());
        store.put(&2_u32.to_ne_bytes(), "2020".as_bytes());
        store.remove(&2_u32.to_ne_bytes());
        store.flush_pending_writes();
        store.put(&3_u32.to_ne_bytes(), "old".as_bytes());
        store.put(&1_u32.to_ne_bytes(), "101010".as_bytes());
        store.flush_pending_writes();
        assert_eq!(store.current_file_id, 3);

        store.put(&3_u32.to_ne_bytes(), "new".as_bytes());

        store.compact();
        let store_files = Store::get_store_files(&PathBuf::from(&test_dir));

        let expected_store_files = 1;
        let actual_store_files = store_files.len();

        assert_eq!(expected_store_files, actual_store_files);
        assert_eq!(
            store.get(&3_u32.to_ne_bytes()),
            Some("new".as_bytes().to_vec())
        );
    }

    #[test]
    fn compaction_will_squash_multiple_of_same_key_into_latest_value() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "file-compaction/remove-duplicates";
        let mut store = Store::new(Path::new(&test_dir), false);
        let key_one = "Arbitrary string".as_bytes();
        let key_two = 2_u32.to_ne_bytes();
        let key_three = 3_u32.to_ne_bytes();
        store.put(&key_one, "10".as_bytes());
        store.put(&key_one, "1010".as_bytes());
        store.put(&key_two, "20".as_bytes());
        store.put(&key_two, "2020".as_bytes());
        store.remove(&key_two);
        store.put(&key_two, "202020".as_bytes());
        store.flush_pending_writes();
        store.put(&key_one, "101010".as_bytes());
        store.flush_pending_writes();
        assert_eq!(store.current_file_id, 3);
        store.put(&key_three, "new".as_bytes());

        store.compact();

        assert_eq!(store.get(&key_one), Some("101010".as_bytes().to_vec()));
        assert_eq!(store.get(&key_two), Some("202020".as_bytes().to_vec()));
    }

    #[test]
    fn mem_table_tombstones_removed_values() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "tombstone/mem-table-tombstone";
        let mut store = Store::new(Path::new(&test_dir), false);
        let key_to_remove = 1_u32.to_ne_bytes();
        store.put(&key_to_remove, "10".as_bytes());
        store.flush_pending_writes();
        assert_eq!(store.get(&key_to_remove), Some("10".as_bytes().to_vec()));
        store.remove(&key_to_remove);
        assert_eq!(store.get(&key_to_remove), None);
    }
    // TODO: Some tombstone tests
}
