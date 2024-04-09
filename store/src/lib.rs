// TODO: "Log" Compaction
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::{BufWriter, Read, Write},
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

type FileOffset = usize;

const STORE_FILENAME_SUFFIX: &str = ".store.kv";

type StoreData = HashMap<u32, Entry>;
type StoreIndexes = HashMap<u64, StoreData>; // file id to store data index

//
//
//
//
// TODO - Make the move to an LSM-Tree. Need to start saving file segments as SSTables, and keep
// a memtable of writes, then persist
//
// Plan:
//  Memtable first (writes happen here)
//  Write out memtable as an SSTable
//  Check memtable for reads
//  Check previous sstable segments for reads after memtable (don't worry about indexing the
//  SSTables right now. Can do later)

pub struct Store {
    current_file_id: u64,
    dir: PathBuf,
    // NOTE: file size limit is not enforced for compacted files
    pub file_size_limit_in_bytes: u64,
    active_mem_table: BTreeMap<u32, Vec<u8>>,
    store_indexes: StoreIndexes,
}

#[derive(Debug, PartialEq)]
struct Entry {
    // TODO: Change these usize's to u32 for now, whilst we sort things out (easier to reason with
    // a hard 4 byte size than usize's variable size)
    value_size: usize,
    key_size: usize,
    byte_offset: FileOffset,
    file_id: u64,
}

#[derive(Debug, PartialEq)]
struct KeyValue {
    value_size: u32,
    key_size: u32,
    key: u32,
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
        return Store {
            current_file_id: store_info.1,
            dir: dir_path.to_path_buf(),
            file_size_limit_in_bytes: 5000,
            active_mem_table: BTreeMap::new(),
            store_indexes: store_info.0,
        };
    }

    pub fn flush(&mut self) {
        self.write_mem_table_to_disk();
    }

    // Stores value with key. User is responsible for serializing/deserializing
    pub fn put(&mut self, key: u32, value: &[u8]) {
        // TODO: Make key a byte slice as well

        self.active_mem_table.insert(key, value.to_owned());
        if self.active_mem_table.len() == 5 {
            // TODO: Keep track of how many bytes the mem table holds for deciding threshold to write
            // to disk
            // TODO: Handle ongoing writes as we persist the mem table in the background
            self.write_mem_table_to_disk();
        }
    }

    /// Returns how many bytes were written in total
    fn append_kv_to_file(
        writer: &mut BufWriter<File>,
        key_size: u32,
        key: u32,
        value_size: u32,
        value: &[u8],
    ) -> usize {
        // TODO: Make sure this ALWAYS appends and doesn't just write wherever
        let key_size_bytes = key_size.to_le_bytes();
        writer.write_all(&key_size_bytes).unwrap();
        let key_bytes = key.to_le_bytes();
        writer.write_all(&key_bytes).unwrap();
        let value_size_bytes = value_size.to_le_bytes();
        writer.write_all(&value_size_bytes).unwrap();
        writer.write_all(value).unwrap();
        writer.flush().unwrap();
        return key_size_bytes.len() + key_bytes.len() + value_size_bytes.len() + value.len();
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

    pub fn get(&self, key: &u32) -> Option<Vec<u8>> {
        // TODO: Handle this unwrap

        match self.active_mem_table.get(key).cloned() {
            Some(v) => return Some(v),
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
            let value_size = value.len() as u32;
            let key_size = 4;
            let bytes_written =
                Self::append_kv_to_file(&mut writer, key_size, *key, value_size, value);
            let entry = Entry {
                value_size: value_size as usize,
                key_size: key_size as usize,
                byte_offset: file_offset,
                file_id: self.current_file_id,
            };
            store_index.insert(*key, entry);
            file_offset += bytes_written;
        }
        self.active_mem_table.clear();
    }

    pub fn remove(&mut self, key: u32) {
        // No value after key is our "tombstone" for now - Not a great idea if we ever wanted to
        // checksum rows for corruption/crash recovery. No value = No bytes = Nothing to use as a
        // tombstone checksum
        // TODO: Cleanup duplication with regular "store" method"

        self.active_mem_table.remove(&key); // TODO: FIXME: This should be a tombstone value, otherwise
                                            // removing a key that has been written to disk, then trying
                                            // to read that key, will result in looking up the value from
                                            // disk (Which is wrong!) - Add a test
    }

    // TODO: This name feels a bit misleading since it's just the "data" we're building up
    fn build_store_from_dir(dir_path: &Path) -> (StoreIndexes, u64) {
        let mut entries = fs::read_dir(dir_path)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        entries.sort();
        // TODO: Assuming that the directory only holds good data, and no bad files (such as
        // malformed file names).

        let mut highest_file_id = 1;
        let mut store_index = HashMap::new();
        for entry in entries {
            let filename = entry.strip_prefix(dir_path).unwrap();
            let current_file_id = Store::file_id_from_filename(filename);

            if current_file_id > highest_file_id {
                // This feels like an unnecessary check since the files should be sorted, but
                // better safe than sorry
                highest_file_id = current_file_id;
            }

            store_index.insert(current_file_id, StoreData::new());
            let store_data = store_index.get_mut(&current_file_id).unwrap();

            Self::parse_file_into_store_data(&dir_path, current_file_id, store_data);
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

        let key = u32::from_le_bytes(bytes[*byte_offset..(*byte_offset + 4)].try_into().unwrap());
        *byte_offset += key_size as usize;
        // TODO: Check this doesn't contain newline
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

    fn parse_file_into_kv(dir_path: &Path, file_id: u64, key_values: &mut HashMap<u32, Vec<u8>>) {
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

    // TODO: Why do we take a mut reference to store data and return an actual struct?
    fn parse_file_into_store_data(dir_path: &Path, file_id: u64, store_data: &mut StoreData) {
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
            let entry = Entry {
                value_size: kv.value_size as usize,
                key_size: kv.key_size as usize,
                byte_offset: byte_offset_for_key,
                file_id,
            };

            store_data.insert(kv.key, entry);
        }
    }

    fn file_id_from_filename(filename: &Path) -> u64 {
        let filename = filename.to_str().unwrap();
        let filename_sections: Vec<_> = filename.split(".").collect();
        let file_id = filename_sections[0].parse().unwrap();
        return file_id;
    }

    pub fn compact(&mut self) {
        // TODO: Background thread!
        let mut store_files = fs::read_dir(&self.dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        store_files.sort(); // FIXME: This sorts as strings, 101 > 1000. Write a test for it as well (might need to be a separate long running packaged test)

        // TODO: NOTE: Assuming the filepaths here are all perfect for the program for now
        let mut files_for_compaction: Vec<_> = store_files.into_iter().collect();
        // TODO: Is there a limit to compaction for files? i.e a certain length? Ignoring for
        // now!

        files_for_compaction.sort();

        let mut compacted_kvs = HashMap::new();
        for entry in &files_for_compaction {
            let filename = entry.strip_prefix(&self.dir).unwrap();
            let file_id = Self::file_id_from_filename(filename);
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
                Self::create_entry(&mut compaction_file, *k, v, file_offset, compaction_file_id);

            file_offset += bytes_written;

            mapping_entries.insert(*k, entry);
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
        key: u32,
        value: &[u8],
        file_offset: FileOffset,
        file_id: u64,
    ) -> (Entry, usize) {
        let value_size = value.len() as u32;
        let key_size = 4;
        let bytes_written = Store::append_kv_to_file(writer, key_size, key, value_size, value);
        let entry = Entry {
            value_size: value_size as usize,
            key_size: key_size as usize, // TODO: Use the actual key's size once it's not just a u32
            byte_offset: file_offset,
            file_id,
        };
        return (entry, bytes_written);
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
        let test_key = 50;
        assert_eq!(store.get(&test_key), None);

        store.put(test_key, "100".as_bytes());
        assert_eq!(store.get(&test_key).unwrap(), 100.to_string().as_bytes());
        store.put(test_key, "101".as_bytes());
        assert_eq!(store.get(&test_key).unwrap(), 101.to_string().as_bytes());

        store.put(test_key + 1, "101".as_bytes());
        assert_eq!(
            store.get(&(test_key + 1)).unwrap(),
            101.to_string().as_bytes()
        );
    }

    #[test]
    fn it_deletes() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "deletes";
        let mut store = Store::new(Path::new(&test_dir), false);
        let test_key = 50;
        store.put(test_key, "100".as_bytes());

        store.remove(test_key);
        assert_eq!(store.get(&test_key), None);
    }

    #[test]
    fn it_persists() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "persists";
        let mut store = Store::new(Path::new(&test_dir), false);
        let deleted_test_key = 50;
        let other_test_key = 999;
        store.put(deleted_test_key, "100".as_bytes());
        store.remove(deleted_test_key);

        store.put(other_test_key, "1000".as_bytes());
        store.remove(other_test_key);
        store.put(other_test_key, "2000".as_bytes());

        store.flush();

        let mut store = Store::new(Path::new(&test_dir), true);

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
        let key = 1;
        let value = "2".as_bytes();
        store.put(key, value);

        let bytes = store.get(&key).unwrap();
        assert_eq!(bytes, value);
        let key = 500;
        let value = "5000000".as_bytes();
        store.put(key, value);
        let bytes = store.get(&key).unwrap();
        assert_eq!(bytes, value);
    }

    #[test]
    fn it_creates_a_new_file_after_crossing_filesize_limit() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "mutliple-files";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.file_size_limit_in_bytes = 1;
        assert_eq!(store.current_file_id, 1);
        let key = 1;
        let value = "2".as_bytes();
        store.put(key, value);

        let key = 500;
        let value = "5000000".as_bytes();
        store.put(key, value);
        assert_eq!(store.current_file_id, 2);
        let files: Vec<_> = fs::read_dir(test_dir).unwrap().collect();
        assert_eq!(files.len(), 2);
    }

    #[test]
    fn it_reads_from_across_files() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "mutliple-files-reading";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.file_size_limit_in_bytes = 1;
        assert_eq!(store.current_file_id, 1);

        let test_value = "10".as_bytes();
        store.put(1, test_value);
        store.put(2, "20".as_bytes());
        store.put(3, "30".as_bytes());

        let result = store.get(&1).unwrap();

        assert_eq!(result, test_value);
    }

    #[test]
    fn it_compacts_old_files_into_a_merged_file_without_touching_active_file() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "file-compaction/untouched-active-file";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.put(1, "10".as_bytes());
        store.put(1, "1010".as_bytes());
        store.put(2, "20".as_bytes());
        store.put(2, "2020".as_bytes());
        store.remove(2);
        store.flush();
        store.put(3, "old".as_bytes());
        store.put(1, "101010".as_bytes());
        store.flush();
        assert_eq!(store.current_file_id, 3);
        store.put(3, "new".as_bytes());

        store.compact();
        let entries = fs::read_dir(test_dir).unwrap();

        let expected_num_files = 2;
        let actual_num_files = entries.count();

        assert_eq!(expected_num_files, actual_num_files);
        assert_eq!(store.get(&3), Some("new".as_bytes().to_vec()));
    }

    #[test]
    fn compaction_will_squash_multiple_of_same_key_into_latest_value() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "file-compaction/remove-duplicates";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.put(1, "10".as_bytes());
        store.put(1, "1010".as_bytes());
        store.put(2, "20".as_bytes());
        store.put(2, "2020".as_bytes());
        store.remove(2);
        store.put(2, "202020".as_bytes());
        store.flush();
        store.put(1, "101010".as_bytes());
        store.flush();
        assert_eq!(store.current_file_id, 3);
        store.put(3, "new".as_bytes());

        store.compact();

        assert_eq!(store.get(&1), Some("101010".as_bytes().to_vec()));
        assert_eq!(store.get(&2), Some("202020".as_bytes().to_vec()));
    }
    // TODO: Some tombstone tests
}
