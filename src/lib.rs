// TODO: "Log" Compaction
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Seek, Write},
    os::unix::prelude::FileExt,
};

type FileOffset = usize;

const DEFAULT_STORE_FILENAME: &str = "store.kv";

pub struct Store {
    store: File,
    data: HashMap<u32, Entry>,
    file_offset: usize,
}

#[derive(Debug, PartialEq)]
struct Entry {
    value_size: usize,
    key_size: usize,
    byte_offset_for_key: FileOffset,
}

impl Store {
    pub fn new(filename: Option<&str>, keep_existing_file: bool) -> Self {
        let mut file = if keep_existing_file {
            fs::File::options()
                .append(true)
                .create(true)
                .read(true)
                .open(filename.unwrap_or(DEFAULT_STORE_FILENAME))
                .unwrap()
        } else {
            fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(filename.unwrap_or(DEFAULT_STORE_FILENAME))
                .unwrap()
        };
        let file_size = file.metadata().unwrap().len() as usize;
        let data = Store::build_store_from_file(&mut file);

        return Store {
            store: file,
            data,
            file_offset: file_size,
        };
    }

    /// Does not escape any characters
    pub fn put(&mut self, key: u32, value: &str) {
        // The plan is for now: We store string, and we read string. That is it. Users can serialize
        // and deserialize as needed.
        let row = format!("{},{}\n", key, value);
        let bytes = row.as_bytes();
        let row_size = row.as_bytes().len();
        // TODO: Make sure this ALWAYS appends and doesn't just write wherever
        self.store.write_all(bytes).unwrap();
        let key_str = key.to_string();
        // TODO: calculate size of the value once we go generic
        let entry = Entry {
            value_size: value.len(),
            key_size: key_str.len(),
            byte_offset_for_key: self.file_offset,
        };
        self.data.insert(key, entry);
        self.file_offset += row_size;
    }

    pub fn get(&mut self, key: &u32) -> Option<Vec<u8>> {
        // TODO: Handle this unwrap
        let entry = self.data.get(key)?;
        let mut buffer: Vec<u8> = vec![0; entry.value_size];

        let separator_byte_size = 1;
        let value_offset_in_file = entry.byte_offset_for_key + entry.key_size + separator_byte_size; // - 1 since we
                                                                                                     // start at 0
                                                                                                     // We're storing the key offset, so need to skip over the size of the key, and the size of
                                                                                                     // the separator ","

        self.store
            .read_exact_at(&mut buffer, value_offset_in_file as u64)
            .unwrap();
        if buffer.is_empty() {
            // Is there a valid use case for having an empty value for a key? Assuming it is
            // the tombstone for now
            return None;
        }
        return Some(buffer);
    }

    fn build_store_from_file(file: &mut File) -> HashMap<u32, Entry> {
        let mut data = HashMap::new();
        // Go through the entire file
        let mut buffer = String::new();
        // Need to rewind back before any writes until we only read file on startup
        file.rewind().unwrap();
        file.read_to_string(&mut buffer).unwrap();
        let mut byte_offset = 0;
        for line in buffer.lines() {
            let splits: Vec<_> = line.split_terminator(",").collect();
            let key_size = splits[0].len();
            let key: u32 = splits[0].parse().unwrap();
            let value_size = if splits.len() == 1 {
                // Assume our tombstone is just "nothing" after a comma for now(?)
                0
            } else {
                splits[1].len()
            };
            // debug_assert_eq!(splits.len(), 2);
            let entry = Entry {
                value_size,
                key_size,
                byte_offset_for_key: byte_offset,
            };
            byte_offset += line.len() + 1; // 1 for newline
            data.insert(key, entry);
        }
        return data;
    }

    pub fn remove(&mut self, key: u32) {
        // No value after key is our "tombstone" for now
        let row = format!("{},\n", key);
        // TODO: Cleanup duplication with regular "store" method"
        let bytes = row.as_bytes();
        let row_size = row.as_bytes().len();
        self.store.write_all(bytes).unwrap();
        let key_str = key.to_string();
        let entry = Entry {
            value_size: 0,
            key_size: key_str.len(),
            byte_offset_for_key: self.file_offset,
        };
        self.data.insert(key, entry);
        self.file_offset += row_size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEMP_TEST_FILE_DIR: &str = "./tmp_test_files/";

    #[test]
    fn it_stores_and_retreives() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "stores_and_retrieves.kv";
        let mut store = Store::new(Some(&test_filename), false);
        let test_key = 50;
        assert_eq!(store.get(&test_key), None);

        store.put(test_key, "100");
        assert_eq!(store.get(&test_key).unwrap(), 100.to_string().as_bytes());
        store.put(test_key, "101");
        assert_eq!(store.get(&test_key).unwrap(), 101.to_string().as_bytes());

        store.put(test_key + 1, "101");
        assert_eq!(
            store.get(&(test_key + 1)).unwrap(),
            101.to_string().as_bytes()
        );
    }

    #[test]
    fn it_deletes() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "deletes.kv";
        let mut store = Store::new(Some(&test_filename), false);
        let test_key = 50;
        store.put(test_key, "100");

        store.remove(test_key);
        assert_eq!(store.get(&test_key), None);
    }

    #[test]
    fn it_persists() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "persists.kv";
        let mut store = Store::new(Some(&test_filename), false);
        let deleted_test_key = 50;
        let other_test_key = 999;
        store.put(deleted_test_key, "100");
        store.remove(deleted_test_key);

        store.put(other_test_key, "1000");
        store.remove(other_test_key);
        store.put(other_test_key, "2000");

        let mut store = Store::new(Some(&test_filename), true);

        assert_eq!(store.get(&deleted_test_key), None);
        let val = store.get(&other_test_key).unwrap();
        let expected_val = 2000.to_string();
        let expected_val = expected_val.as_bytes();
        assert_eq!(val, expected_val);
    }

    #[test]
    fn it_stores_and_retrieves_using_entries() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "entries-store.kv";
        let mut store = Store::new(Some(&test_filename), false);
        assert_eq!(store.file_offset, 0);
        let key = 1;
        let value = "2";
        store.put(key, value);

        let bytes = store.get(&key).unwrap();
        assert_eq!(bytes, value.to_string().as_bytes());
        let key = 500;
        let value = "5000000";
        store.put(key, value);
        let bytes = store.get(&key).unwrap();
        assert_eq!(bytes, value.to_string().as_bytes());
    }
    // TODO: Some tombstone tests
}
