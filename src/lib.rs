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
    // NOTE: TODO: Our naive approach here of just storing stuff in a hashmap doesn't allow us to
    // have arbitrary values
    data: HashMap<u32, u32>,

    data2: HashMap<u32, Entry>,
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

        let data2 = HashMap::new();

        return Store {
            store: file,
            data,
            data2,
            file_offset: file_size,
        };
    }

    /// Does not escape any characters
    pub fn store(&mut self, key: u32, value: u32) {
        self.data.insert(key, value);
        let row = format!("{},{}\n", key, value);
        let bytes = row.as_bytes();
        let row_size = row.as_bytes().len();
        // TODO: Make sure this ALWAYS appends and doesn't just write wherever
        self.store.write_all(bytes).unwrap();
        let value_str = value.to_string();
        let key_str = key.to_string();
        // TODO: calculate size of the value once we go generic
        let entry = Entry {
            value_size: value_str.len(),
            key_size: key_str.len(),
            byte_offset_for_key: self.file_offset,
        };
        self.data2.insert(key, entry);
        self.file_offset += row_size;
    }

    pub fn get2(&mut self, key: &u32) -> Option<Vec<u8>> {
        // TODO: Handle this unwrap
        let entry = self.data2.get(key)?;
        let mut buffer: Vec<u8> = vec![0; entry.value_size];

        let seperator_byte_size = 1;
        let value_offset_in_file =
            entry.byte_offset_for_key + entry.key_size + seperator_byte_size; // - 1 since we
        // start at 0
        // We're storing the key offset, so need to skip over the size of the key, and the size of
        // the separator ","

        self.store
            .read_exact_at(&mut buffer, value_offset_in_file as u64)
            .unwrap();
        return Some(buffer);
    }

    fn build_store_from_file(file: &mut File) -> HashMap<u32, u32> {
        let mut data = HashMap::new();
        // Go through the entire file
        let mut buffer = String::new();
        // Need to rewind back before any writes until we only read file on startup
        file.rewind().unwrap();
        file.read_to_string(&mut buffer).unwrap();
        for line in buffer.lines() {
            let splits: Vec<_> = line.split_terminator(",").collect();
            let key: u32 = splits[0].parse().unwrap();
            if splits.len() == 1 {
                // Assume our tombstone is just "nothing" after a comma
                data.remove(&key);
                continue;
            }
            debug_assert_eq!(splits.len(), 2);
            let value: u32 = splits[1].parse().unwrap();
            data.insert(key, value);
        }
        return data;
    }

    pub fn remove(&mut self, key: u32) {
        self.data.remove(&key);
        // No value after key is our "tombstone" for now
        let row = format!("{},\n", key);
        self.store.write_all(row.as_bytes()).unwrap();
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
        assert_eq!(store.get2(&test_key), None);

        store.store(test_key, 100);
        assert_eq!(store.get2(&test_key).unwrap(), 100.to_string().as_bytes());
        store.store(test_key, 101);
        assert_eq!(store.get2(&test_key).unwrap(), 101.to_string().as_bytes());

        store.store(test_key + 1, 101);
        assert_eq!(store.get2(&(test_key + 1)).unwrap(), 101.to_string().as_bytes());
    }

    #[test]
    fn it_deletes() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "deletes.kv";
        let mut store = Store::new(Some(&test_filename), false);
        let test_key = 50;
        store.store(test_key, 100);

        store.remove(test_key);
        assert_eq!(store.get2(&test_key), None);
    }

    #[test]
    fn it_persists() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "persists.kv";
        let mut store = Store::new(Some(&test_filename), false);
        let deleted_test_key = 50;
        let other_test_key = 999;
        store.store(deleted_test_key, 100);
        store.remove(deleted_test_key);

        store.store(other_test_key, 1000);
        store.remove(other_test_key);
        store.store(other_test_key, 2000);

        let mut store = Store::new(Some(&test_filename), true);

        assert_eq!(store.get2(&deleted_test_key), None);
        assert_eq!(store.get2(&other_test_key).unwrap(), 2000.to_string().as_bytes());
    }

    #[test]
    fn it_stores_and_retrieves_using_entries() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "entries-store.kv";
        let mut store = Store::new(Some(&test_filename), false);
        assert_eq!(store.file_offset, 0);
        let key = 1;
        let value = 2;
        store.store(key, value);

        let bytes = store.get2(&key).unwrap();
        assert_eq!(bytes, value.to_string().as_bytes());
        let key = 500;
        let value = 5000000;
        store.store(key, value);
        let bytes = store.get2(&key).unwrap();
        assert_eq!(bytes, value.to_string().as_bytes());
    }
}
