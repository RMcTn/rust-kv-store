use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Read, Seek, Write},
};

const DEFAULT_STORE_FILENAME: &str = "store.kv";
pub struct Store {
    store: File,
    data: HashMap<u32, u32>,
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
        let data = Store::build_store_from_file(&mut file);

        return Store { store: file, data };
    }

    /// Does not escape any characters
    pub fn store(&mut self, key: u32, value: u32) {
        self.data.insert(key, value);
        let row = format!("{},{}\n", key, value);
        self.store.write_all(row.as_bytes()).unwrap();
    }

    pub fn get(&self, key: &u32) -> Option<&u32> {
        return self.data.get(key);
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
        assert_eq!(store.get(&test_key), None);

        store.store(test_key, 100);
        assert_eq!(store.get(&test_key), Some(&100));
        store.store(test_key, 101);
        assert_eq!(store.get(&test_key), Some(&101));

        store.store(test_key + 1, 101);
        assert_eq!(store.get(&(test_key + 1)), Some(&101));
    }

    #[test]
    fn it_deletes() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "deletes.kv";
        let mut store = Store::new(Some(&test_filename), false);
        let test_key = 50;
        store.store(test_key, 100);

        store.remove(test_key);
        assert_eq!(store.get(&test_key), None);
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

        let store = Store::new(Some(&test_filename), true);

        assert_eq!(store.get(&deleted_test_key), None);
        assert_eq!(store.get(&other_test_key), Some(&2000));
    }
}
