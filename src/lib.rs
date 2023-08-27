use std::{
    fs::{self, File},
    io::{Read, Seek, Write},
};

const DEFAULT_STORE_FILENAME: &str = "store.kv";
pub struct Store {
    store: File,
}

impl Store {
    pub fn new(filename: Option<String>, keep_existing_file: bool) -> Self {
        let file = if keep_existing_file {
            fs::File::options()
                .append(true)
                .create(true)
                .read(true)
                .open(filename.unwrap_or(DEFAULT_STORE_FILENAME.to_string()))
                .unwrap()
        } else {
            fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(filename.unwrap_or(DEFAULT_STORE_FILENAME.to_string()))
                .unwrap()
        };
        return Store { store: file };
    }

    /// Does not escape any characters
    pub fn store(&mut self, key: u32, value: u32) {
        let row = format!("{},{}\n", key, value);
        self.store.write_all(row.as_bytes()).unwrap();
    }

    pub fn get(&mut self, key: u32) -> Option<u32> {
        // Go through the entire file
        let mut buffer = String::new();
        // Need to rewind back before any writes until we only read file on startup
        self.store.rewind().unwrap();
        self.store.read_to_string(&mut buffer).unwrap();
        dbg!("About to iterate lines");
        for line in buffer.lines().rev() {
            dbg!(&line);
            let splits: Vec<_> = line.split_terminator(",").collect();
            dbg!(&splits);
            if splits.len() == 1 {
                // Assume our tombstone is just "nothing" after a comma
                return None;
            }
            debug_assert_eq!(splits.len(), 2);
            let parsed_key: u32 = splits[0].parse().unwrap();
            if key == parsed_key {
                return Some(splits[1].parse().unwrap());
            }
        }
        return None;
    }

    pub fn remove(&mut self, key: u32) {
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
        let mut store = Store::new(Some(test_filename), false);
        let test_key = 50;
        assert_eq!(store.get(test_key), None);

        store.store(test_key, 100);
        assert_eq!(store.get(test_key), Some(100));
        store.store(test_key, 101);
        assert_eq!(store.get(test_key), Some(101));

        store.store(test_key + 1, 101);
        assert_eq!(store.get(test_key + 1), Some(101));
    }

    #[test]
    fn it_deletes() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "deletes.kv";
        let mut store = Store::new(Some(test_filename), false);
        let test_key = 50;
        store.store(test_key, 100);

        store.remove(test_key);
        assert_eq!(store.get(test_key), None);
    }
}
