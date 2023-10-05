// TODO: "Log" Compaction
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Read, Seek, Write},
    os::unix::prelude::FileExt,
    path::Path,
};

type FileOffset = usize;

const STORE_FILENAME_SUFFIX: &str = ".store.kv";

pub struct Store {
    writer: BufWriter<File>,
    reader: File,
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
    pub fn new(dir_path: &Path, keep_existing_file: bool) -> Self {
        fs::create_dir_all(&dir_path).unwrap();
        let filename = "1".to_string() + STORE_FILENAME_SUFFIX;
        let mut file_path = dir_path.to_path_buf();
        file_path.push(&filename);

        let mut file = if keep_existing_file {
            fs::File::options()
                .append(true)
                .create(true)
                .read(true)
                .open(&file_path)
                .unwrap()
        } else {
            fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&file_path)
                .unwrap()
        };
        let file_size = file.metadata().unwrap().len() as usize;
        let data = Store::build_store_from_file(&mut file);

        let writer = BufWriter::new(file.try_clone().unwrap());

        return Store {
            writer,
            reader: file,
            data,
            file_offset: file_size,
        };
    }

    // Stores value with key. User is responsible for serializing/deserializing
    pub fn put(&mut self, key: u32, value: &[u8]) {
        // TODO: Make key a byte slice as well

        // TODO: Going to change it from providing a filename for the store to providing a
        // directory for the store. We can then keep a incrementing counter for file id that is
        // used for filenames too (inside that dir anyway)
        // Steps here:
        // Have keystore load from a directory (we'll default a file name for now). - DONE
        // Name file with incrementing file id prefix.
        // Start writing multiple files after some "limit" is passed for each file (say 5 writes
        // for testing or something).
        // Load up multiple files in the store dir (latest file id will be from counting)
        // Compaction + Merging

        let key_and_sep = format!("{},", key);

        let bytes = key_and_sep.as_bytes();
        let key_and_sep_size = key_and_sep.as_bytes().len();
        // TODO: Make sure this ALWAYS appends and doesn't just write wherever
        self.writer.write_all(bytes).unwrap();
        self.writer.write_all(value).unwrap();
        self.writer.write_all("\n".as_bytes()).unwrap();
        self.writer.flush().unwrap();
        let key_str = key.to_string();
        let entry = Entry {
            // TODO: Start storing what file the entry is stored in, so we can have separate files
            // for compaction
            value_size: value.len(),
            key_size: key_str.len(),
            byte_offset_for_key: self.file_offset,
        };
        self.data.insert(key, entry);
        let newline_size = 1;
        self.file_offset += key_and_sep_size + value.len() + newline_size;
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

        // TODO: Might just need to open the file we get from the Entry value here for reading.
        // Don't think we can keep the files open constantly
        self.reader
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
        // TODO: Build store from files
        let mut data = HashMap::new();
        // Go through the entire file
        let mut buffer = String::new();
        // Need to rewind back before any writes until we only read file on startup
        file.rewind().unwrap();
        file.read_to_string(&mut buffer).unwrap();
        let mut byte_offset = 0;
        // SPEEDUP: Store the size of the key + values so we don't need to parse them, we can just
        // read as raw bytes
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
        self.writer.write_all(bytes).unwrap();
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
        assert_eq!(store.file_offset, 0);
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
    // TODO: Some tombstone tests
}
