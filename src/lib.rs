// TODO: "Log" Compaction
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Read, Write},
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

type FileOffset = usize;

const STORE_FILENAME_SUFFIX: &str = ".store.kv";

type StoreData = HashMap<u32, Entry>;
pub struct Store {
    writer: BufWriter<File>,
    data: StoreData,
    file_offset: usize,
    current_file_id: u64,
    dir: PathBuf,
    file_size_limit_in_bytes: u64,
}

#[derive(Debug, PartialEq)]
struct Entry {
    value_size: usize,
    key_size: usize,
    byte_offset_for_key: FileOffset,
    file_id: u64,
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
        let file_id = 1;
        fs::create_dir_all(dir_path).unwrap();

        let file = Self::create_store_file(file_id, &dir_path);

        let store_info = Store::build_store_from_dir(dir_path);

        let writer = BufWriter::new(file.try_clone().unwrap());

        let file_size = file.metadata().unwrap().len() as usize;
        return Store {
            writer,
            data: store_info.0,
            file_offset: file_size,
            current_file_id: store_info.1,
            dir: dir_path.to_path_buf(),
            file_size_limit_in_bytes: 5000,
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
        // Name file with incrementing file id prefix. - DONE.
        // Start writing multiple files after some "limit" is passed for each file (say 5 writes
        // for testing or something). - DONE.
        // Store the file ID with the entry. - DONE.
        // Load up multiple files in the store dir (latest file id will be from counting)
        // Compaction + Merging

        if self.file_offset as u64 >= self.file_size_limit_in_bytes {
            // NOTE: TODO: The file size limit can be surpassed if the values/keys we write are
            //  large enough, since we don't do the file limit check before the offending key/value
            //  is written.
            self.increment_file_id();
            let new_file = Self::create_store_file(self.current_file_id, &self.dir);
            let writer = BufWriter::new(new_file);
            self.writer = writer;
            self.file_offset = 0;
        }
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
            value_size: value.len(),
            key_size: key_str.len(),
            byte_offset_for_key: self.file_offset,
            file_id: self.current_file_id,
        };
        self.data.insert(key, entry);
        let newline_size = 1;
        self.file_offset += key_and_sep_size + value.len() + newline_size;
    }

    fn file_path_for_file_id(file_id: u64, dir_path: &Path) -> PathBuf {
        let filename = file_id.to_string() + STORE_FILENAME_SUFFIX;
        let mut file_path = dir_path.to_path_buf();
        file_path.push(&filename);
        return file_path;
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
        self.read_from_store_file(entry.file_id, &mut buffer, value_offset_in_file as u64);
        if buffer.is_empty() {
            // Is there a valid use case for having an empty value for a key? Assuming it is
            // the tombstone for now
            return None;
        }
        return Some(buffer);
    }

    fn read_from_store_file(&self, file_id: u64, buffer: &mut [u8], offset: u64) {
        let path = Self::file_path_for_file_id(file_id, &self.dir);
        let file = File::open(path).unwrap();
        file.read_exact_at(buffer, offset as u64).unwrap();
    }

    pub fn remove(&mut self, key: u32) {
        // No value after key is our "tombstone" for now - Not a great idea if we ever wanted to
        // checksum rows for corruption/crash recovery. No value = No bytes = Nothing to use as a
        // tombstone checksum
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
            file_id: self.current_file_id,
        };
        self.data.insert(key, entry);
        self.file_offset += row_size;
    }

    // TODO: This name feels a bit misleading since it's just the "data" we're building up
    fn build_store_from_dir(dir_path: &Path) -> (StoreData, u64) {
        let mut entries = fs::read_dir(dir_path)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        entries.sort();
        // TODO: Assuming that the directory only holds good data, and no bad files (such as
        // malformed file names).

        let mut data = HashMap::new();
        let mut highest_file_id = 1;
        for entry in entries {
            let filename = entry.strip_prefix(dir_path).unwrap();
            let filename = filename.to_str().unwrap();
            let filename_sections: Vec<_> = filename.split(".").collect();
            let current_file_id = filename_sections[0].parse().unwrap();
            if current_file_id > highest_file_id {
                // This feels like an unnecessary check since the files should be sorted, but
                // better safe than sorry
                highest_file_id = current_file_id;
            }

            data = Self::parse_file_into_store_data(&dir_path, current_file_id, data);
        }
        return (data, highest_file_id);
    }

    // TODO: Dear lord, rename this
    fn parse_entry_thing_from_line(
        current_file_id: u64,
        byte_offset: &mut FileOffset,
        line: &str,
    ) -> (u32, Entry) {
        let splits: Vec<_> = line.split_terminator(",").collect();
        let key_size = splits[0].len();
        let key: u32 = splits[0].parse().unwrap();
        let value_size = if splits.len() == 1 {
            // Assume our tombstone is just "nothing" after a comma for now(?)
            0
        } else {
            splits[1].len()
        };
        let entry = Entry {
            value_size,
            key_size,
            byte_offset_for_key: *byte_offset,
            file_id: current_file_id,
        };
        *byte_offset += line.len() + 1; // 1 for newline
        (key, entry)
    }

    fn parse_file_into_store_data(
        dir_path: &Path,
        file_id: u64,
        mut store_data: StoreData,
    ) -> StoreData {
        // Compacting an existing file is the same as just creating store_data from the file
        let path_to_open = Self::file_path_for_file_id(file_id, dir_path);
        let mut file = File::open(path_to_open).unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();
        let mut byte_offset = 0;
        for line in buffer.lines() {
            let record = Self::parse_entry_thing_from_line(file_id, &mut byte_offset, line);
            store_data.insert(record.0, record.1);
        }
        return store_data;
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
    fn it_gets_the_highest_file_id_from_dir_when_creating_store() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "retrieving-highest-file-id";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.file_size_limit_in_bytes = 1;
        store.put(1, "10".as_bytes());
        store.put(2, "20".as_bytes());
        store.put(3, "30".as_bytes());

        assert_eq!(store.current_file_id, 3);

        let new_store = Store::new(Path::new(&test_dir), true);
        assert_eq!(new_store.current_file_id, 3);
    }
    // TODO: Some tombstone tests
}
