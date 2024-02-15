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
            self.force_new_file();
        }

        let value_size = value.len() as u32;
        let key_size = 4;
        let bytes_written =
            Store::append_kv_to_file(&mut self.writer, key_size, key, value_size, value);
        let entry = Entry {
            value_size: value_size as usize,
            key_size: key_size as usize, // TODO: Use the actual key's size once it's not just a u32
            byte_offset: self.file_offset,
            file_id: self.current_file_id,
        };
        self.data.insert(key, entry);
        self.file_offset += bytes_written;
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

        let value_offset_in_file = entry.byte_offset + 4 + entry.key_size + 4; // Skip everything until the actual value

        self.read_from_store_file(entry.file_id, &mut buffer, value_offset_in_file);
        if buffer.is_empty() {
            // Is there a valid use case for having an empty value for a key? Assuming it is
            // the tombstone for now
            return None;
        }
        return Some(buffer);
    }

    fn read_from_store_file(&self, file_id: u64, buffer: &mut [u8], offset: usize) {
        let path = Self::file_path_for_file_id(file_id, &self.dir);
        let file = File::open(path).unwrap();
        file.read_exact_at(buffer, offset as u64).unwrap();
    }

    fn force_new_file(&mut self) {
        self.increment_file_id();
        let new_file = Self::create_store_file(self.current_file_id, &self.dir);
        let writer = BufWriter::new(new_file);
        self.writer = writer;
        self.file_offset = 0;
    }

    pub fn remove(&mut self, key: u32) {
        // No value after key is our "tombstone" for now - Not a great idea if we ever wanted to
        // checksum rows for corruption/crash recovery. No value = No bytes = Nothing to use as a
        // tombstone checksum
        // TODO: Cleanup duplication with regular "store" method"

        let key_size = 4;
        let value_size = 0;
        let bytes_written =
            Store::append_kv_to_file(&mut self.writer, key_size, key, value_size, &[]);
        let entry = Entry {
            value_size: value_size as usize,
            key_size: key_size as usize,
            byte_offset: self.file_offset,
            file_id: self.current_file_id,
        };
        self.data.insert(key, entry);
        self.file_offset += bytes_written;
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
            let current_file_id = Store::file_id_from_filename(filename);

            if current_file_id > highest_file_id {
                // This feels like an unnecessary check since the files should be sorted, but
                // better safe than sorry
                highest_file_id = current_file_id;
            }

            data = Self::parse_file_into_store_data(&dir_path, current_file_id, data);
        }
        return (data, highest_file_id);
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

    fn parse_file_into_kv(dir_path: &Path, file_id: u64) -> Vec<KeyValue> {
        let path_to_open = Self::file_path_for_file_id(file_id, dir_path);
        let mut file = File::open(path_to_open).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        let mut byte_offset = 0;
        let mut key_values = Vec::new();
        loop {
            let kv = Store::parse_key_value_from_bytes(&mut byte_offset, &buffer);
            key_values.push(kv);
            if byte_offset >= buffer.len() {
                break;
            }
        }

        return key_values;
    }

    // TODO: Why do we take a mut reference to store data and return an actual struct?
    fn parse_file_into_store_data(
        dir_path: &Path,
        file_id: u64,
        mut store_data: StoreData,
    ) -> StoreData {
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
        return store_data;
    }

    fn file_id_from_filename(filename: &Path) -> u64 {
        let filename = filename.to_str().unwrap();
        let filename_sections: Vec<_> = filename.split(".").collect();
        let file_id = filename_sections[0].parse().unwrap();
        return file_id;
    }

    fn compact(&mut self) {
        let mut entries = fs::read_dir(&self.dir)
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        entries.sort();
        // TODO: Get current_file_id filepath name and just compare with each entry filename,
        // rejecting the one that doesn't match
        let current_file_filepath = Self::file_path_for_file_id(self.current_file_id, &self.dir);
        // TODO: NOTE: Assuming the filepaths here are all perfect for the program for now
        let entries: Vec<_> = entries
            .into_iter()
            .filter(|filepath| {
                return current_file_filepath != *filepath;
            })
            .collect();
        // Merged file will just use the last highest from the files that are going to be merged.
        // TODO: Is there a limit to compaction for files? i.e a certain length? Ignoring for
        // now!

        // Get all files that aren't currently active (something about file id)
        //  - iterate from 1 til current_file id?
        //      - really it's iterate from the previous highest file id
        // Parse each file doing the parse thingy
        // Somehow "compress" these various data stores into one (note:
        // can't we just use the previous data store as the arg to the parsing stuff? and do that
        // for each one, since later files should 'overwrite' any entries that are mentioned in
        // multiple files)
        dbg!(entries);
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

    #[test]
    fn it_compacts_old_files_into_a_merged_file_without_touching_active_file() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "file-compaction";
        let mut store = Store::new(Path::new(&test_dir), false);
        store.put(1, "10".as_bytes());
        store.put(1, "1010".as_bytes());
        store.put(2, "20".as_bytes());
        store.put(2, "2020".as_bytes());
        store.remove(2);
        store.force_new_file();
        store.put(1, "101010".as_bytes());
        store.force_new_file();
        store.put(3, "new".as_bytes());

        store.compact();
        let mut entries = fs::read_dir(test_dir).unwrap();

        let expected_num_files = 2;
        let actual_num_files = entries.count();

        assert_eq!(expected_num_files, actual_num_files);
    }

    #[test]
    fn compaction_will_squash_multiple_of_same_key_into_last_value() {
        todo!()
    }
    // TODO: Some tombstone tests
}
