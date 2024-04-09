use std::path::Path;

use store::Store;

fn main() {
    // TODO: Turn this into a server that accepts requests
    let mut store = Store::new(Path::new("stuff"), false);

    store.put(50000000, "hello".as_bytes());
    for i in 0..=1000000 {
        store.put(i, &(i + 1).to_ne_bytes());
    }

    store.compact();
    let returned = String::from_utf8(store.get(&50000000).unwrap()).unwrap();
    dbg!(returned);
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde::{Deserialize, Serialize};

    use store::Store;

    // TODO: Move this const to some shared place. Some cargo thing possibly?
    const TEMP_TEST_FILE_DIR: &str = "./tmp_test_files/";

    #[test]
    fn you_can_serialize_and_stuff() {
        let test_dir = TEMP_TEST_FILE_DIR.to_string() + "json";

        #[derive(Debug, Deserialize, Serialize, PartialEq)]
        struct Thing {
            x: u32,
            string: String,
        }

        let thing = Thing {
            x: 5,
            string: "Hello, storage!".to_string(),
        };

        let json = serde_json::to_string(&thing).unwrap();

        let mut store = Store::new(Path::new(&test_dir), false);
        let key = 1;
        store.put(key, &json.as_bytes());

        for i in 5..10 {
            store.put(i, "Some stuff here and that".as_bytes());
        }

        store.put(key, &json.as_bytes());

        let bytes = store.get(&key).unwrap();
        let stored_json = std::str::from_utf8(&bytes).unwrap();
        let parsed_thing: Thing = serde_json::from_str(&stored_json).unwrap();
        assert_eq!(stored_json, json);
        assert_eq!(thing, parsed_thing);
    }
}
