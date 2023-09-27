use append_key_value_store::Store;

fn main() {
    // TODO: Turn this into a server that accepts requests
    let mut store = Store::new(None, true);

    for i in 0..=1000000 {
        store.put(i, &(i + 1).to_string());
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use append_key_value_store::Store;

    // TODO: Move this const to some shared place. Some cargo thing possibly?
    const TEMP_TEST_FILE_DIR: &str = "./tmp_test_files/";

    #[test]
    fn you_can_serialize_and_stuff() {
        let test_filename = TEMP_TEST_FILE_DIR.to_string() + "json.kv";

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

        let mut store = Store::new(Some(&test_filename), false);
        let key = 1;
        store.put(key, &json);

        for i in 5..10 {
            store.put(i, "Some stuff here and that");
        }

        store.put(key, &json);

        let bytes = store.get(&key).unwrap();
        let stored_json = std::str::from_utf8(&bytes).unwrap();
        let parsed_thing: Thing = serde_json::from_str(&stored_json).unwrap();
        assert_eq!(stored_json, json);
        assert_eq!(thing, parsed_thing);
    }
}
