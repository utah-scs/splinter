use std::collections::HashMap;

type BS = Vec<u8>;

#[derive(Debug,Default)]
pub struct DB {
    map: HashMap<BS, BS>,
}

impl DB {
    pub fn get<'b>(&self, key: &'b [u8]) -> Option<&[u8]> {
        let v = self.map.get(key);
        match v {
            Some(v) => Some(v.as_slice()),
            None => None,
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let ins;
        {
            let v = self.map.get_mut(key);
            match v {
                Some(v) => {
                    v.clear();
                    v.extend_from_slice(value);
                    ins = false;
                },
                None => {
                    ins = true;
                }
            }
        }
        if ins {
            let mut v = Vec::new();
            v.extend_from_slice(value);
            self.map.insert(key.to_vec(), v);
        }
    }
}

