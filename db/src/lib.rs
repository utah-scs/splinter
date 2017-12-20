extern crate spin;

use std::collections::HashMap;
use spin::{Mutex}; // Use spin lock.

type BS = Vec<u8>;

// Must be a power of two.
const N_BUCKETS : usize = 32;

#[derive(Debug,Default)]
pub struct DB {
    maps: [Mutex<HashMap<BS, BS>>; N_BUCKETS],
}

impl DB {
    pub fn get<'b>(&self, key: &'b [u8]) -> Option<Vec<u8>> {
        let bucket = (key[0] & (N_BUCKETS - 1) as u8) as usize;
        let b = self.maps[bucket].lock();
        b.get(key).map(|v| v.clone())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let bucket = (key[0] & (N_BUCKETS - 1) as u8) as usize;
        let ins;
        {
            if let Some(v) = self.maps[bucket].lock().get_mut(key) {
                v.clear();
                v.extend_from_slice(value);
                ins = false;
            } else {
                ins = true;
            }
        }
        if ins {
            let mut v = Vec::new();
            v.extend_from_slice(value);
            // Danger about not holding lock whole time here. Need to repeat lookup for
            // linearizable behavior (or hold lock at the top).
            self.maps[bucket].lock().insert(key.to_vec(), v);
        }
    }
}

