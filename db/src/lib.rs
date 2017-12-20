// std::sync::RwLock seems to lose to spin and parking_lot, but it's a toss
// up between them for the most part.
extern crate spin;
use spin::{RwLock};
//extern crate parking_lot;
//use parking_lot::{RwLock};

use std::collections::HashMap;


type BS = Vec<u8>;

// Must be a power of two.
const N_BUCKETS : usize = 32;

#[derive(Debug,Default)]
pub struct DB {
    maps: [RwLock<HashMap<BS, BS>>; N_BUCKETS],
}

impl DB {
    pub fn get<'b>(&self, key: &'b [u8]) -> Option<Vec<u8>> {
        let bucket = (key[0] & (N_BUCKETS - 1) as u8) as usize;
        let b = self.maps[bucket].read();
        b.get(key).map(|v| v.clone())
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        let bucket = (key[0] & (N_BUCKETS - 1) as u8) as usize;
        let ins;
        {
            if let Some(v) = self.maps[bucket].write().get_mut(key) {
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
            self.maps[bucket].write().insert(key.to_vec(), v);
        }
    }
}

