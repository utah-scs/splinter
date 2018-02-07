/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use std::collections::HashMap;

use spin::{ RwLock };
use bytes::{ Bytes };

// The number of buckets in the hash table. Must be a power of two.
const N_BUCKETS : usize = 32;

#[derive(Debug, Default)]
pub struct Table {
    maps: [RwLock<HashMap<Bytes, Bytes>>; N_BUCKETS],
}

impl Table {
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        // First, identify the bucket the key falls into.
        let bucket: usize = key[0] as usize & (N_BUCKETS - 1);
        let map = self.maps[bucket].read();

        // Perform the lookup, and return.
        return map.get(key).and_then(| value | { Some(value.clone()) });
    }

    pub fn put(&self, key: Bytes, value: Bytes) {
        // First, identify the bucket the key falls into.
        let bucket: usize = key.slice(0, 1)[0] as usize & (N_BUCKETS - 1);
        let mut map = self.maps[bucket].write();

        // Next, remove the key from the hash map.
        let _ = map.remove(&key);

        // Perform the insert.
        let _ = map.insert(key, value);
    }
}
