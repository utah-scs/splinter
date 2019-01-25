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

use hashbrown::HashMap;

use spin::{RwLock};
use bytes::{Bytes};
use std::sync::atomic::{AtomicU64, Ordering};

// The number of buckets in the hash table. Must be a power of two.
// If you want to change this number, then you will also have to modify
// the implementation of the Default trait below.
//
// The number 128 was chosen based on experiments conducted on
// CloudLab d430's with 16 threads on 02/09/2018.
//     32 buckets: 14.4 Million ops/s (read-only), 7.2 Million ops/s (50-50)
//     64 buckets: 17.0 Million ops/s (read-only)
//    128 buckets: 18.5 Million ops/s (read-only), 12.3 Million ops/s (50-50)
const N_BUCKETS : usize = 128;

// Each Entry in a Table has an associated Version that is per-key monotonic.
// This is used for concurrency control to identify when the value associated
// with a key has changed.
struct Version(u64);

// An Entry in a Table which stores metadata about the stored value and a smart
// pointer to the value itself.
struct Entry {
  // A unique, per-table-key monotonic id for the value associated with this
  // verison.
  version: Version,
  // A ref-counted smart pointer to a stored value.
  value: Bytes,
}

/// This struct represents a single table in Sandstorm. A table is indexed using
/// an unordered map, which hashes an object's key to it's value. Tables can be
/// safely accessed concurrently from multiple threads.
pub struct Table {
    // Each table is effectively an array of hash-maps, each of which is
    // protected by a read-write lock. Each element of this array is effectively
    // a "bucket".
    //
    // Keys and values are currently represented using Bytes from the bytes
    // crate because:
    //     1) The Bytes type is basically one layer of indirection over an
    //        underlying array of u8's. As a result, this underlying array
    //        can be allocated separately, wrapped up inside a Bytes and
    //        handed over to a hash-map. The hash-map takes ownership of the
    //        Bytes and not the underlying array.
    //     2) The Bytes type has an atomic ref count over this underlying array,
    //        allowing for multiple threads/procedures to hold references to an
    //        object, without worrying about concurrent updates. An object will
    //        be dropped only when this ref-count goes to zero.
    maps: [RwLock<HashMap<Bytes, Entry>>; N_BUCKETS],

    // Represents the highest version number of any entry that was removed from
    // map. This is used to ensure all future entries associated with that key
    // will have a higher version. An attempt is made to raise this value
    // in delete() and it is accessed in put() when a new entry is inserted
    // into map.
    max_deleted_version: AtomicU64,
}

// Implementation of the Default trait for Table.
impl Default for Table {
    // This method returns a table with 128 buckets. The maps array needs to
    // be explicitly initialized this way because the Default trait cannot be
    // derived for arrays with more than 32 elements.
    fn default() -> Table {
        Table {
            maps: [RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                   RwLock::new(HashMap::new()), RwLock::new(HashMap::new()),
                ],
           max_deleted_version: AtomicU64::new(0),
        }
    }
}

// Implementation of Table
impl Table {
    /// This function reads an object from a table.
    ///
    /// # Arguments
    ///
    /// * `key`: A slice of bytes corresponding to the object's key.
    ///
    /// # Return
    ///
    /// The object corresponding to the supplied key if one exists. The
    /// object is returned wrapped up in a Bytes type. The underlying object
    /// is guaranteed to exist atleast until the returned Bytes is dropped.
    /// If the object does not exist in the Table, this method returns None.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        // First, identify the bucket the key falls into.
        let bucket: usize = key[0] as usize & (N_BUCKETS - 1);
        let map = self.maps[bucket].read();

        // Perform the lookup, and return.
        return map.get(key).and_then(| entry | { Some(entry.value.clone()) });
    }

    /// This function writes an object into a table.
    ///
    /// # Arguments
    ///
    /// * `key`:    A Bytes wrapping the key for the object.
    /// * `object`: A Bytes wrapping the entire object to be written to
    ///             the table.
    pub fn put(&self, key: Bytes, value: Bytes) {
        // First, identify the bucket the key falls into.
        let bucket: usize = key.slice(0, 1)[0] as usize & (N_BUCKETS - 1);
        let mut map = self.maps[bucket].write();

        if let Some(entry) = map.get_mut(&key) {
            // If an entry already exists, then update it (we are holding a
            // bucket lock).
            entry.value = value;
            entry.version.0 += 1;
            return;
        }

        // If an entry does not exist we need to insert it while making
        // sure that its version number is higher than any version that
        // could have previously been associated with this key.
        let version = Version(self.max_deleted_version.load(Ordering::Relaxed) + 1);
        let _entry = map.insert(key, Entry{version, value});
    }

    /// This function deletes an object from a table.
    ///
    /// # Arguments
    ///
    /// * `key`: The key of the object to be deleted, passed in as a slice of bytes.
    pub fn delete(&self, key: &[u8]) {
        // First, identify the bucket the key falls into.
        let bucket: usize = key[0] as usize & (N_BUCKETS - 1);
        let mut map = self.maps[bucket].write();

        // Next, remove the key from the hash map if it already exists.
        if let Some(entry) = map.remove(key) {
            // Record the version number so we never use a lower version for any
            // future value associated with the key.

            // Be careful here if this function eventually becomes lock-free:
            // here removal of the entry isn't visible until after the
            // max_deleted_version is incremented. This ensures all future inserts
            // to the removed key will have a version number higher than the one
            // on the removed entry. That invariant has to be maintained .

            self.max_deleted_version.fetch_max(entry.version.0, Ordering::Relaxed);
        }
    }
}

// This module contains a few basic unit tests for Table. These tests are
// independent of how Sandstorm represents objects, and are only meant to
// test basic functionality like reference counting etc.
#[cfg(test)]
mod tests {
    use super::Table;
    use bytes::{BufMut, Bytes, BytesMut};

    // This unit test inserts a key-value pair into a table, performs a read
    // on the key, and asserts that the value matches. If the key was not found,
    // then this test panics to indicate the failure.
    #[test]
    fn test_get_some() {
        let table = Table::default();

        let key: &[u8] = &[0; 30];
        let val: &[u8] = &[1; 30];

        // Create the object corresponding to the above key and value.
        let mut object = BytesMut::with_capacity(key.len() + val.len());
        object.put_slice(&key);
        object.put_slice(&val);
        let mut object: Bytes = object.freeze();

        // Populate the table with the object.
        let key_ref: Bytes = object.split_to(key.len());
        table.put(key_ref, object);

        // Perform a lookup on the key, and assert that it's value is
        // the same as that populated above.
        match table.get(key) {
            Some(value) => {
                assert_eq!(val, &value[..]);
            }

            // Indicate failure if the key wasn't found in the table.
            None => {
                panic!("get() request on table returned None.");
            }
        }
    }

    // This test verifies that a get request for a non-existent key
    // returns None.
    #[test]
    fn test_get_none() {
        let table = Table::default();

        // Lookup a key that does not exist in the table. Assert that
        // the method returns None.
        assert_eq!(None, table.get(&[0; 30]));
    }

    // This test populates a table with one object and performs a read on
    // the object. It then performs an update on this object and checks
    // if the previously read value is still accessible.
    #[test]
    fn test_num_refs() {
        let table = Table::default();

        let key: &[u8] = &[0; 30];
        let val: &[u8] = &[1; 30];

        // Create the object that will go into the table.
        let mut obj: BytesMut = BytesMut::with_capacity(key.len() + val.len());
        obj.put_slice(key);
        obj.put_slice(val);
        let mut obj: Bytes = obj.freeze();

        // Add the object to the table.
        let key_ref: Bytes = obj.split_to(key.len());
        table.put(key_ref, obj);

        // Perform a lookup on the object.
        match table.get(key) {
            Some(value) => {
                let new_val: &[u8] = &[2; 30];

                // Create an object with the same key, but new value.
                let mut obj: BytesMut = BytesMut::with_capacity(key.len() +
                                                                new_val.len());
                obj.put_slice(key);
                obj.put_slice(new_val);
                let mut obj: Bytes = obj.freeze();

                // Add the new object to the table.
                let key_ref: Bytes = obj.split_to(key.len());
                table.put(key_ref, obj);

                // Check if the result of the get is still accessible.
                assert_eq!(val, &value[..]);
            }

            // Indicate failure if the key wasn't found in the table.
            None => {
                panic!("get() request on table returned None.");
            }
        }
    }

    // This function tests that once deleted from a table, an object cannot be accessed again.
    #[test]
    fn test_delete() {
        let table = Table::default();

        let key: &[u8] = &[0; 30];
        let val: &[u8] = &[1; 30];

        // Create the object that will go into the table.
        let mut obj: BytesMut = BytesMut::with_capacity(key.len() + val.len());
        obj.put_slice(key);
        obj.put_slice(val);
        let mut obj: Bytes = obj.freeze();

        // Add the object to the table.
        let key_ref: Bytes = obj.split_to(key.len());
        table.put(key_ref, obj);

        // Next, delete the key from the table.
        table.delete(key);

        // Assert that the key was deleted.
        assert_eq!(None, table.get(key));
    }
}
