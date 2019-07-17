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

use spin::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use bytes::{Bytes};
use std::sync::atomic::{AtomicU64, Ordering};
use std::ops::Deref;

use super::tx::{TX};
use super::wireformat::{Record};
use super::cycles;

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

#[derive(Copy,Clone,PartialEq,Debug)]
/// Each Entry in a Table has an associated Version that is per-key monotonic.
/// This is used for concurrency control to identify when the value associated
/// with a key has changed.
pub struct Version(u64);

#[derive(Clone)]
/// An Entry in a Table which stores metadata about the stored value and a smart
/// pointer to the value itself.
pub struct Entry {
  /// A unique, per-table-key monotonic id for the value associated with this
  /// verison.
  pub version: Version,
  /// A ref-counted smart pointer to a stored value.
  pub value: Bytes,
}

/// Indicates commit (Ok(())) or abort (Err(())) decision from validate().
type Decision = Result<(), ()>;
/// Returned by validate() to indicate the tx has been applied.
const COMMIT: Decision = Result::Ok(());
/// Returned by validate() to indicate the tx was not applied.
const ABORT: Decision = Result::Err(());

/// Type alias for each map that occupies a Table's bucket array.
/// Simplifies several types in validation.
type Map = HashMap<Bytes, Entry>;

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
    maps: [RwLock<Map>; N_BUCKETS],

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
    pub fn get(&self, key: &[u8]) -> Option<Entry> {
        // First, identify the bucket the key falls into.
        let map = self.maps[Self::bucket(key)].read();

        // Perform the lookup, and return.
        return map.get(key).and_then(| entry | { Some((*entry).clone()) });
    }

    /// This function writes an object into a table.
    ///
    /// # Arguments
    ///
    /// * `key`:    A Bytes wrapping the key for the object.
    /// * `object`: A Bytes wrapping the entire object to be written to
    ///             the table.
    pub fn put(&self, key: Bytes, value: Bytes) -> Option<Entry> {
        // First, identify the bucket the key falls into.
        let mut map = self.maps[Self::bucket(&key[..])].write();

        if let Some(entry) = map.get_mut(&key) {
            // If an entry already exists, then update it (we are holding a
            // bucket lock).
            entry.value = value;
            entry.version.0 += 1;
            return Some(entry.clone());
        }

        // If an entry does not exist we need to insert it while making
        // sure that its version number is higher than any version that
        // could have previously been associated with this key.
        let version = Version(self.max_deleted_version.load(Ordering::Relaxed) + 1);
        return map.insert(key, Entry{version, value});
    }

    /// This function deletes an object from a table.
    ///
    /// # Arguments
    ///
    /// * `key`: The key of the object to be deleted, passed in as a slice of bytes.
    pub fn delete(&self, key: &[u8]) {
        // First, identify the bucket the key falls into.
        let mut map = self.maps[Self::bucket(&key[..])].write();

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

    /// Return which bucket in a Table maps field this key should be housed within.
    fn bucket(key: &[u8]) -> usize {
        key[0] as usize & (N_BUCKETS - 1)
    }

    /// Atomically apply a transaction to this table or indicate abort if
    /// this transaction is non-serializable with previously validated
    /// transactions and puts.
    ///
    /// # Arguments
    ///
    /// * `tx`: The transaction to atomically apply to the table.
    ///         Contains the set of keys read and written along with
    ///         version numbers for records that were read.
    ///         validate() is non-destructive to `tx`.
    /// 
    /// # Return
    /// * `COMMIT`: if `tx` was serializable and its writes were applied to `self`.
    /// * `ABORT`: if a record version `tx` has changed.
    pub fn validate(&self, tx: &mut TX) -> Decision {
        // Sorting RW set will avoid deadlock later when we
        // validate in parallel.
        tx.sort();

        // Used to track lock guards for each bucket in the table
        // for this transaction.
        enum Lock<'a> {
            Unlocked, // Bucket not yet locked by this tx.
            ReadLocked(RwLockReadGuard<'a, Map>), // Bucket read-locked by tx.
            WriteLocked(RwLockWriteGuard<'a, Map>) // Bucket write-locked by tx.
        }
        
        // Create an array of N_BUCKETS lock guards; the unsafe code here is to get
        // around the fact that there isn't a way to initialize with Unlocked for each
        // element since Lock is non-Copy.
        let mut locks: [Lock; N_BUCKETS] = unsafe {
            let mut a: [Lock; N_BUCKETS] = ::std::mem::uninitialized();
            for i in &mut a[..] {
                ::std::ptr::write(i, Lock::Unlocked);
            }
            a
        };

        // Acquire write locks.
        tx.writes().iter().for_each(| record | {
            let bucket = Self::bucket(&record.get_key()[..]);
            let lock = unsafe{ locks.get_unchecked_mut(bucket) };
            match lock {
                Lock::Unlocked => {
                    let guard = self.maps[bucket].write();
                    *lock = Lock::WriteLocked(guard);
                },
                Lock::WriteLocked(_guard) => {
                    // Nothing to do; already locked.
                },
                Lock::ReadLocked(_guard) => {
                    assert!(false); // Impossible; acquired further down.
                }
            }
        });

        fn record_version_ok<Guard>(guard: &Guard, record: &Record) -> Decision
            where Guard: Deref<Target = Map>
        {
            let map: &Map = &*guard; // Convert from guard ref-like to actual Map ref.
            if let Some(entry) = map.get(&record.get_key()[..]) {
                if record.get_version() == entry.version {
                    COMMIT
                } else {
                    ABORT
                }
            } else {
                ABORT
            }
        }

        // Acquire read locks and validate each version as we go.
        tx.reads().iter().map(| record | {
            let bucket = Self::bucket(&record.get_key()[..]);
            let lock = unsafe{ locks.get_unchecked_mut(bucket) };
            match lock {
                Lock::Unlocked => {
                    let guard = self.maps[bucket].read();
                    let result = record_version_ok(&guard, record);
                    *lock = Lock::ReadLocked(guard);
                    result
                },
                Lock::WriteLocked(guard) => {
                    // Already locked; just validate.
                    record_version_ok(guard, record)
                },
                Lock::ReadLocked(guard) => {
                    // Already locked; just validate.
                    record_version_ok(guard, record)
                }
            }
        }).collect::<Decision>()?;

        // Allocate a version number/timestamp.
        // Note: the current approach depends A) on the monotonicity of rdtsc().
        // Currently, we don't handle non-monotonicity that might happen e.g. due to reboots/restarts.
        let version: Version = Version(cycles::rdtsc());

        // Install write set.
        tx.writes().iter().for_each(| record | {
            let key = &record.get_key()[..];
            let bucket = Self::bucket(key);

            let lock = unsafe{ locks.get_unchecked_mut(bucket) };
            if let Lock::WriteLocked(ref mut guard) = lock {
                let map: &mut Map = &mut*guard;
                if let Some(entry) = map.get_mut(key) {
                    entry.version = version;
                    entry.value = record.get_object();
                } else {
                    assert!(false); // Bucket was locked, but value disappeared! Impossible.
                }
            } else {
                assert!(false); // Unlocked or only ReadLocked; should be impossible.
            }
        });

        COMMIT
    }
}

// This module contains a few basic unit tests for Table. These tests are
// independent of how Sandstorm represents objects, and are only meant to
// test basic functionality like reference counting etc.
#[cfg(test)]
mod tests {
    use super::{Table, Version, TX, Record, COMMIT, ABORT};
    use super::super::wireformat::{OpType};
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
            Some(entry) => {
                assert_eq!(val, &entry.value[..]);
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
        if let Some(_entry) = table.get(&[0; 30]) {
            panic!("There shouldn't be anything in the table.");
        }
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
            Some(entry) => {
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
                assert_eq!(val, &entry.value[..]);
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
        if let Some(_entry) = table.get(key) {
            panic!("Key should not exist.");
        }
    }

    fn mkval(n: u8) -> Bytes {
        let mut value = [0; 30];
        value[0] = n;
        Bytes::from(&value[..])
    }

    fn create_tx(setup: Vec<(bool, u8, u64)>) -> TX {
        let mut tx = TX::new();
        setup.into_iter().for_each(| (read, key, version) | {
            let r = Record::new(
                if read { OpType::SandstormRead } else { OpType::SandstormWrite },
                Version(version),
                mkval(key),
                mkval(key)
            );
            if read {
                tx.record_get(r);
            } else {
                tx.record_put(r);
            }
        });
        tx
    }

    fn filled_table() -> Table {
        let table = Table::default();
        for i in 0..10 {
            table.put(mkval(i), mkval(i)); // Will have Version 1 to 10
        }
        table
    }

    #[test]
    fn test_validate_rotx_ok() {
        let table = filled_table();
        let mut tx = create_tx(
            vec![(true, 0, 1)] // Read k0 v1.
        );
        // Unchanged, so commit.
        assert_eq!(COMMIT, table.validate(&mut tx));
    }

    #[test]
    fn test_validate_rotx_rwconflict() {
        let table = filled_table();
        let mut tx = create_tx(vec![
            (true, 0, 1) // Read k0 v1.
        ]);
        table.put(mkval(0), mkval(0)); // Other wrote k0 (v becomes 2).
        assert_eq!(ABORT, table.validate(&mut tx));
    }

    #[test]
    fn test_validate_rotx_wwconflict() {
        let table = filled_table();
        let mut tx = create_tx(vec![
            (false, 0, 1) // Wrote k0, version becomes tsc.
        ]);
        table.put(mkval(0), mkval(0)); // Other wrote k0 (v becomes 2).
        // WW is blind, so commit ok.
        assert_eq!(COMMIT, table.validate(&mut tx));
    }

    #[test]
    fn test_validate_rotx_rw_and_wwconflict() {
        let table = filled_table();
        let mut tx = create_tx(vec![
            (true, 0, 1),  // Read k0 v1.
            (false, 0, 1), // Wrote k0, version becomes tsc.
        ]);
        table.put(mkval(0), mkval(0)); // Other wrote k0 (v becomes 2).
        // Abort due to RW conflict.
        assert_eq!(ABORT, table.validate(&mut tx));
    }

    #[test]
    fn test_validate_double_read_lock_no_deadlock() {
        let table = filled_table();
        let mut tx = create_tx(vec![
            (true, 0, 1),  // Read k0 v1.
            (true, 0, 1),  // Read k0 v1.
        ]);
        assert_eq!(COMMIT, table.validate(&mut tx));
    }

    #[test]
    fn test_validate_double_write_lock_no_deadlock() {
        let table = filled_table();
        let mut tx = create_tx(vec![
            (false, 0, 1),  // Write k0.
            (false, 0, 1),  // Write k0.
        ]);
        assert_eq!(COMMIT, table.validate(&mut tx));
    }

    #[test]
    fn test_validate_aborted_discards_writes() {
        let table = filled_table();
        let mut tx = create_tx(vec![
            (true, 0, 1),  // Read k0 v1.
            (false, 1, 99),  // Write k1 = 99 (initially version 1).
        ]);
        table.put(mkval(0), mkval(0)); // Other wrote k0 (v becomes 2).
        assert_eq!(ABORT, table.validate(&mut tx));
        assert_eq!(Version(1), table.get(&mkval(1)[..]).unwrap().version);
    }
}
