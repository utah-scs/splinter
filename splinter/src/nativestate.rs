/* Copyright (c) 2019 University of Utah
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

extern crate bytes;

use self::bytes::Bytes;
use db::cycles;
use proxy::KV;
use sandstorm::buf::ReadBuf;
use std::ops::{Generator, GeneratorState};

/// Pushback client uses this state for native excution.
pub struct PushbackState {
    /// This distinguishes the operation number amoung multiple dependent operations.
    pub op_num: u8,
    /// This maintains the read-write set to send at commit time.
    pub commit_payload: Vec<u8>,
    /// A list of the records in Read-set for the extension.
    pub readset: Vec<KV>,
}

// Functions need to manipulate `PushbackState` struct.
impl PushbackState {
    /// This method creates a new object of `PushbackState` struct.
    ///
    /// # Return
    /// A new object for PushbackState struct.
    pub fn new(capacity: usize) -> PushbackState {
        PushbackState {
            op_num: 1,
            commit_payload: Vec::with_capacity(capacity),
            readset: Vec::with_capacity(capacity),
        }
    }

    /// This method updates the commit payload.
    ///
    /// # Argument
    /// *`record`: The read-write set sent by the server in response payload.
    pub fn update_rwset(&mut self, record: &[u8], keylen: usize) {
        self.commit_payload.extend_from_slice(record);
        let (version, entry) = record.split_at(1).1.split_at(8);
        let (key, value) = entry.split_at(keylen);
        self.readset.push(KV::new(
            Bytes::from(version),
            Bytes::from(key),
            Bytes::from(value),
        ));
    }

    #[allow(unreachable_code)]
    #[allow(unused_assignments)]
    #[allow(unused_variables)]
    /// This method executes the task.
    ///
    /// # Arguments
    /// *`number`: Number of records to aggregate
    /// *`order`: The amount of compute in each extension.
    pub fn execute_task(&mut self, number: u32, order: u32) {
        let mut generator = move || {
            let mut mul: u64 = 0;
            let mut keys: Vec<u8> = Vec::with_capacity(30);
            keys.extend_from_slice(&self.readset[0].key);
            for i in 0..number {
                let obj = self.search_key(&keys);

                if i == number - 1 {
                    match obj {
                        // If the object was found, use the response.
                        Some(val) => {
                            mul = val.read()[0] as u64;
                        }

                        // If the object was not found, write an error message to the
                        // response.
                        None => {
                            let error = "Object does not exist";
                            return 1;
                        }
                    }
                } else {
                    // find the key for the second request.
                    match obj {
                        // If the object was found, find the key from the response.
                        Some(val) => {
                            keys[0..4].copy_from_slice(&val.read()[0..4]);
                        }

                        // If the object was not found, write an error message to the
                        // response.
                        None => {
                            let error = "Object does not exist";
                            return 2;
                        }
                    }
                }
            }

            // Compute part for this extension
            let start = cycles::rdtsc();
            while cycles::rdtsc() - start < order as u64 {}
            return 0;

            // XXX: This yield is required to get the compiler to compile this closure into a
            // generator. It is unreachable and benign.
            yield 0;
        };

        unsafe {
            match generator.resume() {
                GeneratorState::Yielded(val) => {
                    panic!("Pushback native execution is buggy");
                }
                GeneratorState::Complete(val) => {
                    if val != 0 {
                        panic!("Pushback native execution is buggy");
                    }
                }
            }
        }
    }

    /// This method search the key inside the readset.
    ///
    /// # Arguments
    /// *`key`: key to lookup in readset.
    ///
    /// # Return
    /// Return either the `Some(value)` or `None`.
    pub fn search_key(&mut self, key: &[u8]) -> Option<ReadBuf> {
        let length = self.readset.len();
        for i in 0..length {
            if self.readset[i].key == key {
                let value = self.readset[i].value.clone();
                unsafe {
                    return Some(ReadBuf::new(value));
                }
            }
        }
        return None;
    }
}
