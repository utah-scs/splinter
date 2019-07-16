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

use sandstorm::buf::Record;

/// This type is used by the extension invocation to record the read-write set.
/// And the read-write set is transferred back to the client on Pushback. Also,
/// it is used by the table type to validate and commit the transaction.
pub struct TX {
    // This vector maintains the read-set for a transaction.
    reads: Vec<Record>,

    // This vector maintains the write-set for a transaction.
    writes: Vec<Record>,
}

impl TX {
    /// This method returns an object for TX type.
    pub fn new() -> TX {
        TX {
            reads: Vec::with_capacity(4),
            writes: Vec::with_capacity(2),
        }
    }

    /// This method returns the reference to the read-set.
    pub fn reads(&self) -> &Vec<Record> {
        &self.reads
    }

    /// This method returns the reference to the write-set.
    pub fn writes(&self) -> &Vec<Record> {
        &self.writes
    }

    /// This method sorts the read and write set based on the key.
    pub fn sort(&mut self) {
        self.reads.sort_by_key(|record| record.get_key());
        self.writes.sort_by_key(|record| record.get_object());
    }

    /// This method adds new record to the read-set.
    ///
    /// # Arguments
    /// *`record`: The record containing the Optype and key+value.
    pub fn record_get(&mut self, record: Record) {
        self.reads.push(record);
    }

    /// This method adds new record to the write-set.
    ///
    /// # Arguments
    /// *`record`: The record containing the Optype and key+value.
    pub fn record_put(&mut self, record: Record) {
        self.writes.push(record);
    }
}
