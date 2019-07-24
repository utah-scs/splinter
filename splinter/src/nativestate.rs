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

/// Pushback client uses this state for native excution.
pub struct PushbackState {
    /// This distinguishes the operation number amoung multiple dependent operations.
    pub op_num: u8,
    /// This maintains the read-write set to send at commit time.
    pub commit_payload: Vec<u8>,
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
        }
    }

    /// This method updates the commit payload.
    ///
    /// # Argument
    /// *`record`: The read-write set sent by the server in response payload.
    pub fn update_rwset(&mut self, record: &[u8]) {
        self.commit_payload.extend_from_slice(record);
    }
}
