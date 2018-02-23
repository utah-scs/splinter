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

use bytes::{BufMut, Bytes, BytesMut};

/// This type represents a read-only buffer of bytes that can be received from
/// the database. This type is primarily used to read objects from the database.
pub struct ReadBuf {
    inner: Bytes,
}

// Methods on ReadBuf.
impl ReadBuf {
    /// This method returns a ReadBuf. The returned type is basically a wrapper
    /// over a `Bytes` type.
    ///
    /// This function is marked `unsafe` to prevent extensions from constructing
    /// a `ReadBuf` on their own. The only way an extension should be able to
    /// see a `ReadBuf` is by making a get() call on some type that implements
    /// the `DB` trait.
    ///
    /// # Arguments
    ///
    /// * `buffer`: The underlying buffer that will be wrapped up inside a
    ///             `ReadBuf`.
    ///
    /// # Return
    /// The `ReadBuf` wrapping the passed in buffer.
    unsafe pub fn new(buffer: Bytes) -> ReadBuf {
        ReadBuf {
            inner: buffer,
        }
    }
}

/// This type represents a read-write buffer of bytes that can be received from
/// the database. This type is primarily intended to be used to receive
/// allocations from, and write to the database.
pub struct WriteBuf {
    inner: BytesMut,
}

// Methods on WriteBuf.
impl WriteBuf {
    /// This method returns a `WriteBuf`. This returned type is basically a
    /// wrapper around a passed in buffer of type `BytesMut`.
    ///
    /// This method is marked unsafe to prevent extensions from constructing
    /// a `WriteBuf` of their own. The only way an extension should be able
    /// to see a `WriteBuf` is by making an alloc() call down to the database.
    ///
    /// # Arguments
    ///
    /// * `buffer`: The underlying buffer that will be wrapped up inside a
    ///             `WriteBuf`.
    ///
    /// # Return
    /// The `WriteBuf` wrapping the passed in buffer.
    unsafe pub fn new(buffer: BytesMut) -> WriteBuf {
        WriteBuf {
            inner: buffer,
        }
    }
}
