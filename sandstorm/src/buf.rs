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

extern crate bytes;

use std::cell::Cell;

use self::bytes::{BufMut, Bytes, BytesMut};

/// This type represents a read-only buffer of bytes that can be received from
/// the database. This type is primarily used to read objects from the database.
pub struct ReadBuf {
    // The inner `Bytes` that actually holds the data.
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
    pub unsafe fn new(buffer: Bytes) -> ReadBuf {
        ReadBuf { inner: buffer }
    }

    /// This method returns the number of bytes present inside the `ReadBuf`.
    ///
    /// # Return
    ///
    /// The number of bytes present inside the `ReadBuf`.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// This method indicates if the `ReadBuf` is empty.
    ///
    /// # Return
    ///
    /// True if the `ReadBuf` is empty. False otherwise.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// This method returns a slice of bytes to the data contained inside the
    /// `ReadBuf`.
    ///
    /// # Return
    ///
    /// A slice to the data contained inside the `ReadBuf`.
    pub fn read(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

/// This type represents a read-write buffer of bytes that can be received from
/// the database. This type is primarily intended to be used to receive
/// allocations from, and write to the database.
pub struct WriteBuf {
    // Identifier for the data table this buffer will eventually be written to.
    table: u64,

    // The inner BytesMut that will actually be written to.
    inner: BytesMut,

    // The number of metadata bytes that was added in by the database when the
    // allocation was performed.
    meta_len: usize,
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
    /// * `table`:  Identifier for the table the allocation was made for.
    /// * `buffer`: The underlying buffer that will be wrapped up inside a
    ///             `WriteBuf`.
    ///
    /// # Return
    /// The `WriteBuf` wrapping the passed in buffer.
    pub unsafe fn new(table: u64, buffer: BytesMut) -> WriteBuf {
        let init_len = buffer.len();

        WriteBuf {
            table: table,
            inner: buffer,
            meta_len: init_len,
        }
    }

    /// This method returns the number of bytes that have been written to the
    /// `WriteBuf` by the extension so far.
    ///
    /// # Return
    /// The number of bytes written to the `WriteBuf` by the extension so far.
    pub fn len(&self) -> usize {
        self.inner.len() - self.meta_len
    }

    /// This method returns the total number of bytes that can be written by an
    /// extension into the `WriteBuf`.
    ///
    /// # Return
    /// The total number of bytes that can be written into the `WriteBuf` by an
    /// extension.
    pub fn capacity(&self) -> usize {
        self.inner.capacity() - self.meta_len
    }

    /// This method writes a slice of bytes to the end of the `WriteBuf`.
    ///
    /// # Arguments
    ///
    /// * `data`: The slice of bytes to be written into the `WriteBuf`.
    ///
    /// # Abort
    ///
    /// This method will cause the extension to abort if there is insufficent
    /// space left inside the `WriteBuf` to perform the write.
    pub fn write_slice(&mut self, data: &[u8]) {
        self.inner.put_slice(data);
    }

    /// This method writes a single byte to the end of the `WriteBuf`.
    ///
    /// # Arguments
    ///
    /// * `data`: The byte to be written into the `WriteBuf`.
    ///
    /// # Abort
    ///
    /// This method will cause the extension to abort if there is insufficent
    /// space left inside the `WriteBuf` to perform the write.
    pub fn write_u8(&mut self, data: u8) {
        self.inner.put_u8(data);
    }

    /// This method writes a single u16 to the end of the `WriteBuf`.
    ///
    /// # Arguments
    ///
    /// * `data`: The u16 to be written into the `WriteBuf`.
    /// * `le`:   The ordering to be used while performing the write. If true,
    ///           little-endian will be used. If false, big-endian will be used.
    ///
    /// # Abort
    ///
    /// This method will cause the extension to abort if there is insufficent
    /// space left inside the `WriteBuf` to perform the write.
    pub fn write_u16(&mut self, data: u16, le: bool) {
        match le {
            true => {
                self.inner.put_u16_le(data);
            }

            false => {
                self.inner.put_u16_be(data);
            }
        }
    }

    /// This method writes a single u32 to the end of the `WriteBuf`.
    ///
    /// # Arguments
    ///
    /// * `data`: The u32 to be written into the `WriteBuf`.
    /// * `le`:   The ordering to be used while performing the write. If true,
    ///           little-endian will be used. If false, big-endian will be used.
    ///
    /// # Abort
    ///
    /// This method will cause the extension to abort if there is insufficent
    /// space left inside the `WriteBuf` to perform the write.
    pub fn write_u32(&mut self, data: u32, le: bool) {
        match le {
            true => {
                self.inner.put_u32_le(data);
            }

            false => {
                self.inner.put_u32_be(data);
            }
        }
    }

    /// This method writes a single u64 to the end of the `WriteBuf`.
    ///
    /// # Arguments
    ///
    /// * `data`: The u64 to be written into the `WriteBuf`.
    /// * `le`:   The ordering to be used while performing the write. If true,
    ///           little-endian will be used. If false, big-endian will be used.
    ///
    /// # Abort
    ///
    /// This method will cause the extension to abort if there is insufficent
    /// space left inside the `WriteBuf` to perform the write.
    pub fn write_u64(&mut self, data: u64, le: bool) {
        match le {
            true => {
                self.inner.put_u64_le(data);
            }

            false => {
                self.inner.put_u64_be(data);
            }
        }
    }

    /// This method consumes the `WriteBuf`, returning a read-only view to the
    /// contained data.
    ///
    /// This method is marked unsafe to prevent extensions from calling it.
    ///
    /// # Return
    /// A `Bytes` handle to the underlying data that can no longer be mutated.
    pub unsafe fn freeze(self) -> (u64, Bytes) {
        (self.table, self.inner.freeze())
    }
}

/// This type represents a read-only buffer of bytes that can be received from
/// the database. This type is primarily used to read objects from the database
/// in response to a multiget() operation.
pub struct MultiReadBuf {
    inner: Vec<Bytes>,

    index: Cell<usize>,

    panic: Cell<bool>,
}

impl MultiReadBuf {
    /// This method returns a MultiReadBuf. The returned type is basically a wrapper
    /// over a vector of `Bytes` type.
    ///
    /// This function is marked `unsafe` to prevent extensions from constructing
    /// a `MultiReadBuf` on their own. The only way an extension should be able to
    /// see a `MultiReadBuf` is by making a multiget() call on some type that implements
    /// the `DB` trait.
    ///
    /// # Arguments
    ///
    /// * `list`: The underlying vector of bytes that will be wrapped up inside a
    ///             `MultiReadBuf`.
    ///
    /// # Return
    /// The `MultiReadBuf` wrapping the passed in vector.
    pub unsafe fn new(list: Vec<Bytes>) -> MultiReadBuf {
        MultiReadBuf {
            inner: list,
            index: Cell::new(0),
            panic: Cell::new(false),
        }
    }

    /// This method return the number of elements wrapped up inside a
    /// `MultiReadBuf` by the extension so far.
    ///
    /// # Return
    /// The number of elements in the `MultiReadBuf`.
    pub fn num(&self) -> usize {
        self.inner.len()
    }

    /// This method returns the number of bytes on the current `index` in
    /// `MultiReadBuf`.
    ///
    /// # Return
    /// The number of bytes on the current index in `MultiReadBuf`.
    pub fn len(&self) -> usize {
        if self.panic.get() {
            panic!("Out of bounds on MultiReadBuf.");
        }

        self.inner[self.index.get()].len()
    }

    /// This method returns true if the next element is present in the list, false otherwise.
    ///
    /// # Return
    /// True if the next value is in the list, false otherwise.
    pub fn next(&self) -> bool {
        let curr = self.index.get();

        match curr >= self.inner.len() - 1 {
            true => {
                self.panic.set(true);
                return false;
            }

            false => {
                self.index.set(curr + 1);
                return true;
            }
        }
    }

    /// This method returns true if the previous element is present in the list, false otherwise.
    ///
    /// # Return
    /// True if the previous value is in the list, false otherwise.
    pub fn prev(&self) -> bool {
        let curr = self.index.get();

        match curr == 0 {
            true => {
                self.panic.set(true);
                return false;
            }

            false => {
                self.index.set(curr - 1);
                return true;
            }
        }
    }

    /// This method returns the reference to an element which is present on the current index in
    /// `MultiReadBuf`.
    ///
    /// # Return
    /// A reference to an element on the current index in the list.
    pub fn read(&self) -> &[u8] {
        if self.panic.get() {
            panic!("Out of bounds on MultiReadBuf.");
        }

        self.inner[self.index.get()].as_ref()
    }
}

/// This enum represents the type of a completed database operation. A value 'SandstormRead'
/// means that the operation was a get() operation  and a value 'SandstormWrite' means that the
/// operation was a put() operation. The value is used in the response to represent if the record
/// belongs to read set or the write set.
#[repr(u8)]
#[derive(PartialEq, Clone, Debug)]
pub enum OpType {
    /// When the value if SandstormRead, the record encapsulated in the response will be added to
    /// the read set corresponding to that extension.
    SandstormRead = 0x1,

    /// When the value if SandstormWrite, the record encapsulated in the response will be added to
    /// the write set corresponding to that extension.
    SandstormWrite = 0x2,

    /// Any value beyond this represents an invalid record.
    InvalidRecord = 0x3,
}

/// This struct represents a record for a read/write set. Each record in the read/write set will
/// be of this type.
#[derive(Clone)]
pub struct Record {
    /// This variable shows the type of operation for the record, Read or Write.
    optype: OpType,

    /// This variable stores the Key for the record.
    key: Bytes,

    /// This variable stores the Value for the record.
    object: Bytes,
}

impl Record {
    /// This method returns a new record which can be transferred to the client as a read/write set
    /// which server decides to pushback the extension to the client.
    ///
    /// # Arguments
    /// * `r_optype`: The type of the record, either a Read or a Write.
    /// * `r_key`: The key for the record.
    /// * `r_object`: The value for the record.
    ///
    /// # Return
    /// A read-write set record with the operation type, a key and a value.
    pub fn new(r_optype: OpType, r_key: Bytes, r_object: Bytes) -> Record {
        Record {
            optype: r_optype,
            key: r_key,
            object: r_object,
        }
    }

    /// Return the optype(Read/Write) for the operation.
    pub fn get_optype(&self) -> OpType {
        self.optype.clone()
    }

    /// Return the key for the performed operation.
    pub fn get_key(&self) -> Bytes {
        self.key.clone()
    }

    /// Return the object for the performed operation.
    pub fn get_object(&self) -> Bytes {
        self.object.clone()
    }
}

/// This struct maintains the read/write set per extension. When the extension is pushed-back
/// without completing it on the server. This READ/WRITE set is also trasferred to the client.
#[derive(Clone)]
pub struct ReadWriteSetBuf {
    /// The read-write set, a vector of type `Record`.
    pub readwriteset: Vec<Record>,
}

// Implementation of methods on ReadWriteSetBuf.
impl ReadWriteSetBuf {
    /// This function creates a new instance of ReadWriteSetBuf.
    ///
    /// # Return
    ///
    /// A `ReadWriteSetBuf` which can be used to store the read/write set per extension.
    pub fn new() -> ReadWriteSetBuf {
        ReadWriteSetBuf {
            readwriteset: Vec::new(),
        }
    }
}

// This module implements simple unit tests for MultiReadBuf, ReadBuf, ReadWriteSetBuf and WriteBuf.
#[cfg(test)]
mod tests {
    use super::{MultiReadBuf, OpType, ReadBuf, ReadWriteSetBuf, Record, WriteBuf};
    use buf::bytes::{BufMut, Bytes, BytesMut};

    // This method tests the "len()" method on ReadBuf.
    #[test]
    fn test_readbuf_len() {
        // Write 3 bytes into a BytesMut.
        let mut buf = BytesMut::with_capacity(10);
        buf.put_slice(&[1, 2, 3]);

        // Wrap the BytesMut inside a ReadBuf, and verify it's length.
        unsafe {
            let buf = ReadBuf::new(buf.freeze());
            assert_eq!(3, buf.len());
        }
    }

    // This method tests that "is_empty()" returns true when the ReadBuf
    // is empty.
    #[test]
    fn test_readbuf_isempty_true() {
        // Wrap a Bytes inside a ReadBuf, and verify that it is empty.
        unsafe {
            let buf = ReadBuf::new(Bytes::with_capacity(10));
            assert!(buf.is_empty());
        }
    }

    // This method tests that "is_empty()" returns false when the ReadBuf
    // is not empty.
    #[test]
    fn test_readbuf_isempty_false() {
        // Write 3 bytes into a BytesMut.
        let mut buf = BytesMut::with_capacity(10);
        buf.put_slice(&[1, 2, 3]);

        // Wrap the BytesMut inside a ReadBuf, and verify that it isn't empty.
        unsafe {
            let buf = ReadBuf::new(buf.freeze());
            assert!(!buf.is_empty());
        }
    }

    // This method tests the functionality of the "read()" method on ReadBuf.
    #[test]
    fn test_readbuf_read() {
        // Write data into a BytesMut.
        let mut buf = BytesMut::with_capacity(10);
        let data = &[1, 2, 3, 4, 5, 6, 7, 18, 19];
        buf.put_slice(data);

        // Wrap the BytesMut inside a ReadBuf, and verify that the contents
        // of the slice returned by "read()" match what was written in above.
        unsafe {
            let buf = ReadBuf::new(buf.freeze());
            assert_eq!(data, buf.read());
        }
    }

    // This method tests the functionality of the "len()" method on WriteBuf.
    #[test]
    fn test_writebuf_len() {
        // Allocate and write into a BytesMut.
        let mut buf = BytesMut::with_capacity(100);
        buf.put_slice(&[1; 30]);

        // Wrap up the above BytesMut inside a WriteBuf, and write more data in.
        // Verify that the length reported by len() does not include the data
        // written above.
        unsafe {
            let mut buf = WriteBuf::new(1, buf);
            let data = &[1, 2, 3, 4];
            buf.inner.put_slice(data);
            assert_eq!(data.len(), buf.len());
        }
    }

    // This method tests the functionality of the "capacity()" method on
    // WriteBuf.
    #[test]
    fn test_writebuf_capacity() {
        // Allocate and write into a BytesMut.
        let mut buf = BytesMut::with_capacity(100);
        let meta = &[1; 30];
        buf.put_slice(meta);

        // Wrap up the above BytesMut inside a WriteBuf, and verify that the
        // WriteBuf's capacity does not include the data written above.
        unsafe {
            let buf = WriteBuf::new(1, buf);
            assert_eq!(100 - meta.len(), buf.capacity());
        }
    }

    // This method tests the functionality of the "write_slice()" method on
    // WriteBuf.
    #[test]
    fn test_writebuf_writeslice() {
        // Create a WriteBuf, write into it with write_slice(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            let data = &[1, 2, 3, 4, 5];
            buf.write_slice(data);
            assert_eq!(data, &buf.inner[..]);
        }
    }

    // This method tests that the "write_slice()" method on WriteBuf panics in
    // the case of a write overflow.
    #[test]
    #[should_panic]
    fn test_writebuf_writeslice_overflow() {
        // Create a WriteBuf, and write one byte more than it's capacity.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(100));
            let data = &[1; 101];
            buf.write_slice(data);
        }
    }

    // This method tests the functionality of the "write_u8()" method on
    // WriteBuf.
    #[test]
    fn test_writebuf_writeu8() {
        // Create a WriteBuf, write into it with write_u8(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u8(200);

            let expected = &[200];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests that the "write_u8()" method on WriteBuf panics in
    // the case of a write overflow.
    #[test]
    #[should_panic]
    fn test_writebuf_writeu8_overflow() {
        // Create a WriteBuf, and write one byte more than it's capacity.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(100));
            let data = &[1; 100];
            buf.write_slice(data);

            buf.write_u8(200);
        }
    }

    // This method tests the functionality of the "write_u16()" method on
    // WriteBuf, when the write order is Little endian.
    #[test]
    fn test_writebuf_writeu16_le() {
        // Create a WriteBuf, write into it with write_u16(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u16(258, true);

            let expected = &[2, 1];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests the functionality of the "write_u16()" method on
    // WriteBuf, when the write order is Big endian.
    #[test]
    fn test_writebuf_writeu16_be() {
        // Create a WriteBuf, write into it with write_u16(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u16(258, false);

            let expected = &[1, 2];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests that the "write_u16()" method on WriteBuf panics in
    // the case of a write overflow.
    #[test]
    #[should_panic]
    fn test_writebuf_writeu16_overflow() {
        // Create a WriteBuf, and write two bytes more than it's capacity.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(100));
            let data = &[1; 100];
            buf.write_slice(data);

            buf.write_u16(258, true);
        }
    }

    // This method tests the functionality of the "write_u32()" method on
    // WriteBuf, when the write order is Little endian.
    #[test]
    fn test_writebuf_writeu32_le() {
        // Create a WriteBuf, write into it with write_u32(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u32(84148994, true);

            let expected = &[2, 3, 4, 5];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests the functionality of the "write_u32()" method on
    // WriteBuf, when the write order is Big endian.
    #[test]
    fn test_writebuf_writeu32_be() {
        // Create a WriteBuf, write into it with write_u32(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u32(84148994, false);

            let expected = &[5, 4, 3, 2];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests that the "write_u32()" method on WriteBuf panics in
    // the case of a write overflow.
    #[test]
    #[should_panic]
    fn test_writebuf_writeu32_overflow() {
        // Create a WriteBuf, and write four bytes more than it's capacity.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(100));
            let data = &[1; 100];
            buf.write_slice(data);

            buf.write_u32(84148994, true);
        }
    }

    // This method tests the functionality of the "write_u64()" method on
    // WriteBuf, when the write order is Little endian.
    #[test]
    fn test_writebuf_writeu64_le() {
        // Create a WriteBuf, write into it with write_u64(), and the verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u64(8674083586, true);

            let expected = &[2, 3, 4, 5, 2, 0, 0, 0];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests the functionality of the "write_u64()" method on
    // WriteBuf, when the write order is Big endian.
    #[test]
    fn test_writebuf_writeu64_be() {
        // Create a WriteBuf, write into it with write_u64(), and then verify
        // that it's contents match what's expected.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(10));
            buf.write_u64(8674083586, false);

            let expected = &[0, 0, 0, 2, 5, 4, 3, 2];
            assert_eq!(expected, &buf.inner[..]);
        }
    }

    // This method tests that the "write_u64()" method on WriteBuf panics in
    // the case of a write overflow.
    #[test]
    #[should_panic]
    fn test_writebuf_writeu64_overflow() {
        // Create a WriteBuf, and write eight bytes more than it's capacity.
        unsafe {
            let mut buf = WriteBuf::new(1, BytesMut::with_capacity(100));
            let data = &[1; 100];
            buf.write_slice(data);

            buf.write_u64(8674083586, true);
        }
    }

    // This method tests the number of elements in the MultiReadBuf. It should be
    // exacty the number of elements added to the list.
    #[test]
    fn test_multireadbuf_num() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.num(), 1);
        }
    }

    // This method checks the length of one element in the list.
    #[test]
    fn test_multireadbuf_len() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.len(), 100);
        }
    }

    // This method tests if the index is out of bound, then len() method should panic.
    #[test]
    #[should_panic]
    fn test_multireadbuf_len_panic() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.next(), false);
            multibuf.len();
        }
    }

    // This method checks that next() should return false when there is
    // no next element in the list.
    #[test]
    fn test_multireadbuf_next() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.next(), false);
        }
    }

    // This method checks that next() should return true when there is
    // a next element in the list.
    #[test]
    fn test_multireadbuf_next1() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);
        let data1 = vec![1; 100];
        let buf1 = Bytes::from(data1);

        let mut list = Vec::new();
        list.push(buf);
        list.push(buf1);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.next(), true);
        }
    }

    // This method checks that prev() should return false when there is
    // no previous element in the list.
    #[test]
    fn test_multireadbuf_prev() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            multibuf.next();
            assert_eq!(multibuf.prev(), false);
        }
    }

    // This method checks that prev() should return true when there is
    // a previous element in the list.
    #[test]
    fn test_multireadbuf_prev1() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);
        let data1 = vec![1; 100];
        let buf1 = Bytes::from(data1);

        let mut list = Vec::new();
        list.push(buf);
        list.push(buf1);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.next(), true);
            assert_eq!(multibuf.prev(), true);
        }
    }

    // This method checks if read() method returns the pointer to the element present
    // on the current index.
    #[test]
    fn test_multireadbuf_read() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.read().len(), 100);
        }
    }

    // If the element a current index doesn't exist then read() method should panic.
    #[test]
    #[should_panic]
    fn test_multireadbuf_read_panic() {
        let data = vec![1; 100];
        let buf = Bytes::from(data);

        let mut list = Vec::new();
        list.push(buf);
        unsafe {
            let multibuf = MultiReadBuf::new(list);
            assert_eq!(multibuf.next(), false);
            multibuf.read();
        }
    }

    // This method checks the length of the readwrite set before and after
    // adding the record into it.
    #[test]
    fn test_readwriteset_len() {
        let mut buf = ReadWriteSetBuf::new();
        let data = vec![1; 100];
        let data = Bytes::from(data);
        let record = Record::new(OpType::SandstormRead, data.clone(), data);
        assert_eq!(buf.readwriteset.len(), 0);
        buf.readwriteset.push(record);
        assert_eq!(buf.readwriteset.len(), 1);
    }

    // This method tests the get_*() functions for the Record implementation.
    #[test]
    fn test_record_gets() {
        let data = vec![1; 100];
        let data = Bytes::from(data);
        let record = Record::new(OpType::SandstormRead, data.clone(), data);
        assert_eq!(record.get_key().len(), 100);
        assert_eq!(record.get_object().len(), 100);
        assert_eq!(record.get_optype(), OpType::SandstormRead);
    }

    // This method tests the equality of OpType values.
    #[test]
    fn test_optype_equal() {
        assert_eq!(OpType::SandstormRead, OpType::SandstormRead);
        assert_eq!(OpType::SandstormWrite, OpType::SandstormWrite);
        assert_eq!(OpType::InvalidRecord, OpType::InvalidRecord);
    }

    // This method tests the not equal value for OpType enum.
    #[test]
    fn test_optype_ntequal() {
        assert_ne!(OpType::SandstormRead, OpType::SandstormWrite);
        assert_ne!(OpType::SandstormRead, OpType::InvalidRecord);

        assert_ne!(OpType::SandstormWrite, OpType::SandstormRead);
        assert_ne!(OpType::SandstormWrite, OpType::InvalidRecord);

        assert_ne!(OpType::InvalidRecord, OpType::SandstormRead);
        assert_ne!(OpType::InvalidRecord, OpType::SandstormWrite);
    }
}
