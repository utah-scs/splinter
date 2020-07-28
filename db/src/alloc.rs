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

use std::mem::size_of;

use bytes::{BufMut, Bytes, BytesMut};

/// This type represents the memory allocator in Sandstorm. The allocator
/// allocates and initializes objects that can then be inserted into a
/// particular tenant's hash table. Each allocated object has the following
/// layout in memory (Metadata is written in little-endian):
///      ______________________________________________________________________
///     |           |           |            |             |                   |
///     | Tenant-ID | Table-ID  | Key-Length |     Key     |       Value       |
///     |___________|___________|____________|_____________|___________________|
///        4 Bytes     8 Bytes     2 Bytes      Var Length       Var Length
pub struct Allocator {}

// Implementation of methods on Allocator.
impl Allocator {
    /// This method returns an allocator that can be used to allocate objects
    /// that can be added to a tenant's data table.
    ///
    /// # Return
    /// An allocator of type `Allocator`.
    pub fn new() -> Allocator {
        Allocator {}
    }

    /// This method allocates space for an object, and writes metadata and only
    /// the key into the allocated region. Space will be allocated for the
    /// object's value, but nothing will be written into this allocated space.
    ///
    /// This method is primarily meant to serve allocation requests by
    /// extensions for objects that will eventually be added to a table, but
    /// whose value requires some complex computation.
    ///
    /// Additionally, providing an extension with a direct handle to an
    /// allocation can improve performance by reducing the number of
    /// intermediate copies.
    ///
    /// # Arguments
    ///
    /// * `tenant`:  An identifier for the tenant requesting the allocation.
    /// * `table`:   An identifier for the table the object will be added into.
    /// * `key`:     A slice of bytes representing the key corresponding to the
    ///              object. The ordering (little/big endian) is determined by
    ///              the tenant.
    /// * `val_len`: The number of bytes to pre-allocate for the object's value.
    ///
    /// # Return
    /// A `BytesMut` to the underlying allocation. Any writes to this handle
    /// will be added to the object's value.
    pub fn raw(&self, tenant: u32, table: u64, key: &[u8], val_len: u64) -> Option<BytesMut> {
        // Allocate space for the object.
        match self.alloc(tenant, table, key.len() as u16, val_len) {
            // The allocation was successfull.
            Some(mut object) => {
                // Write the key into the object.
                object.put_slice(key);

                return Some(object);
            }

            // The allocation failed.
            None => {
                return None;
            }
        }
    }

    /// This method allocates a full object that can be directly added into a
    /// tenant's data table.
    ///
    /// # Arguments
    ///
    /// * `tenant`: An identifier for the tenant requesting for the allocation.
    /// * `table`:  An identifier for the table the allocated object will be
    ///             added to.
    /// * `key`:    A slice of bytes representing the key for the object. The
    ///             ordering of these bytes (little/big endian) is up to the
    ///             tenant.
    /// * `val`:    A slice of bytes representing the value for the object. The
    ///             ordering of these bytes (little/big endian) is again, up to
    ///             the tenant.
    ///
    /// # Return
    /// A tupule corresponding to the allocated object. The first member is a
    /// `Bytes` handle over the underlying object's key. The second, is again a
    /// `Bytes` handle to the entire object. Returning both these handles allows
    /// for easy insertion into the tenant's table.
    pub fn object(
        &self,
        tenant: u32,
        table: u64,
        key: &[u8],
        val: &[u8],
    ) -> Option<(Bytes, Bytes)> {
        // Allocate space for the object.
        match self.alloc(tenant, table, key.len() as u16, val.len() as u64) {
            // The allocation was successfull.
            Some(mut object) => {
                // Write the key and value into the object, and convert it to
                // read-only.
                object.put_slice(key);
                object.put_slice(val);
                let object: Bytes = object.freeze();

                // Return a view to the key and the object.
                let meta = self.meta_size();
                return Some((object.slice(meta, meta + key.len()), object));
            }

            // The allocation failed.
            None => {
                return None;
            }
        }
    }

    // This is an internal method the performs the actual allocation. The head
    // of each allocated piece of memory consists of metadata identifying the
    // tenant this object belongs to, the data table the object belongs to, and
    // the length of the objects key.
    //
    // - `tenant`:  An identifier for the tenant requesting the allocation.
    // - `table`:   An identifier for the data table this object will be added
    //              to.
    // - `key_len`: The amount of space to be allocated for the object's key.
    // - `val_len`: The amount of space to be allocated for the object's value.
    //
    // - `return`: A `BytesMut` handle to the underlying region of memory.
    fn alloc(&self, tenant: u32, table: u64, key_len: u16, val_len: u64) -> Option<BytesMut> {
        // Calculate the amount of memory to be allocated for metadata.
        let meta = self.meta_size();

        // Calculate the total amount of memory to be allocated for the object.
        let size = meta +
                    key_len as usize + // To store the key.
                    val_len as usize; // To store the value.

        // Allocate space for the object.
        // XXX This could actually allocate more than size bytes.
        let mut object = BytesMut::with_capacity(size);

        // Write metadata into the object.
        object.put_u32_le(tenant);
        object.put_u64_le(table);
        object.put_u16_le(key_len);

        return Some(object);
    }

    /// This method takes in a previously allocated object, and returns a handle
    /// to it's key, and a handle to it's value.
    ///
    /// # Arguments
    ///
    /// * `object`: A previously allocated object.
    ///
    /// # Return
    /// A tupule consisting of two `Bytes`. The first is a handle to the passed
    /// in object's key, and the second is a handle to it's value.
    pub fn resolve(&self, object: Bytes) -> Option<(Bytes, Bytes)> {
        // Read the two bytes corresponding to the key length from the object.
        let meta = self.meta_size();
        let (left, right) = (object.get(meta - 2), object.get(meta - 1));

        match (left, right) {
            // The above read was successfull. Compute the key length assuming
            // little endian, and return Bytes handle to the object's key and a
            // Bytes handle to the object's value.
            (Some(lb), Some(rb)) => {
                let key_len = (*lb as u16) + (*rb as u16) * 256;

                Some((
                    object.slice(meta, meta + key_len as usize),
                    object.slice_from(meta + key_len as usize),
                ))
            }

            // The key length could not be read from the passed in object.
            _ => {
                return None;
            }
        }
    }

    // This method returns the amount of metadata on each allocated object.
    #[inline]
    fn meta_size(&self) -> usize {
        let meta = size_of::<u32>() +  // To store tenant id.
                    size_of::<u64>() + // To store table id.
                    size_of::<u16>(); // To store key length.
        return meta;
    }
}

// This module contains simple unit tests for Allocator.
#[cfg(test)]
mod tests {
    use super::Allocator;
    use bytes::{BufMut, BytesMut};

    // This unit test verifies the return value of the "meta_size()" method
    // on Allocator.
    #[test]
    fn test_meta_size() {
        let heap = Allocator::new();
        assert_eq!(14, heap.meta_size());
    }

    // This unit test tests the functionality of the "resolve()" method on
    // Allocator.
    #[test]
    fn test_resolve() {
        let heap = Allocator::new();

        let key: &[u8] = &[12, 24, 24, 12];
        let val: &[u8] = &[48, 96, 96, 48];

        // Allocate an object, and resolve the allocation into it's key
        // and value.
        let (_, obj) = heap
            .object(0, 0, key, val)
            .expect("Failed to allocate object.");
        let (k, v) = heap.resolve(obj).expect("Failed to resolve object.");

        // Check the contents of the resolved key and value against their
        // expected values.
        assert_eq!(key[..], k[..]);
        assert_eq!(val[..], v[..]);
    }

    // This is a basic unit test for Allocator's "test_alloc()" method. It
    // requests for an allocation, and verifies the size and content's of the
    // returned handle.
    #[test]
    fn test_alloc() {
        let heap = Allocator::new();

        let tenant: u32 = 0;
        let table: u64 = 11;
        let key_len: u16 = 30;
        let val_len: u64 = 100;

        // The expected result of the allocation.
        let mut expected = BytesMut::with_capacity(144);
        expected.put_slice(&[0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 30, 0]);

        // Request for an allocation.
        match heap.alloc(tenant, table, key_len, val_len) {
            // Success. Check the size and contents of the allocation against
            // the expected result.
            Some(obj) => {
                assert_eq!(expected.len(), obj.len());
                assert_eq!(expected[..], obj[..]);
                assert_eq!(expected.capacity(), obj.capacity());
            }

            // Failure!? This path should *not* be taken. Throw a panic
            // indicating something went wrong.
            None => {
                panic!("Call to alloc() returned None instead of object.");
            }
        }
    }

    // This unit test verifies Allocator's "raw()" method. It requests for
    // an allocation through the method, and verifies the size and contents
    // of the returned handle.
    #[test]
    fn test_raw() {
        let heap = Allocator::new();

        let tenant: u32 = 0;
        let table: u64 = 11;
        let key: [u8; 4] = [12, 45, 200, 99];
        let val_len: u64 = 100;

        // The expected result.
        let mut expected = BytesMut::with_capacity(118);
        expected.put_slice(&[0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 4, 0]);
        expected.put_slice(&key);

        // Perform the request.
        match heap.raw(tenant, table, &key, val_len) {
            // Success, check the returned handles size and contents against
            // the expected value(s).
            Some(obj) => {
                assert_eq!(expected.len(), obj.len());
                assert_eq!(expected[..], obj[..]);
                assert_eq!(expected.capacity(), obj.capacity());
            }

            // Failure!? This should not happen. Throw a panic.
            None => {
                panic!("Call to raw() returned None instead of object.");
            }
        }
    }

    // This unit test verifies Allocator's "object()" method. It requests for
    // an allocation through the method, and verifies the size and contents
    // of the returned handle.
    #[test]
    fn test_object() {
        let heap = Allocator::new();

        let tenant: u32 = 0;
        let table: u64 = 11;
        let key: [u8; 4] = [12, 45, 200, 99];
        let val: [u8; 100] = [100; 100];

        // The expected result.
        let mut expected = BytesMut::with_capacity(118);
        expected.put_slice(&[0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0, 4, 0]);
        expected.put_slice(&key);
        expected.put_slice(&val);

        // Perform the request.
        match heap.object(tenant, table, &key, &val) {
            // Success, check the returned handles size and contents against
            // the expected value(s).
            Some((key_actual, obj)) => {
                assert_eq!(key, key_actual[..]);
                assert_eq!(expected[..], obj[..]);
            }

            // Failure!? This should not happen. Throw a panic.
            None => {
                panic!("Call to object() returned None instead of object.");
            }
        }
    }
}
