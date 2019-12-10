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

use super::buf::{MultiReadBuf, ReadBuf, WriteBuf};
use std::sync::Arc;
use util::model::Model;

/// Definition of the DB trait that will allow extensions to access
/// the database.
pub trait DB {
    /// This method will perform a lookup on a key-value pair inside the
    /// database, and return a handle that can be used to read the value
    /// if the key-value pair exists.
    ///
    /// # Arguments
    ///
    /// * `table`: An identifier of the data table the key-value pair
    ///            belongs to.
    /// * `key`:   A slice of bytes over the key to be looked up.
    ///
    /// # Return
    ///
    /// A handle that can be used to read the value if the key-value pair
    /// exists inside the database.
    fn get(&self, table: u64, key: &[u8]) -> Option<ReadBuf>;

    /// This method performs a lookup for a set of keys stored inside the database as
    /// key-value pairs, and returns a hanle that can be used to read the value for each key
    /// if the key-value pair exists.
    ///
    /// # Arguments
    /// * `table`: An identifier of the data table the key-value pair
    ///            belongs to.
    /// * `key_len`: Length of each key in the key list.
    /// * `keys`: A slice which contains multiple keys.
    ///
    /// # Return
    ///
    /// A handle that can be used to read the value for each key in the list, if the key-value
    /// pair exists inside the database.
    fn multiget(&self, table: u64, key_len: u16, keys: &[u8]) -> Option<MultiReadBuf>;

    /// This method will allocate space for a key-value pair inside the
    /// database, and if the allocation was successfull, return a handle that
    /// can be used to write a value into the allocation, and that can be
    /// subsequently written to the database.
    ///
    /// # Arguments
    ///
    /// * `table`:   An identifier of the data table the key-value pair
    ///              belongs to.
    /// * `key`:     A slice of bytes over the key for the key-value pair that
    ///              will be written into the allocation.
    /// * `val_len`: The length of the value that will eventually be written
    ///              into the allocation.
    ///
    /// # Return
    ///
    /// If the allocation was successfull, a handle that can be used to write
    /// into the allocated space. This handle will already hold the key, and
    /// contain enough space to hold val_len bytes. The handle is not part of
    /// the database yet. To add it to the database, use the `put` method on
    /// the DB trait.
    fn alloc(&self, table: u64, key: &[u8], val_len: u64) -> Option<WriteBuf>;

    /// This method will add a previously allocated region of memory to the
    /// database.
    ///
    /// # Arguments
    ///
    /// * `buf`: A previously allocated handle to be added to the database.
    ///
    /// # Return
    ///
    /// True if the handle was successfully added to the database.
    /// False otherwise.
    fn put(&self, buf: WriteBuf) -> bool;

    /// This method will delete a key-value pair from the database if it exists.
    ///
    /// # Arguments
    ///
    /// * `table`: An identifier of the data table the key-value pair
    ///            belongs to.
    /// * `key`:   A slice of bytes over the key of the object to be deleted.
    fn del(&self, table: u64, key: &[u8]);

    /// This method will return a serialized version of the arguments that were
    /// passed in by the tenant invoking the extension.
    ///
    /// # Return
    ///
    /// A slice over the arguments to the extension. This is a serialized
    /// version, de-serialization is left to the tenant for now.
    fn args(&self) -> &[u8];

    /// This method will write a response for the tenant that invoked the
    /// extension.
    ///
    /// # Arguments
    ///
    /// * `response`: A slice over a serialized response for the tenant. The
    ///               extension should perform said serialization for now.
    fn resp(&self, response: &[u8]);

    /// This method is meant for testing, and will not do anything in the real
    /// system.
    fn debug_log(&self, msg: &str);

    /// This method will perform a lookup on a key-value pair inside the
    /// local cache, and return a handle that can be used to read the value
    /// if the key-value pair exists.
    ///
    /// # Arguments
    ///
    /// * `table`: An identifier of the data table the key-value pair
    ///            belongs to.
    /// * `key`:   A slice of bytes over the key to be looked up.
    ///
    /// # Return
    ///
    /// A tupule of the form (bool, bool, Option<ReadBuf>). The first member is True if the
    /// function is called inside server; False otherwise. The second member is True of the search
    /// is successful; False otherwise. And the third member represents a handle that can be used
    /// to read the value if the key-value pair exists inside the local cache.
    fn search_get_in_cache(&self, table: u64, key: &[u8]) -> (bool, bool, Option<ReadBuf>);

    /// This method performs a lookup for a set of keys stored inside the local cache as
    /// key-value pairs, and returns a hanle that can be used to read the value for each key
    /// if the key-value pair exists.
    ///
    /// # Arguments
    /// * `table`: An identifier of the data table the key-value pair
    ///            belongs to.
    /// * `key_len`: Length of each key in the key list.
    /// * `keys`: A slice which contains multiple keys.
    ///
    /// # Return
    ///
    /// A handle that can be used to read the value for each key in the list, if the key-value
    /// pair exists inside the database.
    fn search_multiget_in_cache(
        &self,
        table: u64,
        key_len: u16,
        keys: &[u8],
    ) -> (bool, bool, Option<MultiReadBuf>);

    /// This method will return the ML model for the given extension. If the model does't exist
    /// for an extension then the method will return `None`.
    ///
    /// # Return
    ///
    /// The model if exists; None otherwise.
    fn get_model(&self) -> Option<Arc<Model>>;
}
