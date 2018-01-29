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

/// This type indicates to a procedure whether a call to the database
/// succeeded or failed.
enum SandstormErr {
    /// This value indicates that the call succeeded.
    SUCCESS,

    /// This value indicates that the call failed because the table
    /// does not exist.
    TABLE_DOES_NOT_EXIST,
}

/// Definition of the `DBInterface` trait that will allow untrusted procedures
/// to access the database.
///
/// Every tenant is going to want to represent keys, values, arguments, and
/// responses in it's own format. As a result, the database needs some way of
/// converting between these types and the underlying types it uses to represent
/// data. This is achieved by asking tenants to supply to following types
/// wherever relevant.
///     - `K`: The type used by a procedure to represent a key.
///     - `V`: The type used by a procedure to represent a value.
///     - `A`: The type used by a procedure for its arguments.
///     - `R`: The type used by a procedure for its response.
pub trait DBInterface {
    /// This method should create a table at the database.
    ///
    /// - `table_id`: An identifier for the table to be created.
    ///
    /// - `return`:   True if the table was successfully created. False
    ///               otherwise.
    fn create_table(table_id: u64) -> bool;

    /// This method should allow a procedure to lookup a key of type 'K' from
    /// the database. It should return a reference to a value of type 'V'.
    ///
    /// - `table_id`: An identifier for the table the key-value pair belongs to.
    /// - `key`:      A reference to the key being looked up by the procedure.
    ///
    /// - `return`:   The value corresponding to the key if one exists. A
    ///               reference to this value is handed to the procedure wrapped
    ///               inside a Result<>. The Result<> is required because the
    ///               table might not contain this key.
    fn get_key<K, V>(table_id: u64, key: &K) -> Result<&V, SandstormErr>;

    /// This method should allow a procedure to lookup a key of type 'K' from
    /// the database. It should return the value as an opaque slice of bytes.
    /// This method is handy for when the tenant represents values esoterically.
    ///
    /// - `table_id`: An identifier for the table the key-value pair belongs to.
    /// - `key`:      A reference to the key being looked up by the procedure.
    ///
    /// - `return`:   An opaque slice of bytes corresponding to the value if it
    ///               exists. This slice is wrapped up inside a Result<>.
    fn get_key_opaque<K>(table_id: u64, key: &K) -> Result<[u8], SandstormErr>;

    /// This method should allow a procedure to lookup a list of keys, each of
    /// type 'K' from the database. It should return a list of references to the
    /// corresponding values, each of type 'V'.
    ///
    /// - `table_id`: An identifier for the table the key-value pair belongs to.
    /// - `key_list`: A vector of references to the keys to be looked up. Each
    ///               key should be of type 'K'.
    ///
    /// - `return`:   A vector of references to the corresponding values. Each
    ///               of which is wrapped up inside an Option<> to handle cases
    ///               where a particular key does not exist. The vector itself
    ///               is wrapped up inside a Result<> to indicate errors such
    ///               as when the table does not exist etc.
    fn get_key_list<K, V>(table_id: u64,
                          key_list: Sandstorm::Vec<&K>)
                          -> Result<Sandstorm::Vec<Option<&V>>, SandstormErr>;

    /// TODO: Spec out a table iterator.

    /// This method should execute a passed in closure over every key-value
    /// pair in a table.
    ///
    /// - `table_id`: An identifier for the table the key-value pair belongs to.
    /// - `filter`:   A closure that takes in references to a key-value pair,
    ///               and returns an Option<> wrapping this pair. The idea is
    ///               for this closure to be a filter on the table's contents.
    fn table_filter_unordered<K, V>(table_id: u64,
                                    filter: Fn(&K, &V) -> Option<(&K, &V)>)
                                    -> Result<Sandstorm::Vec<&K, &V>,
                                              SandstormErr>;

    /// This method should write a key-value pair to a table. The key should
    /// be of type 'K', and the value should be of type 'V'.
    ///
    /// - `table_id`: An identifier for the table the key-value pair belongs to.
    /// - `key`:      A reference to the key being written by the procedure.
    /// - `value`:    A reference to the value being written by the procedure.
    ///
    /// - `return`:   An error code of type 'SandstormErr' indicating whether
    ///               the operation succeeded or failed.
    fn put_key<K, V>(table_id: u64, key: &K, value: &V) -> SandstormErr;

    /// This method should write a key-value pair to a table. The key should
    /// be of type 'K', and the value should be an opaque slice of bytes.
    ///
    /// - `table_id`: An identifier for the table the key-value pair belongs to.
    /// - `key`:      A reference to the key being written by the procedure.
    /// - `value`:    A slice of bytes corresponding to the value being written
    ///               by the procedure.
    ///
    /// - `return`:   An error code of type 'SandstormErr' indicating whether
    ///               the operation succeeded or failed.
    fn put_key_opaque<K>(table_id: u64, key: &K, value: [u8]) -> SandstormErr;

    /// This method should write a list of key-value pairs to a table. Each
    /// key should be of type 'K', and each value should be of type 'V'.
    ///
    /// - `table_id`:     An identifier for the table the key-value pair
    ///                   belongs to.
    /// - `key_val_list`: A vector of references to the key-value pairs that
    ///                   need to be written to the database.
    ///
    /// - `return`:   An error code of type 'SandstormErr' indicating whether
    ///               the operation succeeded or failed.
    fn put_list<K, V>(table_id: u64, key_val_list: Sandstorm::Vec<(&K, &V)>)
                      -> SandstormErr;

    /// This method should re-interpret a slice of objects from one type (T)
    /// into another type (U).
    ///
    /// - `arg`:    The slice of objects to be re-interpretted, each of type T.
    ///
    /// - `return`: The passed in slice re-interpretted as a slice of objects,
    ///             each of type U.
    fn reinterpret_as<T, U>(arg: [T]) -> [U];

    /// This method should re-interpret a slice of objects from type T into
    /// a slice of opaque bytes.
    ///
    /// - `arg`:    The slice of objects to be re-interpretted.
    ///
    /// - `return`: The objects re-interpretted as a slice of bytes.
    fn reinterpret_as_opaque<T>(arg: [T]) -> [u8];

    /// This method should re-interpret a slice of bytes as a slice of objects,
    /// each with type T.
    ///
    /// - `arg`:    The slice of bytes to be re-interpretted.
    ///
    /// - `return`: The passed in slice re-interpretted as a slice of objects,
    ///             each of type T.
    fn reinterpret_opaque_as<T>(arg: [u8]) -> [T];

    /// This method should return a reference to the arguments to the procedure
    /// that were passed in by through an RPC sent by a client.
    ///
    /// - `return`: A reference to the arguments type.
    fn read_arguments<A>() -> &A;

    /// This method should write a response for the client that issued the RPC
    /// to invoke this procedure.
    ///
    /// - `response`: A reference to the response to be written, passed in with
    ///               type 'R'.
    ///
    /// - `return`:   An error code of type 'SandstormErr' indicating whether
    ///               the operation succeeded or failed.
    fn write_response<R>(response: &R) -> SandstormErr;
}
