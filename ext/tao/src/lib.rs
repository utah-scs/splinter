#![feature(generators)]
#![feature(generator_trait)]
#![forbid(unsafe_code)]
#![allow(bare_trait_objects)]

extern crate sandstorm;

use sandstorm::buf::WriteBuf;
use sandstorm::db::DB;
use sandstorm::{LittleEndian, ReadBytesExt, WriteBytesExt};

use sandstorm::boxed::Box;
use sandstorm::convert::From;
use sandstorm::rc::Rc;
use sandstorm::result::Result;
use sandstorm::size_of;
use sandstorm::time::{SystemTime, UNIX_EPOCH};
use sandstorm::vec::*;

use std::ops::Generator;
use std::pin::Pin;

type Id = u64;
type ObjectType = u16;
type Time = u64;

enum TaoOp {
    ObjGet = 0,
    ObjAdd = 1,
    ObjUpdate = 2,
    ObjDelete = 3,
    AssocGet = 4,
    AssocAdd = 5,
    AssocDelete = 6,
}

/// Converts a u8 into a TaoOp.
impl From<u8> for TaoOp {
    fn from(original: u8) -> Self {
        match original {
            0 => TaoOp::ObjGet,
            1 => TaoOp::ObjAdd,
            2 => TaoOp::ObjUpdate,
            3 => TaoOp::ObjDelete,
            4 => TaoOp::AssocGet,
            5 => TaoOp::AssocAdd,
            6 => TaoOp::AssocDelete,
            _ => panic!("Invalid Tao opcode."),
        }
    }
}

type ResponseHandler = fn(db: Rc<DB>, otype: &[u8], object: &[u8]);
type AssocResponseHandler = fn(db: Rc<DB>, assoc: Association);

#[no_mangle]
#[allow(unreachable_code)]
#[allow(unused_assignments)]
pub fn init(db: Rc<DB>) -> Pin<Box<Generator<Yield = u64, Return = u64>>> {
    Box::pin(move || {
        {
            return dispatch(db);
        }
        yield 0;
    })
}

/// Manages the arguments and calls to execute TAO code by the client.
///
/// # Arguments
/// * `db` - a connection to the database.
fn dispatch(db: Rc<DB>) -> u64 {
    // Each packet should contain a 1 byte opcode denoting which method to call.
    if db.args().len() < 1 {
        let error = "Invalid args";
        db.resp(error.as_bytes());
        return 1;
    }

    let (opcode, ops) = db.args().split_at(1);
    let opcode: u8 = 0 | opcode[0] as u8;

    match TaoOp::from(opcode) {
        TaoOp::ObjGet => obj_get_dispatch(Rc::clone(&db), ops),
        TaoOp::ObjAdd => obj_add_dispatch(Rc::clone(&db), ops),
        TaoOp::ObjUpdate => obj_update_dispatch(Rc::clone(&db), ops),
        TaoOp::ObjDelete => obj_delete_dispatch(Rc::clone(&db), ops),
        _ => assoc_dispatch(opcode, Rc::clone(&db), ops),
    };

    return 0;
}

/// Handles the response to a client for an object.
///
/// # Arguments
/// * `db` - a connection to the database.
/// * `otype` - the type of the object.
/// * `object` - the bytes representing the objects value.
fn object_response_handler(db: Rc<DB>, _otype: &[u8], object: &[u8]) {
    // db.resp(otype);
    db.resp(object);
}

/// Handles the response to a client for an association.
///
/// # Arguments
/// * `db` - a connection to the database.
/// * `assoc` - the association which needs to be written into the response to the client.
fn assoc_response_handler(db: Rc<DB>, assoc: Association) {
    let mut assoc_serialized: Vec<u8> = Vec::with_capacity(Association::size());
    assoc_serialized
        .write_u64::<LittleEndian>(assoc.id)
        .unwrap();
    assoc_serialized
        .write_u64::<LittleEndian>(assoc.time)
        .unwrap();

    db.resp(assoc_serialized.as_slice());
}

/// Manages the resquest to perform an object_get. The response is the object retrieved from the
/// db or an error.
///
/// # Packet structure
/// |table_id = 8|obj_id = 8|
///
/// # Arguments
/// * `db` - a connection to the database.
/// * `ops` - packet information
fn obj_get_dispatch(db: Rc<DB>, ops: &[u8]) {
    // |table_id = 8|obj_id = 8|
    if ops.len() != 16 {
        let error = "Invalid packet length.";
        db.resp(error.as_bytes());
        return;
    }

    let (table, key) = ops.split_at(8);
    let table: u64 = convert_from_slice(table);

    let tao = TAO::new(Rc::clone(&db), table, 0);
    if tao.object_get(key, object_response_handler) == false {
        db.resp("ERROR: could not get object.".as_bytes());
    }
}

/// Manages the resquest to perform an object_add. The response is the object_id of the new object.
///
/// # Packet structure
/// |table_id = 8|obj_type = 2|value = n > 0|
///
/// # Arguments
/// * `db` - a connection to the database.
/// * `ops` - packet information
fn obj_add_dispatch(db: Rc<DB>, ops: &[u8]) {
    // |table_id = 8|obj_type = 2|value = n > 0|
    if ops.len() <= 10 {
        let error = "Invalid packet length.";
        db.resp(error.as_bytes());
        return;
    }

    let (table, type_value) = ops.split_at(8);
    let table: u64 = convert_from_slice(table);

    let (otype, value) = type_value.split_at(2);
    let otype: u16 = 0 | otype[0] as u16 | (otype[1] as u16) << 8;

    let mut tao = TAO::new(Rc::clone(&db), table, 0);
    db.resp(tao.object_add(otype, value).as_slice());
}

/// Manages the resquest to perform an object_update. The response is empty if the call was
/// successful, or an error message otherwise.
///
/// # Packet structure
/// |table_id = 8|obj_id = 8|obj_type = 2|value = n > 0|
///
/// # Arguments
/// * `db` - a connection to the database.
/// * `ops` - packet information
fn obj_update_dispatch(db: Rc<DB>, ops: &[u8]) {
    // |table_id = 8|obj_id = 8|obj_type = 2|value = n > 0|
    if ops.len() <= 18 {
        let error = "Invalid packet length.";
        db.resp(error.as_bytes());
        return;
    }

    let (table, rest) = ops.split_at(8);
    let table: u64 = convert_from_slice(table);

    let (obj_id, rest2) = rest.split_at(8);

    let (obj_type, value) = rest2.split_at(2);
    let obj_type: u16 = 0 | obj_type[0] as u16 | (obj_type[1] as u16) << 8;

    let tao = TAO::new(Rc::clone(&db), table, 0);
    if tao.object_update(obj_id, obj_type, value) == false {
        db.resp("ERROR: unsuccessful update".as_bytes());
    }
}

/// Manages the resquest to perform an object_delete. The response is empty if the call was
/// successful, or an error message otherwise.
///
/// # Packet structure
/// |table_id = 8|obj_id = 8|
///
/// # Arguments
/// * `db` - a connection to the database.
/// * `ops` - packet information
fn obj_delete_dispatch(db: Rc<DB>, ops: &[u8]) {
    // |table_id = 8|obj_id = 8|
    if ops.len() != 16 {
        let error = "Invalid packet length.";
        db.resp(error.as_bytes());
        return;
    }

    let (table, obj_id) = ops.split_at(8);
    let table: u64 = convert_from_slice(table);

    let tao = TAO::new(Rc::clone(&db), table, 0);
    if tao.object_delete(obj_id) == false {
        db.resp("ERROR: unable to delete object".as_bytes());
    }
}

/// Manages the resquest to perform any of the association functions.
/// The response for
///     add: empty if successful, error message otherwise.
///     delete: empty if successful, error message otherwise.
///     get: bytes representing the association if sucessful, error message otherwise.
///
/// # Packet structure
/// |table_id = 8|id1 = 8|assoc_type = 2|id2 = 8|
///
/// # Arguments
/// * `opcode` - identifier for which association operation should be called.
/// * `db` - a connection to the database.
/// * `ops` - packet information.
fn assoc_dispatch(opcode: u8, db: Rc<DB>, ops: &[u8]) {
    // |table_id = 8|id1 = 8|assoc_type = 2|id2 = 8|
    if ops.len() != 26 {
        db.resp("Invalid packet length.".as_bytes());
        return;
    }

    let (table, rest) = ops.split_at(8);
    let table: u64 = 0 | table[0] as u64 | (table[1] as u64) << 8 | (table[2] as u64) << 16
        | (table[3] as u64) << 24 | (table[4] as u64) << 32
        | (table[5] as u64) << 40 | (table[6] as u64) << 48
        | (table[7] as u64) << 56;

    let (id1, rest2) = rest.split_at(8);
    let (assoc_type, id2) = rest2.split_at(2);
    let tao = TAO::new(Rc::clone(&db), 0, table);

    match TaoOp::from(opcode) {
        TaoOp::AssocGet => {
            if tao.association_get(id1, assoc_type, id2, assoc_response_handler) == false {
                db.resp("ERROR: could not get association.".as_bytes());
            }
        }
        TaoOp::AssocAdd => {
            if tao.association_add(id1, assoc_type, id2) == false {
                db.resp("ERROR: unsuccessful update".as_bytes());
            }
        }
        TaoOp::AssocDelete => {
            if tao.association_delete(id1, assoc_type, id2) == false {
                db.resp("ERROR: unable to delete the association".as_bytes());
            }
        }
        _ => {} // ERROR invalid opcode.
    };
}

pub struct TAO {
    client: Rc<DB>,
    object_table_id: u64,
    association_table_id: u64,
    next_id: Id,
}

impl TAO {
    /// Returns a TAO instance connecting to the given client.
    ///
    /// # Arguments
    /// * `client` - Access to a sandstorm::DB in which to add DB info to.
    /// * `object_table_id` - table id for the object table associated with this TAO instance.
    /// * `association_table_id` - table id for the association table associated with this TAO instance.
    pub fn new(client: Rc<DB>, object_table_id: u64, association_table_id: u64) -> TAO {
        // Create a table if they don't already exist.
        // client.create_table(object_table_id);
        // client.create_table(association_table_id);

        TAO {
            client,
            object_table_id,
            association_table_id,
            next_id: 1, // TODO: this is invalid if the user is creating a new TAO instance, for existing tables. This needs to be read from DB?
        }
    }

    /// Returns the id of the newly created object.
    ///
    /// # Arguments
    /// * `object_type` - Type of the object being added.
    /// * 'data' - kvpairs which make up the object.
    pub fn object_add(&mut self, otype: ObjectType, data: &[u8]) -> Vec<u8> {
        let object_id = self.allocate_unique_id();

        self.object_update(object_id.as_slice(), otype, data);
        return object_id;
    }

    /// Updates the object with the given id and type to contain the data provided.
    ///
    /// # Arguments
    /// * `id` - id of the object to be updated.
    /// * `otype` - type of the object to be updated.
    /// * `data` - updated data to replace current data with.
    pub fn object_update(&self, id: &[u8], otype: ObjectType, data: &[u8]) -> bool {
        let space_needed = ObjectHeader::size() + data.len();

        let mut container = match self.client
            .alloc(self.object_table_id, id, space_needed as u64)
        {
            None => return false,
            Some(o) => o,
        };

        //put the header into container
        let header: ObjectHeader = ObjectHeader { otype };

        header.serialize(&mut container);

        //put the data into container
        container.write_slice(data);

        return self.client.put(container);
    }

    /// Deletes the object with the given id.
    ///
    /// # Arguments
    /// * `id` - id of the object to be created.
    pub fn object_delete(&self, id: &[u8]) -> bool {
        self.client.del(self.object_table_id, id);
        return true;
    }

    /// Gets the data for the object with the given id. Returns the type of the object.
    /// Puts object data into the slice provided.
    ///
    /// # Arguments
    /// * `id` - id of the object to get.
    /// * `data` - a container to put the data in.
    // pub fn object_get(&self, id: Id, mut data: Vec<u8>) -> ObjectType {
    pub fn object_get(&self, id: &[u8], callback: ResponseHandler) -> bool {
        let obj = self.client.get(self.object_table_id, id);

        match obj {
            Some(data) => {
                //  [..header..|.........object data.........]
                let size_of_header = ObjectHeader::size();
                let data_slice: &[u8] = data.read();
                callback(
                    Rc::clone(&self.client),
                    &data_slice[0..size_of_header],
                    &data_slice[size_of_header..data_slice.len() - 1],
                );
                return true;
            }
            None => {
                return false;
            }
        }
    }

    /// Adds the given Association (id1, type, id2) to the AssociationList (id1, type) if one exists.
    /// Otherwise, creates a new AssociationList and populates it with the given Association.
    ///
    /// # Arguments
    /// * `id1` - the id of the first object in this Association.
    /// * `association_type` - the type of this association.
    /// * `id2` - the id of the second object in this Association.
    pub fn association_add(&self, id1: &[u8], association_type: &[u8], id2: &[u8]) -> bool {
        //Add the association to the table. (id1, atype, id2)<key> -> data<value>.
        let new_assoc = Association {
            id: convert_from_slice(id2),
            time: self.current_time(),
        };

        let mut assoc_key: Vec<u8> =
            Vec::with_capacity(id1.len() + association_type.len() + id2.len());
        assoc_key.extend_from_slice(id1);
        assoc_key.extend_from_slice(association_type);
        assoc_key.extend_from_slice(id2);

        let space_needed = Association::size();
        let mut assoc_container = match self.client.alloc(
            self.association_table_id,
            assoc_key.as_slice(),
            space_needed as u64,
        ) {
            None => return false,
            Some(o) => o,
        };

        new_assoc.serialize(&mut assoc_container);

        if self.client.put(assoc_container) {
            // Add the association to the list. (id1, atype) -> (id2)
            // To do this, assume the list exists. if it doesn't exist, add our entry and add the list
            // to the db.
            let mut list_key: Vec<u8> =
                Vec::with_capacity(id1.len() + association_type.len() + id2.len());
            list_key.extend_from_slice(id1);
            list_key.extend_from_slice(association_type);

            let mut list = match self.client
                .get(self.association_table_id, list_key.as_slice())
            {
                Some(list_serialized) => match AssociationList::deserialize(list_serialized.read())
                {
                    Ok(ls) => ls,
                    Err(_) => return false,
                },
                None => {
                    // Create a new AssociationList.
                    AssociationList::new()
                }
            };

            list.add(new_assoc);

            let mut list_container = match self.client.alloc(
                self.association_table_id,
                list_key.as_slice(),
                list.size() as u64,
            ) {
                None => return false,
                Some(o) => o,
            };

            list.serialize(&mut list_container);

            return self.client.put(list_container);
        } else {
            return false;
        }
    }

    /// Deletes the Association (id1, type, id2) and removes it from the List.
    ///
    /// # Arguments
    /// * `id1` - the id of the first object in this Association.
    /// * `association_type` - the type of this association.
    /// * `id2` - the id of the second object in this Association.
    pub fn association_delete(&self, id1: &[u8], association_type: &[u8], id2: &[u8]) -> bool {
        let assoc = Association {
            id: convert_from_slice(id2),
            time: 0, //This doesn't matter because we will find the assoc via the id.
        };
        let mut list_key: Vec<u8> =
            Vec::with_capacity(id1.len() + association_type.len() + id2.len());
        list_key.extend_from_slice(id1);
        list_key.extend_from_slice(association_type);

        let mut list = match self.client
            .get(self.association_table_id, list_key.as_slice())
        {
            Some(list_serialized) => match AssociationList::deserialize(list_serialized.read()) {
                Ok(ls) => ls,
                Err(_) => return false,
            },
            None => return false,
        };

        list.remove(assoc.id);

        // recommit the list
        let mut list_container = match self.client.alloc(
            self.association_table_id,
            list_key.as_slice(),
            list.size() as u64,
        ) {
            None => return false,
            Some(o) => o,
        };

        list.serialize(&mut list_container);

        if self.client.put(list_container) {
            // Delete the association
            let mut assoc_key: Vec<u8> =
                Vec::with_capacity(id1.len() + association_type.len() + id2.len());
            assoc_key.extend_from_slice(id1);
            assoc_key.extend_from_slice(association_type);
            assoc_key.extend_from_slice(id2);
            self.client
                .del(self.association_table_id, assoc_key.as_slice());
            return true;
        } else {
            return false;
        }
    }

    /// Returns true if the operation was successful, false otherwise;
    ///
    /// # Arguments
    /// * `id1` - the id of the first object in this Association.
    /// * `association_type` - the type of this association.
    /// * `id2` - the id of the second object in this Association.
    pub fn association_get(
        &self,
        id1: &[u8],
        association_type: &[u8],
        id2: &[u8],
        _assoc_response_handler: AssocResponseHandler,
    ) -> bool {
        let key_len = id1.len() + association_type.len() + id2.len();
        let mut assoc_key: Vec<u8> = Vec::with_capacity(key_len * 4);
        assoc_key.extend_from_slice(id1);
        assoc_key.extend_from_slice(association_type);
        assoc_key.extend_from_slice(id2);

        // Get the length of the id and atype combined.
        let len = id1.len() + association_type.len();

        // Lookup the association list.
        match self.client
            .get(self.association_table_id, &assoc_key[0..len])
        {
            Some(a_list) => {
                let list = a_list.read();

                // Get the number of assocs in the list.
                let s = size_of::<Association>();
                let n = list.len() / s;

                // Construct a key for the database lookup.
                for i in 0..n {
                    let l = i * s;
                    let r = l + size_of::<Id>();
                    let id = &list[l..r];

                    let l = (key_len * i) + len;
                    let r = l + id2.len();

                    match i {
                        0 => assoc_key[l..r].copy_from_slice(id),

                        _ => {
                            assoc_key.extend_from_slice(id1);
                            assoc_key.extend_from_slice(association_type);
                            assoc_key.extend_from_slice(id);
                        }
                    }
                }

                // Lookup the assocs, add them to the response buffer.
                let buf =
                    self.client
                        .multiget(self.association_table_id, key_len as u16, &assoc_key);

                match buf {
                    Some(vals) => {
                        if vals.num() > 0 {
                            self.client.resp(vals.read());
                        }

                        while vals.next() {
                            self.client.resp(vals.read());
                        }

                        return true;
                    }

                    None => return false,
                }
            }

            None => return false, //Error assoc does not exist.
        }
    }

    /// Returns seconds since unix epoch.
    fn current_time(&self) -> Time {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("invalid time");
        now.as_secs()
    }

    // Returns a simple unique integer.
    fn allocate_unique_id(&mut self) -> Vec<u8> {
        self.next_id += 1;
        let mut id = Vec::new();
        id.write_u64::<LittleEndian>(self.next_id).unwrap();
        return id;
    }
}

/// converts a slice into an u64
///
/// # Arguments
/// * `val` - the slice being converted.
fn convert_from_slice(val: &[u8]) -> u64 {
    let val: u64 = 0 | val[0] as u64 | (val[1] as u64) << 8 | (val[2] as u64) << 16
        | (val[3] as u64) << 24 | (val[4] as u64) << 32 | (val[5] as u64) << 40
        | (val[6] as u64) << 48 | (val[7] as u64) << 56;
    return val;
}

struct ObjectHeader {
    otype: ObjectType,
}
impl ObjectHeader {
    /// Returns the space in memory required to serialize this struct.
    fn size() -> usize {
        let otype_size = size_of::<ObjectType>();
        otype_size
    }

    fn serialize(&self, bytes: &mut WriteBuf) {
        bytes.write_u16(self.otype, true)
    }

    fn deserialize(mut bytes: &[u8]) -> Result<ObjectHeader, sandstorm::io::Error> {
        let obj_type = match bytes.read_u16::<LittleEndian>() {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        Ok(ObjectHeader { otype: obj_type })
    }
}

#[derive(Debug)]
pub struct Association {
    id: Id,
    time: Time,
}
impl PartialEq for Association {
    fn eq(&self, other: &Association) -> bool {
        self.id == other.id
    }
}
impl Association {
    /// Returns the space in memory required to serialize this struct.
    fn size() -> usize {
        let id_size = size_of::<Id>();
        let time_size = size_of::<Time>();
        id_size + time_size
    }

    fn serialize(&self, bytes: &mut WriteBuf) {
        bytes.write_u64(self.id, true);
        bytes.write_u64(self.time, true);
    }

    fn deserialize(mut bytes: &[u8]) -> Result<Association, sandstorm::io::Error> {
        Ok(Association {
            id: bytes.read_u64::<LittleEndian>().unwrap(),
            time: bytes.read_u64::<LittleEndian>().unwrap(),
        })

        // match bytes.read_u64::<LittleEndian>() {
        //     Ok(id) => match bytes.read_u64::<LittleEndian>() {
        //         Ok(time) => return Ok(Association {
        //             id: id,
        //             time: time,
        //         }),
        //         Err(e) => return Err(e),
        //     },
        //     Err(e) => return Err(e),
        // };
    }
}

#[derive(Debug)]
struct AssociationList {
    list: Vec<Association>,
}
impl PartialEq for AssociationList {
    fn eq(&self, other: &AssociationList) -> bool {
        if self.len() != other.len() {
            return false;
        }
        // left == right
        let mut idx: usize = 0;
        while idx < self.len() {
            if self.association_at(idx) != other.association_at(idx) {
                return false;
            }
            idx += 1;
        }
        return true;
    }
}
impl AssociationList {
    fn new() -> AssociationList {
        let my_vector: Vec<Association> = Vec::with_capacity(1); //This is not allowed...
        AssociationList { list: my_vector }
    }
    /// Returns the number of associations in this list.
    fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns the space in memory required to serialize this struct.
    fn size(&self) -> usize {
        // (self.len() as usize) * size_of::<Association>()
        (self.len() as usize) * Association::size()
    }

    /// Returns the association at the given index.
    /// (id_1, type, id_2)
    ///
    /// # Costs
    /// Copies: 0
    /// Time: O(1)
    ///
    /// # Arguments
    /// * `idx` - the index of the association to be returned.
    fn association_at(&self, idx: usize) -> &Association {
        &self.list[idx]
    }

    /// Returns a serialized list or an error.
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n) where n is the length of the list.
    fn serialize(&self, bytes: &mut WriteBuf) {
        // let count: usize = self.len() as usize;
        for i in 0..self.len() {
            self.association_at(i).serialize(bytes)
        }
    }

    /// Returns an AssociationList or an error is something goes wrong.
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n) where n is the length of the list.
    fn deserialize(bytes: &[u8]) -> Result<AssociationList, sandstorm::io::Error> {
        let capacity = bytes.len() / Association::size();

        let mut list: Vec<Association> = Vec::with_capacity(capacity);

        let mut start: usize = 0;
        while start < bytes.len() {
            let end = start + Association::size();
            let assoc: Association = Association::deserialize(&bytes[start..end]).unwrap();
            list.push(assoc);
            start += Association::size();
        }
        Ok(AssociationList { list: list })
    }

    /// Removes the association with the given id from this association list.
    /// (id_1, type, id_2)
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n), where n = size of assoc_list
    ///
    /// # Arguments
    /// * `id_2` - the id of the association to be removed.
    fn remove(&mut self, id_2: Id) {
        for pos in 0..self.len() {
            // Find the association.
            if self.association_at(pos).id == id_2 {
                // Shift everything down by one.
                for idx in pos..self.len() - 1 {
                    //re-arrange references.
                    self.list[idx] = Association {
                        id: self.list[idx - 1].id,
                        time: self.list[idx - 1].time,
                    };
                }
                // Remove extra space at end? This looks to copy data but might save space?
                self.list.shrink_to_fit();
                break;
            }
        }
    }

    /// Adds the association to this list.
    /// Farily innefficient to use.
    /// (id_1, type, id_2)
    ///
    /// # Costs
    /// Memory: O(n) -> Shifts values into place.
    /// Time: O(n), where n is the length of AssociationList
    ///
    /// # Arguments
    /// * `association` - the association to be added.
    fn add(&mut self, association: Association) {
        // when associations get added, they get added in order of time. newest -> oldest.
        // Could do a binary search to find insertion point but.. that's probably silly.
        self.list.push(Association {
            id: association.id,
            time: association.time,
        });

        for pos in 0..self.len() {
            if self.association_at(pos).time < association.time {
                // Make room for insertion
                for idx in self.len() - 2..pos + 1 {
                    self.list[idx] = Association {
                        id: self.list[idx + 1].id,
                        time: self.list[idx + 1].time,
                    };
                }
                self.list[pos] = association;
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
    #[test]
    fn ser_dser() {
        let assoc = Association { id: 1, time: 2 };
        let mut data = vec![];
        let assoc_des: Association = match assoc.serialize(&mut data) {
            None => Association::deserialize(data.as_slice()).unwrap(),
            Some(e) => return,
        };
        assert_eq!(assoc, assoc_des);
    }

    #[test]
    fn ser_dser_list() {
        let mut list: Vec<Association> = Vec::with_capacity(1);
        for i in 0..50000 {
            list.push(Association {
                id: i,
                time: i as u64,
            });
        }
        let alist = AssociationList { list: list };

        let mut data = vec![];
        let assoc_des = match alist.serialize(&mut data) {
            None => AssociationList::deserialize(data.as_slice()).unwrap(),
            Some(e) => return,
        };

        assert_eq!(alist, assoc_des);
    }
}
