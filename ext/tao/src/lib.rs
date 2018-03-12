#![crate_type = "dylib"]
#![feature(no_unsafe)]

extern crate sandstorm;
extern crate byteorder;

// use super::buf::{ReadBuf, WriteBuf};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use sandstorm::DB;
use std::result::Result;
use std::time::{SystemTime, UNIX_EPOCH};

type Id = u64;
type ObjectType = u16;
type AssociationType = u16;
type Time = u64;


pub struct Tao<'a> {
    client: &'a sandstorm::MockDB,
    object_table_id: u64,
    association_table_id: u64,
    next_id: Id
}

impl<'a> Tao<'a>  {
    /// Returns a TAO instance connecting to the given client.
    ///
    /// # Arguments
    /// * `client` - Access to a sandstorm::DB in which to add DB info to.
    /// * `object_table_id` - table id for the object table associated with this TAO instance.
    /// * `association_table_id` - table id for the association table associated with this TAO instance.
    pub fn new(client: &sandstorm::MockDB, object_table_id: u64, association_table_id: u64) -> Tao {
        //Create a table if they don't already exist.
        client.create_table(object_table_id);
        client.create_table(association_table_id);

        Tao {
            client,
            object_table_id,
            association_table_id,
            next_id: 1 //TODO: this is invalid if the user is creating a new TAO instance, for existing tables. This need to be read from DB.
        }
    }

    /// Returns the id of the newly created object.
    ///
    /// # Arguments
    /// * `object_type` - Type of the object being added.
    /// * 'data' - kvpairs which make up the object.
    pub fn object_add(&mut self, otype: ObjectType, data: &[u8]) -> Id {
        let object_id: Id = self.allocate_unique_id();

        self.object_update(object_id, otype, data);

        object_id
    }

    /// Updates the object with the given id and type to contain the data provided.
    ///
    /// # Arguments
    /// * `id` - id of the object to be updated.
    /// * `otype` - type of the object to be updated.
    /// * `data` - updated data to replace current data with.
    pub fn object_update(&self, id: Id, otype: ObjectType, data: &[u8]){
        let space_needed = ObjectHeader::size() + data.len();
        let mut container = match self.client.alloc(self.object_table_id, &id, space_needed){
            None => return,
            Some(o) => o,
        };

        //put the header into container
        let header: ObjectHeader = ObjectHeader {
            otype
        };
        match header.serialize(&mut container) {
            None => (),
            Some(_) => return,
        };

        //put the data into container
        container.extend_from_slice(data);

        self.client.put_key(self.object_table_id, &id, &container);
    }

    /// Deletes the object with the given id.
    ///
    /// # Arguments
    /// * `id` - id of the object to be created.
    pub fn object_delete(&self, id: Id){
        self.client.delete_key(self.object_table_id, &id);
    }

    /// Gets the data for the object with the given id. Returns the type of the object.
    /// Puts object data into the slice provided.
    ///
    /// # Arguments
    /// * `id` - id of the object to get.
    /// * `data` - a container to put the data in.
    pub fn object_get(&self, id: Id, mut data: Vec<u8>) -> ObjectType {
        let object: &Vec<u8> = match self.client.get_key(self.object_table_id, &id) {
            Ok(v) => v,
            Err(_) => return 0,
        };

        //  [..header..|.........object data.........]
        let size_of_header = ObjectHeader::size();

        //Fill data.
        data.extend_from_slice(&object[size_of_header..object.len()-1]);

        //Return type
        match ObjectHeader::deserialize(&object[0..size_of_header]){
            Ok(v) => return v.otype,
            Err(_) => return 0
        }
    }


    /// Adds the given Association (id1, type, id2) to the AssociationList (id1, type) if one exists.
    /// Otherwise, creates a new AssociationList and populates it with the given Association.
    ///
    /// # Arguments
    /// * `id1` - the id of the first object in this Association.
    /// * `association_type` - the type of this association.
    /// * `id2` - the id of the second object in this Association.
    pub fn assocation_add(&self, id1: Id, association_type: AssociationType, id2: Id){
        //Add the association to the table. (id1, atype, id2)<key> -> data<value>.
        let new_assoc = Association{
            id: id2,
            time: self.current_time()
        };
        let assoc_key = AssociationKey{
            id1: id1,
            atype: association_type,
            id2: id2
        };
        //TODO: Serialize keys

        let space_needed = Association::size();
        let mut assoc_container = match self.client.alloc(self.association_table_id, &assoc_key, space_needed){
            None => return,
            Some(o) => o,
        };

        new_assoc.serialize(&mut assoc_container);

        self.client.put_key(self.association_table_id, &assoc_key, &assoc_container);


        // Add the association to the list. (id1, atype) -> (id2)
        // To do this, assume the list exists. if it doesn't exist, add our entry and add the list
        // to the db.
        let list_key = AssociationListKey{
            id1: id1,
            atype: association_type
        };

        let list_serialized: &[u8] = match self.client.get_key(self.association_table_id, &list_key){
            Ok(&v) => v,
            Err(_) => return //TODO: create a new list.
        };

        let mut list = match AssociationList::deserialize(list_serialized) {
            Ok(v) => v,
            Err(_) => return
        };

        list.add(new_assoc);

        let new_size = list.size() + Association::size();
        let mut list_container = match self.client.alloc(self.association_table_id, &list_key, new_size){
            None => return,
            Some(o) => o,
        };

        list.serialize(&mut list_container);

        self.client.put_key(self.association_table_id, &list_key, &list_container);
    }

    /// Deletes the Association (id1, type, id2) and removes it from the List.
    ///
    /// # Arguments
    /// * `id1` - the id of the first object in this Association.
    /// * `association_type` - the type of this association.
    /// * `id2` - the id of the second object in this Association.
    pub fn association_delete(&self, id1: Id, association_type: AssociationType, id2: Id){
        let assoc_key = AssociationKey{
            id1: id1,
            atype: association_type,
            id2: id2
        };

        self.client.delete_key(self.association_table_id, &assoc_key);

        let space_needed = Association::size();
        let mut container = match self.client.alloc(self.object_table_id, &id1, space_needed){
            None => return,
            Some(o) => o,
        };

        match assoc_key.serialize(&mut container) {
            None => (),
            Some(_) => return
        };
        //TODO: Remove from list.
    }

    /// Returns the Association (id1, type, id2)
    ///
    /// # Arguments
    /// * `id1` - the id of the first object in this Association.
    /// * `association_type` - the type of this association.
    /// * `id2` - the id of the second object in this Association.
    pub fn association_get(&self, id1: Id, association_type: AssociationType, id2: Id) -> Association {
        let assoc_key = AssociationKey{
            id1: id1,
            atype: association_type,
            id2: id2
        };
        //TODO: Serialize assoc_key

        let assoc_serialized: &Vec<u8> = match self.client.get_key(self.association_table_id, &assoc_key) {
            Ok(v) => v,
            Err(_) => return Association {id: 0, time: 0},
        };

        match Association::deserialize(assoc_serialized){
            Ok(v) => return v,
            Err(_) => return Association {id: 0, time: 0}
        }
    }

    /// Returns seconds since unix epoch.
    fn current_time(&self) -> Time {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("invalid time");
        now.as_secs()
    }

    // Returns a simple unique integer.
    fn allocate_unique_id(&mut self) -> Id {
        self.next_id += 1;
        self.next_id
    }
}

struct ObjectHeader {
    otype: ObjectType
}
impl ObjectHeader {
    /// Returns the space in memory required to serialize this struct.
    fn size() -> usize {
        let otype_size = std::mem::size_of::<ObjectType>();
        otype_size
    }

    fn serialize(&self, bytes: &mut Vec<u8>) -> Option<std::io::Error>{
        let res = bytes.write_u16::<LittleEndian>(self.otype);
        match res{
            Ok(_) => return None,
            Err(e)=> return Some(e),
        };
    }

    fn deserialize(mut bytes: &[u8]) -> Result<ObjectHeader, std::io::Error> {
        let obj_type = match bytes.read_u16::<LittleEndian>() {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        Ok(ObjectHeader {
            otype: obj_type,
        })
    }
}


struct AssociationListKey {
    id1: Id,
    atype: AssociationType
}
impl AssociationListKey {
    /// Returns the space in memory required to serialize this struct.
    fn size() -> usize {
        let id1_size = std::mem::size_of::<Id>();
        let atype_size = std::mem::size_of::<AssociationType>();
        id1_size + atype_size
    }

    fn serialize(&self, bytes: &mut Vec<u8>) -> Option<std::io::Error>{
        bytes.write_u64::<LittleEndian>(self.id1);
        bytes.write_u16::<LittleEndian>(self.atype);
        None

        // match bytes.write_u64::<LittleEndian>(self.id1) {
        //     Ok(v) =>{
        //         match bytes.write_u16::<LittleEndian>(self.atype) {
        //             Ok(v) => return None,
        //             Err(e) => return Some(e),
        //         }
        //     },
        //     Err(e) => return Err(e),
        // };
    }

    fn deserialize(mut bytes: &[u8]) -> Result<AssociationListKey, std::io::Error> {
        Ok(AssociationListKey {
            id1: bytes.read_u64::<LittleEndian>().unwrap(),
            atype: bytes.read_u16::<LittleEndian>().unwrap(),
        })
        // match bytes.read_u64::<LittleEndian>() {
        //     Ok(id) => match bytes.read_u16::<LittleEndian>() {
        //         Ok(atype) => return Ok(AssociationListKey {
        //             id1: id,
        //             atype: atype,
        //         }),
        //         Err(e) => return Err(e),
        //     },
        //     Err(e) => return Err(e),
        // };
    }
}

struct AssociationKey {
    id1: Id,
    atype: AssociationType,
    id2: Id
}
impl AssociationKey {
    /// Returns the space in memory required to serialize this struct.
    fn size() -> usize {
        let id_size = std::mem::size_of::<Id>();
        let atype_size = std::mem::size_of::<AssociationType>();
        id_size + atype_size + id_size
    }

    fn serialize(&self, bytes: &mut Vec<u8>) -> Option<std::io::Error>{
        bytes.write_u64::<LittleEndian>(self.id1);
        bytes.write_u16::<LittleEndian>(self.atype);
        bytes.write_u64::<LittleEndian>(self.id2);
        None
        // match bytes.write_u64::<LittleEndian>(self.id1){
        //     Ok(v) => {
        //         match bytes.write_u16::<LittleEndian>(self.atype) {
        //             Ok(v) => {
        //                 match bytes.write_u64::<LittleEndian>(self.id2) {
        //                     Ok(v) => Ok(bytes),
        //                     Err(e) => Err(e),
        //                 }
        //             },
        //             Err(e) => Err(e),
        //         }
        //     },
        //     Err(e) => Err(e),
        // }
    }

    fn deserialize(mut bytes: &[u8]) -> Result<AssociationKey, std::io::Error> {
        Ok(AssociationKey {
            id1: bytes.read_u64::<LittleEndian>().unwrap(),
            atype: bytes.read_u16::<LittleEndian>().unwrap(),
            id2: bytes.read_u64::<LittleEndian>().unwrap(),
        })
        // match bytes.read_u64::<LittleEndian>() {
        //     Ok(id1) => match bytes.read_u16::<LittleEndian>() {
        //         Ok(atype) => match bytes.read_u64::<LittleEndian>() {
        //             Ok(id2) => return Ok(AssociationKey {
        //                 id1: id1,
        //                 atype: atype,
        //                 id2: id2
        //             }),
        //             Err(e) => return Err(e),
        //         },
        //         Err(e) => return Err(e),
        //     },
        //     Err(e) => return Err(e),
        // };
    }
}

#[derive(Debug)]
pub struct Association {
    id: Id,
    time: Time
}
impl PartialEq for Association {
    fn eq(&self, other: &Association) -> bool {
        self.id == other.id
    }
}
impl Association {
    /// Returns the space in memory required to serialize this struct.
    fn size() -> usize {
        let id_size = std::mem::size_of::<Id>();
        let time_size = std::mem::size_of::<Time>();
        id_size + time_size
    }

    fn serialize(&self, bytes: &mut Vec<u8>) -> Option<std::io::Error>{
        bytes.write_u64::<LittleEndian>(self.id);
        bytes.write_u64::<LittleEndian>(self.time);
        None
    }

    fn deserialize(mut bytes: &[u8]) -> Result<Association, std::io::Error> {
        Ok(Association {
            id: bytes.read_u64::<LittleEndian>().unwrap(),
            time: bytes.read_u64::<LittleEndian>().unwrap()
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
struct AssociationList  {
    //TODO: Refactor to be a serialized list.
    list: Vec<Association>
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
        let my_vector: Vec<Association> = std::vec::Vec::with_capacity(1); //This is not allowed...
        AssociationList {
            list: my_vector
        }
    }
    /// Returns the number of associations in this list.
    fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns the space in memory required to serialize this struct.
    fn size(&self) -> usize {
        (self.len() as usize) * std::mem::size_of::<Association>()
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
    fn association_at(& self, idx: usize) -> &Association {
        &self.list[idx]
    }

    /// Returns a serialized list or an error.
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n) where n is the length of the list.
    fn serialize(&self, bytes: &mut Vec<u8>) -> Option<std::io::Error>{
        // let count: usize = self.len() as usize;
        for i in 0..self.len() {
            match self.association_at(i).serialize(bytes) {
                None => continue, //Do nothing because we have already written to bytes.
                Some(e) => return Some(e),
            };
        }
        None
    }

    /// Returns an AssociationList or an error is something goes wrong.
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n) where n is the length of the list.
    fn deserialize(bytes: &[u8]) -> Result<AssociationList, std::io::Error> {
        let capacity = bytes.len()/Association::size();

        // TODO: fix this with refactoring.
        let mut list: Vec<Association> = std::vec::Vec::with_capacity(capacity); //This isn't allowed.

        let mut start: usize = 0;
        while start < bytes.len() {
            let end = start + Association::size();
            let assoc: Association = Association::deserialize(&bytes[start..end]).unwrap();
            list.push(assoc);
            start += Association::size();
        }
        Ok(AssociationList{
            list: list
        })
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
    fn remove(&mut self, id_2: Id){
        for pos in 0..self.len() {
            // Find the assocation.
            if self.association_at(pos).id == id_2 {
                // Shift everything down by one.
                for idx in pos..self.len()-1 {
                    //re-arrange references.
                    self.list[idx] = Association{
                        id: self.list[idx - 1].id,
                        time: self.list[idx - 1].time
                    };
                }
                // Remove extra space at end? This looks to copy data but might save space?
                self.list.shrink_to_fit();
                break;
            }
        }
    }

    /// Removes the association with the given id from this association list and serializes the list.
    /// This method is faster than calling remove and then serialize back to back.
    /// (id_1, type, id_2)
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n), where n = size of assoc_list
    ///
    /// # Arguments
    /// * `id_2` - the id of the association to be removed.
    fn remove_and_serialize(&mut self, id_2: Id, bytes: &mut Vec<u8>) ->  Option<std::io::Error> {
        let mut pos = 0;
        while pos < self.len() {
            if self.association_at(pos).id != id_2 {
                match self.association_at(pos).serialize(bytes) {
                    None => continue,
                    Some(e) => return Some(e),
                };
            }
            else {
                //Dont serialize the item we are removing. Simply skip over it.
            }
            pos += 1;
        }
        None
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
        self.list.push(Association{
                            id: association.id,
                            time: association.time
                            }
                    );

        for pos in 0..self.len() {
            if self.association_at(pos).time < association.time {
                // Make room for insertion
                for idx in self.len()-2..pos+1 {
                    self.list[idx] = Association{
                        id: self.list[idx + 1].id,
                        time: self.list[idx + 1].time
                    };
                }
                self.list[pos] = association;
                return;
            }
        }
    }

    /// Adds the association to this list and serializes the list.
    /// It is much faster to call this function then add and serialize separately.
    /// (id_1, type, id_2)
    ///
    /// # Costs
    /// Memory: O(n) -> Allocates structure to return.
    /// Time: O(n), where n is the length of AssociationList
    ///
    /// # Arguments
    /// * `association` - the association to be added.
    fn add_and_serialize(&mut self, association: Association, bytes: &mut Vec<u8>) -> Option<std::io::Error> {
        // when associations get added, they get added in order of time. newest -> oldest.
        let mut pos = 0;
        while pos < self.size() {
            if self.association_at(pos).time < association.time {
                // insert
                match association.serialize(bytes) {
                    None => continue,
                    Some(e) => return Some(e),
                };
            }
            else {
                // shift down
                match self.association_at(pos).serialize(bytes) {
                    None => pos += 1, //continue
                    Some(e) => return Some(e),
                };
            }
        }
        None
    }
}

/********************************** TESTING **************************************/

// Multiple instances of a sinle SO *do* share state.
// Q: Does it interfere with other SOs? And/or with the hosting process?
// No: each SO is in it's own namespace.
// static mut N: u32 = 0;
#[no_mangle]
pub fn init(db: &sandstorm::MockDB) {
  //let m;
  //unsafe {
  //  m = N;
  //  N +=1;
  //}
  // db.debug_log(&format!("TAO Initialized! {}", m));
  // db.debug_log("TAO Initialized");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
    #[test]
    fn ser_dser(){
        let assoc = Association {
            id : 1,
            time : 2
        };
        let mut data = vec![];
        let assoc_des: Association = match assoc.serialize(&mut data) {
            None => Association::deserialize(data.as_slice()).unwrap(),
            Some(e) => return,
        };
        assert_eq!(assoc, assoc_des);
    }

    #[test]
    fn ser_dser_list(){
        let mut list: Vec<Association> = std::vec::Vec::with_capacity(1);
        for i in 0..50000 {
            list.push(Association {
                id: i,
                time: i as u64
            });
        }
        let alist = AssociationList{
            list: list
        };

        let mut data = vec![];
        let assoc_des = match alist.serialize(&mut data) {
            None => AssociationList::deserialize(data.as_slice()).unwrap(),
            Some(e) => return,
        };

        assert_eq!(alist, assoc_des);
    }
}
