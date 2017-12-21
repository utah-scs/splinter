use std::collections::HashMap;
use super::common::*;
use super::rpc::*;
use super::table::*;
use super::service::*;

struct User {
    // TODO(stutsman) Need some form of interior mutability here.
    id: UserId,
    tables: HashMap<TableId, Table>,
}

impl User {
    fn new(id: UserId) -> User {
        User{
            id: id,
            tables: HashMap::new(),
        }
    }

    fn create_table(&mut self, table_id: u32) {
        let table = Table::default();
        self.tables.insert(table_id, table);
    }
}

pub struct Master {
    // TODO(stutsman) Need some form of interior mutability here.
    users: HashMap<UserId, User>,
}

struct GetRequest<'a> {
    key: &'a [u8],
}

impl<'a> GetRequest<'a> {
    fn parse(request: &Request) -> GetRequest {
        GetRequest{
            key: request,
        }
    }
}

impl Master {
    pub fn new() -> Master {
        let mut user = User::new(0);
        user.create_table(0);

        let mut master = Master{
            users: HashMap::new(),
        };

        master.users.insert(user.id, user);

        master
    }

    fn get(&self, key: &[u8]) -> Option<Response> {
        debug!("Servicing get({:?})", key);
        if let Some(ref user) = self.users.get(&0u32) {
            if let Some(ref table) = user.tables.get(&0u32) {
                if let Some(_value) = table.get(key) {
                    None // Create response with value in it.
                } else {
                    None // Key not found response.
                }
            } else {
                None // Create table not found response.
            }
        } else {
            None // Create user not found response.
        }
    }
}

impl Service for Master {
    fn dispatch(&self, request: &BS) -> Option<Response> {
        match parse_opcode(&request) {
            Some(OP_GET) => {
                let args = GetRequest::parse(&request);
                self.get(args.key)
            }
            Some(opcode) => {
                warn!("Master got unexpected opcode {}", opcode);
                None
            }
            _ => {
                warn!("Master got a malformed request");
                None
            }
        }
    }
}

