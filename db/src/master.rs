extern crate sandstorm;

use std::collections::HashMap;

use super::common::*;
use super::rpc::*;
use super::table::*;
use super::service::*;
use super::ext::*;

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
    extensions: ExtensionManager,
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

struct PutRequest<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> PutRequest<'a> {
    fn parse(request: &Request) -> PutRequest {
        PutRequest{
            key: request,
            value: request,
        }
    }
}

impl Master {
    pub fn new() -> Master {
        let mut user = User::new(0);
        user.create_table(0);

        let mut master = Master{
            users: HashMap::new(),
            extensions: ExtensionManager::new(),
        };

        master.users.insert(user.id, user);

        master
    }

    fn get(&self, key: &[u8], mut response: Response) -> Option<Response> {
        debug!("Servicing get({:?})", key);
        if let Some(ref user) = self.users.get(&0u32) {
            if let Some(ref table) = user.tables.get(&0u32) {
                if let Some(value) = table.get(key) {
                    debug!("Fetching key {:?}", key);
                    // TODO(stutsman) fill the response directly in table get.
                    response.extend(value.into_iter());
                    Some(response)
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

    fn put(&self, key: &[u8], value: &[u8], mut response: Response) -> Option<Response> {
        debug!("Servicing put({:?})", key);
        if let Some(ref user) = self.users.get(&0u32) {
            if let Some(ref table) = user.tables.get(&0u32) {
                debug!("Storing under key {:?}", key);
                table.put("\x01".as_bytes(), value); // TODO(stutsman) Temp hack to make gets find something.
                Some(response)
            } else {
                None // Create table not found response.
            }
        } else {
            None // Create user not found response.
        }
    }

    pub fn test_exts(&self) {
        self.extensions.load_test_modules(1);
        self.extensions.call(self, 0, "tao0");
    }
}

impl Service for Master {
    fn dispatch(&self, request: &Request, response: Response) -> Option<Response> {
        match parse_opcode(&request) {
            Some(OP_GET) => {
                let args = GetRequest::parse(&request);
                self.get(args.key, response)
            }
            Some(OP_PUT) => {
                let args = PutRequest::parse(&request);
                self.put(args.key, args.value, response)
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

impl sandstorm::DB for Master {
    // TODO(stutsman): Clearly the DB needs a way to find the calling extension information. We can
    // force them to hand it to us, or we can track it in e.g. TLS.
    fn debug_log(&self, message: &str) {
        info!("EXT {}", message);
    }
}

