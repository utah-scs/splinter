extern crate sandstorm;
extern crate libloading as lib;

use std::sync::{Arc, RwLock};
use std::collections::HashMap;

use super::common::*;

type Procedure<'lib> = lib::Symbol<'lib, unsafe extern fn(&sandstorm::DB)>;

struct Extension {
    library: lib::Library,
    //procedure: Option<Procedure<'lib>>,
}

impl Extension {
    fn load(ext_path: &str) -> Extension {
        let lib = lib::Library::new(ext_path).unwrap();
        Extension{library: lib}
    }

    fn call(&self, db: &sandstorm::DB) {
        let init: Procedure;
        unsafe {
            init = self.library.get(b"init").unwrap();
            init(db);
        }
    }
}

pub struct ExtensionManager {
    extensions: RwLock<HashMap<(UserId, String), Arc<Extension>>>,
}

impl ExtensionManager {
    pub fn new() -> ExtensionManager {
        ExtensionManager{extensions: RwLock::new(HashMap::new())}
    }

    pub fn load_test_modules(&self) {
        let exts = vec![("../ext/tao/target/debug/libtao.so", "tao")];

        for (path, name) in exts {
            println!("{}", path);
            self.load(path, 0, name);
        }
    }

    pub fn load(&self, ext_path: &str, user_id: UserId, proc_name: &str) {
        let ext = Arc::new(Extension::load(ext_path));
        let mut exts = self.extensions.write().unwrap();
        exts.insert((user_id, String::from(proc_name)), ext);
    }

    // TODO(stutsman) Use interior mutability here.
    pub fn call(&self, db: &sandstorm::DB, user_id: UserId, proc_name: &str) {
        let exts = self.extensions.read().unwrap();
        // TODO(stutsman) Pointless malloc here for string.
        let ext = exts.get(&(user_id, String::from(proc_name))).unwrap();
        ext.call(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_basic() {
        let db = sandstorm::MockDB::new();
        let m = ExtensionManager::new();

        m.load_test_modules();
        m.call(&db, 0, "tao");

        db.assert_messages(&vec!["TAO Initialized! 0"]);
    }
}
