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

    pub fn load_test_modules(&self, n: usize) {
        let exts : Vec<_> = (0..n).map(|i| ("../ext/tao/target/debug/libtao.so", format!("tao{}", i))).collect();

        for (path, name) in exts {
            println!("{}", path);
            self.load(path, 0, &name);
        }
    }

    pub fn load(&self, ext_path: &str, user_id: UserId, proc_name: &str) {
        let ext = Arc::new(Extension::load(ext_path));
        let mut exts = self.extensions.write().unwrap();
        exts.insert((user_id, String::from(proc_name)), ext);
    }

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

        let n = 10000;

        m.load_test_modules(n);

        let expected : Vec<String> = (0..n).map(|i| format!("TAO Initialized! {}", i)).collect();

        let proc_names : Vec<String> = (0..n).map(|i| format!("tao{}", i)).collect();

        {
            let _ = util::Bench::start(Some("Call all exts"));
            for p in proc_names {
                m.call(&db, 0, &p);
            }
        }

        db.assert_messages(expected.as_slice());
    }
}
