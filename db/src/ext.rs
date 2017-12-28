extern crate sandstorm;
extern crate libloading as lib;

use std::sync::{Arc, RwLock};
use std::io::Result;

use std::collections::HashMap;

use super::common::*;

type Procedure<'lib> = lib::Symbol<'lib, unsafe extern fn(&sandstorm::DB)>;

struct Extension {
    library: lib::Library,
    //procedure: Option<Procedure<'lib>>,
}

impl Extension {
    fn load(ext_path: &str) -> Result<Extension> {
        let lib = lib::Library::new(ext_path)?;
        Ok(Extension{library: lib})
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

    pub fn load_one_test_module(&self) {
        self.load( "../ext/tao/target/release/deps/libtao.so", 0, "tao")
            .or(self.load( "../ext/tao/target/debug/deps/libtao.so", 0, "tao"))
            .unwrap();
    }

    pub fn load_many_test_modules(&self, n: usize) {
        let exts : Vec<_> = (0..n)
            .map(|i| (format!("../ext/tao/target/release/deps/libtao{}.so", i),
                                            format!("tao{}", i))).collect();

        for (path, name) in exts {
            self.load(&path, 0, &name);
        }
    }

    pub fn load(&self, ext_path: &str, user_id: UserId, proc_name: &str) -> Result<()> {
        let ext = Arc::new(Extension::load(ext_path)?);
        let mut exts = self.extensions.write().unwrap();
        exts.insert((user_id, String::from(proc_name)), ext);
        Ok(())
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
    extern crate time;

    #[test]
    fn ext_basic() {
        //let db = sandstorm::MockDB::new();
        let db = sandstorm::NullDB::new();
        let m = ExtensionManager::new();

        let n = 100;

        {
            // WATCH OUT let _ doesn't hold drop to end of block: #10488.
            let _x = util::Bench::start(Some("Load all exts"));
            m.load_many_test_modules(n);
        }

        let expected : Vec<String> = (0..n).map(|i| format!("TAO Initialized! 0")).collect();

        let proc_names : Vec<String> = (0..n).map(|i| format!("tao{}", i)).collect();

        {
            let _x = util::Bench::start(Some("Call all exts"));
            for p in proc_names.iter() {
                m.call(&db, 0, &p);
            }
        }

        db.assert_messages(expected.as_slice());
        db.clear_messages();

        let expected : Vec<String> = (0..n).map(|i| format!("TAO Initialized! 1")).collect();
        {
            let _x  = util::Bench::start(Some("Call all exts again"));
            for _ in 0..1000 {
                for p in proc_names.iter() {
                    m.call(&db, 0, &p);
                }
            }
        }
        db.assert_messages(expected.as_slice());
    }
}
