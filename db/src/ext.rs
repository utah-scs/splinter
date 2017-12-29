extern crate sandstorm;
extern crate libloading as lib;
extern crate spin;

use std::sync::{Arc};
use self::spin::{RwLock};
use std::io::Result;

use std::collections::HashMap;

use super::common::*;

type Procedure<'lib> = lib::Symbol<'lib, unsafe extern fn(&sandstorm::DB)>;
type ProcedureRaw = lib::os::unix::Symbol<unsafe extern fn(&sandstorm::DB)>;

struct Extension {
    library: lib::Library,
    procedure: ProcedureRaw,
}

impl Extension {
    fn load(ext_path: &str) -> Result<Extension> {
        let lib = lib::Library::new(ext_path)?;
        let init: ProcedureRaw;
        unsafe {
            let wrapped : Procedure = lib.get(b"init").unwrap();
            init = wrapped.into_raw();
        }
        Ok(Extension{library: lib, procedure: init})
    }

    fn call(&self, db: &sandstorm::DB) {
        unsafe {
            (self.procedure)(db);
        }
    }
}

pub struct ExtensionManager {
    extensions: RwLock<HashMap<String, Arc<Extension>>>,
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
            self.load(&path, 0, &name).unwrap();
        }
    }

    pub fn load(&self, ext_path: &str, _: UserId, proc_name: &str) -> Result<()> {
        let ext = Arc::new(Extension::load(ext_path)?);
        let mut exts = self.extensions.write();
        exts.insert(String::from(proc_name), ext);
        Ok(())
    }

    pub fn call(&self, db: &sandstorm::DB, _: UserId, proc_name: &str) {
        let exts = self.extensions.read();
        let ext = exts.get(proc_name).unwrap().clone();
        drop(exts);
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

        let expected : Vec<String> = (0..n).map(|_| format!("TAO Initialized! 0")).collect();

        let proc_names : Vec<String> = (0..n).map(|i| format!("tao{}", i)).collect();

        {
            let _x = util::Bench::start(Some("Call all exts"));
            for p in proc_names.iter() {
                m.call(&db, 0, &p);
            }
        }

        db.assert_messages(expected.as_slice());
        db.clear_messages();

        let expected : Vec<String> = (0..n).map(|_| format!("TAO Initialized! 1")).collect();

        let mut c = 0;
        let duration = util::Bench::run(|| {
            for _ in 0..100000 {
                for p in proc_names.iter() {
                    m.call(&db, 0, &p);
                    c = c + 1;
                }
            }
        });
        println!("Time per call {} ns", duration.num_nanoseconds().unwrap() / c);

        db.assert_messages(expected.as_slice());
    }
}
