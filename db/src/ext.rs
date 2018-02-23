use std::sync::{Arc};
use std::io::Result;
use std::collections::HashMap;

use super::common::*;

use spin::{RwLock};
use sandstorm::db::DB;
use libloading as lib;

type Procedure<'lib> = lib::Symbol<'lib, unsafe extern fn(&DB)>;
type ProcedureRaw = lib::os::unix::Symbol<unsafe extern fn(&DB)>;

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

    fn call(&self, db: &DB) {
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

    pub fn call(&self, db: &DB, _: UserId, proc_name: &str) {
        let exts = self.extensions.read();
        let ext = exts.get(proc_name).unwrap().clone();
        drop(exts);
        ext.call(db)
    }
}
