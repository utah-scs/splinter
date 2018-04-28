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

use std::rc::Rc;
use std::sync::Arc;
use std::ops::Generator;
use std::collections::HashMap;

use super::common::TenantId;

use spin::RwLock;
use sandstorm::db::DB;
use libloading::Library;
use libloading::os::unix::Symbol;

// Number of buckets in the `extensions` hashmap in Extension Manager.
const EXT_BUCKETS: usize = 32;

// The type signature of the function that will be searched for inside an so.
type Proc = unsafe extern "C" fn(Rc<DB>) -> Box<Generator<Yield = u64, Return = u64>>;

/// This type represents an extension that has been successfully loaded into
/// the database. As long as this type is not dropped, the extension will exist
/// inside the database's address space, and can be called into.
#[allow(dead_code)]
pub struct Extension {
    // A wrapper around the entire dynamically loaded library corresponding
    // to this extension. While this member is not used once the extension
    // has been loaded, it needs to be kept around to guarantee that the
    // extension is never unloaded while it is still in use.
    library: Library,

    // The actual symbol inside the dynamically loaded library that will be
    // used by the database during an "invoke".
    procedure: Symbol<Proc>,
}

// Implementation of methods on Extension.
impl Extension {
    /// This function loads an .so file containing a symbol called "init" into
    /// the database. It returns a handle that can be used to retrieve a
    /// generator from the loaded so.
    ///
    /// # Safety
    ///
    /// The safety of this method depends on how the .so file was compiled. As
    /// long as it was compiled with the rust compiler, and contains no unsafe
    /// blocks, the returned extension can be safely called into. The rust
    /// compiler will guarantee that there are no NULL pointers etc inside the
    /// .so file.
    ///
    /// # Arguments
    ///
    /// * `name`: The path (absolute or relative) of the .so file to be loaded.
    ///
    /// # Return
    ///
    /// An `Extension` if the .so file was found, and contains a symbol called
    /// "init". This handle can then be used to call into the so.
    pub fn load(name: &str) -> Option<Extension> {
        // First, try to dynamically load the .so file into the database.
        if let Ok(lib) = Library::new(name) {
            // If the load was successfull, try to find a function called
            // "init" inside the .so file.
            let mut procedure = None;
            unsafe {
                if let Ok(ext) = lib.get::<Proc>(b"init") {
                    // If the "init" function was found, then unwrap it.
                    procedure = Some(ext.into_raw());
                }
            }

            // If the init function was unwrapped, return an extension.
            if let Some(procedure) = procedure {
                return Some(Extension {
                    library: lib,
                    procedure: procedure,
                });
            }
        }

        // Failed to load the .so file, or failed to find the procedure within.
        return None;
    }

    /// This function calls into a previously loaded extension, and returns a
    /// generator that can be scheduled by the database.
    ///
    /// # Arguments
    ///
    /// * `db`: A ref-counted type that implements the `DB` interface that the
    ///         returned generator will be initialized with.
    ///
    /// # Return
    ///
    /// A generator that can be scheduled by the database.
    pub fn get(&self, db: Rc<DB>) -> Box<Generator<Yield = u64, Return = u64>> {
        // Call into the procedure, and return the generator.
        unsafe { (self.procedure)(db) }
    }
}

/// This type represents an extension manager which keeps track of extensions
/// in the database, and the tenants that own them.
pub struct ExtensionManager {
    // A simple map from tenants and extension names to extensions.
    extensions: [RwLock<HashMap<(TenantId, String), Arc<Extension>>>; EXT_BUCKETS],
}

// Implementation of methods on ExtensionManager.
impl ExtensionManager {
    /// This function returns an extension manager that can be used to load
    /// and retrieve extensions from the database.
    ///
    /// # Return
    ///
    /// An `ExtensionManager`.
    pub fn new() -> ExtensionManager {
        ExtensionManager {
            // Can't use the copy constructor because of the Arc<Extension>.
            extensions: [
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
                RwLock::new(HashMap::new()),
            ],
        }
    }

    /// This method loads an extension for a particular tenant into the
    /// database.
    ///
    /// # Arguments
    ///
    /// * `path`:   The path (absolute or relative) of the .so file containing
    ///             the extension.
    /// * `tenant`: The tenant owning the extension.
    /// * `name`:   The name of the extension. Subsequent calls to get must use
    ///             this name.
    ///
    /// # Return
    ///
    /// True if the extension was successfully loaded. False otherwise.
    pub fn load(&self, path: &str, tenant: TenantId, name: &str) -> bool {
        // Try to load the extension from the supplied path.
        Extension::load(path)
                    // If the extension was loaded successfully, write it into
                    // the extension manager. The bucket is determined by the
                    // least significant byte of the tenant id.
                    .and_then(| ext | {
                        let bucket = (tenant & 0xff) as usize & (EXT_BUCKETS - 1);
                        self.extensions[bucket].write()
                                        .insert((tenant, String::from(name)),
                                                Arc::new(ext));
                        Some(()) })
                    .is_some()
    }

    /// This method retrieves an extension that was previously loaded into the
    /// database.
    ///
    /// # Arguments
    ///
    /// * `tenant`: The tenant owning the extension.
    /// * `name`:   The name of the extension.
    ///
    /// # Return
    ///
    /// A ref-counted handle to the extension if it was found.
    pub fn get(&self, tenant: TenantId, name: &str) -> Option<Arc<Extension>> {
        // Lookup the extension, if it exists, bump up it's refcount, and
        // return it. The bucket is determined by the least significant byte
        // of the tenant id.
        let bucket = (tenant & 0xff) as usize & (EXT_BUCKETS - 1);
        self.extensions[bucket]
            .read()
            .get(&(tenant, String::from(name)))
            .and_then(|ext| Some(Arc::clone(&ext)))
    }

    /// Shares a previously loaded extension with another tenant.
    ///
    /// # Arguments
    ///
    /// * `owner`: The tenant that owns the extension.
    /// * `share`: The tenant the extension must be shared with.
    /// * `name`:  The name of the extension to be shared.
    ///
    /// # Return
    ///
    /// True, if the extension was successfully shared. False, if it was not found.
    pub fn share(&self, owner: TenantId, share: TenantId, name: &str) -> bool {
        // First, try to retrieve a copy (Arc) of the extension from the owner.
        // If successfull, then share it with the tenant identified by `share`.
        self.get(owner, name)
            .and_then(|ext| {
                let bucket = (share & 0xff) as usize & (EXT_BUCKETS - 1);
                self.extensions[bucket]
                    .write()
                    .insert((share, String::from(name)), ext);
                Some(())
            })
            .is_some()
    }
}

// This module contains simple tests for Extension and ExtensionManager.
#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::ops::GeneratorState;

    use sandstorm::null::NullDB;
    use super::{Extension, ExtensionManager};

    // This function attempts to load and run a test extension, and asserts
    // that both operations were successfull.
    #[test]
    fn test_ext_load() {
        // Load and retrieve a generator from a test extension.
        let ext = Extension::load("../ext/test/target/release/libtest.so").unwrap();
        let mut gen = ext.get(Rc::new(NullDB::new()));

        // Assert that the test extension has one yield statement.
        unsafe { assert_eq!(GeneratorState::Yielded(0), gen.resume()) };
        unsafe { assert_eq!(GeneratorState::Complete(0), gen.resume()) };
    }

    // This function tests that an extension without the "init" symbol cannot
    // be loaded.
    #[test]
    #[should_panic]
    fn test_ext_load_err_init() {
        Extension::load("../ext/err/target/release/liberr.so").unwrap();
    }

    // This function tests that a non-existent extension cannot be loaded.
    #[test]
    #[should_panic]
    fn test_ext_load_err_file() {
        Extension::load("../ext/err/target/release/libxyz.so").unwrap();
    }

    // This function tests that the extension manager can load a valid
    // extension.
    #[test]
    fn test_man_load() {
        let man = ExtensionManager::new();
        assert!(man.load("../ext/test/target/release/libtest.so", 0, "test"));
    }

    // This function tests that the extension manager cannot load an extension
    // without the "init" symbol.
    #[test]
    fn test_man_load_err_init() {
        let man = ExtensionManager::new();
        assert!(!man.load("../ext/test/target/release/liberr.so", 0, "err"));
    }

    // This function tests that the extension manager cannot load an extension
    // that does not exist.
    #[test]
    fn test_man_load_err_file() {
        let man = ExtensionManager::new();
        assert!(!man.load("../ext/test/target/release/libxyz.so", 0, "xyz"));
    }

    // This function tests that a previously loaded extension can be retrieved
    // from the extension manager and run.
    #[test]
    fn test_man_get() {
        // Load an extension into the extension manager.
        let man = ExtensionManager::new();
        assert!(man.load("../ext/test/target/release/libtest.so", 0, "test"));

        // Retrieve the extension, and the generator.
        let ext = man.get(0, "test").unwrap();
        let mut gen = ext.get(Rc::new(NullDB::new()));

        // Assert that the test extension has one yield statement.
        unsafe { assert_eq!(GeneratorState::Yielded(0), gen.resume()) };
        unsafe { assert_eq!(GeneratorState::Complete(0), gen.resume()) };
    }

    // This function tests that a non-existent extension cannot be retrieved
    // from the extension manager.
    #[test]
    #[should_panic]
    fn test_man_get_err() {
        let man = ExtensionManager::new();
        man.get(0, "test").unwrap();
    }
}
