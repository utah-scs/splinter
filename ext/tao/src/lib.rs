#![crate_type = "dylib"]
#![feature(no_unsafe)]

extern crate sandstorm;

use sandstorm::db::DB;

// Multiple instances of a sinle SO *do* share state.
// Q: Does it interfere with other SOs? And/or with the hosting process?
// No: each SO is in it's own namespace.
// static mut N: u32 = 0;

#[no_mangle]
pub fn init(db: &DB) {
  //let m;
  //unsafe {
  //  m = N;
  //  N +=1;
  //}
  //db.debug_log(&format!("TAO Initialized! {}", m));
  db.debug_log("TAO Initialized!");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
