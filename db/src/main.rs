#[macro_use]
extern crate log;
extern crate db;

use std::time::Instant;

fn main() {
    let mut db = db::DB::default();

    let mut key = Vec::new();
    key.extend_from_slice("longkey_".as_bytes());
    let value = "10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000".as_bytes();

    let n_iters : u32 = 1_000_000;

    let start = Instant::now();
    for i in 0..n_iters {
        key[7] = i as u8 & 0x7f;
        db.put(key.as_slice(), value);
    }
    let put_time = start.elapsed();

    let start = Instant::now();
    let mut s = 0u64;
    for i in 0..n_iters {
        key[7] = i as u8 & 0x7f;
        let b = db.get(key.as_slice()).unwrap()[0] as u64;
        s += b;
        /*
        println!("get('{:?}') = {:?}",
            str::from_utf8(&key).unwrap(),
            str::from_utf8(v).unwrap());
        */
    }
    let get_time = start.elapsed();
    info!("Total: {:?}", put_time + get_time);
    info!("Per Put: {:?}", put_time/(n_iters as u32));
    info!("Per Get: {:?}", get_time/(n_iters as u32));
    info!("Print some stuff to avoid compiler opts: {:?}", s);
}

