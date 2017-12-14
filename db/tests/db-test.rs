extern crate db;

use std::time::Instant;
use db::DB;

//fn parallel_bench<F>(n_threads: usize, f: F)
//    where F: FnOnce() -> () + Clone + Send
//{
//    let mut threads = Vec::with_capacity(n_threads);
//    let barrier = Arc::new(Barrier::new(n_threads));
//
//    for _ in 0..n_threads {
//        let b = barrier.clone();
//    }
//
//    for thread in threads {
//        thread.join().expect("Thread join failed in parallel_bench test.");
//    }
//}
//
//#[test]
//fn db_parallel_get() {
//    parallel_bench(4,
//                  move || {
//                       println!("Alive!");
//                   });
//}

#[test]
fn db_get() {
    let mut db = DB::default();

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
    let mut s = 0;
    for i in 0..n_iters {
        key[7] = i as u8 & 0x7f;
        db.put(key.as_slice(), value);
        s += db.get(key.as_slice()).unwrap()[0];
        /*
        println!("get('{:?}') = {:?}",
            str::from_utf8(&key).unwrap(),
            str::from_utf8(v).unwrap());
        */
    }
    let get_time = start.elapsed();
    println!("Total: {:?}", put_time + get_time);
    println!("Per Put: {:?}", put_time/(n_iters as u32));
    println!("Per Get: {:?}", get_time/(n_iters as u32));
    println!("Print some stuff to avoid compiler opts: {:?}", s);

    // Why is this ok? Can't a put to a key screw me? In that case do
    // we/how would we get a runtime exception or segfault??
   
    db.put(key.as_slice(), value);
    s += db.get(key.as_slice()).unwrap()[0];
    println!("Print some stuff to avoid compiler opts: {:?}", s);
}

