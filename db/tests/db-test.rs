extern crate db;

use std::thread;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;
use db::DB;

fn parallel_bench(n_threads: usize,
                  setup: fn (&mut DB) -> (),
                  run: fn (Arc<Barrier>, Arc<Mutex<DB>>)) -> ()
{
    let mut db = DB::default();
    setup(&mut db);

    let db = Arc::new(Mutex::new(db));

    let mut threads = Vec::with_capacity(n_threads);
    let barrier = Arc::new(Barrier::new(n_threads));


    for _ in 0..n_threads {
        let b = barrier.clone();
        let db = db.clone();
        threads.push(thread::spawn(move || { run(b, db);}));
    }

    for thread in threads {
        thread.join().expect("Thread join failed in parallel_bench test.");
    }
}

#[test]
fn sequential_db_bench() {
    parallel_bench_db(1);
}

#[test]
fn four_way_db_bench() {
    parallel_bench_db(4);
}

fn parallel_bench_db(n_threads: usize) {
    parallel_bench(n_threads,
        |db| {
            println!("Setting up db...");
            let n_iters : u32 = 1_000_000;

            let mut key = Vec::new();
            key.extend_from_slice("longkey_".as_bytes());
            let value = "10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000".as_bytes();

            let start = Instant::now();
            for i in 0..n_iters {
                key[7] = i as u8 & 0x7f;
                db.put(key.as_slice(), value);
            }
            let put_time = start.elapsed();

            println!("Took: {:?}", put_time);
            println!("Per Put: {:?}", put_time/(n_iters as u32));
        },
        |barrier, db| {
            let mut key = Vec::new();
            key.extend_from_slice("longkey_".as_bytes());
            let n_iters : u32 = 1_000_000;

            barrier.wait();

            let start = Instant::now();
            let mut s = 0u64;
            for i in 0..n_iters {
                key[7] = i as u8 & 0x7f;
                let mut db = db.lock().unwrap();
                s += db.get(key.as_slice()).unwrap()[0] as u64;
            }
            let get_time = start.elapsed();

            println!("Per Get: {:?}", get_time/(n_iters as u32));
            println!("Print some stuff to avoid compiler opts: {:?}", s);
            println!("Done!");
        });
}

