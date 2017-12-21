#[macro_use] extern crate log;
extern crate env_logger;

extern crate db;

use db::Service; // TODO(stutsman) Seems like something is screwed up in lib.rs if we require this.

fn main() {
    env_logger::init().unwrap();
    info!("Starting Sandstorm");

    let master = db::Master::new();

    let mut request = db::BS::new();

    for _ in 0..20 {
        db::fill_request(&mut request);

        // Right now services borrow the request. It could make more sense for ownership to be
        // transferred later if some request/responses outlast the stack (e.g. via futures) and we
        // are still worried about copy-out costs. This seems a bit unlikely, though.
        master.dispatch(&request);
    }
}

