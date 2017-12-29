pub type BS = Vec<u8>;

pub type UserId = u32;
pub type TableId = u32;

pub mod util {
    extern crate time;

    pub struct Bench<'a> {
        start: time::PreciseTime,
        name: Option<&'a str>,
    }

    impl<'a> Bench<'a> {
        pub fn start(name: Option<&'a str>) -> Bench {
            Bench{ start: time::PreciseTime::now(), name }
        }

        pub fn run<F>(f: F) -> time::Duration
            where F: FnOnce()
        {
            let b = Self::start(None);
            f();
            b.start.to(time::PreciseTime::now())
        }
    }

    impl<'a> Drop for Bench<'a> {
        fn drop(&mut self) {
            let time = self.start.to(time::PreciseTime::now());
            if let Some(name) = self.name {
                println!("{} took {} us", name, time.num_microseconds().unwrap());
            }
        }
    }
}
