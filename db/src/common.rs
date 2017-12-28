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

        pub fn run<F>(name: &str, f: F)
            where F: FnOnce()
        {
            let _ = Self::start(Some(name));
            f()
        }
    }

    impl<'a> Drop for Bench<'a> {
        fn drop(&mut self) {
            let time = self.start.to(time::PreciseTime::now());
            println!("{} took {} us", self.name.unwrap_or("<bench>"), time.num_microseconds().unwrap());
        }
    }
}
