pub type BS = Vec<u8>;

pub type UserId = u32;
pub type TableId = u32;


pub mod util {
    use std::time::{Duration, Instant};

    fn to_seconds(d: &Duration) -> f64 {
        d.as_secs() as f64 + (d.subsec_nanos() as f64 / 1e9)
    }

    pub struct Bench<'a> {
        start: Instant,
        name: Option<&'a str>,
    }

    impl<'a> Bench<'a> {
        pub fn start(name: Option<&'a str>) -> Bench {
            Bench{ start: Instant::now(), name }
        }

        pub fn run<F>(name: &str, f: F)
            where F: Fn()
        {
            let _ = Self::start(Some(name));
            f()
        }
    }

    impl<'a> Drop for Bench<'a> {
        fn drop(&mut self) {
            let time = self.start.elapsed();
            println!("{} took {} us", self.name.unwrap_or("<bench>"), to_seconds(&time) * 1e6);
        }
    }
}
