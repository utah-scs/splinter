#![feature(type_ascription)]

pub trait DB {
    fn debug_log(&self, &str);
}

use std::cell::RefCell;

pub struct MockDB {
    messages: RefCell<Vec<String>>,
}

impl MockDB {
    pub fn new() -> MockDB {
        MockDB{messages: RefCell::new(Vec::new())}
    }

    pub fn assert_messages<S>(&self, messages: &[S])
        where S: std::fmt::Debug + PartialEq<String>
    {
        let found = self.messages.borrow();
        assert_eq!(messages, found.as_slice());
    }

    pub fn clear_messages(&self) {
        let mut messages = self.messages.borrow_mut();
        messages.clear();
    }
}

impl DB for MockDB {
    fn debug_log(&self, message: &str) {
        let mut messages = self.messages.borrow_mut();
        messages.push(String::from(message));
    }
}

pub struct NullDB {}

impl NullDB {
    pub fn new() -> NullDB {
        NullDB{}
    }

    pub fn assert_messages<S>(&self, messages: &[S])
        where S: std::fmt::Debug + PartialEq<String>
    {}

    pub fn clear_messages(&self) {}
}

impl DB for NullDB {
    fn debug_log(&self, message: &str) {}
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
