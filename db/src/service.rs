use super::common::*;
use super::rpc::*;

pub trait Service {
    fn dispatch(&self, request: &BS) -> Option<Response>;
}
