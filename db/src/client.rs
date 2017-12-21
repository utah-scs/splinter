use super::common::*;
use super::rpc::*;

pub fn fill_request(request: &mut BS) {
    request.clear();
    request.push(OP_GET);
}

