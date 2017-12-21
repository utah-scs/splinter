use super::common::*;
use super::rpc::*;

pub fn fill_get_request(request: &mut BS) {
    request.clear();
    request.push(OP_GET);
}

pub fn fill_put_request(request: &mut BS) {
    request.clear();
    request.push(OP_PUT);
}

pub fn create_response() -> Response {
    Response::new()
}
