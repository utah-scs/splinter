use super::common::*;

pub type Request = BS;
pub type Response = BS;

pub type Opcode = u8;

// TODO(stutsman) Would prefer an enum, but Rust doesn't support packed enums.
pub const OP_GET : Opcode = 1;
pub const OP_PUT : Opcode = 2;
pub const OP_UNDEFINED : Opcode = 3;

pub fn parse_opcode(request: &Request) -> Option<Opcode> {
    match request.get(0) {
        Some(&opcode) if opcode < OP_UNDEFINED => Some(opcode),
        Some(opcode) => {
            warn!("Received illegal opcode {}", opcode);
            None
        }
        _ => {
            warn!("Received zero-byte request");
            None
        }
    }
}
