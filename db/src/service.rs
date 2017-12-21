use super::rpc::*;

pub trait Service {
    /// Processing a message targeted toward this service.
    /// Parses `request`, determines `Opcode`, and calls the appropriate RPC handler.
    /// 
    /// - `request`: borrowed referenced to message byte string.
    /// - `response`: response byte string. Ownership is given so the RPC handler can transfer
    ///               ownership to the transport layer later. It is created ahead of the call to
    ///               dispatch since it may be comprised of special memory (e.g. registered
    ///               packet buffers). As a convenience dispatch can just return `response` and
    ///               the caller will schedule it for transmit.
    fn dispatch(&self, request: &Request, response: Response) -> Option<Response>;
}
