/* Copyright (c) 2018 University of Utah
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use super::master::Master;

/// This type is responsible for servicing the install() RPC in Sandstorm. It listens for incoming
/// RPCs on a TCP socket, and hands them off the Master.
pub struct Installer {
    /// Master service that the install() RPC is handed off to.
    master: Arc<Master>,

    /// TCP endpoint that install() RPCs will be received on.
    listener: TcpListener,
}

// Implementation of methods on Installer.
impl Installer {
    /// Constructs an Installer.
    ///
    /// # Arguments
    ///
    /// * `master`: A master service that incoming RPCs can be handed off to.
    /// * `addr`:   Network address (IPv4:Port) that RPCs will be received on.
    pub fn new(master: Arc<Master>, addr: String) -> Installer {
        Installer {
            master: Arc::clone(&master),
            listener: TcpListener::bind(addr.as_str())
                .expect("Failed to bind installer to network."),
        }
    }

    /// Fires up the Installer.
    pub fn execute(&mut self) {
        // Listen for incoming RPCs.
        for stream in self.listener.incoming() {
            let mut stream = stream.expect("Installer failed to unwrap incoming TCP stream.");

            let mut req: Vec<u8> = vec![];
            // Read from a connection.
            if let Ok(num) = stream.read_to_end(&mut req) {
                if num == 0 {
                    continue;
                }

                // Handoff to Master.
                // TODO: Check Service and OpCode in RPC header.
                req.truncate(num);
                let res = self.master.install(req);

                // Return a response to the client.
                stream.write_all(&res).unwrap();
            }
        }
    }
}
