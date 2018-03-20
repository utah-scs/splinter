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

use std::error::{Error};
use std::fmt;
use std::fs::File;
use std::io::Read;

use super::e2d2::headers::*;
use super::toml;

#[derive(Debug,Clone)]
pub struct ParseError;

impl Error for ParseError {
    fn description(&self) -> &str {
        "Malformed MAC address."
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Malformed MAC address.")
    }
}

/// Parses str into a MacAddress or returns ParseError.
/// str must be formatted six colon-separated hex literals.
pub fn parse_mac(mac: &str) -> Result<MacAddress, ParseError> {
    let bytes : Result<Vec<_>, _> =
        mac.split(':')
            .map(|s| u8::from_str_radix(s, 16))
            .collect();

    match bytes {
        Ok(bytes) => 
            if bytes.len() == 6 {
                Ok(MacAddress::new(bytes[0], bytes[1], bytes[2],
                                   bytes[3], bytes[4], bytes[5]))
            } else {
                Err(ParseError{})
            },
        Err(_) => Err(ParseError{})
    }
}

/// All of the various configuration options needed to run a server, both optional and required.
/// Normally this config is recovered from a server.toml file (an example of which is in
/// server.toml-example). If this file is malformed or missing, the server will typically
/// crash when it cannot determine a MAC address to bind to.
#[derive(Serialize, Deserialize, Debug)]
pub struct ServerConfig {
    mac_address: String,
    pub ip_address: String,
    pub udp_port: u16,
}

impl ServerConfig {
    /// Create a new, default instance. Some required fields cannot be defaulted (`mac_address`) so the
    /// returned config will likely need some reinitialization to be useful.
    fn new() -> ServerConfig {
        ServerConfig {
            mac_address: String::from("<INVALID MAC ADDRESS>"),
            ip_address: String::from("<INVALID IP ADDRESS>"),
            udp_port: 0,
        }
    }

    /// Load server config from server.toml file in the current directory or otherwise return a
    /// default structure.
    pub fn load() -> ServerConfig {
        let mut contents = String::new();

        let _ = File::open("server.toml").and_then(|mut file|
                    file.read_to_string(&mut contents));

        toml::from_str(&mut contents)
            .ok()
            .unwrap_or_else(|| {
                warn!("No valid server.toml; using default server config.");
                ServerConfig::new()
            })
    }

    /// Parse `mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ServerConfig, but TOML parsing makes that tricky.
    pub fn parse_mac(&self) -> MacAddress {
        parse_mac(&self.mac_address)
            .expect("Missing or malformed mac_address field in server config.")
    }
}

#[cfg(test)]
mod tests {
    use super::parse_mac;

    #[test]
    fn empty_str() {
        if let Err(e) = parse_mac("") {
            assert_eq!("Malformed MAC address.", e.to_string());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn ok_str() {
        if let Ok(m) = parse_mac("A1:b2:C3:d4:E5:f6") {
            assert_eq!(0xa1, m.addr[0]);
            assert_eq!(0xb2, m.addr[1]);
            assert_eq!(0xc3, m.addr[2]);
            assert_eq!(0xd4, m.addr[3]);
            assert_eq!(0xe5, m.addr[4]);
            assert_eq!(0xf6, m.addr[5]);
        } else {
            assert!(false);
        }
    }

    #[test]
    fn bad_examples() {
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5:g6") { assert!(false); } else {}
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5: 6") { assert!(false); } else {}
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5") { assert!(false); } else {}
        if let Ok(_) = parse_mac(":::::") { assert!(false); } else {}
    }

}
