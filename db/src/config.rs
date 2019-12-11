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

use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;

use super::e2d2::headers::*;
use super::toml;

/// To show the error while parsing the MAC address.
#[derive(Debug, Clone)]
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
    let bytes: Result<Vec<_>, _> = mac.split(':').map(|s| u8::from_str_radix(s, 16)).collect();

    match bytes {
        Ok(bytes) => {
            if bytes.len() == 6 {
                Ok(MacAddress::new_from_slice(&bytes))
            } else {
                Err(ParseError {})
            }
        }
        Err(_) => Err(ParseError {}),
    }
}

/// Load a config from `filename` otherwise return a default structure.
fn load_config(filename: &str) -> ServerConfig {
    let mut contents = String::new();

    let _ = File::open(filename).and_then(|mut file| file.read_to_string(&mut contents));

    match toml::from_str(&contents) {
        Ok(config) => config,
        Err(e) => {
            warn!("Failure paring config file {}: {}", filename, e);
            ServerConfig::default()
        }
    }
}

/// Load a config from `filename` otherwise return a default structure.
fn load_config_cl(filename: &str) -> ClientConfig {
    let mut contents = String::new();

    let _ = File::open(filename).and_then(|mut file| file.read_to_string(&mut contents));

    match toml::from_str(&contents) {
        Ok(config) => config,
        Err(e) => {
            warn!("Failure paring config file {}: {}", filename, e);
            ClientConfig::default()
        }
    }
}

/// All of the various configuration options needed to run a server, both optional and required.
/// Normally this config is recovered from a server.toml file (an example of which is in
/// server.toml-example). If this file is malformed or missing, the server will typically
/// crash when it cannot determine a MAC address to bind to.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ServerConfig {
    /// Server MAC Address.
    mac_address: String,
    /// Server IP Address.
    pub ip_address: String,
    /// Server UDP port for response packets.
    pub udp_port: u16,
    /// PCI address for the NIC.
    pub nic_pci: String,
    /// Client MAC Address.
    client_mac: String,
    /// Client IP Address.
    pub client_ip: String,
    /// Number of tenants to intialize the tables.
    pub num_tenants: u32,
    /// Network endpoint to install new extensions.
    pub install_addr: String,
    /// Type of workload; TAO, YCSB, AGGREGATE etc.
    pub workload: String,
    /// Number of records in the table for each tenant.
    pub num_records: u32,
}

impl ServerConfig {
    /// Load server config from server.toml file in the current directory or otherwise return a
    /// default structure.
    pub fn load() -> ServerConfig {
        load_config("server.toml")
    }

    /// Parse `mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ServerConfig, but TOML parsing makes that tricky.
    pub fn parse_mac(&self) -> MacAddress {
        parse_mac(&self.mac_address)
            .expect("Missing or malformed mac_address field in server config.")
    }

    /// Parse `client_mac` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ServerConfig, but TOML parsing makes that tricky.
    pub fn parse_client_mac(&self) -> MacAddress {
        parse_mac(&self.client_mac)
            .expect("Missing or malformed mac_address field in server config.")
    }
}

/// All of the various configuration options needed to run a client, both optional and required.
/// Normally this config is recovered from a client.toml file (an example of which is in
/// client.toml-example). If this file is malformed or missing, the client will typically
/// crash when it cannot determine a MAC address to bind to.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ClientConfig {
    /// Client MAC Address.
    mac_address: String,
    /// Client IP Address.
    pub ip_address: String,
    /// PCI address for the NIC.
    pub nic_pci: String,

    /// Server MAC Address.
    server_mac_address: String,
    /// Server IP Address.
    pub server_ip_address: String,
    /// Number of UDP ports to send requests to.
    pub server_udp_ports: u16,
    /// Number of tenants for requests generation.
    pub num_tenants: u32,
    /// Server network endpoint to install new extensions.
    pub install_addr: String,

    /// Type of workload; YCSBT etc.
    pub workload: String,
    /// This parameter decides the load type; open-loop load or closed-loop load.
    pub open_load: bool,
    /// This parameter decides the requests type; native or extension.
    pub use_invoke: bool,

    /// Length of the key for requests generation.
    pub key_len: usize,
    /// Length of the value for requests generation and response parsing.
    pub value_len: usize,
    /// Number of records in the table, needed in requests generation.
    pub n_keys: usize,
    /// Percentage of put() requests for YCSB workload.
    pub put_pct: usize,
    /// Used for enabling or disabling scan requests for YCSB workload.
    pub enable_scan: bool,
    /// Used for indicating the range of the scan query.
    pub scan_range: u32,
    /// Skew in Zipf distribution used for YCSB workload.
    pub skew: f64,
    /// Tenant skew to show the gain due to workstealing on the server side.
    pub tenant_skew: f64,

    /// Total number of requets generated by the client for one run.
    pub num_reqs: usize,
    /// Number of requets generated per second.
    pub req_rate: usize,

    /// Number of records aggregated per requests for AGGREGATE workload.
    pub num_aggr: u32,
    /// Number of multiplications done per aggregation for AGGREGATE workload.
    pub order: u32,

    /// If true, then an invoke() based run will use native requests for an obj_get.
    pub combined: bool,
    /// The percentage of assoc_range() requests.
    pub assocs_p: usize,

    /// The percentage of invoke() based requests that are long running.
    pub long_pct: usize,
    /// The frequency at which the extension should yield to the database.
    pub yield_f: u8,

    ///The number of bad requests to generate for every 10 million operations.
    pub bad_ptm: usize,
}

impl ClientConfig {
    /// Load client config from client.toml file in the current directory or otherwise return a
    /// default structure.
    pub fn load() -> ClientConfig {
        load_config_cl("client.toml")
    }

    /// Parse `mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ClientConfig, but TOML parsing makes that tricky.
    pub fn parse_mac(&self) -> MacAddress {
        parse_mac(&self.mac_address)
            .expect("Missing or malformed mac_address field in client config.")
    }

    /// Parse `server_mac_address` into NetBrick's format or panic if malformed.
    /// Linear time, so ideally we'd store this in ClientConfig, but TOML parsing makes that tricky.
    pub fn parse_server_mac(&self) -> MacAddress {
        parse_mac(&self.server_mac_address)
            .expect("Missing or malformed server_mac_address field in client config.")
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
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5:g6") {
            assert!(false);
        } else {
        }
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5: 6") {
            assert!(false);
        } else {
        }
        if let Ok(_) = parse_mac("A1:b2:C3:d4:E5") {
            assert!(false);
        } else {
        }
        if let Ok(_) = parse_mac(":::::") {
            assert!(false);
        } else {
        }
    }
}
