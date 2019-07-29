/* Copyright (c) 2019 University of Utah
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

extern crate db;
extern crate rand;
extern crate splinter;

use std::mem;
use std::mem::transmute;
use std::sync::Arc;

use db::config::ClientConfig;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::interface::*;
use db::log::*;
use db::wireformat::{GetResponse, MultiGetResponse, OpCode, PutResponse};

use rand::distributions::Sample;
use rand::{Rng, SeedableRng, XorShiftRng};
use zipf::ZipfDistribution;

use self::splinter::dispatch::Sender;
use self::splinter::workload::Workload;

pub struct YCSBT {
    put_pct: usize,
    rng: Box<Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    multikey_buf: Vec<u8>,
    invoke_get: Vec<u8>,
    invoke_get_modify: Vec<u8>,
    _sender: Arc<Sender>,
}

impl YCSBT {
    pub fn new(table_id: u64, config: &ClientConfig, sender: Arc<Sender>) -> YCSBT {
        let seed: [u32; 4] = rand::random::<[u32; 4]>();

        let mut key_buf: Vec<u8> = Vec::with_capacity(config.key_len);
        key_buf.resize(config.key_len, 0);
        let mut value_buf: Vec<u8> = Vec::with_capacity(config.value_len);
        value_buf.resize(config.value_len, 0);
        let mut multikey_buf: Vec<u8> = Vec::with_capacity(2 * config.key_len);
        multikey_buf.resize(2 * config.value_len, 0);

        // The payload on an invoke() based get request consists of the extensions name ("ycsbt"),
        // the table id to perform the lookup on, and the key to lookup.
        let payload_len = "ycsbt".as_bytes().len() + mem::size_of::<u64>() + config.key_len;
        let mut invoke_get: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get.extend_from_slice("ycsbt".as_bytes());
        invoke_get.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get.resize(payload_len, 0);

        // The payload on an invoke() based get request consists of the extensions name ("ycsbt"),
        // the table id to perform the lookup on, and the two keys to lookup and modify.
        let payload_len =
            "ycsbt".as_bytes().len() + mem::size_of::<u64>() + config.key_len + config.key_len;
        let mut invoke_get_modify: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get_modify.extend_from_slice("ycsbt".as_bytes());
        invoke_get_modify
            .extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get_modify.resize(payload_len, 0);

        YCSBT {
            put_pct: config.put_pct,
            rng: Box::new(XorShiftRng::from_seed(seed)),
            key_rng: Box::new(
                ZipfDistribution::new(config.n_keys, config.skew)
                    .expect("Couldn't create key RNG."),
            ),
            tenant_rng: Box::new(
                ZipfDistribution::new(config.num_tenants as usize, config.tenant_skew)
                    .expect("Couldn't create tenant RNG."),
            ),
            key_buf: key_buf,
            value_buf: value_buf,
            multikey_buf: multikey_buf,
            invoke_get: invoke_get,
            invoke_get_modify: invoke_get_modify,
            _sender: sender,
        }
    }
}

impl Workload for YCSBT {
    /// Lookup the `Workload` trait for documentation on this method.
    fn next_optype(&mut self) -> OpCode {
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;
        if is_get == true {
            OpCode::SandstormGetRpc
        } else {
            OpCode::SandstormMultiGetRpc
        }
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn get_invoke_request(&mut self) -> (u32, &[u8]) {
        let t = self.tenant_rng.sample(&mut self.rng) as u32;
        let is_get = (self.rng.gen::<u32>() % 100) >= self.put_pct as u32;
        if is_get == true {
            let k = self.key_rng.sample(&mut self.rng) as u32;
            let k: [u8; 4] = unsafe { transmute(k.to_le()) };
            self.invoke_get[13..17].copy_from_slice(&k);
            (t, self.invoke_get.as_slice())
        } else {
            let k = self.key_rng.sample(&mut self.rng) as u32;
            let k: [u8; 4] = unsafe { transmute(k.to_le()) };
            self.invoke_get_modify[13..17].copy_from_slice(&k);

            let k = self.key_rng.sample(&mut self.rng) as u32;
            let k: [u8; 4] = unsafe { transmute(k.to_le()) };
            self.invoke_get_modify[43..47].copy_from_slice(&k);
            (t, self.invoke_get_modify.as_slice())
        }
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn get_get_request(&mut self) -> (u32, &[u8]) {
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        (t, self.key_buf.as_slice())
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn get_put_request(&mut self) -> (u32, &[u8], &[u8]) {
        info!("Shouldn't be used for this application");
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.key_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);
        self.value_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        (t, self.key_buf.as_slice(), self.value_buf.as_slice())
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn get_multiget_request(&mut self) -> (u32, u32, &[u8]) {
        let t = self.tenant_rng.sample(&mut self.rng) as u32;

        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.multikey_buf[0..mem::size_of::<u32>()].copy_from_slice(&k);

        let k = self.key_rng.sample(&mut self.rng) as u32;
        let k: [u8; 4] = unsafe { transmute(k.to_le()) };
        self.multikey_buf[30..34].copy_from_slice(&k);

        (t, 2, self.multikey_buf.as_slice())
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_get_response(&mut self, packet: &Packet<GetResponse, EmptyMetadata>) {
        let record = packet.get_payload();
        println!("Get {}", record.len());
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_put_response(&mut self, packet: &Packet<PutResponse, EmptyMetadata>) {
        let record = packet.get_payload();
        println!("Put {}", record.len());
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_multiget_response(&mut self, _packet: &Packet<MultiGetResponse, EmptyMetadata>) {}
}
