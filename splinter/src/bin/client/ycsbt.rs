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
use std::ops::{Generator, GeneratorState};
use std::pin::Pin;
use std::slice;
use std::sync::Arc;

use db::config::ClientConfig;
use db::cycles;
use db::e2d2::common::EmptyMetadata;
use db::e2d2::interface::*;
use db::log::*;
use db::wireformat::{GetResponse, MultiGetResponse, OpCode, OpType, PutResponse};

use rand::distributions::Sample;
use rand::{Rng, SeedableRng, XorShiftRng};
use zipf::ZipfDistribution;

use self::splinter::dispatch::Sender;
use self::splinter::workload::Workload;

pub struct YCSBT {
    put_pct: usize,
    rng: Box<dyn Rng>,
    key_rng: Box<ZipfDistribution>,
    tenant_rng: Box<ZipfDistribution>,
    key_buf: Vec<u8>,
    value_buf: Vec<u8>,
    multikey_buf: Vec<u8>,
    invoke_get: Vec<u8>,
    invoke_get_modify: Vec<u8>,
    sender: Arc<Sender>,
    table_id: u64,
    order: u32,
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

        let order = config.order as u32;

        // The payload on an invoke() based get request consists of the extensions name ("ycsbt"),
        // operation type, the table id to perform the lookup on, and the key to lookup.
        let payload_len = "ycsbt".as_bytes().len()
            + mem::size_of::<u64>()
            + config.key_len
            + mem::size_of::<u32>()
            + mem::size_of::<u8>();
        let mut invoke_get: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get.extend_from_slice("ycsbt".as_bytes());
        invoke_get.extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get.extend_from_slice(&[0; 30]); // Placeholder for key
        invoke_get.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(order.to_le()) });
        invoke_get.extend_from_slice(&[1]);
        invoke_get.resize(payload_len, 0);

        // The payload on an invoke() based get request consists of the extensions name ("ycsbt"),
        // the table id to perform the lookup on, and the two keys to lookup and modify.
        let payload_len = "ycsbt".as_bytes().len()
            + mem::size_of::<u64>()
            + config.key_len
            + config.key_len
            + mem::size_of::<u32>()
            + mem::size_of::<u8>();
        let mut invoke_get_modify: Vec<u8> = Vec::with_capacity(payload_len);
        invoke_get_modify.extend_from_slice("ycsbt".as_bytes());
        invoke_get_modify
            .extend_from_slice(&unsafe { transmute::<u64, [u8; 8]>(table_id.to_le()) });
        invoke_get_modify.extend_from_slice(&[0; 60]); // Placeholder for 2 keys
        invoke_get_modify.extend_from_slice(&unsafe { transmute::<u32, [u8; 4]>(order.to_le()) });
        invoke_get_modify.extend_from_slice(&[2]);
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
            sender: sender,
            table_id: table_id,
            order: config.order as u32,
        }
    }

    #[allow(unreachable_code)]
    /// This method executes the task.
    ///
    /// # Arguments
    /// *`order`: The amount of compute in each extension.
    pub fn execute_task(&mut self, order: u32) {
        let mut generator = move || {
            // Compute part for this extension
            let start = cycles::rdtsc();
            while cycles::rdtsc() - start < order as u64 {}
            return 0;

            // XXX: This yield is required to get the compiler to compile this closure into a
            // generator. It is unreachable and benign.
            yield 0;
        };

        match Pin::new(&mut generator).resume() {
            GeneratorState::Yielded(val) => {
                if val != 0 {
                    panic!("Pushback native execution is buggy");
                }
            }
            GeneratorState::Complete(val) => {
                if val != 0 {
                    panic!("Pushback native execution is buggy");
                }
            }
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
    fn name_length(&self) -> u32 {
        "ycsbt".as_bytes().len() as u32
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
        let header = packet.get_header();
        let record = packet.get_payload();
        self.sender.send_commit(
            header.common_header.tenant,
            self.table_id,
            record,
            header.common_header.stamp,
            30,
            100,
        );
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_put_response(&mut self, packet: &Packet<PutResponse, EmptyMetadata>) {
        let record = packet.get_payload();
        info!("Put {}", record.len());
    }

    /// Lookup the `Workload` trait for documentation on this method.
    fn process_multiget_response(&mut self, packet: &Packet<MultiGetResponse, EmptyMetadata>) {
        let mut value1: Vec<u8> = Vec::with_capacity(100);
        let mut value2: Vec<u8> = Vec::with_capacity(100);
        let mut commit_payload = Vec::with_capacity(4 * 139);
        commit_payload.extend_from_slice(packet.get_payload());

        let header = packet.get_header();
        let records = packet.get_payload();
        let (record1, record2) = records.split_at(139);
        let (key1, v1) = record1.split_at(9).1.split_at(30);
        let (key2, v2) = record2.split_at(9).1.split_at(30);
        value1.extend_from_slice(v1);
        value2.extend_from_slice(v2);
        if value1[0] > 0 && value2[0] < 255 {
            value1[0] -= 1;
            value2[0] += 1;
        } else if value2[0] > 0 && value1[0] < 255 {
            value1[0] += 1;
            value2[0] -= 1;
        }

        let ptr = &OpType::SandstormWrite as *const _ as *const u8;
        let optype = unsafe { slice::from_raw_parts(ptr, mem::size_of::<OpType>()) };
        let version: [u8; 8] = unsafe { transmute(0u64.to_le()) };
        commit_payload.extend_from_slice(optype);
        commit_payload.extend_from_slice(&version);
        commit_payload.extend_from_slice(key1);
        commit_payload.extend_from_slice(&value1);

        commit_payload.extend_from_slice(optype);
        commit_payload.extend_from_slice(&version);
        commit_payload.extend_from_slice(key2);
        commit_payload.extend_from_slice(&value2);

        // Execute the compute part.
        let order = self.order;
        self.execute_task(order);

        self.sender.send_commit(
            header.common_header.tenant,
            self.table_id,
            &commit_payload,
            header.common_header.stamp,
            30,
            100,
        );
    }
}
