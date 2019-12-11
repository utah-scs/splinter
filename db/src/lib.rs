/* Copyright (c) 2017 University of Utah
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

//! This crate is useful in writing a new client and handling pushback
//! extension on the client side.
#![feature(generators, generator_trait, asm, integer_atomics, atomic_min_max)]
#![allow(bare_trait_objects)]
#![warn(missing_docs)]

extern crate libloading;
extern crate sandstorm;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate bincode;
extern crate crypto;
extern crate hashbrown;
extern crate spin;
extern crate time;
extern crate toml;
extern crate util;

pub extern crate bytes;
pub extern crate e2d2;
pub extern crate env_logger;
#[macro_use]
pub extern crate log;

mod alloc;
mod container;
mod context;
mod native;
mod service;
mod tenant;

// Public modules for binaries.
/// This module is needed to parse the server and config file.
pub mod config;
/// This module is needed to add cycles counters at various place in the code.
#[allow(dead_code)]
#[allow(unused_imports)]
pub mod cyclecounter;
/// This module provides functionality to manipulate CPU cycle ticks.
pub mod cycles;
/// This module provides functionality to send and receive packets over the network.
pub mod dispatch;
/// This module provides functionality to install a new extension on the server.
pub mod install;
/// This module helps in initializing the tables and task creation for each extension.
pub mod master;
/// This module helps in parsing the rpc arguments from the packets.
pub mod rpc;
/// This module helps in task scheduling on the server threads.
pub mod sched;
/// This module provides functionality related to the tables.
pub mod table;
/// This modules has a trait which should be implemented by each task instance.
pub mod task;
/// This module contains the transaction related code.
pub mod tx;
/// This module contains the wireformat realted to the various functionalities.
pub mod wireformat;
