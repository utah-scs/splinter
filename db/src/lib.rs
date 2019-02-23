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

#![feature(generators, generator_trait, asm)]

extern crate libloading;
extern crate sandstorm;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate hashbrown;
extern crate spin;
extern crate time;
extern crate toml;

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
pub mod config;
#[allow(dead_code)]
#[allow(unused_imports)]
pub mod cyclecounter;
pub mod cycles;
pub mod dispatch;
pub mod install;
pub mod master;
pub mod rpc;
pub mod sched;
pub mod table;
pub mod task;
pub mod wireformat;
