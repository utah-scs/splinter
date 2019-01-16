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
extern crate spin;
extern crate toml;
extern crate time;

pub extern crate bytes;
pub extern crate e2d2;
pub extern crate env_logger;
#[macro_use]
pub extern crate log;

mod alloc;
mod container;
mod context;
mod service;
mod tenant;
mod native;

// Public modules for binaries.
pub mod rpc;
pub mod cycles;
#[allow(dead_code)]
#[allow(unused_imports)]
pub mod cyclecounter;
pub mod config;
pub mod dispatch;
pub mod table;
pub mod wireformat;
pub mod master;
pub mod sched;
pub mod task;
pub mod install;
