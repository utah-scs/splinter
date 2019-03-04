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

//! This crate is useful in writing a new client and handling pushback
//! extension on the client side.
#![feature(generators, generator_trait, asm)]
#![warn(missing_docs)]

extern crate db;
extern crate sandstorm;
pub extern crate env_logger;
#[macro_use]
pub extern crate log;

mod container;

// Public modules for binaries.
#[allow(unused_imports)]
/// Needed to send and receive the packets on the client side.
pub mod dispatch;
/// Needed to handle and resume the pushback extension on the client side.
pub mod manager;
/// Proxy to the database on the client side, searches the local cache for
/// data and if not present on the cache then issues a request to the server.
pub mod proxy;
