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

//! This crate contains all the traits and common code which
//! is used on both client and server.
#![feature(type_ascription)]
#![feature(generator_trait)]
#![feature(rustc_private)]
#![allow(bare_trait_objects)]
#![warn(missing_docs)]

/// Allocator/deallocator of heap memory for the table.
pub mod allocator;
/// Module to manipulate various type of buffer for the entire system.
pub mod buf;
/// Common constants used in the system, example: PACKET_ETYPE: u16 = 0x0800.
pub mod common;
/// DB trait which define the functions for each `DB` implementation.
pub mod db;
/// Module to manage the extensions; load, install, get etc.
pub mod ext;
/// Module to put all the db related macros like GET(), PUT(), etc.
pub mod macros;
/// Mock implementation of `DB` trait.
pub mod mock;
/// Null DB implemention to be used in ext_bench benchmark.
pub mod null;
/// Module to serialize bytes which can be transferred over the network.
pub mod pack;

pub use std::boxed;
pub use std::convert;
pub use std::io;
pub use std::mem::size_of;
pub use std::rc;
pub use std::result;
pub use std::time;
pub use std::vec;

extern crate byteorder;
extern crate hashbrown;
extern crate libloading;
extern crate spin;
extern crate util;
pub use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
