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
extern crate libc;

use self::libc::c_void;
use std::alloc::{GlobalAlloc, Layout};
use std::ptr;

/// Allocator used to allocate memory in heap for the table.
pub struct SandstormAllocator;

//Upper limit on the amount of heap allocation an extensions can ask for.
static mut MEM_LIMIT: usize = 10240;

// This Constant only gets compiled if the architecture is any one of these
#[cfg(all(any(
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mipsel",
    target_arch = "powerpc"
)))]
const MIN_ALIGN: usize = 8;

#[cfg(all(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "mips64",
    target_arch = "s390x",
    target_arch = "sparc64"
)))]
const MIN_ALIGN: usize = 16;

unsafe impl GlobalAlloc for SandstormAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEM_LIMIT > layout.size() {
            if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                let ptr = libc::malloc(layout.size());
                MEM_LIMIT -= layout.size();
                ptr as *mut u8
            } else {
                let ptr = libc::memalign(layout.align(), layout.size());
                MEM_LIMIT -= layout.size();
                ptr as *mut u8
            }
        } else {
            panic!("Allocator can't allow more heap memory for the extension")
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        MEM_LIMIT += layout.size();
        libc::free(ptr as *mut c_void)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if MEM_LIMIT > layout.size() {
            if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                let ptr = libc::calloc(layout.size(), 1);
                MEM_LIMIT -= layout.size();
                ptr as *mut u8
            } else {
                let ptr = libc::memalign(layout.align(), layout.size());
                if !ptr.is_null() {
                    ptr::write_bytes(ptr, 0, layout.size());
                }
                MEM_LIMIT -= layout.size();
                ptr as *mut u8
            }
        } else {
            panic!("Allocator can't allow more heap memory for the extension")
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if MEM_LIMIT > new_size - layout.size() {
            MEM_LIMIT -= new_size - layout.size();
            libc::realloc(ptr as *mut libc::c_void, new_size) as *mut u8
        } else {
            panic!("Allocator can't allow more heap memory for the extension")
        }
    }
}
