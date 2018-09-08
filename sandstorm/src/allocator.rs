// Most of the allocator code is copied from
// https://github.com/alexcrichton/jemallocator/blob/master/src/lib.rs
// which is official implementation of jemallocator in rust

extern crate jemalloc_sys;
extern crate libc;

use self::jemalloc_sys::{calloc, mallocx, rallocx, sdallocx, MALLOCX_ALIGN, MALLOCX_ZERO};
use self::libc::{c_int, c_void};
use std::alloc::{GlobalAlloc, Layout};

pub struct SandstormAllocator;

//Upper limit on the amount of heap allocation an extensions can ask for.
static mut MEM_LIMIT: usize = 10240;

// This Constant only gets compiled if the architecture is any one of these
#[cfg(
    all(
        any(
            target_arch = "arm",
            target_arch = "mips",
            target_arch = "mipsel",
            target_arch = "powerpc"
        )
    )
)]
const MIN_ALIGN: usize = 8;

#[cfg(
    all(
        any(
            target_arch = "x86",
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "powerpc64",
            target_arch = "powerpc64le",
            target_arch = "mips64",
            target_arch = "s390x",
            target_arch = "sparc64"
        )
    )
)]
const MIN_ALIGN: usize = 16;

fn layout_to_flags(align: usize, size: usize) -> c_int {
    if align <= MIN_ALIGN && align <= size {
        0
    } else {
        MALLOCX_ALIGN(align)
    }
}

unsafe impl GlobalAlloc for SandstormAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if MEM_LIMIT - layout.size() > 0 {
            let flags = layout_to_flags(layout.align(), layout.size());
            let ptr = mallocx(layout.size(), flags);
            MEM_LIMIT -= layout.size();
            ptr as *mut u8
        } else {
            panic!()
        }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let flags = layout_to_flags(layout.align(), layout.size());
        MEM_LIMIT += layout.size();
        sdallocx(ptr as *mut c_void, layout.size(), flags)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if MEM_LIMIT - layout.size() > 0 {
            let ptr = if layout.align() <= MIN_ALIGN && layout.align() <= layout.size() {
                calloc(1, layout.size())
            } else {
                let flags = layout_to_flags(layout.align(), layout.size()) | MALLOCX_ZERO;
                mallocx(layout.size(), flags)
            };
            MEM_LIMIT -= layout.size();
            ptr as *mut u8
        } else {
            panic!()
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if MEM_LIMIT - new_size - layout.size() > 0 {
            let flags = layout_to_flags(layout.align(), new_size);
            let ptr = rallocx(ptr as *mut c_void, new_size, flags);
            MEM_LIMIT -= new_size - layout.size();
            ptr as *mut u8
        } else {
            panic!()
        }
    }
}
