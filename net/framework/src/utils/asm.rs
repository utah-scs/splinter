#[inline]
pub fn cpuid() {
    unsafe {
        llvm_asm!("movl $$0x2, %eax":::"eax");
        llvm_asm!("movl $$0x0, %ecx":::"ecx");
        llvm_asm!("cpuid"
             :
             :
             : "rax rbx rcx rdx");
    }
}

#[inline]
pub fn rdtsc_unsafe() -> u64 {
    unsafe {
        let low: u32;
        let high: u32;
        llvm_asm!("rdtsc"
             : "={eax}" (low), "={edx}" (high)
             :
             : "rdx rax"
             : "volatile");
        ((high as u64) << 32) | (low as u64)
    }
}

#[inline]
pub fn rdtscp_unsafe() -> u64 {
    let high: u32;
    let low: u32;
    unsafe {
        llvm_asm!("rdtscp"
             : "={eax}" (low), "={edx}" (high)
             :
             : "ecx"
             : "volatile");
        ((high as u64) << 32) | (low as u64)
    }
}

#[inline]
pub fn pause() {
    unsafe {
        llvm_asm!("pause"::::"volatile");
    }
}
