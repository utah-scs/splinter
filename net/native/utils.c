#include <rte_config.h>
#include <rte_hash_crc.h>
#include <rte_ip.h>

#include <sched.h>
#include <unistd.h>
#include <sys/syscall.h>

// Make rte_hash_crc available to Rust. This adds some cost, will look into producing a pure Rust
// version.
uint32_t crc_hash_native(const void* data, uint32_t len, uint32_t initial) {
    return rte_hash_crc(data, len, initial);
}

uint16_t ipv4_cksum(const void* iphdr) {
    return rte_ipv4_cksum((const struct ipv4_hdr*)iphdr);
}

// Returns the ID of the current thread as maintained by the OS.
uint64_t get_thread_id(void) {
    return (uint64_t)syscall(SYS_gettid);
}

// Sets the affinity of a thread.
//
// \param tid:  Identifier of the thread whose affinity should be set. Should
//              have been obtained by calling get_thread_id() above.
// \param core: Identifier of the core the thread should be affinitized to.
//
// \return: 0 if the affinity was correctly set. -1 otherwise.
int set_affinity(uint64_t tid, uint64_t core) {
    cpu_set_t cpuset;

    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);

    return sched_setaffinity((pid_t)tid, sizeof(cpuset), &cpuset);
}
