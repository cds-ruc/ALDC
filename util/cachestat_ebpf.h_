#ifndef CACHESTAT_EBPF_H
#define CACHESTAT_EBPF_H

#include <unistd.h>
#include <fstream>
#include <iostream>
#include <string>

#include "BPF.h"
#include "bcc_exception.h"

namespace  leveldb{

namespace cache_ebpf {
//cyf notice: do not insert any comments in BPF_PROGRAM
//https://github.com/iovisor/bcc/blob/master/docs/reference_guide.md
const std::string BPF_PROGRAM = R"(
#include <uapi/linux/ptrace.h>
struct key_t {
    u64 ip;
    u32 pid;
    u32 uid;
    char comm[16];
};

    BPF_HASH(counts, struct key_t);

    int do_count(struct pt_regs *ctx) {
        struct key_t key = {};
        u64 pid = bpf_get_current_pid_tgid();
        u32 uid = bpf_get_current_uid_gid();

        key.ip = PT_REGS_IP(ctx);
        key.pid = pid & 0xFFFFFFFF;
        key.uid = uid & 0xFFFFFFFF;
        bpf_get_current_comm(&(key.comm), 16);

        counts.increment(key);
        return 0;
}
)";



}


struct key_t {
    uint64_t ip;
    uint32_t pid;
    uint32_t uid;
    char comm[16];
};

//page cache parameters through eBPF
struct cache_info{
    uint64_t mpa;//mark_page_accessed
    uint64_t mbd;//mark_buffer_dirty
    uint64_t apcl;//add_to_page_cache_lru
    uint64_t apd;//account_page_dirtied

    cache_info():mpa(0),mbd(0),apcl(0),apd(0){

    }
};


class Cachestat_eBPF
{
public:
    Cachestat_eBPF();
    void attach_kernel_probe_event();
    void detach_kernel_probe_event();
    struct cache_info get_cache_info();
    ebpf::StatusTuple attach_kernel_fun(std::string kernel_fun, std::string probe_fun);
    //~Cachestat_eBPF();



private:
    ebpf::BPF bpf_;
    struct cache_info cache_info_;

};

}
#endif // CACHESTAT_EBPF_H
