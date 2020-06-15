#include "cachestat_ebpf.h"

#include <bcc_syms.h>
#include "syms.h"


leveldb::Cachestat_eBPF::Cachestat_eBPF()
{
    //bpf_ = new ebpf::BPF;
    auto init_res = bpf_.init(cache_ebpf::BPF_PROGRAM);
      if (init_res.code() != 0)
          std::cout << init_res.msg() << std::endl;
      //attach_kernel_probe_event();
      std::cout <<"initial Cachestat_eBPF()"<<std::endl;
      //struct cache_info cif=  get_cache_info();
      //detach_kernel_probe_event();

}

void leveldb::Cachestat_eBPF:: attach_kernel_probe_event()
{
    while(1){
    ebpf::StatusTuple s1 = bpf_.attach_kprobe("mark_page_accessed", "do_count");
    ebpf::StatusTuple s2 = bpf_.attach_kprobe("mark_buffer_dirty", "do_count");
    ebpf::StatusTuple s3 = bpf_.attach_kprobe("add_to_page_cache_lru", "do_count");
    ebpf::StatusTuple s4 = bpf_.attach_kprobe("account_page_dirtied", "do_count");

    if(s1.code() || s2.code() || s3.code() || s4.code() ){
        std::cout <<"attach_kernel_probe error"<<std::endl;
    }
    else {
        std::cout <<"attch kernel completed!"<<std::endl;
       break;
    }
    }
//    std::string apcl = bpf_.get_syscall_fnname("add_to_page_cache_lru");
//    std::string mpa = bpf_.get_syscall_fnname("mark_page_accessed");
//    std::string apd = bpf_.get_syscall_fnname("account_page_dirtied");
//    std::string mbd = bpf_.get_syscall_fnname("mark_buffer_dirty");
//    std::string hello = bpf_.get_syscall_fnname("hello");
//    std::cout <<"apcl: "<<apcl<<" mpa: "<<mpa<<" apd: "<<apd<<" mbd: "<<mbd<<" hello: "<<hello<<std::endl;

//    attach_kernel_fun("add_to_page_cache_lru", "do_count");
//    attach_kernel_fun("mark_page_accessed", "do_count");
//    attach_kernel_fun("account_page_dirtied", "do_count");
//    attach_kernel_fun("mark_buffer_dirty", "do_count");

}

void leveldb::Cachestat_eBPF::detach_kernel_probe_event()
{
    bpf_.detach_kprobe("add_to_page_cache_lru");
    bpf_.detach_kprobe("mark_page_accessed");
    bpf_.detach_kprobe("account_page_dirtied");
    bpf_.detach_kprobe("mark_buffer_dirty");
}

leveldb::cache_info leveldb::Cachestat_eBPF::get_cache_info()
{
    //struct cache_info info;
    std::string pid_name;
    struct cache_info cif;

    auto cache_hash_table = bpf_.get_hash_table<struct key_t, uint64_t>("counts");
    //std::cout<< "Cachestat_eBPF::get_cache_info() table_size:"<<cache_hash_table.get_table_offline().size()<<std::endl;
    //for (auto it: cache_hash_table.get_table_offline()) {
    auto v_tmp = cache_hash_table.get_table_offline();
    for (size_t i=0; i<v_tmp.size();i++) {
        pid_name.clear();
        //pid_name = it.first.comm;
        pid_name = v_tmp[i].first.comm;
        //std::cout <<"The pid name: "<< pid_name <<std::endl;//cyf add
        //this if is used to judge whether the process belongs to ycsb
        if( pid_name.find("ycsbc") == 0 || pid_name.find("db_bench") == 0){
            //std::cout <<"PID: "<<v_tmp[i].first.pid<<" pid_name: "<<pid_name<<" value: "<<v_tmp[i].second<<std::endl;
            //printf("%p\n",v_tmp[i].first.ip);

            struct bcc_symbol b_symbol;
            KSyms ksyms;
            ksyms.resolve_addr(v_tmp[i].first.ip, &b_symbol);

            /* std::cout<<"PID: "<<v_tmp[i].first.pid<<" pid_name: "<<pid_name
                    <<"b_symbol.name: "<<b_symbol.name<<" count times: "<<v_tmp[i].second<<std::endl;
            */

            std::string fun_name = b_symbol.name;
            if(fun_name.find("add_to_page_cache_lru") != std::string::npos){
                cif.apcl += v_tmp[i].second;
            }
            else if(fun_name.find("mark_page_accessed") != std::string::npos) {
                cif.mpa += v_tmp[i].second;
            }
            else if(fun_name.find("account_page_dirtied") != std::string::npos) {
                cif.apd += v_tmp[i].second;
            }
            else if(fun_name.find("mark_buffer_dirty") != std::string::npos) {
                cif.mbd += v_tmp[i].second;
            }




        }
    }
    //std::cout << "mpa: \t"<<cif.mpa<<"\t mbd: \t"<<cif.mbd<<"\t apcl: \t"<<cif.apcl<<"\t apd: \t"<<cif.apd<<std::endl;
    cache_hash_table.clear_table_non_atomic();
    v_tmp.clear();
    //std::cout <<"============================================================================================"<<std::endl;

    return cif;

}

ebpf::StatusTuple leveldb::Cachestat_eBPF::attach_kernel_fun(std::string kernel_fun, std::string probe_fun)
{
    while(1){
    ebpf::StatusTuple s = bpf_.attach_kprobe(kernel_fun,probe_fun);
    if(s.code() != 0){

        std::cout <<"attach_kernel_fun: " << s.msg() <<std::endl;
    }
    else {
       break;
    }
    }
}

//leveldb::Cachestat_eBPF::~Cachestat_eBPF()
//{
//    std::cout<<"Cachestat_eBPF::~Cachestat_eBPF()"<<std::endl;
//    detach_kernel_probe_event();
//    bpf_->detach_all();
//    delete bpf_;
//}
