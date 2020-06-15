TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt

INCLUDEPATH += ../
INCLUDEPATH += ../db
INCLUDEPATH += ../include/
#cyf add for using eBPF
INCLUDEPATH += ../../bcc/src/cc/libbpf/include/uapi/
INCLUDEPATH += ../../bcc/src/cc/api
INCLUDEPATH += ../../bcc/src/cc/

SOURCES += \
    ../db/autocompact_test.cc \
    ../db/buffer_iterator.cc \
    ../db/builder.cc \
    ../db/c.cc \
    ../db/corruption_test.cc \
    ../db/db_bench.cc \
    ../db/db_impl.cc \
    ../db/db_iter.cc \
    ../db/db_test.cc \
    ../db/dbformat.cc \
    ../db/dbformat_test.cc \
    ../db/dumpfile.cc \
    ../db/fault_injection_test.cc \
    ../db/filename.cc \
    ../db/filename_test.cc \
    ../db/leveldbutil.cc \
    ../db/log_reader.cc \
    ../db/log_test.cc \
    ../db/log_writer.cc \
    ../db/memtable.cc \
    ../db/recovery_test.cc \
    ../db/repair.cc \
    ../db/skiplist_test.cc \
    ../db/table_cache.cc \
    ../db/version_edit.cc \
    ../db/version_edit_test.cc \
    ../db/version_set.cc \
    ../db/version_set_test.cc \
    ../db/write_batch.cc \
    ../db/write_batch_test.cc \
    ../doc/bench/db_bench_sqlite3.cc \
    ../doc/bench/db_bench_tree_db.cc \
    ../helpers/memenv/memenv.cc \
    ../helpers/memenv/memenv_test.cc \
    ../issues/issue178_test.cc \
    ../issues/issue200_test.cc \
    ../port/port_posix.cc \
    ../table/block.cc \
    ../table/block_builder.cc \
    ../table/filter_block.cc \
    ../table/filter_block_test.cc \
    ../table/format.cc \
    ../table/iterator.cc \
    ../table/merger.cc \
    ../table/table.cc \
    ../table/table_builder.cc \
    ../table/table_test.cc \
    ../table/two_level_iterator.cc \
    ../util/arena.cc \
    ../util/arena_test.cc \
    ../util/bloom.cc \
    ../util/bloom_test.cc \
    ../util/cache.cc \
    ../util/cache_test.cc \
    ../util/cachestat_ebpf.cc \
    ../util/coding.cc \
    ../util/coding_test.cc \
    ../util/comparator.cc \
    ../util/crc32c.cc \
    ../util/crc32c_test.cc \
    ../util/env.cc \
    ../util/env_posix.cc \
    ../util/env_test.cc \
    ../util/filter_policy.cc \
    ../util/hash.cc \
    ../util/hash_test.cc \
    ../util/histogram.cc \
    ../util/logging.cc \
    ../util/options.cc \
    ../util/status.cc \
    ../util/testharness.cc \
    ../util/testutil.cc \
    ../db/c_test.c

DISTFILES += \
    ../Makefile \
    ../perf.data \
    ../build_detect_platform \
    ../doc/doc.css \
    ../doc/benchmark.html \
    ../doc/impl.html \
    ../doc/index.html \
    ../CONTRIBUTING.md \
    ../README.md \
    ../doc/log_format.txt \
    ../doc/table_format.txt \
    ../LICENSE \
    ../NEWS \
    ../TODO \
    ../AUTHORS \
    ../port/README

SUBDIRS += \
    leveldb_2pc.pro

HEADERS += \
    ../db/buffer_iterator.h \
    ../db/builder.h \
    ../db/db_impl.h \
    ../db/db_iter.h \
    ../db/dbformat.h \
    ../db/filename.h \
    ../db/log_format.h \
    ../db/log_reader.h \
    ../db/log_writer.h \
    ../db/memtable.h \
    ../db/skiplist.h \
    ../db/snapshot.h \
    ../db/table_cache.h \
    ../db/version_edit.h \
    ../db/version_set.h \
    ../db/write_batch_internal.h \
    ../helpers/memenv/memenv.h \
    ../include/leveldb/c.h \
    ../include/leveldb/cache.h \
    ../include/leveldb/comparator.h \
    ../include/leveldb/db.h \
    ../include/leveldb/dumpfile.h \
    ../include/leveldb/env.h \
    ../include/leveldb/filter_policy.h \
    ../include/leveldb/iterator.h \
    ../include/leveldb/options.h \
    ../include/leveldb/slice.h \
    ../include/leveldb/status.h \
    ../include/leveldb/table.h \
    ../include/leveldb/table_builder.h \
    ../include/leveldb/write_batch.h \
    ../port/win/stdint.h \
    ../port/atomic_pointer.h \
    ../port/port.h \
    ../port/port_example.h \
    ../port/port_posix.h \
    ../port/thread_annotations.h \
    ../table/block.h \
    ../table/block_builder.h \
    ../table/filter_block.h \
    ../table/format.h \
    ../table/iterator_wrapper.h \
    ../table/merger.h \
    ../table/two_level_iterator.h \
    ../util/arena.h \
    #../util/cachestat_ebpf.h \
    ../util/coding.h \
    ../util/crc32c.h \
    ../util/hash.h \
    ../util/histogram.h \
    ../util/logging.h \
    ../util/mutexlock.h \
    ../util/posix_logger.h \
    ../util/random.h \
    ../util/testharness.h \
    ../util/testutil.h
