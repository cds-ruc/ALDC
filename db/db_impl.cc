// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include <iostream>
#include <fstream>

#include <thread>
#include <pthread.h>

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

bool DBImpl::isProbingEnd =false;
bool DBImpl::swith_isprobe_start = true;
double DBImpl::LDC_MERGE_RATIO_ = config::kLDCMergeSizeRatio;
uint32_t DBImpl::LDC_MERGE_LINK_NUM_ = config::kThresholdBufferNum;//cyf add for link clear under read heavy workload
uint32_t DBImpl::LDC_AMPLIFY_FACTOR_ = config::kCuttleTreeAmplifyFactor;
uint64_t DBImpl::CuttleTreeFirstLevelSize  = config::kCuttleTreeFirstLevelSize;
// Information kept for every waiting writer
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

struct DBImpl::CompactionState {
  Compaction* const compaction;

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  // Files produced by compaction
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest;
    InternalKey largest;
    std::vector<InternalKey> p_size_key;//cyf add for get key size distribution
    explicit Output(){
        p_size_key.reserve(config::kLDCLinkKVSizeInterval);
        InternalKey key;
        key.DecodeFrom(Slice("0000000000000000"));
        for (size_t i = 0; i < config::kLDCLinkKVSizeInterval; ++i) {
            p_size_key.push_back(key);
        }
    }
  };
  std::vector<Output> outputs;

  // State kept for output being generated
  WritableFile* outfile;
  TableBuilder* builder;

  uint64_t total_bytes;

  //whc add
  std::vector<FileMetaData> buffer_input;//cyf seems no use

  Output* current_output() { return &outputs[outputs.size()-1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        outfile(NULL),
        builder(NULL),
        total_bytes(0) {
  }
};


//whc add
struct DBImpl::PartialCompactionStats{
  int64_t micros;
  int64_t bytes_read;
  int64_t bytes_written;

  // level compact times
  int64_t compact_times;

  // number of level file reads and writes when compaction
  int64_t read_file_nums;
  int64_t write_file_nums;
  PartialCompactionStats() :
      micros(0),
      bytes_read(0),
      bytes_written(0),
      compact_times(0),
      read_file_nums(0),
      write_file_nums(0) { }
  void Add(const PartialCompactionStats& c) {
    this->micros += c.micros;
    this->bytes_read += c.bytes_read;
    this->bytes_written += c.bytes_written;
    this->compact_times += c.compact_times;
    this->read_file_nums += c.read_file_nums;
    this->write_file_nums += c.write_file_nums;
  }

  void SubstractBy(const PartialCompactionStats& c) {
    this->micros = c.micros - this->micros;
    this->bytes_read = c.bytes_read - this->bytes_read;
    this->bytes_written = c.bytes_written - this->bytes_written;
    this->compact_times = c.compact_times - this->compact_times;
    this->read_file_nums = c.read_file_nums - this->read_file_nums;
    this->write_file_nums = c.write_file_nums - this->write_file_nums;
  }
};

struct DBImpl::OneTimeCompactionStats {
  PartialCompactionStats partial_stats;
  int64_t ll_file_num;
  int64_t hl_file_num;
  OneTimeCompactionStats() :
      partial_stats(),
      ll_file_num(-1),
      hl_file_num(-1) { }
  };

// Per level compaction stats.  stats_[level] stores the stats for
// compactions that produced data for the specified "level".
struct DBImpl::CompactionStats {
  static const int max_read_file_nums = 50;
  PartialCompactionStats partial_stats;

  // ll_num + hl_num compact times
  uint64_t lh_compact_times[max_read_file_nums][max_read_file_nums];

  CompactionStats() : partial_stats() {
    memset(lh_compact_times, 0, sizeof(lh_compact_times));
  }

  static void UpdateWhileCompact(const CompactionState* compact,
                                 int64_t micros,
                                 OneTimeCompactionStats& ll_stats,
                                 OneTimeCompactionStats& hl_stats);

  static void UpdateWhileBufferCompact(const CompactionState* compact,
                                                 int64_t micros,
                                                 OneTimeCompactionStats& ll_stats,
                                                 OneTimeCompactionStats& hl_stats,
                                                 uint64_t inputsize,
                                                 uint64_t outputsize);


  void Add(const OneTimeCompactionStats& c) {
	this->partial_stats.Add(c.partial_stats);
    if(c.ll_file_num >= 0 && c.hl_file_num >= 0) {
      this->lh_compact_times[c.ll_file_num][c.hl_file_num] += 1;
    }
  }

  //cyf add for get the substraction value between two probes
  void SubstractBy(const CompactionStats& c) {
    this->partial_stats.SubstractBy(c.partial_stats);

      for(int i = 0; i< max_read_file_nums; i++)
          for(int j = 0; j < max_read_file_nums; j++){
              this->lh_compact_times[i][j] = c.lh_compact_times[i][j] - this->lh_compact_times[i][j];
          }

  }

};

void DBImpl::CompactionStats::UpdateWhileCompact(const CompactionState* compact,
                                                 int64_t micros,
                                                 OneTimeCompactionStats& ll_stats,
                                                 OneTimeCompactionStats& hl_stats) {
  // Only update read stats for low-level
  ll_stats.partial_stats.read_file_nums = compact->compaction->num_input_files(0);
  
  for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
    //hl_stats.partial_stats.bytes_read += compact->compaction->input(0, i)->file_size;
    //cyf change hl_stats to ll_stats
    ll_stats.partial_stats.bytes_read += compact->compaction->input(0, i)->file_size;
  }
  
  // High-level
  hl_stats.partial_stats.micros = micros;

  hl_stats.partial_stats.read_file_nums = compact->compaction->num_input_files(1);
  for (int i = 0; i < compact->compaction->num_input_files(1); i++) {
    hl_stats.partial_stats.bytes_read += compact->compaction->input(1, i)->file_size;
  }

  hl_stats.partial_stats.write_file_nums = compact->outputs.size();
  for(size_t i = 0; i < compact->outputs.size(); i++) {
    hl_stats.partial_stats.bytes_written += compact->outputs[i].file_size;
    //std::cout<<"level write: i="<<i<<" size="<<compact->outputs[i].file_size<<std::endl;
  }

  //std::cout<<"level write:"<<hl_stats.partial_stats.bytes_written<<std::endl;

  hl_stats.partial_stats.compact_times++;

  hl_stats.ll_file_num = ll_stats.partial_stats.read_file_nums;
  hl_stats.hl_file_num = hl_stats.partial_stats.read_file_nums;
}

void DBImpl::CompactionStats::UpdateWhileBufferCompact(const CompactionState* compact,
                                                 int64_t micros,
                                                 OneTimeCompactionStats& ll_stats,
                                                 OneTimeCompactionStats& hl_stats,
                                                 uint64_t inputsize,
                                                 uint64_t outputsize) {
  // Only update read stats for low-level
  //hl_stats.partial_stats.read_file_nums = compact->compaction->num_input_files(0);
  hl_stats.partial_stats.read_file_nums = compact->compaction->num_input_files(0);
  /*
  for (int i = 0; i < compact->compaction->num_input_files(0); i++) {
    hl_stats.partial_stats.bytes_read += compact->compaction->input(0, i)->file_size;
  }
  */
  // low-level(buffer total size)
  //ll_stats.partial_stats.read_file_nums = 0;
  //cyf change for use buffers' num as low level read file num
  ll_stats.partial_stats.read_file_nums = compact->compaction->input(0,0)->buffer->nodes.size();
  //ll_stats.partial_stats.bytes_read += inputsize - hl_stats.partial_stats.bytes_read;
  
  // High-level
  hl_stats.partial_stats.micros = micros;


  hl_stats.partial_stats.write_file_nums = compact->outputs.size();
  hl_stats.partial_stats.bytes_written += outputsize;
  hl_stats.partial_stats.bytes_read += inputsize;
  //std::cout<<"level write:"<<hl_stats.partial_stats.bytes_written<<std::endl;

  hl_stats.partial_stats.compact_times++;

  //hl_stats.ll_file_num = 0;
  //cyf change for use buffers' num as low level read file num
  hl_stats.ll_file_num = ll_stats.partial_stats.read_file_nums;

  hl_stats.hl_file_num = 1;
}

struct DBImpl::CompactionStats stats_[config::kNumLevels];
struct DBImpl::CompactionStats stmp_[config::kNumLevels];//cyf add for get values in a period


// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
  ClipToRange(&result.max_file_size,     1<<20,                       1<<30);
  ClipToRange(&result.block_size,        1<<10,                       4<<20);
  if (result.info_log == NULL) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
    //s = src.env->NewLogger(WInfoLogFileName(dbname), &w_log);
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  //whc change
  if (result.block_cache == NULL) {
      result.block_cache = NewLRUCache(config::kLDCBlockCacheSize);//cyf changed to 1GB, default 8MB
    //result.block_cache = NewLRUCache(8);
  }
  return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),
      internal_filter_policy_(raw_options.filter_policy),
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      db_lock_(NULL),
      shutting_down_(NULL),
      bg_cv_(&mutex_),
      mem_(NULL),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      tmp_batch_(new WriteBatch),
      bg_compaction_scheduled_(false),
      manual_compaction_(NULL),
      pth(NULL)
      //eBPF_(NULL)
      //ssdname_() {
    {
    has_imm_.Release_Store(NULL);

  // Reserve ten files or so for other uses and give the rest to TableCache.
  //whc change
  if(config::kSSDPath.length()>0)
      ssdname_ = config::kSSDPath;
  else ssdname_ = dbname_;
  
  
  //const int ssd_table_cache_size = 2500;
  const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
  //std::cout << "DBImpl:table_cache_size:" << table_cache_size << std::endl;
  table_cache_ = new TableCache(dbname_, &options_, table_cache_size);

  //whc add
  //ssd_table_cache_ = new TableCache(ssdname_, &options_, ssd_table_cache_size);
  ssd_table_cache_ = table_cache_;

//  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                             //&internal_comparator_);
  versions_ = new VersionSet(dbname_, &options_, table_cache_,
                               &internal_comparator_,ssdname_,ssd_table_cache_);

  //whc add
  //versions_ ->SetSSDCache(ssd_table_cache_);
  //whc add
  Status s = options_.env->NewLogger("/tmp/WLOG", &w_log);
  versions_->w_log = w_log;
  //VersionSet::Builder::TableCount = 0;
  //env_->StartThread(BCC_BGWork,nullptr);//cyf add for kernel probe


  //pthread_create(&pth,NULL,BCC_BGWork,(void*)this);




}

DBImpl::~DBImpl() {
  // whc add
	for(int i=0;i<config::kNumLevels;i++)
        std::cout<<"level: \t"<<i<<"\t Files nums: \t"<<versions_->current_->NumFiles(i)<<std::endl;
        
    std::cout<<"mem getnum: \t"<<ReadStatic::mem_get<<std::endl;
    for(int i=0;i<config::kNumLevels;i++)
        std::cout<<"level: \t"<<i<<"\t getnum: \t"<<ReadStatic::level_get[i]<<std::endl;
        
    std::cout<<"table get: \t "<<ReadStatic::table_get<<std::endl;
    std::cout<<"bloomfilter miss: \t "<<ReadStatic::table_bloomfilter_miss<<std::endl;
    std::cout<<"readfile miss: \t"<<ReadStatic::table_readfile_miss<<std::endl;
    std::cout<<"table cache shoot: \t"<<ReadStatic::table_cache_shoot<<std::endl;
    std::cout<<"data block read: \t"<<ReadStatic::data_block_read<<std::endl;
    std::cout<<"index size: \t"<<ReadStatic::index_block_size<<std::endl;
    std::cout<<"block cache read: \t"<<ReadStatic::block_cache_read<<std::endl;
    std::cout<<"Users Put request num: \t"<<ReadStatic::put_num<<std::endl;
    std::cout<<"Users Get request num: \t"<<ReadStatic::get_num<<std::endl;



    std::cout <<"run DBImpl::~DBImpl()"<<std::endl;
	// Wait for background work to finish
  mutex_.Lock();
  shutting_down_.Release_Store(this);  // Any non-NULL value is ok
  while (bg_compaction_scheduled_) {
    bg_cv_.Wait();
  }
  mutex_.Unlock();

  //cyf cancel the probe thread after back ground compaction totally complete.
  if(pth != NULL){
  int pc = pthread_cancel(pth);
  void* res ;
  pthread_join(pth,&res);
  if(res == PTHREAD_CANCELED) std::cout<< "BCC_WORK thread is canceled!"<<std::endl;
  }

  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);
  }
  
  //whc add
  // print db property
  std::string sst_property;
  this->GetProperty("leveldb.sstables", &sst_property);
  std::string mem_usage;
  this->GetProperty("leveldb.approximate-memory-usage", &mem_usage);
  std::string stats_property;
  this->GetProperty("leveldb.stats", &stats_property);
  std::string lh_compact_times;
  this->GetProperty("leveldb.lh_compact_times", &lh_compact_times);

//  std::cout << "##sst_property" << std::endl;
//  std::cout << sst_property << std::endl;
  std::cout << "##mem_usage :\t" << std::endl;
  std::cout << mem_usage << std::endl;
  std::cout << "##stats_property: \t" << std::endl;
  std::cout << stats_property << std::endl;
  std::cout << "##lh_compact_times: \t" << std::endl;
  std::cout << lh_compact_times << std::endl;


  //cyf add for printing link number info
  /*
  uint64_t total = 0;
  for (int i = 0; i < config::kBufferResveredNum; ++i) {
      std::cout << "LDC_link_num:"<<i<<" 's compaction did "<<link_stats_LDC_[i]<<" times."<<std::endl;
      total += link_stats_LDC_[i];
  }
  std::cout <<"the total link times are: "<<total<<std::endl;
  */

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file);
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->DeleteFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

void DBImpl::DeleteObsoleteFiles() {
  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) {
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);
      }
    }
  }
  
  if(config::kSwitchSSD){
      std::vector<std::string> ssd_filenames;
      env_->GetChildren(ssdname_, &ssd_filenames); // Ignoring errors on purpose
      uint64_t ssd_number;
      FileType ssd_type;
      for (size_t i = 0; i < ssd_filenames.size(); i++) {
        if (ParseFileName(ssd_filenames[i], &ssd_number, &ssd_type)) {
          bool keep = true;
          switch (ssd_type) {
            case kLogFile:
              keep = ((ssd_number >= versions_->LogNumber()) ||
                      (ssd_number == versions_->PrevLogNumber()));
              break;
            case kDescriptorFile:
              // Keep my manifest file, and any newer incarnations'
              // (in case there is a race that allows other incarnations)
              keep = (ssd_number >= versions_->ManifestFileNumber());
              break;
            case kTableFile:
              keep = (live.find(ssd_number) != live.end());
              break;
            case kTempFile:
              // Any temp files that are currently being written to must
              // be recorded in pending_outputs_, which is inserted into "live"
              keep = (live.find(ssd_number) != live.end());
              break;
            case kCurrentFile:
            case kDBLockFile:
            case kInfoLogFile:
              keep = true;
              break;
          }

          if (!keep) {
            if (ssd_type == kTableFile) {
              ssd_table_cache_->Evict(ssd_number);
            }
            Log(options_.info_log, "Delete type=%d #%lld\n",
                int(type),
                static_cast<unsigned long long>(ssd_number));
            env_->DeleteFile(ssdname_ + "/" + ssd_filenames[i]);
          }
        }
      }
  }
  
}

Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
    if (options_.create_if_missing) {
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    snprintf(buf, sizeof(buf), "%d missing files; e.g.",
             static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
      mem->Unref();
      mem = NULL;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == NULL);
    assert(log_ == NULL);
    assert(mem_ == NULL);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != NULL) {
        mem_ = mem;
        mem = NULL;
      } else {
        // mem can be NULL if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != NULL) {
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, NULL);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();
  FileMetaData meta;
  meta.number = versions_->NewFileNumber();
  pending_outputs_.insert(meta.number);
  Iterator* iter = mem->NewIterator();
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long) meta.number);

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long) meta.number,
      (unsigned long long) meta.file_size,
      s.ToString().c_str());
  delete iter;
  pending_outputs_.erase(meta.number);


  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();
    if (base != NULL) {
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }
    edit->AddFile(level, meta.number, meta.file_size,
                  meta.smallest, meta.largest);
  }



  OneTimeCompactionStats hl_stats;

  hl_stats.partial_stats.micros = env_->NowMicros() - start_micros;
  hl_stats.partial_stats.bytes_written = meta.file_size;
  hl_stats.partial_stats.compact_times++;
  if(meta.file_size > 0){
	hl_stats.partial_stats.write_file_nums++;
    hl_stats.ll_file_num = 1;
    hl_stats.hl_file_num = 0;
  }

  stats_[level].Add(hl_stats);

  return s;
}

void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != NULL);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.Acquire_Load()) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = NULL;
    has_imm_.Release_Store(NULL);
    DeleteObsoleteFiles();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == NULL) {
    manual.end = NULL;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
    if (manual_compaction_ == NULL) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      bg_cv_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = NULL;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // NULL batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), NULL);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != NULL && bg_error_.ok()) {
      bg_cv_.Wait();
    }
    if (imm_ != NULL) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    bg_cv_.SignalAll();
  }
}

void DBImpl::MaybeScheduleCompaction() {
    //struct cache_info cif =  this->ebpf_.get_cache_info();//cyf add
  mutex_.AssertHeld();
  if (bg_compaction_scheduled_) {
    // Already scheduled
  } else if (shutting_down_.Acquire_Load()) {
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == NULL &&
             manual_compaction_ == NULL &&
             !versions_->NeedsCompaction()) {
    // No work to be done
  } else {
    bg_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(bg_compaction_scheduled_);
  if (shutting_down_.Acquire_Load()) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  bg_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  bg_cv_.SignalAll();
}

//whc add
void DBImpl::CopyToSSD( void* state2){
	CompactionState* state = reinterpret_cast<CompactionState*>(state2);
	std::ifstream in;
	std::ofstream out;
	std::string SourceFile;
	std::string NewFile;
	std::vector<FileMetaData*>  inputs = state->compaction->GetLevelFIle();
	for(int i=0;i<inputs.size();i++){
		SourceFile = TableFileName(dbname_,inputs[i]->number);
		NewFile = TableFileName(ssdname_,inputs[i]->number);
		in.open(SourceFile.c_str(),std::ios::binary);//打开源文件
		  if(in.fail())//打开源文件失败
		  {
		     std::cout<<"Error 1: Fail to open the source file."<<std::endl;
		     in.close();
		     out.close();
		     return;
		  }
		  out.open(NewFile.c_str(),std::ios::binary);//创建目标文件
		  if(out.fail())//创建文件失败
		  {
		    std:: cout<<"Error 2: Fail to create the new file."<<std::endl;
		    std::cout<<NewFile<<std::endl;
		    out.close();
		     in.close();
		     return;
		  }
		  else//复制文件
		  {
		     out<<in.rdbuf();
		     out.close();
		     in.close();
		     return;
		  }
	}

}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != NULL) {
    CompactMemTable();
    return;
  }
  //struct cache_info cif =  this->ebpf_.get_cache_info();//cyf add
  Compaction* c;
  bool is_manual = (manual_compaction_ != NULL);
  InternalKey manual_end;
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == NULL);
    if (c != NULL) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level,
        (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == NULL) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
    //std::cout <<"cyf: suffering from IsTrivialMove()"<<std::endl;
    assert(c->num_input_files(0) == 1);
    FileMetaData* f = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), f->number);

    //cyf add for Level0 SST's default key size distribution
    //no matter how large the overlap range is, it's awlay 1/10 of SST's limit
    if (0) {
        for (size_t i = 0; i < config::kLDCLinkKVSizeInterval; ++i)

        std::cout << "Key-TrivialMove distribution:["<<i<<"] [ "
                  <<f->percent_size_key[i].Encode().ToString()
                  <<" ]"<<std::endl;
    }

    c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                       f->smallest, f->largest, /*cyf add*/&(f->percent_size_key));
    status = versions_->LogAndApply(c->edit(), &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number),
        c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(),
        versions_->LevelSummary(&tmp));
        
    //whc add
    //Log(w_log,"whc trivialmove in level%d\n",c->level());
  } else {
    CompactionState* compact = new CompactionState(c);
   //std::cout<<"backgroundcompaction:exp2: "<<compact->compaction->inputs_[1].size()<<std::endl;
   //CopyToSSD(compact);
   // status = DoCompactionWork(compact);
   //whc change
  //if(compact->compaction->level_== config::kBufferCompactStartLevel){
  
    bool flag = true;
    if(compact->compaction->level_<=1 || compact->compaction->inputs_[1].size()<12
    || BCJudge::IsBufferCompactLevel(compact->compaction->level_)){
      if(BCJudge::IsBufferCompactLevel(compact->compaction->level_)){
          if(config::kSwitchSSD){//default: false, use full-SSD env, no hybrid storage.
              mutex_.Unlock();
              CopyToSSD(compact);
              mutex_.Lock();
          }
          //std::cout<<"Have copied to SSD"<<std::endl;
          
          assert(c==compact->compaction);
          status = Dispatch(compact);//Level have LDC method to do Link and Merge operation
          flag = false;
      } else {
          status = DoCompactionWork(compact);//Level 0 won't have Link and Merge
      }
    }
  
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    
    //whc change this two lines's position
    if(flag){//cyf LDC's linked files should not be cleaned and released
        CleanupCompaction(compact);
        c->ReleaseInputs();
    }
    
    DeleteObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.Acquire_Load()) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log,
        "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = NULL;
  }
  //whc add
  //for(int i=0;i<config::kNumLevels;i++){
      //std::cout<<"level"<<i<<"  nums: "<<versions_->current_->NumFiles(i)
      //<<"  total size: "<<GetLevelTotalSize(i)<<std::endl;
  //}

}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != NULL) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == NULL);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  assert(compact != NULL);
  assert(compact->builder == NULL);
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.file_size = 0;
    out.smallest.Clear();
    out.largest.Clear();

    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  
  
  
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;
  
  

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  delete compact->outfile;
  compact->outfile = NULL;
  /*
  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable
    Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                                               output_number,
                                               current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log,
          "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long) output_number,
          compact->compaction->level(),
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }
  }
  */
  return s;
}

//whc add
//cyf: no use in LDC
Status DBImpl::FinishBufferCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  const uint64_t output_number = compact->current_output()->number;
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();
  
  
  
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;
  compact->total_bytes += current_bytes;
  delete compact->builder;
  compact->builder = NULL;
  
  

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  if (s.ok()) {
    s = compact->outfile->Close();
  }
  //whc change
  //delete compact->outfile;
  //compact->outfile = NULL;
  
  return s;
}


void* DBImpl::BCC_BGWork(void *db)
{

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
    //pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED,NULL);
    //pthread_detach(pthread_self());
    std::cout <<"BCC_BGWork is running~" <<std::endl;
    //struct cache_info cinfo;
    //Cachestat_eBPF bpf;
    //bpf.attach_kernel_probe_event();

    int64_t files_num_inlevel[config::kNumLevels];
    int64_t bytes_inlevel[config::kNumLevels];
    class ReadStatic readStatic;
    double probe_time;
    Probe_Timer<double> probe_timer;

    double acc_compaction_read_MB = 0;
    double acc_compaction_write_MB = 0;
    uint64_t acc_compaction_times = 0;

   // cinfo = bpf.get_cache_info();
    while(1){

        //start probe time count
        probe_timer.Start();

        if(1){
            std::thread::id tid = std::this_thread::get_id();

            memcpy(stmp_, stats_, sizeof(struct DBImpl::CompactionStats) * config::kNumLevels);
            for(int i = 0; i < config::kNumLevels; i++){
                files_num_inlevel[i] = reinterpret_cast<DBImpl*>(db)->versions_->NumLevelFiles(i);
                bytes_inlevel[i] = reinterpret_cast<DBImpl*>(db)->versions_->NumLevelBytes(i);
            }
            readStatic.getSnapShot();



            sleep(config::kLDCBCCProbeInterval);

            //cinfo = bpf.get_cache_info();
            //std::cout << "mpa: \t"<<cinfo.mpa<<"\t mbd: \t"<<cinfo.mbd
                      //<<"\t apcl: \t"<<cinfo.apcl<<"\t apd: \t"<<cinfo.apd<<std::endl;

            for(int i = 0; i < config::kNumLevels; i++)
                stmp_[i].SubstractBy(stats_[i]);

            readStatic.getReadStaticDelta();

            probe_time = probe_timer.End();
            std::cout << "#_Transaction_throughput_(KTPS): \t"
                <<(readStatic.readStaticDelta_.get_num
                   + readStatic.readStaticDelta_.put_num) / probe_time / 1000
                <<"\t Current_DBImpl::LDC_MERGE_RATIO_: "<<DBImpl::LDC_MERGE_RATIO_
                <<" this->LDC_MERGE_RATIO_: "<<  reinterpret_cast<DBImpl*>(db)->LDC_MERGE_RATIO_
                <<std::endl;

                double readRatio = readStatic.readStaticDelta_.get_num
                        / (readStatic.readStaticDelta_.get_num + readStatic.readStaticDelta_.put_num + 0.01);


                 double writeRatio = readStatic.readStaticDelta_.put_num
                        / (readStatic.readStaticDelta_.get_num + readStatic.readStaticDelta_.put_num + 0.01);



            if( true /*DBImpl::swith_isprobe_start*/){
                double rand_read4k_TP = 40; //40MB/s
                double rand_write4k_TP = 450;//450MB/s
                double user_read_MB = readStatic.readStaticDelta_.data_block_read == 0 ?
                            readStatic.readStaticDelta_.get_num  * 1024 / 1048576.0
                          : readStatic.readStaticDelta_.data_block_read * 1024 / 1048576.0;

                double user_write_MB = readStatic.readStaticDelta_.put_num * 1024 /1048576.0;
                double read_compaction_MB = 0;
                double write_compation_MB = 0;
                double delta_compaction_times = 0;
                acc_compaction_times = 0;
                acc_compaction_read_MB = 0;
                acc_compaction_write_MB = 0;

                for (int i=0;i<config::kNumLevels;i++){
                    read_compaction_MB += (stmp_[i].partial_stats.bytes_read /1048576.0);
                    write_compation_MB += (stmp_[i].partial_stats.bytes_written /1048576.0);
                    delta_compaction_times += (stmp_[i].partial_stats.compact_times);

                    acc_compaction_read_MB += (stats_[i].partial_stats.bytes_read /1048576.0);
                    acc_compaction_write_MB += (stats_[i].partial_stats.bytes_written /1048576.0);
                    acc_compaction_times += (stats_[i].partial_stats.compact_times);

                }

                double current_score = (user_read_MB + read_compaction_MB )/ rand_read4k_TP
                        + (write_compation_MB - user_write_MB) / rand_write4k_TP;

                double increase_score = (user_read_MB * 2 + read_compaction_MB / 2 ) / rand_read4k_TP
                        + (write_compation_MB /2 - user_write_MB) / rand_write4k_TP;

                double decrease_score = (user_read_MB / 2 + read_compaction_MB * 2 ) / rand_read4k_TP
                        + (write_compation_MB * 2 - user_write_MB) / rand_write4k_TP;

                //cyf add compactionIOLimitFactor to avoid to bring huge amount compaction when
                //decrease the LDC_MERGE_RATIO_ parameter too sharply, so use it as a threshold
                double compactionIOLimitFactor = decrease_score / (current_score + 0.001);

             if(config::kUseAdaptiveLDC)
               {
                if((current_score <= increase_score) && (current_score <= decrease_score)){
                    //std::cout<< "No need to tune DBImpl::LDC_MERGE_RATIO!"<<std::endl;

                } else if( (increase_score < decrease_score)){

                    DBImpl::LDC_MERGE_RATIO_ =
                            (DBImpl::LDC_MERGE_RATIO_ * 2) >= 2.0 ? 2.0 : DBImpl::LDC_MERGE_RATIO_ * 2 ;

                } else if( ( (increase_score >= decrease_score) || (readRatio > 0.7) )){
                    if(compactionIOLimitFactor > config::kCompactionIOLimitFactorThreshold)
                    {
                        DBImpl::LDC_MERGE_RATIO_ =
                            (DBImpl::LDC_MERGE_RATIO_ - 0.1) >= 0.1 ? (DBImpl::LDC_MERGE_RATIO_ - 0.1): 0.1;
                        std::cout<< "decrease the LDC_MERGE_RATIO_ parameter softly by 0.1 "<<std::endl;

                        if(db == nullptr){
                            std::cout <<"reinterpret_cast<DBImpl*>(db) is nullptr"<<std::endl;
                        }
                        else{
                            reinterpret_cast<DBImpl*>(db)->MaybeScheduleCompaction();

                        }


                    }
                    else
                    {
                        DBImpl::LDC_MERGE_RATIO_ =
                            (DBImpl::LDC_MERGE_RATIO_ / 2) >= 0.1 ? DBImpl::LDC_MERGE_RATIO_ / 2 : 0.1;
                        if(db == nullptr){
                            std::cout <<"reinterpret_cast<DBImpl*>(db) is nullptr"<<std::endl;
                        }
                        else{
                            reinterpret_cast<DBImpl*>(db)->MaybeScheduleCompaction();

                        }

                    }

                }

                if((readRatio >= 0.85) || (readRatio == 0.0 && writeRatio == 0.0))
                {
                    DBImpl::LDC_MERGE_LINK_NUM_ = 1;
                    DBImpl::LDC_AMPLIFY_FACTOR_ =
                            (DBImpl::LDC_AMPLIFY_FACTOR_ - 2) >= 6  ? DBImpl::LDC_AMPLIFY_FACTOR_ - 2 : 6;

                    DBImpl::CuttleTreeFirstLevelSize = config::kCuttleTreeFirstLevelSize / 2;

                    if(db == nullptr){
                        std::cout <<"reinterpret_cast<DBImpl*>(db) is nullptr"<<std::endl;
                    }
                    else{
                        reinterpret_cast<DBImpl*>(db)->MaybeScheduleCompaction();

                    }


                }else{
                    DBImpl::LDC_MERGE_LINK_NUM_ = config::kThresholdBufferNum;
                    DBImpl::LDC_AMPLIFY_FACTOR_ = config::kCuttleTreeAmplifyFactor;
                    DBImpl::CuttleTreeFirstLevelSize = config::kCuttleTreeFirstLevelSize;

                }
              }

             if(config::kUseCattleTreeMethods)
             {
                 if(readRatio >= 0.85)
                 {
                     DBImpl::LDC_AMPLIFY_FACTOR_ =
                             (DBImpl::LDC_AMPLIFY_FACTOR_ - 2) >= 8  ? DBImpl::LDC_AMPLIFY_FACTOR_ - 2 : 8;

                     DBImpl::CuttleTreeFirstLevelSize = config::kCuttleTreeFirstLevelSize * 0.8;
                 }
                 else if(readRatio <= 0.15)
                 {
                     DBImpl::LDC_AMPLIFY_FACTOR_ =
                             (DBImpl::LDC_AMPLIFY_FACTOR_ + 2) <= 12  ? DBImpl::LDC_AMPLIFY_FACTOR_ + 2 : 12;

                     DBImpl::CuttleTreeFirstLevelSize = config::kCuttleTreeFirstLevelSize * 1.5;
                 }else
                 {
                     DBImpl::LDC_AMPLIFY_FACTOR_ = config::kCuttleTreeAmplifyFactor;
                     DBImpl::CuttleTreeFirstLevelSize = config::kCuttleTreeFirstLevelSize;

                 }
             }
//cyf add :there is some statistic error in write_compaciton_MB to be fixed, now use read_compaction_MB *2 instead
             std::cout << "ReadRatio: "<<readRatio
                          <<" Link num: "<<DBImpl::LDC_MERGE_LINK_NUM_
                         <<" Amplify: "<< DBImpl::LDC_AMPLIFY_FACTOR_
                        <<" Lv1 size: "<<DBImpl::CuttleTreeFirstLevelSize/1048576.0
                       <<" Delta Compact_IO(MB): "<<(read_compaction_MB*2 + 0*write_compation_MB)
                      <<" Delta CompactTimes:"<<delta_compaction_times
                      << " Acc_compact(MB): "<< (acc_compaction_read_MB*2 + 0*acc_compaction_write_MB)
                      <<" Acc_comapactTimes: "<< acc_compaction_times
                         <<std::endl;

            }

            /*
            std::cout << "Probing cost time is: "<<probe_time<<" Seconds"<<std::endl;
            std::cout<<"Delta mem getnum: \t"<<readStatic.readStaticDelta_.mem_get<<std::endl;
            for(int i=0;i<config::kNumLevels;i++)
                std::cout<<"Delta level: \t"<<i<<"\t getnum: \t"<<readStatic.readStaticDelta_.level_get[i]<<std::endl;

            std::cout<<"Delta table get: \t "<<readStatic.readStaticDelta_.table_get<<std::endl;
            std::cout<<"Delta bloomfilter miss: \t "<<readStatic.readStaticDelta_.table_bloomfilter_miss<<std::endl;
            std::cout<<"Delta readfile miss: \t"<<readStatic.readStaticDelta_.table_readfile_miss<<std::endl;
            std::cout<<"Delta table cache shoot: \t"<<readStatic.readStaticDelta_.table_cache_shoot<<std::endl;
            std::cout<<"Delta data block read: \t"<<readStatic.readStaticDelta_.data_block_read<<std::endl;
            std::cout<<"Delta index size: \t"<<readStatic.readStaticDelta_.index_block_size<<std::endl;
            std::cout<<"Delta block cache read: \t"<<readStatic.readStaticDelta_.block_cache_read<<std::endl;

            std::cout<<"Delta Users Get request num: \t"<<readStatic.readStaticDelta_.get_num<<std::endl;
            std::cout<<"Delta Users Put request num: \t"<<readStatic.readStaticDelta_.put_num<<std::endl;

                std::string value;
                char buf[500];

                printf(
                         "                               Delta_Compactions\n"
                         "Level   Files  Size(MB)   Time(sec)   Read(MB)   Write(MB)  ReadFiles   WriteFiles   CompactTimes\n"
                         "-------------------------------------------------------------------------------------------------\n"
                         );


                for (int level = 0; level < config::kNumLevels; level++) {
                  int files = reinterpret_cast<DBImpl*>(db)->versions_->NumLevelFiles(level) ;
                  if ( true ) {
                    printf(
                             " %3d  %8d  %9.0lf  %9.0lf  %9.0lf  %9.0lf  %10lld  %10lld  %10lld\n",
                             level,
                             files - files_num_inlevel[level],
                             (reinterpret_cast<DBImpl*>(db)->versions_->NumLevelBytes(level) - bytes_inlevel[level]) / 1048576.0,
                             stmp_[level].partial_stats.micros / 1e6,
                             stmp_[level].partial_stats.bytes_read / 1048576.0,
                             stmp_[level].partial_stats.bytes_written / 1048576.0,
                             stmp_[level].partial_stats.read_file_nums,
                             stmp_[level].partial_stats.write_file_nums,
                             stmp_[level].partial_stats.compact_times);

                  }
                }

                for (int level = 1; level < config::kNumLevels; level++) {
                  int files = reinterpret_cast<DBImpl*>(db)->versions_->NumLevelFiles(level);
                  if (stmp_[level].partial_stats.micros > 0 || files > 0) {
                    printf("Level %d: ", level);

                    for(int i = 0; i < stmp_[level-1].max_read_file_nums; i++){
                      for(int j = 0; j < stmp_[level].max_read_file_nums; j++){
                        if(stmp_[level].lh_compact_times[i][j] > 0){
                          printf(" (%d+%d,%lld) ", i, j, stmp_[level].lh_compact_times[i][j]);

                        }
                      }
                    }

                  }
                  std::cout << std::endl;
                }*/

        }


    }

    std::cout <<"thread is finished, tid:"<<std::this_thread::get_id()<<std::endl;


}

void DBImpl::ProbeKernelFunction()//cyf won't use anymore
{

    std::cout << "ProbeKernelFunction while outer~ "<< std::endl;
    //struct cache_info cinfo;
    while(true){
        std::cout << "ProbeKernelFunction while inner"<< std::endl;


        if(DBImpl::isProbingEnd){

            break;

        }
    std::thread::id tid = std::this_thread::get_id();
    //cinfo = ebpf_.get_cache_info();

    memcpy(stmp_, stats_, sizeof(struct DBImpl::CompactionStats) * config::kNumLevels);
    sleep(10);


    //std::cout <<"current tid: " << tid << "stmp_[1].partial_stats.bytes_written: "<<stmp_[1].partial_stats.bytes_written<< std::endl;
    //std::cout <<"current tid: " << tid <<"stats_[1].partial_stats.bytes_written: "<<stats_[1].partial_stats.bytes_written<< std::endl;

    for(int i = 0; i < config::kNumLevels; i++)
        stmp_[i].SubstractBy(stats_[i]);

    //std::cout << "SubstractBy stmp_[1].partial_stats.bytes_written: "<<stmp_[1].partial_stats.bytes_written<< std::endl;


        std::string value;
        char buf[1000];
        uint64_t total_compaction_num = 0;//cyf add
        uint64_t total_compaction_duration = 0;
        printf(
                 "                               Compactions\n"
                 "Level   Files  Size(MB)   Time(sec)   Read(MB)   Write(MB)  ReadFiles   WriteFiles   CompactTimes\n"
                 "-------------------------------------------------------------------------------------------------\n"
                 );

        //continue;
        for (int level = 0; level < config::kNumLevels; level++) {
          int files = 0;//d->versions_->NumLevelFiles(level);
          if ( stats_[level].partial_stats.micros >= 0 || files >= 0) {
            printf(
                     "\n %3d  %8d  %9.0lf  %9.0lf  %9.0lf  %9.0lf  %10lld  %10lld  %10lld\n",
                     level,
                     files,
                     0,//d->versions_->NumLevelBytes(level) / 1048576.0,
                     stats_[level].partial_stats.micros / 1e6,
                     stats_[level].partial_stats.bytes_read / 1048576.0,
                     stats_[level].partial_stats.bytes_written / 1048576.0,
                     stats_[level].partial_stats.read_file_nums,
                     stats_[level].partial_stats.write_file_nums,
                     stats_[level].partial_stats.compact_times);


          }
        }
        //snprintf(buf,sizeof (buf),"Total compaction times: %llu \n", total_compaction_num);
        //value.append(buf);
        //snprintf(buf,sizeof (buf),"Total compaction duration: %llu \n", total_compaction_duration);
        //value.append(buf);
        //std::cout << value <<std::endl;






    }
}


Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

 //whc change
  const int level = compact->compaction->level();
  // Add compaction outputs
 if(!BCJudge::IsBufferCompactLevel(level))
	 compact->compaction->AddInputDeletions(compact->compaction->edit());
 else compact->compaction->AddInputUpDeletions(compact->compaction->edit());
 //const int level = compact->compaction->level();
  //whc change
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    //if((!compact->compaction->IsBufferCompact) || level != config::kBufferCompactStartLevel +1)
    if((!compact->compaction->IsBufferCompact) || (!BCJudge::IsBufferCompactLevel(level-1)))
    compact->compaction->edit()->AddFile(
        level + 1,
        out.number, out.file_size, out.smallest, out.largest,
                /*cyf add*/&(compact->outputs[i].p_size_key));
    else compact->compaction->edit()->AddFile(
        level,
        out.number, out.file_size, out.smallest, out.largest,
                /*cyf add*/&(compact->outputs[i].p_size_key));

  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  //whc add
  uint64_t input_size = 0;
  uint64_t output_size = 0;
  //cyf add
  size_t key_distribution_index = 0;

  Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  if (snapshots_.empty()) {//cyf: if use snapshot, be careful for the data consistency problem
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  Iterator* input = versions_->MakeInputIterator(compact->compaction);
  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    //input_size += input->key().size() + input->value().size();
      // Prioritize immutable compaction work
    if (has_imm_.NoBarrier_Load() != NULL) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != NULL) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      //output_size += input->key().size() + input->value().size();
        // Open output file if necessary
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);//cyf: set the smallest key for output
        //cyf add to record the key distribution
        key_distribution_index = 0;
        compact->current_output()->p_size_key[key_distribution_index].DecodeFrom(key);//p_size_key[0] equals smallest key
        key_distribution_index++;//set index to point to next pos
      }
      compact->current_output()->largest.DecodeFrom(key);//cyf: update the largest, key by key
      compact->builder->Add(key, input->value());

      //cyf add
      //std::cout<< "cyf builder->filesize: "<<compact->builder->FileSize()<<std::endl;
      if (static_cast<double>( (config::kLDCLinkKVSizeInterval -1)
                               * compact->builder->FileSize() / options_.max_file_size)
              >= (1.0 * key_distribution_index) )
      {
          //std::cout<< "cyf builder->filesize reach the limit: "<<compact->builder->FileSize()<<std::endl;
          compact->current_output()->p_size_key[key_distribution_index].DecodeFrom(key);
          if(key_distribution_index < (config::kLDCLinkKVSizeInterval - 1))
              key_distribution_index++;
      }
      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
          //cyf add
          if(key_distribution_index < config::kLDCLinkKVSizeInterval){
              for (int index = key_distribution_index; index < config::kLDCLinkKVSizeInterval; index++) {
                  compact->current_output()->p_size_key[index].DecodeFrom(key);
              }

          }

          /*  for (int j=0;j<config::kLDCLinkKVSizeInterval;j++) {
              std::cout << "cyf DoCompactionWork Key distribution:["<<j<<"] [ "
                        <<compact->current_output()->p_size_key[j].Encode().ToString()
                       <<" ]"<<std::endl;
          } */

        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }//endfor: for (; input->Valid() && !shutting_down_.Acquire_Load(); )

  if (status.ok() && shutting_down_.Acquire_Load()) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;



  mutex_.Lock();

  
  if (status.ok()){
      OneTimeCompactionStats ll_stats;
      OneTimeCompactionStats hl_stats;

      int mylevel = compact->compaction->level();
      int64_t micros = env_->NowMicros() - start_micros - imm_micros;

      CompactionStats::UpdateWhileCompact(compact, micros, ll_stats, hl_stats);
      stats_[compact->compaction->level()].Add(ll_stats);
      stats_[compact->compaction->level() + 1].Add(hl_stats);
  }
  
  if (status.ok()) {
    status = InstallCompactionResults(compact);
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "compacted to: %s", versions_->LevelSummary(&tmp));
  
  //CleanupCompaction(compact);
  //compact->compaction->ReleaseInputs();
  return status;
}

//whc add
uint64_t DBImpl::GetLevelTotalSize(int level){
    uint64_t tsize = 0;
    for(int i=0;i<versions_->current_->files_[level].size();i++)
        tsize += versions_->current_->files_[level][i]->file_size;
    return tsize;
}

Status DBImpl::Dispatch(CompactionState* compact) {

  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log,  "Disputing %d@%d + %d@%d files",
      compact->compaction->num_input_files(0),
      compact->compaction->level(),
      compact->compaction->num_input_files(1),
      compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);
  assert(compact->compaction->inputs_[1].size() != 0);
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->number_;
  }

  Status status;
  
  std::string ptr0_key;
  std::string ptr0_fill;
  int ptr1 = 0;

  //std::cout<<"dispatch:input.size()"<<compact->compaction->inputs_.size()<<std::endl;
  
  
  int i;
  for(i=0;i<compact->compaction->inputs_[0].size();i++){
      ptr0_key.assign(compact->compaction->inputs_[0][i]->smallest.Rep());
      
      while(ptr1<compact->compaction->inputs_[1].size() && 
      internal_comparator_.Compare(compact->compaction->inputs_[1][ptr1]->largest.Encode(),
      Slice(ptr0_key))<0 )
        ptr1++;
      
      bool flag = true;
      while(internal_comparator_.Compare(Slice(ptr0_key),
      compact->compaction->inputs_[0][i]->largest.Encode())<0){
          InternalKey nsmallest;
          nsmallest.DecodeFrom(Slice(ptr0_key));
          
          InternalKey nlargest;
          
          int tag = 0;//cyf: tag seems no use ......
          
          if(ptr1>=compact->compaction->inputs_[1].size()){
              nlargest.DecodeFrom(compact->compaction->inputs_[0][i]->largest.Encode());
              tag = 1;
          }else if(internal_comparator_.Compare(compact->compaction->inputs_[0][i]->largest,
                compact->compaction->inputs_[1][ptr1]->largest)>0){
              nlargest.DecodeFrom(compact->compaction->inputs_[1][ptr1]->largest.Encode());
              //std::cout<<"diapatch1:"<<nlargest.Rep()<<std::endl;
              tag = 2;
            }else{
              nlargest.DecodeFrom(compact->compaction->inputs_[0][i]->largest.Encode());
              tag = 3;
            }
          
          //cyf: inputs_[0][i]->file_size change to be the realed link fragement size.
          //std::cout<<"cyf: start AddBufferNode"<<std::endl;
          uint64_t link_size = 0;
          int link_start = 0;
          int link_end = 0;
          FileMetaData* f = compact->compaction->inputs_[0][i];
          for (size_t li = 0; li < config::kLDCLinkKVSizeInterval; ++li) {

              if(internal_comparator_.Compare(nsmallest,f->percent_size_key[link_start]) > 0)
                  link_start++;

              if(internal_comparator_.Compare(nlargest, f->percent_size_key[link_end]) > 0)
                  link_end++;
          }
          //std::cout<<"cyf: start AddBufferNode"<<"linkstart: "<<link_start<< " linkend: "<<link_end<<std::endl;

          if(link_end <= link_start){
              link_size = static_cast<uint64_t>(options_.max_file_size  / (config::kLDCLinkKVSizeInterval - 1));
          }
          else{
              link_size = static_cast<uint64_t>(options_.max_file_size
                                                * (static_cast<double>(link_end - link_start ) / config::kLDCLinkKVSizeInterval));
          }
          assert(link_size != 0);//cyf link size should not be 0


          if(ptr1<compact->compaction->inputs_[1].size()){

              assert(nlargest.Rep().size()>0);
              compact->compaction->edit_.AddBufferNode(compact->compaction->level_+1,
    						compact->compaction->inputs_[0][i]->number,
                            link_size,//cyf change
                            //compact->compaction->inputs_[0][i]->file_size,
							compact->compaction->inputs_[1][ptr1]->number,
                            compact->compaction->inputs_[0][i]->file_size,//0,
							nsmallest,
                            nlargest,
							flag);
               
                            assert(internal_comparator_.Compare(nlargest,
              compact->compaction->inputs_[1][ptr1]->largest)<=0);
              
              ptr0_key.assign(compact->compaction->inputs_[1][ptr1]->largest.Rep());
              ptr1++;
          }else{
              assert(nlargest.Rep().size()>0);
              compact->compaction->edit_.AddBufferNode(compact->compaction->level_+1,
    						compact->compaction->inputs_[0][i]->number,
                            link_size,//cyf change
                            //compact->compaction->inputs_[0][i]->file_size,
                            compact->compaction->inputs_[1][ptr1 - 1]->number,
                            compact->compaction->inputs_[0][i]->file_size,//0,
							nsmallest,
                            nlargest,
							flag);
             ptr0_key.assign(compact->compaction->inputs_[0][i]->largest.Rep());
          }
        flag = false;
        //std::cout<<"cyf: AddBuffer [ "<<nsmallest.Rep()<<" ~ "<<nlargest.Rep()<<" ]"<<" linksize is "<<link_size<<std::endl;
      }
  }

  if (status.ok()) {

    status = InstallCompactionResults(compact);

  }
  

  compact->compaction->ReleaseInputs();
  CleanupCompaction(compact);

  
  
  if(versions_->buffer_compact_switch_ ){
      //std::cout<<"dispatch:going to buffer compact"<<std::endl;
      //CleanupCompaction(compact);
      //compact->compaction->ReleaseInputs();
      Compaction* c2;
      c2 = versions_->PickCompaction();
      CompactionState* compact2 = new CompactionState(c2);
      for(size_t index = 0;index < compact2->compaction->inputs_[0].size();index++){
          status = BufferCompact(compact2,index);
          if (!status.ok())
              break;
      }
      
      if(!status.ok()){
          std::cout<<"buffercompact error!"<<std::endl;
          //return status;
      }
      
      delete compact2->outfile;
      compact2->outfile = NULL;
      status = InstallCompactionResults(compact2);
      
      compact2->compaction->ReleaseInputs();
      CleanupCompaction(compact2);
      //compact2->compaction->ReleaseInputs();
      
      delete c2;
      //std::cout<<"dispatch:going to buffer compact end"<<std::endl;  
  }
  
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log,
      "dispatch to: %s", versions_->LevelSummary(&tmp));
  
  //std::cout<<"diapatch end!"<<std::endl;
  //for(int i=0;i<config::kNumLevels;i++)
		//std::cout<<"level"<<i<<"  nums"<<versions_->current_->NumFiles(i)<<std::endl;
  return status;
}

//cyf: actually means LDC's Merge operation
Status DBImpl::BufferCompact(CompactionState* compact,int index){
    //DBImpl::swith_isprobe_start  = true;
    Status status;
    const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0;  // Micros spent doing imm_ compactions
    uint64_t input_size =0;
    uint64_t output_size =0;

    size_t key_distribution_index = 0;
    //std::cout<<"go into buffer compact"<<std::endl;
    assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
    assert(compact->builder == NULL);
    //assert(compact->outfile == NULL);
    
    if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
    } else {
        compact->smallest_snapshot = snapshots_.oldest()->number_;
    }

    
    mutex_.Unlock();

  Iterator* input = versions_->MakeBufferInputIterator(compact->compaction->inputs_[0][index],
    versions_->current()->sequence_);

  if(compact->compaction->inputs_[0][index]->buffer != nullptr)
      input_size = compact->compaction->inputs_[0][index]->buffer->size;

  input->SeekToFirst();
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;
  //std::cout<<"buffer compact going to loop!!!"<<std::endl;
  for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
    //std::cout<<"buffer compact loop!!!"<<std::endl;
      // Prioritize immutable compaction work
    //input_size += input->value().size() + input->key().size();//cyf change for get the origin buffer read data mount other than selected
    if (has_imm_.NoBarrier_Load() != NULL) {
      //std::cout<<"buffer compact imm!!!"<<std::endl;
        const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != NULL) {
        CompactMemTable();
        bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }
    //std::cout<<"buffer compact loop2!!!"<<std::endl;
    Slice key = input->key();
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key,
                                     Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
      //std::cout<<"buffer compact loop3!!!"<<std::endl;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      //std::cout<<"not drop"<<std::endl;
        // Open output file if necessary
      output_size += input->value().size() + input->key().size();
      if (compact->builder == NULL) {
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
        //cyf add to record the key distribution
        key_distribution_index = 0;
        compact->current_output()->p_size_key[key_distribution_index].DecodeFrom(key);//p_size_key[0] equals smallest key
        key_distribution_index++;//set index to point to next pos
      }
      compact->current_output()->largest.DecodeFrom(key);
      compact->builder->Add(key, input->value());

      //std::cout<<"buffer compact cur file size"<<compact->builder->FileSize()<<std::endl;
      //std::cout<<"buffer compact max file size"<<compact->compaction->MaxOutputFileSize()<<std::endl;

      //cyf add
      if (static_cast<double>((compact->builder->FileSize()
                               * (config::kLDCLinkKVSizeInterval - 1))
                              / options_.max_file_size)
              >= 1.0 * key_distribution_index)
      {
          compact->current_output()->p_size_key[key_distribution_index].DecodeFrom(key);
          if(key_distribution_index < (config::kLDCLinkKVSizeInterval - 1))
              key_distribution_index++;

      }

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) {
          //cyf add
          if(key_distribution_index < config::kLDCLinkKVSizeInterval){
              for (size_t index = key_distribution_index; index < config::kLDCLinkKVSizeInterval; index++) {
                  compact->current_output()->p_size_key[index].DecodeFrom(key);
              }

          }

           /*for (int j=0;j<config::kLDCLinkKVSizeInterval;j++) {
              std::cout << "cyf: Key-buffer distribution:["<<j<<"] [ "
                        <<compact->current_output()->p_size_key[j].Encode().ToString()
                       <<" ]"<<std::endl;
          }*/

        status = FinishCompactionOutputFile(compact, input);
        //std::cout<<"buffer compact out"<<std::endl;
        if (!status.ok()) {
          break;
        }
      }
    }
    //std::cout<<"buffer compact loop4!!!"<<std::endl;
    input->Next();
  }
   
   //std::cout<<"buffer compact end loop"<<std::endl;
   
   if(status.ok()){
       //std::cout<<"buffer compact end loop status ok 1"<<std::endl;
   }
   
  if (status.ok() && shutting_down_.Acquire_Load()) {
       //std::cout<<"Deleting DB during compaction"<<std::endl;
      status = Status::IOError("Deleting DB during compaction");
  }
  
   
  if (status.ok() && compact->builder != NULL) {
    status = FinishCompactionOutputFile(compact, input);
  }
  
  
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = NULL;
  /*
  CompactionStats stats;
  int nodenum = 0;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
      if(compact->compaction->input(which, i)->buffer!=NULL)
          nodenum += compact->compaction->input(which, i)->buffer->nodes.size();
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }
  
  Log(w_log,
  "whc file:%lld buffer:%lld output:%lld \n",
  compact->compaction->input(0, index)->file_size,
  input_size-compact->compaction->input(0, index)->file_size,
  output_size);
*/

  //Log(w_log,
  //"buffer compact level: %d \n",
  //compact->compaction->level()-1
  //);

  mutex_.Lock();
  
  if (status.ok()){
      OneTimeCompactionStats ll_stats;
      OneTimeCompactionStats hl_stats;

      //int mylevel = compact->compaction->level();
      int64_t micros = env_->NowMicros() - start_micros - imm_micros;

      CompactionStats::UpdateWhileBufferCompact(compact, micros, ll_stats, hl_stats,input_size,output_size);
      stats_[compact->compaction->level()-1].Add(ll_stats);
      stats_[compact->compaction->level()].Add(hl_stats);
  }
    
    return status;
}
    

namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  //whc add
  versions_-> iterator_sequence_ = versions_->current()->sequence_;

  cleanup->mu = &mutex_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  ReadStatic::get_num++;//cyf add
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
      //whc add
      ReadStatic::mem_get++;
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
      //whc add
      ReadStatic::mem_get++;
    } else {
      //s = current->Get(options, lkey, value, &stats);
      // whc change
      uint64_t data_block_read_pre = ReadStatic::data_block_read;
      uint64_t readfile_miss_pre = ReadStatic::table_readfile_miss;
      s = current->BufferGet(options, lkey, value, &stats);
      have_stat_update = true;
      //if ((ReadStatic::data_block_read - data_block_read_pre) - (ReadStatic::table_readfile_miss - //readfile_miss_pre) > 1)
          //std::cout<<"dbimpl get error"<<std::endl;
    }
    mutex_.Lock();
  }

  //if (have_stat_update && current->UpdateStats(stats)) {
    //whc change
  if (have_stat_update) {  
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutex_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {

    if(pth == NULL)
        pthread_create(&pth,NULL,BCC_BGWork,(void*)this);

  Writer w(&mutex_);
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;



  ReadStatic::put_num++;//cyf add

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }


  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128<<10)) {
    max_size = size + (128<<10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (
        allow_delay &&
        versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      bg_cv_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      bg_cv_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_;
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.Release_Store(imm_);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    //whc change
    char buf[400];
    uint64_t total_compaction_num = 0;//cyf add
    uint64_t total_compaction_duration = 0;
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level   Files  Size(MB)   Time(sec)   Read(MB)   Write(MB)  ReadFiles   WriteFiles   CompactTimes\n"
             "-------------------------------------------------------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].partial_stats.micros > 0 || files > 0) {
        snprintf(buf,
                 sizeof(buf),
                 "\n %3d  %8d  %9.0lf  %9.0lf  %9.0lf  %9.0lf  %10lld  %10lld  %10lld\n",
                 level,
                 files,
                 versions_->NumLevelBytes(level) / 1048576.0,
                 stats_[level].partial_stats.micros / 1e6,
                 stats_[level].partial_stats.bytes_read / 1048576.0,
                 stats_[level].partial_stats.bytes_written / 1048576.0,
                 stats_[level].partial_stats.read_file_nums,
                 stats_[level].partial_stats.write_file_nums,
                 stats_[level].partial_stats.compact_times);
                 value->append(buf);
                 total_compaction_num += stats_[level].partial_stats.compact_times;//cyf add
                 total_compaction_duration += stats_[level].partial_stats.micros;
      }
    }
    snprintf(buf,sizeof (buf),"Total compaction times: %llu \n", total_compaction_num);
    snprintf(buf,sizeof (buf),"Total compaction duration: %llu \n", total_compaction_duration);
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[100];
    snprintf(buf, sizeof(buf), "%llu",
             static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }else if (in == "lh_compact_times"){   // whc add
    char buf[200];
    for (int level = 1; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].partial_stats.micros > 0 || files > 0) {
        snprintf(buf, sizeof(buf), "Level %d: ", level);
        value->append(buf);
        for(int i = 0; i < stats_[level-1].max_read_file_nums; i++){
          for(int j = 0; j < stats_[level].max_read_file_nums; j++){
            if(stats_[level].lh_compact_times[i][j] > 0){
              snprintf(buf, sizeof(buf), " (%d+%d,%lld) ", i, j, stats_[level].lh_compact_times[i][j]);
              value->append(buf);
            }
          }
        }
        value->append("\n");
      }
    }
  }

  return false;
}

void DBImpl::GetApproximateSizes(
    const Range* range, int n,
    uint64_t* sizes) {
  // TODO(opt): better implementation
  Version* v;
  {
    MutexLock l(&mutex_);
    versions_->current()->Ref();
    v = versions_->current();
  }

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  {
    MutexLock l(&mutex_);
    v->Unref();
  }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {
  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname);
  
  //whc add
  //cyf no more use it
  //std::cout << "amplify=" << options.amplify << std::endl;
  std::cout << " DBImpl::LDC_AMPLIFY_FACTOR_ = " << DBImpl::LDC_AMPLIFY_FACTOR_<< std::endl;
  
  impl->mutex_.Lock();
  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == NULL) {
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->DeleteObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != NULL);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() {
}

Status DestroyDB(const std::string& dbname, const Options& options) {
	Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}


//cyf add for self-adaptive
WRSample::WRSample()
{
    writes_num_ = 0.0;
    reads_num_  = 0.0;

}

WRSample::~WRSample()
{

}


//W:R = 1:1, 1:4, 4:1 means WR balance, read heavy and write heavy mode
double WRSample::getWRRatio()
{
    if(writes_num_ == 0.0) return 0.01;
    else if (reads_num_ == 0.0) return 99.99;
    else return writes_num_ / reads_num_;

}

void WRSample::resetWRSample()
{
    reads_num_ = 0.0;
    writes_num_ = 0.0;

}

}  // namespace leveldb
