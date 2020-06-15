// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include <iostream>

namespace leveldb {

class VersionSet;

struct Buffer;

//whc change, then cyf change it.
struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  std::vector<InternalKey> percent_size_key;     //cyf: percent_size_key[i] shows index key of (i*10)% of SST's size

  Buffer* buffer;//whc add

  FileMetaData() : refs(0), allowed_seeks(100), file_size(0),buffer(NULL) {

      percent_size_key.reserve(config::kLDCLinkKVSizeInterval);
      InternalKey key;
      key.DecodeFrom(Slice("0000000000000000"));
      for (size_t i = 0; i < config::kLDCLinkKVSizeInterval; ++i) {
          percent_size_key.push_back(key);

      }
  }

  /*explicit FileMetaData(FileMetaData& f) : refs(f.refs), allowed_seeks(f.allowed_seeks), file_size(f.file_size),buffer(NULL) {

      percent_size_key.reserve(config::kLDCLinkKVSizeInterval);
      InternalKey key;
      //key.DecodeFrom(Slice("0000000000000000"));
      for (size_t i = 0; i < config::kLDCLinkKVSizeInterval; ++i) {
          percent_size_key.push_back(f.percent_size_key[i]);

      }
  }*/



  
};

//whc add
struct BufferTable{
	int refs;
	uint64_t number;

	BufferTable(uint64_t n):number(n),refs(0){}
};

struct BufferNode{
	InternalKey smallest;
	InternalKey largest;
	uint64_t number;
	uint64_t size;
	uint64_t sequence;
    uint64_t filesize;

	BufferNode(InternalKey& s,InternalKey& l,uint64_t n,uint64_t si,uint64_t se,uint64_t fs):smallest(s),
			largest(l),
			number(n),
			size(si),
			sequence(se),
            filesize(fs){}
};

struct Buffer{
	std::vector<BufferNode> nodes;
	InternalKey smallest;
	InternalKey largest;
	uint64_t size;

    Buffer(){size = 0;}

};

struct BufferNodeEdit{
	InternalKey smallest;
	InternalKey largest;
	uint64_t snumber;  //source number
	uint64_t dnumber; // destination number
	uint64_t size;
    uint64_t filesize;
	bool inend; //true is in end buffer false is not
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddFile(int level, uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest,
               /*cyf add this default parameter*/
               std::vector<InternalKey>* p = nullptr) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    //cyf: adding key size distribution in MANIFEST seems to be so boring......
    if(p != nullptr){

        if((*p).size() >= (config::kLDCLinkKVSizeInterval -1)) {
            for (size_t i=0;i<config::kLDCLinkKVSizeInterval;i++) {
            f.percent_size_key[i].DecodeFrom((*p)[i].Encode());
            }
        }else {
            std::cout<<"AddFile() get an uncompleted percent_size_key!"<<std::endl;
        }
    }else {
            if(f.percent_size_key.size() > 0) f.percent_size_key[0].DecodeFrom(smallest.Encode());
            for (size_t i = 0; i < (config::kLDCLinkKVSizeInterval -1); ++i) {
                //f.percent_size_key.push_back(largest);
                f.percent_size_key[i].DecodeFrom(largest.Encode());
        }

    }
    new_files_.push_back(std::make_pair(level, f));
  }

  // Delete the specified "file" from the specified "level".
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  //whc add
  void AddBufferNode(int level,
                     uint64_t snumber, uint64_t ssize,
                     uint64_t dnumber, uint64_t size,
                     InternalKey& smallest,
                     InternalKey& largest,
                     bool inend ){
	  BufferNodeEdit b;
	  InternalKey fill;
      b.snumber = snumber;
	  b.dnumber = dnumber;
      //b.size = size;//cyf change the two size position
      b.size = ssize;
	  
      if(!inend)
        b.smallest = smallest;
	  else
        b.smallest = fill;
          
      b.largest = largest;
	  b.inend = inend;
      //b.filesize = ssize;
      b.filesize = size;
	  new_buffer_nodes.push_back(std::make_pair(level, b));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  DeletedFileSet deleted_files_;
  std::vector< std::pair<int, FileMetaData> > new_files_;
  //whc add
  std::vector< std::pair<int, BufferNodeEdit> > new_buffer_nodes;
  std::vector<int> reset_end_levels;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
