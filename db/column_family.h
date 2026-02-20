//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/memtable_list.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/write_batch_internal.h"
#include "db/write_controller.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/hash_containers.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class Version;
class VersionSet;
class VersionStorageInfo;
class MemTable;
class MemTableListVersion;
class CompactionPicker;
class Compaction;
class InternalKey;
class InternalStats;
class ColumnFamilyData;
class DBImpl;
class LogBuffer;
class InstrumentedMutex;
class InstrumentedMutexLock;
struct SuperVersionContext;
class BlobFileCache;
class BlobSource;

extern const double kIncSlowdownRatio;
// This file contains a list of data structures for managing column family
// level metadata.
//
// The basic relationships among classes declared here are illustrated as
// following:
//
//       +----------------------+    +----------------------+   +--------+
//   +---+ ColumnFamilyHandle 1 | +--+ ColumnFamilyHandle 2 |   | DBImpl |
//   |   +----------------------+ |  +----------------------+   +----+---+
//   | +--------------------------+                                  |
//   | |                               +-----------------------------+
//   | |                               |
//   | | +-----------------------------v-------------------------------+
//   | | |                                                             |
//   | | |                      ColumnFamilySet                        |
//   | | |                                                             |
//   | | +-------------+--------------------------+----------------+---+
//   | |               |                          |                |
//   | +-------------------------------------+    |                |
//   |                 |                     |    |                v
//   |   +-------------v-------------+ +-----v----v---------+
//   |   |                           | |                    |
//   |   |     ColumnFamilyData 1    | | ColumnFamilyData 2 |    ......
//   |   |                           | |                    |
//   +--->                           | |                    |
//       |                 +---------+ |                    |
//       |                 | MemTable| |                    |
//       |                 |  List   | |                    |
//       +--------+---+--+-+----+----+ +--------------------++
//                |   |  |      |
//                |   |  |      |
//                |   |  |      +-----------------------+
//                |   |  +-----------+                  |
//                v   +--------+     |                  |
//       +--------+--------+   |     |                  |
//       |                 |   |     |       +----------v----------+
// +---> |SuperVersion 1.a +----------------->                     |
//       |                 +------+  |       | MemTableListVersion |
//       +---+-------------+   |  |  |       |                     |
//           |                 |  |  |       +----+------------+---+
//           |      current    |  |  |            |            |
//           |   +-------------+  |  |mem         |            |
//           |   |                |  |            |            |
//         +-v---v-------+    +---v--v---+  +-----v----+  +----v-----+
//         |             |    |          |  |          |  |          |
//         | Version 1.a |    | memtable |  | memtable |  | memtable |
//         |             |    |   1.a    |  |   1.b    |  |   1.c    |
//         +-------------+    |          |  |          |  |          |
//                            +----------+  +----------+  +----------+
//
// DBImpl keeps a ColumnFamilySet, which references to all column families by
// pointing to respective ColumnFamilyData object of each column family.
// This is how DBImpl can list and operate on all the column families.
// ColumnFamilyHandle also points to ColumnFamilyData directly, so that
// when a user executes a query, it can directly find memtables and Version
// as well as SuperVersion to the column family, without going through
// ColumnFamilySet.
//
// ColumnFamilySet points to the latest view of the LSM-tree (list of memtables
// and SST files) indirectly, while ongoing operations may hold references
// to a current or an out-of-date SuperVersion, which in turn points to a
// point-in-time view of the LSM-tree. This guarantees the memtables and SST
// files being operated on will not go away, until the SuperVersion is
// unreferenced to 0 and destoryed.
//
// The following graph illustrates a possible referencing relationships:
//
// Column       +--------------+      current       +-----------+
// Family +---->+              +------------------->+           |
//  Data        | SuperVersion +----------+         | Version A |
//              |      3       |   imm    |         |           |
// Iter2 +----->+              |  +-------v------+  +-----------+
//              +-----+--------+  | MemtableList +----------------> Empty
//                    |           |   Version r  |  +-----------+
//                    |           +--------------+  |           |
//                    +------------------+   current| Version B |
//              +--------------+         |   +----->+           |
//              |              |         |   |      +-----+-----+
// Compaction +>+ SuperVersion +-------------+            ^
//    Job       |      2       +------+  |                |current
//              |              +----+ |  |     mem        |    +------------+
//              +--------------+    | |  +--------------------->            |
//                                  | +------------------------> MemTable a |
//                                  |          mem        |    |            |
//              +--------------+    |                     |    +------------+
//              |              +--------------------------+
//  Iter1 +-----> SuperVersion |    |                          +------------+
//              |      1       +------------------------------>+            |
//              |              +-+  |        mem               | MemTable b |
//              +--------------+ |  |                          |            |
//                               |  |    +--------------+      +-----^------+
//                               |  |imm | MemtableList |            |
//                               |  +--->+   Version s  +------------+
//                               |       +--------------+
//                               |       +--------------+
//                               |       | MemtableList |
//                               +------>+   Version t  +-------->  Empty
//                                 imm   +--------------+
//
// In this example, even if the current LSM-tree consists of Version A and
// memtable a, which is also referenced by SuperVersion, two older SuperVersion
// SuperVersion2 and Superversion1 still exist, and are referenced by a
// compaction job and an old iterator Iter1, respectively. SuperVersion2
// contains Version B, memtable a and memtable b; SuperVersion1 contains
// Version B and memtable b (mutable). As a result, Version B and memtable b
// are prevented from being destroyed or deleted.

// ColumnFamilyHandleImpl is the class that clients use to access different
// column families. It has non-trivial destructor, which gets called when client
// is done using the column family
class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
 public:
  // create while holding the mutex
  ColumnFamilyHandleImpl(ColumnFamilyData* cfd, DBImpl* db,
                         InstrumentedMutex* mutex);
  // destroy without mutex
  virtual ~ColumnFamilyHandleImpl();
  virtual ColumnFamilyData* cfd() const { return cfd_; }

  virtual uint32_t GetID() const override;
  virtual const std::string& GetName() const override;
  virtual Status GetDescriptor(ColumnFamilyDescriptor* desc) override;
  virtual const Comparator* GetComparator() const override;

 private:
  ColumnFamilyData* cfd_;
  DBImpl* db_;
  InstrumentedMutex* mutex_;
};

// Does not ref-count ColumnFamilyData
// We use this dummy ColumnFamilyHandleImpl because sometimes MemTableInserter
// calls DBImpl methods. When this happens, MemTableInserter need access to
// ColumnFamilyHandle (same as the client would need). In that case, we feed
// MemTableInserter dummy ColumnFamilyHandle and enable it to call DBImpl
// methods
class ColumnFamilyHandleInternal : public ColumnFamilyHandleImpl {
 public:
  ColumnFamilyHandleInternal()
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
        internal_cfd_(nullptr) {}

  void SetCFD(ColumnFamilyData* _cfd) { internal_cfd_ = _cfd; }
  virtual ColumnFamilyData* cfd() const override { return internal_cfd_; }

 private:
  ColumnFamilyData* internal_cfd_;
};

// holds references to memtable, all immutable memtables and version
struct SuperVersion {
  // Accessing members of this class is not thread-safe and requires external
  // synchronization (ie db mutex held or on write thread).
  ColumnFamilyData* cfd;
  MemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  MutableCFOptions mutable_cf_options;
  // Version number of the current SuperVersion
  uint64_t version_number;
  WriteStallCondition write_stall_condition;

  // should be called outside the mutex
  SuperVersion() = default;
  ~SuperVersion();
  SuperVersion* Ref();
  // If Unref() returns true, Cleanup() should be called with mutex held
  // before deleting this SuperVersion.
  bool Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init(ColumnFamilyData* new_cfd, MemTable* new_mem,
            MemTableListVersion* new_imm, Version* new_current);

  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;

 private:
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  autovector<MemTable*> to_delete;
};

extern Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options);

extern Status CheckConcurrentWritesSupported(
    const ColumnFamilyOptions& cf_options);

extern Status CheckCFPathsSupported(const DBOptions& db_options,
                                    const ColumnFamilyOptions& cf_options);

extern ColumnFamilyOptions SanitizeOptions(const ImmutableDBOptions& db_options,
                                           const ColumnFamilyOptions& src);
// Wrap user defined table properties collector factories `from cf_options`
// into internal ones in int_tbl_prop_collector_factories. Add a system internal
// one too.
extern void GetIntTblPropCollectorFactory(
    const ImmutableCFOptions& ioptions,
    IntTblPropCollectorFactories* int_tbl_prop_collector_factories);

class ColumnFamilySet;

// This class keeps all the data that a column family needs.
// Most methods require DB mutex held, unless otherwise noted
class ColumnFamilyData {
 public:
  ~ColumnFamilyData();

  // thread-safe
  uint32_t GetID() const { return id_; }
  // thread-safe
  const std::string& GetName() const { return name_; }

  // Ref() can only be called from a context where the caller can guarantee
  // that ColumnFamilyData is alive (while holding a non-zero ref already,
  // holding a DB mutex, or as the leader in a write batch group).
  void Ref() { refs_.fetch_add(1); }

  // UnrefAndTryDelete() decreases the reference count and do free if needed,
  // return true if this is freed else false, UnrefAndTryDelete() can only
  // be called while holding a DB mutex, or during single-threaded recovery.
  bool UnrefAndTryDelete();

  // SetDropped() can only be called under following conditions:
  // 1) Holding a DB mutex,
  // 2) from single-threaded write thread, AND
  // 3) from single-threaded VersionSet::LogAndApply()
  // After dropping column family no other operation on that column family
  // will be executed. All the files and memory will be, however, kept around
  // until client drops the column family handle. That way, client can still
  // access data from dropped column family.
  // Column family can be dropped and still alive. In that state:
  // *) Compaction and flush is not executed on the dropped column family.
  // *) Client can continue reading from column family. Writes will fail unless
  // WriteOptions::ignore_missing_column_families is true
  // When the dropped column family is unreferenced, then we:
  // *) Remove column family from the linked list maintained by ColumnFamilySet
  // *) delete all memory associated with that column family
  // *) delete all the files associated with that column family
  void SetDropped();
  bool IsDropped() const { return dropped_.load(std::memory_order_relaxed); }

  // thread-safe
  int NumberLevels() const { return ioptions_.num_levels; }

  void SetLogNumber(uint64_t log_number) { log_number_ = log_number; }
  uint64_t GetLogNumber() const { return log_number_; }

  // thread-safe
  const FileOptions* soptions() const;
  const ImmutableOptions* ioptions() const { return &ioptions_; }
  // REQUIRES: DB mutex held
  // This returns the MutableCFOptions used by current SuperVersion
  // You should use this API to reference MutableCFOptions most of the time.
  const MutableCFOptions* GetCurrentMutableCFOptions() const {
    return &(super_version_->mutable_cf_options);
  }
  // REQUIRES: DB mutex held
  // This returns the latest MutableCFOptions, which may be not in effect yet.
  const MutableCFOptions* GetLatestMutableCFOptions() const {
    return &mutable_cf_options_;
  }

  // REQUIRES: DB mutex held
  // Build ColumnFamiliesOptions with immutable options and latest mutable
  // options.
  ColumnFamilyOptions GetLatestCFOptions() const;

  bool is_delete_range_supported() { return is_delete_range_supported_; }

  // Validate CF options against DB options
  static Status ValidateOptions(const DBOptions& db_options,
                                const ColumnFamilyOptions& cf_options);
  // REQUIRES: DB mutex held
  Status SetOptions(
      const DBOptions& db_options,
      const std::unordered_map<std::string, std::string>& options_map);

  InternalStats* internal_stats() { return internal_stats_.get(); }

  MemTableList* imm() { return &imm_; }
  MemTable* mem() { return mem_; }

  bool IsEmpty() {
    return mem()->GetFirstSequenceNumber() == 0 && imm()->NumNotFlushed() == 0;
  }

  Version* current() { return current_; }
  Version* dummy_versions() { return dummy_versions_; }
  void SetCurrent(Version* _current);
  uint64_t GetNumLiveVersions() const;    // REQUIRE: DB mutex held
  uint64_t GetTotalSstFilesSize() const;  // REQUIRE: DB mutex held
  uint64_t GetLiveSstFilesSize() const;   // REQUIRE: DB mutex held
  uint64_t GetTotalBlobFileSize() const;  // REQUIRE: DB mutex held
  void SetMemtable(MemTable* new_mem) {
    uint64_t memtable_id = last_memtable_id_.fetch_add(1) + 1;
    new_mem->SetID(memtable_id);
    mem_ = new_mem;
  }

  // calculate the oldest log needed for the durability of this column family
  uint64_t OldestLogToKeep();

  // See Memtable constructor for explanation of earliest_seq param.
  MemTable* ConstructNewMemtable(const MutableCFOptions& mutable_cf_options,
                                 SequenceNumber earliest_seq);
  void CreateNewMemtable(const MutableCFOptions& mutable_cf_options,
                         SequenceNumber earliest_seq);

  TableCache* table_cache() const { return table_cache_.get(); }
  BlobSource* blob_source() const { return blob_source_.get(); }

  // See documentation in compaction_picker.h
  // REQUIRES: DB mutex held
  bool NeedsCompaction() const;
  // REQUIRES: DB mutex held
  Compaction* PickCompaction(const MutableCFOptions& mutable_options,
                             const MutableDBOptions& mutable_db_options,
                             LogBuffer* log_buffer);

  // Check if the passed range overlap with any running compactions.
  // REQUIRES: DB mutex held
  bool RangeOverlapWithCompaction(const Slice& smallest_user_key,
                                  const Slice& largest_user_key,
                                  int level) const;

  // Check if the passed ranges overlap with any unflushed memtables
  // (immutable or mutable).
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status RangesOverlapWithMemtables(const autovector<Range>& ranges,
                                    SuperVersion* super_version,
                                    bool allow_data_in_errors, bool* overlap);

  // A flag to tell a manual compaction is to compact all levels together
  // instead of a specific level.
  static const int kCompactAllLevels;
  // A flag to tell a manual compaction's output is base level.
  static const int kCompactToBaseLevel;
  // REQUIRES: DB mutex held
  Compaction* CompactRange(const MutableCFOptions& mutable_cf_options,
                           const MutableDBOptions& mutable_db_options,
                           int input_level, int output_level,
                           const CompactRangeOptions& compact_range_options,
                           const InternalKey* begin, const InternalKey* end,
                           InternalKey** compaction_end, bool* manual_conflict,
                           uint64_t max_file_num_to_ignore,
                           const std::string& trim_ts);

  CompactionPicker* compaction_picker() { return compaction_picker_.get(); }
  // thread-safe
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  // thread-safe
  const InternalKeyComparator& internal_comparator() const {
    return internal_comparator_;
  }

  const IntTblPropCollectorFactories* int_tbl_prop_collector_factories() const {
    return &int_tbl_prop_collector_factories_;
  }

  SuperVersion* GetSuperVersion() { return super_version_; }
  // thread-safe
  // Return a already referenced SuperVersion to be used safely.
  SuperVersion* GetReferencedSuperVersion(DBImpl* db);
  // thread-safe
  // Get SuperVersion stored in thread local storage. If it does not exist,
  // get a reference from a current SuperVersion.
  SuperVersion* GetThreadLocalSuperVersion(DBImpl* db);
  // Try to return SuperVersion back to thread local storage. Return true on
  // success and false on failure. It fails when the thread local storage
  // contains anything other than SuperVersion::kSVInUse flag.
  bool ReturnThreadLocalSuperVersion(SuperVersion* sv);
  // thread-safe
  uint64_t GetSuperVersionNumber() const {
    return super_version_number_.load();
  }
  // will return a pointer to SuperVersion* if previous SuperVersion
  // if its reference count is zero and needs deletion or nullptr if not
  // As argument takes a pointer to allocated SuperVersion to enable
  // the clients to allocate SuperVersion outside of mutex.
  // IMPORTANT: Only call this from DBImpl::InstallSuperVersion()
  void InstallSuperVersion(SuperVersionContext* sv_context,
                           const MutableCFOptions& mutable_cf_options);
  void InstallSuperVersion(SuperVersionContext* sv_context,
                           InstrumentedMutex* db_mutex);

  void ResetThreadLocalSuperVersions();

  // Protected by DB mutex
  void set_queued_for_flush(bool value) { queued_for_flush_ = value; }
  void set_queued_for_compaction(bool value) { queued_for_compaction_ = value; }
  bool queued_for_flush() { return queued_for_flush_; }
  bool queued_for_compaction() { return queued_for_compaction_; }

  static std::pair<WriteStallCondition, WriteStallCause>
  GetWriteStallConditionAndCause(
      int num_unflushed_memtables, int num_l0_files,
      uint64_t num_compaction_needed_bytes,
      const MutableCFOptions& mutable_cf_options,
      const ImmutableCFOptions& immutable_cf_options);

  // Recalculate some stall conditions, which are changed only during
  // compaction, adding new memtable and/or recalculation of compaction score.
  WriteStallCondition RecalculateWriteStallConditions(
      const MutableCFOptions& mutable_cf_options);

  void set_initialized() { initialized_.store(true); }

  bool initialized() const { return initialized_.load(); }

  const ColumnFamilyOptions& initial_cf_options() {
    return initial_cf_options_;
  }

  Env::WriteLifeTimeHint CalculateSSTWriteHint(int level);

  // created_dirs remembers directory created, so that we don't need to call
  // the same data creation operation again.
  Status AddDirectories(
      std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs);

  FSDirectory* GetDataDir(size_t path_id) const;

  // full_history_ts_low_ can only increase.
  void SetFullHistoryTsLow(std::string ts_low) {
    assert(!ts_low.empty());
    const Comparator* ucmp = user_comparator();
    assert(ucmp);
    if (full_history_ts_low_.empty() ||
        ucmp->CompareTimestamp(ts_low, full_history_ts_low_) > 0) {
      full_history_ts_low_ = std::move(ts_low);
    }
  }

  const std::string& GetFullHistoryTsLow() const {
    return full_history_ts_low_;
  }

  ThreadLocalPtr* TEST_GetLocalSV() { return local_sv_.get(); }
  WriteBufferManager* write_buffer_mgr() { return write_buffer_manager_; }
  std::shared_ptr<CacheReservationManager>
  GetFileMetadataCacheReservationManager() {
    return file_metadata_cache_res_mgr_;
  }

  SequenceNumber GetFirstMemtableSequenceNumber() const;

  static const uint32_t kDummyColumnFamilyDataId;

  // Keep track of whether the mempurge feature was ever used.
  void SetMempurgeUsed() { mempurge_used_ = true; }
  bool GetMempurgeUsed() { return mempurge_used_; }

  // Allocate and return a new epoch number
  uint64_t NewEpochNumber() { return next_epoch_number_.fetch_add(1); }

  // Get the next epoch number to be assigned
  uint64_t GetNextEpochNumber() const { return next_epoch_number_.load(); }

  // Set the next epoch number to be assigned
  void SetNextEpochNumber(uint64_t next_epoch_number) {
    next_epoch_number_.store(next_epoch_number);
  }

  // Reset the next epoch number to be assigned
  void ResetNextEpochNumber() { next_epoch_number_.store(1); }

  // Recover the next epoch number of this CF and epoch number
  // of its files (if missing)
  void RecoverEpochNumbers();

 private:
  friend class ColumnFamilySet;
  ColumnFamilyData(uint32_t id, const std::string& name,
                   Version* dummy_versions, Cache* table_cache,
                   WriteBufferManager* write_buffer_manager,
                   const ColumnFamilyOptions& options,
                   const ImmutableDBOptions& db_options,
                   const FileOptions* file_options,
                   ColumnFamilySet* column_family_set,
                   BlockCacheTracer* const block_cache_tracer,
                   const std::shared_ptr<IOTracer>& io_tracer,
                   const std::string& db_id, const std::string& db_session_id);

  std::vector<std::string> GetDbPaths() const;

  /**
   * id_ - 列族 ID
   *
   * 功能概述:
   *   - 列族的唯一标识符（uint32_t）
   *   - 在数据库中全局唯一
   *   - 用于快速查找和标识列族
   *
   * ID 规则:
   *   - 默认列族的 ID 固定为 0
   *   - 其他列族的 ID 单调递增
   *   - ID 不会重用（即使列族被删除）
   *
   * 使用场景:
   *   1. 列族查找:
   *   - 在 WriteBatch 中通过 ID 找到目标列族
   *   - 从 ColumnFamilySet 中按 ID 查找列族
   *
   *   2. 日志和元数据:
   *   - 记录在 MANIFEST 中
   *   - 标识 SSTable 文件归属的列族
   *   - 记录在 WAL 日志中
   *
   *   3. 内部标识:
   *   - 作为哈希映射的键（column_family_data_）
   *   - 在时间戳大小映射中使用
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - ID 在列族生命周期内不变
   *   - 删除列族不会回收 ID
   *   - ID 分配由 ColumnFamilySet 管理
   */
  uint32_t id_;

  /**
   * name_ - 列族名称
   *
   * 功能概述:
   *   - 列族的名称（字符串）
   *   - 用户定义，用于标识列族
   *   - 在数据库中必须唯一
   *
   * 使用场景:
   *   1. 用户 API:
   *   - 用户通过名称创建、打开、删除列族
   *   - ColumnFamilyHandle::GetName() 返回此名称
   *
   *   2. 元数据:
   *   - 记录在 MANIFEST 中
   *   - 用于日志和错误信息
   *
   *   3. 名称映射:
   *   - 作为 column_families_ 映射的键
   *   - 按名称查找列族
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 默认列族的名称通常为 "default"
   *   - 名称在数据库生命周期内不变
   *   - 大小写敏感
   */
  const std::string name_;

  /**
   * dummy_versions_ - 版本循环链表的哨兵节点
   *
   * 功能概述:
   *   - Version 指针，指向循环双向链表的哨兵节点
   *   - 链表连接所有版本，每个版本代表一个快照
   *   - 哨兵节点不包含真实数据，仅用于链表结构
   *
   * 链表结构:
   *   - 循环双向链表
   *   - dummy_versions_ 是链表头部
   *   - current_ 是当前版本（== dummy_versions_->prev_）
   *   - 链表按时间顺序排列：旧版本 → ... → current_ → dummy_versions_
   *
   * 版本生命周期:
   *   1. 创建:
   *   - LogAndApply() 创建新版本
   *   - 插入到 dummy_versions_ 之前（成为新的 prev_）
   *
   *   2. 使用:
   *   - current_ 被查询、Flush、Compaction 使用
   *   - 旧版本可能被迭代器、后台任务引用
   *
   *   3. 删除:
   *   - 引用计数为 0 时删除
   *   - 从链表中移除
   *
   * 使用场景:
   *   1. 版本管理:
   *   - 跟踪 LSM-tree 的变化
   *   - 支持多版本并发控制
   *
   *   2. 快照:
   *   - 每个版本代表一个点时间的快照
   *   - 支持一致读
   *
   *   3. 迭代器:
   *   - 迭代器持有旧版本的引用
   *   - 保证迭代期间数据不变
   *
   * 4. Compaction 和 Flush:
   *   - 需要参考当前版本
   *   - 完成后创建新版本
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下访问
   *   - 版本引用计数管理并发访问
   *
   * 注意事项:
   *   - dummy_versions_ 始终存在，不会被删除
   *   - 版本链表可能包含多个版本（旧版本未释放）
   *   - 旧版本的引用计数大于 0 时不能删除
   */
  Version* dummy_versions_;  // Head of circular doubly-linked list of versions.

  /**
   * current_ - 当前活跃的版本
   *
   * 功能概述:
   *   - Version 指针，指向当前正在使用的版本
   *   - 始终等于 dummy_versions_->prev_
   *   - 包含最新的 LSM-tree 状态
   *
   * 包含的信息:
   *   - 各层的 SSTable 文件列表
   *   - 文件的大小、序列号范围、时间戳范围
   *   - 文件的重叠关系
   *   - Compaction 指针（用于下一轮 Compaction）
   *   - 文件统计信息
   *
   * 使用场景:
   *   1. 数据查询:
   *   - 查找 key 时遍历版本中的各层文件
   *   - 使用 memtable、immutable memtable 和 SSTable 文件
   *
   *   2. Flush:
   *   - 将 MemTable Flush 到 SSTable 后创建新版本
   *   - 更新文件列表
   *
   *   3. Compaction:
   *   - 根据当前版本选择要合并的文件
   *   - Compaction 完成后创建新版本
   *
   *   4. 统计和监控:
   *   - 获取文件数量、大小等信息
   *   - 计算压缩分数
   *
   * 版本切换:
   *   - InstallSuperVersion() 时可能切换版本
   *   - LogAndApply() 创建新版本
   *   - 原版本保留直到引用计数为 0
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 多线程可以并发读取（通过 SuperVersion）
   *   - 版本引用计数管理并发访问
   *
   * 注意事项:
   *   - current_ 始终指向最新的版本
   *   - 旧版本可能仍然存在（被迭代器、后台任务引用）
   *   - 修改版本需要创建新的 Version 对象
   *   - 不能直接修改 current_ 的内容
   */
  Version* current_;         // == dummy_versions->prev_

  /**
   * refs_ - 引用计数
   *
   * 功能概述:
   *   - 原子整数，记录对 ColumnFamilyData 的活跃引用数
   *   - 用于管理对象生命周期
   *   - 引用计数为 0 时可以删除对象
   *
   * 引用来源:
   *   1. ColumnFamilyHandle:
   *   - 每个客户端的句柄持有一个引用
   *   - 句柄销毁时释放引用
   *
   *   2. 内部任务:
   *   - Flush 任务持有引用
   *   - Compaction 任务持有引用
   *   - 后台清理任务持有引用
   *
   *   3. 迭代器:
   *   - 读迭代器持有引用
   *   - 防止列族被删除
   *
   *   4. SuperVersion:
   *   - SuperVersion 包含对 ColumnFamilyData 的引用
   *   - 但 SuperVersion 的引用计数独立管理
   *
   * 使用场景:
   *   1. 增加引用:
   *   - 创建 ColumnFamilyHandle 时
   *   - 启动后台任务时
   *   - 创建迭代器时
   *
   *   2. 减少引用:
   *   - 销毁 ColumnFamilyHandle 时
   *   - 完成后台任务时
   *   - 销毁迭代器时
   *
   *   3. 删除检查:
   *   - UnrefAndTryDelete() 减少引用计数
   *   - 引用计数为 0 且已标记为 dropped 时删除
   *
   * 线程安全性:
   *   - 原子操作（fetch_add, fetch_sub）
   *   - 多线程安全地增加和减少引用
   *
   * 删除条件:
   *   - refs_ == 0
   *   - dropped_ == true
   *   - 同时满足两个条件才能删除
   *
   * 注意事项:
   *   - 初始引用计数由创建者决定（通常是 1）
   *   - 不能引用计数为 0 的对象（除非重新初始化）
   *   - 必须正确平衡 Ref() 和 UnrefAndTryDelete() 调用
   *   - 泄漏引用会导致内存泄漏
   *   - 过早释放引用会导致 use-after-free
   */
  std::atomic<int> refs_;  // outstanding references to ColumnFamilyData

  /**
   * initialized_ - 初始化标志
   *
   * 功能概述:
   *   - 原子布尔值，标记列族是否已初始化
   *   - 初始化后，列族可以正常使用
   *
   * 初始化时机:
   *   - 创建列族后立即初始化
   *   - 恢复数据库时，恢复所有列族后初始化
   *   - 通过 set_initialized() 设置
   *
   * 初始化内容:
   *   - 完成列族元数据设置
   *   - 完成版本管理初始化
   *   - 完成资源分配（MemTable、缓存等）
   *
   * 使用场景:
   *   1. 检查列族状态:
   *   - initialized() 方法返回此标志
   *   - 确保列族已就绪
   *
   *   2. 条件操作:
   *   - 某些操作需要列族已初始化
   *   - 在初始化前跳过某些操作
   *
   *   3. 错误处理:
   *   - 检测未初始化的列族
   *   - 返回适当的错误信息
   *
   * 线程安全性:
   *   - 原子操作
   *   - 多线程安全读取
   *
   * 状态转换:
   *   - false → true：初始化完成后设置
   *   - 不会从 true 变回 false
   *
   * 注意事项:
   *   - 初始值为 false
   *   - 一旦设置为 true，不会再改变
   *   - 必须在所有初始化工作完成后设置为 true
   *   - 使用时应该检查此标志
   */
  std::atomic<bool> initialized_;

  /**
   * dropped_ - 删除标志
   *
   * 功能概述:
   *   - 原子布尔值，标记列族是否已被用户删除
   *   - 用户调用 DropColumnFamily() 后设置为 true
   *   - 删除后，不再接受新的写入操作
   *
   * 删除流程:
   *   1. 用户调用 DropColumnFamily()
   *   2. SetDropped() 设置 dropped_ = true
   *   3. 从 ColumnFamilySet 的链表中移除
   *   4. 等待引用计数为 0
   *   5. 引用计数为 0 时删除所有资源
   *
   * 删除后的行为:
   *   - 不再接受新的写入操作（除非 ignore_missing_column_families）
   *   - 客户端仍然可以读取数据（持有句柄的情况下）
   *   - Compaction 和 Flush 不再执行
   *   - 文件和内存保留直到引用计数为 0
   *
   * 使用场景:
   *   1. 检查列族状态:
   *   - IsDropped() 方法返回此标志
   *   - 判断列族是否被删除
   *
   *   2. 写入验证:
   *   - 写入前检查列族是否被删除
   *   - 返回适当的错误信息
   *
   *   3. 后台任务:
   *   - 跳过已删除列族的后台任务
   *   - 不再调度 Flush 和 Compaction
   *
   * 线程安全性:
   *   - 原子操作
   *   - 多线程安全读取
   *   - 修改需要在 DB 互斥锁保护下
   *
   * 状态转换:
   *   - false → true：用户删除列族后设置
   *   - 不会从 true 变回 false
   *
   * 与 refs_ 的关系:
   *   - dropped_ = true：列族已标记删除
   *   - refs_ > 0：仍有持有引用的对象
   *   - dropped_ = true && refs_ = 0：可以删除对象
   *
   * 注意事项:
   *   - 初始值为 false
   *   - 一旦设置为 true，不会再改变
   *   - 即使被删除，客户端仍可读取（如果有句柄）
   *   - 删除操作不可逆
   *   - 文件和数据的删除在引用计数为 0 后进行
   */
  std::atomic<bool> dropped_;  // true if client dropped it

  /**
   * internal_comparator_ - 内部键比较器
   *
   * 功能概述:
   *   - InternalKeyComparator 对象，用于比较内部键
   *   - 内部键包含用户键和序列号
   *   - 支持按用户键和序列号进行比较
   *
   * 内部键格式:
   *   - 用户 key
   *   - 序列号（8 字节，大端序）
   *   - 值类型（1 字节）
   *
   * 比较规则:
   *   - 首先比较用户键（使用 user_comparator）
   *   - 用户键相同则比较序列号（大的在前）
   *   - 序列号相同则比较值类型
   *
   * 使用场景:
   *   1. MemTable 操作:
   *   - 插入时查找位置
   *   - 查询时比较键
   *   - 范围查询时确定边界
   *
   *   2. SSTable 操作:
   *   - 文件内的二分查找
   *   - 确定文件重叠
   *   - 索引查找
   *
   *   3. Compaction:
   *   - 合并文件时比较键
   *   - 去重判断
   *   - 范围压缩
   *
   *   4. 迭代器:
   *   - 确定迭代顺序
   *   - Seek 操作
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全使用
   *
   * 注意事项:
   *   - 包含用户定义的比较器（user_comparator）
   *   - 用户比较器决定了键的排序规则
   *   - 内部比较器封装了序列号处理
   *   - 比较器决定了数据的物理存储顺序
   */
  const InternalKeyComparator internal_comparator_;

  /**
   * int_tbl_prop_collector_factories_ - 表属性收集器工厂集合
   *
   * 功能概述:
   *   - 内部表属性收集器工厂的集合
   *   - 在 SSTable 文件创建时收集文件级别的统计信息
   *   - 用于性能分析和优化
   *
   * 收集的属性:
   *   1. 统计信息:
   *   - 文件中的条目数量
   *   - 各层的数据分布
   *   - 压缩比例
   *
   *   2. 键值分析:
   *   - 键的大小分布
   *   - 值的大小分布
   *   - 前缀信息
   *
   *   3. 其他元数据:
   *   - 时间戳信息
   *   - TTL 信息
   *   - 自定义属性
   *
   * 使用场景:
   *   1. 文件创建:
   *   - Flush SSTable 时创建收集器
   *   - Compaction 创建新文件时创建收集器
   *   - 边写入边收集属性
   *
   *   2. 文件读取:
   *   - 从文件元数据读取属性
   *   - 用于查询优化
   *
   *   3. 统计和监控:
   *   - 分析文件使用情况
   *   - 优化压缩策略
   *   - 性能诊断
   *
   * 配置:
   *   - 由 cf_options.table_properties_collector_factories 配置
   *   - RocksDB 会添加系统内部的收集器
   *   - 用户可以添加自定义收集器
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 收集器本身可能有状态（每个文件独立）
   *
   * 注意事项:
   *   - 包含用户定义和系统定义的收集器
   *   - 每个文件创建独立的收集器实例
   *   - 收集器会增加少量开销
   *   - 收集的属性存储在文件元数据中
   */
  IntTblPropCollectorFactories int_tbl_prop_collector_factories_;

  /**
   * initial_cf_options_ - 初始列族选项
   *
   * 功能概述:
   *   - 列族创建时的选项配置
   *   - 不可变，记录列族的初始配置
   *   - 用于验证和恢复
   *
   * 包含的选项:
   *   - 比较器（comparator）
   *   - 合并操作符（merge_operator）
   *   - 压缩选项（compression）
   *   - 前缀提取器（prefix_extractor）
   *   - 布隆过滤器配置
   *   - MemTable 配置
   *   - Compaction 配置
   *   - 其他列族级别选项
   *
   * 使用场景:
   *   1. 创建列族:
   *   - 使用此选项初始化列族
   *   - 设置不可变选项
   *
   *   2. 验证选项:
   *   - 修改选项时验证合法性
   *   - GetLatestCFOptions() 返回此选项 + 最新可变选项
   *
   *   3. 恢复:
   *   - 从 MANIFEST 恢复选项
   *   - 确保选项一致性
   *
   *   4. 日志和诊断:
   *   - 记录初始配置
   *   - 比较当前配置与初始配置
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 某些选项在列族生命周期内不可修改
   *   - 可修改的选项通过 mutable_cf_options_ 管理
   *   - 初始选项永久记录（用于恢复）
   *   - 修改选项需要调用 SetOptions()
   */
  const ColumnFamilyOptions initial_cf_options_;

  /**
   * ioptions_ - 不可变列族选项
   *
   * 功能概述:
   *   - ImmutableOptions 对象，包含列族的不可变选项
   *   - 在列族生命周期内不变的配置
   *   - 合并了数据库和列族的不可变选项
   *
   * 包含的选项:
   *   - 比较器（comparator）
   *   - 合并操作符（merge_operator）
   *   - 前缀提取器（prefix_extractor）
   *   - 环境配置（env）
   *   - 压缩选项
   *   - 文件系统配置
   *   - 缓存配置
   *   - 列族级别其他选项
   *
   * 使用场景:
   *   1. 初始化资源:
   *   - 创建 MemTable
   *   - 创建 CompactionPicker
   *   - 初始化缓存
   *
   *   2. 查询配置:
   *   - 获取压缩选项
   *   - 获取文件系统配置
   *   - 获取其他不可变配置
   *
   *   3. 传递给子模块:
   *   - 传递给 Version
   *   - 传递给 TableCache
   *   - 传递给其他组件
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 合并了数据库和列族的不可变选项
   *   - 修改不可变选项需要重新创建列族
   *   - 可变选项通过 mutable_cf_options_ 管理
   *   - 许多组件持有此选项的引用
   */
  const ImmutableOptions ioptions_;

  /**
   * mutable_cf_options_ - 可变列族选项
   *
   * 功能概述:
   *   - MutableCFOptions 对象，包含列族的可变选项
   *   - 可以在列族生命周期内修改的配置
   *   - 存储在 SuperVersion 中，支持动态更新
   *
   * 包含的选项:
   *   - write_buffer_size（MemTable 大小）
   *   - max_write_buffer_number（最大 MemTable 数量）
   *   - min_write_buffer_number_to_merge
   *   - max_bytes_for_level_multiplier（每层大小倍数）
   *   - target_file_size_base（目标文件大小）
   *   - max_bytes_for_level_base（L1 大小）
   *   - compaction_style（压缩风格）
   *   - 其他可修改的列族选项
   *
   * 使用场景:
   *   1. 动态配置:
   *   - SetOptions() 修改选项
   *   - InstallSuperVersion() 更新选项
   *
   *   2. Flush:
   *   - 使用可变选项确定 Flush 触发条件
   *   - 确定目标文件大小
   *
   *   3. Compaction:
   *   - 使用可变选项选择要压缩的文件
   *   - 确定输出文件大小
   *   - 确定压缩策略
   *
   *   4. MemTable 管理:
   *   - 控制 MemTable 大小和数量
   *   - 控制 MemTable 切换时机
   *
   * 修改流程:
   *   1. 用户调用 SetOptions()
   *   2. 验证新选项
   *   3. 更新 mutable_cf_options_
   *   4. 调用 InstallSuperVersion() 更新 SuperVersion
   *
   * 线程安全性:
   *   - 修改需要在 DB 互斥锁保护下
   *   - 读取需要通过 SuperVersion（或持锁）
   *   - SuperVersion 包含快照的 mutable_cf_options_
   *
   * 注意事项:
   *   - 修改选项需要调用 SetOptions()
   *   - 选项修改后需要重新计算 Compaction 分数
   *   - SuperVersion 中的选项是快照
   *   - 修改选项可能影响性能
   */
  MutableCFOptions mutable_cf_options_;

  /**
   * is_delete_range_supported_ - 是否支持 DeleteRange 操作
   *
   * 功能概述:
   *   - 布尔值，标记列族是否支持 DeleteRange 操作
   *   - DeleteRange 可以删除一个键范围内的所有数据
   *   - 取决于比较器的实现
   *
   * DeleteRange 特性:
   *   - 删除 [start, end) 范围内的所有键
   *   - 比逐个删除更高效
   *   - 需要 Comparator 支持范围比较
   *   - 通常需要特定的比较器实现
   *
   * 使用场景:
   *   1. 批量删除:
   *   - 删除一个时间范围内的数据
   *   - 删除一个前缀下的所有数据
   *
   *   2. 数据清理:
   *   - TTL 删除过期数据
   *   - 清理特定业务的数据
   *
   *   3. 功能检查:
   *   - API 层检查是否支持 DeleteRange
   *   - 返回适当的错误信息
   *
   * 判断条件:
   *   - 比较器支持范围比较
   *   - 列族选项允许 DeleteRange
   *   - 数据库配置允许 DeleteRange
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 并非所有比较器都支持 DeleteRange
   *   - 默认比较器支持 DeleteRange
   *   - 自定义比较器需要实现相关接口
   *   - 使用前应该检查此标志
   */
  const bool is_delete_range_supported_;

  /**
   * table_cache_ - SSTable 缓存
   *
   * 功能概述:
   *   - 智能指针，指向 SSTable 缓存（TableCache）
   *   - 列族专用的 SSTable 缓存（不同于共享的 block_cache）
   *   - 缓存已打开的 SSTable 文件读取器
   *
   * 缓存内容:
   *   - SSTable 文件读取器（TableReader）
   *   - 文件描述符
   *   - 索引和过滤器
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 查询 SSTable 文件时先检查缓存
   *   - 命中则直接使用缓存的数据
   *   - 未命中则打开文件并加入缓存
   *
   *   2. Compaction:
   *   - 读取 SSTable 文件时使用缓存
   *   - 避免重复打开文件
   *
   *   3. 范围查询:
   *   - Seek 操作时可能需要打开多个文件
   *   - 缓存提高性能
   *
   * 缓存策略:
   *   - LRU 淘汰策略
   *   - 限制缓存大小
   *   - 引用计数管理
   *
   * 生命周期:
   *   - 列族创建时创建
   *   - 列族销毁时销毁
   *
   * 线程安全性:
   *   - TableCache 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 独立于 block_cache
   *   - block_cache 缓存块数据
   *   - table_cache_ 缓存文件读取器
   *   - 两级缓存提高性能
   */
  std::unique_ptr<TableCache> table_cache_;

  /**
   * blob_file_cache_ - Blob 文件缓存
   *
   * 功能概述:
   *   - 智能指针，指向 Blob 文件缓存（BlobFileCache）
   *   - 用于 RocksDB 的 BlobDB 功能
   *   - 缓存 Blob 文件（存储大值）
   *
   * BlobDB 特性:
   *   - 将大值存储在独立的 Blob 文件中
   *   - SSTable 中存储 Blob 文件的引用
   *   - 支持大值的高效存储和访问
   *   - 减小 SSTable 文件大小
   *
   * 使用场景:
   *   1. 读取大值:
   *   - 从 Blob 文件中读取大值
   *   - 使用缓存提高性能
   *
   *   2. 写入大值:
   *   - 将大值写入 Blob 文件
   *   - 在 SSTable 中存储引用
   *
   *   3. Compaction:
   *   - 处理 Blob 文件的引用
   *   - 清理无用的 Blob 文件
   *
   * 配置:
   *   - 由 cf_options.enable_blob_files 控制
   *   - 只有启用 BlobDB 功能时才创建
   *
   * 生命周期:
   *   - 列族创建时创建（如果启用）
   *   - 列族销毁时销毁
   *
   * 线程安全性:
   *   - BlobFileCache 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 可能为 nullptr（未启用 BlobDB）
   *   - 只有存储大值时才使用
   *   - 需要额外的文件存储空间
   */
  std::unique_ptr<BlobFileCache> blob_file_cache_;

  /**
   * blob_source_ - Blob 数据源
   *
   * 功能概述:
   *   - 智能指针，指向 Blob 数据源（BlobSource）
   *   - 用于读取 Blob 文件中的数据
   *   - 提供统一的 Blob 数据访问接口
   *
   * 功能:
   *   - 从 Blob 文件中读取数据
   *   - 管理 Blob 文件的缓存
   *   - 处理 Blob 文件的打开和关闭
   *
   * 使用场景:
   *   1. 读取操作:
   *   - SSTable 中的 Blob 引用指向 Blob 文件
   *   - 通过 BlobSource 读取实际数据
   *
   *   2. Compaction:
   *   - 合并 Blob 引用
   *   - 清理无用的 Blob 文件
   *
   *   3. GC:
   *   - 垃圾回收无用的 Blob 数据
   *
   * 与 blob_file_cache_ 的关系:
   *   - blob_file_cache_ 管理文件句柄和缓存
   *   - blob_source_ 提供数据读取接口
   *   - 两者协同工作
   *
   * 配置:
   *   - 由 cf_options.enable_blob_files 控制
   *   - 只有启用 BlobDB 功能时才创建
   *
   * 生命周期:
   *   - 列族创建时创建（如果启用）
   *   - 列族销毁时销毁
   *
   * 线程安全性:
   *   - BlobSource 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 可能为 nullptr（未启用 BlobDB）
   *   - 与 blob_file_cache_ 配合使用
   *   - 增加了读取路径的复杂度
   */
  std::unique_ptr<BlobSource> blob_source_;

  /**
   * internal_stats_ - 内部统计信息
   *
   * 功能概述:
   *   - 智能指针，指向内部统计对象（InternalStats）
   *   - 记录列族的各种统计信息
   *   - 用于性能监控和诊断
   *
   * 统计内容:
   *   1. 写入统计:
   *   - 写入字节数
   *   - 写入条目数
   *   - 写入延迟
   *
   *   2. 读取统计:
   *   - 读取字节数
   *   - 读取条目数
   *   - 缓存命中率
   *   - 读取延迟
   *
   *   3. Flush 统计:
   *   - Flush 次数
   *   - Flush 延迟
   *   - Flush 数据量
   *
   *   4. Compaction 统计:
   *   - Compaction 次数
   *   - Compaction 延迟
   *   - Compaction 数据量
   *   - 各层的文件统计
   *
   *   5. MemTable 统计:
   *   - MemTable 大小
   *   - MemTable 条目数
   *   - 不可变 MemTable 数量
   *
   *   6. 错误统计:
   *   - 错误计数
   *   - 失败计数
   *
   * 使用场景:
   *   1. 监控:
   *   - 实时监控列族性能
   *   - 检测性能问题
   *
   *   2. 诊断:
   *   - 分析性能瓶颈
   *   - 定位问题
   *
   *   3. 优化:
   *   - 根据统计信息调整配置
   *   - 优化查询和写入性能
   *
   *   4. 报告:
   *   - 生成性能报告
   *   - 提供给监控工具
   *
   * 配置:
   *   - 由 db_options.stats_level 控制统计级别
   *   - 可以启用或禁用统计
   *
   * 生命周期:
   *   - 列族创建时创建
   *   - 列族销毁时销毁
   *
   * 线程安全性:
   *   - InternalStats 内部有锁或原子变量
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 统计会带来少量性能开销
   *   - 可以通过配置控制统计级别
   *   - 某些统计可能需要持久化
   */
  std::unique_ptr<InternalStats> internal_stats_;

  /**
   * write_buffer_manager_ - 写入缓冲区管理器
   *
   * 功能概述:
   *   - 指向写入缓冲区管理器（WriteBufferManager）
   *   - 管理所有列族的 MemTable 内存使用
   *   - 由 ColumnFamilySet 拥有，列族仅保存引用
   *
   * 管理内容:
   *   - 活跃 MemTable 的内存使用
   *   - 不可变 MemTable 的内存使用
   *   - 所有列族的 MemTable 总内存
   *
   * 使用场景:
   *   1. 创建 MemTable:
   *   - 向 write_buffer_manager_ 注册
   *   - 分配内存
   *
   *   2. 删除 MemTable:
   *   - 从 write_buffer_manager_ 注销
   *   - 释放内存
   *
   *   3. 内存控制:
   *   - 检查内存使用量
   *   - 超限时触发 Flush
   *
   *   4. 写入控制:
   *   - 内存紧张时延迟写入
   *   - 降低写入速率
   *
   * 线程安全性:
   *   - WriteBufferManager 内部有锁和原子变量
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 由 ColumnFamilySet 拥有和管理
   *   - 所有列族共享此管理器
   *   - MemTable 必须正确注册和注销
   */
  WriteBufferManager* write_buffer_manager_;

  /**
   * mem_ - 活跃 MemTable
   *
   * 功能概述:
   *   - MemTable 指针，指向当前活跃的可变 MemTable
   *   - 存储最新的写入操作
   *   - 接受新的写入操作
   *
   * MemTable 特性:
   *   - 可变，支持并发写入
   *   - 基于跳表（SkipList）实现
   *   - 键按比较器排序
   *   - 支持范围查询
   *   - 满时会切换到不可变 MemTable
   *
   * 使用场景:
   *   1. 写入操作:
   *   - 接受 Put、Delete、Merge 操作
   *   - 写入到内存，不立即持久化
   *
   *   2. 读取操作:
   *   - 查询最新写入的数据
   *   - 配合 SSTable 文件提供完整查询
   *
   *   3. Flush:
   *   - 满（或达到条件）时 Flush 到 SSTable
   *   - Flush 时变为不可变 MemTable
   *   - 创建新的活跃 MemTable
   *
   *   4. 范围查询:
   *   - 查询时先查询 mem_
   *   - 再查询 imm_
   *   - 最后查询 SSTable 文件
   *
   * 切换时机:
   *   - MemTable 大小超过 write_buffer_size
   *   - MemTable 条目数超过限制
   *   - 手动 Flush
   *   - WAL 日志切换
   *
   * 生命周期:
   *   - 列族创建时创建
   *   - Flush 时创建新的 mem_
   *   - 旧的 mem_ 转移到 imm_
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - MemTable 内部支持并发写入
   *
   * 注意事项:
   *   - 活跃 MemTable 可能与不可变 MemTable 并存
   *   - Flush 时会阻塞新的写入（短暂）
   *   - MemTable 的大小影响写入性能和内存使用
   */
  MemTable* mem_;

  /**
   * imm_ - 不可变 MemTable 列表
   *
   * 功能概述:
   *   - MemTableList 对象，管理所有不可变 MemTable
   *   - 这些 MemTable 不再接受写入
   *   - 等待被 Flush 到磁盘
   *
   * 不可变 MemTable 特性:
   *   - 不再接受新的写入操作
   *   - 仍然可以读取
   *   - 等待后台 Flush 线程处理
   *   - Flush 完成后删除
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 查询时遍历 imm_ 中的 MemTable
   *   - 查询还未 Flush 的数据
   *   - 与 mem_ 和 SSTable 文件配合
   *
   *   2. Flush:
   *   - 后台线程从 imm_ 中选择 MemTable Flush
   *   - Flush 完成后从 imm_ 中移除
   *
   *   3. 范围查询:
   *   - 范围查询时需要遍历 imm_
   *   - 找出所有相关的数据
   *
   *   4. 内存管理:
   *   - 统计 imm_ 的内存使用
   *   - 控制 imm_ 的数量
   *
   * 列表特性:
   *   - 按创建时间排序（旧的在前）
   *   - 支持添加和删除
   *   - 支持遍历
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下或通过 SuperVersion
   *
   * 注意事项:
   *   - imm_ 的数量影响内存使用
   *   - Flush 速度慢时 imm_ 可能增长
   *   - 写入压力大会导致 imm_ 增长
   *   - imm_ 满时会阻塞新的写入
   */
  MemTableList imm_;

  /**
   * super_version_ - 当前 SuperVersion
   *
   * 功能概述:
   *   - SuperVersion 指针，指向当前的 SuperVersion
   *   - SuperVersion 是 LSM-tree 的快照
   *   - 包含 mem、imm、current 的引用
   *
   * SuperVersion 包含:
   *   - mem: 活跃 MemTable
   *   - imm: MemTableListVersion（不可变 MemTable 列表的版本）
   *   - current: 当前 Version（SSTable 文件列表）
   *   - mutable_cf_options: 可变列族选项
   *   - version_number: SuperVersion 版本号
   *   - write_stall_condition: 写入停止条件
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 持有 SuperVersion 的引用
   *   - 遍历 mem、imm、current 查询数据
   *   - 保证读一致性
   *
   *   2. 迭代器:
   *   - 迭代器持有 SuperVersion 的引用
   *   - 保证迭代期间数据不变
   *
   *   3. 查询优化:
   *   - SuperVersion 缓存 LSM-tree 状态
   *   - 避免多次查询
   *
   *   4. 版本切换:
   *   - InstallSuperVersion() 更新 SuperVersion
   *   - 旧 SuperVersion 保留直到引用计数为 0
   *
   * 更新时机:
   *   - Flush 完成后
   *   - Compaction 完成后
   *   - 修改选项后
   *   - 创建新 MemTable 后
   *
   * 引用计数:
   *   - SuperVersion 有引用计数
   *   - 持有 SuperVersion 时需要增加引用
   *   - 使用完成后释放引用
   *   - 引用计数为 0 时删除
   *
   * 并发控制:
   *   - 修改需要在 DB 互斥锁保护下
   *   - 读取需要增加引用计数
   *   - 支持多线程并发访问
   *
   * 线程本地缓存:
   *   - local_sv_ 支持线程本地 SuperVersion 缓存
   *   - 减少引用计数操作
   *   - 提高性能
   *
   * 注意事项:
   *   - SuperVersion 是 LSM-tree 的快照
   *   - 旧 SuperVersion 可能仍然存在
   *   - SuperVersion 更新会增加 super_version_number_
   *   - 必须正确管理引用计数
   *   - 超级版本变更需要调用 InstallSuperVersion()
   */
  SuperVersion* super_version_;

  /**
   * super_version_number_ - SuperVersion 版本号
   *
   * 功能概述:
   *   - 原子无符号整数，表示当前 SuperVersion 的版本号
   *   - 每次 SuperVersion 更新时递增
   *   - 用于检测 SuperVersion 是否过期
   *
   * 更新时机:
   *   - InstallSuperVersion() 时更新
   *   - 每次 super_version_ 变化
   *   - 单调递增
   *
   * 使用场景:
   *   1. 版本检查:
   *   - 检查持有的 SuperVersion 是否过期
   *   - 与缓存的版本号比较
   *
   *   2. 线程本地缓存失效:
   *   - 检测线程本地 SuperVersion 是否需要更新
   *   - version_number 不匹配时重新获取
   *
   *   3. 调试和诊断:
   *   - 追踪 SuperVersion 的更新次数
   *   - 分析 SuperVersion 的更新频率
   *
   * 版本号规则:
   *   - 从 1 开始（或从 0 开始）
   *   - 每次更新递增 1
   *   - 单调递增，不重复
   *
   * 线程安全性:
   *   - 原子操作
   *   - 多线程安全读取
   *   - 修改需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 原子变量，但修改需要锁保护
   *   - 用于版本比较，不是唯一标识
   *   - 可能溢出（uint64_t 范围很大）
   */
  // An ordinal representing the current SuperVersion. Updated by
  // InstallSuperVersion(), i.e. incremented every time super_version_
  // changes.
  std::atomic<uint64_t> super_version_number_;

  /**
   * local_sv_ - 线程本地 SuperVersion 指针
   *
   * 功能概述:
   *   - 智能指针，指向线程本地存储（ThreadLocalPtr）
   *   - 每个线程可以缓存一个 SuperVersion
   *   - 避免频繁引用计数操作
   *
   * 性能优化:
   *   - 避免每次查询都获取 SuperVersion 引用
   *   - 线程本地缓存减少锁竞争
   *   - 提高查询性能
   *
   * 使用场景:
   *   1. 查询操作:
   *   - 使用 GetThreadLocalSuperVersion() 获取
   *   - 命中则直接使用
   *   - 未命中则获取新的 SuperVersion
   *
   *   2. 版本检查:
   *   - 检查缓存的 SuperVersion 是否过期
   *   - 使用 super_version_number_ 判断
   *
   *   3. 释放缓存:
   *   - 使用 ReturnThreadLocalSuperVersion() 释放
   *   - 线程退出时自动清理
   *
   * 特殊值:
   *   - kSVInUse: 标记 SuperVersion 正在使用
   *   - kSVObsolete: 标记 SuperVersion 已过期
   *
   * 更新策略:
   *   - SuperVersion 更新时，线程本地缓存可能过期
   *   - 下次查询时检查版本号
   *   - 版本不匹配时重新获取
   *
   * 生命周期:
   *   - 列族创建时创建
   *   - 列族销毁时销毁
   *   - 必须在 mutex_ 之前销毁
   *
   * 线程安全性:
   *   - 每个线程独立存储
   *   - 不需要锁
   *   - ThreadLocalPtr 内部处理线程安全
   *
   * 注意事项:
   *   - 需要检查版本号判断是否过期
   *   - 线程本地缓存可能滞后于全局
   *   - 退出线程时自动清理
   *   - 减少引用计数操作，提高性能
   */
  // Thread's local copy of SuperVersion pointer
  // This needs to be destructed before mutex_
  std::unique_ptr<ThreadLocalPtr> local_sv_;

  /**
   * next_ - 下一个列族指针（双向链表）
   *
   * 功能概述:
   *   - ColumnFamilyData 指针，指向下一个列族
   *   - 与 prev_ 配合形成双向链表
   *   - 用于遍历所有列族
   *
   * 链表结构:
   *   - 哨兵节点（ColumnFamilySet::dummy_cfd_）
   *   - 所有活跃列族形成循环链表
   *   - 哨兵节点的 next_ 指向第一个列族
   *   - 最后一个列族的 next_ 指向哨兵节点
   *
   * 使用场景:
   *   1. 遍历列族:
   *   - 从 dummy_cfd_->next_ 开始
   *   - 通过 next_ 依次访问所有列族
   *   - 到 dummy_cfd_ 时结束
   *
   *   2. 添加列族:
   *   - 插入到 dummy_cfd_ 之后
   *   - 更新 next_ 和 prev_ 指针
   *
   *   3. 删除列族:
   *   - 从链表中移除
   *   - 更新前后节点的 next_ 和 prev_
   *
   *   4. 后台任务:
   *   - 遍历所有列族执行 Flush
   *   - 遍历所有列族执行 Compaction
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 遍历需要在互斥锁保护下（或使用 RefedColumnFamilySet）
   *
   * 注意事项:
   *   - 链表包含所有活跃列族（包括 dropped 列族）
   *   - dropped 列族在引用计数为 0 后从链表移除
   *   - 哨兵节点的 ID 为 kDummyColumnFamilyDataId
   *   - 遍历时需要跳过哨兵节点
   */
  ColumnFamilyData* next_;

  /**
   * prev_ - 上一个列族指针（双向链表）
   *
   * 功能概述:
   *   - ColumnFamilyData 指针，指向上一个列族
   *   - 与 next_ 配合形成双向链表
   *   - 支持双向遍历
   *
   * 链表结构:
   *   - 哨兵节点（ColumnFamilySet::dummy_cfd_）
   *   - 所有活跃列族形成循环链表
   *   - 第一个列族的 prev_ 指向哨兵节点
   *   - 哨兵节点的 prev_ 指向最后一个列族
   *
   * 使用场景:
   *   1. 双向遍历:
   *   - 可以向前遍历列族
   *   - 可以向后遍历列族
   *
   *   2. 添加列族:
   *   - 插入到 dummy_cfd_ 之后
   *   - 更新 next_ 和 prev_ 指针
   *
   *   3. 删除列族:
   *   - 从链表中移除
   *   - 更新前后节点的 next_ 和 prev_
   *
   *   4. 维护链表:
   *   - 保持双向链表的完整性
   *   - 确保指针一致性
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 遍历需要在互斥锁保护下（或使用 RefedColumnFamilySet）
   *
   * 注意事项:
   *   - 与 next_ 一起维护双向链表
   *   - 链表是循环的
   *   - 删除时需要同时更新 next_ 和 prev_
   *   - 哨兵节点的 prev_ 指向最后一个列族
   */
  ColumnFamilyData* prev_;

  /**
   * log_number_ - 最早的 WAL 日志号
   *
   * 功能概述:
   *   - 无符号整数，记录包含此列族数据的最早期 WAL 日志号
   *   - 所有早于此日志号的 WAL 文件与此列族无关
   *   - 用于 WAL 恢复和日志回收
   *
   * WAL 日志号:
   *   - 每个 WAL 文件有一个唯一的日志号
   *   - 日志号单调递增
   *   - 用于标识 WAL 文件的顺序
   *
   * 使用场景:
   *   1. WAL 恢复:
   *   - 从日志号开始恢复数据
   *   - 忽略早于此日志号的 WAL 文件
   *   - 只恢复相关的数据
   *
   *   2. 日志回收:
   *   - 判断 WAL 文件是否可以删除
   *   - 所有列族都不需要的日志号可以删除
   *   - 确保数据不丢失
   *
   *   3. 日志切换:
   *   - WAL 日志切换时更新日志号
   *   - 记录最新的 WAL 日志号
   *
   *   4. 范围查询:
   *   - 确定哪些 WAL 文件需要检查
   *   - 减少需要扫描的 WAL 文件数量
   *
   * 更新时机:
   *   - 创建列族时初始化
   *   - Flush 完成后更新
   *   - WAL 日志切换时更新
   *
   * 计算方式:
   *   - OldestLogToKeep() 计算需要保留的最早日志号
   *   - 考虑 mem_ 和 imm_ 中的数据
   *   - 考虑快照和迭代器
   *
   * 线程安全性:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 注意事项:
   *   - 日志号单调递增
   *   - 删除过早的日志会导致数据丢失
   *   - 需要考虑所有列族的日志号
   *   - 快照和迭代器会影响日志回收
   */
  // This is the earliest log file number that contains data from this
  // Column Family. All earlier log files must be ignored and not
  // recovered from
  uint64_t log_number_;

  /**
   * compaction_picker_ - 压缩选择器
   *
   * 功能概述:
   *   - 智能指针，指向 CompactionPicker 对象
   *   - 负责选择下一个要执行的 Compaction
   *   - 跟踪 Compaction 统计信息
   *
   * 功能:
   *   1. 选择 Compaction:
   *   - 根据压缩分数选择需要压缩的层
   *   - 选择要压缩的文件
   *   - 确定输出层和目标大小
   *
   *   2. 统计信息:
   *   - 跟踪各层的压缩分数
   *   - 记录压缩字节数和次数
   *   - 记录压缩时间
   *
   *   3. Compaction 策略:
   *   - 根据配置实现不同的 Compaction 策略
   *   - Leveled Compaction
   *   - Universal Compaction
   *   - FIFO Compaction
   *
   * 使用场景:
   *   1. Compaction 调度:
   *   - PickCompaction() 选择下一个 Compaction
   *   - NeedsCompaction() 判断是否需要 Compaction
   *
   *   2. 手动 Compaction:
   *   - CompactRange() 执行手动 Compaction
   *   - 确定压缩的范围和文件
   *
   *   3. 统计查询:
   *   - 获取 Compaction 统计信息
   *   - 分析 Compaction 效果
   *
   * 配置:
   *   - 由 cf_options.compaction_style 控制
   *   - 根据不同的 Compaction 策略创建不同的选择器
   *
   * 生命周期:
   *   - 列族创建时创建
   *   - 列族销毁时销毁
   *
   * 线程安全性:
   *   - CompactionPicker 内部可能有状态
   *   - 修改需要在 DB 互斥锁保护下
   *   - 读取需要在互斥锁保护下
   *
   * 注意事项:
   *   - CompactionPicker 的实现取决于 Compaction 策略
   *   - 需要根据列族配置选择合适的策略
   *   - Compaction 策略影响性能
   */
  // An object that keeps all the compaction stats
  // and picks the next compaction
  std::unique_ptr<CompactionPicker> compaction_picker_;

  /**
   * column_family_set_ - 列族集合指针
   *
   * 功能概述:
   *   - 指向列族集合（ColumnFamilySet）
   *   - 列族集合管理数据库的所有列族
   *   - 用于访问共享资源和全局配置
   *
   * ColumnFamilySet 功能:
   *   - 管理所有列族的元数据
   *   - 提供列族查找功能
   *   - 维护列族的 ID 和名称映射
   *   - 管理共享资源（缓存、管理器等）
   *
   * 使用场景:
   *   1. 创建列族:
   *   - 通过 column_family_set_ 创建新列族
   *   - 添加到集合中
   *
   *   2. 删除列族:
   *   - 从 column_family_set_ 中移除
   *   - 释放资源
   *
   *   3. 访问共享资源:
   *   - 获取 table_cache
   *   - 获取 write_buffer_manager
   *   - 获取 write_controller
   *
   *   4. 查找其他列族:
   *   - 按名称或 ID 查找列族
   *   - 访问其他列族的数据
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilyData 仅保存引用
   *   - 必须保证实例生命周期长于此指针
   *
   * 线程安全性:
   *   - ColumnFamilySet 的操作需要 DB 互斥锁保护
   *   - 指针本身不需要保护（不可变）
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - ColumnFamilySet 的生命周期长于列族
   *   - 可以通过 column_family_set_ 访问全局配置
   */
  ColumnFamilySet* column_family_set_;

  /**
   * write_controller_token_ - 写入控制器令牌
   *
   * 功能概述:
   *   - 智能指针，指向 WriteControllerToken
   *   - 用于与写入控制器通信
   *   - 可以防止写入被停止
   *
   * WriteController 功能:
   *   - 控制数据库的写入速率
   *   - 在特定情况下停止写入
   *   - 例如：MemTable 满、L0 文件过多
   *
   * 令牌机制:
   *   - 持有令牌时，写入不会被停止
   *   - 没有令牌时，写入可能被停止
   *   - 用于实现细粒度的写入控制
   *
   * 使用场景:
   *   1. 防止写入停止:
   *   - 持有令牌可以防止写入被停止
   *   - 用于后台任务（如 Compaction）
   *   - 确保任务可以完成
   *
   *   2. 细粒度控制:
   *   - 不同的组件可以持有令牌
   *   - 实现灵活的写入控制
   *
   *   3. 写入恢复:
   *   - 释放令牌后，写入可能被停止
   *   - 等待条件满足后恢复写入
   *
   * 配置:
   *   - 由 DBImpl 管理
   *   - 列族可以获取令牌
   *
   * 生命周期:
   *   - 列族创建时获取令牌
   *   - 列族销毁时释放令牌
   *
   * 线程安全性:
   *   - WriteControllerToken 本身是线程安全的
   *   - 可以在多线程中使用
   *
   * 注意事项:
   *   - 可能为 nullptr（未获取令牌）
   *   - 令牌与写入控制器关联
   *   - 释放令牌可能触发写入停止
   *   - 需要正确管理令牌的生命周期
   */
  std::unique_ptr<WriteControllerToken> write_controller_token_;

  /**
   * queued_for_flush_ - 是否在 Flush 队列中
   *
   * 功能概述:
   *   - 布尔值，标记列族是否在 DBImpl::flush_queue_ 中
   *   - true 表示列族正在等待或执行 Flush
   *   - 用于防止重复添加到 Flush 队列
   *
   * 使用场景:
   *   1. Flush 调度:
   *   - 需要 Flush 时检查此标志
   *   - 如果为 false，添加到队列
   *   - 如果为 true，跳过（已经在队列中）
   *
   *   2. Flush 执行:
   *   - 开始 Flush 时设置为 true
   *   - 完成后设置为 false
   *
   *   3. 重复检查:
   *   - 防止同一个列族被多次添加到队列
   *   - 避免 Flush 重复执行
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 状态转换:
   *   - false → true: 添加到 Flush 队列
   *   - true → false: Flush 完成或取消
   *
   * 注意事项:
   *   - 初始值为 false
   *   - 在 DB 互斥锁保护下访问
   *   - 与 queued_for_compaction_ 类似
   *   - 用于后台任务的队列管理
   */
  // If true --> this ColumnFamily is currently present in DBImpl::flush_queue_
  bool queued_for_flush_;

  /**
   * queued_for_compaction_ - 是否在 Compaction 队列中
   *
   * 功能概述:
   *   - 布尔值，标记列族是否在 DBImpl::compaction_queue_ 中
   *   - true 表示列族正在等待或执行 Compaction
   *   - 用于防止重复添加到 Compaction 队列
   *
   * 使用场景:
   *   1. Compaction 调度:
   *   - 需要 Compaction 时检查此标志
   *   - 如果为 false，添加到队列
   *   - 如果为 true，跳过（已经在队列中）
   *
   *   2. Compaction 执行:
   *   - 开始 Compaction 时设置为 true
   *   - 完成后设置为 false
   *
   *   3. 重复检查:
   *   - 防止同一个列族被多次添加到队列
   *   - 避免 Compaction 重复执行
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 状态转换:
   *   - false → true: 添加到 Compaction 队列
   *   - true → false: Compaction 完成或取消
   *
   * 注意事项:
   *   - 初始值为 false
   *   - 在 DB 互斥锁保护下访问
   *   - 与 queued_for_flush_ 类似
   *   - 用于后台任务的队列管理
   */
  // If true --> this ColumnFamily is currently present in
  // DBImpl::compaction_queue_
  bool queued_for_compaction_;

  /**
   * prev_compaction_needed_bytes_ - 上次需要的 Compaction 字节数
   *
   * 功能概述:
   *   - 无符号整数，记录上次计算时需要 Compaction 的字节数
   *   - 用于优化 Compaction 分数的计算
   *   - 避免重复计算 Compaction 需求
   *
   * Compaction 分数:
   *   - 表示 Compaction 的紧急程度
   *   - 分数越高，越需要 Compaction
   *   - 分数基于各层的大小比例和文件数量
   *
   * 使用场景:
   *   1. 分数计算优化:
   *   - 记录上次的 Compaction 需求
   *   - 如果变化不大，可以跳过更新
   *
   *   2. Compaction 调度:
   *   - 比较当前需求与上次需求
   *   - 决定是否需要更新 Compaction 队列
   *
   *   3. 性能优化:
   *   - 避免频繁计算 Compaction 分数
   *   - 减少锁竞争
   *
   * 更新时机:
   *   - Compaction 分数重新计算时
   *   - Flush 完成后
   *   - Compaction 完成后
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 注意事项:
   *   - 用于优化，不影响正确性
   *   - 可以在重新计算时重置
   *   - 是一个优化技巧，不是必需的
   */
  uint64_t prev_compaction_needed_bytes_;

  /**
   * allow_2pc_ - 是否允许两阶段提交
   *
   * 功能概述:
   *   - 布尔值，标记数据库是否启用了两阶段提交（2PC）
   *   - 用于事务支持
   *   - 取决于数据库配置
   *
   * 两阶段提交（2PC）:
   *   - Prepare: 准备阶段，记录意向
   *   - Commit: 提交阶段，确认提交
   *   - 支持跨多个数据库或资源的原子提交
   *
   * 使用场景:
   *   1. 事务支持:
   *   - 启用 2PC 时支持分布式事务
   *   - 保证跨数据库的原子性
   *
   *   2. WAL 记录:
   *   - WAL 中包含 2PC 记录
   *   - 恢复时需要处理 2PC 记录
   *
   *   3. 操作验证:
   *   - 检查是否允许 2PC 操作
   *   - 返回适当的错误信息
   *
   * 配置:
   *   - 由 db_options.allow_2pc 控制
   *   - 数据库打开时确定
   *
   * 线程安全性:
   *   - 不可变，构造后不再修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 初始值来自数据库配置
   *   - 2PC 会增加 WAL 的复杂度
   *   - 2PC 恢复需要特殊处理
   *   - 如果不使用 2PC，此标志为 false
   */
  // if the database was opened with 2pc enabled
  bool allow_2pc_;

  /**
   * last_memtable_id_ - 上一个 MemTable ID
   *
   * 功能概述:
   *   - 原子无符号整数，记录上一个分配的 MemTable ID
   *   - 用于追踪和标识 MemTable
   *   - 单调递增
   *
   * MemTable ID:
   *   - 每个 MemTable 有唯一的 ID
   *   - 用于追踪 MemTable 的生命周期
   *   - 单调递增，不重复
   *
   * 使用场景:
   *   1. MemTable 标识:
   *   - 为新 MemTable 分配 ID
   *   - 用于日志和调试
   *
   *   2. Flush 追踪:
   *   - 记录正在 Flush 的 MemTable ID
   *   - 追踪 Flush 进度
   *
   *   3. 诊断:
   *   - 追踪 MemTable 的创建和销毁
   *   - 分析 MemTable 的生命周期
   *
   * 分配方式:
   *   - NewMemtable() 或 SetMemtable() 时分配
   *   - 使用 fetch_add() 原子递增
   *   - 返回 last_memtable_id_ + 1
   *
   * 线程安全性:
   *   - 原子操作（fetch_add）
   *   - 多线程安全地分配 ID
   *
   * 注意事项:
   *   - 原子变量
   *   - 单调递增
   *   - 初始值为 0
   *   - ID 可能溢出（uint64_t 范围很大）
   */
  // Memtable id to track flush.
  std::atomic<uint64_t> last_memtable_id_;

  /**
   * data_dirs_ - 数据目录列表
   *
   * 功能概述:
   *   - 共享指针的向量，存储列族的数据目录
   *   - 对应于 cf_paths 配置
   *   - 支持多目录存储 SSTable 文件
   *
   * 多目录存储:
   *   - 将 SSTable 文件分布到多个目录
   *   - 可以是不同的磁盘或文件系统
   *   - 提高存储容量和 I/O 并行度
   *
   * 使用场景:
   *   1. 文件创建:
   *   - 创建 SSTable 文件时选择目录
   *   - 根据路径 ID 选择目录
   *
   *   2. 文件管理:
   *   - 获取指定目录的 FSDirectory
   *   - 管理目录的文件句柄
   *
   *   3. 存储分布:
   *   - 根据配置分布文件到不同目录
   *   - 提高存储容量
   *
   * 配置:
   *   - 由 cf_options.cf_paths 配置
   *   - 每个路径有对应的路径 ID
   *
   * 初始化:
   *   - 列族创建时初始化
   *   - 调用 AddDirectories() 注册目录
   *
   * 线程安全性:
   *   - FSDirectory 内部有锁机制
   *   - 向量本身需要锁保护（读取很少修改）
   *
   * 注意事项:
   *   - 可能为空（单目录存储）
   *   - FSDirectory 封装了文件系统操作
   *   - 需要正确注册和注销目录
   *   - 目录必须在列族生命周期内有效
   */
  // Directories corresponding to cf_paths.
  std::vector<std::shared_ptr<FSDirectory>> data_dirs_;

  /**
   * db_paths_registered_ - 数据库路径是否已注册
   *
   * 功能概述:
   *   - 布尔值，标记数据目录是否已注册
   *   - true 表示已调用 AddDirectories()
   *   - 防止重复注册
   *
   * 使用场景:
   *   1. 注册检查:
   *   - 检查目录是否已注册
   *   - 避免重复注册
   *
   *   2. 目录管理:
   *   - 列族创建时注册目录
   *   - 确保目录只注册一次
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 状态转换:
   *   - false → true: 成功调用 AddDirectories()
   *   - 不会从 true 变回 false
   *
   * 注意事项:
   *   - 初始值为 false
   *   - 注册一次后就不再改变
   *   - 用于防止重复操作
   *   - 与 data_dirs_ 配合使用
   */
  bool db_paths_registered_;

  /**
   * full_history_ts_low_ - 完整历史时间戳下界
   *
   * 功能概述:
   *   - 字符串，记录完整历史的时间戳下界
   *   - 用于时间戳功能的范围查询
   *   - 只能增加，不能减少
   *
   * 时间戳功能:
   *   - RocksDB 支持用户定义的时间戳
   *   - 可以用于 TTL、范围查询等
   *   - 完整历史表示保留所有版本的数据
   *
   * 使用场景:
   *   1. 范围查询:
   *   - 查询特定时间戳范围的数据
   *   - 时间戳 >= full_history_ts_low_ 的数据保留完整历史
   *
   *   2. 数据保留:
   *   - 控制数据的保留策略
   *   - 可以调整保留的历史范围
   *
   *   3. Compaction:
   *   - Compaction 时考虑时间戳范围
   *   - 确保保留足够的历史
   *
   * 设置规则:
   *   - 只能增加（向前推进）
   *   - 不能减少（向后回退）
   *   - 使用 SetFullHistoryTsLow() 设置
   *
   * 并发控制:
   *   - 读取是线程安全的
   *   - 修改需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 初始为空字符串
   *   - 只能增加，不能减少
   *   - 使用比较器比较时间戳
   *   - 影响数据保留策略
   */
  std::string full_history_ts_low_;

  /**
   * file_metadata_cache_res_mgr_ - 文件元数据缓存预留管理器
   *
   * 功能概述:
   *   - 共享指针，指向 CacheReservationManager
   *   - 用于预留文件元数据的缓存空间
   *   - 与此列族的 Version 关联
   *
   * 文件元数据:
   *   - SSTable 文件的元数据信息
   *   - 包括文件大小、键范围、序列号范围等
   *   - 需要占用缓存空间
   *
   * 使用场景:
   *   1. 缓存预留:
   *   - 为新添加的文件预留缓存空间
   *   - 跟踪文件元数据的内存使用
   *
   *   2. 内存管理:
   *   - 管理文件元数据的缓存占用
   *   - 防止缓存溢出
   *
   *   3. Compaction:
   *   - Compaction 创建新文件时预留空间
   *   - 删除旧文件时释放空间
   *
   * 配置:
   *   - 由 db_options 中的缓存配置控制
   *   - 与 block_cache 关联
   *
   * 生命周期:
   *   - 列族创建时创建
   *   - 列族销毁时销毁
   *
   * 线程安全性:
   *   - CacheReservationManager 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 管理文件元数据的缓存空间
   *   - 不是管理数据块的缓存
   *   - 有助于精确控制缓存使用
   *   - 提高缓存的利用效率
   */
  // For charging memory usage of file metadata created for newly added files to
  // a Version associated with this CFD
  std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr_;

  /**
   * mempurge_used_ - 是否使用过 MemPurge 功能
   *
   * 功能概述:
   *   - 布尔值，标记列族是否使用过 MemPurge 功能
   *   - MemPurge 是 RocksDB 的内存清理功能
   *   - 可以清理 MemTable 中的数据
   *
   * MemPurge 功能:
   *   - 清理 MemTable 中不需要的数据
   *   - 可以基于时间戳或范围
   *   - 减少内存占用
   *
   * 使用场景:
   *   1. 功能追踪:
   *   - 记录是否使用过 MemPurge
   *   - 用于统计和诊断
   *
   *   2. 恢复:
   *   - 恢复时检查是否使用过 MemPurge
   *   - 确保正确处理数据
   *
   *   3. 优化:
   *   - 根据使用情况优化策略
   *
   * 设置时机:
   *   - 首次使用 MemPurge 时设置为 true
   *   - 使用 SetMempurgeUsed() 设置
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 状态转换:
   *   - false → true: 首次使用 MemPurge
   *   - 不会从 true 变回 false
   *
   * 注意事项:
   *   - 初始值为 false
   *   - 一旦设置为 true，不会再改变
   *   - 是一个追踪标志，不影响功能
   *   - 用于诊断和统计
   */
  bool mempurge_used_;

  /**
   * next_epoch_number_ - 下一个 Epoch 号
   *
   * 功能概述:
   *   - 原子无符号整数，记录下一个可用的 Epoch 号
   *   - Epoch 用于追踪文件的生命周期
   *   - 单调递增
   *
   * Epoch 功能:
   *   - 标识文件的版本
   *   - 追踪文件的创建和删除
   *   - 支持文件级的垃圾回收
   *
   * 使用场景:
   *   1. 文件标识:
   *   - 为新文件分配 Epoch 号
   *   - 追踪文件的创建顺序
   *
   *   2. 垃圾回收:
   *   - 根据 Epoch 判断文件是否可以删除
   *   - 确保所有引用都释放后删除
   *
   *   3. 恢复:
   *   - 恢复时重建 Epoch 号
   *   - RecoverEpochNumbers() 方法
   *
   * 分配方式:
   *   - NewEpochNumber() 分配新的 Epoch 号
   *   - 使用 fetch_add() 原子递增
   *   - 返回当前值（递增后的值）
   *
   * 查询方式:
   *   - GetNextEpochNumber() 获取下一个 Epoch 号
   *   - SetNextEpochNumber() 设置 Epoch 号（恢复时）
   *   - ResetNextEpochNumber() 重置 Epoch 号
   *
   * 线程安全性:
   *   - 原子操作（fetch_add, load, store）
   *   - 多线程安全地访问
   *
   * 注意事项:
   *   - 原子变量
   *   - 单调递增
   *   - 初始值为 1（或从 1 开始）
   *   - ID 可能溢出（uint64_t 范围很大）
   *   - 恢复时可能需要从文件中读取 Epoch 号
   */
  std::atomic<uint64_t> next_epoch_number_;
};

// ColumnFamilySet has interesting thread-safety requirements
// * CreateColumnFamily() or RemoveColumnFamily() -- need to be protected by DB
// mutex AND executed in the write thread.
// CreateColumnFamily() should ONLY be called from VersionSet::LogAndApply() AND
// single-threaded write thread. It is also called during Recovery and in
// DumpManifest().
// RemoveColumnFamily() is only called from SetDropped(). DB mutex needs to be
// held and it needs to be executed from the write thread. SetDropped() also
// guarantees that it will be called only from single-threaded LogAndApply(),
// but this condition is not that important.
// * Iteration -- hold DB mutex. If you want to release the DB mutex in the
// body of the iteration, wrap in a RefedColumnFamilySet.
// * GetDefault() -- thread safe
// * GetColumnFamily() -- either inside of DB mutex or from a write thread
// * GetNextColumnFamilyID(), GetMaxColumnFamily(), UpdateMaxColumnFamily(),
// NumberOfColumnFamilies -- inside of DB mutex
/**
 * ColumnFamilySet - 列族集合管理器
 *
 * 功能概述:
 *   - 管理数据库中的所有列族（Column Family）
 *   - 提供列族的创建、查找、删除功能
 *   - 维护列族的 ID 和名称映射
 *   - 支持遍历所有列族
 *   - 管理列族的全局资源和配置
 *
 * 设计目的:
 *   - RocksDB 支持多个列族，每个列族有独立的数据、索引、配置
 *   - 需要一个统一的管理器来协调所有列族
 *   - ColumnFamilySet 提供列族的元数据管理和快速查找
 *   - 维护列族之间的共享资源（如 TableCache、WriteBufferManager）
 *
 * 核心数据结构:
 *   1. 名称 → ID 映射 (column_families_):
 *      - 通过列族名称快速查找列族 ID
 *      - 用于按名称访问列族
 *
 *   2. ID → ColumnFamilyData 映射 (column_family_data_):
 *      - 通过列族 ID 快速查找列族数据
 *      - 用于按 ID 访问列族
 *
 *   3. 双向循环链表 (dummy_cfd_ → cfd1 → cfd2 → ... → dummy_cfd_):
 *      - 支持高效遍历所有列族
 *      - 用于迭代器实现
 *      - 使用循环链表（dummy_cfd_ 是哨兵节点）
 *
 * 列族标识:
 *   - 每个列族有唯一的 ID（uint32_t）
 *   - 默认列族的 ID 固定为 0
 *   - 其他列族的 ID 单调递增（通过 GetNextColumnFamilyID() 分配）
 *   - ID 在数据库生命周期内不重复（即使列族被删除）
 *
 * 使用场景:
 *   1. DBImpl 初始化:
 *      - 加载 MANIFEST 文件时重建列族集合
 *      - 为每个列族创建 ColumnFamilyData
 *
 *   2. 创建列族:
 *      - 用户调用 DB::CreateColumnFamily()
 *      - 分配新的列族 ID
 *      - 创建 ColumnFamilyData 并添加到集合
 *
 *   3. 删除列族:
 *      - 用户调用 DB::DropColumnFamily()
 *      - 标记列为 dropped
 *      - 引用计数为 0 时从集合中移除
 *
 *   4. 查找列族:
 *      - WriteBatch 写入时根据 ID 查找列族
 *      - 读取操作根据名称或 ID 查找列族
 *
 *   5. 遍历列族:
 *      - Flush 所有列族的 MemTable
 *      - Compaction 调度
 *      - 统计和监控
 *
 * 共享资源:
 *   - table_cache_: 所有列族共享的 SSTable 缓存
 *   - write_buffer_manager_: 管理所有列族的内存缓冲
 *   - write_controller_: 控制写入速率
 *   - block_cache_tracer_: 追踪 Block Cache 访问
 *
 * 线程安全性:
 *   - 修改操作（创建、删除）需要:
 *      1. 持有 DB 互斥锁
 *      2. 当前线程是单线程写线程
 *
 *   - 读取操作（查找、遍历）需要至少满足:
 *      1. 持有 DB 互斥锁，或
 *      2. 从单线程写线程调用
 *
 *   - 大多数操作需要在 DB 互斥锁保护下进行
 *
 * 性能优化:
 *   - 默认列族缓存 (default_cfd_cache_): 直接返回指针，避免查找
 *   - 双向哈希映射: 按名称/ID 查找都是 O(1) 平均时间
 *   - 循环链表: 遍历所有列族 O(n)，添加/删除 O(1)
 */
class ColumnFamilySet {
 public:
  // ColumnFamilySet supports iteration
  class iterator {
   public:
    explicit iterator(ColumnFamilyData* cfd) : current_(cfd) {}
    // NOTE: minimum operators for for-loop iteration
    iterator& operator++() {
      current_ = current_->next_;
      return *this;
    }
    bool operator!=(const iterator& other) const {
      return this->current_ != other.current_;
    }
    ColumnFamilyData* operator*() { return current_; }

   private:
    ColumnFamilyData* current_;
  };

  /**
   * 构造函数 - 创建列族集合
   *
   * 功能概述:
   *   - 初始化 ColumnFamilySet 实例
   *   - 创建 dummy_cfd_ 作为循环链表的哨兵节点
   *   - 初始化链表结构（dummy_cfd_ 指向自己）
   *   - 保存全局资源和配置的引用
   *
   * 参数说明:
   *   @param dbname: 数据库名称（路径）
   *   @param db_options: 不可变数据库选项（指针）
   *   @param file_options: 文件操作选项
   *   @param table_cache: SSTable 缓存（所有列族共享）
   *   @param _write_buffer_manager: 写入缓冲区管理器
   *   @param _write_controller: 写入控制器（流量控制）
   *   @param block_cache_tracer: Block Cache 追踪器
   *   @param io_tracer: IO 追踪器
   *   @param db_id: 数据库 ID
   *   @param db_session_id: 数据库会话 ID
   *
   * 初始化状态:
   *   - column_families_: 空（无列族）
   *   - column_family_data_: 空（无列族）
   *   - max_column_family_: 0
   *   - dummy_cfd_: 创建并初始化为循环链表
   *   - default_cfd_cache_: nullptr（待初始化）
   */
  ColumnFamilySet(const std::string& dbname,
                  const ImmutableDBOptions* db_options,
                  const FileOptions& file_options, Cache* table_cache,
                  WriteBufferManager* _write_buffer_manager,
                  WriteController* _write_controller,
                  BlockCacheTracer* const block_cache_tracer,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  const std::string& db_id, const std::string& db_session_id);

  /**
   * 析构函数 - 销毁列族集合
   *
   * 功能概述:
   *   - 清理所有列族的数据结构
   *   - 释放所有 ColumnFamilyData 对象
   *   - 释放 dummy_cfd_ 哨兵节点
   *
   * 清理流程:
   *   1. 遍历所有列族（column_family_data_）
   *   2. 对每个列族调用 UnrefAndTryDelete()
   *   3. 列族的析构函数会自动从 column_family_data_ 中移除自己
   *   4. 最后释放 dummy_cfd_
   */
  ~ColumnFamilySet();

  /**
   * GetDefault - 获取默认列族
   *
   * 功能概述:
   *   - 返回默认列族的 ColumnFamilyData 指针
   *   - 默认列族的 ID 固定为 0
   *   - 使用 default_cfd_cache_ 缓存优化性能
   *
   * 返回值:
   *   - 默认列族的 ColumnFamilyData 指针
   *
   * 注意事项:
   *   - 需要先创建默认列族
   *   - 调用者不应释放此指针
   */
  ColumnFamilyData* GetDefault() const;

  /**
   * GetColumnFamily - 根据列族 ID 获取列族
   *
   * 功能概述:
   *   - 通过列族 ID 查找对应的列族数据
   *   - 在哈希映射中查找，时间复杂度 O(1)
   *
   * 参数说明:
   *   @param id: 列族 ID（uint32_t）
   *
   * 返回值:
   *   - 成功：列族的 ColumnFamilyData 指针
   *   - 失败：nullptr（列族不存在）
   */
  // GetColumnFamily() calls return nullptr if column family is not found
  ColumnFamilyData* GetColumnFamily(uint32_t id) const;

  /**
   * GetColumnFamily - 根据列族名称获取列族
   *
   * 功能概述:
   *   - 通过列族名称查找对应的列族数据
   *   - 先在名称 → ID 映射中查找 ID
   *   - 再在 ID → ColumnFamilyData 映射中查找数据
   *
   * 参数说明:
   *   @param name: 列族名称（字符串）
   *
   * 返回值:
   *   - 成功：列族的 ColumnFamilyData 指针
   *   - 失败：nullptr（列族不存在）
   */
  ColumnFamilyData* GetColumnFamily(const std::string& name) const;

  /**
   * GetNextColumnFamilyID - 获取下一个可用的列族 ID
   *
   * 功能概述:
   *   - 分配一个新的列族 ID
   *   - ID 单调递增，确保唯一性
   *   - 即使删除列族，ID 也不会重用
   *
   * 返回值:
   *   - 新的列族 ID（uint32_t）
   *
   * 注意事项:
   *   - 返回的 ID 大于任何已存在的列族 ID
   *   - 保证在整个 RocksDB 实例历史中也是唯一的
   */
  // this call will return the next available column family ID. it guarantees
  // that there is no column family with id greater than or equal to the
  // returned value in the current running instance or anytime in RocksDB
  // instance history.
  uint32_t GetNextColumnFamilyID();

  /**
   * GetMaxColumnFamily - 获取当前最大的列族 ID
   *
   * 返回值:
   *   - 当前最大列族 ID（uint32_t）
   */
  uint32_t GetMaxColumnFamily();

  /**
   * UpdateMaxColumnFamily - 更新最大列族 ID
   *
   * 功能概述:
   *   - 更新 max_column_family_ 为指定值
   *   - 只能增大，不能减小
   *   - 用于从 MANIFEST 恢复时同步 ID 状态
   *
   * 参数说明:
   *   @param new_max_column_family: 新的最大列族 ID
   */
  void UpdateMaxColumnFamily(uint32_t new_max_column_family);

  /**
   * NumberOfColumnFamilies - 获取列族数量
   *
   * 返回值:
   *   - 列族数量（size_t）
   */
  size_t NumberOfColumnFamilies() const;

  /**
   * CreateColumnFamily - 创建新的列族
   *
   * 功能概述:
   *   - 创建新的列族并添加到集合
   *   - 初始化列族的元数据和资源
   *   - 更新名称 → ID 映射和 ID → ColumnFamilyData 映射
   *   - 将列族添加到双向链表
   *
   * 参数说明:
   *   @param name: 列族名称（必须唯一）
   *   @param id: 列族 ID（通过 GetNextColumnFamilyID() 分配）
   *   @param dummy_version: 哨兵版本（用于版本管理）
   *   @param options: 列族选项（配置参数）
   *
   * 返回值:
   *   - 新创建的 ColumnFamilyData 指针
   *
   * 调用要求:
   *   - 必须在 DB 互斥锁保护下调用
   *   - 必须从单线程写线程调用
   */
  ColumnFamilyData* CreateColumnFamily(const std::string& name, uint32_t id,
                                       Version* dummy_version,
                                       const ColumnFamilyOptions& options);

  /**
   * GetRunningColumnFamiliesTimestampSize - 获取运行中列族的时间戳大小映射
   *
   * 返回值:
   *   - 列族 ID → 时间戳大小（size_t）的映射
   *   - 包含所有列族（包括时间戳大小为 0 的）
   */
  const UnorderedMap<uint32_t, size_t>& GetRunningColumnFamiliesTimestampSize()
      const {
    return running_ts_sz_;
  }

  /**
   * GetColumnFamiliesTimestampSizeForRecord - 获取需要记录的列族时间戳大小映射
   *
   * 返回值:
   *   - 列族 ID → 时间戳大小（size_t）的映射
   *   - 只包含时间戳大小 > 0 的列族
   */
  const UnorderedMap<uint32_t, size_t>&
  GetColumnFamiliesTimestampSizeForRecord() const {
    return ts_sz_for_record_;
  }

  /**
   * begin - 获取迭代器的起始位置
   *
   * 返回值:
   *   - 迭代器，指向第一个列族
   */
  iterator begin() { return iterator(dummy_cfd_->next_); }

  /**
   * end - 获取迭代器的结束位置
   *
   * 返回值:
   *   - 迭代器，指向 dummy_cfd_
   */
  iterator end() { return iterator(dummy_cfd_); }

  /**
   * get_table_cache - 获取 SSTable 缓存
   *
   * 返回值:
   *   - TableCache 指针
   */
  Cache* get_table_cache() { return table_cache_; }

  /**
   * write_buffer_manager - 获取写入缓冲区管理器
   *
   * 返回值:
   *   - WriteBufferManager 指针
   */
  WriteBufferManager* write_buffer_manager() { return write_buffer_manager_; }

  /**
   * write_controller - 获取写入控制器
   *
   * 返回值:
   *   - WriteController 指针
   */
  WriteController* write_controller() { return write_controller_; }

 private:
  friend class ColumnFamilyData;
  // helper function that gets called from cfd destructor
  // REQUIRES: DB mutex held
  void RemoveColumnFamily(ColumnFamilyData* cfd);

  /**
   * column_families_ - 列族名称到 ID 的映射
   *
   * 功能概述:
   *   - 哈希映射，存储列族名称到列族 ID 的对应关系
   *   - 支持按列族名称快速查找列族 ID
   *   - 平均查找时间复杂度：O(1)
   *
   * 数据结构:
   *   - Key: 列族名称（std::string）
   *   - Value: 列族 ID（uint32_t）
   *
   * 使用场景:
   *   1. 按名称查找列族:
   *      - 调用 GetColumnFamily(name) 时先查找 ID
   *      - 再在 column_family_data_ 中查找 ColumnFamilyData
   *
   *   2. 创建列族时:
   *      - 检查列族名称是否已存在
   *      - 避免重复创建同名列族
   *
   *   3. 删除列族时:
   *      - 从此映射中移除条目
   *
   * 并发控制:
   *   - 修改操作需要:
   *      1. 持有 DB 互斥锁
   *      2. 当前线程是单线程写线程
   *
   *   - 读取操作需要至少满足:
   *      1. 持有 DB 互斥锁，或
   *      2. 从单线程写线程调用
   *
   * 注意事项:
   *   - 默认列族的名称通常为 "default"，ID 固定为 0
   *   - 列族名称在数据库中必须唯一
   *   - 即使列族被删除，名称映射也会立即移除
   */
  UnorderedMap<std::string, uint32_t> column_families_;

  /**
   * column_family_data_ - 列族 ID 到 ColumnFamilyData 的映射
   *
   * 功能概述:
   *   - 哈希映射，存储列族 ID 到列族数据对象的对应关系
   *   - 支持按列族 ID 快速查找 ColumnFamilyData
   *   - 平均查找时间复杂度：O(1)
   *
   * 数据结构:
   *   - Key: 列族 ID（uint32_t）
   *   - Value: ColumnFamilyData 指针（ColumnFamilyData*）
   *
   * ColumnFamilyData 包含:
   *   - 列族名称和 ID
   *   - MemTable（活跃和不可变）
   *   - 版本信息（Version）
   *   - 比较器（Comparator）
   *   - 列族选项（ColumnFamilyOptions）
   *   - SuperVersion（版本缓存）
   *   - 统计信息（InternalStats）
   *   - Compaction 相关信息
   *
   * 使用场景:
   *   1. 按ID查找列族:
   *      - 调用 GetColumnFamily(id) 时直接查找
   *      - 返回对应的 ColumnFamilyData
   *
   *   2. WriteBatch 写入时:
   *      - 根据操作中的列族 ID 查找目标列族
   *      - 获取对应的 MemTable 进行写入
   *
   *   3. 列族遍历:
   *      - 配合链表结构遍历所有列族
   *
   *   4. 创建列族时:
   *      - 将新创建的 ColumnFamilyData 添加到此映射
   *
   *   5. 删除列族时:
   *      - 从此映射中移除条目（在引用计数为 0 后）
   *
   * 并发控制:
   *   - 修改操作需要:
   *      1. 持有 DB 互斥锁
   *      2. 当前线程是单线程写线程
   *
   *   - 读取操作需要至少满足:
   *      1. 持有 DB 互斥锁，或
   *      2. 从单线程写线程调用
   *
   * 所有权管理:
   *   - ColumnFamilySet 拥有此映射中所有 ColumnFamilyData 的所有权
   *   - ColumnFamilyData 的生命周期由引用计数和此映射共同管理
   *   - 当引用计数为 0 且已标记为 dropped 时，从映射中移除并删除
   *
   * 注意事项:
   *   - 默认列族的 ID 固定为 0
   *   - 列族 ID 单调递增，不会重用
   *   - 即使列族被删除，ID 也不会被重新分配
   */
  UnorderedMap<uint32_t, ColumnFamilyData*> column_family_data_;

  /**
   * running_ts_sz_ - 运行中列族的时间戳大小映射
   *
   * 功能概述:
   *   - 哈希映射，存储所有运行中列族的自定义时间戳大小
   *   - 时间戳大小指用户定义的时间戳字段的字节数
   *   - 用于支持 RocksDB 的时间戳功能（Timestamp）
   *
   * 数据结构:
   *   - Key: 列族 ID（uint32_t）
   *   - Value: 时间戳大小（size_t，字节数）
   *
   * 时间戳功能:
   *   - RocksDB 支持用户自定义时间戳
   *   - 时间戳可以用于 TTL、数据过期、范围查询等功能
   *   - 每个列族可以有自己的时间戳大小配置
   *   - 时间戳大小通常为 0、4、8 或其他值（用户自定义）
   *
   * 使用场景:
   *   1. 获取所有列族的时间戳大小:
   *      - 调用 GetRunningColumnFamiliesTimestampSize()
   *      - 返回所有列族的时间戳大小（包括为 0 的）
   *
   *   2. 恢复 MANIFEST 时:
   *      - 读取列族的时间戳大小配置
   *      - 重建时间戳映射
   *
   *   3. 验证时间戳配置:
   *      - 检查列族的时间戳大小是否合理
   *   4. 记录元数据:
   *      - 将时间戳大小写入 MANIFEST 或其他元数据文件
   *
   * 并发控制:
   *   - 修改和读取操作与 column_families_ 相同:
   *      - 修改需要：DB 互斥锁 + 单线程写线程
   *      - 读取需要：DB 互斥锁 或 单线程写线程
   *
   * 与 ts_sz_for_record_ 的区别:
   *   - running_ts_sz_: 包含所有列族（时间戳大小可能为 0）
   *   - ts_sz_for_record_: 只包含时间戳大小 > 0 的列族
   *   - running_ts_sz_ 用于完整的列族视图
   *   - ts_sz_for_record_ 用于需要记录时间戳的场景
   *
   * 注意事项:
   *   - 时间戳大小为 0 表示列族未启用时间戳功能
   *   - 时间戳大小在列族创建时确定，后续不可更改
   *   - 此映射在列族创建时初始化，删除时移除
   */
  // Mutating / reading `running_ts_sz_` and `ts_sz_for_record_` follow
  // the same requirements as `column_families_` and `column_family_data_`.
  // Mapping from column family id to user-defined timestamp size for all
  // running column families.
  UnorderedMap<uint32_t, size_t> running_ts_sz_;

  /**
   * ts_sz_for_record_ - 需要记录的列族时间戳大小映射
   *
   * 功能概述:
   *   - 哈希映射，存储启用时间戳功能的列族的时间戳大小
   *   - 只包含时间戳大小 > 0 的列族
   *   - 用于需要记录或使用时间戳的场景
   *
   * 数据结构:
   *   - Key: 列族 ID（uint32_t）
   *   - Value: 时间戳大小（size_t，字节数，> 0）
   *
   * 使用场景:
   *   1. 获取需要记录时间戳的列族:
   *      - 调用 GetColumnFamiliesTimestampSizeForRecord()
   *      - 返回只包含时间戳大小 > 0 的列族映射
   *
   *   2. 写入数据时添加时间戳:
   *      - 只为启用时间戳的列族添加时间戳字段
   *      - 减少不必要的内存和处理开销
   *
   *   3. 比较器初始化:
   *      - 只为时间戳大小 > 0 的列族配置时间戳比较逻辑
   *
   *   4. 版本编辑和元数据:
   *      - 只记录启用时间戳的列族配置
   *   5. 数据验证:
   *      - 验证数据中的时间戳是否符合列族配置
   *
   * 并发控制:
   *   - 修改和读取操作与 running_ts_sz_ 相同:
   *      - 修改需要：DB 互斥锁 + 单线程写线程
   *      - 读取需要：DB 互斥锁 或 单线程写线程
   *
   * 与 running_ts_sz_ 的区别:
   *   - ts_sz_for_record_: 只包含时间戳大小 > 0 的列族
   *   - running_ts_sz_: 包含所有列族（时间戳大小可能为 0）
   *   - ts_sz_for_record_ 优化了需要使用时间戳的场景
   *   - running_ts_sz_ 提供完整的列族视图
   *
   * 性能优化:
   *   - 避免遍历所有列族来判断是否启用时间戳
   *   - 快速找到需要处理时间戳的列族
   *   - 减少不必要的计算和内存访问
   *
   * 注意事项:
   *   - 此映射是 running_ts_sz_ 的子集
   *   - 所有在此映射中的列族也在 running_ts_sz_ 中
   *   - 此映射在列族创建且时间戳大小 > 0 时初始化
   *   - 列族删除时从此映射中移除
   *   - 时间戳大小在列族创建时确定，后续不可更改
   */
  // Mapping from column family id to user-defined timestamp size for
  // column families with non-zero user-defined timestamp size.
  UnorderedMap<uint32_t, size_t> ts_sz_for_record_;

  /**
   * max_column_family_ - 当前最大列族 ID
   *
   * 功能概述:
   *   - 记录当前所有列族中最大的 ID 值
   *   - 用于分配新的列族 ID
   *   - 保证 ID 分配的单调递增性
   *
   * 初始值:
   *   - 默认为 0（因为默认列族的 ID 固定为 0）
   *
   * 使用场景:
   *   1. 分配新列族 ID:
   *      - 调用 GetNextColumnFamilyID()
   *      - 返回 max_column_family_ + 1
   *      - 新 ID 自动成为新的 max_column_family_
   *
   *   2. 从 MANIFEST 恢复时:
   *      - 读取 MANIFEST 中的最大列族 ID
   *      - 调用 UpdateMaxColumnFamily() 更新
   *      - 确保 ID 分配连续性
   *
   *   3. 查询最大 ID:
   *      - 调用 GetMaxColumnFamily()
   *      - 返回当前最大列族 ID
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下访问
   *
   * ID 分配策略:
   *   - 列族 ID 从 0 开始（默认列族）
   *   - 新列族的 ID 为 max_column_family_ + 1
   *   - ID 单调递增，不会重用
   *   - 即使列族被删除，ID 也不会被重新分配
   *   - 这样可以避免 ID 混淆和历史数据访问问题
   *
   * 注意事项:
   *   - 此值只能增大，不能减小
   *   - 默认列族的 ID 固定为 0
   *   - 即使没有其他列族，max_column_family_ 至少为 0
   *   - 删除列族不会影响此值
   */
  uint32_t max_column_family_;

  /**
   * file_options_ - 文件操作选项
   *
   * 功能概述:
   *   - 存储文件操作相关的配置选项
   *   - 用于控制 SSTable 文件、WAL 文件、MANIFEST 文件的读写行为
   *   - 影响文件 I/O 性能、缓存、预读等行为
   *
   * 包含的选项:
   *   - 文件读取缓冲区大小
   *   - 文件写入缓冲区大小
   *   - 是否使用直接 I/O（Direct I/O）
   *   - 是否使用 fdatasync
   *   - 文件预读策略
   *   - 文件压缩选项
   *   - 文件校验和选项
   *   - 并发文件打开数量限制
   *
   * 使用场景:
   *   1. 创建列族时:
   *      - 使用此选项初始化列族的文件操作配置
   *      - 影响列族的 SSTable 文件读写
   *
   *   2. 文件系统操作:
   *      - 打开、关闭、读取、写入文件时使用
   *      - 影响所有列族的文件 I/O 行为
   *
   *   3. 性能调优:
   *      - 通过调整文件选项优化 I/O 性能
   *      - 例如：增大缓冲区、启用预读等
   *
   * 生命周期:
   *   - 在 ColumnFamilySet 构造时初始化
   *   - 整个生命周期内不变（const）
   *   - 所有列族共享此配置
   *
   * 注意事项:
   *   - 此选项在 DB 打开时确定，后续不可更改
   *   - 所有列族使用相同的文件操作选项
   *   - 修改此选项需要重新打开数据库
   */
  const FileOptions file_options_;

  /**
   * dummy_cfd_ - 循环链表的哨兵节点
   *
   * 功能概述:
   *   - ColumnFamilyData 类型的哨兵节点
   *   - 用于构建双向循环链表，连接所有列族
   *   - 不代表真实的列族，仅用于链表结构
   *
   * 链表结构:
   *   - 哨兵节点形成双向循环链表
   *   - 链表遍历：dummy_cfd_->next_ → cfd1 → cfd2 → ... → dummy_cfd_
   *   - 所有真实列族都在 dummy_cfd_ 的 next_ 和 prev_ 之间
   *   - 当没有列族时：dummy_cfd_->next_ == dummy_cfd_
   *   - 当有一个列族时：dummy_cfd_->next_ == cfd1, cfd1->next_ == dummy_cfd_
   *
   * 使用场景:
   *   1. 迭代器实现:
   *      - begin() 返回 iterator(dummy_cfd_->next_)
   *      - end() 返回 iterator(dummy_cfd_)
   *      - 支持 for 循环遍历所有列族
   *
   *   2. 遍历所有列族:
   *      - Flush 所有列族的 MemTable
   *      - Compaction 调度
   *      - 统计和监控
   *      - 检查所有列族的状态
   *
   *   3. 添加列族:
   *      - 将新列族插入到 dummy_cfd_ 之后
   *      - 更新链表指针
   *
   *   4. 删除列族:
   *      - 从链表中移除列族
   *      - 更新前后节点的指针
   *
   * 设计优势:
   *   - 统一的迭代接口：begin() 和 end() 都有明确的语义
   *   - 简化边界条件处理：不需要检查 null
   *   - 高效的添加/删除：O(1) 时间复杂度
   *   - 支持双向遍历：可以通过 prev_ 向后遍历
   *
   * ID 特性:
   *   - dummy_cfd_ 的 ID 为 kDummyColumnFamilyDataId（特殊值）
   *   - 此 ID 不与任何真实列族冲突
   *   - 用于标识哨兵节点
   *
   * 生命周期:
   *   - 在 ColumnFamilySet 构造时创建
   *   - 初始化为循环链表（指向自己）
   *   - 在 ColumnFamilySet 析构时销毁
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下访问
   *
   * 注意事项:
   *   - dummy_cfd_ 不是真实的列族
   *   - 不应该访问 dummy_cfd_ 的列族数据（如名称、MemTable 等）
   *   - 只用于链表结构，不代表任何列族
   *   - 迭代时会在 dummy_cfd_ 处结束
   */
  ColumnFamilyData* dummy_cfd_;

  /**
   * default_cfd_cache_ - 默认列族的缓存
   *
   * 功能概述:
   *   - 缓存默认列族的 ColumnFamilyData 指针
   *   - 优化默认列族的访问性能
   *   - 避免每次都需要从 column_family_data_ 中查找
   *
   * 性能优化:
   *   - 大多数操作都针对默认列族
   *   - 直接返回指针，避免哈希查找
   *   - 提升高频操作的性能（Put、Get、Delete 等）
   *
   * 使用场景:
   *   1. 获取默认列族:
   *      - 调用 GetDefault()
   *      - 直接返回 default_cfd_cache_
   *
   *   2. WriteBatch 写入默认列族:
   *      - 列族 ID 为 0 时，直接返回缓存
   *   3. 读取默认列族:
   *      - 快速获取默认列族的指针
   *
   * 生命周期:
   *   - 初始为 nullptr
   *   - 在默认列族创建后初始化
   *   - 不随 ColumnFamilySet 析构而清理
   *
   * 所有权管理:
   *   - 不持有引用计数
   *   - 不负责清理 default_cfd_cache_
   *   - 默认列族始终存在，因此不需要引用计数
   *   - 默认列族的生命周期由 column_family_data_ 管理
   *
   * 初始化时机:
   *   - 在默认列族（ID 为 0）创建后设置
   *   - 通常在 DBImpl::Recover() 或 DBImpl::Open() 时
   *   - 不会在 ColumnFamilySet 构造时初始化（此时列族还未创建）
   *
   * 并发控制:
   *   - 读操作（GetDefault）是线程安全的
   *   - 写操作（初始化）需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 仅用于性能优化，不是必须的
   *   - 如果缓存失效，可以从 column_family_data_ 中重新查找
   *   - 默认列族的 ID 固定为 0，查找开销较小
   *   - 缓存在默认列族创建后立即设置，不会是 nullptr（正常情况下）
   */
  // We don't hold the refcount here, since default column family always exists
  // We are also not responsible for cleaning up default_cfd_cache_. This is
  // just a cache that makes common case (accessing default column family)
  // faster
  ColumnFamilyData* default_cfd_cache_;

  /**
   * db_name_ - 数据库名称（路径）
   *
   * 功能概述:
   *   - 存储数据库的名称或路径
   *   - 用于标识数据库实例
   *   - 用于构建文件路径（SSTable、WAL、MANIFEST 等）
   *
   * 使用场景:
   *   1. 文件路径构建:
   *      - WAL 文件：db_name_/LOG
   *      - MANIFEST 文件：db_name_/MANIFEST-xxxxx
   *      - SSTable 文件：db_name_/xxx.sst
   *
   *   2. 日志和错误信息:
   *   - 打印数据库名称
   *   - 标识日志来源
   *
   *   3. 创建列族目录:
   *   - 确保列族文件存储在正确的位置
   *
   * 生命周期:
   *   - 在 ColumnFamilySet 构造时初始化
   *   - 整个生命周期内不变（const）
   *
   * 注意事项:
   *   - 可以是相对路径或绝对路径
   *   - 路径分隔符使用系统特定的字符（如 '/' 或 '\'）
   *   - 此路径由 DBImpl 创建 ColumnFamilySet 时传入
   */
  const std::string db_name_;

  /**
   * db_options_ - 不可变数据库选项
   *
   * 功能概述:
   *   - 指向数据库的不可变选项（ImmutableDBOptions）
   *   - 包含数据库级别的配置参数
   *   - 这些选项在数据库打开后不可更改
   *
   * 包含的选项:
   *   - 环境配置（Env）
   *   - 压缩选项
   *   - 统计信息配置
   *   - 并发配置（max_background_jobs 等）
   *   - WAL 配置
   *   - 缓存配置
   *   - 前缀布隆过滤器配置
   *   - 允许的操作配置（如 allow_2pc）
   *   - 数据库 ID 和会话 ID
   *   - 其他数据库级别选项
   *
   * 使用场景:
   *   1. 创建列族时:
   *      - 使用 db_options_ 初始化列族
   *      - 影响列族的不可变选项
   *
   *   2. 环境操作:
   *      - 访问 Env 对象（文件系统操作）
   *      - 创建线程池
   *
   *   3. 配置查询:
   *      - 获取数据库级别的配置
   *      - 检查是否允许某些操作
   *
   *   4. 统计信息:
   *   - 记录和查询数据库统计
   *   - 性能分析
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilySet 仅保存引用（const）
   *   - 必须保证实例生命周期长于此指针
   *
   * 线程安全性:
   *   - ImmutableDBOptions 本身是线程安全的（不可变）
   *   - 多个线程可以同时读取
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - 指针指向的对象在数据库生命周期内不变
   *   - 修改数据库选项需要重新打开数据库
   */
  const ImmutableDBOptions* const db_options_;

  /**
   * table_cache_ - SSTable 缓存
   *
   * 功能概述:
   *   - 指向 SSTable 的缓存对象（TableCache）
   *   - 所有列族共享此缓存
   *   - 缓存已打开的 SSTable 文件和索引
   *
   * 缓存内容:
   *   - SSTable 文件描述符
   *   - SSTable 索引
   *   - SSTable 布隆过滤器
   *   - SSTable 块缓存引用
   *
   * 使用场景:
   *   1. 读取操作:
   *      - 查询 SSTable 文件时先检查缓存
   *   2. Compaction:
   *   - 合并 SSTable 文件时使用缓存
   *
   *   3. 多列族共享:
   *      - 所有列族的 SSTable 都缓存在此处
   *      - 提高缓存利用率
   *
   * 性能优化:
   *   - 避免重复打开 SSTable 文件
   *   - 减少磁盘 I/O
   *   - 提高查询性能
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilySet 仅保存引用
   *   - 必须保证实例生命周期长于此指针
   *
   * 线程安全性:
   *   - TableCache 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - 缓存大小由 block_cache 配置控制
   *   - 缓存会根据 LRU 策略淘汰
   */
  Cache* table_cache_;

  /**
   * write_buffer_manager_ - 写入缓冲区管理器
   *
   * 功能概述:
   *   - 指向写入缓冲区管理器（WriteBufferManager）
   *   - 管理所有列族的 MemTable 和不可变 MemTable 的内存使用
   *   - 控制总内存使用量，防止内存溢出
   *
   * 管理的内存:
   *   - 活跃 MemTable（可变 MemTable）
   *   - 不可变 MemTable（等待 Flush 的 MemTable）
   *   - MemTable 的索引和数据
   *
   * 功能:
   *   - 跟踪所有列族的 MemTable 内存使用
   *   - 当内存使用超过限制时，触发 Flush
   *   - 控制写入速率（通过延迟写入）
   *   - 支持内存配额管理
   *
   * 使用场景:
   *   1. 创建 MemTable:
   *      - 向 write_buffer_manager_ 注册 MemTable
   *   2. 删除 MemTable:
   *   - 从 write_buffer_manager_ 注销 MemTable
   *
   *   3. 内存使用检查:
   *   - 定期检查内存使用量
   *   - 决定是否需要 Flush
   *
   *   4. 写入控制:
   *   - 内存紧张时延迟写入
   *   - 降低写入速率
   *
   * 配置:
   *   - 由 db_options.write_buffer_manager 控制
   *   - 可以设置内存上限
   *   - 可以设置缓存大小配额
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilySet 仅保存引用
   *   - 必须保证实例生命周期长于此指针
   *
   * 线程安全性:
   *   - WriteBufferManager 内部有锁和原子变量
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - 所有列族共享此管理器
   *   - MemTable 必须正确注册和注销
   */
  WriteBufferManager* write_buffer_manager_;

  /**
   * write_controller_ - 写入控制器
   *
   * 功能概述:
   *   - 指向写入控制器（WriteController）
   *   - 控制数据库的写入速率
   *   - 防止系统过载，保证稳定性
   *
   * 控制的场景:
   *   1. MemTable 满:
   *   - 限制写入速率
   *   - 等待 Flush 完成
   *
   *   2. Level 0 文件过多:
   *   - 限制写入速率
   *   - 等待 Compaction 完成
   *
   *   3. 磁盘空间不足:
   *   - 限制写入速率
   *   - 等待空间释放
   *
   *   4. 手动暂停写入:
   *   - 用户请求暂停写入
   *   - 等待用户恢复
   *
   * 功能:
   *   - 延迟写入（DelayWrites）
   *   - 停止写入（StopWrites）
   *   - 获取和释放令牌（Token）
   *   - 设置写入停止条件
   *
   * 使用场景:
   *   1. MemTable 满:
   *   - 写入时检查是否需要延迟
   *   - 必要时等待
   *
   *   2. Compaction 排队:
   *   - Compaction 太慢时限制写入
   *   - 防止写入速度超过处理速度
   *
   *   3. 磁盘监控:
   *   - 磁盘空间不足时停止写入
   *   - 保护系统稳定性
   *
   * 令牌机制:
   *   - 外部组件可以获取令牌
   *   - 有令牌时，写入不会停止
   *   - 没有令牌时，写入可能被停止
   *   - 用于实现细粒度的写入控制
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilySet 仅保存引用
   *   - 必须保证实例生命周期长于此指针
   *
   * 线程安全性:
   *   - WriteController 内部有锁和条件变量
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - 所有列族共享此控制器
   *   - 控制器会影响所有写入操作
   */
  WriteController* write_controller_;

  /**
   * block_cache_tracer_ - Block Cache 追踪器
   *
   * 功能概述:
   *   - 指向 Block Cache 追踪器（BlockCacheTracer）
   *   - 追踪 Block Cache 的访问情况
   *   - 用于性能分析和诊断
   *
   * 追踪内容:
   *   - Block 的访问次数
   *   - Block 的访问时间戳
   *   - Block 的缓存命中/未命中情况
   *   - Block 的来源（SSTable 文件）
   *   - 访问的列族和表
   *
   * 使用场景:
   *   1. 性能分析:
   *   - 分析 Block Cache 的使用情况
   *   - 找出热点数据
   *
   *   2. 缓存优化:
   *   - 根据访问模式调整缓存大小
   *   - 优化缓存策略
   *
   *   3. 问题诊断:
   *   - 诊断查询性能问题
   *   - 找出性能瓶颈
   *
   *   4. 监控:
   *   - 实时监控 Cache 命中率
   *   - 统计 Cache 使用情况
   *
   * 配置:
   *   - 由 db_options.block_cache_tracer 控制
   *   - 可以启用或禁用
   *   - 可以设置追踪的采样率
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilySet 仅保存引用
   *   - 必须保证实例生命周期长于此指针
   *
   * 线程安全性:
   *   - BlockCacheTracer 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - 可能为 nullptr（未启用追踪）
   *   - 追踪功能会增加少量性能开销
   */
  BlockCacheTracer* const block_cache_tracer_;

  /**
   * io_tracer_ - IO 追踪器
   *
   * 功能概述:
   *   - 共享指针指向 IO 追踪器（IOTracer）
   *   - 追踪数据库的 I/O 操作
   *   - 用于性能分析和诊断
   *
   * 追踪内容:
   *   - 文件读取操作
   *   - 文件写入操作
   *   - 随机读取和顺序读取
   *   - I/O 操作的大小和时间
   *   - I/O 操作的来源（Compaction、Flush、查询等）
   *
   * 使用场景:
   *   1. 性能分析:
   *   - 分析 I/O 操作的分布
   *   - 找出 I/O 热点
   *
   *   2. 系统调优:
   *   - 根据访问模式调整文件系统配置
   *   - 优化 I/O 性能
   *
   *   3. 问题诊断:
   *   - 诊断 I/O 性能问题
   *   - 找出 I/O 瓶颈
   *
   *   4. 监控:
   *   - 实时监控 I/O 情况
   *   - 统计 I/O 量
   *
   * 配置:
   *   - 由 db_options.io_tracer 控制
   *   - 可以启用或禁用
   *   - 可以设置追踪的详细程度
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - 使用共享指针管理（shared_ptr）
   *   - 可以被多个对象共享
   *
   * 线程安全性:
   *   - IOTracer 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 使用共享指针管理，生命周期由引用计数控制
   *   - 可能为空（shared_ptr 默认构造）
   *   - 追踪功能会增加少量性能开销
   */
  std::shared_ptr<IOTracer> io_tracer_;

  /**
   * db_id_ - 数据库 ID（引用）
   *
   * 功能概述:
   *   - 数据库的唯一标识符
   *   - 用于区分不同的数据库实例
   *   - 引用类型，不转移所有权
   *
   * 格式:
   *   - 通常是字符串格式
   *   - 可以包含时间戳、随机数等
   *
   * 使用场景:
   *   1. 日志记录:
   *   - 在日志中标记数据库 ID
   *   - 区分不同数据库的日志
   *
   *   2. 元数据:
   *   - 写入 MANIFEST 等元数据文件
   *   - 标识数据的来源
   *
   *   3. 监控和诊断:
   *   - 标识数据库实例
   *   - 追踪数据库操作
   *
   *   4. 集群环境:
   *   - 在分布式环境中标识数据库
   *   - 防止数据库混淆
   *
   * 生成时机:
   *   - 数据库首次打开时生成
   *   - 从 MANIFEST 中恢复（已存在的数据库）
   *   - 可以通过 db_options 指定
   *
   * 生命周期:
   *   - 数据库打开时设置
   *   - 整个生命周期内不变
   *   - 由 DBImpl 拥有和管理
   *
   * 线程安全性:
   *   - 只读变量，线程安全
   *
   * 注意事项:
   *   - 引用类型，不转移所有权
   *   - 必须保证被引用对象的生命周期有效
   *   - 数据库关闭后，此 ID 仍然有效（但不应再使用）
   *   - 不能修改此 ID
   */
  const std::string& db_id_;

  /**
   * db_session_id_ - 数据库会话 ID
   *
   * 功能概述:
   *   - 数据库会话的唯一标识符
   *   - 用于区分数据库的不同会话（实例）
   *   - 每次打开数据库时生成新的会话 ID
   *
   * 与 db_id_ 的区别:
   *   - db_id_: 数据库的唯一标识，数据库生命周期内不变
   *   - db_session_id_: 会话的唯一标识，每次打开数据库时改变
   *   - db_id_ 用于标识数据库实例
   *   - db_session_id_ 用于标识数据库的打开会话
   *
   * 格式:
   *   - 通常是字符串格式
   *   - 可以包含时间戳、随机数、进程 ID 等
   *
   * 使用场景:
   *   1. 日志记录:
   *   - 在日志中标记会话 ID
   *   - 区分同一数据库的不同会话
   *
   *   2. 故障恢复:
   *   - 检测数据库是否被重新打开
   *   - 区分崩溃恢复和正常关闭
   *
   *   3. 监控和诊断:
   *   - 追踪数据库的打开/关闭
   *   - 分析数据库的使用模式
   *
   *   4. 元数据:
   *   - 写入某些元数据文件
   *   - 标识操作的会话
   *
   * 生成时机:
   *   - 数据库打开时生成
   *   - 包含时间戳、随机数等信息
   *   - 每次打开数据库时都会生成新的 ID
   *
   * 生命周期:
   *   - 数据库打开时生成
   *   - 数据库关闭后失效
   *   - 由 ColumnFamilySet 拥有和管理
   *
   * 线程安全性:
   *   - 只读变量，线程安全
   *
   * 注意事项:
   *   - 每次打开数据库时都会改变
   *   - 与 db_id_ 不同，db_session_id_ 是可变的
   *   - 用于标识会话，不是标识数据库
   *   - 不能修改此 ID（仅在构造时初始化）
   */
  std::string db_session_id_;
};

// A wrapper for ColumnFamilySet that supports releasing DB mutex during each
// iteration over the iterator, because the cfd is Refed and Unrefed during
// each iteration to prevent concurrent CF drop from destroying it (until
// Unref).
class RefedColumnFamilySet {
 public:
  explicit RefedColumnFamilySet(ColumnFamilySet* cfs) : wrapped_(cfs) {}

  class iterator {
   public:
    explicit iterator(ColumnFamilySet::iterator wrapped) : wrapped_(wrapped) {
      MaybeRef(*wrapped_);
    }
    ~iterator() { MaybeUnref(*wrapped_); }
    inline void MaybeRef(ColumnFamilyData* cfd) {
      if (cfd->GetID() != ColumnFamilyData::kDummyColumnFamilyDataId) {
        cfd->Ref();
      }
    }
    inline void MaybeUnref(ColumnFamilyData* cfd) {
      if (cfd->GetID() != ColumnFamilyData::kDummyColumnFamilyDataId) {
        cfd->UnrefAndTryDelete();
      }
    }
    // NOTE: minimum operators for for-loop iteration
    inline iterator& operator++() {
      ColumnFamilyData* old = *wrapped_;
      ++wrapped_;
      // Can only unref & potentially free cfd after accessing its next_
      MaybeUnref(old);
      MaybeRef(*wrapped_);
      return *this;
    }
    inline bool operator!=(const iterator& other) const {
      return this->wrapped_ != other.wrapped_;
    }
    inline ColumnFamilyData* operator*() { return *wrapped_; }

   private:
    ColumnFamilySet::iterator wrapped_;
  };

  iterator begin() { return iterator(wrapped_->begin()); }
  iterator end() { return iterator(wrapped_->end()); }

 private:
  ColumnFamilySet* wrapped_;
};

// We use ColumnFamilyMemTablesImpl to provide WriteBatch a way to access
// memtables of different column families (specified by ID in the write batch)
/**
 * ColumnFamilyMemTablesImpl - 列族 MemTable 查找和管理实现
 *
 * 功能概述:
 *   - 提供跨多个列族的 MemTable 访问接口
 *   - 支持通过列族 ID 快速查找对应的 MemTable
 *   - 管理当前选中的列族及其相关资源（MemTable、日志号等）
 *   - 为 WriteBatch 写入过程提供列族到 MemTable 的映射服务
 *
 * 设计目的:
 *   - RocksDB 支持多个列族（Column Family），每个列族有独立的 MemTable
 *   - WriteBatch 可以包含多个列族的写入操作
 *   - 插入到 MemTable 时，需要根据列族 ID 找到对应的 MemTable
 *   - 此类封装了列族查找逻辑，简化 WriteBatch 的处理流程
 *
 * 使用场景:
 *   1. WriteBatch 写入到 MemTable:
 *      - WriteBatch 可能包含多个列族的 Put/Delete/Merge 操作
 *      - 遍历 WriteBatch 时，对每个操作调用 Seek(column_family_id)
 *      - 找到对应的 MemTable 后，执行插入操作
 *
 *   2. WAL 日志恢复:
 *      - 重放 WAL 日志时，需要将操作恢复到对应的列族 MemTable
 *      - 使用此类快速查找目标列族
 *
 *   3. 迭代器遍历:
 *      - 遍历多个列族的数据时，需要动态切换列族上下文
 *
 * 核心成员:
 *   - column_family_set_: 列族集合（包含所有列族的元数据）
 *   - current_: 当前选中的列族数据（ColumnFamilyData*）
 *   - handle_: 当前列族的内部句柄（用于返回给调用者）
 *
 * 线程安全性:
 *   - 所有公共方法都需要在 DB 互斥锁保护下调用，或从写线程调用
 *   - current_ 成员是可变的，但在互斥锁保护下安全
 *   - Seek() 方法会修改 current_，不能与其他线程的 Seek() 并发调用
 *
 * 使用示例:
 *
 *   // 示例1: WriteBatch 写入到 MemTable
 *   ColumnFamilyMemTablesImpl cf_mems(column_family_set);
 *   WriteBatch batch;
 *   // ... 添加 Put/Delete 操作到 batch ...
 *
 *   // 遍历 batch 中的每个操作
 *   for (auto& entry : batch) {
 *     uint32_t cf_id = entry.column_family;
 *
 *     // 查找对应的列族 MemTable
 *     if (cf_mems.Seek(cf_id)) {
 *       MemTable* mem = cf_mems.GetMemTable();
 *       // 将操作插入到 mem
 *       mem->Add(entry.sequence, entry.type, entry.key, entry.value);
 *     } else {
 *       // 列族不存在，忽略或报错
 *     }
 *   }
 *
 *   // 示例2: 获取列族日志号
 *   if (cf_mems.Seek(cf_id)) {
 *     uint64_t log_num = cf_mems.GetLogNumber();
 *     printf("Column family %u log number: %lu\n", cf_id, log_num);
 *   }
 *
 *   // 示例3: 获取列族句柄
 *   if (cf_mems.Seek(cf_id)) {
 *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
 *     // 使用句柄进行后续操作
 *   }
 *
 * 性能优化:
 *   - Seek(column_family_id == 0) 快速路径：直接获取默认列族
 *   - 避免频繁的列族查找：批量处理同一列族的多个操作
 *   - 使用 ColumnFamilyHandleInternal 避免不必要的对象创建
 *
 * 错误处理:
 *   - Seek() 返回 false 表示列族不存在
 *   - 其他方法（GetMemTable、GetLogNumber）依赖 Seek() 成功
 *   - 如果 Seek() 未成功就调用其他方法，会触发 assert
 *
 * 相关类:
 *   - ColumnFamilyMemTables: 抽象基类（接口定义）
 *   - ColumnFamilyMemTablesDefault: 默认实现（单列族场景）
 *   - ColumnFamilySet: 列族集合（管理所有列族）
 *   - ColumnFamilyData: 列族数据结构（包含 MemTable）
 *   - ColumnFamilyHandleInternal: 列族内部句柄
 *
 * 注意事项:
 *   1. 必须在 DB 互斥锁保护下使用所有方法
 *   2. Seek() 会修改内部状态（current_），不能并发调用
 *   3. Seek() 必须在调用 GetMemTable()、GetLogNumber() 之前调用
 *   4. 调用者需要检查 Seek() 的返回值，确保列族存在
 */
class ColumnFamilyMemTablesImpl : public ColumnFamilyMemTables {
 public:
  /**
   * 构造函数 - 使用列族集合创建实例
   *
   * 功能概述:
   *   - 初始化 ColumnFamilyMemTablesImpl 实例
   *   - 保存列族集合的引用（不拥有所有权）
   *   - 初始化当前列族为 nullptr（需要先调用 Seek）
   *
   * 参数说明:
   *   @param column_family_set: 列族集合指针，包含数据库的所有列族
   *                           调用者需要保证此指针在整个实例生命周期内有效
   *                           不转移所有权，仅保存引用
   *
   * 初始化状态:
   *   - column_family_set_: 指向列族集合
   *   - current_: nullptr（未选中任何列族）
   *   - handle_: 空句柄（内部 cfd_ 为 nullptr）
   *
   * 使用场景:
   *   - DBImpl 初始化时创建此实例
   *   - 用于 WriteBatch 写入到 MemTable
   *   - 用于 WAL 日志恢复
   *
   * 示例代码:
   *   ColumnFamilySet* cf_set = db_impl->GetColumnFamilySet();
   *   ColumnFamilyMemTablesImpl cf_mems(cf_set);
   *
   *   // 需要先调用 Select 才能使用其他方法
   *   if (cf_mems.Seek(default_cf_id)) {
   *     MemTable* mem = cf_mems.GetMemTable();
   *     // 使用 mem...
   *   }
   *
   * 注意事项:
   *   - column_family_set 必须非空
   *   - column_family_set 的生命周期必须长于此实例
   *   - 构造后需要调用 Seek() 才能使用其他方法
   */
  explicit ColumnFamilyMemTablesImpl(ColumnFamilySet* column_family_set)
      : column_family_set_(column_family_set), current_(nullptr) {}

  /**
   * 拷贝构造函数 - 从另一个实例创建等价实例
   *
   * 功能概述:
   *   - 创建一个新的 ColumnFamilyMemTablesImpl 实例
   *   - 与原实例共享同一个列族集合
   *   - 重置当前列族为 nullptr（独立状态）
   *
   * 参数说明:
   *   @param orig: 原实例指针，用于复制其列族集合引用
   *               不会复制 current_ 状态（新实例的 current_ 为 nullptr）
   *
   * 使用场景:
   *   - 需要创建独立的列族 MemTable 查找器
   *   - 多个线程需要独立的列族查找上下文
   *   - 临时创建一个查找器用于批量操作
   *
   * 设计考虑:
   *   - 共享列族集合：避免重复存储列族信息
   *   - 独立当前状态：不同实例可以独立查找不同列族
   *   - 不复制 current_：新实例需要重新调用 Seek()
   *
   * 示例代码:
   *   ColumnFamilyMemTablesImpl cf_mems1(cf_set);
   *   cf_mems1.Seek(cf_id1);
   *
   *   // 创建第二个实例，独立于 cf_mems1
   *   ColumnFamilyMemTablesImpl cf_mems2(&cf_mems1);
   *   cf_mems2.Seek(cf_id2);  // 不会影响 cf_mems1 的 current_
   *
   * 注意事项:
   *   - orig 必须非空
   *   - 新实例的 current_ 初始化为 nullptr
   *   - 新实例与原实例共享列族集合（线程安全由外部保证）
   */
  // Constructs a ColumnFamilyMemTablesImpl equivalent to one constructed
  // with the arguments used to construct *orig.
  explicit ColumnFamilyMemTablesImpl(ColumnFamilyMemTablesImpl* orig)
      : column_family_set_(orig->column_family_set_), current_(nullptr) {}

  /**
   * Seek - 查找并选中指定的列族
   *
   * 功能概述:
   *   - 根据列族 ID 在列族集合中查找对应的列族
   *   - 更新内部状态 current_ 指向找到的列族数据
   *   - 更新内部句柄 handle_ 指向当前列族
   *   - 支持默认列族的快速路径优化
   *
   * 查找逻辑:
   *   1. 如果 column_family_id == 0:
   *      - 直接获取默认列族（GetDefault()）
   *      - 这是常见的优化路径，因为大多数操作针对默认列族
   *
   *   2. 如果 column_family_id != 0:
   *      - 在列族集合中查找指定 ID 的列族（GetColumnFamily()）
   *      - 如果列族不存在，返回 nullptr
   *
   *   3. 更新内部句柄:
   *      - 设置 handle_.internal_cfd_ = current_
   *      - 用于 GetColumnFamilyHandle() 返回
   *
   * 参数说明:
   *   @param column_family_id: 要查找的列族 ID
   *                           0 表示默认列族（常见优化）
   *                           非 0 表示指定的列族 ID
   *
   * 返回值:
   *   - true: 成功找到列族，current_ 已更新
   *   - false: 列族不存在，current_ 为 nullptr
   *
   * 调用要求:
   *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
   *   - 不能与其他线程的 Seek() 并发调用（会修改 current_）
   *   - 调用后必须检查返回值，确保列族存在
   *
   * 使用示例:
   *   // 示例1: 查找默认列族
   *   if (cf_mems.Seek(0)) {
   *     MemTable* mem = cf_mems.GetMemTable();
   *     // 使用 mem...
   *   }
   *
   *   // 示例2: 查找指定列族
   *   uint32_t cf_id = GetColumnFamilyID(handle);
   *   if (cf_mems.Seek(cf_id)) {
   *     MemTable* mem = cf_mems.GetMemTable();
   *     uint64_t log_num = cf_mems.GetLogNumber();
   *     // 使用 mem 和 log_num...
   *   } else {
   *     LOG(ERROR) << "Column family " << cf_id << " not found";
   *   }
   *
   * 性能优化:
   *   - column_family_id == 0 的快速路径：直接获取默认列族，避免查找
   *   - 减少重复查找：批量处理同一列族的多个操作时，只调用一次 Seek()
   *
   * 错误处理:
   *   - 列族不存在时返回 false
   *   - 后续方法（GetMemTable、GetLogNumber）会 assert(current_ != nullptr)
   *   - 调用者必须检查返回值
   *
   * 并发控制:
   *   - 必须在 DB 互斥锁保护下调用
   *   - 不能与其他线程的 Seek() 并发调用
   *   - 线程间的其他方法调用（GetMemTable 等）也需要互斥锁保护
   *
   * 相关方法:
   *   - GetMemTable(): 需要先调用 Seek()
   *   - GetLogNumber(): 需要先调用 Seek()
   *   - GetColumnFamilyHandle(): 需要先调用 Seek()
   */
  // sets current_ to ColumnFamilyData with column_family_id
  // returns false if column family doesn't exist
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  bool Seek(uint32_t column_family_id) override;

  /**
   * GetLogNumber - 获取当前选中列族的日志号
   *
   * 功能概述:
   *   - 返回当前列族的 WAL 日志号
   *   - 日志号表示该列族数据关联的 WAL 文件编号
   *   - 用于判断写入是否已经持久化到磁盘
   *
   * 返回值:
   *   - 当前列族的日志号（uint64_t）
   *
   * 调用要求:
   *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
   *   - REQUIRES: 必须先调用 Seek() 并返回 true
   *   - 如果 current_ 为 nullptr，会触发 assert
   *
   * 使用场景:
   *   1. WAL 恢复时判断操作是否已经处理:
   *      - 如果当前日志号 >= 操作的日志号，说明操作已经处理过
   *      - 可以跳过该操作，避免重复写入
   *
   *   2. 日志文件切换时更新:
   *      - 当 WAL 文件切换时，更新列族的日志号
   *      - 用于跟踪每个列族的写入进度
   *
   *   3. 列族 Flush 管理:
   *      - 在 Flush 时，检查日志号以确定哪些数据需要持久化
   *
   * 示例代码:
   *   // WAL 恢复时的使用
   *   uint64_t wal_log_number = wal_reader->GetLogNumber();
   *
   *   if (cf_mems.Seek(cf_id)) {
   *     uint64_t cf_log_number = cf_mems.GetLogNumber();
   *     if (cf_log_number >= wal_log_number) {
   *       // 操作已经处理过，跳过
   *       continue;
   *     }
   *     // 将操作插入到 MemTable...
   *   }
   *
   * 实现细节:
   *   - 直接调用 current_->GetLogNumber()
   *   - current_ 是 ColumnFamilyData 指针
   *   - ColumnFamilyData 内部维护日志号状态
   *
   * 错误处理:
   *   - 如果 current_ 为 nullptr（Seek 未成功），会触发 assert
   *   - 调用者必须确保 Seek() 返回 true
   *
   * 并发控制:
   *   - 必须在 DB 互斥锁保护下调用
   *   - 与其他方法（Seek、GetMemTable 等）共享互斥锁
   */
  // Returns log number of the selected column family
  // REQUIRES: under a DB mutex OR from a write thread
  uint64_t GetLogNumber() const override;

  /**
   * GetMemTable - 获取当前选中列族的 MemTable
   *
   * 功能概述:
   *   - 返回当前列族的活跃 MemTable 指针
   *   - MemTable 用于存储最近的写入操作（未持久化到磁盘）
   *   - 调用者可以使用此指针进行数据的插入、查询等操作
   *
   * 返回值:
   *   - 当前列族的 MemTable 指针（MemTable*）
   *   - 指针由 ColumnFamilyData 拥有，调用者不应释放
   *
   * 调用要求:
   *   - REQUIRES: 必须先调用 Seek() 并返回 true
   *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
   *   - 如果 current_ 为 nullptr，会触发 assert
   *
   * 使用场景:
   *   1. WriteBatch 写入到 MemTable:
   *      - 遍历 WriteBatch 中的每个操作
   *      - 对每个操作调用 GetMemTable() 获取目标 MemTable
   *      - 调用 MemTable->Add() 插入数据
   *
   *   2. 数据查询:
   *      - 查询 MemTable 中的数据
   *   3. 统计和监控:
   *      - 获取 MemTable 的大小、条目数等信息
   *
   * 示例代码:
   *   // WriteBatch 写入到 MemTable
   *   for (auto& entry : batch) {
   *     uint32_t cf_id = entry.column_family;
   *
   *     if (cf_mems.Seek(cf_id)) {
   *       MemTable* mem = cf_mems.GetMemTable();
   *
   *       // 插入到 MemTable
   *       mem->Add(entry.sequence, entry.type, entry.key, entry.value);
   *     }
   *   }
   *
   * 实现细节:
   *   - 直接调用 current_->mem()
   *   - ColumnFamilyData 内部维护活跃 MemTable 的引用
   *   - MemTable 的生命周期由 ColumnFamilyData 管理
   *
   * MemTable 特性:
   *   - 存储格式：跳表（SkipList）
   *   - 支持并发写入（使用 mutex 或无锁数据结构）
   *   - 满时会切换到不可变 MemTable（Immutable MemTable）
   *   - 不可变 MemTable 会被后台线程 Flush 到磁盘
   *
   * 错误处理:
   *   - 如果 current_ 为 nullptr（Seek 未成功），会触发 assert
   *   - 调用者必须确保 Seek() 返回 true
   *   - 返回的 MemTable 指针可能为 nullptr（异常情况）
   *
   * 并发控制:
   *   - 必须在 DB 互斥锁保护下调用
   *   - MemTable 本身支持并发写入（有自己的锁机制）
   *   - 调用者不应在持有 DB 互斥锁的情况下长时间操作 MemTable
   *
   * 性能考虑:
   *   - 批量操作同一列族时，可以多次调用 GetMemTable()（开销小）
   *   - 避免频繁调用 Seek() 和 GetMemTable()（优化路径）
   *   - MemTable 插入是内存操作，速度快
   */
  // REQUIRES: Seek() called first
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual MemTable* GetMemTable() const override;

  /**
   * GetColumnFamilyHandle - 获取当前选中列族的句柄
   *
   * 功能概述:
   *   - 返回当前列族的内部句柄（ColumnFamilyHandleInternal*）
   *   - 句柄可以用于后续的列族操作（如读取、写入）
   *   - 句柄的生命周期与 ColumnFamilyMemTablesImpl 实例绑定
   *
   * 返回值:
   *   - 当前列族的句柄（ColumnFamilyHandle*）
   *   - 指向内部的 handle_ 成员
   *   - 调用者不应释放此指针
   *
   * 调用要求:
   *   - REQUIRES: 必须先调用 Seek() 并返回 true
   *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
   *   - 如果 current_ 为 nullptr，会触发 assert
   *
   * 使用场景:
   *   1. 传递给需要 ColumnFamilyHandle 的 API:
   *      - 某些函数需要 ColumnFamilyHandle 作为参数
   *      - 使用此方法获取句柄并传递
   *
   *   2. 获取列族元数据:
   *      - 通过句柄获取列族 ID、比较器等
   *
   *   3. 事务操作:
   *      - 在事务中使用列族句柄指定目标列族
   *
   * 示例代码:
   *   // 示例1: 获取列族 ID
   *   if (cf_mems.Seek(cf_id)) {
   *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
   *     uint32_t id = handle->GetID();
   *     printf("Column family ID: %u\n", id);
   *   }
   *
   *   // 示例2: 用于 ReadOptions
   *   if (cf_mems.Seek(cf_id)) {
   *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
   *     ReadOptions opts;
   *     auto iter = db->NewIterator(opts, handle);
   *     // 使用迭代器...
   *   }
   *
   * 实现细节:
   *   - 返回内部成员 handle_ 的指针
   *   - handle_ 是 ColumnFamilyHandleInternal 类型
   *   - handle_.internal_cfd_ 在 Seek() 时被设置为 current_
   *   - handle_ 的生命周期与 ColumnFamilyMemTablesImpl 实例绑定
   *
   * ColumnFamilyHandleInternal 特性:
   *   - 继承自 ColumnFamilyHandleImpl
   *   - 支持动态设置关联的 ColumnFamilyData（SetCFD）
   *   - 用于内部场景，避免频繁创建新的句柄对象
   *   - 比标准的 ColumnFamilyHandle 更轻量级
   *
   * 与标准句柄的区别:
   *   - 标准句柄（ColumnFamilyHandleImpl）在创建时固定列族
   *   - 内部句柄（ColumnFamilyHandleInternal）可以动态切换列族
   *   - 内部句柄主要用于 DB 内部操作，不暴露给用户
   *
   * 错误处理:
   *   - 如果 current_ 为 nullptr（Seek 未成功），会触发 assert
   *   - 调用者必须确保 Seek() 返回 true
   *   - 返回的句柄可能指向无效数据（异常情况）
   *
   * 并发控制:
   *   - 必须在 DB 互斥锁保护下调用
   *   - 句柄本身不支持并发使用
   *   - 调用者不应在多线程间共享此句柄
   *
   * 注意事项:
   *   - 句柄的生命周期与 ColumnFamilyMemTablesImpl 实例绑定
   *   - 不要在实例销毁后使用返回的句柄
   *   - 调用 Seek() 后，句柄会更新为新的列族
   */
  // Returns column family handle for the selected column family
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() override;

  /**
   * current - 获取当前选中的列族数据（ColumnFamilyData）
   *
   * 功能概述:
   *   - 返回当前选中的列族数据结构指针
   *   - 提供对列族元数据的直接访问
   *   - 用于内部操作，需要直接访问 ColumnFamilyData 的情况
   *
   * 返回值:
   *   - 当前列族的 ColumnFamilyData 指针
   *   - 如果未调用 Seek() 或 Seek() 失败，返回 nullptr
   *
   * 调用要求:
   *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
   *   - 不能与其他线程的 Seek() 并发调用（会修改 current_）
   *
   * 使用场景:
   *   1. 直接访问列族元数据:
   *      - 获取列族名称、ID、比较器等
   *      - 检查列族状态（是否被删除、是否正在 Flush 等）
   *
   *   2. 内部操作:
   *   - DBImpl 内部某些操作需要直接访问 ColumnFamilyData
   *   - 用于版本管理、Compaction 等内部逻辑
   *
   *   3. 调试和诊断:
   *   - 打印列族信息
   *   - 检查列族状态
   *
   * 示例代码:
   *   // 检查列族状态
   *   if (cf_mems.Seek(cf_id)) {
   *     ColumnFamilyData* cfd = cf_mems.current();
   *     printf("Column family: %s, ID: %u\n",
   *            cfd->GetName().c_str(), cfd->GetID());
   *
   *     // 检查是否正在 Flush
   *     if (cfd->IsFlushScheduled()) {
   *       printf("Flush is scheduled\n");
   *     }
   *   }
   *
   * 实现细节:
   *   - 直接返回内部成员 current_
   *   - current_ 在 Seek() 时被设置
   *   - 无额外开销（直接返回指针）
   *
   * ColumnFamilyData 包含的信息:
   *   - 列族名称和 ID
   *   - MemTable 引用（活跃、不可变）
   *   - 版本信息（Version）
   *   - 比较器（Comparator）
   *   - 选项（ColumnFamilyOptions）
   *   - SuperVersion（版本缓存）
   *   - 统计信息（如写入字节数、条目数）
   *
   * 并发控制:
   *   - 必须在 DB 互斥锁保护下调用
   *   - 不能与其他线程的 Seek() 并发调用
   *   - 返回的指针在互斥锁保护下有效
   *   - 释放锁后，指针可能失效（列族可能被删除）
   *
   * 注意事项:
   *   - 返回的指针仅在持有 DB 互斥锁时有效
   *   - 不要在释放锁后使用此指针
   *   - 不要释放此指针（由 ColumnFamilySet 管理）
   *   - 调用 Seek() 后，current_ 会更新
   */
  // Cannot be called while another thread is calling Seek().
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  virtual ColumnFamilyData* current() override { return current_; }

 private:
  /**
   * column_family_set_ - 列族集合指针
   *
   * 功能概述:
   *   - 指向数据库的列族集合（ColumnFamilySet）
   *   - 包含数据库中所有列族的元数据和管理信息
   *   - 用于查找和访问列族
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - ColumnFamilyMemTablesImpl 仅保存引用
   *   - 必须保证实例生命周期长于此成员指针
   *
   * 使用场景:
   *   - Seek() 时在集合中查找列族
   *   - 获取默认列族（GetDefault()）
   *   - 获取指定 ID 的列族（GetColumnFamily()）
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 调用者需要保证指针有效
   *   - 线程安全由外部保证（DB 互斥锁）
   */
  ColumnFamilySet* column_family_set_;

  /**
   * current_ - 当前选中的列族数据
   *
   * 功能概述:
   *   - 指向当前选中的列族的 ColumnFamilyData
   *   - 在 Seek() 时被设置
   *   - 用于快速访问当前列族的 MemTable、日志号等
   *
   * 状态转换:
   *   - 初始化时：nullptr
   *   - Seek() 成功后：指向找到的 ColumnFamilyData
   *   - Seek() 失败后：nullptr
   *
   * 使用场景:
   *   - GetMemTable() 返回 current_->mem()
   *   - GetLogNumber() 返回 current_->GetLogNumber()
   *   - handle_.SetCFD(current_)
   *
   * 并发控制:
   *   - 只能在 DB 互斥锁保护下访问
   *   - Seek() 会修改此成员
   *   - 不能与其他线程的 Seek() 并发
   */
  ColumnFamilyData* current_;

  /**
   * handle_ - 当前列族的内部句柄
   *
   * 功能概述:
   *   - ColumnFamilyHandleInternal 类型的内部句柄
   *   - 用于返回给需要 ColumnFamilyHandle 的 API
   *   - 在 Seek() 时更新 internal_cfd_ 为 current_
   *
   * 优势:
   *   - 避免频繁创建新的句柄对象
   *   - 支持动态切换列族（通过 SetCFD）
   *   - 比标准句柄更轻量级
   *
   * 使用场景:
   *   - GetColumnFamilyHandle() 返回此句柄
   *   - 需要传递 ColumnFamilyHandle 的内部操作
   *
   * 生命周期:
   *   - 与 ColumnFamilyMemTablesImpl 实例绑定
   *   - 实例销毁时句柄失效
   */
  ColumnFamilyHandleInternal handle_;
};

extern uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family);

extern const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family);

}  // namespace ROCKSDB_NAMESPACE
