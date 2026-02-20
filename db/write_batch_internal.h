//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <array>
#include <vector>

#include "db/flush_scheduler.h"
#include "db/kv_checksum.h"
#include "db/trim_history_scheduler.h"
#include "db/write_thread.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/write_batch.h"
#include "util/autovector.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

class MemTable;
class FlushScheduler;
class ColumnFamilyData;

class ColumnFamilyMemTables {
 public:
  virtual ~ColumnFamilyMemTables() {}
  virtual bool Seek(uint32_t column_family_id) = 0;
  // returns true if the update to memtable should be ignored
  // (useful when recovering from log whose updates have already
  // been processed)
  virtual uint64_t GetLogNumber() const = 0;
  virtual MemTable* GetMemTable() const = 0;
  virtual ColumnFamilyHandle* GetColumnFamilyHandle() = 0;
  virtual ColumnFamilyData* current() { return nullptr; }
};

class ColumnFamilyMemTablesDefault : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesDefault(MemTable* mem)
      : ok_(false), mem_(mem) {}

  bool Seek(uint32_t column_family_id) override {
    ok_ = (column_family_id == 0);
    return ok_;
  }

  uint64_t GetLogNumber() const override { return 0; }

  MemTable* GetMemTable() const override {
    assert(ok_);
    return mem_;
  }

  ColumnFamilyHandle* GetColumnFamilyHandle() override { return nullptr; }

 private:
  bool ok_;
  MemTable* mem_;
};

struct WriteBatch::ProtectionInfo {
  // `WriteBatch` usually doesn't contain a huge number of keys so protecting
  // with a fixed, non-configurable eight bytes per key may work well enough.
  autovector<ProtectionInfoKVOC64> entries_;

  size_t GetBytesPerKey() const { return 8; }
};

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:
  // WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
  static constexpr size_t kHeader = 12;

  // WriteBatch methods with column_family_id instead of ColumnFamilyHandle*
  static Status Put(WriteBatch* batch, uint32_t column_family_id,
                    const Slice& key, const Slice& value);

  static Status Put(WriteBatch* batch, uint32_t column_family_id,
                    const SliceParts& key, const SliceParts& value);

  static Status PutEntity(WriteBatch* batch, uint32_t column_family_id,
                          const Slice& key, const WideColumns& columns);

  static Status Delete(WriteBatch* batch, uint32_t column_family_id,
                       const SliceParts& key);

  static Status Delete(WriteBatch* batch, uint32_t column_family_id,
                       const Slice& key);

  static Status SingleDelete(WriteBatch* batch, uint32_t column_family_id,
                             const SliceParts& key);

  static Status SingleDelete(WriteBatch* batch, uint32_t column_family_id,
                             const Slice& key);

  static Status DeleteRange(WriteBatch* b, uint32_t column_family_id,
                            const Slice& begin_key, const Slice& end_key);

  static Status DeleteRange(WriteBatch* b, uint32_t column_family_id,
                            const SliceParts& begin_key,
                            const SliceParts& end_key);

  static Status Merge(WriteBatch* batch, uint32_t column_family_id,
                      const Slice& key, const Slice& value);

  static Status Merge(WriteBatch* batch, uint32_t column_family_id,
                      const SliceParts& key, const SliceParts& value);

  static Status PutBlobIndex(WriteBatch* batch, uint32_t column_family_id,
                             const Slice& key, const Slice& value);

  static Status MarkEndPrepare(WriteBatch* batch, const Slice& xid,
                               const bool write_after_commit = true,
                               const bool unprepared_batch = false);

  static Status MarkRollback(WriteBatch* batch, const Slice& xid);

  static Status MarkCommit(WriteBatch* batch, const Slice& xid);

  static Status MarkCommitWithTimestamp(WriteBatch* batch, const Slice& xid,
                                        const Slice& commit_ts);

  static Status InsertNoop(WriteBatch* batch);

  // Return the number of entries in the batch.
  static uint32_t Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  static void SetCount(WriteBatch* batch, uint32_t n);

  // Return the sequence number for the start of this batch.
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // Returns the offset of the first entry in the batch.
  // This offset is only valid if the batch is not empty.
  static size_t GetFirstOffset(WriteBatch* batch);

  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  static Status SetContents(WriteBatch* batch, const Slice& contents);

  static Status CheckSlicePartsLength(const SliceParts& key,
                                      const SliceParts& value);

  // Inserts batches[i] into memtable, for i in 0..num_batches-1 inclusive.
  //
  // If ignore_missing_column_families == true. WriteBatch
  // referencing non-existing column family will be ignored.
  // If ignore_missing_column_families == false, processing of the
  // batches will be stopped if a reference is found to a non-existing
  // column family and InvalidArgument() will be returned.  The writes
  // in batches may be only partially applied at that point.
  //
  // If log_number is non-zero, the memtable will be updated only if
  // memtables->GetLogNumber() >= log_number.
  //
  // If flush_scheduler is non-null, it will be invoked if the memtable
  // should be flushed.
  //
  // Under concurrent use, the caller is responsible for making sure that
  // the memtables object itself is thread-local.
  static Status InsertInto(
      WriteThread::WriteGroup& write_group, SequenceNumber sequence,
      ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
      TrimHistoryScheduler* trim_history_scheduler,
      bool ignore_missing_column_families = false, uint64_t log_number = 0,
      DB* db = nullptr, bool concurrent_memtable_writes = false,
      bool seq_per_batch = false, bool batch_per_txn = true);

  // Convenience form of InsertInto when you have only one batch
  // next_seq returns the seq after last sequence number used in MemTable insert
  static Status InsertInto(
      const WriteBatch* batch, ColumnFamilyMemTables* memtables,
      FlushScheduler* flush_scheduler,
      TrimHistoryScheduler* trim_history_scheduler,
      bool ignore_missing_column_families = false, uint64_t log_number = 0,
      DB* db = nullptr, bool concurrent_memtable_writes = false,
      SequenceNumber* next_seq = nullptr, bool* has_valid_writes = nullptr,
      bool seq_per_batch = false, bool batch_per_txn = true);

  static Status InsertInto(WriteThread::Writer* writer, SequenceNumber sequence,
                           ColumnFamilyMemTables* memtables,
                           FlushScheduler* flush_scheduler,
                           TrimHistoryScheduler* trim_history_scheduler,
                           bool ignore_missing_column_families = false,
                           uint64_t log_number = 0, DB* db = nullptr,
                           bool concurrent_memtable_writes = false,
                           bool seq_per_batch = false, size_t batch_cnt = 0,
                           bool batch_per_txn = true,
                           bool hint_per_batch = false);

  // Appends src write batch to dst write batch and updates count in dst
  // write batch. Returns OK if the append is successful. Checks number of
  // checksum against count in dst and src write batches, and returns Corruption
  // if the count is inconsistent.
  static Status Append(WriteBatch* dst, const WriteBatch* src,
                       const bool WAL_only = false);

  // Returns the byte size of appending a WriteBatch with ByteSize
  // leftByteSize and a WriteBatch with ByteSize rightByteSize
  static size_t AppendedByteSize(size_t leftByteSize, size_t rightByteSize);

  // Iterate over [begin, end) range of a write batch
  static Status Iterate(const WriteBatch* wb, WriteBatch::Handler* handler,
                        size_t begin, size_t end);

  // This write batch includes the latest state that should be persisted. Such
  // state meant to be used only during recovery.
  static void SetAsLatestPersistentState(WriteBatch* b);
  static bool IsLatestPersistentState(const WriteBatch* b);

  static std::tuple<Status, uint32_t, size_t> GetColumnFamilyIdAndTimestampSize(
      WriteBatch* b, ColumnFamilyHandle* column_family);

  static bool TimestampsUpdateNeeded(const WriteBatch& wb) {
    return wb.needs_in_place_update_ts_;
  }

  static bool HasKeyWithTimestamp(const WriteBatch& wb) {
    return wb.has_key_with_ts_;
  }

  // Update per-key value protection information on this write batch.
  // If checksum is provided, the batch content is verfied against the checksum.
  static Status UpdateProtectionInfo(WriteBatch* wb, size_t bytes_per_key,
                                     uint64_t* checksum = nullptr);
};

// ============================================================================
// LocalSavePoint 类
//
// 功能概述:
//   LocalSavePoint 是一个类似"作用域守卫"（Scope Guard）的 RAII（资源获取即初始化）类。
//   用于在 WriteBatch 操作中创建保存点，以便在操作失败时回滚到保存点状态。
//   主要用于处理 max_bytes_ 限制，防止 WriteBatch 超过最大字节限制。
//
// 工作原理:
//   1. 构造时：记录当前 WriteBatch 的状态（大小、计数、标志位）
//   2. 执行操作：调用方可以添加数据到 WriteBatch
//   3. 提交时：检查是否超过 max_bytes_ 限制
//      - 如果超过：回滚到保存点的状态，返回 Status::MemoryLimit()
//      - 如果未超过：保持修改，返回 Status::OK()
//
// RAII 模式:
//   - 资源：WriteBatch 的状态（通过 SavePoint 记录）
//   - 获取：构造函数中记录状态
//   - 释放：commit() 函数中检查并可能回滚
//   - 保证：析构函数中验证 commit() 是否被调用（调试模式下）
//
// SavePoint 结构:
//   struct SavePoint {
//     size_t size;           // rep_ 的大小（字节数）
//     int count;             // 操作计数（Put/Delete/Merge 等的数量）
//     uint32_t content_flags; // 内容标志位（HAS_PUT、HAS_DELETE 等）
//   };
//
// 使用示例:
//   void WriteToBatch(WriteBatch* batch, const Slice& key, const Slice& value) {
//     LocalSavePoint save(batch);  // 创建保存点
//     WriteBatchInternal::Put(batch, 0, key, value);  // 添加数据
//     Status s = save.commit();   // 提交并检查限制
//     if (!s.ok()) {
//       // 处理错误（超过大小限制）
//       // batch 已回滚到保存点状态
//     }
//   }
// ============================================================================
// LocalSavePoint is similar to a scope guard
class LocalSavePoint {
 public:
  // 构造函数：创建保存点，记录当前 WriteBatch 的状态
  // 记录的状态包括：
  //   1. rep_ 的大小（GetDataSize()）
  //   2. 操作计数（Count()）
  //   3. 内容标志位（content_flags_）
  // 使用 relaxed 内存序读取 content_flags_，因为只是记录状态，不需要同步
  explicit LocalSavePoint(WriteBatch* batch)
      : batch_(batch),
        savepoint_(batch->GetDataSize(), batch->Count(),
                   batch->content_flags_.load(std::memory_order_relaxed))
#ifndef NDEBUG
        ,
        committed_(false)  // 调试模式下，记录是否已调用 commit()
#endif
  {
  }

#ifndef NDEBUG
  // 析构函数：验证 commit() 是否被调用
  // 如果 committed_ 为 false，表示未调用 commit()，触发断言失败
  // 这是调试检查，确保代码正确使用 LocalSavePoint
  ~LocalSavePoint() { assert(committed_); }
#endif

  // ============================================================================
  // LocalSavePoint::commit()
  //
  // 功能描述:
  //   提交保存点，检查 WriteBatch 是否超过 max_bytes_ 限制。
  //   如果超过限制，回滚到保存点的状态；否则，保持修改。
  //   这是 LocalSavePoint 的核心功能，实现"事务式"操作。
  //
  // 返回值:
  //   Status:
  //     - Status::OK(): 操作成功，WriteBatch 未超过 max_bytes_ 限制
  //     - Status::MemoryLimit(): 操作失败，WriteBatch 超过 max_bytes_ 限制，已回滚
  //
  // 实现逻辑:
  //   1. 设置已提交标志（调试模式下）
  //   2. 检查是否超过 max_bytes_ 限制
  //   3. 如果超过：回滚到保存点状态
  //   4. 如果未超过：保持修改
  //
  // 回滚过程（如果超过限制）:
  //   1. 恢复 rep_ 的大小：batch_->rep_.resize(savepoint_.size)
  //   2. 恢复操作计数：WriteBatchInternal::SetCount(batch_, savepoint_.count)
  //   3. 恢复保护信息（如果启用）：prot_info_->entries_.resize(savepoint_.count)
  //   4. 恢复内容标志位：content_flags_.store(savepoint_.content_flags)
  //
  // max_bytes_ 的含义:
  //   - max_bytes_ 是 WriteBatch 的最大允许字节数限制
  //   - 如果 max_bytes_ == 0，表示不限制大小
  //   - 如果 max_bytes_ != 0，则 rep_.size() 不能超过 max_bytes_
  //   - 用于防止单个批次过大，影响系统性能
  //
  // 保存点状态说明:
  //   - savepoint_.size: 保存点创建时 rep_ 的大小（字节数）
  //   - savepoint_.count: 保存点创建时的操作计数
  //   - savepoint_.content_flags: 保存点创建时的内容标志位
  //
  // 回滚的必要性:
  //   - 如果 WriteBatch 超过 max_bytes_ 限制，后续操作可能失败
  //   - 回滚确保 WriteBatch 保持一致的状态
  //   - 避免部分修改导致的数据不一致
  //
  // 性能考虑:
  //   - 正常情况（未超过限制）：只需要比较大小，O(1) 时间
  //   - 回滚情况：需要恢复多个状态，O(1) 时间（resize 和赋值）
  //   - 原子操作（content_flags_.store）使用 relaxed 内存序，开销小
  //
  // 线程安全性:
  //   - 不线程安全：假设单线程访问 WriteBatch
  //   - content_flags_ 使用原子操作，但整个操作不是原子的
  //   - 调用方负责确保单线程访问
  //
  // 调试检查:
  //   - NDEBUG 宏控制调试检查
  //   - 如果定义了 NDEBUG，调试检查被禁用（生产环境）
  //   - 如果未定义 NDEBUG，确保 commit() 被调用
  //
  // 使用示例:
  //
  //   示例 1: 正常情况（未超过限制）
  //   LocalSavePoint save(batch);
  //   WriteBatchInternal::Put(batch, 0, key, value);  // batch 大小未超过限制
  //   Status s = save.commit();  // 返回 Status::OK()
  //   // batch 保持修改
  //
  //   示例 2: 回滚情况（超过限制）
  //   LocalSavePoint save(batch);
  //   WriteBatchInternal::Put(batch, 0, large_key, large_value);  // batch 大小超过限制
  //   Status s = save.commit();  // 返回 Status::MemoryLimit()
  //   // batch 已回滚到保存点状态
  //
  //   示例 3: 使用 LocalSavePoint 保证一致性
  //   Status WriteWithLimit(WriteBatch* batch, const Slice& key, const Slice& value) {
  //     LocalSavePoint save(batch);
  //     WriteBatchInternal::Put(batch, 0, key, value);
  //     return save.commit();  // 自动检查并回滚（如果需要）
  //   }
  //
  // 注意事项:
  //   1. 必须调用 commit():
  //      - 调试模式下，未调用 commit() 会触发断言失败
  //      - 确保 commit() 在析构前被调用
  //
  //   2. max_bytes_ 的设置:
  //      - 如果 max_bytes_ == 0，表示不限制大小
  //      - 如果 max_bytes_ != 0，则检查大小限制
  //      - 合理设置 max_bytes_ 可以防止单个批次过大
  //
  //   3. 回滚的状态:
  //      - 回滚后，WriteBatch 恢复到保存点创建时的状态
  //      - 包括 rep_、计数、标志位、保护信息
  //      - 其他状态（如 save_points_ 栈）不受影响
  //
  //   4. 与 SetSavePoint 的区别:
  //      - LocalSavePoint 是临时的、局部的保存点
  //      - SetSavePoint 是全局的、持久的保存点
  //      - LocalSavePoint 用于 max_bytes_ 限制检查
  //      - SetSavePoint 用于事务回滚
  //
  //   5. 保护信息的处理:
  //      - 如果 prot_info_ != nullptr，需要回滚保护信息
  //      - prot_info_->entries_.resize(savepoint_.count)
  //      - 确保护息与操作计数一致
  //
  // 相关函数:
  //   - LocalSavePoint::LocalSavePoint(): 构造函数，创建保存点
  //   - LocalSavePoint::~LocalSavePoint(): 析构函数，验证 commit() 是否被调用
  //   - WriteBatchInternal::SetCount(): 设置操作计数
  //   - WriteBatch::GetDataSize(): 获取 rep_ 的大小
  //   - WriteBatch::Count(): 获取操作计数
  //
  // 实现细节:
  //   1. 设置已提交标志:
  //      - committed_ = true（调试模式下）
  //      - 确保析构函数的断言通过
  //
  //   2. 检查 max_bytes_ 限制:
  //      - 条件: batch_->max_bytes_ && batch_->rep_.size() > batch_->max_bytes_
  //      - max_bytes_ != 0: 启用大小限制
  //      - rep_.size() > max_bytes_: 超过限制
  //
  //   3. 回滚 rep_:
  //      - batch_->rep_.resize(savepoint_.size)
  //      - 调整 rep_ 的大小到保存点的大小
  //      - 删除超出部分的数据
  //
  //   4. 回滚计数:
  //      - WriteBatchInternal::SetCount(batch_, savepoint_.count)
  //      - 恢复操作计数到保存点的计数
  //
  //   5. 回滚保护信息:
  //      - prot_info_->entries_.resize(savepoint_.count)
  //      - 调整保护信息条目数量
  //      - 与操作计数保持一致
  //
  //   6. 回滚标志位:
  //      - content_flags_.store(savepoint_.content_flags, std::memory_order_relaxed)
  //      - 恢复内容标志位
  //      - 使用 relaxed 内存序（不需要强同步）
  //
  // 返回值:
  //   - Status::MemoryLimit(): 超过限制，已回滚
  //   - Status::OK(): 未超过限制，保持修改
  // ============================================================================
  Status commit() {
#ifndef NDEBUG
    // 设置已提交标志为 true
    // 确保析构函数的断言通过（assert(committed_)）
    // 这是一个调试检查，生产环境（NDEBUG）会被禁用
    committed_ = true;
#endif

    // 检查 WriteBatch 是否超过 max_bytes_ 限制
    // 条件：
    //   1. batch_->max_bytes_ != 0: 启用了大小限制（0 表示不限制）
    //   2. batch_->rep_.size() > batch_->max_bytes_: 当前大小超过限制
    // 如果两个条件都满足，则需要回滚到保存点状态
    if (batch_->max_bytes_ && batch_->rep_.size() > batch_->max_bytes_) {
      // 回滚步骤 1: 恢复 rep_ 的大小
      // 将 rep_ 调整到保存点创建时的大小
      // 这会删除超出部分的数据（最新添加的操作）
      // resize() 会截断 rep_，保留前 savepoint_.size 个字节
      batch_->rep_.resize(savepoint_.size);

      // 回滚步骤 2: 恢复操作计数
      // 将操作计数恢复到保存点创建时的计数
      // 操作计数存储在 rep_ 的 [8-11] 字节（4 字节固定整数）
      // SetCount() 会直接修改 rep_ 的 [8-11] 字节
      WriteBatchInternal::SetCount(batch_, savepoint_.count);

      // 回滚步骤 3: 恢复保护信息（如果启用）
      // 保护信息（ProtectionInfo）存储每个 key-value 对的校验和
      // 如果启用了保护（prot_info_ != nullptr），需要恢复保护信息条目
      // entries_ 是一个 autovector，存储 ProtectionInfoKVOC64 对象
      // resize() 会截断 entries_，保留前 savepoint_.count 个条目
      // 确保护息与操作计数保持一致
      if (batch_->prot_info_ != nullptr) {
        batch_->prot_info_->entries_.resize(savepoint_.count);
      }

      // 回滚步骤 4: 恢复内容标志位
      // 将内容标志位恢复到保存点创建时的标志位
      // 内容标志位用于快速查询批次中是否包含特定类型的操作
      // 使用原子操作（store）保证线程安全
      // 使用 relaxed 内存序，因为标志位不需要强同步（只读操作）
      // relaxed 内存序是足够的，因为标志位的读取不需要与其他操作同步
      batch_->content_flags_.store(savepoint_.content_flags,
                                   std::memory_order_relaxed);

      // 返回 Status::MemoryLimit() 表示超过大小限制
      // 调用方应该处理这个错误（如分批写入）
      // WriteBatch 已回滚到保存点状态，可以继续使用
      return Status::MemoryLimit();
    }

    // 未超过 max_bytes_ 限制，保持修改
    // 返回 Status::OK() 表示操作成功
    // WriteBatch 保持修改，可以继续添加操作
    return Status::OK();
  }

 private:
  // 指向要保护的 WriteBatch 对象
  // 不是所有权指针，不负责释放
  WriteBatch* batch_;

  // 保存点的状态，记录创建时刻的 WriteBatch 状态
  // 包含三个字段：
  //   1. size: rep_ 的大小（字节数）
  //   2. count: 操作计数（Put/Delete/Merge 等的数量）
  //   3. content_flags: 内容标志位（HAS_PUT、HAS_DELETE 等）
  // 用于在需要回滚时恢复状态
  SavePoint savepoint_;

#ifndef NDEBUG
  // 调试标志，记录 commit() 是否被调用
  // 如果未调用 commit()，析构函数会触发断言失败
  // 确保代码正确使用 LocalSavePoint（必须调用 commit()）
  // 生产环境（NDEBUG）此成员不存在（节省内存）
  bool committed_;
#endif
};

template <typename TimestampSizeFuncType>
class TimestampUpdater : public WriteBatch::Handler {
 public:
  explicit TimestampUpdater(WriteBatch::ProtectionInfo* prot_info,
                            TimestampSizeFuncType&& ts_sz_func, const Slice& ts)
      : prot_info_(prot_info),
        ts_sz_func_(std::move(ts_sz_func)),
        timestamp_(ts) {
    assert(!timestamp_.empty());
  }

  ~TimestampUpdater() override {}

  Status PutCF(uint32_t cf, const Slice& key, const Slice&) override {
    return UpdateTimestamp(cf, key);
  }

  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return UpdateTimestamp(cf, key);
  }

  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return UpdateTimestamp(cf, key);
  }

  Status DeleteRangeCF(uint32_t cf, const Slice& begin_key,
                       const Slice& end_key) override {
    Status s = UpdateTimestamp(cf, begin_key, true /* is_key */);
    if (s.ok()) {
      s = UpdateTimestamp(cf, end_key, false /* is_key */);
    }
    return s;
  }

  Status MergeCF(uint32_t cf, const Slice& key, const Slice&) override {
    return UpdateTimestamp(cf, key);
  }

  Status PutBlobIndexCF(uint32_t cf, const Slice& key, const Slice&) override {
    return UpdateTimestamp(cf, key);
  }

  Status MarkBeginPrepare(bool) override { return Status::OK(); }

  Status MarkEndPrepare(const Slice&) override { return Status::OK(); }

  Status MarkCommit(const Slice&) override { return Status::OK(); }

  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    return Status::OK();
  }

  Status MarkRollback(const Slice&) override { return Status::OK(); }

  Status MarkNoop(bool /*empty_batch*/) override { return Status::OK(); }

 private:
  // @param is_key specifies whether the update is for key or value.
  Status UpdateTimestamp(uint32_t cf, const Slice& buf, bool is_key = true) {
    Status s = UpdateTimestampImpl(cf, buf, idx_, is_key);
    ++idx_;
    return s;
  }

  Status UpdateTimestampImpl(uint32_t cf, const Slice& buf, size_t /*idx*/,
                             bool is_key) {
    if (timestamp_.empty()) {
      return Status::InvalidArgument("Timestamp is empty");
    }
    size_t cf_ts_sz = ts_sz_func_(cf);
    if (0 == cf_ts_sz) {
      // Skip this column family.
      return Status::OK();
    } else if (std::numeric_limits<size_t>::max() == cf_ts_sz) {
      // Column family timestamp info not found.
      return Status::NotFound();
    } else if (cf_ts_sz != timestamp_.size()) {
      return Status::InvalidArgument("timestamp size mismatch");
    }
    UpdateProtectionInformationIfNeeded(buf, timestamp_, is_key);

    char* ptr = const_cast<char*>(buf.data() + buf.size() - cf_ts_sz);
    assert(ptr);
    memcpy(ptr, timestamp_.data(), timestamp_.size());
    return Status::OK();
  }

  void UpdateProtectionInformationIfNeeded(const Slice& buf, const Slice& ts,
                                           bool is_key) {
    if (prot_info_ != nullptr) {
      const size_t ts_sz = ts.size();
      SliceParts old(&buf, 1);
      Slice old_no_ts(buf.data(), buf.size() - ts_sz);
      std::array<Slice, 2> new_key_cmpts{{old_no_ts, ts}};
      SliceParts new_parts(new_key_cmpts.data(), 2);
      if (is_key) {
        prot_info_->entries_[idx_].UpdateK(old, new_parts);
      } else {
        prot_info_->entries_[idx_].UpdateV(old, new_parts);
      }
    }
  }

  // No copy or move.
  TimestampUpdater(const TimestampUpdater&) = delete;
  TimestampUpdater(TimestampUpdater&&) = delete;
  TimestampUpdater& operator=(const TimestampUpdater&) = delete;
  TimestampUpdater& operator=(TimestampUpdater&&) = delete;

  WriteBatch::ProtectionInfo* const prot_info_ = nullptr;
  const TimestampSizeFuncType ts_sz_func_{};
  const Slice timestamp_;
  size_t idx_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
