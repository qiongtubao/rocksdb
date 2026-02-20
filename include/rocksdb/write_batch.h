// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#pragma once

#include <stdint.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/status.h"
#include "rocksdb/write_batch_base.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class ColumnFamilyHandle;
struct SavePoints;
struct SliceParts;

struct SavePoint {
  size_t size;  // size of rep_
  int count;    // count of elements in rep_
  uint32_t content_flags;

  SavePoint() : size(0), count(0), content_flags(0) {}

  SavePoint(size_t _size, int _count, uint32_t _flags)
      : size(_size), count(_count), content_flags(_flags) {}

  void clear() {
    size = 0;
    count = 0;
    content_flags = 0;
  }

  bool is_cleared() const { return (size | count | content_flags) == 0; }
};

class WriteBatch : public WriteBatchBase {
 public:
  explicit WriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : WriteBatch(reserved_bytes, max_bytes, 0, 0) {}

  // `protection_bytes_per_key` is the number of bytes used to store
  // protection information for each key entry. Currently supported values are
  // zero (disabled) and eight.
  explicit WriteBatch(size_t reserved_bytes, size_t max_bytes,
                      size_t protection_bytes_per_key, size_t default_cf_ts_sz);
  ~WriteBatch() override;

  using WriteBatchBase::Put;
  // Store the mapping "key->value" in the database.
  // The following Put(..., const Slice& key, ...) API can also be used when
  // user-defined timestamp is enabled as long as `key` points to a contiguous
  // buffer with timestamp appended after user key. The caller is responsible
  // for setting up the memory buffer pointed to by `key`.
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& value) override;
  Status Put(const Slice& key, const Slice& value) override {
    return Put(nullptr, key, value);
  }
  Status Put(ColumnFamilyHandle* column_family, const Slice& key,
             const Slice& ts, const Slice& value) override;

  // Variant of Put() that gathers output like writev(2).  The key and value
  // that will be written to the database are concatenations of arrays of
  // slices.
  // The following Put(..., const SliceParts& key, ...) API can be used when
  // user-defined timestamp is enabled as long as the timestamp is the last
  // Slice in `key`, a SliceParts (array of Slices). The caller is responsible
  // for setting up the `key` SliceParts object.
  Status Put(ColumnFamilyHandle* column_family, const SliceParts& key,
             const SliceParts& value) override;
  Status Put(const SliceParts& key, const SliceParts& value) override {
    return Put(nullptr, key, value);
  }

  // Store the mapping "key->{column1:value1, column2:value2, ...}" in the
  // column family specified by "column_family".
  using WriteBatchBase::PutEntity;
  Status PutEntity(ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns) override;

  using WriteBatchBase::Delete;
  // If the database contains a mapping for "key", erase it.  Else do nothing.
  // The following Delete(..., const Slice& key) can be used when user-defined
  // timestamp is enabled as long as `key` points to a contiguous buffer with
  // timestamp appended after user key. The caller is responsible for setting
  // up the memory buffer pointed to by `key`.
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key) override;
  Status Delete(const Slice& key) override { return Delete(nullptr, key); }
  Status Delete(ColumnFamilyHandle* column_family, const Slice& key,
                const Slice& ts) override;

  // variant that takes SliceParts
  // These two variants of Delete(..., const SliceParts& key) can be used when
  // user-defined timestamp is enabled as long as the timestamp is the last
  // Slice in `key`, a SliceParts (array of Slices). The caller is responsible
  // for setting up the `key` SliceParts object.
  Status Delete(ColumnFamilyHandle* column_family,
                const SliceParts& key) override;
  Status Delete(const SliceParts& key) override { return Delete(nullptr, key); }

  using WriteBatchBase::SingleDelete;
  // WriteBatch implementation of DB::SingleDelete().  See db.h.
  Status SingleDelete(ColumnFamilyHandle* column_family,
                      const Slice& key) override;
  Status SingleDelete(const Slice& key) override {
    return SingleDelete(nullptr, key);
  }
  Status SingleDelete(ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override;

  // variant that takes SliceParts
  Status SingleDelete(ColumnFamilyHandle* column_family,
                      const SliceParts& key) override;
  Status SingleDelete(const SliceParts& key) override {
    return SingleDelete(nullptr, key);
  }

  using WriteBatchBase::DeleteRange;
  // WriteBatch implementation of DB::DeleteRange().  See db.h.
  Status DeleteRange(ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key) override;
  Status DeleteRange(const Slice& begin_key, const Slice& end_key) override {
    return DeleteRange(nullptr, begin_key, end_key);
  }
  // begin_key and end_key should be user keys without timestamp.
  Status DeleteRange(ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key, const Slice& ts) override;

  // variant that takes SliceParts
  Status DeleteRange(ColumnFamilyHandle* column_family,
                     const SliceParts& begin_key,
                     const SliceParts& end_key) override;
  Status DeleteRange(const SliceParts& begin_key,
                     const SliceParts& end_key) override {
    return DeleteRange(nullptr, begin_key, end_key);
  }

  using WriteBatchBase::Merge;
  // Merge "value" with the existing value of "key" in the database.
  // "key->merge(existing, value)"
  Status Merge(ColumnFamilyHandle* column_family, const Slice& key,
               const Slice& value) override;
  Status Merge(const Slice& key, const Slice& value) override {
    return Merge(nullptr, key, value);
  }
  Status Merge(ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*ts*/, const Slice& /*value*/) override;

  // variant that takes SliceParts
  Status Merge(ColumnFamilyHandle* column_family, const SliceParts& key,
               const SliceParts& value) override;
  Status Merge(const SliceParts& key, const SliceParts& value) override {
    return Merge(nullptr, key, value);
  }

  using WriteBatchBase::PutLogData;
  // Append a blob of arbitrary size to the records in this batch. The blob will
  // be stored in the transaction log but not in any other file. In particular,
  // it will not be persisted to the SST files. When iterating over this
  // WriteBatch, WriteBatch::Handler::LogData will be called with the contents
  // of the blob as it is encountered. Blobs, puts, deletes, and merges will be
  // encountered in the same order in which they were inserted. The blob will
  // NOT consume sequence number(s) and will NOT increase the count of the batch
  //
  // Example application: add timestamps to the transaction log for use in
  // replication.
  Status PutLogData(const Slice& blob) override;

  using WriteBatchBase::Clear;
  // Clear all updates buffered in this batch.
  void Clear() override;

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  void SetSavePoint() override;

  // Remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
  // most recent call to SetSavePoint() and removes the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  Status RollbackToSavePoint() override;

  // Pop the most recent save point.
  // If there is no previous call to SetSavePoint(), Status::NotFound()
  // will be returned.
  // Otherwise returns Status::OK().
  Status PopSavePoint() override;

  // Support for iterating over the contents of a batch.
  // Objects of subclasses of Handler will be used by WriteBatch::Iterate().
  class Handler {
   public:
    virtual ~Handler();
    // All handler functions in this class provide default implementations so
    // we won't break existing clients of Handler on a source code level when
    // adding a new member function.

    // default implementation will just call Put without column family for
    // backwards compatibility. If the column family is not default,
    // the function is noop
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) {
      if (column_family_id == 0) {
        // Put() historically doesn't return status. We didn't want to be
        // backwards incompatible so we didn't change the return status
        // (this is a public API). We do an ordinary get and return Status::OK()
        Put(key, value);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and PutCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void Put(const Slice& /*key*/, const Slice& /*value*/) {}

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status PutEntityCF(uint32_t /* column_family_id */,
                               const Slice& /* key */,
                               const Slice& /* entity */) {
      return Status::NotSupported("PutEntityCF not implemented");
    }

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status DeleteCF(uint32_t column_family_id, const Slice& key) {
      if (column_family_id == 0) {
        Delete(key);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and DeleteCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void Delete(const Slice& /*key*/) {}

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) {
      if (column_family_id == 0) {
        SingleDelete(key);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and SingleDeleteCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void SingleDelete(const Slice& /*key*/) {}

    // If user-defined timestamp is enabled, then `begin_key` and `end_key`
    // both include timestamp.
    virtual Status DeleteRangeCF(uint32_t /*column_family_id*/,
                                 const Slice& /*begin_key*/,
                                 const Slice& /*end_key*/) {
      return Status::InvalidArgument("DeleteRangeCF not implemented");
    }

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) {
      if (column_family_id == 0) {
        Merge(key, value);
        return Status::OK();
      }
      return Status::InvalidArgument(
          "non-default column family and MergeCF not implemented");
    }
    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual void Merge(const Slice& /*key*/, const Slice& /*value*/) {}

    // If user-defined timestamp is enabled, then `key` includes timestamp.
    virtual Status PutBlobIndexCF(uint32_t /*column_family_id*/,
                                  const Slice& /*key*/,
                                  const Slice& /*value*/) {
      return Status::InvalidArgument("PutBlobIndexCF not implemented");
    }

    // The default implementation of LogData does nothing.
    virtual void LogData(const Slice& blob);

    virtual Status MarkBeginPrepare(bool = false) {
      return Status::InvalidArgument("MarkBeginPrepare() handler not defined.");
    }

    virtual Status MarkEndPrepare(const Slice& /*xid*/) {
      return Status::InvalidArgument("MarkEndPrepare() handler not defined.");
    }

    virtual Status MarkNoop(bool /*empty_batch*/) {
      return Status::InvalidArgument("MarkNoop() handler not defined.");
    }

    virtual Status MarkRollback(const Slice& /*xid*/) {
      return Status::InvalidArgument(
          "MarkRollbackPrepare() handler not defined.");
    }

    virtual Status MarkCommit(const Slice& /*xid*/) {
      return Status::InvalidArgument("MarkCommit() handler not defined.");
    }

    virtual Status MarkCommitWithTimestamp(const Slice& /*xid*/,
                                           const Slice& /*commit_ts*/) {
      return Status::InvalidArgument(
          "MarkCommitWithTimestamp() handler not defined.");
    }

    // Continue is called by WriteBatch::Iterate. If it returns false,
    // iteration is halted. Otherwise, it continues iterating. The default
    // implementation always returns true.
    virtual bool Continue();

   protected:
    friend class WriteBatchInternal;
    enum class OptionState {
      kUnknown,
      kDisabled,
      kEnabled,
    };
    virtual OptionState WriteAfterCommit() const {
      return OptionState::kUnknown;
    }
    virtual OptionState WriteBeforePrepare() const {
      return OptionState::kUnknown;
    }
  };
  Status Iterate(Handler* handler) const;

  // Retrieve the serialized version of this batch.
  const std::string& Data() const { return rep_; }

  // Release the serialized data and clear this batch.
  std::string Release();

  // Retrieve data size of the batch.
  size_t GetDataSize() const { return rep_.size(); }

  // Returns the number of updates in the batch
  uint32_t Count() const;

  // Returns true if PutCF will be called during Iterate
  bool HasPut() const;

  // Returns true if PutEntityCF will be called during Iterate
  bool HasPutEntity() const;

  // Returns true if DeleteCF will be called during Iterate
  bool HasDelete() const;

  // Returns true if SingleDeleteCF will be called during Iterate
  bool HasSingleDelete() const;

  // Returns true if DeleteRangeCF will be called during Iterate
  bool HasDeleteRange() const;

  // Returns true if MergeCF will be called during Iterate
  bool HasMerge() const;

  // Returns true if MarkBeginPrepare will be called during Iterate
  bool HasBeginPrepare() const;

  // Returns true if MarkEndPrepare will be called during Iterate
  bool HasEndPrepare() const;

  // Returns true if MarkCommit will be called during Iterate
  bool HasCommit() const;

  // Returns true if MarkRollback will be called during Iterate
  bool HasRollback() const;

  // Experimental.
  //
  // Update timestamps of existing entries in the write batch if
  // applicable. If a key is intended for a column family that disables
  // timestamp, then this API won't set the timestamp for this key.
  // This requires that all keys, if enable timestamp, (possibly from multiple
  // column families) in the write batch have timestamps of the same format.
  //
  // ts_sz_func: callable object to obtain the timestamp sizes of column
  // families. If ts_sz_func() accesses data structures, then the caller of this
  // API must guarantee thread-safety. Like other parts of RocksDB, this API is
  // not exception-safe. Therefore, ts_sz_func() must not throw.
  //
  // in: cf, the column family id.
  // ret: timestamp size of the given column family. Return
  //      std::numeric_limits<size_t>::max() indicating "don't know or column
  //      family info not found", this will cause UpdateTimestamps() to fail.
  // size_t ts_sz_func(uint32_t cf);
  Status UpdateTimestamps(const Slice& ts,
                          std::function<size_t(uint32_t /*cf*/)> ts_sz_func);

  // Verify the per-key-value checksums of this write batch.
  // Corruption status will be returned if the verification fails.
  // If this write batch does not have per-key-value checksum,
  // OK status will be returned.
  Status VerifyChecksum() const;

  using WriteBatchBase::GetWriteBatch;
  WriteBatch* GetWriteBatch() override { return this; }

  // Constructor with a serialized string object
  explicit WriteBatch(const std::string& rep);
  explicit WriteBatch(std::string&& rep);

  WriteBatch(const WriteBatch& src);
  WriteBatch(WriteBatch&& src) noexcept;
  WriteBatch& operator=(const WriteBatch& src);
  WriteBatch& operator=(WriteBatch&& src);

  // marks this point in the WriteBatch as the last record to
  // be inserted into the WAL, provided the WAL is enabled
  void MarkWalTerminationPoint();
  const SavePoint& GetWalTerminationPoint() const { return wal_term_point_; }

  void SetMaxBytes(size_t max_bytes) override { max_bytes_ = max_bytes; }

  struct ProtectionInfo;
  size_t GetProtectionBytesPerKey() const;

 private:
  friend class WriteBatchInternal;
  friend class LocalSavePoint;
  // TODO(myabandeh): this is needed for a hack to collapse the write batch and
  // remove duplicate keys. Remove it when the hack is replaced with a proper
  // solution.
  friend class WriteBatchWithIndex;
  std::unique_ptr<SavePoints> save_points_;

  // When sending a WriteBatch through WriteImpl we might want to
  // specify that only the first x records of the batch be written to
  // the WAL.
  SavePoint wal_term_point_;

  // Is the content of the batch the application's latest state that meant only
  // to be used for recovery? Refer to
  // TransactionOptions::use_only_the_last_commit_time_batch_for_recovery for
  // more details.
  bool is_latest_persistent_state_ = false;

  // False if all keys are from column families that disable user-defined
  // timestamp OR UpdateTimestamps() has been called at least once.
  // This flag will be set to true if any of the above Put(), Delete(),
  // SingleDelete(), etc. APIs are called at least once.
  // Calling Put(ts), Delete(ts), SingleDelete(ts), etc. will not set this flag
  // to true because the assumption is that these APIs have already set the
  // timestamps to desired values.
  bool needs_in_place_update_ts_ = false;

  // True if the write batch contains at least one key from a column family
  // that enables user-defined timestamp.
  bool has_key_with_ts_ = false;

  // For HasXYZ.  Mutable to allow lazy computation of results
  mutable std::atomic<uint32_t> content_flags_;

  // Performs deferred computation of content_flags if necessary
  uint32_t ComputeContentFlags() const;

  // Maximum size of rep_.
  size_t max_bytes_;

  std::unique_ptr<ProtectionInfo> prot_info_;

  size_t default_cf_ts_sz_ = 0;

 protected:
  // ============================================================================
  // rep_: WriteBatch 内部表示缓冲区（序列化数据）
  //
  // 概述:
  // rep_ 是 WriteBatch 的核心成员变量，用于存储所有操作序列化后的二进制数据。
  // 所有的 Put/Delete/Merge 等操作都会按照特定格式序列化到这个字符串缓冲区中。
  //
  // 数据格式（write_batch.cc:10-37）:
  //
  //   WriteBatch::rep_ :=
  //       sequence: fixed64           [0-7 字节] 序列号（写入时由 DB 分配）
  //       count: fixed32              [8-11 字节] 操作记录数量（包括 Put/Delete/Merge 等）
  //       data: record[count]         [12字节开始] 操作记录数组
  //
  //   record := （操作记录，按添加顺序排列）
  //       kTypeValue varstring varstring                        [Put 操作，默认列族]
  //           varstring = len: varint32 + data: uint8[len]
  //           格式: [类型][key长度][key数据][value长度][value数据]
  //
  //       kTypeDeletion varstring                                [Delete 操作，默认列族]
  //           格式: [类型][key长度][key数据]
  //
  //       kTypeSingleDeletion varstring                          [SingleDelete 操作]
  //           格式: [类型][key长度][key数据]
  //
  //       kTypeRangeDeletion varstring varstring                 [DeleteRange 操作]
  //           格式: [类型][begin_key长度][begin_key数据][end_key长度][end_key数据]
  //
  //       kTypeMerge varstring varstring                          [Merge 操作]
  //           格式: [类型][key长度][key数据][value长度][value数据]
  //
  //       kTypeColumnFamilyValue varint32 varstring varstring     [Put 操作，指定列族]
  //           格式: [类型][列族ID][key长度][key数据][value长度][value数据]
  //
  //       kTypeColumnFamilyDeletion varint32 varstring            [Delete 操作，指定列族]
  //           格式: [类型][列族ID][key长度][key数据]
  //
  //       kTypeColumnFamilySingleDeletion varint32 varstring      [SingleDelete，指定列族]
  //           格式: [类型][列族ID][key长度][key数据]
  //
  //       kTypeColumnFamilyRangeDeletion varint32 varstring varstring  [DeleteRange，指定列族]
  //           格式: [类型][列族ID][begin_key长度][begin_key数据][end_key长度][end_key数据]
  //
  //       kTypeColumnFamilyMerge varint32 varstring varstring    [Merge 操作，指定列族]
  //           格式: [类型][列族ID][key长度][key数据][value长度][value数据]
  //
  //       kTypeBeginPrepareXID                                    [两阶段提交：开始 Prepare]
  //           格式: [类型]
  //
  //       kTypeEndPrepareXID varstring                            [两阶段提交：结束 Prepare]
  //           格式: [类型][XID长度][XID数据]
  //
  //       kTypeCommitXID varstring                                [两阶段提交：Commit]
  //           格式: [类型][XID长度][XID数据]
  //
  //       kTypeCommitXIDAndTimestamp varstring varstring         [两阶段提交：Commit with Timestamp]
  //           格式: [类型][XID长度][XID数据][timestamp长度][timestamp数据]
  //
  //       kTypeRollbackXID varstring                              [两阶段提交：Rollback]
  //           格式: [类型][XID长度][XID数据]
  //
  //       kTypeBeginPersistedPrepareXID                           [持久化 Prepare 开始]
  //           格式: [类型]
  //
  //       kTypeBeginUnprepareXID                                  [Unprepare 操作]
  //           格式: [类型]
  //
  //       kTypeWideColumnEntity varstring varstring               [宽列实体操作]
  //           格式: [类型][key长度][key数据][entity长度][entity数据]
  //
  //       kTypeColumnFamilyWideColumnEntity varint32 varstring varstring  [宽列，指定列族]
  //           格式: [类型][列族ID][key长度][key数据][entity长度][entity数据]
  //
  //       kTypeNoop                                               [空操作（No-Op）]
  //           格式: [类型]
  //
  //   varstring := （变长字符串）
  //       len: varint32            [1-5 字节] 字符串长度（使用变长整数编码）
  //       data: uint8[len]        [len 字节] 字符串数据
  //
  // 内存布局示例:
  //
  //   示例 1: 默认列族的 Put 操作
  //   操作: batch.Put("key1", "value1")
  //   rep_ 内容（十六进制）:
  //       00000000 00000000    [0-7]   sequence = 0（未分配）
  //       01000000              [8-11]  count = 1
  //       0A                    [12]    kTypeValue = 0x0A
  //       04                    [13]    key 长度 = 4
  //       6B657931              [14-17] key = "key1"
  //       06                    [18]    value 长度 = 6
  //       76616C756531         [19-24] value = "value1"
  //
  //   示例 2: 指定列族的 Delete 操作
  //   操作: batch.Delete(cf_handle, "key2")
  //   rep_ 内容（续上）:
  //       1F                    [25]    kTypeColumnFamilyDeletion = 0x1F
  //       05                    [26]    列族 ID = 5（varint32）
  //       04                    [27]    key 长度 = 4
  //       6B657932              [28-31] key = "key2"
  //
  //   示例 3: Merge 操作
  //   操作: batch.Merge("key3", "delta")
  //   rep_ 内容（续上）:
  //       0E                    [32]    kTypeMerge = 0x0E
  //       04                    [33]    key 长度 = 4
  //       6B657933              [34-37] key = "key3"
  //       05                    [38]    value 长度 = 5
  //       64656C7461            [39-43] value = "delta"
  //
  // 类型常量（db/dbformat.h）:
  //   kTypeValue = 0x0A                              [Put，默认列族]
  //   kTypeDeletion = 0x0C                           [Delete，默认列族]
  //   kTypeSingleDeletion = 0x0F                     [SingleDelete]
  //   kTypeMerge = 0x0E                              [Merge]
  //   kTypeRangeDeletion = 0x42                      [DeleteRange]
  //   kTypeColumnFamilyValue = 0x1A                  [Put，指定列族]
  //   kTypeColumnFamilyDeletion = 0x1F               [Delete，指定列族]
  //   kTypeColumnFamilySingleDeletion = 0x2C         [SingleDelete，指定列族]
  //   kTypeColumnFamilyRangeDeletion = 0x56          [DeleteRange，指定列族]
  //   kTypeColumnFamilyMerge = 0x2D                  [Merge，指定列族]
  //   kTypeBeginPrepareXID = 0xB                     [2PC: Begin Prepare]
  //   kTypeEndPrepareXID = 0xC                       [2PC: End Prepare]
  //   kTypeCommitXID = 0xD                           [2PC: Commit]
  //   kTypeCommitXIDAndTimestamp = 0x12              [2PC: Commit with Timestamp]
  //   kTypeRollbackXID = 0xE                          [2PC: Rollback]
  //   kTypeBeginPersistedPrepareXID = 0x20           [2PC: Begin Persisted Prepare]
  //   kTypeBeginUnprepareXID = 0x21                   [2PC: Begin Unprepare]
  //   kTypeWideColumnEntity = 0x2E                   [宽列实体]
  //   kTypeColumnFamilyWideColumnEntity = 0x2F       [宽列实体，指定列族]
  //   kTypeNoop = 0x0                                 [空操作]
  //
  // 主要用途:
  //   1. 序列化存储: 将所有操作序列化到二进制缓冲区，便于：
  //      - 写入 WAL（Write-Ahead Log）
  //      - 网络传输（如复制）
  //      - 持久化存储
  //
  //   2. 批量写入: 将多个操作打包为一个批次，实现原子写入
  //      - 所有操作要么全部成功，要么全部失败
  //      - 通过序列号保证顺序一致性
  //
  //   3. 重放和恢复: 从 WAL 或备份中恢复数据
  //      - 通过 Iterate() 解析 rep_ 中的所有操作
  //      - 重新应用到 MemTable
  //
  //   4. 事务支持: 两阶段提交（2PC）的实现基础
  //      - Prepare 阶段: 写入 WAL（rep_）
  //      - Commit 阶段: 写入 MemTable
  //
  // 使用示例:
  //
  //   1. 添加操作:
  //   WriteBatch batch;
  //   batch.Put("key1", "value1");      // 序列化到 rep_[12..24]
  //   batch.Delete("key2");            // 序列化到 rep_[25..31]
  //   batch.Merge("key3", "delta");     // 序列化到 rep_[32..43]
  //
  //   2. 获取序列化数据:
  //   Slice data = WriteBatchInternal::Contents(&batch);  // 返回 rep_
  //   printf("Batch size: %zu\n", data.size());           // 输出 rep_.size()
  //
  //   3. 从序列化数据创建:
  //   std::string serialized_data = "..."  // 从 WAL 或备份中读取
  //   WriteBatch batch_from_serialized(serialized_data);
  //
  //   4. 迭代操作:
  //   batch.Iterate(handler);  // 解析 rep_，调用 handler 的回调函数
  //
  //   5. 清空:
  //   batch.Clear();  // rep_.clear()，重置为空（只保留 sequence 和 count）
  //
  // 线程安全性:
  //   - 多个线程可以同时调用 const 方法（如 GetDataSize()）
  //   - 多个线程调用非 const 方法（如 Put/Delete）需要外部同步
  //   - rep_ 本身不是线程安全的（std::string 不保证线程安全）
  //
  // 性能考虑:
  //   - std::string 自动扩容：当 rep_ 空间不足时，会重新分配内存并复制数据
  //   - 预分配：构造函数可以预分配空间（reserved_bytes 参数）
  //   - 最小化复制：WriteBatch 支持移动语义（move constructor/assignment）
  //
  // 相关函数:
  //   - WriteBatchInternal::Contents(): 获取 rep_ 的 Slice 视图
  //   - WriteBatchInternal::SetContents(): 设置 rep_ 的内容
  //   - WriteBatchInternal::GetDataSize(): 获取 rep_ 的数据大小
  //   - WriteBatch::Iterate(): 遍历 rep_ 中的所有操作
  //   - WriteBatch::Clear(): 清空 rep_
  //
  // 注意事项:
  //   - rep_ 的格式是内部实现细节，不应依赖具体的字节偏移
  //   - 序列号（sequence）在写入时由 DB 分配，创建 WriteBatch 时为 0
  //   - count 包括所有操作类型（Put/Delete/Merge/LogData 等）
  //   - 变长整数编码（varint32）节省空间，但需要解码
  //   - 时间戳（Timestamp）存储在 key 的末尾（如果列族启用）
  //   - 保护信息（ProtectionInfo，如校验和）存储在 prot_info_ 中，不在 rep_ 中
  // ============================================================================
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace ROCKSDB_NAMESPACE
