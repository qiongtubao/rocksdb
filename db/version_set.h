//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of table files per level, as well as a
// set of blob files. The entire set of versions is maintained in a
// VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/cache_helpers.h"
#include "db/blob/blob_file_meta.h"
#include "db/blob/blob_index.h"
#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_picker.h"
#include "db/dbformat.h"
#include "db/file_indexer.h"
#include "db/log_reader.h"
#include "db/range_del_aggregator.h"
#include "db/read_callback.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "env/file_system_tracer.h"
#if USE_COROUTINES
#include "folly/experimental/coro/BlockingWait.h"
#include "folly/experimental/coro/Collect.h"
#endif
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "table/get_context.h"
#include "table/multiget_context.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/autovector.h"
#include "util/coro_utils.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

namespace log {
class Writer;
}

class BlobIndex;
class Compaction;
class LogBuffer;
class LookupKey;
class MemTable;
class Version;
class VersionSet;
class WriteBufferManager;
class MergeContext;
class ColumnFamilySet;
class MergeIteratorBuilder;
class SystemClock;
class ManifestTailer;
class FilePickerMultiGet;

// VersionEdit is always supposed to be valid and it is used to point at
// entries in Manifest. Ideally it should not be used as a container to
// carry around few of its fields as function params because it can cause
// readers to think it's a valid entry from Manifest. To avoid that confusion
// introducing VersionEditParams to simply carry around multiple VersionEdit
// params. It need not point to a valid record in Manifest.
using VersionEditParams = VersionEdit;

// Return the smallest index i such that file_level.files[i]->largest >= key.
// Return file_level.num_files if there is no such file.
// REQUIRES: "file_level.files" contains a sorted list of
// non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const LevelFilesBrief& file_level, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, file_level.files[]
// contains disjoint ranges in sorted order.
extern bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                                  bool disjoint_sorted_files,
                                  const LevelFilesBrief& file_level,
                                  const Slice* smallest_user_key,
                                  const Slice* largest_user_key);

// Generate LevelFilesBrief from vector<FdWithKeyRange*>
// Would copy smallest_key and largest_key data to sequential memory
// arena: Arena used to allocate the memory
extern void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                                      const std::vector<FileMetaData*>& files,
                                      Arena* arena);
enum EpochNumberRequirement {
  kMightMissing,
  kMustPresent,
};

// Information of the storage associated with each Version, including number of
// levels of LSM tree, files information at each level, files marked for
// compaction, blob files, etc.
class VersionStorageInfo {
 public:
  VersionStorageInfo(const InternalKeyComparator* internal_comparator,
                     const Comparator* user_comparator, int num_levels,
                     CompactionStyle compaction_style,
                     VersionStorageInfo* src_vstorage,
                     bool _force_consistency_checks,
                     EpochNumberRequirement epoch_number_requirement =
                         EpochNumberRequirement::kMustPresent);
  // No copying allowed
  VersionStorageInfo(const VersionStorageInfo&) = delete;
  void operator=(const VersionStorageInfo&) = delete;
  ~VersionStorageInfo();

  void Reserve(int level, size_t size) { files_[level].reserve(size); }

  void AddFile(int level, FileMetaData* f);

  // Resize/Initialize the space for compact_cursor_
  void ResizeCompactCursors(int level) {
    compact_cursor_.resize(level, InternalKey());
  }

  const std::vector<InternalKey>& GetCompactCursors() const {
    return compact_cursor_;
  }

  // REQUIRES: ResizeCompactCursors has been called
  void AddCursorForOneLevel(int level,
                            const InternalKey& smallest_uncompacted_key) {
    compact_cursor_[level] = smallest_uncompacted_key;
  }

  // REQUIRES: lock is held
  // Update the compact cursor and advance the file index using increment
  // so that it can point to the next cursor (increment means the number of
  // input files in this level of the last compaction)
  const InternalKey& GetNextCompactCursor(int level, size_t increment) {
    int cmp_idx = next_file_to_compact_by_size_[level] + (int)increment;
    assert(cmp_idx <= (int)files_by_compaction_pri_[level].size());
    // TODO(zichen): may need to update next_file_to_compact_by_size_
    // for parallel compaction.
    InternalKey new_cursor;
    if (cmp_idx >= (int)files_by_compaction_pri_[level].size()) {
      cmp_idx = 0;
    }
    // TODO(zichen): rethink if this strategy gives us some good guarantee
    return files_[level][files_by_compaction_pri_[level][cmp_idx]]->smallest;
  }

  void ReserveBlob(size_t size) { blob_files_.reserve(size); }

  void AddBlobFile(std::shared_ptr<BlobFileMetaData> blob_file_meta);

  void PrepareForVersionAppend(const ImmutableOptions& immutable_options,
                               const MutableCFOptions& mutable_cf_options);

  // REQUIRES: PrepareForVersionAppend has been called
  void SetFinalized();

  // Update the accumulated stats from a file-meta.
  void UpdateAccumulatedStats(FileMetaData* file_meta);

  // Decrease the current stat from a to-be-deleted file-meta
  void RemoveCurrentStats(FileMetaData* file_meta);

  // Updates internal structures that keep track of compaction scores
  // We use compaction scores to figure out which compaction to do next
  // REQUIRES: db_mutex held!!
  // TODO find a better way to pass compaction_options_fifo.
  void ComputeCompactionScore(const ImmutableOptions& immutable_options,
                              const MutableCFOptions& mutable_cf_options);

  // Estimate est_comp_needed_bytes_
  void EstimateCompactionBytesNeeded(
      const MutableCFOptions& mutable_cf_options);

  // This computes files_marked_for_compaction_ and is called by
  // ComputeCompactionScore()
  void ComputeFilesMarkedForCompaction(int last_level);

  // This computes ttl_expired_files_ and is called by
  // ComputeCompactionScore()
  void ComputeExpiredTtlFiles(const ImmutableOptions& ioptions,
                              const uint64_t ttl);

  // This computes files_marked_for_periodic_compaction_ and is called by
  // ComputeCompactionScore()
  void ComputeFilesMarkedForPeriodicCompaction(
      const ImmutableOptions& ioptions,
      const uint64_t periodic_compaction_seconds, int last_level);

  // This computes bottommost_files_marked_for_compaction_ and is called by
  // ComputeCompactionScore() or UpdateOldestSnapshot().
  //
  // Among bottommost files (assumes they've already been computed), marks the
  // ones that have keys that would be eliminated if recompacted, according to
  // the seqnum of the oldest existing snapshot. Must be called every time
  // oldest snapshot changes as that is when bottom-level files can become
  // eligible for compaction.
  //
  // REQUIRES: DB mutex held
  void ComputeBottommostFilesMarkedForCompaction();

  // This computes files_marked_for_forced_blob_gc_ and is called by
  // ComputeCompactionScore()
  //
  // REQUIRES: DB mutex held
  void ComputeFilesMarkedForForcedBlobGC(
      double blob_garbage_collection_age_cutoff,
      double blob_garbage_collection_force_threshold);

  bool level0_non_overlapping() const { return level0_non_overlapping_; }

  // Updates the oldest snapshot and related internal state, like the bottommost
  // files marked for compaction.
  // REQUIRES: DB mutex held
  void UpdateOldestSnapshot(SequenceNumber oldest_snapshot_seqnum);

  int MaxInputLevel() const;
  int MaxOutputLevel(bool allow_ingest_behind) const;

  // Return level number that has idx'th highest score
  int CompactionScoreLevel(int idx) const { return compaction_level_[idx]; }

  // Return idx'th highest score
  double CompactionScore(int idx) const { return compaction_score_[idx]; }

  void GetOverlappingInputs(
      int level, const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,               // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,        // index of overlap file
      int* file_index = nullptr,  // return index of overlap file
      bool expand_range = true,   // if set, returns files which overlap the
                                  // range and overlap each other. If false,
                                  // then just files intersecting the range
      InternalKey** next_smallest = nullptr)  // if non-null, returns the
      const;  // smallest key of next file not included
  void GetCleanInputsWithinInterval(
      int level, const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,               // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,        // index of overlap file
      int* file_index = nullptr)  // return index of overlap file
      const;

  void GetOverlappingInputsRangeBinarySearch(
      int level,                 // level > 0
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index,                // index of overlap file
      int* file_index,               // return index of overlap file
      bool within_interval = false,  // if set, force the inputs within interval
      InternalKey** next_smallest = nullptr)  // if non-null, returns the
      const;  // smallest key of next file not included

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Returns true iff the first or last file in inputs contains
  // an overlapping user key to the file "just outside" of it (i.e.
  // just after the last file, or just before the first file)
  // REQUIRES: "*inputs" is a sorted list of non-overlapping files
  bool HasOverlappingUserKey(const std::vector<FileMetaData*>* inputs,
                             int level);

  int num_levels() const { return num_levels_; }

  // REQUIRES: PrepareForVersionAppend has been called
  int num_non_empty_levels() const {
    assert(finalized_);
    return num_non_empty_levels_;
  }

  // REQUIRES: PrepareForVersionAppend has been called
  // This may or may not return number of level files. It is to keep backward
  // compatible behavior in universal compaction.
  int l0_delay_trigger_count() const { return l0_delay_trigger_count_; }

  void set_l0_delay_trigger_count(int v) { l0_delay_trigger_count_ = v; }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  int NumLevelFiles(int level) const {
    assert(finalized_);
    return static_cast<int>(files_[level].size());
  }

  // Return the combined file size of all files at the specified level.
  uint64_t NumLevelBytes(int level) const;

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  const std::vector<FileMetaData*>& LevelFiles(int level) const {
    return files_[level];
  }

  bool HasMissingEpochNumber() const;
  uint64_t GetMaxEpochNumberOfFiles() const;
  EpochNumberRequirement GetEpochNumberRequirement() const {
    return epoch_number_requirement_;
  }
  void SetEpochNumberRequirement(
      EpochNumberRequirement epoch_number_requirement) {
    epoch_number_requirement_ = epoch_number_requirement;
  }
  void RecoverEpochNumbers(ColumnFamilyData* cfd);

  class FileLocation {
   public:
    FileLocation() = default;
    FileLocation(int level, size_t position)
        : level_(level), position_(position) {}

    int GetLevel() const { return level_; }
    size_t GetPosition() const { return position_; }

    bool IsValid() const { return level_ >= 0; }

    bool operator==(const FileLocation& rhs) const {
      return level_ == rhs.level_ && position_ == rhs.position_;
    }

    bool operator!=(const FileLocation& rhs) const { return !(*this == rhs); }

    static FileLocation Invalid() { return FileLocation(); }

   private:
    int level_ = -1;
    size_t position_ = 0;
  };

  // REQUIRES: PrepareForVersionAppend has been called
  FileLocation GetFileLocation(uint64_t file_number) const {
    const auto it = file_locations_.find(file_number);

    if (it == file_locations_.end()) {
      return FileLocation::Invalid();
    }

    assert(it->second.GetLevel() < num_levels_);
    assert(it->second.GetPosition() < files_[it->second.GetLevel()].size());
    assert(files_[it->second.GetLevel()][it->second.GetPosition()]);
    assert(files_[it->second.GetLevel()][it->second.GetPosition()]
               ->fd.GetNumber() == file_number);

    return it->second;
  }

  // REQUIRES: PrepareForVersionAppend has been called
  FileMetaData* GetFileMetaDataByNumber(uint64_t file_number) const {
    auto location = GetFileLocation(file_number);

    if (!location.IsValid()) {
      return nullptr;
    }

    return files_[location.GetLevel()][location.GetPosition()];
  }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  using BlobFiles = std::vector<std::shared_ptr<BlobFileMetaData>>;
  const BlobFiles& GetBlobFiles() const { return blob_files_; }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  BlobFiles::const_iterator GetBlobFileMetaDataLB(
      uint64_t blob_file_number) const;

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  std::shared_ptr<BlobFileMetaData> GetBlobFileMetaData(
      uint64_t blob_file_number) const {
    const auto it = GetBlobFileMetaDataLB(blob_file_number);

    assert(it == blob_files_.end() || *it);

    if (it != blob_files_.end() &&
        (*it)->GetBlobFileNumber() == blob_file_number) {
      return *it;
    }

    return std::shared_ptr<BlobFileMetaData>();
  }

  // REQUIRES: This version has been saved (see VersionBuilder::SaveTo)
  struct BlobStats {
    uint64_t total_file_size = 0;
    uint64_t total_garbage_size = 0;
    double space_amp = 0.0;
  };

  BlobStats GetBlobStats() const {
    uint64_t total_file_size = 0;
    uint64_t total_garbage_size = 0;

    for (const auto& meta : blob_files_) {
      assert(meta);

      total_file_size += meta->GetBlobFileSize();
      total_garbage_size += meta->GetGarbageBlobBytes();
    }

    double space_amp = 0.0;
    if (total_file_size > total_garbage_size) {
      space_amp = static_cast<double>(total_file_size) /
                  (total_file_size - total_garbage_size);
    }

    return BlobStats{total_file_size, total_garbage_size, space_amp};
  }

  const ROCKSDB_NAMESPACE::LevelFilesBrief& LevelFilesBrief(int level) const {
    assert(level < static_cast<int>(level_files_brief_.size()));
    return level_files_brief_[level];
  }

  // REQUIRES: PrepareForVersionAppend has been called
  const std::vector<int>& FilesByCompactionPri(int level) const {
    assert(finalized_);
    return files_by_compaction_pri_[level];
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>& FilesMarkedForCompaction()
      const {
    assert(finalized_);
    return files_marked_for_compaction_;
  }

  void TEST_AddFileMarkedForCompaction(int level, FileMetaData* f) {
    f->marked_for_compaction = true;
    files_marked_for_compaction_.emplace_back(level, f);
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  // Used by Leveled Compaction only.
  const autovector<std::pair<int, FileMetaData*>>& ExpiredTtlFiles() const {
    assert(finalized_);
    return expired_ttl_files_;
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  // Used by Leveled and Universal Compaction.
  const autovector<std::pair<int, FileMetaData*>>&
  FilesMarkedForPeriodicCompaction() const {
    assert(finalized_);
    return files_marked_for_periodic_compaction_;
  }

  void TEST_AddFileMarkedForPeriodicCompaction(int level, FileMetaData* f) {
    files_marked_for_periodic_compaction_.emplace_back(level, f);
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>&
  BottommostFilesMarkedForCompaction() const {
    assert(finalized_);
    return bottommost_files_marked_for_compaction_;
  }

  // REQUIRES: ComputeCompactionScore has been called
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>& FilesMarkedForForcedBlobGC()
      const {
    assert(finalized_);
    return files_marked_for_forced_blob_gc_;
  }

  int base_level() const { return base_level_; }
  double level_multiplier() const { return level_multiplier_; }

  // REQUIRES: lock is held
  // Set the index that is used to offset into files_by_compaction_pri_ to find
  // the next compaction candidate file.
  void SetNextCompactionIndex(int level, int index) {
    next_file_to_compact_by_size_[level] = index;
  }

  // REQUIRES: lock is held
  int NextCompactionIndex(int level) const {
    return next_file_to_compact_by_size_[level];
  }

  // REQUIRES: PrepareForVersionAppend has been called
  const FileIndexer& file_indexer() const {
    assert(finalized_);
    return file_indexer_;
  }

  // Only the first few entries of files_by_compaction_pri_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t kNumberFilesToSort = 50;

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[1000];
  };
  struct FileSummaryStorage {
    char buffer[3000];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;
  // Return a human-readable short (single-line) summary of files
  // in a specified level.  Uses *scratch as backing store.
  const char* LevelFileSummary(FileSummaryStorage* scratch, int level) const;

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  uint64_t MaxNextLevelOverlappingBytes();

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false) const;

  /**
   * @brief 获取所有采样文件的平均 Value 大小（单位：字节）
   *
   * 本函数用于计算 DB 中所有文件的平均 Value 大小，这个值在
   * ComputeCompensatedSizes() 中用于估计删除条目节省的空间。
   *
   * 计算公式：
   *   avg_value_size = (raw_value_size / num_non_deletions) *
   *                    (file_size / (raw_key_size + raw_value_size))
   *
   * 拆解说明：
   *   第一部分：raw_value_size / num_non_deletions
   *   - 这是未压缩的每个非删除条目的平均 value 大小
   *   - raw_value_size 是所有条目的未压缩 value 总大小
   *   - num_non_deletions 是非删除条目的总数
   *
   *   第二部分：file_size / (raw_key_size + raw_value_size)
   *   - 这是压缩后的文件大小与未压缩数据大小的比率
   *   - file_size 是压缩后的实际文件大小
   *   - raw_key_size + raw_value_size 是未压缩的 key + value 总大小
   *   - 这个比率反映了压缩算法的效果（越小说明压缩比越高）
   *
   * 最终结果：avg_value_size × 压缩比率
   *   - 即：压缩后的平均 value 大小
   *   - 用于更准确地估计删除条目在压缩文件中节省的空间
   *
   * 为什么要这样计算？
   * 1. 删除条目在压缩文件中占用的空间与压缩比率相关
   * 2. 如果只使用 raw_value_size / num_non_deletions，会高估节省的空间
   *   （因为数据是压缩存储的）
   * 3. 乘以压缩比率后，得到的值更接近实际情况
   *
   * 数据来源（accumulated_* 统计数据）：
   * 这些统计数据在 VersionStorageInfo::UpdateAccumulatedStats() 中累计，
   * 每当有新文件被初始化时（通过 MaybeInitializeFileMetaData），
   * 就会将该文件的统计信息累加到 accumulated_* 变量中。
   *
   * 具体参数含义：
   *
   * accumulated_raw_key_size_：
   *   - 含义：所有采样文件的未压缩 key 总大小（字节）
   *   - 来源：file_meta->raw_key_size 的累计值
   *   - FileMetaData::raw_key_size：
   *     * 该文件中所有 key 的未压缩大小之和
   *     * 包括删除条目的 key（删除条目也有 key）
   *     * 在 Flush/Compaction 时计算并保存
   *
   * accumulated_raw_value_size_：
   *   - 含义：所有采样文件的未压缩 value 总大小（字节）
   *   - 来源：file_meta->raw_value_size 的累计值
   *   - FileMetaData::raw_value_size：
   *     * 该文件中所有 value 的未压缩大小之和
   *     * 删除条目没有 value（删除操作只有 key）
   *     * 在 Flush/Compaction 时计算并保存
   *
   * accumulated_file_size_：
   *   - 含义：所有采样文件的实际文件总大小（字节）
   *   - 来源：file_meta->fd.GetFileSize() 的累计值
   *   - FileMetaData::fd.file_size：
   *     * 该文件在磁盘上的实际大小（压缩后）
   *     * 包括数据块、索引块、元数据块等所有内容
   *
   * accumulated_num_non_deletions_：
   *   - 含义：所有采样文件的非删除条目总数
   *   - 来源：(file_meta->num_entries - file_meta->num_deletions) 的累计值
   *   - FileMetaData::num_entries：
   *     * 该文件中所有条目的总数（包括删除和非删除）
   *   - FileMetaData::num_deletions：
   *     * 该文件中删除条目的总数（包括单点删除和范围删除）
   *
   * 采样策略（重要）：
   *   - 不是所有文件都会被采样，最多采样 kMaxInitCount = 20 个文件
   *   - 从 L0 到更高层，逐层扫描，找到前 20 个未初始化的文件
   *   - 优先采样低层文件（L0 > L1 > L2 ...）
   *   - 原因：低层文件更新频繁，更能反映当前工作负载
   *   - 限制采样数量是为了控制 I/O 开销（读取文件元数据需要 I/O）
   *
   * 何时更新统计数据？
   *   - 新 Version 创建时（Version::UpdateAccumulatedStats）
   *   - 文件元数据从磁盘加载后（MaybeInitializeFileMetaData）
   *   - 文件通过 Flush 或 Compaction 创建后
   *
   * 为什么要采样而不是统计所有文件？
   *   - 减少 I/O 开销（读取 Table Properties 需要 I/O）
   *   - 对于大型 DB，统计所有文件成本太高
   *   - 采样 20 个文件足够获得有代表性的平均值
   *   - 统计数据只是估计值，不需要精确到所有文件
   *
   * 特殊情况处理：
   *   - 如果 accumulated_num_non_deletions_ == 0，返回 0
   *     * 说明没有非删除条目（可能只有删除条目或文件为空）
   *     * 此时无法计算平均值，返回 0
   *
   * 使用场景：
   *   - VersionStorageInfo::ComputeCompensatedSizes()
   *     * 估计删除条目节省的空间
   *     * 计算补偿文件大小（compensated_file_size）
   *     * 公式：补偿大小 += 额外删除数 × avg_value_size × 2
   *
   * @return 平均 Value 大小（字节），如果无法计算则返回 0
   *
   * @note 这是压缩后的平均 value 大小，不是未压缩的
   * @note 采样数量限制：kMaxInitCount = 20
   * @note 采样策略：优先低层文件（L0 -> L1 -> L2 ...）
   * @note 统计数据会从一个 Version 传递到下一个 Version（通过复制）
   *
   * @see UpdateAccumulatedStats 更新累计统计数据
   * @see MaybeInitializeFileMetaData 初始化文件元数据
   * @see ComputeCompensatedSizes 使用此函数计算补偿大小
   */
  uint64_t GetAverageValueSize() const {
    // 如果没有非删除条目，无法计算平均值，返回 0
    if (accumulated_num_non_deletions_ == 0) {
      return 0;
    }
    // 断言：必须有数据（raw_key_size 和 raw_value_size 不能同时为 0）
    assert(accumulated_raw_key_size_ + accumulated_raw_value_size_ > 0);
    // 断言：必须有文件大小（accumulated_file_size_ 不能为 0）
    assert(accumulated_file_size_ > 0);
    // 计算压缩后的平均 value 大小：
    // 公式 = (未压缩平均 value 大小) × (压缩比率)
    //       = (raw_value_size / num_non_deletions) × (file_size / (raw_key_size + raw_value_size))
    return accumulated_raw_value_size_ / accumulated_num_non_deletions_ *
           accumulated_file_size_ /
           (accumulated_raw_key_size_ + accumulated_raw_value_size_);
  }

  uint64_t GetEstimatedActiveKeys() const;

  double GetEstimatedCompressionRatioAtLevel(int level) const;

  // re-initializes the index that is used to offset into
  // files_by_compaction_pri_
  // to find the next compaction candidate file.
  void ResetNextCompactionIndex(int level) {
    next_file_to_compact_by_size_[level] = 0;
  }

  const InternalKeyComparator* InternalComparator() const {
    return internal_comparator_;
  }

  // Returns maximum total bytes of data on a given level.
  uint64_t MaxBytesForLevel(int level) const;

  // Returns an estimate of the amount of live data in bytes.
  uint64_t EstimateLiveDataSize() const;

  uint64_t estimated_compaction_needed_bytes() const {
    return estimated_compaction_needed_bytes_;
  }

  void TEST_set_estimated_compaction_needed_bytes(uint64_t v) {
    estimated_compaction_needed_bytes_ = v;
  }

  bool force_consistency_checks() const { return force_consistency_checks_; }

  SequenceNumber bottommost_files_mark_threshold() const {
    return bottommost_files_mark_threshold_;
  }

  // Returns whether any key in [`smallest_key`, `largest_key`] could appear in
  // an older L0 file than `last_l0_idx` or in a greater level than `last_level`
  //
  // @param last_level Level after which we check for overlap
  // @param last_l0_idx If `last_level == 0`, index of L0 file after which we
  //    check for overlap; otherwise, must be -1
  bool RangeMightExistAfterSortedRun(const Slice& smallest_user_key,
                                     const Slice& largest_user_key,
                                     int last_level, int last_l0_idx);

 private:
  /**
   * internal_comparator_ - 内部键比较器
   *
   * 功能概述:
   *   - 指向 InternalKeyComparator，用于比较内部键
   *   - 封装了用户比较器和序列号比较逻辑
   *   - 用于 SSTable 文件中键的排序和查找
   *
   * 包含的比较逻辑:
   *   1. 用户键比较:
   *   - 使用 user_comparator_ 比较用户键部分
   *   - 支持自定义的比较器
   *
   *   2. 序列号比较:
   *   - 对于相同用户键，按序列号降序排列
   *   - 新的记录排在前面
   *
   *   3. 值类型比较:
   *   - 考虑键的类型（Put、Delete、Merge 等）
   *
   * 使用场景:
   *   1. 文件排序:
   *   - 保持各层文件的有序性
   *   - 文件内部键有序
   *
   *   2. 文件查找:
   *   - 二分查找文件
   *   - 查找文件中的键
   *
   *   3. 范围查询:
   *   - 确定键范围
   *   - 查找重叠文件
   *
   *   4. Compaction:
   *   - 确定输入文件的范围
   *   - 合并有序的键值对
   *
   * 线程安全性:
   *   - 指针本身不可变
   *   - Comparator 本身通常是线程安全的
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 比较器必须在整个生命周期内有效
   *   - 用户定义的比较器必须满足严格弱序
   */
  const InternalKeyComparator* internal_comparator_;

  /**
   * user_comparator_ - 用户键比较器
   *
   * 功能概述:
   *   - 指向用户自定义的比较器（Comparator）
   *   - 用于比较用户键部分
   *   - 由 internal_comparator_ 封装使用
   *
   * 包含的比较逻辑:
   *   - 用户定义的比较逻辑
   *   - 可以是 BytewiseComparator、ReverseBytewiseComparator
   *   - 或用户自定义的比较器
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 比较用户键进行查找
   *   - 支持前缀查询
   *
   *   2. 范围查询:
   *   - 确定用户键范围
   *   - 查找键的边界
   *
   *   3. 迭代器:
   *   - Seek 操作
   *   - 遍历有序的键
   *
   * 线程安全性:
   *   - Comparator 本身通常是线程安全的
   *   - 取决于具体实现
   *
   * 注意事项:
   *   - 不转移所有权
   *   - 比较器必须在整个生命周期内有效
   *   - 用户比较器必须满足严格弱序
   *   - 通常通过 internal_comparator_ 访问
   */
  const Comparator* user_comparator_;

  /**
   * num_levels_ - 层级数量
   *
   * 功能概述:
   *   - 整数，表示 LSM 树的层级数量
   *   - 通常为 7 层（L0 - L6）
   *   - 可以通过配置调整
   *
   * 层级结构:
   *   - L0: 特殊层级，文件可能有重叠
   *   - L1-L6: 每层内部文件不重叠
   *   - 层级越大，文件越大，越稳定
   *
   * 使用场景:
   *   1. 数组分配:
   *   - 分配文件数组的大小
   *   - 分配相关元数据数组
   *
   *   2. 层级遍历:
   *   - 遍历所有层
   *   - 访问特定层
   *
   *   3. 边界检查:
   *   - 验证层级索引的有效性
   *
   * 配置:
   *   - 由列族选项配置
   *   - 通常为 7 层
   *   - 可以通过 ReduceNumberOfLevels 减少
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时设置
   *   - 整个生命周期内不变
   *
   * 线程安全性:
   *   - 不可变，多线程安全读取
   *
   * 注意事项:
   *   - const 成员，构造后不再修改
   *   - 通常是 7 层，但可以调整
   *   - 减少层级需要重启数据库
   */
  int num_levels_;            // Number of levels

  /**
   * num_non_empty_levels_ - 非空层级数量
   *
   * 功能概述:
   *   - 整数，记录非空层级的数量
   *   - 大于等于此索引的层级保证为空
   *   - 用于优化层级遍历
   *
   * 特性:
   *   - L0 可能是空的（刚启动时）
   *   - 高层级可能为空（未达到）
   *   - 动态更新
   *
   * 使用场景:
   *   1. 层级遍历优化:
   *   - 只遍历非空层级
   *   - 跳过空层级
   *
   *   2. Compaction 选择:
   *   - 只考虑非空层级
   *   - 避免不必要的检查
   *
   *   3. 统计:
   *   - 计算实际使用的层级
   *
   * 更新时机:
   *   - PrepareForVersionAppend 时计算
   *   - 每次版本变化时更新
   *
   * 并发控制:
   *   - 需要在 finalized_ 为 true 时才能访问
   *   - 读取需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 必须在 finalized_ 为 true 后才能访问
   *   - 大于此索引的层级保证为空
   *   - 不是 const，版本变化时会更新
   *   - 用于优化性能
   */
  int num_non_empty_levels_;  // Number of levels. Any level larger than it
                              // is guaranteed to be empty.

  /**
   * level_max_bytes_ - 每层最大字节数
   *
   * 功能概述:
   *   - 向量，存储每层的最大字节数限制
   *   - 用于确定是否需要 Compaction
   *   - 根据配置动态计算
   *
   * 计算规则:
   *   - L0: 由 write_buffer_size * max_write_buffer_number 控制
   *   - L1: 由 max_bytes_for_level_base 控制
   *   - L2+: L1 * max_bytes_for_level_multiplier^(level-1)
   *
   * 使用场景:
   *   1. Compaction 触发:
   *   - 比较当前大小与限制
   *   - 计算 Compaction 分数
   *
   *   2. 资源管理:
   *   - 控制每层的存储使用
   *   - 平衡层级大小
   *
   *   3. 动态调整:
   *   - level_compaction_dynamic_level_bytes 时动态调整
   *
   * 配置:
   *   - max_bytes_for_level_base（L1 大小）
   *   - max_bytes_for_level_multiplier（倍数）
   *   - write_buffer_size（L0）
   *
   * 生命周期:
   *   - PrepareForVersionAppend 时计算
   *   - 版本变化时重新计算
   *
   * 并发控制:
   *   - 读取需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 不是 const，版本变化时会更新
   *   - 受可变选项影响
   *   - 动态层级模式下会动态调整
   */
  // Per-level max bytes
  std::vector<uint64_t> level_max_bytes_;

  /**
   * level_files_brief_ - 每层文件简要信息
   *
   * 功能概述:
   *   - 向量，存储每层的文件简要信息（LevelFilesBrief）
   *   - LevelFilesBrief 包含文件的键范围和文件指针
   *   - 用于快速查找和范围查询
   *
   * 包含的信息:
   *   - 文件数量
   *   - 文件指针数组（FdWithKeyRange*）
   *   - 键范围（smallest_key 和 largest_key）
   *
   * 使用场景:
   *   1. 范围查询:
   *   - 快速查找重叠的文件
   *   - 确定查询范围
   *
   *   2. 文件查找:
   *   - 二分查找文件
   *   - 快速定位文件
   *
   *   3. Compaction:
   *   - 确定输入文件范围
   *   - 计算输出文件大小
   *
   * 内存优化:
   *   - 使用 Arena 分配连续内存
   *   - 减少内存碎片
   *   - 提高缓存命中率
   *
   * 生命周期:
   *   - GenerateLevelFilesBrief 时生成
   *   - 使用 arena_ 分配内存
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *   - 读取需要持有 Version 引用
   *
   * 注意事项:
   *   - 必须在 finalized_ 为 true 后才能访问
   *   - 文件指针数组的内存由 arena_ 管理
   *   - 用于性能优化的数据结构
   */
  // A short brief metadata of files per level
  autovector<ROCKSDB_NAMESPACE::LevelFilesBrief> level_files_brief_;

  /**
   * file_indexer_ - 文件索引器
   *
   * 功能概述:
   *   - FileIndexer 对象，提供快速文件索引
   *   - 支持根据键范围快速查找文件
   *   - 优化多键查找性能
   *
   * 功能:
   *   1. 快速查找:
   *   - 根据键范围快速定位文件
   *   - 支持 MultiGet 操作
   *
   *   2. 范围查询:
   *   - 快速确定需要读取的文件
   *   - 减少不必要的文件打开
   *
   *   3. 索引优化:
   *   - 建立层级间的索引关系
   *   - 支持跨层查询
   *
   * 使用场景:
   *   1. MultiGet:
   *   - 快速定位多个键所在的文件
   *   - 减少 I/O 操作
   *
   *   2. 范围查询:
   *   - 快速确定查询范围
   *   - 优化查找性能
   *
   *   3. Compaction:
   *   - 确定需要合并的文件
   *
   * 生命周期:
   *   - GenerateFileIndexer 时生成
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *   - 读取需要持有 Version 引用
   *
   * 注意事项:
   *   - 必须在 finalized_ 为 true 后才能访问
   *   - 用于性能优化的数据结构
   *   - 多键查询时特别有用
   */
  FileIndexer file_indexer_;

  /**
   * arena_ - 内存分配器
   *
   * 功能概述:
   *   - Arena 对象，用于批量分配内存
   *   - 管理临时内存的分配和释放
   *   - 提高性能，减少内存碎片
   *
   * 用途:
   *   1. level_files_brief_ 的内存分配
   *   - 分配文件指针数组
   *   - 分配键范围数据
   *
   *   2. FileIndexer 的内存分配
   *   - 分配索引数据结构
   *
   *   3. 其他临时内存需求
   *
   * 优势:
   *   - 批量分配，提高速度
   *   - 减少内存碎片
   *   - 简化内存管理（一次性释放）
   *
   * 使用场景:
   *   - GenerateLevelFilesBrief
   *   - GenerateFileIndexer
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时创建
   *   - VersionStorageInfo 销毁时自动释放
   *
   * 注意事项:
   *   - 分配的内存无法单独释放
   *   - 整个 Arena 一起释放
   *   - 适合临时对象和批量操作
   */
  Arena arena_;  // Used to allocate space for file_levels_

  /**
   * compaction_style_ - Compaction 风格
   *
   * 功能概述:
   *   - 枚举，表示 Compaction 的策略
   *   - 决定 Compaction 如何选择和合并文件
   *   - 影响数据库的写入和读取性能
   *
   * 可选值:
   *   1. kCompactionStyleLevel: 层级压缩（默认）
   *      - Leveled Compaction
   *      - 每层文件大小有严格限制
   *      - 适合读多写少的场景
   *
   *   2. kCompactionStyleUniversal: 通用压缩
   *      - Universal Compaction
   *      - 类似 LevelDB 的策略
   *      - 适合写多读少的场景
   *
   *   3. kCompactionStyleFIFO: 先进先出压缩
   *      - FIFO Compaction
   *      - 按时间顺序删除旧文件
   *      - 适合时序数据
   *
   *   4. kCompactionStyleNone: 无压缩
   *      - 不执行 Compaction
   *   - 只 Flush 到 L0
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - 选择不同的 Compaction 策略
   *   - 计算不同的 Compaction 分数
   *
   *   2. 文件选择:
   *   - 不同策略有不同的文件选择逻辑
   *
   *   3. 性能调优:
   *   - 根据工作负载选择合适的策略
   *
   * 配置:
   *   - 由列族选项配置
   *   - 可以动态修改
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时设置
   *   - 通常不变，除非配置修改
   *
   * 注意事项:
   *   - 影响 Compaction 的所有决策
   *   - 不同策略有不同的性能特征
   *   - 选择时要考虑工作负载
   */
  CompactionStyle compaction_style_;

  /**
   * files_ - 每层文件列表
   *
   * 功能概述:
   *   - 向量指针，指向每层的文件元数据数组
   *   - 每层是一个 FileMetaData* 的向量
   *   - 文件按键升序排列
   *
   * 文件元数据（FileMetaData）:
   *   - 文件号
   *   - 文件大小
   *   - 键范围（smallest 和 largest）
   *   - 序列号范围
   *   - 压缩统计信息
   *   - TTL 信息
   *   - 是否被标记为需要 Compaction
   *
   * 文件排序:
   *   - L0: 文件可能重叠，按序列号排序
   *   - L1-L6: 文件不重叠，按键范围排序
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 查找键所在的文件
   *   - 构建迭代器
   *
   *   2. Compaction:
   *   - 选择输入文件
   *   - 计算输出文件大小
   *
   *   3. 范围查询:
   *   - 查找重叠的文件
   *   - 确定查询范围
   *
   *   4. 统计:
   *   - 计算每层的文件数量和大小
   *
   * 内存管理:
   *   - 数组本身由 VersionStorageInfo 管理
   *   - FileMetaData 对象由 VersionSet 管理
   *
   * 并发控制:
   *   - 必须在 finalized_ 为 true 后才能访问
   *   - 读取需要持有 Version 引用
   *
   * 注意事项:
   *   - 必须在 finalized_ 为 true 后才能访问
   *   - 文件元数据可能被其他版本共享
   *   - 修改需要创建新版本
   */
  // List of files per level, files in each level are arranged
  // in increasing order of keys
  std::vector<FileMetaData*>* files_;

  /**
   * file_locations_ - 文件位置映射
   *
   * 功能概述:
   *   - 无序映射，将文件号映射到文件位置
   *   - 快速查找文件所在的层级和位置
   *   - 优化文件查找性能
   *
   * 映射内容:
   *   - Key: 文件号（uint64_t）
   *   - Value: FileLocation（层级 + 位置）
   *
   * FileLocation:
   *   - level_: 文件所在的层级
   *   - position_: 文件在该层级的位置（索引）
   *
   * 使用场景:
   *   1. 文件查找:
   *   - 根据文件号快速找到文件
   *   - 避免遍历所有层级
   *
   *   2. Compaction:
   *   - 快速定位文件
   *   - 处理文件移动和删除
   *
   *   3. 恢复:
   *   - 根据 MANIFEST 恢复文件位置
   *
   * 性能优化:
   *   - 无序映射，平均 O(1) 查找
   *   - 比遍历所有层级快得多
   *
   * 生命周期:
   *   - GenerateFileLocationIndex 时生成
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *   - 读取需要持有 Version 引用
   *
   * 注意事项:
   *   - 必须在 finalized_ 为 true 后才能访问
   *   - 文件号在数据库中唯一
   *   - 用于性能优化的数据结构
   */
  // Map of all table files in version. Maps file number to (level, position on
  // level).
  using FileLocations = UnorderedMap<uint64_t, FileLocation>;
  FileLocations file_locations_;

  /**
   * blob_files_ - Blob 文件列表
   *
   * 功能概述:
   *   - 向量，存储所有的 Blob 文件元数据
   *   - Blob 文件用于存储大值
   *   - 按 Blob 文件号排序
   *
   * BlobDB 功能:
   *   - 将大值存储在独立的 Blob 文件中
   *   - SSTable 中存储 Blob 文件的引用（BlobIndex）
   *   - 支持大值的高效存储和访问
   *
   * BlobFileMetaData:
   *   - Blob 文件号
   *   - 文件大小
   *   - 垃圾数据大小
   *   - TTL 信息
   *   - 是否过期
   *
   * 使用场景:
   *   1. 读取大值:
   *   - 根据 BlobIndex 查找 Blob 文件
   *   - 从 Blob 文件读取数据
   *
   *   2. Blob GC:
   *   - 清理过期的 Blob 数据
   *   - 回收垃圾数据
   *
   *   3. Compaction:
   *   - 处理 Blob 文件的引用
   *   - 清理无用的 Blob 文件
   *
   * 配置:
   *   - 启用 BlobDB 功能时创建
   *   - 由列族选项配置
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时初始化
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要持有 Version 引用
   *
   * 注意事项:
   *   - 使用共享指针管理 BlobFileMetaData
   *   - 可能为空（未启用 BlobDB）
   *   - 按文件号排序
   */
  // Vector of blob files in version sorted by blob file number.
  BlobFiles blob_files_;

  /**
   * base_level_ - 基础层级
   *
   * 功能概述:
   *   - 整数，表示 L0 数据应该压缩到的层级
   *   - 所有小于 base_level_ 的层级应该是空的
   *   - 仅适用于 Leveled Compaction
   *
   * 含义:
   *   - L0 数据压缩到 base_level_
   *   - 例如: base_level_ = 2 表示 L0 压缩到 L2
   *   - L1 应该是空的（或很少文件）
   *
   * 计算规则:
   *   - 基于 L1 的大小和目标大小
   *   - 如果 L1 未达到目标大小，base_level_ = 1
   *   - 如果 L1 已满，base_level_ > 1
   *
   * 使用场景:
   *   1. Compaction:
   *   - 确定 L0 数据的目标层级
   *   - 计算 Compaction 的范围
   *
   *   2. 动态层级调整:
   *   - 动态调整 L1 的大小
   *   - 平衡各层的大小
   *
   *   3. 性能优化:
   *   - 减少不必要的 Compaction
   *   - 优化写入放大
   *
   * 配置:
   *   - level_compaction_dynamic_level_bytes = true 时动态计算
   *   - 否则固定为 1
   *
   * 生命周期:
   *   - CalculateBaseBytes 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 非 Leveled Compaction 时为 -1
   *   - 动态层级模式下会变化
   *   - 影响 L0 Compaction 的目标
   */
  // Level that L0 data should be compacted to. All levels < base_level_ should
  // be empty. -1 if it is not level-compaction so it's not applicable.
  int base_level_;

  /**
   * lowest_unnecessary_level_ - 最低不需要层级
   *
   * 功能概述:
   *   - 整数，表示最低的"不需要"层级
   *   - 所有小于等于此层级的非空层都不需要，会自动排空
   *   - 仅适用于 level_compaction_dynamic_level_bytes=true
   *
   * 含义:
   *   - 这些层级的数据可以移动到更高层级
   *   - 用于动态调整层级大小
   *   - 减少层级数量，简化管理
   *
   * 使用场景:
   *   1. 动态层级调整:
   *   - 减少不必要的层级
   *   - 优化存储布局
   *
   *   2. Compaction:
   *   - 将数据从低层级移到高层级
   *   - 排空不需要的层级
   *
   *   3. 性能优化:
   *   - 减少读取路径的层级
   *   - 降低读取延迟
   *
   * 配置:
   *   - level_compaction_dynamic_level_bytes = true 时启用
   *   - 基于 L1 的目标大小计算
   *
   * 生命周期:
   *   - CalculateBaseBytes 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 没有不需要的层级时为 -1
   *   - 仅动态层级模式使用
   *   - 非空层级会逐步排空
   */
  // Applies to level compaction when
  // `level_compaction_dynamic_level_bytes=true`. All non-empty levels <=
  // lowest_unnecessary_level_ are not needed and will be drained automatically.
  // -1 if there is no unnecessary level,
  int lowest_unnecessary_level_;

  /**
   * level_multiplier_ - 层级倍数
   *
   * 功能概述:
   *   - 双精度浮点数，表示层级大小的倍数
   *   - 用于计算各层级的大小限制
   *   - 影响 LSM 树的形状和性能
   *
   * 计算规则:
   *   - L1 大小 = max_bytes_for_level_base
   *   - L2 大小 = L1 × level_multiplier
   *   - L3 大小 = L2 × level_multiplier
   *   - ...
   *   - Ln 大小 = Ln-1 × level_multiplier^(n-1)
   *
   * 默认值:
   *   - 通常为 10.0
   *   - 可以通过配置调整
   *
   * 性能影响:
   *   - 倍数越大:
   *     - 写入放大越小
   *     - 读取延迟越大
   *     - 空间放大越大
   *
   *   - 倍数越小:
   *     - 写入放大越大
   *     - 读取延迟越小
   *     - 空间放大越小
   *
   * 使用场景:
   *   1. Compaction 分数计算:
   *   - 计算各层的目标大小
   *   - 判断是否需要 Compaction
   *
   *   2. 资源管理:
   *   - 控制各层的存储使用
   *
   *   3. 性能调优:
   *   - 调整倍数平衡性能
   *
   * 配置:
   *   - max_bytes_for_level_multiplier
   *   - 可以动态修改
   *
   * 生命周期:
   *   - PrepareForVersionAppend 时设置
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 影响 LSM 树的整体性能
   *   - 需要根据工作负载调整
   *   - 典型值为 10.0
   */
  double level_multiplier_;

  /**
   * files_by_compaction_pri_ - 按压缩优先级排序的文件
   *
   * 功能概述:
   *   - 向量，存储按文件大小排序的文件索引
   *   - 每层按文件大小降序排列
   *   - 用于优先选择大文件进行 Compaction
   *
   * 存储内容:
   *   - 每层是一个索引向量
   *   - 索引指向 files_[level] 中的文件
   *   - 文件按大小降序排列
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - 优先选择大文件
   *   - 减少小文件的 Compaction
   *
   *   2. 性能优化:
   *   - 减少文件数量
   *   - 降低读取延迟
   *
   *   3. 优先级管理:
   *   - 根据文件大小确定 Compaction 优先级
   *
   * 排序优化:
   *   - 只排序前 50 个文件
   *   - 减少排序开销
   *   - 大多数情况只需前几个文件
   *
   * 生命周期:
   *   - UpdateFilesByCompactionPri 时生成
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 只排序前 50 个文件
   *   - 需要在 finalized_ 为 true 后才能访问
   *   - 用于 Compaction 优化
   */
  // A list for the same set of files that are stored in files_,
  // but files in each level are now sorted based on file
  // size. The file with the largest size is at the front.
  // This vector stores the index of the file from files_.
  std::vector<std::vector<int>> files_by_compaction_pri_;

  /**
   * level0_non_overlapping_ - L0 文件是否不重叠
   *
   * 功能概述:
   *   - 布尔值，表示 L0 文件的键是否不重叠
   *   - 为 true 时可以优化 L0 的读取路径
   *   - 为 false 时需要检查所有 L0 文件
   *
   * 含义:
   *   - true: L0 文件按键范围不重叠，可以二分查找
   *   - false: L0 文件可能重叠，需要检查所有文件
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 为 true 时可以优化查找
   *   - 为 false 时需要检查所有 L0 文件
   *
   *   2. 范围查询:
   *   - 为 true 时可以快速确定查询范围
   *   - 为 false 时需要检查所有文件
   *
   *   3. 迭代器:
   *   - 为 true 时可以优化迭代器
   *
   * 计算时机:
   *   - GenerateLevel0NonOverlapping 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要持有 Version 引用
   *
   * 注意事项:
   *   - L0 通常可能重叠
   *   - 不重叠时可以大幅优化读取性能
   *   - 某些情况下可能为 true（如使用 FIFO）
   */
  // If true, means that files in L0 have keys with non overlapping ranges
  bool level0_non_overlapping_;

  /**
   * next_file_to_compact_by_size_ - 下一个按大小压缩的文件索引
   *
   * 功能概述:
   *   - 向量，存储每层下一个要压缩的文件索引
   *   - 索引指向 files_by_compaction_pri_[level]
   *   - 用于轮询式选择 Compaction 文件
   *
   * 功能:
   *   - 记录每层下一个要压缩的文件位置
   *   - 避免 Compaction 总是选择相同的文件
   *   - 实现轮询式 Compaction
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - 从索引处开始选择文件
   *   - Compaction 后更新索引
   *
   *   2. 轮询式 Compaction:
   *   - 遍历所有大文件
   *   - 避免某些文件被忽略
   *
   *   3. 并行 Compaction:
   *   - 多个 Compaction 任务使用不同的索引
   *
   * 更新规则:
   *   - Compaction 后向前移动索引
   *   - 可能需要考虑文件增加/减少
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *
   * 注意事项:
   *   - 每层一个索引
   *   - Compaction 后需要更新
   *   - 可以重置（ResetNextCompactionIndex）
   */
  // An index into files_by_compaction_pri_ that specifies the first
  // file that is not yet compacted
  std::vector<int> next_file_to_compact_by_size_;

  /**
   * number_of_files_to_sort_ - 需要排序的文件数量
   *
   * 功能概述:
   *   - 静态常量，表示需要排序的文件数量
   *   - 只排序前 N 个最大的文件
   *   - 优化性能，减少排序开销
   *
   * 设计考虑:
   *   - 只需看前几个最大的文件
   *   - 新版本经常创建（几秒/几分钟）
   *   - 大多数情况下只需前 50 个文件
   *
   * 性能优化:
   *   - 减少排序时间
   *   - O(N log K) vs O(N log N)
   *   - K = 50, N 可能有数千
   *
   * 使用场景:
   *   1. UpdateFilesByCompactionPri
   *   - 只排序前 50 个文件
   *   - 其余文件不保证有序
   *
   *   2. Compaction 选择:
   *   - 从排序的文件中选择
   *   - 优先选择大文件
   *
   * 注意事项:
   *   - 静态常量，值为 50
   *   - 只适用于 files_by_compaction_pri_
   *   - 未排序的文件可能在后面被选择
   */
  // Only the first few entries of files_by_compaction_pri_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t number_of_files_to_sort_ = 50;

  /**
   * files_marked_for_compaction_ - 标记为需要压缩的文件
   *
   * 功能概述:
   *   - 自动扩展向量，存储标记为需要 Compaction 的文件
   *   - 每个元素是 (层级, 文件元数据) 对
   *   - 这些文件没有被当前正在进行的 Compaction 处理
   *
   * 标记原因:
   *   1. 手动标记:
   *   - CompactFiles() API 手动标记
   *
   *   2. Compaction 重新调度:
   *   - Compaction 失败或取消后标记
   *
   *   3. 其他条件:
   *   - 文件满足某些特定条件
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - 优先处理标记的文件
   *   - 确保标记的文件被处理
   *
   *   2. 手动 Compaction:
   *   - 处理用户指定的文件
   *
   *   3. 错误恢复:
   *   - 重试失败的 Compaction
   *
   * 更新时机:
   *   - ComputeCompactionScore 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - Leveled 和 Universal Compaction 都使用
   *   - 排除正在被压缩的文件
   *   - 标记后会被清除
   */
  // This vector contains list of files marked for compaction and also not
  // currently being compacted. It is protected by DB mutex. It is calculated in
  // ComputeCompactionScore(). Used by Leveled and Universal Compaction.
  autovector<std::pair<int, FileMetaData*>> files_marked_for_compaction_;

  /**
   * expired_ttl_files_ - TTL 过期的文件
   *
   * 功能概述:
   *   - 自动扩展向量，存储 TTL 过期的文件
   *   - 每个元素是 (层级, 文件元数据) 对
   *   - 这些文件需要尽快 Compaction
   *
   * TTL 机制:
   *   - 文件创建时有 TTL
   *   - 超过 TTL 时间后过期
   *   - 过期文件需要 Compaction 清理
   *
   * 使用场景:
   *   1. Leveled Compaction:
   *   - 优先处理过期的文件
   *   - 清理过期数据
   *
   *   2. 数据生命周期管理:
   *   - 自动清理过期数据
   *   - 节省存储空间
   *
   *   3. 合规性:
   *   - 满足数据保留策略
   *
   * 更新时机:
   *   - ComputeExpiredTtlFiles 时计算
   *   - 版本变化时更新
   *
   * 配置:
   *   - 通过列族选项配置 TTL
   *   - TTL 以秒为单位
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 仅 Leveled Compaction 使用
   *   - 过期文件会被标记
   *   - 需要及时 Compaction
   */
  autovector<std::pair<int, FileMetaData*>> expired_ttl_files_;

  /**
   * files_marked_for_periodic_compaction_ - 标记为周期性压缩的文件
   *
   * 功能概述:
   *   - 自动扩展向量，存储标记为周期性 Compaction 的文件
   *   - 每个元素是 (层级, 文件元数据) 对
   *   - 这些文件超过了周期性 Compaction 的时间间隔
   *
   * 周期性 Compaction:
   *   - 文件创建后一定时间内需要 Compaction
   *   - 即使文件大小不大也需要 Compaction
   *   - 用于清理删除的数据
   *
   * 使用场景:
   *   1. 清理删除数据:
   *   - 删除的数据占用空间
   *   - 周期性 Compaction 回收空间
   *
   *   2. 数据更新:
   *   - 确保数据定期更新
   *   - 优化读取性能
   *
   *   3. 空间回收:
   *   - 回收垃圾数据的空间
   *
   * 配置:
   *   - periodic_compaction_seconds（周期时间）
   *   - 以秒为单位
   *
   * 更新时机:
   *   - ComputeFilesMarkedForPeriodicCompaction 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - Leveled 和 Universal Compaction 都使用
   *   - 超过周期的文件会被标记
   *   - 用于垃圾回收优化
   */
  autovector<std::pair<int, FileMetaData*>>
      files_marked_for_periodic_compaction_;

  /**
   * bottommost_files_ - 底层文件列表
   *
   * 功能概述:
   *   - 自动扩展向量，存储所有的底层文件
   *   - 底层文件是指键不会出现在更低层的文件
   *   - 这些文件不一定在同一层级
   *
   * 底层文件定义:
   *   - 文件中的键在所有更低层都不存在
   *   - L6 的文件总是底层文件（最底层）
   *   - 如果某些层为空，较高层的文件也可能是底层文件
   *
   * 使用场景:
   *   1. 确定 Compaction 候选:
   *   - 检查底层文件是否需要 Compaction
   *   - 清理重复的键版本
   *
   *   2. 垃圾回收:
   *   - 底层文件的删除数据可以安全删除
   *   - 不影响更低层
   *
   *   3. 快照管理:
   *   - 快照影响底层文件的 Compaction
   *
   * 更新时机:
   *   - GenerateBottommostFiles 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 包含所有底层文件
   *   - 不一定都是 L6 文件
   *   - 与 bottommost_files_marked_for_compaction_ 配合
   */
  // These files are considered bottommost because none of their keys can exist
  // at lower levels. They are not necessarily all in the same level. The marked
  // ones are eligible for compaction because they contain duplicate key
  // versions that are no longer protected by snapshot. These variables are
  // protected by DB mutex and are calculated in `GenerateBottommostFiles()` and
  // `ComputeBottommostFilesMarkedForCompaction()`.
  autovector<std::pair<int, FileMetaData*>> bottommost_files_;

  /**
   * bottommost_files_marked_for_compaction_ - 标记为需要压缩的底层文件
   *
   * 功能概述:
   *   - 自动扩展向量，存储标记为需要 Compaction 的底层文件
   *   - 这些文件包含重复的键版本，不再受快照保护
   *   - 可以安全地 Compaction 以回收空间
   *
   * 标记条件:
   *   - 是底层文件（bottommost_files_）
   *   - 包含键的多个版本
   *   - 旧版本的序列号 < 最旧快照的序列号
   *   - 旧版本可以安全删除
   *
   * 快照保护:
   *   - 快照保护旧的键版本
   *   - 快照释放后，旧版本可以删除
   *   - 底层文件的重复键可以合并
   *
   * 使用场景:
   *   1. 空间回收:
   *   - Compaction 后删除旧版本
   *   - 回收存储空间
   *
   *   2. 性能优化:
   *   - 减少读取时需要检查的版本
   *   - 降低读取延迟
   *
   *   3. 快照管理:
   *   - 快照释放后更新标记
   *   - 释放不再需要的数据
   *
   * 更新时机:
   *   - ComputeBottommostFilesMarkedForCompaction 时计算
   *   - 快照变化时更新
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 只包含底层文件
   *   - 受快照保护机制影响
   *   - Compaction 后会被清除
   */
  autovector<std::pair<int, FileMetaData*>>
      bottommost_files_marked_for_compaction_;

  /**
   * files_marked_for_forced_blob_gc_ - 标记为强制 Blob GC 的文件
   *
   * 功能概述:
   *   - 自动扩展向量，存储标记为强制 Blob GC 的文件
   *   - 这些文件引用的 Blob 数据需要垃圾回收
   *   - 用于清理 Blob 文件中的垃圾数据
   *
   * Blob GC 机制:
   *   - Blob 文件中可能包含垃圾数据
   *   - 垃圾数据是指不再被任何 SSTable 引用的 Blob
   *   - GC 回收这些空间
   *
   * 标记条件:
   *   - Blob 垃圾比例超过阈值
   *   - Blob 文件年龄超过阈值
   *   - 满足强制 GC 条件
   *
   * 使用场景:
   *   1. Blob GC:
   *   - 强制执行 Blob 垃圾回收
   *   - 回收 Blob 文件空间
   *
   *   2. 空间管理:
   *   - 控制 Blob 文件的空间使用
   *   - 优化存储效率
   *
   *   3. 性能优化:
   *   - 减少 Blob 文件大小
   *   - 提高 Blob 读取性能
   *
   * 配置:
   *   - blob_garbage_collection_age_cutoff（年龄阈值）
   *   - blob_garbage_collection_force_threshold（强制阈值）
   *
   * 更新时机:
   *   - ComputeFilesMarkedForForcedBlobGC 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 仅适用于 BlobDB
   *   - 用于强制 GC
   *   - GC 后会被清除
   */
  autovector<std::pair<int, FileMetaData*>> files_marked_for_forced_blob_gc_;

  /**
   * bottommost_files_mark_threshold_ - 底层文件标记阈值
   *
   * 功能概述:
   *   - 序列号，标记底层文件的最小最大非零序列号
   *   - 用于快速检查是否需要重新标记底层文件
   *   - 优化快照释放后的检查
   *
   * 含义:
   *   - 未标记的底层文件的最大序列号的最小值
   *   - 任何序列号 < 此阈值的文件都应该被标记
   *   - 阈值 = min(未标记底层文件的最大序列号)
   *
   * 使用场景:
   *   1. 快照释放检查:
   *   - 快照释放时检查是否需要重新标记
   *   - 避免遍历所有底层文件
   *
   *   2. 性能优化:
   *   - 快速判断是否需要更新标记
   *   - 减少计算开销
   *
   *   3. 增量更新:
   *   - 只检查受影响的文件
   *
   * 更新时机:
   *   - ComputeBottommostFilesMarkedForCompaction 时更新
   *   - 快照变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 初始值为 kMaxSequenceNumber
   *   - 用于性能优化
   *   - 与快照管理机制配合
   */
  // Threshold for needing to mark another bottommost file. Maintain it so we
  // can quickly check when releasing a snapshot whether more bottommost files
  // became eligible for compaction. It's defined as the min of the max nonzero
  // seqnums of unmarked bottommost files.
  SequenceNumber bottommost_files_mark_threshold_ = kMaxSequenceNumber;

  /**
   * oldest_snapshot_seqnum_ - 最旧快照的序列号
   *
   * 功能概述:
   *   - 序列号，记录当前最旧快照的序列号
   *   - 单调递增
   *   - 为 0 时表示没有释放过快照
   *
   * 含义:
   *   - 所有序列号 >= 此值的键都受快照保护
   *   - 序列号 < 此值的键可以安全删除
   *   - 影响底层文件的 Compaction 标记
   *
   * 使用场景:
   *   1. 底层文件标记:
   *   - 确定哪些底层文件可以 Compaction
   *   - 检查文件的键是否受快照保护
   *
   *   2. 垃圾回收:
   *   - 确定可以删除的键版本
   *   - 回收存储空间
   *
   *   3. 快照管理:
   *   - 快照释放后更新
   *   - 创建快照时不影响
   *
   * 更新时机:
   *   - 快照释放时更新（UpdateOldestSnapshot）
   *   - 单调递增
   *
   * 特殊情况:
   *   - 0: 没有释放过快照
   *   - 所有快照释放后: 设置为当前序列号
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 单调递增
   *   - 影响底层文件的 Compaction
   *   - 需要保护（可能仍有快照引用它）
   */
  // Monotonically increases as we release old snapshots. Zero indicates no
  // snapshots have been released yet. When no snapshots remain we set it to the
  // current seqnum, which needs to be protected as a snapshot can still be
  // created that references it.
  SequenceNumber oldest_snapshot_seqnum_ = 0;

  /**
   * compaction_score_ - 压缩分数
   *
   * 功能概述:
   *   - 向量，存储各层的 Compaction 分数
   *   - 分数越高，越需要 Compaction
   *   - 用于确定下一层要 Compaction 的层级
   *
   * 分数含义:
   *   - 分数 = 当前大小 / 目标大小
   *   - 分数 < 1: 不需要 Compaction
   *   - 分数 >= 1: 需要 Compaction
   *   - 分数越大，越紧急
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - 选择分数最高的层级
   *   - 确定优先级
   *
   *   2. 调度:
   *   - compaction_level_[0] 分数最高
   *   - 按分数降序排列
   *
   *   3. 性能监控:
   *   - 监控各层的压缩压力
   *
   * 计算规则:
   *   - Leveled: 当前大小 / 目标大小
   *   - Universal: 基于文件数量和大小
   *   - FIFO: 基于 TTL
   *
   * 更新时机:
   *   - ComputeCompactionScore 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 与 compaction_level_ 配对使用
   *   - 按分数降序排列
   *   - 用于 Compaction 调度
   */
  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by ComputeCompactionScore.
  // The most critical level to be compacted is listed first
  // These are used to pick the best compaction level
  std::vector<double> compaction_score_;

  /**
   * compaction_level_ - 压缩层级
   *
   * 功能概述:
   *   - 向量，存储对应的 Compaction 分数的层级
   *   - 与 compaction_score_ 配对使用
   *   - 按 Compaction 分数降序排列
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - compaction_level_[0]: 最高优先级的层级
   *   - 获取对应的 Compaction 分数
   *
   *   2. 优先级管理:
   *   - 确定各层的 Compaction 优先级
   *
   *   3. 调度:
   *   - 按分数从高到低处理各层
   *
   * 示例:
   *   - compaction_level_[0] = 2, compaction_score_[0] = 2.5
   *   - compaction_level_[1] = 4, compaction_score_[1] = 1.8
   *   - 优先 Compaction L2，然后 L4
   *
   * 更新时机:
   *   - ComputeCompactionScore 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 由 DB 互斥锁保护
   *   - 读取需要持有互斥锁
   *
   * 注意事项:
   *   - 与 compaction_score_ 配对
   *   - 按分数降序排列
   *   - 用于 Compaction 调度
   */
  std::vector<int> compaction_level_;

  /**
   * l0_delay_trigger_count_ - L0 延迟触发计数
   *
   * 功能概述:
   *   - 整数，记录 L0 文件数量阈值
   *   - 用于触发写入降速和停止
   *   - 控制 L0 的写入速率
   *
   * 触发机制:
   *   - L0 文件数达到阈值时触发
   *   - 降速: 减慢写入速率
   *   - 停止: 暂停写入
   *
   * 使用场景:
   *   1. 写入控制:
   *   - L0 文件过多时降速
   *   - 避免写入放大过大
   *
   *   2. 资源管理:
   *   - 控制 L0 的大小
   *   - 给 Compaction 留出时间
   *
   *   3. 性能保护:
   *   - 防止 L0 过大
   *   - 避免读取延迟过高
   *
   * 配置:
   *   - level0_slowdown_writes_trigger
   *   - level0_stop_writes_trigger
   *
   * 更新时机:
   *   - 准备版本 Append 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 读取需要持有 DB 互斥锁
   *
   * 注意事项:
   *   - 只用于 L0
   *   - 用于写入限流
   *   - 可能影响写入性能
   */
  int l0_delay_trigger_count_ = 0;  // Count used to trigger slow down and stop
                                    // for number of L0 files.

  /**
   * compact_cursor_ - 压缩游标
   *
   * 功能概述:
   *   - 向量，存储每层的 Compaction 游标
   *   - 用于轮询式 Compaction
   *   - 每个 InternalKey 表示游标位置
   *
   * 轮询式 Compaction:
   *   - 游标标记当前 Compaction 的起始位置
   *   - 下次 Compaction 从游标之后开始
   *   - 遍历整个层级后回到起点
   *
   * 使用场景:
   *   1. 轮询式 Compaction:
   *   - 遍历所有键范围
   *   - 避免某些区域被忽略
   *
   *   2. 范围 Compaction:
   *   - 从游标位置开始
   *   - 处理游标之后的文件
   *
   *   3. 并行 Compaction:
   *   - 多个 Compaction 任务使用不同游标
   *
   * 更新规则:
   *   - Compaction 后更新游标
   *   - GetNextCompactCursor 计算下一个游标
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时初始化
   *   - Compaction 时更新
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *
   * 注意事项:
   *   - 每层一个游标
   *   - 用于轮询式 Compaction
   *   - 可以重置
   */
  // Compact cursors for round-robin compactions in each level
  std::vector<InternalKey> compact_cursor_;

  /**
   * accumulated_file_size_ - 累计文件大小
   *
   * 功能概述:
   *   - 64 位无符号整数，记录采样文件的累计文件大小
   *   - 用于计算平均 Value 大小
   *   - 采样统计信息的一部分
   *
   * 统计内容:
   *   - 采样文件的实际文件总大小（压缩后）
   *   - 包括数据块、索引块等
   *
   * 使用场景:
   *   1. 计算压缩比率:
   *   - accumulated_file_size_ / (accumulated_raw_key_size_ + accumulated_raw_value_size_)
   *   - 用于估计压缩效果
   *
   *   2. 计算平均 Value 大小:
   *   - GetAverageValueSize 使用
   *   - 估计删除条目节省的空间
   *
   *   3. Compaction 决策:
   *   - 估计 Compaction 的收益
   *
   * 采样策略:
   *   - 最多采样 20 个文件
   *   - 优先采样低层文件
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时更新
   *   - 每个采样文件添加时累计
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *
   * 注意事项:
   *   - 是采样数据，不是全部文件
   *   - 用于估计，不是精确值
   *   - 与其他 accumulated_* 变量配合
   */
  // the following are the sampled temporary stats.
  // the current accumulated size of sampled files.
  uint64_t accumulated_file_size_;

  /**
   * accumulated_raw_key_size_ - 累计原始键大小
   *
   * 功能概述:
   *   - 64 位无符号整数，记录采样文件的累计未压缩键大小
   *   - 用于计算平均 Value 大小和压缩比率
   *   - 采样统计信息的一部分
   *
   * 统计内容:
   *   - 采样文件的所有键的未压缩总大小
   *   - 包括删除条目的键
   *
   * 使用场景:
   *   1. 计算压缩比率:
   *   - accumulated_file_size_ / (accumulated_raw_key_size_ + accumulated_raw_value_size_)
   *   - 用于估计压缩效果
   *
   *   2. 计算平均 Value 大小:
   *   - GetAverageValueSize 使用
   *   - 估计删除条目节省的空间
   *
   *   3. Compaction 决策:
   *   - 估计 Compaction 的收益
   *
   * 采样策略:
   *   - 最多采样 20 个文件
   *   - 从文件元数据中获取
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时更新
   *   - 每个采样文件添加时累计
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *
   * 注意事项:
   *   - 是采样数据，不是全部文件
   *   - 未压缩的大小
   *   - 包括删除条目的键
   */
  // the current accumulated size of all raw keys based on the sampled files.
  uint64_t accumulated_raw_key_size_;

  /**
   * accumulated_raw_value_size_ - 累计原始值大小
   *
   * 功能概述:
   *   - 64 位无符号整数，记录采样文件的累计未压缩值大小
   *   - 用于计算平均 Value 大小和压缩比率
   *   - 采样统计信息的一部分
   *
   * 统计内容:
   *   - 采样文件的所有值的未压缩总大小
   *   - 删除条目没有值
   *
   * 使用场景:
   *   1. 计算平均 Value 大小:
   *   - accumulated_raw_value_size_ / accumulated_num_non_deletions_
   *   - 计算每个非删除条目的平均值大小
   *
   *   2. 计算压缩比率:
   *   - accumulated_file_size_ / (accumulated_raw_key_size_ + accumulated_raw_value_size_)
   *   - 用于估计压缩效果
   *
   *   3. Compaction 决策:
   *   - 估计 Compaction 的收益
   *
   * 采样策略:
   *   - 最多采样 20 个文件
   *   - 从文件元数据中获取
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时更新
   *   - 每个采样文件添加时累计
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *
   * 注意事项:
   *   - 是采样数据，不是全部文件
   *   - 未压缩的大小
   *   - 删除条目没有值
   */
  // the current accumulated size of all raw keys based on the sampled files.
  uint64_t accumulated_raw_value_size_;

  /**
   * accumulated_num_non_deletions_ - 累计非删除条目数
   *
   * 功能概述:
   *   - 64 位无符号整数，记录采样文件的累计非删除条目数
   *   - 用于计算平均 Value 大小
   *   - 采样统计信息的一部分
   *
   * 统计内容:
   *   - 采样文件的所有非删除条目总数
   *   - 包括 Put、Merge 等操作
   *
   * 使用场景:
   *   1. 计算平均 Value 大小:
   *   - accumulated_raw_value_size_ / accumulated_num_non_deletions_
   *   - 计算每个非删除条目的平均值大小
   *
   *   2. 计算删除比例:
   *   - accumulated_num_deletions_ / accumulated_num_non_deletions_
   *   - 估计删除数据的比例
   *
   *   3. Compaction 决策:
   *   - 估计 Compaction 的收益
   *
   * 采样策略:
   *   - 最多采样 20 个文件
   *   - 从文件元数据中获取
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时更新
   *   - 每个采样文件添加时累计
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *
   * 注意事项:
   *   - 是采样数据，不是全部文件
   *   - 不包括删除条目
   *   - 为 0 时无法计算平均值
   */
  // total number of non-deletion entries
  uint64_t accumulated_num_non_deletions_;

  /**
   * accumulated_num_deletions_ - 累计删除条目数
   *
   * 功能概述:
   *   - 64 位无符号整数，记录采样文件的累计删除条目数
   *   - 用于计算删除比例
   *   - 采样统计信息的一部分
   *
   * 统计内容:
   *   - 采样文件的所有删除条目总数
   *   - 包括单点删除和范围删除
   *
   * 使用场景:
   *   1. 计算删除比例:
   *   - accumulated_num_deletions_ / accumulated_num_non_deletions_
   *   - 估计删除数据的比例
   *
   *   2. Compaction 决策:
   *   - 估计 Compaction 的收益
   *   - 删除比例高时 Compaction 收益大
   *
   *   3. 性能分析:
   *   - 分析工作负载特征
   *
   * 采样策略:
   *   - 最多采样 20 个文件
   *   - 从文件元数据中获取
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时更新
   *   - 每个采样文件添加时累计
   *
   * 并发控制:
   *   - 读取需要在 finalized_ 为 true 后
   *
   * 注意事项:
   *   - 是采样数据，不是全部文件
   *   - 包括单点删除和范围删除
   *   - 用于估计删除比例
   */
  // total number of deletion entries
  uint64_t accumulated_num_deletions_;

  /**
   * current_num_non_deletions_ - 当前非删除条目数
   *
   * 功能概述:
   *   - 64 位无符号整数，记录当前版本的非删除条目数
   *   - 用于动态更新累计统计
   *   - 采样统计信息的一部分
   *
   * 使用场景:
   *   1. 动态更新:
   *   - 文件删除时更新
   *   - 文件添加时更新
   *
   *   2. 统计同步:
   *   - 保持累计统计的准确性
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时设置
   *   - RemoveCurrentStats 时更新
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 当前版本的统计
   *   - 动态更新
   *   - 与 accumulated_* 配合
   */
  // current number of non_deletion entries
  uint64_t current_num_non_deletions_;

  /**
   * current_num_deletions_ - 当前删除条目数
   *
   * 功能概述:
   *   - 64 位无符号整数，记录当前版本的删除条目数
   *   - 用于动态更新累计统计
   *   - 采样统计信息的一部分
   *
   * 使用场景:
   *   1. 动态更新:
   *   - 文件删除时更新
   *   - 文件添加时更新
   *
   *   2. 统计同步:
   *   - 保持累计统计的准确性
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时设置
   *   - RemoveCurrentStats 时更新
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 当前版本的统计
   *   - 动态更新
   *   - 与 accumulated_* 配合
   */
  // current number of deletion entries
  uint64_t current_num_deletions_;

  /**
   * current_num_samples_ - 当前采样数量
   *
   * 功能概述:
   *   - 64 位无符号整数，记录当前的采样文件数量
   *   - 限制最多采样 20 个文件
   *   - 采样统计信息的一部分
   *
   * 使用场景:
   *   1. 采样限制:
   *   - 最多采样 20 个文件
   *   - 超过 20 个后不再采样
   *
   *   2. 采样策略:
   *   - 优先采样低层文件
   *   - 控制采样开销
   *
   * 更新时机:
   *   - UpdateAccumulatedStats 时更新
   *   - 每次采样文件时递增
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 最大值为 20
   *   - 控制采样开销
   *   - 优先低层文件
   */
  // current number of file samples
  uint64_t current_num_samples_;

  /**
   * estimated_compaction_needed_bytes_ - 估计需要的压缩字节数
   *
   * 功能概述:
   *   - 64 位无符号整数，估计需要 Compaction 的字节数
   *   - 计算所有层级达到目标大小需要 Compaction 的总字节数
   *   - 用于 Compaction 调度和资源规划
   *
   * 计算方法:
   *   - 遍历所有层级
   *   - 计算每层超过目标大小的字节数
   *   - 累计所有超出的字节数
   *
   * 使用场景:
   *   1. Compaction 调度:
   *   - 评估 Compaction 压力
   *   - 确定需要启动的 Compaction 数量
   *
   *   2. 资源规划:
   *   - 估计需要的 I/O 资源
   *   - 规划后台线程数量
   *
   *   3. 性能监控:
   *   - 监控 Compaction 进度
   *   - 检测 Compaction 是否落后
   *
   * 更新时机:
   *   - EstimateCompactionBytesNeeded 时计算
   *   - 版本变化时更新
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - 估计值，不是精确值
   *   - 用于 Compaction 调度
   *   - 影响后台线程数量
   */
  // Estimated bytes needed to be compacted until all levels' size is down to
  // target sizes.
  uint64_t estimated_compaction_needed_bytes_;

  /**
   * finalized_ - 是否已最终确定
   *
   * 功能概述:
   *   - 布尔值，标记 VersionStorageInfo 是否已最终确定
   *   - 为 true 时才能访问某些数据
   *   - 用于防止不一致的状态
   *
   * 生命周期:
   *   1. 创建阶段:
   *   - finalized_ = false
   *   - 正在添加文件和元数据
   *
   *   2. 准备阶段:
   *   - PrepareForVersionAppend 被调用
   *   - 计算派生数据
   *
   *   3. 最终确定:
   *   - SetFinalized 被调用
   *   - finalized_ = true
   *   - 可以被安装和使用
   *
   * 使用场景:
   *   1. 数据访问保护:
   *   - 某些数据需要 finalized_ = true
   *   - 防止访问未准备好的数据
   *
   *   2. 状态检查:
   *   - 验证版本是否可用
   *
   *   3. 调试:
   *   - 检查版本状态
   *
   * 注意事项:
   *   - 必须在 finalized_ = true 后才能访问某些数据
   *   - 一次性设置，不会变回 false
   *   - 版本安装时必须为 true
   */
  bool finalized_;

  /**
   * force_consistency_checks_ - 强制一致性检查
   *
   * 功能概述:
   *   - 布尔值，是否强制执行一致性检查
   *   - 为 true 时，即使在 Release 模式也执行检查
   *   - 用于调试和验证
   *
   * 检查内容:
   *   1. 文件顺序:
   *   - 文件是否按键范围有序
   *   - 文件是否重叠（除 L0）
   *
   *   2. 文件元数据:
   *   - 文件号是否唯一
   *   - 键范围是否有效
   *
   *   3. 层级结构:
   *   - 层级大小是否合理
   *   - 文件数量是否合理
   *
   * 使用场景:
   *   1. 调试:
   *   - 在 Release 模式也执行检查
   *   - 帮助发现 Bug
   *
   *   2. 验证:
   *   - 验证数据的正确性
   *   - 确保结构一致
   *
   *   3. 测试:
   *   - 在测试中启用
   *   - 验证实现正确性
   *
   * 性能影响:
   *   - 检查会增加开销
   *   - 生产环境通常禁用
   *
   * 配置:
   *   - 构造函数参数
   *   - 通常为 false
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时设置
   *   - 整个生命周期内不变
   *
   * 注意事项:
   *   - 影响性能
   *   - 仅用于调试和测试
   *   - Release 模式通常不使用
   */
  // If set to true, we will run consistency checks even if RocksDB
  // is compiled in release mode
  bool force_consistency_checks_;

  /**
   * epoch_number_requirement_ - Epoch 号要求
   *
   * 功能概述:
   *   - 枚举，表示对文件 Epoch 号的要求
   *   - Epoch 号用于文件恢复和一致性检查
   *   - 影响是否要求文件必须有 Epoch 号
   *
   * 枚举值:
   *   1. kMightMissing:
   *      - 文件可能没有 Epoch 号
   *      - 允许缺失 Epoch 号
   *
   *   2. kMustPresent:
   *      - 文件必须有 Epoch 号
   *      - 不允许缺失 Epoch 号
   *
   * Epoch 机制:
   *   - Epoch 号是文件的版本号
   *   - 用于恢复时验证文件顺序
   *   - 用于检测文件损坏或不一致
   *
   * 使用场景:
   *   1. 恢复:
   *   - 根据 Epoch 号恢复文件顺序
   *   - 验证文件的一致性
   *
   *   2. 一致性检查:
   *   - 检查文件的 Epoch 号是否有效
   *
   *   3. 兼容性:
   *   - 支持旧版本文件（kMightMissing）
   *
   * 配置:
   *   - 构造函数参数
   *   - 通常为 kMustPresent
   *
   * 生命周期:
   *   - VersionStorageInfo 创建时设置
   *   - 可以动态修改
   *
   * 注意事项:
   *   - 影响恢复行为
   *   - 新版本通常要求 Epoch 号
   *   - 旧版本可能不要求
   */
  EpochNumberRequirement epoch_number_requirement_;

 private:
  // 以下函数在 PrepareForVersionAppend 中调用，实现内部辅助功能
  void ComputeCompensatedSizes();
  void UpdateNumNonEmptyLevels();
  void UpdateFilesByCompactionPri(const ImmutableOptions& immutable_options,
                                   const MutableCFOptions& mutable_cf_options);
  void GenerateFileIndexer();
  void GenerateLevelFilesBrief();
  void GenerateLevel0NonOverlapping();
  void GenerateBottommostFiles();
  void GenerateFileLocationIndex();
  void CalculateBaseBytes(const ImmutableOptions& ioptions,
                         const MutableCFOptions& mutable_cf_options);

  friend class Version;
  friend class VersionSet;
};

struct ObsoleteFileInfo {
  FileMetaData* metadata;
  std::string path;
  // If true, the FileMataData should be destroyed but the file should
  // not be deleted. This is because another FileMetaData still references
  // the file, usually because the file is trivial moved so two FileMetadata
  // is managing the file.
  bool only_delete_metadata = false;

  ObsoleteFileInfo() noexcept
      : metadata(nullptr), only_delete_metadata(false) {}
  ObsoleteFileInfo(FileMetaData* f, const std::string& file_path,
                   std::shared_ptr<CacheReservationManager>
                       file_metadata_cache_res_mgr_arg = nullptr)
      : metadata(f),
        path(file_path),
        only_delete_metadata(false),
        file_metadata_cache_res_mgr(file_metadata_cache_res_mgr_arg) {}

  ObsoleteFileInfo(const ObsoleteFileInfo&) = delete;
  ObsoleteFileInfo& operator=(const ObsoleteFileInfo&) = delete;

  ObsoleteFileInfo(ObsoleteFileInfo&& rhs) noexcept : ObsoleteFileInfo() {
    *this = std::move(rhs);
  }

  ObsoleteFileInfo& operator=(ObsoleteFileInfo&& rhs) noexcept {
    path = std::move(rhs.path);
    metadata = rhs.metadata;
    rhs.metadata = nullptr;
    file_metadata_cache_res_mgr = rhs.file_metadata_cache_res_mgr;
    rhs.file_metadata_cache_res_mgr = nullptr;

    return *this;
  }
  void DeleteMetadata() {
    if (file_metadata_cache_res_mgr) {
      Status s = file_metadata_cache_res_mgr->UpdateCacheReservation(
          metadata->ApproximateMemoryUsage(), false /* increase */);
      s.PermitUncheckedError();
    }
    delete metadata;
    metadata = nullptr;
  }

 private:
  std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr;
};

class ObsoleteBlobFileInfo {
 public:
  ObsoleteBlobFileInfo(uint64_t blob_file_number, std::string path)
      : blob_file_number_(blob_file_number), path_(std::move(path)) {}

  uint64_t GetBlobFileNumber() const { return blob_file_number_; }
  const std::string& GetPath() const { return path_; }

 private:
  uint64_t blob_file_number_;
  std::string path_;
};

using MultiGetRange = MultiGetContext::Range;
// A column family's version consists of the table and blob files owned by
// the column family at a certain point in time.
class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // @param read_options Must outlive any iterator built by
  // `merger_iter_builder`.
  void AddIterators(const ReadOptions& read_options,
                    const FileOptions& soptions,
                    MergeIteratorBuilder* merger_iter_builder,
                    bool allow_unprepared_value);

  // @param read_options Must outlive any iterator built by
  // `merger_iter_builder`.
  void AddIteratorsForLevel(const ReadOptions& read_options,
                            const FileOptions& soptions,
                            MergeIteratorBuilder* merger_iter_builder,
                            int level, bool allow_unprepared_value);

  Status OverlapWithLevelIterator(const ReadOptions&, const FileOptions&,
                                  const Slice& smallest_user_key,
                                  const Slice& largest_user_key, int level,
                                  bool* overlap);

  // Lookup the value for key or get all merge operands for key.
  // If do_merge = true (default) then lookup value for key.
  // Behavior if do_merge = true:
  //    If found, store it in *value and
  //    return OK.  Else return a non-OK status.
  //    Uses *operands to store merge_operator operations to apply later.
  //
  //    If the ReadOptions.read_tier is set to do a read-only fetch, then
  //    *value_found will be set to false if it cannot be determined whether
  //    this value exists without doing IO.
  //
  //    If the key is Deleted, *status will be set to NotFound and
  //                        *key_exists will be set to true.
  //    If no key was found, *status will be set to NotFound and
  //                      *key_exists will be set to false.
  //    If seq is non-null, *seq will be set to the sequence number found
  //    for the key if a key was found.
  // Behavior if do_merge = false
  //    If the key has any merge operands then store them in
  //    merge_context.operands_list and don't merge the operands
  // REQUIRES: lock is not held
  // REQUIRES: pinned_iters_mgr != nullptr
  void Get(const ReadOptions&, const LookupKey& key, PinnableSlice* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* status,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           PinnedIteratorsManager* pinned_iters_mgr,
           bool* value_found = nullptr, bool* key_exists = nullptr,
           SequenceNumber* seq = nullptr, ReadCallback* callback = nullptr,
           bool* is_blob = nullptr, bool do_merge = true);

  void MultiGet(const ReadOptions&, MultiGetRange* range,
                ReadCallback* callback = nullptr);

  // Interprets blob_index_slice as a blob reference, and (assuming the
  // corresponding blob file is part of this Version) retrieves the blob and
  // saves it in *value.
  // REQUIRES: blob_index_slice stores an encoded blob reference
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 const Slice& blob_index_slice,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read) const;

  // Retrieves a blob using a blob reference and saves it in *value,
  // assuming the corresponding blob file is part of this Version.
  Status GetBlob(const ReadOptions& read_options, const Slice& user_key,
                 const BlobIndex& blob_index,
                 FilePrefetchBuffer* prefetch_buffer, PinnableSlice* value,
                 uint64_t* bytes_read) const;

  struct BlobReadContext {
    BlobReadContext(const BlobIndex& blob_idx, const KeyContext* key_ctx)
        : blob_index(blob_idx), key_context(key_ctx) {}

    BlobIndex blob_index;
    const KeyContext* key_context;
    PinnableSlice result;
  };

  using BlobReadContexts = std::vector<BlobReadContext>;
  void MultiGetBlob(const ReadOptions& read_options, MultiGetRange& range,
                    std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs);

  // Loads some stats information from files (if update_stats is set) and
  // populates derived data structures. Call without mutex held. It needs to be
  // called before appending the version to the version set.
  void PrepareAppend(const MutableCFOptions& mutable_cf_options,
                     const ReadOptions& read_options, bool update_stats);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  // Decrease reference count. Delete the object if no reference left
  // and return true. Otherwise, return false.
  bool Unref();

  // Add all files listed in the current version to *live_table_files and
  // *live_blob_files.
  void AddLiveFiles(std::vector<uint64_t>* live_table_files,
                    std::vector<uint64_t>* live_blob_files) const;

  // Remove live files that are in the delete candidate lists.
  void RemoveLiveFiles(
      std::vector<ObsoleteFileInfo>& sst_delete_candidates,
      std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const;

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false, bool print_stats = false) const;

  // Returns the version number of this version
  uint64_t GetVersionNumber() const { return version_number_; }

  // REQUIRES: lock is held
  // On success, "tp" will contains the table properties of the file
  // specified in "file_meta".  If the file name of "file_meta" is
  // known ahead, passing it by a non-null "fname" can save a
  // file-name conversion.
  Status GetTableProperties(const ReadOptions& read_options,
                            std::shared_ptr<const TableProperties>* tp,
                            const FileMetaData* file_meta,
                            const std::string* fname = nullptr) const;

  // REQUIRES: lock is held
  // On success, *props will be populated with all SSTables' table properties.
  // The keys of `props` are the sst file name, the values of `props` are the
  // tables' properties, represented as std::shared_ptr.
  Status GetPropertiesOfAllTables(const ReadOptions& read_options,
                                  TablePropertiesCollection* props);
  Status GetPropertiesOfAllTables(const ReadOptions& read_options,
                                  TablePropertiesCollection* props, int level);
  Status GetPropertiesOfTablesInRange(const ReadOptions& read_options,
                                      const Range* range, std::size_t n,
                                      TablePropertiesCollection* props) const;

  // Print summary of range delete tombstones in SST files into out_str,
  // with maximum max_entries_to_print entries printed out.
  Status TablesRangeTombstoneSummary(int max_entries_to_print,
                                     std::string* out_str);

  // REQUIRES: lock is held
  // On success, "tp" will contains the aggregated table property among
  // the table properties of all sst files in this version.
  Status GetAggregatedTableProperties(
      const ReadOptions& read_options,
      std::shared_ptr<const TableProperties>* tp, int level = -1);

  uint64_t GetEstimatedActiveKeys() {
    return storage_info_.GetEstimatedActiveKeys();
  }

  size_t GetMemoryUsageByTableReaders(const ReadOptions& read_options);

  ColumnFamilyData* cfd() const { return cfd_; }

  // Return the next Version in the linked list.
  Version* Next() const { return next_; }

  int TEST_refs() const { return refs_; }

  VersionStorageInfo* storage_info() { return &storage_info_; }
  const VersionStorageInfo* storage_info() const { return &storage_info_; }

  VersionSet* version_set() { return vset_; }

  void GetColumnFamilyMetaData(ColumnFamilyMetaData* cf_meta);

  void GetSstFilesBoundaryKeys(Slice* smallest_user_key,
                               Slice* largest_user_key);

  uint64_t GetSstFilesSize();

  // Retrieves the file_creation_time of the oldest file in the DB.
  // Prerequisite for this API is max_open_files = -1
  void GetCreationTimeOfOldestFile(uint64_t* creation_time);

  const MutableCFOptions& GetMutableCFOptions() { return mutable_cf_options_; }

  InternalIterator* TEST_GetLevelIterator(
      const ReadOptions& read_options, MergeIteratorBuilder* merge_iter_builder,
      int level, bool allow_unprepared_value);

 private:
  Env* env_;
  SystemClock* clock_;

  friend class ReactiveVersionSet;
  friend class VersionSet;
  friend class VersionEditHandler;
  friend class VersionEditHandlerPointInTime;

  const InternalKeyComparator* internal_comparator() const {
    return storage_info_.internal_comparator_;
  }
  const Comparator* user_comparator() const {
    return storage_info_.user_comparator_;
  }

  // Returns true if the filter blocks in the specified level will not be
  // checked during read operations. In certain cases (trivial move or preload),
  // the filter block may already be cached, but we still do not access it such
  // that it eventually expires from the cache.
  bool IsFilterSkipped(int level, bool is_file_last_in_level = false);

  // The helper function of UpdateAccumulatedStats, which may fill the missing
  // fields of file_meta from its associated TableProperties.
  // Returns true if it does initialize FileMetaData.
  bool MaybeInitializeFileMetaData(const ReadOptions& read_options,
                                   FileMetaData* file_meta);

  // Update the accumulated stats associated with the current version.
  // This accumulated stats will be used in compaction.
  void UpdateAccumulatedStats(const ReadOptions& read_options);

  DECLARE_SYNC_AND_ASYNC(
      /* ret_type */ Status, /* func_name */ MultiGetFromSST,
      const ReadOptions& read_options, MultiGetRange file_range,
      int hit_file_level, bool skip_filters, bool skip_range_deletions,
      FdWithKeyRange* f,
      std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs,
      TableCache::TypedHandle* table_handle, uint64_t& num_filter_read,
      uint64_t& num_index_read, uint64_t& num_sst_read);

#ifdef USE_COROUTINES
  // MultiGet using async IO to read data blocks from SST files in parallel
  // within and across levels
  Status MultiGetAsync(
      const ReadOptions& options, MultiGetRange* range,
      std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs);

  // A helper function to lookup a batch of keys in a single level. It will
  // queue coroutine tasks to mget_tasks. It may also split the input batch
  // by creating a new batch with keys definitely not in this level and
  // enqueuing it to to_process.
  Status ProcessBatch(
      const ReadOptions& read_options, FilePickerMultiGet* batch,
      std::vector<folly::coro::Task<Status>>& mget_tasks,
      std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs,
      autovector<FilePickerMultiGet, 4>& batches, std::deque<size_t>& waiting,
      std::deque<size_t>& to_process, unsigned int& num_tasks_queued,
      std::unordered_map<int, std::tuple<uint64_t, uint64_t, uint64_t>>&
          mget_stats);
#endif

  /**
   * cfd_ - 所属的列族数据
   *
   * 功能概述:
   *   - 指向此 Version 所属的 ColumnFamilyData
   *   - 提供对列族元数据和资源的访问
   *   - 建立与列族的关联关系
   *
   * 使用场景:
   *   1. 访问列族配置:
   *   - 获取列族选项
   *   - 获取比较器
   *   - 获取统计信息
   *
   *   2. 访问列族资源:
   *   - 获取 MemTable
   *   - 获取 SuperVersion
   *   - 获取 TableCache
   *
   *   3. 获取列族信息:
   *   - 列族名称和 ID
   *   - 列族状态
   *
   * 生命周期:
   *   - Version 创建时设置
   *   - Version 销毁时保持有效（不释放）
   *
   * 线程安全性:
   *   - 指针本身不可变
   *   - 通过 ColumnFamilyData 的方法访问需要锁保护
   *
   * 注意事项:
   *   - 不转移所有权
   *   - ColumnFamilyData 的生命周期长于 Version
   *   - 必须保证指针在整个 Version 生命周期内有效
   */
  ColumnFamilyData* cfd_;  // ColumnFamilyData to which this Version belongs

  /**
   * info_log_ - 信息日志记录器
   *
   * 功能概述:
   *   - 指向日志记录器（Logger）
   *   - 用于记录信息和调试消息
   *   - 输出到 RocksDB 的日志文件
   *
   * 使用场景:
   *   1. 记录信息:
   *   - Version 创建和销毁
   *   - 文件添加和删除
   *   - 错误和警告
   *
   *   2. 调试:
   *   - 追踪 Version 的变化
   *   - 记录关键操作
   *
   *   3. 问题诊断:
   *   - 记录错误信息
   *   - 帮助定位问题
   *
   * 配置:
   *   - 由 DBImpl 配置
   *   - 所有列族共享同一个 Logger
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - Version 仅保存引用
   *
   * 线程安全性:
   *   - Logger 内部有锁机制
   *   - 支持多线程并发写入
   *
   * 注意事项:
   *   - 不转移所有权
   *   - Logger 的生命周期长于 Version
   *   - 频繁的日志可能影响性能
   */
  Logger* info_log_;

  /**
   * db_statistics_ - 数据库统计信息
   *
   * 功能概述:
   *   - 指向数据库统计信息对象（Statistics）
   *   - 用于记录数据库的各种统计信息
   *   - 支持性能监控和分析
   *
   * 统计内容:
   *   1. 操作计数:
   *   - 读操作次数
   *   - 写操作次数
   *   - 删除操作次数
   *
   *   2. 性能指标:
   *   - 读延迟
   *   - 写延迟
   *   - 缓存命中率
   *
   *   3. 资源使用:
   *   - 文件 I/O 量
   *   - 缓存使用量
   *   - 内存使用量
   *
   * 使用场景:
   *   1. 性能监控:
   *   - 实时监控数据库性能
   *   - 检测性能问题
   *
   *   2. 调试:
   *   - 分析操作模式
   *   - 定位瓶颈
   *
   *   3. 优化:
   *   - 根据统计信息调整配置
   *   - 优化查询和写入性能
   *
   * 配置:
   *   - 由 DBImpl 配置
   *   - 可以启用或禁用统计
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - Version 仅保存引用
   *
   * 线程安全性:
   *   - Statistics 内部有原子操作或锁
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - Statistics 的生命周期长于 Version
   *   - 统计会带来少量性能开销
   *   - 可以通过配置控制统计级别
   */
  Statistics* db_statistics_;

  /**
   * table_cache_ - SSTable 缓存
   *
   * 功能概述:
   *   - 指向 SSTable 缓存（TableCache）
   *   - 缓存已打开的 SSTable 文件读取器
   *   - 用于快速访问 SSTable 文件
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
   * 配置:
   *   - 由 DBImpl 配置
   *   - 所有列族共享此缓存
   *
   * 生命周期:
   *   - 由 DBImpl 拥有和管理
   *   - Version 仅保存引用
   *
   * 线程安全性:
   *   - TableCache 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - TableCache 的生命周期长于 Version
   *   - 缓存大小由 block_cache 配置控制
   *   - 缓存会根据 LRU 策略淘汰
   */
  TableCache* table_cache_;

  /**
   * blob_source_ - Blob 数据源
   *
   * 功能概述:
   *   - 指向 Blob 数据源（BlobSource）
   *   - 用于读取 Blob 文件中的数据
   *   - 提供 BlobDB 功能的数据访问
   *
   * BlobDB 功能:
   *   - 将大值存储在独立的 Blob 文件中
   *   - SSTable 中存储 Blob 文件的引用
   *   - 支持大值的高效存储和访问
   *
   * 使用场景:
   *   1. 读取大值:
   *   - 从 SSTable 中的 Blob 引用读取数据
   *   - 通过 BlobSource 获取实际数据
   *
   *   2. Compaction:
   *   - 处理 Blob 文件的引用
   *   - 清理无用的 Blob 文件
   *
   *   3. GC:
   *   - 垃圾回收无用的 Blob 数据
   *
   * 配置:
   *   - 由列族选项配置
   *   - 只有启用 BlobDB 功能时才创建
   *
   * 生命周期:
   *   - 由 ColumnFamilyData 拥有和管理
   *   - Version 仅保存引用
   *
   * 线程安全性:
   *   - BlobSource 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 不转移所有权
   *   - BlobSource 的生命周期长于 Version
   *   - 可能为 nullptr（未启用 BlobDB）
   *   - 与 blob_file_cache_ 配合使用
   */
  BlobSource* blob_source_;

  /**
   * merge_operator_ - 合并操作符
   *
   * 功能概述:
   *   - 指向合并操作符（MergeOperator）
   *   - 用于处理 Merge 操作
   *   - 定义如何合并多个值
   *
   * Merge 操作:
   *   - 允许对同一个 key 进行多次 Merge
   *   - 最后一次读取时合并所有值
   *   - 支持增量更新
   *
   * 使用场景:
   *   1. Merge 操作:
   *   - 处理 Merge 类型的记录
   *   - 合并多个值为一个值
   *
   *   2. 读取操作:
   *   - 读取时合并所有 Merge 值
   *   - 应用合并逻辑
   *
   *   3. Compaction:
   *   - Compaction 时合并相邻的 Merge 记录
   *   - 减少 Merge 链长度
   *
   * 配置:
   *   - 由列族选项配置
   *   - 用户自定义的 MergeOperator
   *
   * 生命周期:
   *   - 由列族选项拥有和管理
   *   - Version 仅保存引用
   *
   * 线程安全性:
   *   - MergeOperator 本身通常是线程安全的
   *   - 取决于具体实现
   *
   * 注意事项:
   *   - 不转移所有权
   *   - MergeOperator 的生命周期长于 Version
   *   - 如果没有设置 MergeOperator，Merge 操作会失败
   *   - MergeOperator 必须是线程安全的
   */
  const MergeOperator* merge_operator_;

  /**
   * storage_info_ - 版本存储信息
   *
   * 功能概述:
   *   - VersionStorageInfo 对象，存储版本的存储信息
   *   - 包含各层的 SSTable 文件列表
   *   - 包含 Blob 文件列表
   *   - 包含 Compaction 分数和其他元数据
   *
   * 包含的信息:
   *   1. 文件信息:
   *   - 各层的 SSTable 文件列表
   *   - 文件元数据（大小、键范围、序列号范围等）
   *   - 文件索引和位置
   *
   *   2. Blob 文件:
   *   - Blob 文件列表
   *   - Blob 文件元数据
   *
   *   3. Compaction 信息:
   *   - 各层的 Compaction 分数
   *   - 标记为需要 Compaction 的文件
   *   - TTL 过期的文件
   *   - 周期性 Compaction 的文件
   *
   *   4. 其他元数据:
   *   - 基础层级（base_level）
   *   - 层级倍数（level_multiplier）
   *   - 压缩游标（compact_cursor）
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 查询各层的文件列表
   *   - 确定需要读取的文件
   *
   *   2. Compaction:
   *   - 选择需要 Compaction 的文件
   *   - 计算 Compaction 分数
   *   - 确定 Compaction 的范围
   *
   *   3. 范围查询:
   *   - 查找重叠的文件
   *   - 确定查询范围
   *
   *   4. 统计和监控:
   *   - 获取文件数量和大小
   *   - 计算 Compaction 分数
   *
   * 生命周期:
   *   - Version 创建时创建
   *   - Version 销毁时销毁
   *
   * 线程安全性:
   *   - storage_info_ 本身是 Version 的一部分
   *   - 读取需要在 DB 互斥锁保护下或持有 Version 引用
   *   - 修改需要在 DB 互斥锁保护下
   *
   * 注意事项:
   *   - storage_info_ 的内容不可变（Version 创建后）
   *   - 读取时需要通过 Version 引用保证有效性
   *   - 包含所有必要的存储信息
   *   - 是 Version 的核心数据结构
   */
  VersionStorageInfo storage_info_;

  /**
   * vset_ - 所属的版本集合
   *
   * 功能概述:
   *   - 指向此 Version 所属的 VersionSet
   *   - VersionSet 管理数据库的所有 Version
   *   - 提供全局的 Version 管理功能
   *
   * VersionSet 功能:
   *   - 管理所有列族的 Version
   *   - 处理 Version 的创建和销毁
   *   - 管理 MANIFEST 文件
   *   - 分配文件号和序列号
   *
   * 使用场景:
   *   1. Version 链表:
   *   - Version 链表由 VersionSet 管理
   *   - next_ 和 prev_ 指向链表中的其他 Version
   *
   *   2. 文件管理:
   *   - 获取文件号
   *   - 管理过时的文件
   *
   *   3. MANIFEST 管理:
   *   - 写入 MANIFEST
   *   - 恢复 MANIFEST
   *
   *   4. 全局资源:
   *   - 访问共享资源（如 Logger、Statistics）
   *   - 获取数据库配置
   *
   * 生命周期:
   *   - Version 创建时设置
   *   - Version 销毁时保持有效（不释放）
   *
   * 线程安全性:
   *   - 指针本身不可变
   *   - 通过 VersionSet 的方法访问需要锁保护
   *
   * 注意事项:
   *   - 不转移所有权
   *   - VersionSet 的生命周期长于 Version
   *   - 必须保证指针在整个 Version 生命周期内有效
   */
  VersionSet* vset_;  // VersionSet to which this Version belongs

  /**
   * next_ - 下一个 Version（双向链表）
   *
   * 功能概述:
   *   - Version 指针，指向链表中的下一个 Version
   *   - 与 prev_ 配合形成双向链表
   *   - 用于管理所有 Version
   *
   * 链表结构:
   *   - 哨兵节点（ColumnFamilyData::dummy_versions_）
   *   - 所有 Version 形成循环链表
   *   - 哨兵节点的 next_ 指向最新的 Version
   *   - 最旧的 Version 的 next_ 指向哨兵节点
   *
   * 链表顺序:
   *   - 从最新到最旧：next_ 指向更旧的 Version
   *   - current_ = dummy_versions_->prev_（最新的 Version）
   *   - 链表按创建时间排序（新的在后）
   *
   * 使用场景:
   *   1. Version 遍历:
   *   - 遍历所有 Version
   *   - 统计 Version 数量
   *   - 清理过时的文件
   *
   *   2. Version 管理:
   *   - 添加新 Version
   *   - 删除旧 Version
   *
   *   3. 资源清理:
   *   - 查找未被引用的 Version
   *   - 清理相关的文件
   *
   * 生命周期:
   *   - Version 创建时初始化
   *   - Version 销毁时从链表移除
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下或持有 Version 引用
   *
   * 注意事项:
   *   - 与 prev_ 一起维护双向链表
   *   - 链表是循环的
   *   - 遍历时需要检查哨兵节点
   *   - 旧 Version 可能仍然存在（被引用）
   */
  Version* next_;     // Next version in linked list

  /**
   * prev_ - 上一个 Version（双向链表）
   *
   * 功能概述:
   *   - Version 指针，指向链表中的上一个 Version
   *   - 与 next_ 配合形成双向链表
   *   - 支持双向遍历
   *
   * 链表结构:
   *   - 哨兵节点（ColumnFamilyData::dummy_versions_）
   *   - 所有 Version 形成循环链表
   *   - 最新 Version 的 prev_ 指向哨兵节点
   *   - 哨兵节点的 prev_ 指向最新的 Version（即 current_）
   *
   * 特殊情况:
   *   - current_ = dummy_versions_->prev_（最新的 Version）
   *   - 如果是 current_，prev_ 指向 dummy_versions_
   *
   * 使用场景:
   *   1. 双向遍历:
   *   - 可以向前遍历 Version
   *   - 可以向后遍历 Version
   *
   *   2. Version 管理:
   *   - 添加新 Version
   *   - 删除旧 Version
   *
   *   3. 获取当前 Version:
   *   - 通过 dummy_versions_->prev_ 获取
   *   - 获取最新的 Version
   *
   * 生命周期:
   *   - Version 创建时初始化
   *   - Version 销毁时从链表移除
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下或持有 Version 引用
   *
   * 注意事项:
   *   - 与 next_ 一起维护双向链表
   *   - 链表是循环的
   *   - 删除时需要同时更新 next_ 和 prev_
   *   - prev_ 指向 dummy_versions_ 时表示是最新的 Version
   */
  Version* prev_;     // Previous version in linked list

  /**
   * refs_ - 引用计数
   *
   * 功能概述:
   *   - 整数，记录对 Version 的活跃引用数
   *   - 用于管理 Version 生命周期
   *   - 引用计数为 0 时可以删除 Version
   *
   * 引用来源:
   *   1. SuperVersion:
   *   - SuperVersion 包含对 Version 的引用
   *   - SuperVersion 被查询、迭代器等使用
   *
   *   2. 迭代器:
   *   - 读迭代器持有 Version 的引用
   *   - 防止 Version 被删除
   *
   *   3. 后台任务:
   *   - Compaction 任务持有 Version 的引用
   *   - Flush 任务持有 Version 的引用
   *
   *   4. 内部操作:
   *   - 某些内部操作需要持有 Version 引用
   *   - 确保操作期间 Version 不变
   *
   * 使用场景:
   *   1. 增加引用:
   *   - 创建 SuperVersion 时
   *   - 创建迭代器时
   *   - 启动后台任务时
   *
   *   2. 减少引用:
   *   - 销毁 SuperVersion 时
   *   - 销毁迭代器时
   *   - 完成后台任务时
   *
   *   3. 删除检查:
   *   - Unref() 减少引用计数
   *   - 引用计数为 0 时删除 Version
   *
   * 并发控制:
   *   - Ref() 和 Unref() 需要在 DB 互斥锁保护下
   *   - 或者在单线程上下文中调用
   *
   * 删除条件:
   *   - refs_ == 0
   *   - 从链表中移除
   *   - 删除对象
   *
   * 注意事项:
   *   - 非原子变量（需要锁保护）
   *   - 必须正确平衡 Ref() 和 Unref() 调用
   *   - 泄漏引用会导致内存泄漏
   *   - 过早释放引用会导致 use-after-free
   */
  int refs_;          // Number of live refs to this version

  /**
   * file_options_ - 文件操作选项
   *
   * 功能概述:
   *   - 文件操作选项（FileOptions）
   *   - 用于控制 SSTable 文件的读写行为
   *   - 影响 I/O 性能和行为
   *
   * 包含的选项:
   *   - 文件读取缓冲区大小
   *   - 文件写入缓冲区大小
   *   - 是否使用直接 I/O（Direct I/O）
   *   - 是否使用 fdatasync
   *   - 文件预读策略
   *   - 文件压缩选项
   *   - 并发文件打开数量限制
   *
   * 使用场景:
   *   1. 文件读取:
   *   - 打开 SSTable 文件时使用
   *   - 读取数据块、索引块时使用
   *
   *   2. Compaction:
   *   - 读取输入文件时使用
   *   - 写入输出文件时使用
   *
   *   3. 性能调优:
   *   - 通过调整文件选项优化 I/O 性能
   *   - 例如：增大缓冲区、启用预读等
   *
   * 配置:
   *   - 由 DBImpl 配置
   *   - 所有 Version 共享此配置
   *
   * 生命周期:
   *   - Version 创建时设置（const）
   *   - 整个生命周期内不变
   *
   * 线程安全性:
   *   - 不可变，多线程安全读取
   *
   * 注意事项:
   *   - const 成员，构造后不再修改
   *   - 所有文件操作使用此选项
   *   - 修改文件选项需要创建新的 Version
   */
  const FileOptions file_options_;

  /**
   * mutable_cf_options_ - 可变列族选项
   *
   * 功能概述:
   *   - MutableCFOptions 对象，包含列族的可变选项
   *   - 记录创建此 Version 时的列族选项
   *   - 用于 Compaction 和 Flush
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
   *   1. Compaction:
   *   - 使用可变选项选择要压缩的文件
   *   - 确定输出文件大小
   *   - 确定压缩策略
   *
   *   2. Flush:
   *   - 使用可变选项确定 Flush 触发条件
   *   - 确定目标文件大小
   *
   *   3. 统计和监控:
   *   - 使用可变选项计算 Compaction 分数
   *
   * 配置:
   *   - 创建 Version 时从 ColumnFamilyData 获取
   *   - 记录创建时的配置快照
   *
   * 生命周期:
   *   - Version 创建时设置（const）
   *   - 整个生命周期内不变
   *
   * 线程安全性:
   *   - 不可变，多线程安全读取
   *
   * 注意事项:
   *   - const 成员，构造后不再修改
   *   - 记录的是创建时的配置快照
   *   - 与 ColumnFamilyData 的可变选项可能不同
   *   - 用于 Compaction 和 Flush 决策
   */
  const MutableCFOptions mutable_cf_options_;

  /**
   * max_file_size_for_l0_meta_pin_ - L0 元数据最大固定文件大小
   *
   * 功能概述:
   *   - 大小常量，记录 L0 文件的最大固定大小阈值
   *   - 用于决定是否固定 L0 文件的元数据
   *   - 缓存值，避免每次读取都重新计算
   *
   * 固定元数据:
   *   - 固定元数据意味着将文件元数据保持在内存中
   *   - 避免频繁的元数据加载
   *   - 提高查询性能
   *
   * 计算规则:
   *   - 基于列族的可变选项
   *   - 通常与 target_file_size_base 相关
   *   - 可能还考虑其他因素
   *
   * 使用场景:
   *   1. 文件元数据管理:
   *   - 决定是否固定文件元数据
   *   - 减少磁盘 I/O
   *
   *   2. 性能优化:
   *   - 固定小文件的元数据
   *   - 避免频繁加载
   *
   *   3. 内存管理:
   *   - 控制固定元数据的内存使用
   *   - 避免内存溢出
   *
   * 配置:
   *   - 创建 Version 时计算
   *   - 基于可变选项计算
   *
   * 生命周期:
   *   - Version 创建时计算（const）
   *   - 整个生命周期内不变
   *
   * 线程安全性:
   *   - 不可变，多线程安全读取
   *
   * 注意事项:
   *   - const 成员，构造后不再修改
   *   - 缓存值，避免重复计算
   *   - 影响 L0 文件的元数据管理策略
   *   - 可能影响内存使用和查询性能
   */
  // Cached value to avoid recomputing it on every read.
  const size_t max_file_size_for_l0_meta_pin_;

  /**
   * version_number_ - 版本号
   *
   * 功能概述:
   *   - 无符号整数，表示此 Version 的唯一标识
   *   - 用于调试和日志记录
   *   - 单调递增
   *
   * 版本号特性:
   *   - 每个 Version 有唯一的版本号
   *   - 由 VersionSet 分配
   *   - 单调递增，不重复
   *
   * 使用场景:
   *   1. 调试:
   *   - 在日志中记录版本号
   *   - 追踪 Version 的变化
   *
   *   2. 日志记录:
   *   - 标识 Version
   *   - 帮助定位问题
   *
   *   3. 诊断:
   *   - 比较 Version 的版本号
   *   - 分析 Version 的创建顺序
   *
   * 分配方式:
   *   - 由 VersionSet 分配
   *   - VersionSet::current_version_number_ 递增
   *   - 每个 Version 分配唯一的版本号
   *
   * 生命周期:
   *   - Version 创建时分配
   *   - 整个生命周期内不变
   *
   * 线程安全性:
   *   - 不可变，多线程安全读取
   *
   * 注意事项:
   *   - 只用于调试和日志
   *   - 不用于逻辑判断
   *   - 单调递增
   *   - 可能溢出（uint64_t 范围很大）
   */
  // A version number that uniquely represents this version. This is
  // used for debugging and logging purposes only.
  uint64_t version_number_;

  /**
   * io_tracer_ - IO 追踪器
   *
   * 功能概述:
   *   - 共享指针，指向 IO 追踪器（IOTracer）
   *   - 用于追踪数据库的 I/O 操作
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
   *   - 由 DBImpl 配置
   *   - 可以启用或禁用
   *   - 所有 Version 共享此追踪器
   *
   * 生命周期:
   *   - 使用共享指针管理
   *   - 可以被多个对象共享
   *
   * 线程安全性:
   *   - IOTracer 内部有锁机制
   *   - 支持多线程并发访问
   *
   * 注意事项:
   *   - 使用共享指针管理生命周期
   *   - 可能为空（未启用 IO 追踪）
   *   - 追踪功能会增加少量性能开销
   *   - 与 VersionSet 共享
   */
  std::shared_ptr<IOTracer> io_tracer_;

  /**
   * use_async_io_ - 是否使用异步 I/O
   *
   * 功能概述:
   *   - 布尔值，标记是否使用异步 I/O
   *   - 影响读取操作的实现方式
   *   - 用于性能优化
   *
   * 异步 I/O 特性:
   *   - 使用协程或异步接口
   *   - 支持并行读取多个数据块
   *   - 减少 I/O 延迟
   *
   * 使用场景:
   *   1. MultiGet:
   *   - 并行读取多个 key
   *   - 减少总延迟
   *
   *   2. 范围查询:
   *   - 并行读取多个文件
   *   - 提高吞吐量
   *
   *   3. Compaction:
   *   - 并行读取输入文件
   *   - 提高压缩速度
   *
   * 配置:
   *   - 由 DBImpl 配置
   *   - 取决于是否支持异步 I/O
   *
   * 生命周期:
   *   - Version 创建时设置
   *   - 整个生命周期内不变
   *
   * 线程安全性:
   *   - 不可变，多线程安全读取
   *
   * 注意事项:
   *   - 不可变，构造后不再修改
   *   - 影响读取操作的选择
   *   - 需要系统支持异步 I/O
   *   - 使用协程时需要相应的编译选项
   */
  bool use_async_io_;

  Version(ColumnFamilyData* cfd, VersionSet* vset, const FileOptions& file_opt,
          MutableCFOptions mutable_cf_options,
          const std::shared_ptr<IOTracer>& io_tracer,
          uint64_t version_number = 0,
          EpochNumberRequirement epoch_number_requirement =
              EpochNumberRequirement::kMustPresent);

  ~Version();

  // No copying allowed
  Version(const Version&) = delete;
  void operator=(const Version&) = delete;
};

class BaseReferencedVersionBuilder;

class AtomicGroupReadBuffer {
 public:
  AtomicGroupReadBuffer() = default;
  Status AddEdit(VersionEdit* edit);
  void Clear();
  bool IsFull() const;
  bool IsEmpty() const;

  uint64_t TEST_read_edits_in_atomic_group() const {
    return read_edits_in_atomic_group_;
  }
  std::vector<VersionEdit>& replay_buffer() { return replay_buffer_; }

 private:
  uint64_t read_edits_in_atomic_group_ = 0;
  std::vector<VersionEdit> replay_buffer_;
};

// VersionSet is the collection of versions of all the column families of the
// database. Each database owns one VersionSet. A VersionSet has access to all
// column families via ColumnFamilySet, i.e. set of the column families.
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const ImmutableDBOptions* db_options,
             const FileOptions& file_options, Cache* table_cache,
             WriteBufferManager* write_buffer_manager,
             WriteController* write_controller,
             BlockCacheTracer* const block_cache_tracer,
             const std::shared_ptr<IOTracer>& io_tracer,
             const std::string& db_id, const std::string& db_session_id);
  // No copying allowed
  VersionSet(const VersionSet&) = delete;
  void operator=(const VersionSet&) = delete;

  virtual ~VersionSet();

  Status LogAndApplyToDefaultColumnFamily(
      const ReadOptions& read_options, VersionEdit* edit, InstrumentedMutex* mu,
      FSDirectory* dir_contains_current_file, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr) {
    ColumnFamilyData* default_cf = GetColumnFamilySet()->GetDefault();
    const MutableCFOptions* cf_options =
        default_cf->GetLatestMutableCFOptions();
    return LogAndApply(default_cf, *cf_options, read_options, edit, mu,
                       dir_contains_current_file, new_descriptor_log,
                       column_family_options);
  }

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // column_family_options has to be set if edit is column family add
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(
      ColumnFamilyData* column_family_data,
      const MutableCFOptions& mutable_cf_options,
      const ReadOptions& read_options, VersionEdit* edit, InstrumentedMutex* mu,
      FSDirectory* dir_contains_current_file, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr) {
    autovector<ColumnFamilyData*> cfds;
    cfds.emplace_back(column_family_data);
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(&mutable_cf_options);
    autovector<autovector<VersionEdit*>> edit_lists;
    autovector<VersionEdit*> edit_list;
    edit_list.emplace_back(edit);
    edit_lists.emplace_back(edit_list);
    return LogAndApply(cfds, mutable_cf_options_list, read_options, edit_lists,
                       mu, dir_contains_current_file, new_descriptor_log,
                       column_family_options);
  }
  // The batch version. If edit_list.size() > 1, caller must ensure that
  // no edit in the list column family add or drop
  Status LogAndApply(
      ColumnFamilyData* column_family_data,
      const MutableCFOptions& mutable_cf_options,
      const ReadOptions& read_options,
      const autovector<VersionEdit*>& edit_list, InstrumentedMutex* mu,
      FSDirectory* dir_contains_current_file, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr,
      const std::function<void(const Status&)>& manifest_wcb = {}) {
    autovector<ColumnFamilyData*> cfds;
    cfds.emplace_back(column_family_data);
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(&mutable_cf_options);
    autovector<autovector<VersionEdit*>> edit_lists;
    edit_lists.emplace_back(edit_list);
    return LogAndApply(cfds, mutable_cf_options_list, read_options, edit_lists,
                       mu, dir_contains_current_file, new_descriptor_log,
                       column_family_options, {manifest_wcb});
  }

  // The across-multi-cf batch version. If edit_lists contain more than
  // 1 version edits, caller must ensure that no edit in the []list is column
  // family manipulation.
  virtual Status LogAndApply(
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const MutableCFOptions*>& mutable_cf_options_list,
      const ReadOptions& read_options,
      const autovector<autovector<VersionEdit*>>& edit_lists,
      InstrumentedMutex* mu, FSDirectory* dir_contains_current_file,
      bool new_descriptor_log = false,
      const ColumnFamilyOptions* new_cf_options = nullptr,
      const std::vector<std::function<void(const Status&)>>& manifest_wcbs =
          {});

  static Status GetCurrentManifestPath(const std::string& dbname,
                                       FileSystem* fs,
                                       std::string* manifest_filename,
                                       uint64_t* manifest_file_number);
  void WakeUpWaitingManifestWriters();

  // Recover the last saved descriptor (MANIFEST) from persistent storage.
  // If read_only == true, Recover() will not complain if some column families
  // are not opened
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only = false, std::string* db_id = nullptr,
                 bool no_error_if_files_missing = false);

  Status TryRecover(const std::vector<ColumnFamilyDescriptor>& column_families,
                    bool read_only,
                    const std::vector<std::string>& files_in_dbname,
                    std::string* db_id, bool* has_missing_table_file);

  // Try to recover the version set to the most recent consistent state
  // recorded in the specified manifest.
  Status TryRecoverFromOneManifest(
      const std::string& manifest_path,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      bool read_only, std::string* db_id, bool* has_missing_table_file);

  // Recover the next epoch number of each CFs and epoch number
  // of their files (if missing)
  void RecoverEpochNumbers();

  // Reads a manifest file and returns a list of column families in
  // column_families.
  static Status ListColumnFamilies(std::vector<std::string>* column_families,
                                   const std::string& dbname, FileSystem* fs);
  static Status ListColumnFamiliesFromManifest(
      const std::string& manifest_path, FileSystem* fs,
      std::vector<std::string>* column_families);

  // Try to reduce the number of levels. This call is valid when
  // only one level from the new max level to the old
  // max level containing files.
  // The call is static, since number of levels is immutable during
  // the lifetime of a RocksDB instance. It reduces number of levels
  // in a DB by applying changes to manifest.
  // For example, a db currently has 7 levels [0-6], and a call to
  // to reduce to 5 [0-4] can only be executed when only one level
  // among [4-6] contains files.
  static Status ReduceNumberOfLevels(const std::string& dbname,
                                     const Options* options,
                                     const FileOptions& file_options,
                                     int new_levels);

  // Get the checksum information of all live files
  Status GetLiveFilesChecksumInfo(FileChecksumList* checksum_list);

  // printf contents (for debugging)
  Status DumpManifest(Options& options, std::string& manifestFileName,
                      bool verbose, bool hex = false, bool json = false,
                      const std::vector<ColumnFamilyDescriptor>& cf_descs = {});

  const std::string& DbSessionId() const { return db_session_id_; }

  // Return the current manifest file number
  uint64_t manifest_file_number() const { return manifest_file_number_; }

  uint64_t options_file_number() const { return options_file_number_; }

  uint64_t pending_manifest_file_number() const {
    return pending_manifest_file_number_;
  }

  uint64_t current_next_file_number() const { return next_file_number_.load(); }

  uint64_t min_log_number_to_keep() const {
    return min_log_number_to_keep_.load();
  }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

  // Fetch And Add n new file number
  uint64_t FetchAddFileNumber(uint64_t n) {
    return next_file_number_.fetch_add(n);
  }

  // Return the last sequence number.
  uint64_t LastSequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  // Note: memory_order_acquire must be sufficient.
  uint64_t LastAllocatedSequence() const {
    return last_allocated_sequence_.load(std::memory_order_seq_cst);
  }

  // Note: memory_order_acquire must be sufficient.
  uint64_t LastPublishedSequence() const {
    return last_published_sequence_.load(std::memory_order_seq_cst);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    // Last visible sequence must always be less than last written seq
    assert(!db_options_->two_write_queues || s <= last_allocated_sequence_);
    last_sequence_.store(s, std::memory_order_release);
  }

  // Note: memory_order_release must be sufficient
  void SetLastPublishedSequence(uint64_t s) {
    assert(s >= last_published_sequence_);
    last_published_sequence_.store(s, std::memory_order_seq_cst);
  }

  // Note: memory_order_release must be sufficient
  void SetLastAllocatedSequence(uint64_t s) {
    assert(s >= last_allocated_sequence_);
    last_allocated_sequence_.store(s, std::memory_order_seq_cst);
  }

  // Note: memory_order_release must be sufficient
  uint64_t FetchAddLastAllocatedSequence(uint64_t s) {
    return last_allocated_sequence_.fetch_add(s, std::memory_order_seq_cst);
  }

  // Mark the specified file number as used.
  // REQUIRED: this is only called during single-threaded recovery or repair.
  void MarkFileNumberUsed(uint64_t number);

  // Mark the specified log number as deleted
  // REQUIRED: this is only called during single-threaded recovery or repair, or
  // from ::LogAndApply where the global mutex is held.
  void MarkMinLogNumberToKeep(uint64_t number);

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t prev_log_number() const { return prev_log_number_; }

  // Returns the minimum log number which still has data not flushed to any SST
  // file.
  // In non-2PC mode, all the log numbers smaller than this number can be safely
  // deleted, although we still use `min_log_number_to_keep_` to determine when
  // to delete a WAL file.
  uint64_t MinLogNumberWithUnflushedData() const {
    return PreComputeMinLogNumberWithUnflushedData(nullptr);
  }

  // Returns the minimum log number which still has data not flushed to any SST
  // file.
  // Empty column families' log number is considered to be
  // new_log_number_for_empty_cf.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      uint64_t new_log_number_for_empty_cf) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      uint64_t num =
          cfd->IsEmpty() ? new_log_number_for_empty_cf : cfd->GetLogNumber();
      if (min_log_num > num && !cfd->IsDropped()) {
        min_log_num = num;
      }
    }
    return min_log_num;
  }
  // Returns the minimum log number which still has data not flushed to any SST
  // file, except data from `cfd_to_skip`.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      const ColumnFamilyData* cfd_to_skip) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (cfd == cfd_to_skip) {
        continue;
      }
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      if (min_log_num > cfd->GetLogNumber() && !cfd->IsDropped()) {
        min_log_num = cfd->GetLogNumber();
      }
    }
    return min_log_num;
  }
  // Returns the minimum log number which still has data not flushed to any SST
  // file, except data from `cfds_to_skip`.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      const std::unordered_set<const ColumnFamilyData*>& cfds_to_skip) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (cfds_to_skip.count(cfd)) {
        continue;
      }
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      if (min_log_num > cfd->GetLogNumber() && !cfd->IsDropped()) {
        min_log_num = cfd->GetLogNumber();
      }
    }
    return min_log_num;
  }

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  // @param read_options Must outlive the returned iterator.
  // @param start, end indicates compaction range
  InternalIterator* MakeInputIterator(
      const ReadOptions& read_options, const Compaction* c,
      RangeDelAggregator* range_del_agg,
      const FileOptions& file_options_compactions,
      const std::optional<const Slice>& start,
      const std::optional<const Slice>& end);

  // Add all files listed in any live version to *live_table_files and
  // *live_blob_files. Note that these lists may contain duplicates.
  void AddLiveFiles(std::vector<uint64_t>* live_table_files,
                    std::vector<uint64_t>* live_blob_files) const;

  // Remove live files that are in the delete candidate lists.
  void RemoveLiveFiles(
      std::vector<ObsoleteFileInfo>& sst_delete_candidates,
      std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const;

  // Return the approximate size of data to be scanned for range [start, end)
  // in levels [start_level, end_level). If end_level == -1 it will search
  // through all non-empty levels
  uint64_t ApproximateSize(const SizeApproximationOptions& options,
                           const ReadOptions& read_options, Version* v,
                           const Slice& start, const Slice& end,
                           int start_level, int end_level,
                           TableReaderCaller caller);

  // Return the size of the current manifest file
  uint64_t manifest_file_size() const { return manifest_file_size_; }

  Status GetMetadataForFile(uint64_t number, int* filelevel,
                            FileMetaData** metadata, ColumnFamilyData** cfd);

  // This function doesn't support leveldb SST filenames
  void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata);

  void AddObsoleteBlobFile(uint64_t blob_file_number, std::string path) {
    assert(table_cache_);

    table_cache_->Erase(GetSliceForKey(&blob_file_number));

    obsolete_blob_files_.emplace_back(blob_file_number, std::move(path));
  }

  void GetObsoleteFiles(std::vector<ObsoleteFileInfo>* files,
                        std::vector<ObsoleteBlobFileInfo>* blob_files,
                        std::vector<std::string>* manifest_filenames,
                        uint64_t min_pending_output);

  // REQUIRES: DB mutex held
  uint64_t GetObsoleteSstFilesSize() const;

  ColumnFamilySet* GetColumnFamilySet() { return column_family_set_.get(); }

  const UnorderedMap<uint32_t, size_t>& GetRunningColumnFamiliesTimestampSize()
      const {
    return column_family_set_->GetRunningColumnFamiliesTimestampSize();
  }

  const UnorderedMap<uint32_t, size_t>&
  GetColumnFamiliesTimestampSizeForRecord() const {
    return column_family_set_->GetColumnFamiliesTimestampSizeForRecord();
  }

  RefedColumnFamilySet GetRefedColumnFamilySet() {
    return RefedColumnFamilySet(GetColumnFamilySet());
  }

  const FileOptions& file_options() { return file_options_; }
  void ChangeFileOptions(const MutableDBOptions& new_options) {
    file_options_.writable_file_max_buffer_size =
        new_options.writable_file_max_buffer_size;
  }

  const ImmutableDBOptions* db_options() const { return db_options_; }

  static uint64_t GetNumLiveVersions(Version* dummy_versions);

  static uint64_t GetTotalSstFilesSize(Version* dummy_versions);

  static uint64_t GetTotalBlobFileSize(Version* dummy_versions);

  // Get the IO Status returned by written Manifest.
  const IOStatus& io_status() const { return io_status_; }

  // The returned WalSet needs to be accessed with DB mutex held.
  const WalSet& GetWalSet() const { return wals_; }

  void TEST_CreateAndAppendVersion(ColumnFamilyData* cfd) {
    assert(cfd);

    const auto& mutable_cf_options = *cfd->GetLatestMutableCFOptions();
    Version* const version =
        new Version(cfd, this, file_options_, mutable_cf_options, io_tracer_);

    constexpr bool update_stats = false;
    const ReadOptions read_options;
    version->PrepareAppend(mutable_cf_options, read_options, update_stats);
    AppendVersion(cfd, version);
  }

 protected:
  using VersionBuilderMap =
      UnorderedMap<uint32_t, std::unique_ptr<BaseReferencedVersionBuilder>>;

  struct ManifestWriter;

  friend class Version;
  friend class VersionEditHandler;
  friend class VersionEditHandlerPointInTime;
  friend class DumpManifestHandler;
  friend class DBImpl;
  friend class DBImplReadOnly;

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t /*bytes*/, const Status& s) override {
      if (status->ok()) {
        *status = s;
      }
    }
  };

  void Reset();

  // Returns approximated offset of a key in a file for a given version.
  uint64_t ApproximateOffsetOf(const ReadOptions& read_options, Version* v,
                               const FdWithKeyRange& f, const Slice& key,
                               TableReaderCaller caller);

  // Returns approximated data size between start and end keys in a file
  // for a given version.
  uint64_t ApproximateSize(const ReadOptions& read_options, Version* v,
                           const FdWithKeyRange& f, const Slice& start,
                           const Slice& end, TableReaderCaller caller);

  struct MutableCFState {
    uint64_t log_number;
    std::string full_history_ts_low;

    explicit MutableCFState() = default;
    explicit MutableCFState(uint64_t _log_number, std::string ts_low)
        : log_number(_log_number), full_history_ts_low(std::move(ts_low)) {}
  };

  // Save current contents to *log
  Status WriteCurrentStateToManifest(
      const std::unordered_map<uint32_t, MutableCFState>& curr_state,
      const VersionEdit& wal_additions, log::Writer* log, IOStatus& io_s);

  void AppendVersion(ColumnFamilyData* column_family_data, Version* v);

  ColumnFamilyData* CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                       const ReadOptions& read_options,
                                       const VersionEdit* edit);

  Status VerifyFileMetadata(const ReadOptions& read_options,
                            ColumnFamilyData* cfd, const std::string& fpath,
                            int level, const FileMetaData& meta);

  // Protected by DB mutex.
  WalSet wals_;

  std::unique_ptr<ColumnFamilySet> column_family_set_;
  Cache* table_cache_;
  Env* const env_;
  FileSystemPtr const fs_;
  SystemClock* const clock_;
  const std::string dbname_;
  std::string db_id_;
  const ImmutableDBOptions* const db_options_;
  std::atomic<uint64_t> next_file_number_;
  // Any WAL number smaller than this should be ignored during recovery,
  // and is qualified for being deleted.
  std::atomic<uint64_t> min_log_number_to_keep_ = {0};
  uint64_t manifest_file_number_;
  uint64_t options_file_number_;
  uint64_t options_file_size_;
  uint64_t pending_manifest_file_number_;
  // The last seq visible to reads. It normally indicates the last sequence in
  // the memtable but when using two write queues it could also indicate the
  // last sequence in the WAL visible to reads.
  std::atomic<uint64_t> last_sequence_;
  // The last sequence number of data committed to the descriptor (manifest
  // file).
  SequenceNumber descriptor_last_sequence_ = 0;
  // The last seq that is already allocated. It is applicable only when we have
  // two write queues. In that case seq might or might not have appreated in
  // memtable but it is expected to appear in the WAL.
  // We have last_sequence <= last_allocated_sequence_
  std::atomic<uint64_t> last_allocated_sequence_;
  // The last allocated sequence that is also published to the readers. This is
  // applicable only when last_seq_same_as_publish_seq_ is not set. Otherwise
  // last_sequence_ also indicates the last published seq.
  // We have last_sequence <= last_published_sequence_ <=
  // last_allocated_sequence_
  std::atomic<uint64_t> last_published_sequence_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  std::unique_ptr<log::Writer> descriptor_log_;

  // generates a increasing version number for every new version
  uint64_t current_version_number_;

  // Queue of writers to the manifest file
  std::deque<ManifestWriter*> manifest_writers_;

  // Current size of manifest file
  uint64_t manifest_file_size_;

  std::vector<ObsoleteFileInfo> obsolete_files_;
  std::vector<ObsoleteBlobFileInfo> obsolete_blob_files_;
  std::vector<std::string> obsolete_manifests_;

  // env options for all reads and writes except compactions
  FileOptions file_options_;

  BlockCacheTracer* const block_cache_tracer_;

  // Store the IO status when Manifest is written
  IOStatus io_status_;

  std::shared_ptr<IOTracer> io_tracer_;

  std::string db_session_id_;

 private:
  // REQUIRES db mutex at beginning. may release and re-acquire db mutex
  Status ProcessManifestWrites(std::deque<ManifestWriter>& writers,
                               InstrumentedMutex* mu,
                               FSDirectory* dir_contains_current_file,
                               bool new_descriptor_log,
                               const ColumnFamilyOptions* new_cf_options,
                               const ReadOptions& read_options);

  void LogAndApplyCFHelper(VersionEdit* edit,
                           SequenceNumber* max_last_sequence);
  Status LogAndApplyHelper(ColumnFamilyData* cfd, VersionBuilder* b,
                           VersionEdit* edit, SequenceNumber* max_last_sequence,
                           InstrumentedMutex* mu);
};

// ReactiveVersionSet represents a collection of versions of the column
// families of the database. Users of ReactiveVersionSet, e.g. DBImplSecondary,
// need to replay the MANIFEST (description log in older terms) in order to
// reconstruct and install versions.
class ReactiveVersionSet : public VersionSet {
 public:
  ReactiveVersionSet(const std::string& dbname,
                     const ImmutableDBOptions* _db_options,
                     const FileOptions& _file_options, Cache* table_cache,
                     WriteBufferManager* write_buffer_manager,
                     WriteController* write_controller,
                     const std::shared_ptr<IOTracer>& io_tracer);

  ~ReactiveVersionSet() override;

  Status ReadAndApply(
      InstrumentedMutex* mu,
      std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
      Status* manifest_read_status,
      std::unordered_set<ColumnFamilyData*>* cfds_changed);

  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
                 std::unique_ptr<log::Reader::Reporter>* manifest_reporter,
                 std::unique_ptr<Status>* manifest_reader_status);
#ifndef NDEBUG
  uint64_t TEST_read_edits_in_atomic_group() const;
#endif  //! NDEBUG

  std::vector<VersionEdit>& replay_buffer();

 protected:
  // REQUIRES db mutex
  Status ApplyOneVersionEditToBuilder(
      VersionEdit& edit, std::unordered_set<ColumnFamilyData*>* cfds_changed,
      VersionEdit* version_edit);

  Status MaybeSwitchManifest(
      log::Reader::Reporter* reporter,
      std::unique_ptr<log::FragmentBufferedReader>* manifest_reader);

 private:
  std::unique_ptr<ManifestTailer> manifest_tailer_;
  // TODO: plumb Env::IOActivity
  const ReadOptions read_options_;
  using VersionSet::LogAndApply;
  using VersionSet::Recover;

  Status LogAndApply(
      const autovector<ColumnFamilyData*>& /*cfds*/,
      const autovector<const MutableCFOptions*>& /*mutable_cf_options_list*/,
      const ReadOptions& /* read_options */,
      const autovector<autovector<VersionEdit*>>& /*edit_lists*/,
      InstrumentedMutex* /*mu*/, FSDirectory* /*dir_contains_current_file*/,
      bool /*new_descriptor_log*/, const ColumnFamilyOptions* /*new_cf_option*/,
      const std::vector<std::function<void(const Status&)>>& /*manifest_wcbs*/)
      override {
    return Status::NotSupported("not supported in reactive mode");
  }

  // No copy allowed
  ReactiveVersionSet(const ReactiveVersionSet&);
  ReactiveVersionSet& operator=(const ReactiveVersionSet&);
};

}  // namespace ROCKSDB_NAMESPACE
