//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction.h"

#include <cinttypes>
#include <vector>

#include "db/column_family.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/sst_partitioner.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const uint64_t kRangeTombstoneSentinel =
    PackSequenceAndType(kMaxSequenceNumber, kTypeRangeDeletion);

int sstableKeyCompare(const Comparator* uc, const Slice& a, const Slice& b) {
  auto c = uc->CompareWithoutTimestamp(ExtractUserKey(a), ExtractUserKey(b));
  if (c != 0) {
    return c;
  }
  auto a_footer = ExtractInternalKeyFooter(a);
  auto b_footer = ExtractInternalKeyFooter(b);
  if (a_footer == kRangeTombstoneSentinel) {
    if (b_footer != kRangeTombstoneSentinel) {
      return -1;
    }
  } else if (b_footer == kRangeTombstoneSentinel) {
    return 1;
  }
  return 0;
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey* a,
                      const InternalKey& b) {
  if (a == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, *a, b);
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey* b) {
  if (b == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, a, *b);
}

uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->fd.GetFileSize();
  }
  return sum;
}

void Compaction::SetInputVersion(Version* _input_version) {
  input_version_ = _input_version;
  cfd_ = input_version_->cfd();

  cfd_->Ref();
  input_version_->Ref();
  edit_.SetColumnFamily(cfd_->GetID());
}

void Compaction::GetBoundaryKeys(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs, Slice* smallest_user_key,
    Slice* largest_user_key, int exclude_level) {
  bool initialized = false;
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].files.empty() || inputs[i].level == exclude_level) {
      continue;
    }
    if (inputs[i].level == 0) {
      // we need to consider all files on level 0
      for (const auto* f : inputs[i].files) {
        const Slice& start_user_key = f->smallest.user_key();
        if (!initialized ||
            ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
          *smallest_user_key = start_user_key;
        }
        const Slice& end_user_key = f->largest.user_key();
        if (!initialized ||
            ucmp->Compare(end_user_key, *largest_user_key) > 0) {
          *largest_user_key = end_user_key;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      const Slice& start_user_key = inputs[i].files[0]->smallest.user_key();
      if (!initialized ||
          ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
        *smallest_user_key = start_user_key;
      }
      const Slice& end_user_key = inputs[i].files.back()->largest.user_key();
      if (!initialized || ucmp->Compare(end_user_key, *largest_user_key) > 0) {
        *largest_user_key = end_user_key;
      }
      initialized = true;
    }
  }
}

std::vector<CompactionInputFiles> Compaction::PopulateWithAtomicBoundaries(
    VersionStorageInfo* vstorage, std::vector<CompactionInputFiles> inputs) {
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); i++) {
    if (inputs[i].level == 0 || inputs[i].files.empty()) {
      continue;
    }
    inputs[i].atomic_compaction_unit_boundaries.reserve(inputs[i].files.size());
    AtomicCompactionUnitBoundary cur_boundary;
    size_t first_atomic_idx = 0;
    auto add_unit_boundary = [&](size_t to) {
      if (first_atomic_idx == to) return;
      for (size_t k = first_atomic_idx; k < to; k++) {
        inputs[i].atomic_compaction_unit_boundaries.push_back(cur_boundary);
      }
      first_atomic_idx = to;
    };
    for (size_t j = 0; j < inputs[i].files.size(); j++) {
      const auto* f = inputs[i].files[j];
      if (j == 0) {
        // First file in a level.
        cur_boundary.smallest = &f->smallest;
        cur_boundary.largest = &f->largest;
      } else if (sstableKeyCompare(ucmp, *cur_boundary.largest, f->smallest) ==
                 0) {
        // SSTs overlap but the end key of the previous file was not
        // artificially extended by a range tombstone. Extend the current
        // boundary.
        cur_boundary.largest = &f->largest;
      } else {
        // Atomic compaction unit has ended.
        add_unit_boundary(j);
        cur_boundary.smallest = &f->smallest;
        cur_boundary.largest = &f->largest;
      }
    }
    add_unit_boundary(inputs[i].files.size());
    assert(inputs[i].files.size() ==
           inputs[i].atomic_compaction_unit_boundaries.size());
  }
  return inputs;
}

// 辅助函数：判断当前压缩是否在最底层（bottommost level）进行
//
// 【功能说明】
// 判断压缩任务的输出是否位于LSM树的最底层。在最底层压缩有以下特点：
// 1. 压缩后的数据不会被更底层的文件覆盖
// 2. 可以执行更激进的优化，如：
//    - 删除被覆盖的旧记录
//    - 移除已经过期的range tombstone
//    - 应用compaction filter进行数据清理
// 3. 生成的文件是LSM树中最终的存储位置，无需再次压缩
//
// 【参数说明】
// @param output_level    压缩输出层的目标level编号（如L6）
// @param vstorage        VersionStorageInfo指针，包含版本存储信息（各层的文件列表）
// @param inputs          压缩输入文件列表，包含从哪些level选择了哪些文件
//
// 【返回值】
// @return true  - 压缩在最底层进行（bottommost compaction）
// @return false - 压缩不在最底层进行，输出文件后续可能需要再次压缩
//
// 【底层逻辑】
// 1. 对于Level压缩：检查output_level之下是否还有level包含重叠的数据
// 2. 对于Universal压缩：检查是否压缩到所有sorted runs的最后一个
// 3. 通过RangeMightExistAfterSortedRun判断输出范围是否会被更底层的sorted run覆盖
//
// 【使用场景】
// - 在Compaction构造时设置bottommost_level_标志（见第240-248行）
// - 在KeyNotExistsBeyondOutputLevel函数中直接返回true（见第531-532行）
// - 影响输出文件大小策略（见第287-290行，bottommost level使用target_output_file_size_）
// - 决定是否可以使用BOTTOM优先级的线程池进行压缩
//
// 【注意事项】
// - L0层压缩永远不会是bottommost（因为L0之下还有其他层）
// - Universal压缩风格可能没有明确的"最底层"概念
// - kExternalSstIngestion和kRefitLevel压缩原因会强制设置bottommost_level_=false
bool Compaction::IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  int output_l0_idx;
  // 处理L0层输出的特殊情况
  // L0层的文件是无序且可能重叠的，需要找到最后一个输入文件在L0层中的位置索引
  // output_l0_idx用于确定从L0层的哪个位置开始查找是否有后续文件与输出范围重叠
  if (output_level == 0) {
    output_l0_idx = 0;
    // 遍历L0层所有文件，找到最后一个输入文件（inputs[0].files.back()）的位置
    for (const auto* file : vstorage->LevelFiles(0)) {
      if (inputs[0].files.back() == file) {
        break;
      }
      ++output_l0_idx;
    }
    // 断言确保找到了文件位置
    assert(static_cast<size_t>(output_l0_idx) < vstorage->LevelFiles(0).size());
  } else {
    // 非L0层输出，不需要output_l0_idx索引（设置为-1表示未使用）
    output_l0_idx = -1;
  }
  // 获取当前压缩输入文件的边界键范围
  // smallest_key: 最小用户键
  // largest_key: 最大用户键
  // 这个键范围代表压缩输出的数据范围
  Slice smallest_key, largest_key;
  GetBoundaryKeys(vstorage, inputs, &smallest_key, &largest_key);
  
  // 核心判断逻辑：检查输出范围之后是否还存在有序sorted run
  // RangeMightExistAfterSortedRun 返回 true 表示存在可能覆盖输出范围的数据
  // 取反（!）后的含义：
  //   - 返回true：输出范围之后没有数据，说明这是最底层压缩
  //   - 返回false：输出范围之后还有数据，说明不是最底层
  //
  // 对于Level压缩风格：
  //   - 如果output_level = L6（最后一层），则output_level + 1不存在，返回true（是最底层）
  //   - 如果output_level = L5，检查L6层是否有文件与[smallest_key, largest_key]范围重叠
  //
  // 对于Universal压缩风格：
  //   - 检查是否有sorted run位于当前输出位置之后
  //   - 如果有则不是bottommost，因为后续的run可能包含更新的数据
  return !vstorage->RangeMightExistAfterSortedRun(smallest_key, largest_key,
                                                  output_level, output_l0_idx);
}

// test function to validate the functionality of IsBottommostLevel()
// function -- determines if compaction with inputs and storage is bottommost
bool Compaction::TEST_IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  return IsBottommostLevel(output_level, vstorage, inputs);
}

bool Compaction::IsFullCompaction(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  size_t num_files_in_compaction = 0;
  size_t total_num_files = 0;
  for (int l = 0; l < vstorage->num_levels(); l++) {
    total_num_files += vstorage->NumLevelFiles(l);
  }
  for (size_t i = 0; i < inputs.size(); i++) {
    num_files_in_compaction += inputs[i].size();
  }
  return num_files_in_compaction == total_num_files;
}

Compaction::Compaction(
    VersionStorageInfo* vstorage, const ImmutableOptions& _immutable_options,
    const MutableCFOptions& _mutable_cf_options,
    const MutableDBOptions& _mutable_db_options,
    std::vector<CompactionInputFiles> _inputs, int _output_level,
    uint64_t _target_file_size, uint64_t _max_compaction_bytes,
    uint32_t _output_path_id, CompressionType _compression,
    CompressionOptions _compression_opts, Temperature _output_temperature,
    uint32_t _max_subcompactions, std::vector<FileMetaData*> _grandparents,
    bool _manual_compaction, const std::string& _trim_ts, double _score,
    bool _deletion_compaction, bool l0_files_might_overlap,
    CompactionReason _compaction_reason,
    BlobGarbageCollectionPolicy _blob_garbage_collection_policy,
    double _blob_garbage_collection_age_cutoff)
    : input_vstorage_(vstorage),
      start_level_(_inputs[0].level),
      output_level_(_output_level),
      target_output_file_size_(_target_file_size),
      max_compaction_bytes_(_max_compaction_bytes),
      max_subcompactions_(_max_subcompactions),
      immutable_options_(_immutable_options),
      mutable_cf_options_(_mutable_cf_options),
      input_version_(nullptr),
      number_levels_(vstorage->num_levels()),
      cfd_(nullptr),
      output_path_id_(_output_path_id),
      output_compression_(_compression),
      output_compression_opts_(_compression_opts),
      output_temperature_(_output_temperature),
      deletion_compaction_(_deletion_compaction),
      l0_files_might_overlap_(l0_files_might_overlap),
      inputs_(PopulateWithAtomicBoundaries(vstorage, std::move(_inputs))),
      grandparents_(std::move(_grandparents)),
      score_(_score),
      bottommost_level_(
          // For simplicity, we don't support the concept of "bottommost level"
          // with
          // `CompactionReason::kExternalSstIngestion` and
          // `CompactionReason::kRefitLevel`
          (_compaction_reason == CompactionReason::kExternalSstIngestion ||
           _compaction_reason == CompactionReason::kRefitLevel)
              ? false
              : IsBottommostLevel(output_level_, vstorage, inputs_)),
      is_full_compaction_(IsFullCompaction(vstorage, inputs_)),
      is_manual_compaction_(_manual_compaction),
      trim_ts_(_trim_ts),
      is_trivial_move_(false),
      compaction_reason_(_compaction_reason),
      notify_on_compaction_completion_(false),
      enable_blob_garbage_collection_(
          _blob_garbage_collection_policy == BlobGarbageCollectionPolicy::kForce
              ? true
              : (_blob_garbage_collection_policy ==
                         BlobGarbageCollectionPolicy::kDisable
                     ? false
                     : mutable_cf_options()->enable_blob_garbage_collection)),
      blob_garbage_collection_age_cutoff_(
          _blob_garbage_collection_age_cutoff < 0 ||
                  _blob_garbage_collection_age_cutoff > 1
              ? mutable_cf_options()->blob_garbage_collection_age_cutoff
              : _blob_garbage_collection_age_cutoff),
      penultimate_level_(
          // For simplicity, we don't support the concept of "penultimate level"
          // with `CompactionReason::kExternalSstIngestion` and
          // `CompactionReason::kRefitLevel`
          _compaction_reason == CompactionReason::kExternalSstIngestion ||
                  _compaction_reason == CompactionReason::kRefitLevel
              ? Compaction::kInvalidLevel
              : EvaluatePenultimateLevel(vstorage, immutable_options_,
                                         start_level_, output_level_)) {
  MarkFilesBeingCompacted(true);
  if (is_manual_compaction_) {
    compaction_reason_ = CompactionReason::kManualCompaction;
  }
  if (max_subcompactions_ == 0) {
    max_subcompactions_ = _mutable_db_options.max_subcompactions;
  }

  // for the non-bottommost levels, it tries to build files match the target
  // file size, but not guaranteed. It could be 2x the size of the target size.
  max_output_file_size_ =
      bottommost_level_ || grandparents_.empty() ||
              !_immutable_options.level_compaction_dynamic_file_size
          ? target_output_file_size_
          : 2 * target_output_file_size_;

#ifndef NDEBUG
  for (size_t i = 1; i < inputs_.size(); ++i) {
    assert(inputs_[i].level > inputs_[i - 1].level);
  }
#endif

  // setup input_levels_
  {
    input_levels_.resize(num_input_levels());
    for (size_t which = 0; which < num_input_levels(); which++) {
      DoGenerateLevelFilesBrief(&input_levels_[which], inputs_[which].files,
                                &arena_);
    }
  }

  GetBoundaryKeys(vstorage, inputs_, &smallest_user_key_, &largest_user_key_);

  // Every compaction regardless of any compaction reason may respect the
  // existing compact cursor in the output level to split output files
  output_split_key_ = nullptr;
  if (immutable_options_.compaction_style == kCompactionStyleLevel &&
      immutable_options_.compaction_pri == kRoundRobin) {
    const InternalKey* cursor =
        &input_vstorage_->GetCompactCursors()[output_level_];
    if (cursor->size() != 0) {
      const Slice& cursor_user_key = ExtractUserKey(cursor->Encode());
      auto ucmp = vstorage->InternalComparator()->user_comparator();
      // May split output files according to the cursor if it in the user-key
      // range
      if (ucmp->CompareWithoutTimestamp(cursor_user_key, smallest_user_key_) >
              0 &&
          ucmp->CompareWithoutTimestamp(cursor_user_key, largest_user_key_) <=
              0) {
        output_split_key_ = cursor;
      }
    }
  }

  PopulatePenultimateLevelOutputRange();
}

void Compaction::PopulatePenultimateLevelOutputRange() {
  if (!SupportsPerKeyPlacement()) {
    return;
  }

  // exclude the last level, the range of all input levels is the safe range
  // of keys that can be moved up.
  int exclude_level = number_levels_ - 1;
  penultimate_output_range_type_ = PenultimateOutputRangeType::kNonLastRange;

  // For universal compaction, the penultimate_output_range could be extended if
  // all penultimate level files are included in the compaction (which includes
  // the case that the penultimate level is empty).
  if (immutable_options_.compaction_style == kCompactionStyleUniversal) {
    exclude_level = kInvalidLevel;
    penultimate_output_range_type_ = PenultimateOutputRangeType::kFullRange;
    std::set<uint64_t> penultimate_inputs;
    for (const auto& input_lvl : inputs_) {
      if (input_lvl.level == penultimate_level_) {
        for (const auto& file : input_lvl.files) {
          penultimate_inputs.emplace(file->fd.GetNumber());
        }
      }
    }
    auto penultimate_files = input_vstorage_->LevelFiles(penultimate_level_);
    for (const auto& file : penultimate_files) {
      if (penultimate_inputs.find(file->fd.GetNumber()) ==
          penultimate_inputs.end()) {
        exclude_level = number_levels_ - 1;
        penultimate_output_range_type_ =
            PenultimateOutputRangeType::kNonLastRange;
        break;
      }
    }
  }

  GetBoundaryKeys(input_vstorage_, inputs_,
                  &penultimate_level_smallest_user_key_,
                  &penultimate_level_largest_user_key_, exclude_level);
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  if (cfd_ != nullptr) {
    cfd_->UnrefAndTryDelete();
  }
}

bool Compaction::SupportsPerKeyPlacement() const {
  return penultimate_level_ != kInvalidLevel;
}

int Compaction::GetPenultimateLevel() const { return penultimate_level_; }

// smallest_key and largest_key include timestamps if user-defined timestamp is
// enabled.
bool Compaction::OverlapPenultimateLevelOutputRange(
    const Slice& smallest_key, const Slice& largest_key) const {
  if (!SupportsPerKeyPlacement()) {
    return false;
  }
  const Comparator* ucmp =
      input_vstorage_->InternalComparator()->user_comparator();

  return ucmp->CompareWithoutTimestamp(
             smallest_key, penultimate_level_largest_user_key_) <= 0 &&
         ucmp->CompareWithoutTimestamp(
             largest_key, penultimate_level_smallest_user_key_) >= 0;
}

// key includes timestamp if user-defined timestamp is enabled.
bool Compaction::WithinPenultimateLevelOutputRange(const Slice& key) const {
  if (!SupportsPerKeyPlacement()) {
    return false;
  }

  if (penultimate_level_smallest_user_key_.empty() ||
      penultimate_level_largest_user_key_.empty()) {
    return false;
  }

  const Comparator* ucmp =
      input_vstorage_->InternalComparator()->user_comparator();

  return ucmp->CompareWithoutTimestamp(
             key, penultimate_level_smallest_user_key_) >= 0 &&
         ucmp->CompareWithoutTimestamp(
             key, penultimate_level_largest_user_key_) <= 0;
}

bool Compaction::InputCompressionMatchesOutput() const {
  int base_level = input_vstorage_->base_level();
  bool matches =
      (GetCompressionType(input_vstorage_, mutable_cf_options_, start_level_,
                          base_level) == output_compression_);
  if (matches) {
    TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:Matches");
    return true;
  }
  TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:DidntMatch");
  return matches;
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // If start_level_== output_level_, the purpose is to force compaction
  // filter to be applied to that level, and thus cannot be a trivial move.

  // Check if start level have files with overlapping ranges
  if (start_level_ == 0 && input_vstorage_->level0_non_overlapping() == false &&
      l0_files_might_overlap_) {
    // We cannot move files from L0 to L1 if the L0 files in the LSM-tree are
    // overlapping, unless we are sure that files picked in L0 don't overlap.
    return false;
  }

  if (is_manual_compaction_ &&
      (immutable_options_.compaction_filter != nullptr ||
       immutable_options_.compaction_filter_factory != nullptr)) {
    // This is a manual compaction and we have a compaction filter that should
    // be executed, we cannot do a trivial move
    return false;
  }

  if (start_level_ == output_level_) {
    // It doesn't make sense if compaction picker picks files just to trivial
    // move to the same level.
    return false;
  }

  if (compaction_reason_ == CompactionReason::kChangeTemperature) {
    // Changing temperature usually requires rewriting the file.
    return false;
  }

  // Used in universal compaction, where trivial move can be done if the
  // input files are non overlapping
  if ((mutable_cf_options_.compaction_options_universal.allow_trivial_move) &&
      (output_level_ != 0) &&
      (cfd_->ioptions()->compaction_style == kCompactionStyleUniversal)) {
    return is_trivial_move_;
  }

  if (!(start_level_ != output_level_ && num_input_levels() == 1 &&
        input(0, 0)->fd.GetPathId() == output_path_id() &&
        InputCompressionMatchesOutput())) {
    return false;
  }

  // assert inputs_.size() == 1

  if (output_level_ + 1 < number_levels_) {
    std::unique_ptr<SstPartitioner> partitioner = CreateSstPartitioner();
    for (const auto& file : inputs_.front().files) {
      std::vector<FileMetaData*> file_grand_parents;
      input_vstorage_->GetOverlappingInputs(output_level_ + 1, &file->smallest,
                                            &file->largest,
                                            &file_grand_parents);
      const auto compaction_size =
          file->fd.GetFileSize() + TotalFileSize(file_grand_parents);
      if (compaction_size > max_compaction_bytes_) {
        return false;
      }

      if (partitioner.get() != nullptr) {
        if (!partitioner->CanDoTrivialMove(file->smallest.user_key(),
                                           file->largest.user_key())) {
          return false;
        }
      }
    }
  }

  // PerKeyPlacement compaction should never be trivial move.
  if (SupportsPerKeyPlacement()) {
    return false;
  }

  return true;
}

void Compaction::AddInputDeletions(VersionEdit* out_edit) {
  for (size_t which = 0; which < num_input_levels(); which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      out_edit->DeleteFile(level(which), inputs_[which][i]->fd.GetNumber());
    }
  }
}

bool Compaction::KeyNotExistsBeyondOutputLevel(
    const Slice& user_key, std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  if (bottommost_level_) {
    return true;
  } else if (output_level_ != 0 &&
             cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_->LevelFiles(lvl);
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // We've advanced far enough
          // In the presence of user-defined timestamp, we may need to handle
          // the case in which f->smallest.user_key() (including ts) has the
          // same user key, but the ts part is smaller. If so,
          // Compare(user_key, f->smallest.user_key()) returns -1.
          // That's why we need CompareWithoutTimestamp().
          if (user_cmp->CompareWithoutTimestamp(user_key,
                                                f->smallest.user_key()) >= 0) {
            // Key falls in this file's range, so it may
            // exist beyond output level
            return false;
          }
          break;
        }
      }
    }
    return true;
  }
  return false;
}

bool Compaction::KeyRangeNotExistsBeyondOutputLevel(
    const Slice& begin_key, const Slice& end_key,
    std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  assert(cfd_->user_comparator()->CompareWithoutTimestamp(begin_key, end_key) <
         0);
  if (bottommost_level_) {
    return true /* does not overlap */;
  } else if (output_level_ != 0 &&
             cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_->LevelFiles(lvl);
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        // Advance until the first file with begin_key <= f->largest.user_key()
        if (user_cmp->CompareWithoutTimestamp(begin_key,
                                              f->largest.user_key()) > 0) {
          continue;
        }
        // We know that the previous file prev_f, if exists, has
        // prev_f->largest.user_key() < begin_key.
        if (user_cmp->CompareWithoutTimestamp(end_key,
                                              f->smallest.user_key()) <= 0) {
          // not overlapping with this level
          break;
        } else {
          // We have:
          // - begin_key < end_key,
          // - begin_key <= f->largest.user_key(), and
          // - end_key > f->smallest.user_key()
          return false /* overlap */;
        }
      }
    }
    return true /* does not overlap */;
  }
  return false /* overlaps */;
};

// Mark (or clear) each file that is being compacted
void Compaction::MarkFilesBeingCompacted(bool mark_as_compacted) {
  for (size_t i = 0; i < num_input_levels(); i++) {
    for (size_t j = 0; j < inputs_[i].size(); j++) {
      assert(mark_as_compacted ? !inputs_[i][j]->being_compacted
                               : inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = mark_as_compacted;
    }
  }
}

// Sample output:
// If compacting 3 L0 files, 2 L3 files and 1 L4 file, and outputting to L5,
// print: "3@0 + 2@3 + 1@4 files to L5"
const char* Compaction::InputLevelSummary(
    InputLevelSummaryBuffer* scratch) const {
  int len = 0;
  bool is_first = true;
  for (auto& input_level : inputs_) {
    if (input_level.empty()) {
      continue;
    }
    if (!is_first) {
      len +=
          snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, " + ");
      len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
    } else {
      is_first = false;
    }
    len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                    "%" ROCKSDB_PRIszt "@%d", input_level.size(),
                    input_level.level);
    len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
           " files to L%d", output_level());

  return scratch->buffer;
}

uint64_t Compaction::CalculateTotalInputSize() const {
  uint64_t size = 0;
  for (auto& input_level : inputs_) {
    for (auto f : input_level.files) {
      size += f->fd.GetFileSize();
    }
  }
  return size;
}

void Compaction::ReleaseCompactionFiles(Status status) {
  MarkFilesBeingCompacted(false);
  cfd_->compaction_picker()->ReleaseCompactionFiles(this, status);
}

void Compaction::ResetNextCompactionIndex() {
  assert(input_version_ != nullptr);
  input_vstorage_->ResetNextCompactionIndex(start_level_);
}

namespace {
int InputSummary(const std::vector<FileMetaData*>& files, char* output,
                 int len) {
  *output = '\0';
  int write = 0;
  for (size_t i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret;
    char sztxt[16];
    AppendHumanBytes(files.at(i)->fd.GetFileSize(), sztxt, 16);
    ret = snprintf(output + write, sz, "%" PRIu64 "(%s) ",
                   files.at(i)->fd.GetNumber(), sztxt);
    if (ret < 0 || ret >= sz) break;
    write += ret;
  }
  // if files.size() is non-zero, overwrite the last space
  return write - !!files.size();
}
}  // namespace

void Compaction::Summary(char* output, int len) {
  int write =
      snprintf(output, len, "Base version %" PRIu64 " Base level %d, inputs: [",
               input_version_->GetVersionNumber(), start_level_);
  if (write < 0 || write >= len) {
    return;
  }

  for (size_t level_iter = 0; level_iter < num_input_levels(); ++level_iter) {
    if (level_iter > 0) {
      write += snprintf(output + write, len - write, "], [");
      if (write < 0 || write >= len) {
        return;
      }
    }
    write +=
        InputSummary(inputs_[level_iter].files, output + write, len - write);
    if (write < 0 || write >= len) {
      return;
    }
  }

  snprintf(output + write, len - write, "]");
}

uint64_t Compaction::OutputFilePreallocationSize() const {
  uint64_t preallocation_size = 0;

  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      preallocation_size += file->fd.GetFileSize();
    }
  }

  if (max_output_file_size_ != std::numeric_limits<uint64_t>::max() &&
      (immutable_options_.compaction_style == kCompactionStyleLevel ||
       output_level() > 0)) {
    preallocation_size = std::min(max_output_file_size_, preallocation_size);
  }

  // Over-estimate slightly so we don't end up just barely crossing
  // the threshold
  // No point to preallocate more than 1GB.
  return std::min(uint64_t{1073741824},
                  preallocation_size + (preallocation_size / 10));
}

std::unique_ptr<CompactionFilter> Compaction::CreateCompactionFilter() const {
  if (!cfd_->ioptions()->compaction_filter_factory) {
    return nullptr;
  }

  if (!cfd_->ioptions()
           ->compaction_filter_factory->ShouldFilterTableFileCreation(
               TableFileCreationReason::kCompaction)) {
    return nullptr;
  }

  CompactionFilter::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.column_family_id = cfd_->GetID();
  context.reason = TableFileCreationReason::kCompaction;
  return cfd_->ioptions()->compaction_filter_factory->CreateCompactionFilter(
      context);
}

std::unique_ptr<SstPartitioner> Compaction::CreateSstPartitioner() const {
  if (!immutable_options_.sst_partitioner_factory) {
    return nullptr;
  }

  SstPartitioner::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.output_level = output_level_;
  context.smallest_user_key = smallest_user_key_;
  context.largest_user_key = largest_user_key_;
  return immutable_options_.sst_partitioner_factory->CreatePartitioner(context);
}

bool Compaction::IsOutputLevelEmpty() const {
  return inputs_.back().level != output_level_ || inputs_.back().empty();
}

bool Compaction::ShouldFormSubcompactions() const {
  if (cfd_ == nullptr) {
    return false;
  }

  // Round-Robin pri under leveled compaction allows subcompactions by default
  // and the number of subcompactions can be larger than max_subcompactions_
  if (cfd_->ioptions()->compaction_pri == kRoundRobin &&
      cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    return output_level_ > 0;
  }

  if (max_subcompactions_ <= 1) {
    return false;
  }

  if (cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    return (start_level_ == 0 || is_manual_compaction_) && output_level_ > 0;
  } else if (cfd_->ioptions()->compaction_style == kCompactionStyleUniversal) {
    return number_levels_ > 1 && output_level_ > 0;
  } else {
    return false;
  }
}

bool Compaction::DoesInputReferenceBlobFiles() const {
  assert(input_version_);

  const VersionStorageInfo* storage_info = input_version_->storage_info();
  assert(storage_info);

  if (storage_info->GetBlobFiles().empty()) {
    return false;
  }

  for (size_t i = 0; i < inputs_.size(); ++i) {
    for (const FileMetaData* meta : inputs_[i].files) {
      assert(meta);

      if (meta->oldest_blob_file_number != kInvalidBlobFileNumber) {
        return true;
      }
    }
  }

  return false;
}

uint64_t Compaction::MinInputFileOldestAncesterTime(
    const InternalKey* start, const InternalKey* end) const {
  uint64_t min_oldest_ancester_time = std::numeric_limits<uint64_t>::max();
  const InternalKeyComparator& icmp =
      column_family_data()->internal_comparator();
  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      if (start != nullptr && icmp.Compare(file->largest, *start) < 0) {
        continue;
      }
      if (end != nullptr && icmp.Compare(file->smallest, *end) > 0) {
        continue;
      }
      uint64_t oldest_ancester_time = file->TryGetOldestAncesterTime();
      if (oldest_ancester_time != 0) {
        min_oldest_ancester_time =
            std::min(min_oldest_ancester_time, oldest_ancester_time);
      }
    }
  }
  return min_oldest_ancester_time;
}

uint64_t Compaction::MinInputFileEpochNumber() const {
  uint64_t min_epoch_number = std::numeric_limits<uint64_t>::max();
  for (const auto& inputs_per_level : inputs_) {
    for (const auto& file : inputs_per_level.files) {
      min_epoch_number = std::min(min_epoch_number, file->epoch_number);
    }
  }
  return min_epoch_number;
}

int Compaction::EvaluatePenultimateLevel(
    const VersionStorageInfo* vstorage,
    const ImmutableOptions& immutable_options, const int start_level,
    const int output_level) {
  // TODO: currently per_key_placement feature only support level and universal
  //  compaction
  if (immutable_options.compaction_style != kCompactionStyleLevel &&
      immutable_options.compaction_style != kCompactionStyleUniversal) {
    return kInvalidLevel;
  }
  if (output_level != immutable_options.num_levels - 1) {
    return kInvalidLevel;
  }

  int penultimate_level = output_level - 1;
  assert(penultimate_level < immutable_options.num_levels);
  if (penultimate_level <= 0) {
    return kInvalidLevel;
  }

  // If the penultimate level is not within input level -> output level range
  // check if the penultimate output level is empty, if it's empty, it could
  // also be locked for the penultimate output.
  // TODO: ideally, it only needs to check if there's a file within the
  //  compaction output key range. For simplicity, it just check if there's any
  //  file on the penultimate level.
  if (start_level == immutable_options.num_levels - 1 &&
      (immutable_options.compaction_style != kCompactionStyleUniversal ||
       !vstorage->LevelFiles(penultimate_level).empty())) {
    return kInvalidLevel;
  }

  bool supports_per_key_placement =
      immutable_options.preclude_last_level_data_seconds > 0;

  // it could be overridden by unittest
  TEST_SYNC_POINT_CALLBACK("Compaction::SupportsPerKeyPlacement:Enabled",
                           &supports_per_key_placement);
  if (!supports_per_key_placement) {
    return kInvalidLevel;
  }

  return penultimate_level;
}

}  // namespace ROCKSDB_NAMESPACE
