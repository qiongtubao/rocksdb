//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_level.h"

#include <string>
#include <utility>
#include <vector>

#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief 判断是否需要对列族进行Compaction
 *
 * 该函数检查多种Compaction触发条件：
 * 1. TTL过期文件
 * 2. 标记为周期性Compaction的文件
 * 3. 底层标记为需要Compaction的文件
 * 4. 标记为需要Compaction的文件（手动或自动标记）
 * 5. 强制Blob GC的文件
 * 6. Level层大小超过阈值（CompactionScore >= 1）
 *
 * 对于Level风格的Compaction，最关键的是第6条：当某层的实际大小超过目标大小时触发
 *
 * @param vstorage 版本存储信息，包含各层的文件和Compaction分数
 * @return true表示需要Compaction，false表示不需要
 */
bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  // 检查TTL（Time To Live）过期的文件
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  // 检查标记为周期性Compaction的文件
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  // 检查底层标记为需要Compaction的文件（如删除的key可以回收）
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  // 检查标记为需要Compaction的文件（手动Compact或其他原因）
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  // 检查标记为强制Blob GC的文件
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  // 遍历所有可能的输入层，检查是否有层的Compaction分数>=1
  // 这是最核心的Level层满触发逻辑
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      // CompactionScore >= 1 表示该层实际大小已达到目标大小
      // 需要进行Compaction来控制层级大小
      return true;
    }
  }
  return false; // 没有触发条件，不需要Compaction
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();
  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // From `start_level_`, pick files to compact to `output_level_`.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one for
  // all compaction priorities except round-robin. For round-robin,
  // multiple consecutive files may be put into inputs->files.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // Return true if a L0 trivial move is picked up.
  bool TryPickL0TrivialMove();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the initial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNonL0TrivialMove(int start_index,
                                 bool only_expand_right = false);

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      bool compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  bool is_l0_trivial_move_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

/**
 * @brief 从给定的文件列表中选择一个文件作为 compaction 的起始文件
 *
 * 本函数遍历给定的文件列表，尝试选择一个文件作为 compaction 的起点：
 * 1. 跳过正在被 compact 的文件（being_compacted 标志）
 * 2. 跳过最后一层的文件（如果 compact_to_next_level 为 true）
 * 3. 跳过 L0 文件（如果已有 L0 compaction 在进行）
 * 4. 调用 ExpandInputsToCleanCut() 扩展文件到 clean cut
 *
 * @param level_files 待选择的文件列表，每个元素是 (level, FileMetaData*) 对
 * @param compact_to_next_level 是否 compact 到下一层
 *        - true: output level = level + 1（或 L0->base_level）
 *        - false: output level = level（Intra-L0 compaction）
 *
 * @note 成功选择后，start_level_inputs_ 和 output_level_ 会被设置
 * @note 如果没有找到合适的文件，start_level_inputs_.files 会被清空
 *
 * @note level_files 的来源：
 *   - SetupInitialFiles() 中默认调用：使用 compaction score 排序的文件列表
 *   - 手动指定：如 BottommostFilesMarkedForCompaction()、ExpiredTtlFiles() 等
 */
void LevelCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    bool compact_to_next_level) {
  // 遍历所有候选文件
  for (auto& level_file : level_files) {
    // 断言：文件不能已经被标记为 being_compacted
    // 如果断言失败，说明某个函数标记了文件为 being_compacted，但没有调用 ComputeCompactionScore()
    // ComputeCompactionScore() 会重新计算 score 并更新 being_compacted 状态
    //
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);

    // 设置 start level 为当前文件的层级
    start_level_ = level_file.first;

    // 检查是否需要跳过当前文件
    //
    // 情况 1：compact_to_next_level 为 true，且当前是最后一层
    //         最后一层不能 compact 到下一层，因为没有下一层
    //         skip if it's the last level and we're compacting to next level
    //
    // 情况 2：当前是 L0，且已有 L0 compaction 在进行
    //         L0 有独占限制，同一时间只能有一个 L0 compaction
    //         level0_compactions_in_progress_ 保存正在进行的 L0 compaction
    if ((compact_to_next_level &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      // 跳过当前文件，继续尝试下一个
      continue;
    }

    // 确定 output level
    // compact_to_next_level: true -> 输出到下一层
    //                       false -> 输出到同一层（Intra-L0 compaction）
    if (compact_to_next_level) {
      // L0 -> base level（通常是 L1，但如果 L1 空则可能是 L2）
      // 其他层 -> level + 1
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      // 输出到同一层（Intra-L0 compaction）
      output_level_ = start_level_;
    }

    // 设置初始输入文件：只包含当前选中的文件
    // 稍后会通过 ExpandInputsToCleanCut() 扩展到多个文件
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;

    // 尝试扩展输入文件到"clean cut"
    // ExpandInputsToCleanCut() 的作用：
    //   1. 添加相邻的重叠文件，确保不切割相同的 user key
    //   2. 检查是否有文件被其他 compaction 锁定
    //   3. 检查是否可以形成有效的 compaction
    //
    // 如果返回 true，表示成功扩展并选择了文件
    // 如果返回 false，表示无法形成 clean cut（如文件被锁定），需要继续尝试下一个文件
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      // 成功选择了文件，直接返回
      return;
    }
    // ExpandInputsToCleanCut() 返回 false，清空 inputs，继续循环
  }

  // 没有找到任何合适的文件，清空 inputs
  // start_level_inputs_.files.clear() 表示没有选择到文件
  start_level_inputs_.files.clear();
}

/**
 * @brief 设置 compaction 的初始输入文件
 *
 * 本函数是 Level Compaction 的核心选择逻辑，负责：
 * 1. 遍历所有层级，按照 compaction score 从高到低的顺序选择需要 compact 的层
 * 2. 对选中的层调用 PickFileToCompact() 选择初始文件
 * 3. 处理 L0 的特殊情况（L0->base_level compact 和 Intra-L0 compact）
 * 4. 如果没有找到基于 score 的 compaction，则尝试其他类型的 compaction
 *    （标记的文件、底层文件的删除标记清理、TTL compact 等）
 *
 * @note Compaction score 的含义：
 *   - L0: score = num_L0_files / level0_file_num_compaction_trigger
 *   - L1+: score = Level_files_size / MaxBytesForLevel(level)
 *   - score >= 1 表示该层需要 compaction
 *
 * @note 每次调用只选择一个 compaction（一个层），不会同时选择多个层
 */
void LevelCompactionBuilder::SetupInitialFiles() {
  // 标记是否跳过了 L0->base_level 的 compaction
  // 如果为 true，表示 L0 需要 compact 但被阻塞，需要避免 base level 的 compaction 饿死 L0
  bool skipped_l0_to_base = false;

  // 每次都会重新算分 分数从高到低排序。（VersionStorageInfo::ComputeCompactionScore）
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    // 获取第 i 个 score 最高层级的 score 值和对应的层级号
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);

    // 断言：确保 score 是按降序排列的（i=0 是 L0，score 最高）
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));

    // 只有 score >= 1 的层才需要 compaction
    if (start_level_score_ >= 1) {
      // 如果之前跳过了 L0->base_level，且当前是 base level
      // 则跳过 base level 的 compaction，避免饿死 L0
      // 这是因为 L0 文件数过多会导致读性能下降和写 stall
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // 如果 L0->base_level compaction 正在等待，不要调度 base level 的 compaction
        // 否则 L0->base_level compaction 可能会被饿死（永远得不到资源）
        continue;
      }

      // 确定 output level：
      //   - L0 -> base level（通常是 L1，但如果 L1 是空的，base level 可能是 L2）
      //   - 其他 level -> level + 1
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;

      // 尝试选择文件进行 compact
      // PickFileToCompact() 会：
      //   1. 在选定的层级选择一个起始文件（基于 score 或其他规则）
      //   2. 调用 ExpandInputsToCleanCut() 扩展文件集合到"clean cut"
      //      （确保 compaction 边界不会切割相同的 user key）
      //   3. 检查文件是否被其他 compaction 锁定（being_compacted）
      bool picked_file_to_compact = PickFileToCompact();
      TEST_SYNC_POINT_CALLBACK("PostPickFileToCompact",
                               &picked_file_to_compact);

      if (picked_file_to_compact) {
        // 成功选择了文件，设置 compaction 原因
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`  more
          // L0 的 score 是基于文件数量计算，而不是文件大小
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          // L1 及以上层的 score 是基于文件总大小计算
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        // 找到合适的 compaction，退出循环（一次只选一个）
        break;
      } else {
        // 没有找到可 compact 的文件，清空 inputs
        // 可能的原因：
        //   - 该层的所有文件都在 being_compacted 状态
        //   - 该层和 output level 的文件重叠，无法形成 clean cut
        //   - L0->base_level 被 base level 下方的 compaction 阻塞
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          // 标记跳过了 L0->base_level
          skipped_l0_to_base = true;

          // L0->base_level 可能被以下情况阻塞：
          //   1. 正在进行的 L0->base_level compaction（L0 有独占限制）
          //   2. 正在进行的 base level -> level+1 compaction（key range 重叠）
          //
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // 为了减少 L0 文件数量（降低写 stall 的可能性），
          // 可以尝试在 L0 内部进行 compaction（Intra-L0 compaction）
          // Intra-L0 compaction 只处理 L0 内部的文件，不涉及其他层
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            // 成功选择了 Intra-L0 compaction
            // Intra-L0 compaction 的 output level 也是 0
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;  // 找到 compaction，退出循环
          }
        }
        // 如果是 L1+ 层且没有找到文件，继续尝试下一个 score 低的层
      }
    } else {
      // score < 1，不需要 compaction
      // 由于 score 是降序排列的，后续层的 score 也会 < 1
      // 直接退出循环
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }

  // 如果已经选择了文件，直接返回
  if (!start_level_inputs_.empty()) {
    return;
  }

  // ==================== 以下是没有 score >= 1 的情况 ====================

  // 尝试选择被标记为需要 compact 的文件
  // 这些文件可能因为：
  //   - 用户手动调用 CompactFiles()
  //   - 文件标记为需要 compact（通过 MarkFileForCompaction）
  //   - 其他原因需要重写文件
  //
  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    return;
  }

  // 尝试底层的删除标记清理 compaction
  // 底层文件可能包含大量已删除记录的 tombstone，定期清理可以释放空间
  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    return;
  }

  // 尝试 TTL Compaction（Time To Live）
  // 对于设置了 TTL 的列族，超过 TTL 的文件需要被 compact
  // 只有在 compaction_pri 为 kRoundRobin 时才会优先处理
  if (ioptions_.compaction_pri == kRoundRobin &&
      !vstorage_->ExpiredTtlFiles().empty()) {
    auto expired_files = vstorage_->ExpiredTtlFiles();
    // Expired TTL 文件列表应该已经按 level 排序
    // the expired files list should already be sorted by level
    start_level_ = expired_files.front().first;
#ifndef NDEBUG
    for (const auto& file : expired_files) {
      assert(start_level_ <= file.first);
    }
#endif
    if (start_level_ > 0) {
      output_level_ = start_level_ + 1;
      if (PickFileToCompact()) {
        compaction_reason_ = CompactionReason::kRoundRobinTtl;
        return;
      }
    }
  }

  // 尝试通用的 TTL Compaction
  // 处理所有过期的 TTL 文件
  PickFileToCompact(vstorage_->ExpiredTtlFiles(), true);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    return;
  }

  // Forced blob garbage collection
  PickFileToCompact(vstorage_->FilesMarkedForForcedBlobGC(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kForcedBlobGC;
    return;
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0 && !is_l0_trivial_move_) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

void LevelCompactionBuilder::SetupOtherFilesWithRoundRobinExpansion() {
  // We only expand when the start level is not L0 under round robin
  assert(start_level_ >= 1);

  // For round-robin compaction priority, we have 3 constraints when picking
  // multiple files.
  // Constraint 1: We can only pick consecutive files
  //  -> Constraint 1a: When a file is being compacted (or some input files
  //                    are being compacted after expanding, we cannot
  //                    choose it and have to stop choosing more files
  //  -> Constraint 1b: When we reach the last file (with largest keys), we
  //                    cannot choose more files (the next file will be the
  //                    first one)
  // Constraint 2: We should ensure the total compaction bytes (including the
  //               overlapped files from the next level) is no more than
  //               mutable_cf_options_.max_compaction_bytes
  // Constraint 3: We try our best to pick as many files as possible so that
  //               the post-compaction level size is less than
  //               MaxBytesForLevel(start_level_)
  // Constraint 4: We do not expand if it is possible to apply a trivial move
  // Constraint 5 (TODO): Try to pick minimal files to split into the target
  //               number of subcompactions
  TEST_SYNC_POINT("LevelCompactionPicker::RoundRobin");

  // Only expand the inputs when we have selected a file in start_level_inputs_
  if (start_level_inputs_.size() == 0) return;

  uint64_t start_lvl_bytes_no_compacting = 0;
  uint64_t curr_bytes_to_compact = 0;
  uint64_t start_lvl_max_bytes_to_compact = 0;
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  // Constraint 3 (pre-calculate the ideal max bytes to compact)
  for (auto f : level_files) {
    if (!f->being_compacted) {
      start_lvl_bytes_no_compacting += f->fd.GetFileSize();
    }
  }
  if (start_lvl_bytes_no_compacting >
      vstorage_->MaxBytesForLevel(start_level_)) {
    start_lvl_max_bytes_to_compact = start_lvl_bytes_no_compacting -
                                     vstorage_->MaxBytesForLevel(start_level_);
  }

  size_t start_index = vstorage_->FilesByCompactionPri(start_level_)[0];
  InternalKey smallest, largest;
  // Constraint 4 (No need to check again later)
  compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
  CompactionInputFiles output_level_inputs;
  output_level_inputs.level = output_level_;
  vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                  &output_level_inputs.files);
  if (output_level_inputs.empty()) {
    if (TryExtendNonL0TrivialMove((int)start_index,
                                  true /* only_expand_right */)) {
      return;
    }
  }
  // Constraint 3
  if (start_level_inputs_[0]->fd.GetFileSize() >=
      start_lvl_max_bytes_to_compact) {
    return;
  }
  CompactionInputFiles tmp_start_level_inputs;
  tmp_start_level_inputs = start_level_inputs_;
  // TODO (zichen): Future parallel round-robin may also need to update this
  // Constraint 1b (only expand till the end)
  for (size_t i = start_index + 1; i < level_files.size(); i++) {
    auto* f = level_files[i];
    if (f->being_compacted) {
      // Constraint 1a
      return;
    }

    tmp_start_level_inputs.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &tmp_start_level_inputs) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {tmp_start_level_inputs}, output_level_,
            Compaction::EvaluatePenultimateLevel(
                vstorage_, ioptions_, start_level_, output_level_))) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    curr_bytes_to_compact = 0;
    for (auto start_lvl_f : tmp_start_level_inputs.files) {
      curr_bytes_to_compact += start_lvl_f->fd.GetFileSize();
    }

    // Check whether any output level files are locked
    compaction_picker_->GetRange(tmp_start_level_inputs, &smallest, &largest);
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    uint64_t start_lvl_curr_bytes_to_compact = curr_bytes_to_compact;
    for (auto output_lvl_f : output_level_inputs.files) {
      curr_bytes_to_compact += output_lvl_f->fd.GetFileSize();
    }
    if (curr_bytes_to_compact > mutable_cf_options_.max_compaction_bytes) {
      // Constraint 2
      tmp_start_level_inputs.clear();
      return;
    }

    start_level_inputs_.files = tmp_start_level_inputs.files;
    // Constraint 3
    if (start_lvl_curr_bytes_to_compact > start_lvl_max_bytes_to_compact) {
      return;
    }
  }
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    bool round_robin_expanding =
        ioptions_.compaction_pri == kRoundRobin &&
        compaction_reason_ == CompactionReason::kLevelMaxLevelSize;
    if (round_robin_expanding) {
      SetupOtherFilesWithRoundRobinExpansion();
    }
    if (!is_l0_trivial_move_ &&
        !compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_,
            round_robin_expanding)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(
            compaction_inputs_, output_level_,
            Compaction::EvaluatePenultimateLevel(
                vstorage_, ioptions_, start_level_, output_level_))) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    if (!is_l0_trivial_move_) {
      compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                          output_level_inputs_, &grandparents_);
    }
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

/**
 * @brief 选择并构建一个 compaction 任务
 *
 * 本函数是 Level Compaction 的入口点，负责：
 * 1. 调用 SetupInitialFiles() 选择初始输入文件
 * 2. 如果是 L0->base level compact，设置其他 L0 文件（处理 L0 文件重叠）
 * 3. 设置 output level 的输入文件（与 start level 重叠的文件）
 * 4. 构建 Compaction 对象并返回
 *
 * @return Compaction* 返回构建的 compaction 对象，如果没有需要 compact 的文件则返回 nullptr
 *
 * @note 一次 PickCompaction 只返回一个 compaction 对象
 *       但这个 compaction 可能包含多个输入层（L0->L1 时可能涉及 L0 和 L1）
 *       每个输入层可能包含多个文件
 */
Compaction* LevelCompactionBuilder::PickCompaction() {
  // 选择初始文件：根据 compaction score、标记的文件、TTL 等规则
  // 选定的文件可能已经被扩展到 clean cut（不切割相同的 user key）
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();

  // 如果没有选择到任何文件，返回 nullptr
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // 如果是 L0 -> base level 的 compaction
  // L0 文件可能相互重叠，需要添加所有重叠的 L0 文件到输入中
  // 以避免在读取时遗漏数据
  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  if (!SetupOtherL0FilesIfNeeded()) {
    return nullptr;
  }

  // 设置 output level 的输入文件
  // 同时可能需要扩展 start level 的文件（由于 key range 重叠）
  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // 创建并返回 Compaction 对象
  // compaction_inputs_ 包含所有输入层的文件（通常是 1-2 个层）
  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

/**
 * @brief 根据 compaction_inputs_ 构建 Compaction 对象
 *
 * @return Compaction* 返回新创建的 Compaction 对象
 *
 * @note 创建 Compaction 对象后会：
 *   1. 注册 compaction（如果是 L0 compaction，会设置独占标志）
 *   2. 重新计算 compaction score（排除正在被 compact 的文件）
 */
Compaction* LevelCompactionBuilder::GetCompaction() {
  // 关于 L0 Trivial Move 的说明：
  // Trivial Move 是一种优化：当 L0 文件不与任何其他 L0 文件重叠时，
  // 可以直接移动到下一层而不需要读取和重写数据
  // TryPickL0TrivialMove() does not apply to the case when compacting L0 to an
  // empty output level. So L0 files is picked in PickFileToCompact() by
  // compaction score. We may still be able to do trivial move when this file
  // does not overlap with other L0s. This happens when
  // compaction_inputs_[0].size() == 1 since SetupOtherL0FilesIfNeeded() did not
  // pull in more L0s.
  assert(!compaction_inputs_.empty());

  // 判断 L0 文件是否可能重叠
  // L0 文件天然是相互重叠的，但在某些情况下（如 Trivial Move）可以避免
  bool l0_files_might_overlap =
      start_level_ == 0 && !is_l0_trivial_move_ &&
      (compaction_inputs_.size() > 1 || compaction_inputs_[0].size() > 1);

  // 创建 Compaction 对象
  // compaction_inputs_ 是 std::vector<CompactionInputFiles>
  // 每个 CompactionInputFiles 代表一个输入层，包含该层需要 compact 的文件列表
  // 通常：
  //   - L1->L2: inputs_[0] = L1 文件, inputs_[1] = L2 文件（2 个输入层）
  //   - L0->L1: inputs_[0] = L0 文件, inputs_[1] = L1 文件（2 个输入层）
  //   - Intra-L0: inputs_[0] = L0 文件（1 个输入层）
  //
  // 注意：一次 PickCompaction 只创建一个 Compaction 对象，但这个对象可能包含：
  //   - 多个输入层（通常 2 个：start level 和 output level）
  //   - 每个输入层可能有多个文件
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      /* trim_ts */ "", start_level_score_, false /* deletion_compaction */,
      l0_files_might_overlap, compaction_reason_);

  // 注册 compaction
  // 如果是 L0 compaction，会添加到 level0_compactions_in_progress_
  // 确保 L0 同时只有一个 compaction 在运行
  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // 重新计算 compaction score
  // 因为选中的文件被标记为 being_compacted
  // 下次计算 score 时会跳过这些文件，可能改变 score
  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);

  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

/**
 * @brief 尝试选择 L0 层的文件进行 Trivial Move（无需合并的文件移动）
 *
 * 本函数用于识别并选择 L0 层中可以直接移动到 L1 层的文件，无需进行合并操作。
 * Trivial Move 是一种优化手段，可以显著减少 IO 和 CPU 开销。
 *
 * Trivial Move 的条件：
 * 1. 输入文件与输出层没有任何 key 范围重叠
 * 2. 不需要读取、合并、重写数据，只需修改文件元数据（移动文件指针）
 * 3. 输出层（L1）非空（避免 surprising behavior）
 *
 * 函数的工作流程：
 * 1. 前置条件检查：base_level > 0、单 DB path、无压缩配置等
 * 2. 从 L0 最旧的文件开始（rbegin = 逆序迭代器）
 * 3. 向新文件方向扩展，构建一个连续的 trivial move 范围
 * 4. 确保扩展后的文件范围与 L1 没有任何重叠
 * 5. 对选中的文件按 key 范围排序
 *
 * 限制条件：
 * - base_level <= 0：不支持（没有 base level，无法进行 trivial move）
 * - start_level_ 必须是 0（本函数只处理 L0 -> L1）
 * - compression_per_level 必须为空（避免压缩配置导致的复杂性）
 * - output_level_ 必须非空（避免空 L1 时的 surprising behavior）
 * - db_paths.size() <= 1（多路径时难以预测是否为 trivial move）
 *
 * 为什么要从最旧的文件开始？
 * - 最旧的文件通常在列表末尾（按生成时间排序）
 * - 从最旧文件开始，向新文件扩展，可以找到最大的连续 trivial move 范围
 * - 这样可以一次性移动多个文件，提高效率
 *
 * 为什么不考虑中间的文件范围？
 * - In theory，文件列表中间也可能存在 trivial move 范围
 * - 但这种情况比较少见，忽略它们以简化逻辑
 * - 只关注从最旧文件开始的连续范围
 *
 * @return true 成功选中了 trivial move 文件，保存到 start_level_inputs_
 * @return false 未选中文件（条件不满足或找不到合适的范围）
 *
 * @note Trivial Move 的优势：
 *   - 不需要读取和重写数据，只修改文件元数据
 *   - IO 开销极小，只需移动文件指针
 *   - CPU 开销极小，无需解压和压缩
 *   - 显著减少 compaction 造成的写放大
 *
 * @note 为什么不支持 compression_per_level？
 *   - 如果 L0 和 L1 使用不同的压缩算法，需要重新压缩
 *   - 这就不是 trivial move 了，需要完整的读-压缩-写流程
 *   - 避免复杂的判断逻辑，简化实现
 *
 * @note 为什么 output_level 必须非空？
 *   - 如果 L1 为空，L0 的最旧文件总是可以 trivial move
 *   - 这会导致每次都 trivial move，而不是等到有足够多文件
 *   - 避免 surprising behavior（意外行为），减少不必要的操作
 *
 * @note is_l0_trivial_move_ 标志：
 *   - 成功选中文件后，设置为 true
 *   - 后续逻辑会根据此标志决定是否使用 trivial move 优化
 *   - 例如：Compaction 构造时会设置 is_trivial_move = true
 *
 * @see TryExtendNonL0TrivialMove 非 L0 层的 trivial move 扩展逻辑
 */
bool LevelCompactionBuilder::TryPickL0TrivialMove() {
  // 如果 base_level <= 0，说明没有有效的 base level
  // 无法进行 L0->L1 的 trivial move，直接返回 false
  if (vstorage_->base_level() <= 0) {
    return false;
  }
  // 检查前置条件：
  // 1. start_level_ == 0：本函数只处理 L0 -> L1 的 trivial move
  // 2. compression_per_level 为空：避免压缩配置导致的复杂性
  // 3. output_level_ 非空：避免空 L1 时的 surprising behavior
  // 4. db_paths.size() <= 1：多路径时难以预测是否为 trivial move
  if (start_level_ == 0 && mutable_cf_options_.compression_per_level.empty() &&
      !vstorage_->LevelFiles(output_level_).empty() &&
      ioptions_.db_paths.size() <= 1) {
    // Try to pick trivial move from L0 to L1. We start from the oldest
    // file. We keep expanding to newer files if it would form a
    // trivial move.
    // 尝试从 L0 选择 trivial move 文件到 L1。
    // 从最旧的文件开始，向新文件方向扩展，只要扩展后仍是 trivial move。
    // For now we don't support it with
    // mutable_cf_options_.compression_per_level to prevent the logic
    // of determining whether L0 can be trivial moved to the next level.
    // 暂时不支持 compression_per_level，以避免判断 L0 是否可以 trivial move 的复杂逻辑。
    // We skip the case where output level is empty, since in this case, at
    // least the oldest file would qualify for trivial move, and this would
    // be a surprising behavior with few benefits.
    // 跳过 output level 为空的情况，因为在这种情况下，至少最旧文件会符合 trivial move，
    // 这将是一个令人惊讶的行为，且收益很少。

    // We search from the oldest file from the newest. In theory, there are
    // files in the middle can form trivial move too, but it is probably
    // uncommon and we ignore these cases for simplicity.
    // 从最旧文件向新文件搜索。理论上，文件列表中间也可能形成 trivial move，
    // 但这种情况可能不常见，我们为了简单起见忽略这些情况。

    // 获取 L0 层的所有文件（按生成时间排序，最新的在前，最旧的在后）
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);

    // 用于记录当前 trivial move 范围的最小和最大 key
    InternalKey my_smallest, my_largest;
    // 从最旧的文件开始（rbegin = reverse begin，即列表末尾）
    for (auto it = level_files.rbegin(); it != level_files.rend(); ++it) {
      // 创建输出层的输入文件列表
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level_;
      // 当前处理的文件
      FileMetaData* file = *it;
      // 如果是第一个文件（最旧文件），初始化范围
      if (it == level_files.rbegin()) {
        my_smallest = file->smallest;
        my_largest = file->largest;
      } else {
        // 扩展范围：检查当前文件是否与现有范围连接（无重叠）
        // 如果当前文件的最大 key < 范围的最小 key，说明在左边
        if (compaction_picker_->icmp()->Compare(file->largest, my_smallest) <
            0) {
          // 更新范围的最小 key
          my_smallest = file->smallest;
        } else if (compaction_picker_->icmp()->Compare(file->smallest,
                                                       my_largest) > 0) {
          // 如果当前文件的最小 key > 范围的最大 key，说明在右边
          // 更新范围的最大 key
          my_largest = file->largest;
        } else {
          // 否则，当前文件与现有范围有重叠
          // 无法形成 trivial move（需要合并），停止扩展
          break;
        }
      }
      // 检查当前范围是否与输出层（L1）有重叠
      vstorage_->GetOverlappingInputs(output_level_, &my_smallest, &my_largest,
                                      &output_level_inputs.files);
      // 如果没有重叠，说明可以 trivial move
      if (output_level_inputs.empty()) {
        // 确保文件没有被正在压缩
        assert(!file->being_compacted);
        // 添加到选中的文件列表
        start_level_inputs_.files.push_back(file);
      } else {
        // 有重叠，无法 trivial move，停止扩展
        break;
      }
    }
  }

  // 如果成功选中了文件，进行后续处理
  if (!start_level_inputs_.empty()) {
    // Sort files by key range. Not sure it's 100% necessary but it's cleaner
    // to always keep files sorted by key the key ranges don't overlap.
    // 按 key 范围对文件排序。虽然不是 100% 必要，但让文件始终按 key 排序会更清晰，
    // 因为 trivial move 的文件不会有 key 重叠。
    std::sort(start_level_inputs_.files.begin(),
              start_level_inputs_.files.end(),
              [icmp = compaction_picker_->icmp()](FileMetaData* f1,
                                                  FileMetaData* f2) -> bool {
                // 按 smallest key 升序排序
                return (icmp->Compare(f1->smallest, f2->smallest) < 0);
              });

    // 设置标志，标记这是一个 L0 trivial move
    is_l0_trivial_move_ = true;
    return true;  // 成功选中文件
  }
  return false;  // 未选中文件
}

bool LevelCompactionBuilder::TryExtendNonL0TrivialMove(int start_index,
                                                       bool only_expand_right) {
  if (start_level_inputs_.size() == 1 &&
      (ioptions_.db_paths.empty() || ioptions_.db_paths.size() == 1) &&
      (mutable_cf_options_.compression_per_level.empty())) {
    // Only file of `index`, and it is likely a trivial move. Try to
    // expand if it is still a trivial move, but not beyond
    // max_compaction_bytes or 4 files, so that we don't create too
    // much compaction pressure for the next level.
    // Ignore if there are more than one DB path, as it would be hard
    // to predict whether it is a trivial move.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);
    const size_t kMaxMultiTrivialMove = 4;
    FileMetaData* initial_file = start_level_inputs_.files[0];
    size_t total_size = initial_file->fd.GetFileSize();
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    // Expand towards right
    for (int i = start_index + 1;
         i < static_cast<int>(level_files.size()) &&
         start_level_inputs_.size() < kMaxMultiTrivialMove;
         i++) {
      FileMetaData* next_file = level_files[i];
      if (next_file->being_compacted) {
        break;
      }
      vstorage_->GetOverlappingInputs(output_level_, &(initial_file->smallest),
                                      &(next_file->largest),
                                      &output_level_inputs.files);
      if (!output_level_inputs.empty()) {
        break;
      }
      if (i < static_cast<int>(level_files.size()) - 1 &&
          compaction_picker_->icmp()
                  ->user_comparator()
                  ->CompareWithoutTimestamp(
                      next_file->largest.user_key(),
                      level_files[i + 1]->smallest.user_key()) == 0) {
        TEST_SYNC_POINT_CALLBACK(
            "LevelCompactionBuilder::TryExtendNonL0TrivialMove:NoCleanCut",
            nullptr);
        // Not a clean up after adding the next file. Skip.
        break;
      }
      total_size += next_file->fd.GetFileSize();
      if (total_size > mutable_cf_options_.max_compaction_bytes) {
        break;
      }
      start_level_inputs_.files.push_back(next_file);
    }
    // Expand towards left
    if (!only_expand_right) {
      for (int i = start_index - 1;
           i >= 0 && start_level_inputs_.size() < kMaxMultiTrivialMove; i--) {
        FileMetaData* next_file = level_files[i];
        if (next_file->being_compacted) {
          break;
        }
        vstorage_->GetOverlappingInputs(output_level_, &(next_file->smallest),
                                        &(initial_file->largest),
                                        &output_level_inputs.files);
        if (!output_level_inputs.empty()) {
          break;
        }
        if (i > 0 && compaction_picker_->icmp()
                             ->user_comparator()
                             ->CompareWithoutTimestamp(
                                 next_file->smallest.user_key(),
                                 level_files[i - 1]->largest.user_key()) == 0) {
          // Not a clean up after adding the next file. Skip.
          break;
        }
        total_size += next_file->fd.GetFileSize();
        if (total_size > mutable_cf_options_.max_compaction_bytes) {
          break;
        }
        // keep `files` sorted in increasing order by key range
        start_level_inputs_.files.insert(start_level_inputs_.files.begin(),
                                         next_file);
      }
    }
    return start_level_inputs_.size() > 1;
  }
  return false;
}

/**
 * @brief 根据 compaction 优先级选择一个文件进行 compaction（无参数版本）
 *
 * 本函数是 `SetupInitialFiles()` 中调用的无参数重载版本，用于：
 * 1. 检查 L0 独占限制
 * 2. 尝试 L0 Trivial Move 优化
 * 3. 根据 compaction 优先级（size、round-robin 等）选择文件
 * 4. 扩展输入文件到 clean cut
 * 5. 处理 output level 的重叠文件
 *
 * @return bool 成功选择了文件返回 true，否则返回 false
 *
 * @note 与带参数版本的 PickFileToCompact() 的区别：
 *   - 带参数版本：从给定的文件列表中选择，用于手动指定文件
 *   - 无参数版本：根据 compaction 优先级自动选择，用于自动 compaction
 *
 * @note 本函数会设置：
 *   - start_level_inputs_: 输入文件列表
 *   - base_index_: 选中文件的索引
 *   - 下次 compaction 的起始索引（除 RoundRobin 外）
 */
bool LevelCompactionBuilder::PickFileToCompact() {
  // L0 独占检查：L0 文件相互重叠，同一时间只能有一个 L0 compaction
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  // 清空并初始化输入文件
  start_level_inputs_.clear();
  start_level_inputs_.level = start_level_;

  // 断言：start_level_ 必须有效
  assert(start_level_ >= 0);

  // 尝试 L0 Trivial Move 优化
  // 如果 L0 只有一个文件且不与其他 L0 文件重叠，可以直接移动到下一层
  // 不需要读取和重写数据，性能很高
  if (TryPickL0TrivialMove()) {
    return true;
  }

  // 获取当前层级的所有文件
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  // 获取按 compaction 优先级排序的文件索引列表
  // 优先级策略（compaction_pri）：
  //   - kByCompensatedSize: 按文件大小补偿排序
  //   - kOldestLargestSeqFirst: 按最旧的序列号优先
  //   - kOldestSmallestSeqFirst: 按最小的序列号优先
  //   - kMinOverlappingRatio: 按最小重叠比例排序
  //   - kRoundRobin: 轮询方式
  //
  // Pick the file with the highest score in this level that is not already
  // being compacted.
  const std::vector<int>& file_scores =
      vstorage_->FilesByCompactionPri(start_level_);

  // 从上次 compaction 的索引开始遍历（实现轮询，避免总是从头开始）
  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_scores.size(); cmp_idx++) {
    int index = file_scores[cmp_idx];
    auto* f = level_files[index];

    // 检查文件是否正在被 compact
    // being_compacted 标志表示文件已经被其他 compaction 选中
    // 不允许同时被多个 compaction 操作
    //
    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      // RoundRobin 策略的特殊处理
      // 如果文件正在被上一层的 compaction 使用，不能前进 cursor
      // TODO(zichen): this file may be involved in one compaction from
      // an upper level, cannot advance the cursor for round-robin policy.
      // Currently, we do not pick any file to compact in this case. We
      // should fix this later to ensure a compaction is picked but the
      // cursor shall not be advanced.
      if (ioptions_.compaction_pri == kRoundRobin) {
        return false;
      }
      // 其他策略：跳过当前文件，继续尝试下一个
      continue;
    }

    // 将候选文件添加到输入列表
    start_level_inputs_.files.push_back(f);

    // 尝试扩展输入文件到 clean cut
    // 如果失败，说明：
    //   1. 扩展后的文件被其他 compaction 锁定
    //   2. 与 output level 的正在进行的 compaction 重叠
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_,
            Compaction::EvaluatePenultimateLevel(
                vstorage_, ioptions_, start_level_, output_level_))) {
      // 由于 user-key 重叠，拉入了被锁定的文件
      // 清空输入列表，尝试下一个文件
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();

      // RoundRobin 策略：不前进 cursor，返回失败
      if (ioptions_.compaction_pri == kRoundRobin) {
        return false;
      }
      // 其他策略：继续尝试下一个文件
      continue;
    }

    // 输入层完全扩展后，检查 output level 是否有文件被锁定
    //
    // 注意：依赖 ExpandInputsToCleanCut() 来判断 output level 文件是否被锁定
    //       不仅仅是因 user-key 重叠而额外拉入的文件
    //
    // Now that input level is fully expanded, we check whether any output
    // files are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);

    if (output_level_inputs.empty()) {
      // output level 没有重叠文件
      // 对于非 L0 层，尝试扩展为 Trivial Move
      // Trivial Move：如果 input level 文件不与 output level 的文件重叠
      //         可以直接移动而不需要读取和合并
      if (start_level_ > 0 &&
          TryExtendNonL0TrivialMove(index,
                                    ioptions_.compaction_pri ==
                                        kRoundRobin /* only_expand_right */)) {
        // 成功扩展为 Trivial Move，退出循环
        break;
      }
      // 无法扩展，继续尝试下一个文件（隐式）
    } else {
      // output level 有重叠文件，需要合并
      // 尝试将 output level 的重叠文件也扩展到 clean cut
      if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &output_level_inputs)) {
        // 扩展失败，可能是文件被锁定
        start_level_inputs_.clear();

        // RoundRobin 策略：不前进 cursor，返回失败
        if (ioptions_.compaction_pri == kRoundRobin) {
          return false;
        }
        // 其他策略：继续尝试下一个文件
        continue;
      }
      // 扩展成功，选中了 input level 和 output level 的文件
    }

    // 记录选中的文件索引（base_index_ 用于某些特殊处理）
    base_index_ = index;
    // 成功选择了文件，退出循环
    break;
  }

  // 保存下次 compaction 的起始索引（实现轮询，避免重复选择相同的文件）
  // RoundRobin 策略不在这里更新 cursor（在失败时也不前进）
  //
  // store where to start the iteration in the next call to PickCompaction
  if (ioptions_.compaction_pri != kRoundRobin) {
    vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
  }

  // 返回是否成功选择了文件
  return start_level_inputs_.size() > 0;
}

/**
 * @brief 选择 L0 层内部进行 compaction 的文件（L0 -> L0）
 *
 * 本函数是 Intra-L0 Compaction 的入口点，负责：
 * 1. 检查是否满足触发条件（L0 文件数足够多）
 * 2. 调用 FindIntraL0Compaction 查找最佳的文件范围
 * 3. 将选中的文件保存到 start_level_inputs_
 *
 * Intra-L0 Compaction 的作用：
 * - 将 L0 中的多个文件合并为较少的文件
 * - 减少 L0 文件数量，避免 L0 积累过多小文件
 * - 不涉及 L1 或更低层，所有操作都在 L0 内部完成
 *
 * 触发条件：
 * - L0 文件数 >= level0_file_num_compaction_trigger + 2
 * - L0 第一个文件没有被正在压缩（being_compacted = false）
 *
 * 为什么需要 "+2" 的条件？
 * - 正常情况下，当 L0 文件数 >= trigger 时，会触发 L0->Lbase compaction
 * - Intra-L0 是辅助手段，只在 L0 文件"过多"时才使用
 * - "+2" 提供了一定的缓冲，避免频繁进行 Intra-L0 compaction
 * - 这样可以优先进行 L0->Lbase compaction（更高效的数据清理）
 *
 * 参数传递给 FindIntraL0Compaction：
 * - min_files_to_compact = kMinFilesForIntraL0Compaction（通常是 4）
 * - max_compact_bytes_per_del_file = 无限大（不限制每文件压缩字节数）
 * - max_compaction_bytes = max_compaction_bytes（限制总压缩大小）
 *
 * @return true 成功选中了文件，保存到 start_level_inputs_
 * @return false 未选中文件（条件不满足或找不到合适的范围）
 *
 * @note 与 L0->Lbase compaction 的优先级关系：
 *   - 优先级：L0->Lbase > Intra-L0
 *   - SetupInitialFiles() 会先尝试 L0->Lbase，失败后才尝试 Intra-L0
 *   - L0->Lbase 可以清空 L0，将数据下放到 L1
 *   - Intra-L0 只是减少 L0 文件数，数据仍在 L0
 *
 * @note L0 文件列表的特性：
 *   - 文件按大小降序排列（最大的文件在最前面）
 *   - 文件之间可能有 user-key 重叠
 *   - Intra-L0 会合并这些重叠，减少文件总数和读取开销
 *
 * @see FindIntraL0Compaction 实际执行文件查找的函数
 */
bool LevelCompactionBuilder::PickIntraL0Compaction() {
  // 清空之前选择的输入文件
  start_level_inputs_.clear();
  // 获取 L0 层的所有文件列表
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  // 检查触发条件：
  // 1. L0 文件数 >= trigger + 2（确保 L0 文件足够多）
  // 2. 第一个文件没有被正在压缩（避免并发冲突）
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    // 如果 L0 文件数没有远超触发阈值，暂时不进行 Intra-L0 compaction
    // 这样可以优先进行 L0->Lbase compaction（更高效）
    return false;
  }
  // 调用 FindIntraL0Compaction 查找最佳的文件范围
  // 参数：
  // - level_files: L0 层文件列表
  // - kMinFilesForIntraL0Compaction: 最小文件数（通常是 4）
  // - std::numeric_limits<uint64_t>::max(): 不限制每个文件的最大压缩字节数
  // - mutable_cf_options_.max_compaction_bytes: 单次 compaction 的最大总字节数
  // - &start_level_inputs_: 输出参数，保存选中的文件
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               std::numeric_limits<uint64_t>::max(),
                               mutable_cf_options_.max_compaction_bytes,
                               &start_level_inputs_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_,
                                 mutable_db_options);
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE
