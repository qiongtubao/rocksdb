//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker.h"

#include <cinttypes>
#include <limits>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "file/filename.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "monitoring/statistics_impl.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

/**
 * @brief 查找 L0 层内部需要 compaction 的文件范围（Intra-L0 Compaction）
 *
 * 本函数用于选择 L0 层中的一组文件进行内部压缩（L0 -> L0）。
 * 选择策略的核心思想是：找到一段连续的文件，使得"删除每个文件所需的压缩字节数"最小化。
 *
 * 工作原理：
 * 1. 从 L0 层的第一个文件开始，尝试向后扩展文件范围
 * 2. 计算当前范围内，平均每个文件需要处理的字节数
 * 3. 如果扩展文件后，平均处理字节数不再降低（即边际效益递减），则停止扩展
 * 4. 最终选择的文件数至少达到 min_files_to_compact，且满足压缩大小限制
 *
 * 选择标准：
 * - compact_bytes_per_del_file = 总压缩字节数 / 文件数
 * - 扩展条件：新文件的加入使得 compact_bytes_per_del_file 降低或保持不变
 * - 停止条件：
 *   1. 遇到正在压缩的文件
 *   2. compact_bytes_per_del_file 开始增加
 *   3. 总压缩字节数超过 max_compaction_bytes
 * - 最终条件：
 *   1. 文件数 >= min_files_to_compact
 *   2. compact_bytes_per_del_file < max_compact_bytes_per_del_file
 *
 * 为什么这样做？
 * L0 -> L0 compaction 的目的是减少 L0 文件数量，但不想做太多工作。
 * 通过选择"平均每个文件处理成本最低"的范围，可以在减少文件数的同时，
 * 控制压缩的工作量，避免选择过大的范围导致资源浪费。
 *
 * @param level_files L0 层的所有文件列表（按文件大小降序排列）
 * @param min_files_to_compact 触发 Intra-L0 compaction 的最小文件数（通常是 4）
 * @param max_compact_bytes_per_del_file 每个删除文件允许的最大压缩字节数
 * @param max_compaction_bytes 单次 compaction 允许的最大总字节数
 * @param comp_inputs 输出参数，保存选中的文件信息（level=0, files=文件列表）
 *
 * @return true 成功找到符合条件的文件范围
 * @return false 未找到符合条件的文件范围
 *
 * @note L0 文件列表特性：
 *   - 文件按大小降序排列（最大的文件在最前面）
 *   - 文件之间可能有 user-key 重叠
 *   - Intra-L0 compaction 会合并重叠的文件，减少文件总数
 *
 * @note 与 L0->Lbase compaction 的区别：
 *   - L0->Lbase：将 L0 文件合并到 L1（base level），用于清空 L0
 *   - Intra-L0：L0 文件之间合并，用于减少 L0 文件数，但仍在 L0
 *
 * @note 调用时机：
 *   - 当 L0 文件数远超 level0_file_num_compaction_trigger 时（通常 +2）
 *   - 优先级低于 L0->Lbase compaction（先清空 L0，再做 Intra-L0）
 */
bool FindIntraL0Compaction(const std::vector<FileMetaData*>& level_files,
                           size_t min_files_to_compact,
                           uint64_t max_compact_bytes_per_del_file,
                           uint64_t max_compaction_bytes,
                           CompactionInputFiles* comp_inputs) {
  TEST_SYNC_POINT("FindIntraL0Compaction");

  size_t start = 0;  // 压缩范围的起始索引（总是从第一个文件开始）

  // 如果 L0 没有文件或第一个文件正在被压缩，则无法进行 Intra-L0 compaction
  if (level_files.size() == 0 || level_files[start]->being_compacted) {
    return false;
  }

  // 初始化：只包含第一个文件
  size_t compact_bytes = static_cast<size_t>(level_files[start]->fd.file_size);
  // 初始值设为最大，确保第一次扩展后的值会更小（或有更严格限制）
  size_t compact_bytes_per_del_file = std::numeric_limits<size_t>::max();
  // Compaction range will be [start, limit).
  // 压缩范围为 [start, limit)，即从 start 到 limit-1（半开区间）
  size_t limit;
  // Pull in files until the amount of compaction work per deleted file begins
  // increasing or maximum total compaction size is reached.
  // 持续拉入文件，直到"每个删除文件所需的压缩字节数"开始增加，或达到最大总大小
  size_t new_compact_bytes_per_del_file = 0;
  // 从第二个文件开始尝试扩展（limit = start + 1）
  for (limit = start + 1; limit < level_files.size(); ++limit) {
    // 累计新的文件大小到总压缩字节数
    compact_bytes += static_cast<size_t>(level_files[limit]->fd.file_size);
    // 计算新的"平均每个文件需要处理的字节数"
    // 公式：总字节数 / 文件数（文件数 = limit - start）
    new_compact_bytes_per_del_file = compact_bytes / (limit - start);
    // 停止扩展的条件：
    // 1. 当前文件正在被压缩（being_compacted = true）
    // 2. 平均处理字节数开始增加（边际效益递减）
    // 3. 总压缩字节数超过最大限制
    if (level_files[limit]->being_compacted ||
        new_compact_bytes_per_del_file > compact_bytes_per_del_file ||
        compact_bytes > max_compaction_bytes) {
      break;  // 停止扩展，limit 指向第一个不满足条件的文件
    }
    // 更新平均处理字节数，为下一次扩展做准备
    compact_bytes_per_del_file = new_compact_bytes_per_del_file;
  }

  // 检查是否满足最终的压缩条件：
  // 1. 文件数 >= 最小文件数要求（min_files_to_compact，通常是 4）
  // 2. 平均处理字节数 < 每个文件允许的最大压缩字节数
  if ((limit - start) >= min_files_to_compact &&
      compact_bytes_per_del_file < max_compact_bytes_per_del_file) {
    assert(comp_inputs != nullptr);
    // 设置输出层级为 L0（Intra-L0 compaction 的输出层也是 L0）
    comp_inputs->level = 0;
    // 将选中的文件范围 [start, limit) 添加到输出列表
    for (size_t i = start; i < limit; ++i) {
      comp_inputs->files.push_back(level_files[i]);
    }
    return true;  // 成功找到符合条件的文件范围
  }
  return false;  // 未找到符合条件的文件范围
}

// Determine compression type, based on user options, level of the output
// file and whether compression is disabled.
// If enable_compression is false, then compression is always disabled no
// matter what the values of the other two parameters are.
// Otherwise, the compression type is determined based on options and level.
CompressionType GetCompressionType(const VersionStorageInfo* vstorage,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level, int base_level,
                                   const bool enable_compression) {
  if (!enable_compression) {
    // disable compression
    return kNoCompression;
  }

  // If bottommost_compression is set and we are compacting to the
  // bottommost level then we should use it.
  if (mutable_cf_options.bottommost_compression != kDisableCompressionOption &&
      level >= (vstorage->num_non_empty_levels() - 1)) {
    return mutable_cf_options.bottommost_compression;
  }
  // If the user has specified a different compression level for each level,
  // then pick the compression for that level.
  if (!mutable_cf_options.compression_per_level.empty()) {
    assert(level == 0 || level >= base_level);
    int idx = (level == 0) ? 0 : level - base_level + 1;

    const int n =
        static_cast<int>(mutable_cf_options.compression_per_level.size()) - 1;
    // It is possible for level_ to be -1; in that case, we use level
    // 0's compression.  This occurs mostly in backwards compatibility
    // situations when the builder doesn't know what level the file
    // belongs to.  Likewise, if level is beyond the end of the
    // specified compression levels, use the last value.
    return mutable_cf_options
        .compression_per_level[std::max(0, std::min(idx, n))];
  } else {
    return mutable_cf_options.compression;
  }
}

CompressionOptions GetCompressionOptions(const MutableCFOptions& cf_options,
                                         const VersionStorageInfo* vstorage,
                                         int level,
                                         const bool enable_compression) {
  if (!enable_compression) {
    return cf_options.compression_opts;
  }
  // If bottommost_compression_opts is enabled and we are compacting to the
  // bottommost level then we should use the specified compression options.
  if (level >= (vstorage->num_non_empty_levels() - 1) &&
      cf_options.bottommost_compression_opts.enabled) {
    return cf_options.bottommost_compression_opts;
  }
  return cf_options.compression_opts;
}

CompactionPicker::CompactionPicker(const ImmutableOptions& ioptions,
                                   const InternalKeyComparator* icmp)
    : ioptions_(ioptions), icmp_(icmp) {}

CompactionPicker::~CompactionPicker() {}

// Delete this compaction from the list of running compactions.
void CompactionPicker::ReleaseCompactionFiles(Compaction* c, Status status) {
  UnregisterCompaction(c);
  if (!status.ok()) {
    c->ResetNextCompactionIndex();
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  const int level = inputs.level;
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();

  if (level == 0) {
    for (size_t i = 0; i < inputs.size(); i++) {
      FileMetaData* f = inputs[i];
      if (i == 0) {
        *smallest = f->smallest;
        *largest = f->largest;
      } else {
        if (icmp_->Compare(f->smallest, *smallest) < 0) {
          *smallest = f->smallest;
        }
        if (icmp_->Compare(f->largest, *largest) > 0) {
          *largest = f->largest;
        }
      }
    }
  } else {
    *smallest = inputs[0]->smallest;
    *largest = inputs[inputs.size() - 1]->largest;
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs1,
                                const CompactionInputFiles& inputs2,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  assert(!inputs1.empty() || !inputs2.empty());
  if (inputs1.empty()) {
    GetRange(inputs2, smallest, largest);
  } else if (inputs2.empty()) {
    GetRange(inputs1, smallest, largest);
  } else {
    InternalKey smallest1, smallest2, largest1, largest2;
    GetRange(inputs1, &smallest1, &largest1);
    GetRange(inputs2, &smallest2, &largest2);
    *smallest =
        icmp_->Compare(smallest1, smallest2) < 0 ? smallest1 : smallest2;
    *largest = icmp_->Compare(largest1, largest2) < 0 ? largest2 : largest1;
  }
}

void CompactionPicker::GetRange(const std::vector<CompactionInputFiles>& inputs,
                                InternalKey* smallest, InternalKey* largest,
                                int exclude_level) const {
  InternalKey current_smallest;
  InternalKey current_largest;
  bool initialized = false;
  for (const auto& in : inputs) {
    if (in.empty() || in.level == exclude_level) {
      continue;
    }
    GetRange(in, &current_smallest, &current_largest);
    if (!initialized) {
      *smallest = current_smallest;
      *largest = current_largest;
      initialized = true;
    } else {
      if (icmp_->Compare(current_smallest, *smallest) < 0) {
        *smallest = current_smallest;
      }
      if (icmp_->Compare(current_largest, *largest) > 0) {
        *largest = current_largest;
      }
    }
  }
  assert(initialized);
}

/**
 * @brief 将输入文件扩展为 Clean Cut 范围，确保不会分割任何 user-key
 *
 * 本函数是 Level Compaction 的核心优化机制，用于确保：
 * 1. 输入文件范围不会分割任何 user-key
 * 2. Compaction 时不会丢失数据的任何部分
 * 3. 所有版本的数据都被正确处理
 *
 * 什么是 Clean Cut？
 * Clean Cut 是指输入文件范围的边界"干净"，即：
 * - 输入文件的最小 key 左边的文件的最大 key < 输入文件的最小 key
 * - 输入文件的最大 key 右边的文件的最小 key > 输入文件的最大 key
 * - 简单说：没有 user-key 被分割在输入和输出文件之间
 *
 * 为什么需要 Clean Cut？
 * 假设有以下情况：
 *   输入文件范围：[a, c]
 *   输出层有文件：[b, d]
 *   user-key "b" 同时存在于输入和输出文件中
 *
 * 如果不做 Clean Cut 扩展：
 *   - 只选择 [a, c] 作为输入
 *   - 输出层有 [b, d]
 *   - user-key "b" 的新版本可能在输出层，旧版本在输入层
 *   - Compaction 时可能只合并部分版本，导致数据不一致
 *
 * 做 Clean Cut 扩展后：
 *   - 输入范围扩展为 [a, d]（包含输出层的 [b, d]）
 *   - 确保 user-key "b" 的所有版本都被合并
 *   - 不会丢失数据的任何部分
 *
 * 扩展算法（迭代到固定点）：
 * 1. 获取当前输入文件的范围 [smallest, largest]
 * 2. 查找该层级与范围重叠的所有文件
 * 3. 如果新文件列表 > 旧文件列表，说明有新文件被拉入
 * 4. 重复步骤 1-3，直到文件数量不再增长
 * 5. 此时达到 Clean Cut（没有更多的文件与范围重叠）
 *
 * 为什么使用迭代而不是一次性计算？
 * - L1+ 层的文件是按 key 范围排序的
 * - 重叠的文件可能是连续的
 * - 迭代扩展确保找到所有重叠文件
 * - 直到固定点（文件数不再增长）为止
 *
 * L0 层的特殊处理：
 * - L0 层的文件之间可能有 key 重叠
 * - GetOverlappingInputs 已经正确处理了 L0
 * - 不需要额外扩展
 * - 直接返回 true
 *
 * next_smallest 参数：
 * - 输出参数：输入范围之后的第一个 key
 * - 用于后续判断是否可以 trivial move
 * - GetOverlappingInputs 计算并返回此值
 *
 * @param cf_name 列族名称（未使用，保留用于日志）
 * @param vstorage 版本存储信息（包含文件列表）
 * @param inputs 输入/输出参数：初始为选中文件，扩展后为 clean cut 范围
 * @param next_smallest 输出参数：输入范围之后的第一个 key（可为 null）
 *
 * @return true 成功扩展为 clean cut 范围
 * @return false 扩展失败（有文件正在被压缩）
 *
 * @note 只对 L1+ 层有效：
 *   - L0 文件之间可能重叠，GetOverlappingInputs 已处理
 *   - L1+ 文件之间不重叠，需要扩展确保 clean cut
 *
 * @note 迭代终止条件：
 *   - inputs->size() > old_size
 *   - 如果文件数不再增长，说明达到 clean cut
 *   - 此时没有更多的文件与范围重叠
 *
 * @note 如果扩展后包含正在压缩的文件：
 *   - 返回 false
 *   - 调用者需要取消或延迟 compaction
 *   - 避免并发 compaction 冲突
 *
 * @note 为什么先 clear 再 GetOverlappingInputs？
 *   - GetOverlappingInputs 会清空并填充文件列表
 *   - 先 clear 确保列表为空
 *   - 避免重复累积文件
 *
 * @note hint_index 的作用：
 *   - 优化查找重叠文件的性能
 *   - 从上次的索引开始搜索
 *   - 避免每次从头遍历
 *
 * @see GetOverlappingInputs 查找重叠文件的函数
 * @see AreFilesInCompaction 检查文件是否正在被压缩
 * @see PickFileToCompact 选择文件的函数
 */
bool CompactionPicker::ExpandInputsToCleanCut(const std::string& /*cf_name*/,
                                              VersionStorageInfo* vstorage,
                                              CompactionInputFiles* inputs,
                                              InternalKey** next_smallest) {
  // This isn't good compaction
  // 断言：输入文件列表不能为空
  // 如果为空，说明调用者逻辑有问题
  assert(!inputs->empty());

  // 获取输入文件所在的层级
  const int level = inputs->level;

  // GetOverlappingInputs will always do the right thing for level-0.
  // So we don't need to do any expansion if level == 0.
  // GetOverlappingInputs 对于 level-0 总是做正确的事情。
  // 所以如果 level == 0，我们不需要做任何扩展。
  // L0 层的特殊处理：L0 文件之间可能有重叠，
  // GetOverlappingInputs 已经正确处理了 L0 的情况。
  if (level == 0) {
    return true;  // L0 不需要扩展，直接返回成功
  }

  // 用于记录当前输入文件范围的最小和最大 key
  InternalKey smallest, largest;

  // Keep expanding inputs until we are sure that there is a "clean cut"
  // boundary between the files in input and the surrounding files.
  // This will ensure that no parts of a key are lost during compaction.
  // 持续扩展输入文件，直到我们确定输入文件与周围文件之间存在"clean cut"边界。
  // 这将确保在 compaction 期间不会丢失 key 的任何部分。
  //
  // 扩展逻辑：
  // 1. 获取当前输入文件的范围 [smallest, largest]
  // 2. 清空文件列表
  // 3. 查找该层级与范围重叠的所有文件
  // 4. 如果文件数增长，说明有新文件被拉入，继续迭代
  // 5. 如果文件数不再增长，说明达到 clean cut，停止迭代
  //
  // 为什么需要迭代？
  // - 假设初始文件是 [b, c]
  // - 范围 [b, c] 可能与 [a, d] 重叠
  // - 拉入 [a, d] 后，范围变为 [a, d]
  // - 新范围 [a, d] 可能与 [x, y] 重叠
  // - 需要继续扩展，直到不再有新文件被拉入
  int hint_index = -1;  // 优化查找的提示索引（从 -1 开始）
  size_t old_size;  // 记录上一次的文件数量

  // 循环扩展，直到达到固定点（文件数不再增长）
  do {
    // 记录当前文件数量
    old_size = inputs->size();

    // 获取当前输入文件的范围 [smallest, largest]
    GetRange(*inputs, &smallest, &largest);

    // 清空文件列表（为 GetOverlappingInputs 准备）
    // 注意：必须先清空，GetOverlappingInputs 会填充此列表
    inputs->clear();

    // 查找该层级与范围 [smallest, largest] 重叠的所有文件
    // 参数说明：
    // - level：查找的层级
    // - smallest, largest：范围边界
    // - inputs->files：输出，重叠的文件列表
    // - hint_index, &hint_index：输入/输出，优化查找的提示索引
    //   * hint_index 作为输入：从上次找到的索引开始搜索，避免重复
    //   * &hint_index 作为输出：更新为新找到的最后一个文件的索引
    // - true：使用文件大小排序（优化性能）
    // - next_smallest：输出，范围之后的第一个 key（用于 trivial move 判断）
    vstorage->GetOverlappingInputs(level, &smallest, &largest, &inputs->files,
                                   hint_index, &hint_index, true,
                                   next_smallest);
  } while (inputs->size() > old_size);  // 如果文件数增长，继续迭代

  // we started off with inputs non-empty and the previous loop only grew
  // inputs. thus, inputs should be non-empty here
  // 我们开始时 inputs 非空，且之前的循环只会增加 inputs 的大小。
  // 因此，这里 inputs 应该是非空的。
  // 如果为空，说明有逻辑错误（不应该发生）
  assert(!inputs->empty());

  // If, after the expansion, there are files that are already under
  // compaction, then we must drop/cancel this compaction.
  // 如果扩展后，有文件已经在 compaction 中，我们必须放弃/取消这个 compaction。
  // 检查扩展后的文件列表中是否有正在被压缩的文件
  if (AreFilesInCompaction(inputs->files)) {
    return false;  // 有文件正在被压缩，扩展失败
  }

  // 扩展成功，没有文件正在被压缩
  return true;
}

bool CompactionPicker::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int level) const {
  // ==================== 参数说明 ====================
  // @param smallest_user_key 要检查的键范围的最小值（用户键，不含 sequence number）
  // @param largest_user_key  要检查的键范围的最大值（用户键，不含 sequence number）
  // @param level            目标层号，检查该层是否有正在进行的压缩与给定范围重叠
  //
  // ==================== 函数功能 ====================
  // 判断给定的用户键范围 [smallest_user_key, largest_user_key] 是否与目标层（level）
  // 中任何正在进行的压缩（compactions_in_progress_）重叠
  //
  // ==================== 返回值 ====================
  // @return true  - 存在正在进行的压缩与给定范围重叠
  // @return false - 不存在正在进行的压缩与给定范围重叠
  //
  // ==================== 使用场景 ====================
  // 1. FilesRangeOverlapWithCompaction: 检查输入文件范围是否与已有压缩冲突
  // 2. SetupOtherInputs: 查找输出层中与输入范围重叠的文件时避免冲突
  // 3. PickCompaction: 选择压缩任务时避免范围重叠导致并发问题
  //
  // ==================== 重叠判断逻辑 ====================
  // 两个范围重叠的数学条件：
  //   范围 A: [smallest_A, largest_A]
  //   范围 B: [smallest_B, largest_B]
  //   重叠条件: smallest_A <= largest_B && largest_A >= smallest_B
  //
  // 本函数中的条件：
  //   (smallest_user_key <= c->GetLargestUserKey()) &&
  //   (largest_user_key >= c->GetSmallestUserKey())
  // 即：给定范围与正在进行的压缩范围有交集
  //
  // ==================== 为什么使用 CompareWithoutTimestamp？====================
  // 本函数比较的是用户键，需要忽略 timestamp 部分
  // 因为：
  //   1. 压缩范围是按用户键确定的
  //   2. 同一个用户键的不同版本（不同 sequence number 或 timestamp）
  //      属于同一个键的多个实例，不构成范围冲突
  //   3. 范围重叠的判断只需要考虑用户键本身
  //
  // ==================== Per-Key Placement 的特殊处理 ====================
  // 当压缩支持 Per-Key Placement 特性时：
  // - 输出可能同时到最后一层和倒数第二层（penultimate level）
  // - 需要检查与倒数第二层输出范围的重叠
  // - 通过 OverlapPenultimateLevelOutputRange 函数判断
  const Comparator* ucmp = icmp_->user_comparator();
  for (Compaction* c : compactions_in_progress_) {
    // 检查1：输出层是否为目标层
    if (c->output_level() == level &&
        // 检查2：判断两个范围是否重叠
        // 条件1: smallest_user_key <= c->GetLargestUserKey()
        //         给定范围的最小值不超过压缩范围的最大值
        ucmp->CompareWithoutTimestamp(smallest_user_key,
                                      c->GetLargestUserKey()) <= 0 &&
        // 条件2: largest_user_key >= c->GetSmallestUserKey()
        //         给定范围的最大值不小于压缩范围的最小值
        ucmp->CompareWithoutTimestamp(largest_user_key,
                                      c->GetSmallestUserKey()) >= 0) {
      // 范围重叠，返回 true
      // 找到任何一个正在进行的压缩与给定范围重叠即可
      // Overlap
      return true;
    }
    // 检查3：Per-Key Placement 的特殊处理
    // 如果压缩支持 Per-Key Placement（c->SupportsPerKeyPlacement() == true）
    // 则压缩可能同时输出到倒数第二层（penultimate level）
    // 需要检查给定范围是否与该压缩的倒数第二层输出范围重叠
    if (c->SupportsPerKeyPlacement()) {
      // 检查给定范围是否与倒数第二层输出范围重叠
      if (c->OverlapPenultimateLevelOutputRange(smallest_user_key,
                                                largest_user_key)) {
        return true;
      }
    }
  }
  // 检查所有正在进行的压缩后，没有发现重叠
  // Did not overlap with any running compaction in level `level`
  return false;
}

bool CompactionPicker::FilesRangeOverlapWithCompaction(
    const std::vector<CompactionInputFiles>& inputs, int level,
    int penultimate_level) const {
  bool is_empty = true;
  for (auto& in : inputs) {
    if (!in.empty()) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) {
    // No files in inputs
    return false;
  }

  // TODO: Intra L0 compactions can have the ranges overlapped, but the input
  //  files cannot be overlapped in the order of L0 files.
  InternalKey smallest, largest;
  GetRange(inputs, &smallest, &largest, Compaction::kInvalidLevel);
  if (penultimate_level != Compaction::kInvalidLevel) {
    if (ioptions_.compaction_style == kCompactionStyleUniversal) {
      if (RangeOverlapWithCompaction(smallest.user_key(), largest.user_key(),
                                     penultimate_level)) {
        return true;
      }
    } else {
      InternalKey penultimate_smallest, penultimate_largest;
      GetRange(inputs, &penultimate_smallest, &penultimate_largest, level);
      if (RangeOverlapWithCompaction(penultimate_smallest.user_key(),
                                     penultimate_largest.user_key(),
                                     penultimate_level)) {
        return true;
      }
    }
  }

  return RangeOverlapWithCompaction(smallest.user_key(), largest.user_key(),
                                    level);
}

// Returns true if any one of specified files are being compacted
bool CompactionPicker::AreFilesInCompaction(
    const std::vector<FileMetaData*>& files) {
  for (size_t i = 0; i < files.size(); i++) {
    if (files[i]->being_compacted) {
      return true;
    }
  }
  return false;
}

Compaction* CompactionPicker::CompactFiles(
    const CompactionOptions& compact_options,
    const std::vector<CompactionInputFiles>& input_files, int output_level,
    VersionStorageInfo* vstorage, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, uint32_t output_path_id) {
#ifndef NDEBUG
  assert(input_files.size());
  // This compaction output should not overlap with a running compaction as
  // `SanitizeCompactionInputFiles` should've checked earlier and db mutex
  // shouldn't have been released since.
  int start_level = Compaction::kInvalidLevel;
  for (const auto& in : input_files) {
    // input_files should already be sorted by level
    if (!in.empty()) {
      start_level = in.level;
      break;
    }
  }
  assert(output_level == 0 ||
         !FilesRangeOverlapWithCompaction(
             input_files, output_level,
             Compaction::EvaluatePenultimateLevel(vstorage, ioptions_,
                                                  start_level, output_level)));
#endif /* !NDEBUG */

  CompressionType compression_type;
  if (compact_options.compression == kDisableCompressionOption) {
    int base_level;
    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      base_level = vstorage->base_level();
    } else {
      base_level = 1;
    }
    compression_type = GetCompressionType(vstorage, mutable_cf_options,
                                          output_level, base_level);
  } else {
    // TODO(ajkr): `CompactionOptions` offers configurable `CompressionType`
    // without configurable `CompressionOptions`, which is inconsistent.
    compression_type = compact_options.compression;
  }
  auto c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options, input_files,
      output_level, compact_options.output_file_size_limit,
      mutable_cf_options.max_compaction_bytes, output_path_id, compression_type,
      GetCompressionOptions(mutable_cf_options, vstorage, output_level),
      Temperature::kUnknown, compact_options.max_subcompactions,
      /* grandparents */ {}, true);
  RegisterCompaction(c);
  return c;
}

Status CompactionPicker::GetCompactionInputsFromFileNumbers(
    std::vector<CompactionInputFiles>* input_files,
    std::unordered_set<uint64_t>* input_set, const VersionStorageInfo* vstorage,
    const CompactionOptions& /*compact_options*/) const {
  if (input_set->size() == 0U) {
    return Status::InvalidArgument(
        "Compaction must include at least one file.");
  }
  assert(input_files);

  std::vector<CompactionInputFiles> matched_input_files;
  matched_input_files.resize(vstorage->num_levels());
  int first_non_empty_level = -1;
  int last_non_empty_level = -1;
  // TODO(yhchiang): use a lazy-initialized mapping from
  //                 file_number to FileMetaData in Version.
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (auto file : vstorage->LevelFiles(level)) {
      auto iter = input_set->find(file->fd.GetNumber());
      if (iter != input_set->end()) {
        matched_input_files[level].files.push_back(file);
        input_set->erase(iter);
        last_non_empty_level = level;
        if (first_non_empty_level == -1) {
          first_non_empty_level = level;
        }
      }
    }
  }

  if (!input_set->empty()) {
    std::string message(
        "Cannot find matched SST files for the following file numbers:");
    for (auto fn : *input_set) {
      message += " ";
      message += std::to_string(fn);
    }
    return Status::InvalidArgument(message);
  }

  for (int level = first_non_empty_level; level <= last_non_empty_level;
       ++level) {
    matched_input_files[level].level = level;
    input_files->emplace_back(std::move(matched_input_files[level]));
  }

  return Status::OK();
}

// Returns true if any one of the parent files are being compacted
bool CompactionPicker::IsRangeInCompaction(VersionStorageInfo* vstorage,
                                           const InternalKey* smallest,
                                           const InternalKey* largest,
                                           int level, int* level_index) {
  std::vector<FileMetaData*> inputs;
  assert(level < NumberLevels());

  vstorage->GetOverlappingInputs(level, smallest, largest, &inputs,
                                 level_index ? *level_index : 0, level_index);
  return AreFilesInCompaction(inputs);
}

// Populates the set of inputs of all other levels that overlap with the
// start level.
// Now we assume all levels except start level and output level are empty.
// Will also attempt to expand "start level" if that doesn't expand
// "output level" or cause "level" to include a file for compaction that has an
// overlapping user-key with another file.
// REQUIRES: input_level and output_level are different
// REQUIRES: inputs->empty() == false
// Returns false if files on parent level are currently in compaction, which
// means that we can't compact them
bool CompactionPicker::SetupOtherInputs(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, CompactionInputFiles* inputs,
    CompactionInputFiles* output_level_inputs, int* parent_index,
    int base_index, bool only_expand_towards_right) {
  assert(!inputs->empty());
  assert(output_level_inputs->empty());
  const int input_level = inputs->level;
  const int output_level = output_level_inputs->level;
  if (input_level == output_level) {
    // no possibility of conflict
    return true;
  }

  // For now, we only support merging two levels, start level and output level.
  // We need to assert other levels are empty.
  for (int l = input_level + 1; l < output_level; l++) {
    assert(vstorage->NumLevelFiles(l) == 0);
  }

  InternalKey smallest, largest;

  // Get the range one last time.
  GetRange(*inputs, &smallest, &largest);

  // Populate the set of next-level files (inputs_GetOutputLevelInputs()) to
  // include in compaction
  vstorage->GetOverlappingInputs(output_level, &smallest, &largest,
                                 &output_level_inputs->files, *parent_index,
                                 parent_index);
  if (AreFilesInCompaction(output_level_inputs->files)) {
    return false;
  }
  if (!output_level_inputs->empty()) {
    if (!ExpandInputsToCleanCut(cf_name, vstorage, output_level_inputs)) {
      return false;
    }
  }

  // See if we can further grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up. We also choose NOT
  // to expand if this would cause "level" to include some entries for some
  // user key, while excluding other entries for the same user key. This
  // can happen when one user key spans multiple files.
  if (!output_level_inputs->empty()) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    const uint64_t output_level_inputs_size =
        TotalFileSize(output_level_inputs->files);
    const uint64_t inputs_size = TotalFileSize(inputs->files);
    bool expand_inputs = false;

    CompactionInputFiles expanded_inputs;
    expanded_inputs.level = input_level;
    // Get closed interval of output level
    InternalKey all_start, all_limit;
    GetRange(*inputs, *output_level_inputs, &all_start, &all_limit);
    bool try_overlapping_inputs = true;
    if (only_expand_towards_right) {
      // Round-robin compaction only allows expansion towards the larger side.
      vstorage->GetOverlappingInputs(input_level, &smallest, &all_limit,
                                     &expanded_inputs.files, base_index,
                                     nullptr);
    } else {
      vstorage->GetOverlappingInputs(input_level, &all_start, &all_limit,
                                     &expanded_inputs.files, base_index,
                                     nullptr);
    }
    uint64_t expanded_inputs_size = TotalFileSize(expanded_inputs.files);
    if (!ExpandInputsToCleanCut(cf_name, vstorage, &expanded_inputs)) {
      try_overlapping_inputs = false;
    }
    if (try_overlapping_inputs && expanded_inputs.size() > inputs->size() &&
        (mutable_cf_options.ignore_max_compaction_bytes_for_input ||
         output_level_inputs_size + expanded_inputs_size < limit) &&
        !AreFilesInCompaction(expanded_inputs.files)) {
      InternalKey new_start, new_limit;
      GetRange(expanded_inputs, &new_start, &new_limit);
      CompactionInputFiles expanded_output_level_inputs;
      expanded_output_level_inputs.level = output_level;
      vstorage->GetOverlappingInputs(output_level, &new_start, &new_limit,
                                     &expanded_output_level_inputs.files,
                                     *parent_index, parent_index);
      assert(!expanded_output_level_inputs.empty());
      if (!AreFilesInCompaction(expanded_output_level_inputs.files) &&
          ExpandInputsToCleanCut(cf_name, vstorage,
                                 &expanded_output_level_inputs) &&
          expanded_output_level_inputs.size() == output_level_inputs->size()) {
        expand_inputs = true;
      }
    }
    if (!expand_inputs) {
      vstorage->GetCleanInputsWithinInterval(input_level, &all_start,
                                             &all_limit, &expanded_inputs.files,
                                             base_index, nullptr);
      expanded_inputs_size = TotalFileSize(expanded_inputs.files);
      if (expanded_inputs.size() > inputs->size() &&
          (mutable_cf_options.ignore_max_compaction_bytes_for_input ||
           output_level_inputs_size + expanded_inputs_size < limit) &&
          !AreFilesInCompaction(expanded_inputs.files)) {
        expand_inputs = true;
      }
    }
    if (expand_inputs) {
      ROCKS_LOG_INFO(ioptions_.logger,
                     "[%s] Expanding@%d %" ROCKSDB_PRIszt "+%" ROCKSDB_PRIszt
                     "(%" PRIu64 "+%" PRIu64 " bytes) to %" ROCKSDB_PRIszt
                     "+%" ROCKSDB_PRIszt " (%" PRIu64 "+%" PRIu64 " bytes)\n",
                     cf_name.c_str(), input_level, inputs->size(),
                     output_level_inputs->size(), inputs_size,
                     output_level_inputs_size, expanded_inputs.size(),
                     output_level_inputs->size(), expanded_inputs_size,
                     output_level_inputs_size);
      inputs->files = expanded_inputs.files;
    }
  } else {
    // Likely to be trivial move. Expand files if they are still trivial moves,
    // but limit to mutable_cf_options.max_compaction_bytes or 8 files so that
    // we don't create too much compaction pressure for the next level.
  }
  return true;
}

void CompactionPicker::GetGrandparents(
    VersionStorageInfo* vstorage, const CompactionInputFiles& inputs,
    const CompactionInputFiles& output_level_inputs,
    std::vector<FileMetaData*>* grandparents) {
  InternalKey start, limit;
  GetRange(inputs, output_level_inputs, &start, &limit);
  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2 or the first
  // level after that has overlapping files)
  for (int level = output_level_inputs.level + 1; level < NumberLevels();
       level++) {
    vstorage->GetOverlappingInputs(level, &start, &limit, grandparents);
    if (!grandparents->empty()) {
      break;
    }
  }
}

// =============================================================================
// CompactionPicker::CompactRange - 手动压缩范围选择函数
// =============================================================================
//
// 功能概述：
// 为手动压缩（DB::CompactRange）选择需要压缩的文件，并创建 Compaction 对象。
// 支持三种压缩风格：Level、Universal、FIFO（FIFO 有自己的实现）
//
// 工作流程：
// 1. 根据压缩风格选择不同的策略
// 2. Universal All-Levels: 选择所有层的所有文件
// 3. Level/Universal: 选择指定范围内与 [begin, end] 重叠的文件
// 4. 检查冲突（是否正在压缩中）
// 5. 考虑输出层的文件
// 6. 创建 Compaction 对象
//
// 参数说明：
// - cf_name: 列族名称（用于日志记录）
// - mutable_cf_options: 可变的列族选项（如 max_compaction_bytes）
// - mutable_db_options: 可变的数据库选项
// - vstorage: 版本存储信息（包含各层文件信息）
// - input_level: 输入层（kCompactAllLevels 表示所有层）
// - output_level: 输出层（kCompactToBaseLevel 表示 base level）
// - compact_range_options: 压缩范围选项（如 bottommost_level_compaction）
// - begin: 压缩范围的起始键（InternalKey）
// - end: 压缩范围的结束键（InternalKey）
// - compaction_end: 输出参数，返回实际压缩的结束键（用于继续压缩）
// - manual_conflict: 输出参数，返回是否存在冲突（如其他压缩正在运行）
// - max_file_num_to_ignore: 忽略文件编号 >= 此值的文件（用于避免重复压缩）
// - trim_ts: 时间戳裁剪（用于保留特定时间范围的数据）
//
// 返回值：
// - 成功：返回 Compaction 对象（包含输入文件和压缩配置）
// - 失败：返回 nullptr（设置 manual_conflict 或范围为空）
//
// 使用场景：
// - DBImpl::RunManualCompaction 调用此函数选择需要压缩的文件
// - 每次调用返回一个 Compaction 对象，包含一层或所有层的文件
// - 如果范围太大，会被分成多次调用（每次返回一个 Compaction）
//
// 注意事项：
// - 对于 Universal 风格，如果 input_level == kCompactAllLevels，会压缩所有层
// - 对于 Level 风格，从 input_level 压缩到 output_level
// - 会考虑 max_compaction_bytes 限制，避免一次压缩太多数据
// - 会检查文件冲突，避免多个压缩同时处理相同的文件
// =============================================================================
Compaction* CompactionPicker::CompactRange(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    int input_level, int output_level,
    const CompactRangeOptions& compact_range_options, const InternalKey* begin,
    const InternalKey* end, InternalKey** compaction_end, bool* manual_conflict,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts) {
  // 断言：FIFO 压缩风格有自己的实现，不应该调用此函数
  assert(ioptions_.compaction_style != kCompactionStyleFIFO);

  // ========================================================================
  // Universal All-Levels Compaction（压缩所有层）
  // ========================================================================
  // 当 input_level == kCompactAllLevels 时，会压缩所有层的所有文件到最后一个 level
  // 这是 Universal 压缩风格的特殊行为，只用于 multi-level universal compaction
  if (input_level == ColumnFamilyData::kCompactAllLevels) {
    // 断言：必须是 Universal 压缩风格（只有 Universal 支持 kCompactAllLevels）
    assert(ioptions_.compaction_style == kCompactionStyleUniversal);

    // Universal compaction with more than one level always compacts all the
    // files together to the last level.
    // Universal 压缩风格在多层级时，总是将所有层压缩到最后一个 level
    assert(vstorage->num_levels() > 1);
    int max_output_level =
        vstorage->MaxOutputLevel(ioptions_.allow_ingest_behind);
    // DBImpl::CompactRange() set output level to be the last level
    // 输出层必须设置为最后一个 level
    assert(output_level == max_output_level);
    // DBImpl::RunManualCompaction will make full range for universal compaction
    // Universal 压缩必须压缩全范围（begin 和 end 必须为 nullptr）
    assert(begin == nullptr);
    assert(end == nullptr);
    *compaction_end = nullptr;

    // 找到第一个非空的层作为起始层
    int start_level = 0;
    for (; start_level <= max_output_level &&
           vstorage->NumLevelFiles(start_level) == 0;
         start_level++) {
    }
    // 如果所有层都为空，返回 nullptr
    if (start_level > max_output_level) {
      return nullptr;
    }

    // 检查 Level 0 是否有正在进行的压缩
    // Level 0 文件之间可能重叠，只允许一个压缩任务
    if ((start_level == 0) && (!level0_compactions_in_progress_.empty())) {
      *manual_conflict = true;
      // Only one level 0 compaction allowed
      return nullptr;
    }

    // 创建输入文件集合，包含从 start_level 到 max_output_level 的所有文件
    std::vector<CompactionInputFiles> inputs(max_output_level + 1 -
                                             start_level);
    for (int level = start_level; level <= max_output_level; level++) {
      inputs[level - start_level].level = level;
      auto& files = inputs[level - start_level].files;
      // 添加该层的所有文件
      for (FileMetaData* f : vstorage->LevelFiles(level)) {
        files.push_back(f);
      }
      // 检查该层的文件是否已经在压缩中
      if (AreFilesInCompaction(files)) {
        *manual_conflict = true;
        return nullptr;
      }
    }

    // 2 non-exclusive manual compactions could run at the same time producing
    // overlaping outputs in the same level.
    // 检查输出范围是否会与正在运行的压缩产生重叠
    if (FilesRangeOverlapWithCompaction(
            inputs, output_level,
            Compaction::EvaluatePenultimateLevel(vstorage, ioptions_,
                                                 start_level, output_level))) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      *manual_conflict = true;
      return nullptr;
    }

    // 创建 Compaction 对象
    Compaction* c = new Compaction(
        vstorage, ioptions_, mutable_cf_options, mutable_db_options,
        std::move(inputs), output_level,
        MaxFileSizeForLevel(mutable_cf_options, output_level,
                            ioptions_.compaction_style),
        /* max_compaction_bytes */ LLONG_MAX,  // Universal 压缩不限制大小
        compact_range_options.target_path_id,
        GetCompressionType(vstorage, mutable_cf_options, output_level, 1),
        GetCompressionOptions(mutable_cf_options, vstorage, output_level),
        Temperature::kUnknown, compact_range_options.max_subcompactions,
        /* grandparents */ {},  // Universal 压缩不需要 grandparents
        /* is manual */ true, trim_ts, /* score */ -1,
        /* deletion_compaction */ false, /* l0_files_might_overlap */ true,
        CompactionReason::kUnknown,
        compact_range_options.blob_garbage_collection_policy,
        compact_range_options.blob_garbage_collection_age_cutoff);

    // 注册压缩任务（跟踪正在进行的压缩）
    RegisterCompaction(c);
    // 重新计算压缩分数（因为文件正在被压缩，需要更新分数）
    vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);
    return c;
  }

  // ========================================================================
  // Level / Universal Single-Level Compaction（单层压缩）
  // ========================================================================
  // 对于 Level 或 Universal 压缩风格，压缩指定 input_level 的文件
  // 输入文件是与 [begin, end] 范围重叠的文件

  // 创建输入文件集合
  CompactionInputFiles inputs;
  inputs.level = input_level;
  bool covering_the_whole_range = true;  // 标记是否覆盖了整个请求范围

  // All files are 'overlapping' in universal style compaction.
  // We have to compact the entire range in one shot.
  // Universal 压缩风格中，所有文件都相互重叠，必须一次性压缩整个范围
  // 忽略 begin 和 end 参数
  if (ioptions_.compaction_style == kCompactionStyleUniversal) {
    begin = nullptr;
    end = nullptr;
  }

  // 获取与 [begin, end] 范围重叠的文件（对于 Universal，begin 和 end 为 nullptr，返回所有文件）
  // GetOverlappingInputs 会自动扩展范围，确保文件之间没有间隙
  vstorage->GetOverlappingInputs(input_level, begin, end, &inputs.files);
  // 如果没有文件需要压缩，返回 nullptr
  if (inputs.empty()) {
    return nullptr;
  }

  // 检查 Level 0 是否有正在进行的压缩
  // Level 0 文件之间可能重叠，只允许一个压缩任务
  if ((input_level == 0) && (!level0_compactions_in_progress_.empty())) {
    // Only one level 0 compaction allowed
    TEST_SYNC_POINT("CompactionPicker::CompactRange:Conflict");
    *manual_conflict = true;
    return nullptr;
  }

  // ========================================================================
  // 避免一次压缩太多数据（max_compaction_bytes 限制）
  // ========================================================================
  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  // 对于 Level 0，文件之间可能重叠，必须选择所有重叠的文件，不能截断
  // 对于 Level > 0，如果总大小超过 max_compaction_bytes，会截断输入文件
  if (input_level > 0) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    uint64_t input_level_total = 0;  // 输入层总大小
    int hint_index = -1;
    InternalKey* smallest = nullptr;
    InternalKey* largest = nullptr;

    // 遍历输入文件，逐个累加大小，直到达到 limit
    // 注意：使用 i + 1 < inputs.size() 是因为至少要保留最后一个文件（避免空压缩）
    for (size_t i = 0; i + 1 < inputs.size(); ++i) {
      if (!smallest) {
        smallest = &inputs[i]->smallest;
      }
      largest = &inputs[i]->largest;

      uint64_t input_file_size = inputs[i]->fd.GetFileSize();
      uint64_t output_level_total = 0;

      // 计算输出层中与当前输入范围重叠的文件总大小
      if (output_level < vstorage->num_non_empty_levels()) {
        std::vector<FileMetaData*> files;
        vstorage->GetOverlappingInputsRangeBinarySearch(
            output_level, smallest, largest, &files, hint_index, &hint_index);
        for (const auto& file : files) {
          output_level_total += file->fd.GetFileSize();
        }
      }

      input_level_total += input_file_size;

      // 如果输入层和输出层的总大小超过限制，截断输入文件
      if (input_level_total + output_level_total >= limit) {
        covering_the_whole_range = false;
        // still include the current file, so the compaction could be larger
        // than max_compaction_bytes, which is also to make sure the compaction
        // can make progress even `max_compaction_bytes` is small (e.g. smaller
        // than an SST file).
        // 保留当前文件，确保压缩能继续进行（即使 max_compaction_bytes 很小）
        inputs.files.resize(i + 1);
        break;
      }
    }
  }

  // 断言：目标路径 ID 必须有效
  assert(compact_range_options.target_path_id <
         static_cast<uint32_t>(ioptions_.cf_paths.size()));

  // ========================================================================
  // Bottommost Level Compaction: 过滤压缩期间创建的文件
  // ========================================================================
  // for BOTTOM LEVEL compaction only, use max_file_num_to_ignore to filter out
  // files that are created during the current compaction.
  // 对于最底层压缩（input_level == output_level），使用 max_file_num_to_ignore
  // 过滤掉在当前手动压缩过程中创建的文件，避免重复压缩
  if ((compact_range_options.bottommost_level_compaction ==
           BottommostLevelCompaction::kForceOptimized ||
       compact_range_options.bottommost_level_compaction ==
           BottommostLevelCompaction::kIfHaveCompactionFilter) &&
      max_file_num_to_ignore != std::numeric_limits<uint64_t>::max()) {
    // 断言：最底层压缩的输入层和输出层必须相同
    assert(input_level == output_level);
    // inputs_shrunk holds a continuous subset of input files which were all
    // created before the current manual compaction
    // inputs_shrunk 包含在当前手动压缩之前创建的连续文件子集
    std::vector<FileMetaData*> inputs_shrunk;
    size_t skip_input_index = inputs.size();

    for (size_t i = 0; i < inputs.size(); ++i) {
      // 只保留文件编号 < max_file_num_to_ignore 的文件
      // 这些文件是在当前手动压缩之前创建的
      if (inputs[i]->fd.GetNumber() < max_file_num_to_ignore) {
        inputs_shrunk.push_back(inputs[i]);
      } else if (!inputs_shrunk.empty()) {
        // inputs[i] was created during the current manual compaction and
        // need to be skipped
        // inputs[i] 是在当前手动压缩期间创建的，需要跳过
        skip_input_index = i;
        break;
      }
    }
    // 如果所有文件都是在压缩期间创建的，返回 nullptr
    if (inputs_shrunk.empty()) {
      return nullptr;
    }
    // 如果有文件被过滤掉，替换 inputs.files
    if (inputs.size() != inputs_shrunk.size()) {
      inputs.files.swap(inputs_shrunk);
    }
    // set covering_the_whole_range to false if there is any file that need to
    // be compacted in the range of inputs[skip_input_index+1, inputs.size())
    // 如果在跳过的文件之后还有需要压缩的文件（文件编号 < max_file_num_to_ignore），
    // 设置 covering_the_whole_range = false，表示还有文件需要在后续轮次中压缩
    for (size_t i = skip_input_index + 1; i < inputs.size(); ++i) {
      if (inputs[i]->fd.GetNumber() < max_file_num_to_ignore) {
        covering_the_whole_range = false;
      }
    }
  }

  // ========================================================================
  // 扩展输入文件范围以确保干净的边界
  // ========================================================================
  InternalKey key_storage;
  InternalKey* next_smallest = &key_storage;

  // ExpandInputsToCleanCut 会扩展 inputs.files，确保文件之间没有间隙
  // 这是为了保证压缩输出的键范围是连续的，避免数据丢失
  // 如果扩展失败（因为与其他压缩冲突），返回 manual_conflict = true
  if (ExpandInputsToCleanCut(cf_name, vstorage, &inputs, &next_smallest) ==
      false) {
    // manual compaction is now multi-threaded, so it can
    // happen that ExpandWhileOverlapping fails
    // we handle it higher in RunManualCompaction
    // 手动压缩现在是多线程的，可能会因为与其他压缩冲突而失败
    // 会在 RunManualCompaction 中处理这种情况（等待后重试）
    *manual_conflict = true;
    return nullptr;
  }

  // ========================================================================
  // 设置压缩结束键（用于后续轮次）
  // ========================================================================
  // 如果 covering_the_whole_range = true，表示已经覆盖了整个请求范围
  // 如果 next_smallest = nullptr，表示没有下一个文件
  // 这两种情况下，设置 *compaction_end = nullptr，表示压缩已经完成
  // 否则，设置 *compaction_end = *next_smallest，表示下一轮从这个键开始
  if (covering_the_whole_range || !next_smallest) {
    *compaction_end = nullptr;
  } else {
    **compaction_end = *next_smallest;
  }

  // ========================================================================
  // 处理输出层（Output Level）
  // ========================================================================
  CompactionInputFiles output_level_inputs;

  // 如果 output_level == kCompactToBaseLevel，表示需要压缩到 base level
  // 这种情况下，从 Level 0 压缩到 base level（通常 > 0）
  if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
    assert(input_level == 0);
    output_level = vstorage->base_level();
    assert(output_level > 0);
  }
  output_level_inputs.level = output_level;

  // ========================================================================
  // 设置输出层的输入文件（SetupOtherInputs）
  // ========================================================================
  // 如果输入层和输出层不同，需要选择输出层中与输入文件范围重叠的文件
  // 这些文件会被一起压缩，输出到 output_level（或 output_level + 1）
  if (input_level != output_level) {
    int parent_index = -1;
    // SetupOtherInputs 会选择输出层中与输入文件范围重叠的文件
    // 如果失败（因为与其他压缩冲突），返回 manual_conflict = true
    if (!SetupOtherInputs(cf_name, mutable_cf_options, vstorage, &inputs,
                          &output_level_inputs, &parent_index, -1)) {
      // manual compaction is now multi-threaded, so it can
      // happen that SetupOtherInputs fails
      // we handle it higher in RunManualCompaction
      *manual_conflict = true;
      return nullptr;
    }
  }

  // ========================================================================
  // 检查文件冲突（Files In Compaction）
  // ========================================================================
  // 将输入层和输出层的文件合并成一个列表
  std::vector<CompactionInputFiles> compaction_inputs({inputs});
  if (!output_level_inputs.empty()) {
    compaction_inputs.push_back(output_level_inputs);
  }

  // 检查所有输入文件是否已经在压缩中
  // 如果任何一个文件正在被其他压缩使用，返回 manual_conflict = true
  for (size_t i = 0; i < compaction_inputs.size(); i++) {
    if (AreFilesInCompaction(compaction_inputs[i].files)) {
      *manual_conflict = true;
      return nullptr;
    }
  }

  // ========================================================================
  // 检查输出范围冲突（Output Range Overlap）
  // ========================================================================
  // 2 non-exclusive manual compactions could run at the same time producing
  // overlaping outputs in the same level.
  // 检查当前压缩的输出范围是否会与正在运行的压缩的输出范围重叠
  // 这是为了避免多个压缩同时向同一个 level 写入重叠的范围，导致数据不一致
  if (FilesRangeOverlapWithCompaction(
          compaction_inputs, output_level,
          Compaction::EvaluatePenultimateLevel(vstorage, ioptions_, input_level,
                                               output_level))) {
    // This compaction output could potentially conflict with the output
    // of a currently running compaction, we cannot run it.
    *manual_conflict = true;
    return nullptr;
  }

  // ========================================================================
  // 获取 Grandparent 文件（用于输出大小限制）
  // ========================================================================
  // Grandparent 文件是输出层的上一层文件，用于限制输出文件的大小
  // 避免输出文件与 grandparent 文件重叠过多，导致后续压缩成本增加
  std::vector<FileMetaData*> grandparents;
  GetGrandparents(vstorage, inputs, output_level_inputs, &grandparents);

  // ========================================================================
  // 创建 Compaction 对象
  // ========================================================================
  Compaction* compaction = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(compaction_inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          ioptions_.compaction_style, vstorage->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options.max_compaction_bytes,
      compact_range_options.target_path_id,
      GetCompressionType(vstorage, mutable_cf_options, output_level,
                         vstorage->base_level()),
      GetCompressionOptions(mutable_cf_options, vstorage, output_level),
      Temperature::kUnknown, compact_range_options.max_subcompactions,
      std::move(grandparents), /* is manual */ true, trim_ts, /* score */ -1,
      /* deletion_compaction */ false, /* l0_files_might_overlap */ true,
      CompactionReason::kUnknown,
      compact_range_options.blob_garbage_collection_policy,
      compact_range_options.blob_garbage_collection_age_cutoff);

  // 测试同步点，用于单元测试
  TEST_SYNC_POINT_CALLBACK("CompactionPicker::CompactRange:Return", compaction);

  // ========================================================================
  // 注册压缩任务并重新计算压缩分数
  // ========================================================================
  // 注册压缩任务（跟踪正在进行的压缩）
  RegisterCompaction(compaction);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  // 创建压缩会影响压缩分数，因为压缩分数会考虑正在运行的压缩
  // （通过跳过已经在压缩中的文件）。由于刚刚改变了压缩分数，需要重新计算
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);

  return compaction;
}

namespace {
// Test whether two files have overlapping key-ranges.
bool HaveOverlappingKeyRanges(const Comparator* c, const SstFileMetaData& a,
                              const SstFileMetaData& b) {
  if (c->CompareWithoutTimestamp(a.smallestkey, b.smallestkey) >= 0) {
    if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->CompareWithoutTimestamp(a.largestkey, b.largestkey) <= 0) {
    if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}
}  // namespace

Status CompactionPicker::SanitizeCompactionInputFilesForAllLevels(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  auto& levels = cf_meta.levels;
  auto comparator = icmp_->user_comparator();

  // TODO(yhchiang): add is_adjustable to CompactionOptions

  // the smallest and largest key of the current compaction input
  std::string smallestkey;
  std::string largestkey;
  // a flag for initializing smallest and largest key
  bool is_first = false;
  const int kNotFound = -1;

  // For each level, it does the following things:
  // 1. Find the first and the last compaction input files
  //    in the current level.
  // 2. Include all files between the first and the last
  //    compaction input files.
  // 3. Update the compaction key-range.
  // 4. For all remaining levels, include files that have
  //    overlapping key-range with the compaction key-range.
  for (int l = 0; l <= output_level; ++l) {
    auto& current_files = levels[l].files;
    int first_included = static_cast<int>(current_files.size());
    int last_included = kNotFound;

    // identify the first and the last compaction input files
    // in the current level.
    for (size_t f = 0; f < current_files.size(); ++f) {
      const uint64_t file_number = TableFileNameToNumber(current_files[f].name);
      if (input_files->find(file_number) == input_files->end()) {
        continue;
      }
      first_included = std::min(first_included, static_cast<int>(f));
      last_included = std::max(last_included, static_cast<int>(f));
      if (is_first == false) {
        smallestkey = current_files[f].smallestkey;
        largestkey = current_files[f].largestkey;
        is_first = true;
      }
    }
    if (last_included == kNotFound) {
      continue;
    }

    if (l != 0) {
      // expand the compaction input of the current level if it
      // has overlapping key-range with other non-compaction input
      // files in the same level.
      while (first_included > 0) {
        if (comparator->CompareWithoutTimestamp(
                current_files[first_included - 1].largestkey,
                current_files[first_included].smallestkey) < 0) {
          break;
        }
        first_included--;
      }

      while (last_included < static_cast<int>(current_files.size()) - 1) {
        if (comparator->CompareWithoutTimestamp(
                current_files[last_included + 1].smallestkey,
                current_files[last_included].largestkey) > 0) {
          break;
        }
        last_included++;
      }
    } else if (output_level > 0) {
      last_included = static_cast<int>(current_files.size() - 1);
    }

    // include all files between the first and the last compaction input files.
    for (int f = first_included; f <= last_included; ++f) {
      if (current_files[f].being_compacted) {
        return Status::Aborted("Necessary compaction input file " +
                               current_files[f].name +
                               " is currently being compacted.");
      }

      input_files->insert(TableFileNameToNumber(current_files[f].name));
    }

    // update smallest and largest key
    if (l == 0) {
      for (int f = first_included; f <= last_included; ++f) {
        if (comparator->CompareWithoutTimestamp(
                smallestkey, current_files[f].smallestkey) > 0) {
          smallestkey = current_files[f].smallestkey;
        }
        if (comparator->CompareWithoutTimestamp(
                largestkey, current_files[f].largestkey) < 0) {
          largestkey = current_files[f].largestkey;
        }
      }
    } else {
      if (comparator->CompareWithoutTimestamp(
              smallestkey, current_files[first_included].smallestkey) > 0) {
        smallestkey = current_files[first_included].smallestkey;
      }
      if (comparator->CompareWithoutTimestamp(
              largestkey, current_files[last_included].largestkey) < 0) {
        largestkey = current_files[last_included].largestkey;
      }
    }

    SstFileMetaData aggregated_file_meta;
    aggregated_file_meta.smallestkey = smallestkey;
    aggregated_file_meta.largestkey = largestkey;

    // For all lower levels, include all overlapping files.
    // We need to add overlapping files from the current level too because even
    // if there no input_files in level l, we would still need to add files
    // which overlap with the range containing the input_files in levels 0 to l
    // Level 0 doesn't need to be handled this way because files are sorted by
    // time and not by key
    for (int m = std::max(l, 1); m <= output_level; ++m) {
      for (auto& next_lv_file : levels[m].files) {
        if (HaveOverlappingKeyRanges(comparator, aggregated_file_meta,
                                     next_lv_file)) {
          if (next_lv_file.being_compacted) {
            return Status::Aborted(
                "File " + next_lv_file.name +
                " that has overlapping key range with one of the compaction "
                " input file is currently being compacted.");
          }
          input_files->insert(TableFileNameToNumber(next_lv_file.name));
        }
      }
    }
  }
  if (RangeOverlapWithCompaction(smallestkey, largestkey, output_level)) {
    return Status::Aborted(
        "A running compaction is writing to the same output level in an "
        "overlapping key range");
  }
  return Status::OK();
}

Status CompactionPicker::SanitizeCompactionInputFiles(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  assert(static_cast<int>(cf_meta.levels.size()) - 1 ==
         cf_meta.levels[cf_meta.levels.size() - 1].level);
  if (output_level >= static_cast<int>(cf_meta.levels.size())) {
    return Status::InvalidArgument(
        "Output level for column family " + cf_meta.name +
        " must between [0, " +
        std::to_string(cf_meta.levels[cf_meta.levels.size() - 1].level) + "].");
  }

  if (output_level > MaxOutputLevel()) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm --- " +
        std::to_string(MaxOutputLevel()));
  }

  if (output_level < 0) {
    return Status::InvalidArgument("Output level cannot be negative.");
  }

  if (input_files->size() == 0) {
    return Status::InvalidArgument(
        "A compaction must contain at least one file.");
  }

  Status s = SanitizeCompactionInputFilesForAllLevels(input_files, cf_meta,
                                                      output_level);

  if (!s.ok()) {
    return s;
  }

  // for all input files, check whether the file number matches
  // any currently-existing files.
  for (auto file_num : *input_files) {
    bool found = false;
    int input_file_level = -1;
    for (const auto& level_meta : cf_meta.levels) {
      for (const auto& file_meta : level_meta.files) {
        if (file_num == TableFileNameToNumber(file_meta.name)) {
          if (file_meta.being_compacted) {
            return Status::Aborted("Specified compaction input file " +
                                   MakeTableFileName("", file_num) +
                                   " is already being compacted.");
          }
          found = true;
          input_file_level = level_meta.level;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (!found) {
      return Status::InvalidArgument(
          "Specified compaction input file " + MakeTableFileName("", file_num) +
          " does not exist in column family " + cf_meta.name + ".");
    }
    if (input_file_level > output_level) {
      return Status::InvalidArgument(
          "Cannot compact file to up level, input file: " +
          MakeTableFileName("", file_num) + " level " +
          std::to_string(input_file_level) + " > output level " +
          std::to_string(output_level));
    }
  }

  return Status::OK();
}

void CompactionPicker::RegisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
  assert(ioptions_.compaction_style != kCompactionStyleLevel ||
         c->output_level() == 0 ||
         !FilesRangeOverlapWithCompaction(*c->inputs(), c->output_level(),
                                          c->GetPenultimateLevel()));
  // CompactionReason::kExternalSstIngestion's start level is just a placeholder
  // number without actual meaning as file ingestion technically does not have
  // an input level like other compactions
  if ((c->start_level() == 0 &&
       c->compaction_reason() != CompactionReason::kExternalSstIngestion) ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.insert(c);
  }
  compactions_in_progress_.insert(c);
  TEST_SYNC_POINT_CALLBACK("CompactionPicker::RegisterCompaction:Registered",
                           c);
}

void CompactionPicker::UnregisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
  if (c->start_level() == 0 ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.erase(c);
  }
  compactions_in_progress_.erase(c);
}

/**
 * @brief 从被标记为需要 compaction 的文件中选择一个进行压缩
 *
 * 本函数用于处理用户或系统明确标记为需要 compaction 的文件。
 * 这些文件被标记在 files_marked_for_compaction_ 列表中，优先级高于自动选择的文件。
 *
 * 文件被标记的原因：
 * 1. 用户手动请求 compact（CompactFiles API）
 * 2. Table Properties Collector 建议（通过设置标记位）
 * 3. TTL 过期文件
 * 4. 周期性 compaction（periodic_compaction_seconds）
 * 5. Blob 垃圾回收标记
 *
 * 选择策略：
 * 1. 首先随机选择一个标记文件尝试（增加公平性）
 * 2. 如果随机文件无法 compaction，则按顺序遍历所有标记文件
 * 3. 对每个文件调用 continuation lambda，检查是否可以 compaction
 * 4. 找到第一个可 compaction 的文件后立即返回
 *
 * Continuation 的检查逻辑：
 * 1. 确保文件没有被正在压缩（being_compacted = false）
 * 2. 确定 start_level 和 output_level
 * 3. L0 特殊检查：确保没有其他 L0 compaction 在进行
 * 4. 调用 ExpandInputsToCleanCut 扩展输入文件范围
 *
 * 为什么随机选择？
 * - 避免总是选择同一个文件（公平性）
 * - 当多个文件都被标记时，均匀分布选择
 * - 减少对特定文件的偏向
 *
 * 为什么先随机再顺序？
 * - 随机选择快速尝试（可能命中）
 * - 失败后顺序遍历确保不遗漏
 * - 两阶段策略：快速尝试 + 完整遍历
 *
 * 输出参数：
 * - start_level：compaction 的输入层级
 * - output_level：compaction 的输出层级
 * - start_level_inputs：选中的输入文件（可能被扩展）
 *
 * @param cf_name 列族名称（用于日志记录）
 * @param vstorage 版本存储信息（包含文件列表和标记文件）
 * @param start_level 输出参数：compaction 的输入层级
 * @param output_level 输出参数：compaction 的输出层级
 * @param start_level_inputs 输出参数：选中的输入文件列表
 *
 * @note files_marked_for_compaction_ 优先级高于自动选择：
 *   - 在 SetupInitialFiles 之前调用
 *   - 优先处理标记文件
 *   - 自动选择在标记文件为空时才进行
 *
 * @note L0 的特殊限制：
 *   - 同一时间只能有一个 L0 compaction
 *   - L0 文件之间可能有重叠，需要互斥
 *   - 如果有 L0 compaction 在进行，跳过 L0 的标记文件
 *
 * @note ExpandInputsToCleanCut 的作用：
 *   - 将单个文件扩展到 clean cut 范围
 *   - 确保输入文件范围不会分割 user-key
 *   - 避免合并时的复杂性
 *
 * @see ComputeFilesMarkedForCompaction 计算标记文件列表
 * @see ExpandInputsToCleanCut 扩展输入文件范围
 * @see SetupInitialFiles 自动选择文件的逻辑
 */
void CompactionPicker::PickFilesMarkedForCompaction(
    const std::string& cf_name, VersionStorageInfo* vstorage, int* start_level,
    int* output_level, CompactionInputFiles* start_level_inputs) {
  // 如果没有标记为需要 compaction 的文件，直接返回
  // files_marked_for_compaction_ 为空，无需处理
  if (vstorage->FilesMarkedForCompaction().empty()) {
    return;
  }

  // 定义 continuation lambda：尝试将文件扩展为有效的 compaction 输入
  // [&, cf_name]：捕获 cf_name 和所有外部变量（按引用）
  auto continuation = [&, cf_name](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // 如果文件正在被压缩，这里不需要处理。
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    // 如果这个 assert() 失败，意味着某个函数将某些文件标记为 being_compacted，
    // 但没有调用 ComputeCompactionScore()（这会导致状态不一致）。
    assert(!level_file.second->being_compacted);

    // 设置 start_level 为文件所在的层级
    *start_level = level_file.first;

    // 设置 output_level：
    // - 如果 start_level 是 0，输出到 base_level（通常是 L1）
    // - 否则，输出到下一层（start_level + 1）
    *output_level =
        (*start_level == 0) ? vstorage->base_level() : *start_level + 1;

    // L0 特殊检查：确保没有其他 L0 compaction 在进行
    // L0 compaction 是独占的（L0 文件之间可能重叠）
    if (*start_level == 0 && !level0_compactions_in_progress()->empty()) {
      return false;  // 有 L0 compaction 在进行，无法选择此文件
    }

    // 设置输入文件列表为当前文件（单个文件）
    start_level_inputs->files = {level_file.second};
    start_level_inputs->level = *start_level;

    // 尝试将输入文件扩展为 clean cut 范围
    // Clean Cut：确保输入文件范围不会分割任何 user-key
    // 返回 true 表示扩展成功，可以 compaction
    // 返回 false 表示扩展失败，需要尝试下一个文件
    return ExpandInputsToCleanCut(cf_name, vstorage, start_level_inputs);
  };

  // take a chance on a random file first
  // 首先随机选择一个文件尝试（增加公平性，避免总是选择同一个文件）
  // Random64 使用 vstorage 指针作为种子（每次可能不同）
  Random64 rnd(/* seed */ reinterpret_cast<uint64_t>(vstorage));
  // 生成随机索引：[0, files_marked_for_compaction_.size())
  size_t random_file_index = static_cast<size_t>(rnd.Uniform(
      static_cast<uint64_t>(vstorage->FilesMarkedForCompaction().size())));

  // 测试同步点：用于单元测试模拟不同随机选择
  TEST_SYNC_POINT_CALLBACK("CompactionPicker::PickFilesMarkedForCompaction",
                           &random_file_index);

  // 尝试随机选择的文件
  // 如果成功找到可 compaction 的文件，直接返回
  if (continuation(vstorage->FilesMarkedForCompaction()[random_file_index])) {
    // found the compaction!
    return;
  }

  // 随机文件无法 compaction，按顺序遍历所有标记文件
  // 确保不遗漏任何一个可 compaction 的文件
  for (auto& level_file : vstorage->FilesMarkedForCompaction()) {
    // 对每个文件调用 continuation，检查是否可以 compaction
    // 如果返回 true，说明找到了有效的 compaction，立即返回
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  // 所有标记文件都无法 compaction，清空输入文件列表
  // 这可能是由于文件重叠、正在压缩、L0 互斥等原因
  start_level_inputs->files.clear();
}

bool CompactionPicker::GetOverlappingL0Files(
    VersionStorageInfo* vstorage, CompactionInputFiles* start_level_inputs,
    int output_level, int* parent_index) {
  // Two level 0 compaction won't run at the same time, so don't need to worry
  // about files on level 0 being compacted.
  assert(level0_compactions_in_progress()->empty());
  InternalKey smallest, largest;
  GetRange(*start_level_inputs, &smallest, &largest);
  // Note that the next call will discard the file we placed in
  // c->inputs_[0] earlier and replace it with an overlapping set
  // which will include the picked file.
  start_level_inputs->files.clear();
  vstorage->GetOverlappingInputs(0, &smallest, &largest,
                                 &(start_level_inputs->files));

  // If we include more L0 files in the same compaction run it can
  // cause the 'smallest' and 'largest' key to get extended to a
  // larger range. So, re-invoke GetRange to get the new key range
  GetRange(*start_level_inputs, &smallest, &largest);
  if (IsRangeInCompaction(vstorage, &smallest, &largest, output_level,
                          parent_index)) {
    return false;
  }
  assert(!start_level_inputs->files.empty());

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
