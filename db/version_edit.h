//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_garbage.h"
#include "db/dbformat.h"
#include "db/wal_edit.h"
#include "memory/arena.h"
#include "port/malloc.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/advanced_options.h"
#include "table/table_reader.h"
#include "table/unique_id_impl.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed. The number should be forward compatible so
// users can down-grade RocksDB safely. A future Tag is ignored by doing '&'
// between Tag and kTagSafeIgnoreMask field.
enum Tag : uint32_t {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactCursor = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,
  kMinLogNumberToKeep = 10,

  // these are new formats divergent from open source leveldb
  kNewFile2 = 100,
  kNewFile3 = 102,
  kNewFile4 = 103,      // 4th (the latest) format version of adding files
  kColumnFamily = 200,  // specify column family for version edit
  kColumnFamilyAdd = 201,
  kColumnFamilyDrop = 202,
  kMaxColumnFamily = 203,

  kInAtomicGroup = 300,

  kBlobFileAddition = 400,
  kBlobFileGarbage,

  // Mask for an unidentified tag from the future which can be safely ignored.
  kTagSafeIgnoreMask = 1 << 13,

  // Forward compatible (aka ignorable) records
  kDbId,
  kBlobFileAddition_DEPRECATED,
  kBlobFileGarbage_DEPRECATED,
  kWalAddition,
  kWalDeletion,
  kFullHistoryTsLow,
  kWalAddition2,
  kWalDeletion2,
};

enum NewFileCustomTag : uint32_t {
  kTerminate = 1,  // The end of customized fields
  kNeedCompaction = 2,
  // Since Manifest is not entirely forward-compatible, we currently encode
  // kMinLogNumberToKeep as part of NewFile as a hack. This should be removed
  // when manifest becomes forward-compatible.
  kMinLogNumberToKeepHack = 3,
  kOldestBlobFileNumber = 4,
  kOldestAncesterTime = 5,
  kFileCreationTime = 6,
  kFileChecksum = 7,
  kFileChecksumFuncName = 8,
  kTemperature = 9,
  kMinTimestamp = 10,
  kMaxTimestamp = 11,
  kUniqueId = 12,
  kEpochNumber = 13,
  kCompensatedRangeDeletionSize = 14,
  kTailSize = 15,
  kUserDefinedTimestampsPersisted = 16,

  // If this bit for the custom tag is set, opening DB should fail if
  // we don't know this field.
  kCustomTagNonSafeIgnoreMask = 1 << 6,

  // Forward incompatible (aka unignorable) fields
  kPathId,
};

class VersionSet;

constexpr uint64_t kFileNumberMask = 0x3FFFFFFFFFFFFFFF;
constexpr uint64_t kUnknownOldestAncesterTime = 0;
constexpr uint64_t kUnknownFileCreationTime = 0;
constexpr uint64_t kUnknownEpochNumber = 0;
// If `Options::allow_ingest_behind` is true, this epoch number
// will be dedicated to files ingested behind.
constexpr uint64_t kReservedEpochNumberForFileIngestedBehind = 1;

extern uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id);

// A copyable structure contains information needed to read data from an SST
// file. It can contain a pointer to a table reader opened for the file, or
// file number and size, which can be used to create a new table reader for it.
// The behavior is undefined when a copied of the structure is used when the
// file is not in any live version any more.
struct FileDescriptor {
  // Table reader in table_reader_handle
  TableReader* table_reader;
  uint64_t packed_number_and_path_id;
  uint64_t file_size;             // File size in bytes
  SequenceNumber smallest_seqno;  // The smallest seqno in this file
  SequenceNumber largest_seqno;   // The largest seqno in this file

  FileDescriptor() : FileDescriptor(0, 0, 0) {}

  FileDescriptor(uint64_t number, uint32_t path_id, uint64_t _file_size)
      : FileDescriptor(number, path_id, _file_size, kMaxSequenceNumber, 0) {}

  FileDescriptor(uint64_t number, uint32_t path_id, uint64_t _file_size,
                 SequenceNumber _smallest_seqno, SequenceNumber _largest_seqno)
      : table_reader(nullptr),
        packed_number_and_path_id(PackFileNumberAndPathId(number, path_id)),
        file_size(_file_size),
        smallest_seqno(_smallest_seqno),
        largest_seqno(_largest_seqno) {}

  FileDescriptor(const FileDescriptor& fd) { *this = fd; }

  FileDescriptor& operator=(const FileDescriptor& fd) {
    table_reader = fd.table_reader;
    packed_number_and_path_id = fd.packed_number_and_path_id;
    file_size = fd.file_size;
    smallest_seqno = fd.smallest_seqno;
    largest_seqno = fd.largest_seqno;
    return *this;
  }

  uint64_t GetNumber() const {
    return packed_number_and_path_id & kFileNumberMask;
  }
  uint32_t GetPathId() const {
    return static_cast<uint32_t>(packed_number_and_path_id /
                                 (kFileNumberMask + 1));
  }
  uint64_t GetFileSize() const { return file_size; }
};

struct FileSampledStats {
  FileSampledStats() : num_reads_sampled(0) {}
  FileSampledStats(const FileSampledStats& other) { *this = other; }
  FileSampledStats& operator=(const FileSampledStats& other) {
    num_reads_sampled = other.num_reads_sampled.load();
    return *this;
  }

  // number of user reads to this file.
  mutable std::atomic<uint64_t> num_reads_sampled;
};

struct FileMetaData {
  /**
   * fd - 文件描述符
   *
   * 功能概述:
   *   - FileDescriptor 对象，包含文件的基本信息
   *   - 封装文件号、文件大小、文件路径 ID 等
   *   - 包含文件的序列号范围
   *
   * 包含的信息:
   *   1. 文件标识:
   *   - file_number: 文件号（在数据库中唯一）
   *   - file_path_id: 文件路径 ID
   *   - file_size: 文件大小（字节）
   *
   *   2. 序列号范围:
   *   - smallest_seqno: 最小序列号
   *   - largest_seqno: 最大序列号
   *
   *   3. 其他元数据:
   *   - 根据具体实现可能包含其他信息
   *
   * 使用场景:
   *   1. 文件识别:
   *   - 唯一标识一个 SSTable 文件
   *   - 构建文件路径
   *
   *   2. 范围查询:
   *   - 根据序列号范围确定文件是否包含数据
   *   - 快速判断文件是否与查询相关
   *
   *   3. 版本管理:
   *   - 在 MANIFEST 中记录文件信息
   *   - 恢复时重建文件元数据
   *
   * 线程安全性:
   *   - 大部分字段不可变
   *   - 文件号和路径 ID 创建后不变
   *
   * 注意事项:
   *   - 是文件的核心标识
   *   - 与其他字段一起提供完整的文件信息
   *   - 在文件创建时设置
   */
  FileDescriptor fd;

  /**
   * smallest - 最小内部键
   *
   * 功能概述:
   *   - InternalKey 对象，表示文件中的最小键
   *   - 包含用户键、序列号和值类型
   *   - 用于文件范围查询和查找
   *
   * InternalKey 组成:
   *   - user_key: 用户键
   *   - sequence: 序列号
   *   - type: 值类型（Put/Delete/Merge 等）
   *
   * 使用场景:
   *   1. 文件查找:
   *   - 二分查找文件
   *   - 确定键可能在哪些文件中
   *
   *   2. 范围查询:
   *   - 比较查询范围与文件范围
   *   - 快速排除不相关的文件
   *
   *   3. Compaction:
   *   - 确定输入文件的范围
   *   - 检测文件重叠
   *
   *   4. 边界检查:
   *   - 检查文件之间的边界
   *   - 确保文件不重叠（L1-L6）
   *
   * 更新时机:
   *   - 文件创建时设置
   *   - 通过 UpdateBoundaries 更新
   *
   * 线程安全性:
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 与 largest 配合确定文件范围
   *   - L0 文件的 smallest 可能与其他文件重叠
   *   - L1-L6 文件的 smallest 不重叠
   */
  InternalKey smallest;  // Smallest internal key served by table

  /**
   * largest - 最大内部键
   *
   * 功能概述:
   *   - InternalKey 对象，表示文件中的最大键
   *   - 包含用户键、序列号和值类型
   *   - 用于文件范围查询和查找
   *
   * InternalKey 组成:
   *   - user_key: 用户键
   *   - sequence: 序列号
   *   - type: 值类型（Put/Delete/Merge 等）
   *
   * 使用场景:
   *   1. 文件查找:
   *   - 二分查找文件
   *   - 确定键可能在哪些文件中
   *
   *   2. 范围查询:
   *   - 比较查询范围与文件范围
   *   - 快速排除不相关的文件
   *
   *   3. Compaction:
   *   - 确定输入文件的范围
   *   - 检测文件重叠
   *
   *   4. 边界检查:
   *   - 检查文件之间的边界
   *   - 确保文件不重叠（L1-L6）
   *
   * 更新时机:
   *   - 文件创建时设置
   *   - 通过 UpdateBoundaries 更新
   *
   * 线程安全性:
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 与 smallest 配合确定文件范围
   *   - L0 文件的 largest 可能与其他文件重叠
   *   - L1-L6 文件的 largest 不重叠
   */
  InternalKey largest;   // Largest internal key served by table

  /**
   * table_reader_handle - 表读取器缓存句柄
   *
   * 功能概述:
   *   - Cache::Handle 指针，指向 TableCache 中的表读取器
   *   - 缓存已打开的 SSTable 文件读取器
   *   - 当引用计数为 0 时需要释放
   *
   * 功能:
   *   1. 缓存管理:
   *   - 引用 TableCache 中的表读取器
   *   - 避免重复打开文件
   *
   *   2. 生命周期管理:
   *   - 引用计数管理
   *   - refs = 0 时释放缓存
   *
   *   3. 性能优化:
   *   - 避免频繁的文件打开操作
   *   - 减少 I/O 开销
   *
   * 使用场景:
   *   1. 读取操作:
   *   - 首次读取文件时获取缓存
   *   - 后续读取使用缓存
   *
   *   2. Compaction:
   *   - Compaction 时读取文件使用缓存
   *   - 避免 I/O 开销
   *
   *   3. 迭代器:
   *   - 迭代器使用缓存的表读取器
   *
   * 引用管理:
   *   - 获取缓存时增加引用
   *   - 释放缓存时减少引用
   *   - refs = 0 时可以从缓存移除
   *
   * 线程安全性:
   *   - TableCache 内部有锁机制
   *   - 引用计数是原子的
   *
   * 注意事项:
   *   - 初始值为 nullptr
   *   - 引用计数为 0 时需要释放
   *   - 不要直接释放，通过 TableCache 管理
   */
  // Needs to be disposed when refs becomes 0.
  Cache::Handle* table_reader_handle = nullptr;

  /**
   * stats - 文件采样统计信息
   *
   * 功能概述:
   *   - FileSampledStats 对象，包含文件的采样统计信息
   *   - 用于优化 Compaction 和读取性能
   *   - 采样的文件属性和访问模式
   *
   * 包含的信息:
   *   1. 文件属性:
   *   - 文件大小
   *   - 条目数量
   *   - 删除条目比例
   *
   *   2. 访问模式:
   *   - 读取频率
   *   - 读取位置分布
   *   - 冷热数据识别
   *
   *   3. 性能指标:
   *   - 读取延迟
   *   - 缓存命中率
   *
   * 使用场景:
   *   1. Compaction 优化:
   *   - 根据 access pattern 选择文件
   *   - 优化 Compaction 顺序
   *
   *   2. 温度管理:
   *   - 识别冷热数据
   *   - 调整存储策略
   *
   *   3. 性能分析:
   *   - 分析文件访问模式
   *   - 优化存储布局
   *
   * 更新时机:
   *   - 文件访问时更新
   *   - 周期性采样
   *
   * 线程安全性:
   *   - 内部有原子操作
   *   - 支持多线程更新
   *
   * 注意事项:
   *   - 采样数据，不是精确值
   *   - 用于性能优化
   *   - 可能需要定期更新
   */
  FileSampledStats stats;

  /**
   * compensated_file_size - 补偿文件大小
   *
   * 功能概述:
   *   - 64 位无符号整数，文件补偿后的大小
   *   - 考虑删除条目对实际存储占用的影响
   *   - 用于计算文件的 Compaction 优先级
   *
   * 计算方法:
   *   - compensated_file_size = file_size + compensation
   *   - compensation 基于删除条目估计
   *   - 删除条目实际占用空间，但已删除
   *
   * 为什么需要补偿大小:
   *   1. 删除条目占用空间:
   *   - 删除的键值对仍占用存储
   *   - 需要等待 Compaction 清理
   *   - file_size 包含这些垃圾数据
   *
   *   2. Compaction 优先级:
   *   - 垃圾数据多的文件优先 Compaction
   *   - 补偿大小反映实际有效数据 + 垃圾
   *   - 更准确地评估 Compaction 收益
   *
   *   3. 性能优化:
   *   - 优先处理垃圾多的文件
   *   - 提高空间利用率
   *
   * 更新时机:
   *   - 文件创建或首次加载时计算
   *   - Version::ComputeCompensatedSizes()
   *   - 更新后不可变
   *
   * 计算公式:
   *   - compensated_file_size = raw_file_size + extra_deletion_space
   *   - extra_deletion_space 估计删除条目占用的额外空间
   *
   * 使用场景:
   *   1. Compaction 优先级:
   *   - 计算文件的压缩分数
   *   - 选择需要 Compaction 的文件
   *
   *   2. 资源管理:
   *   - 评估实际有效数据量
   *   - 评估垃圾数据量
   *
   * 线程安全性:
   *   - 更新后不可变
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 初次加载时计算
   *   - 更新后（!= 0）不可变
   *   - 用于 Compaction 优先级计算
   */
  // Stats for compensating deletion entries during compaction

  // File size compensated by deletion entry.
  // This is used to compute a file's compaction priority, and is updated in
  // Version::ComputeCompensatedSizes() first time when the file is created or
  // loaded.  After it is updated (!= 0), it is immutable.
  uint64_t compensated_file_size = 0;

  /**
   * num_entries - 条目数量
   *
   * 功能概述:
   *   - 64 位无符号整数，记录文件中的条目总数
   *   - 包括所有类型的条目（Put/Delete/Merge 等）
   *   - 用于统计和 Compaction 决策
   *
   * 统计内容:
   *   - Put 条目
   *   - Delete 条目（单点删除）
   *   - Merge 条目
   *   - Range Delete 条目（范围删除）
   *
   * 使用场景:
   *   1. Compaction 决策:
   *   - 评估文件的工作量
   *   - 估计 Compaction 成本
   *
   *   2. 统计分析:
   *   - 分析工作负载
   *   - 计算平均每文件条目数
   *
   *   3. 性能监控:
   *   - 监控数据库大小
   *   - 评估存储效率
   *
   *   4. 估计值大小:
   *   - 配合 raw_value_size 计算平均 Value 大小
   *
   * 更新时机:
   *   - Flush/Compaction 时计算
   *   - LogAndApply 线程中更新
   *
   * 线程安全性:
   *   - 只能从单线程的 LogAndApply 线程读写
   *   - 避免并发访问
   *
   * 注意事项:
   *   - 可变字段
   *   - 包括所有类型的条目
   *   - 与 num_deletions 配合计算有效条目
   */
  // These values can mutate, but they can only be read or written from
  // single-threaded LogAndApply thread
  uint64_t num_entries = 0;     // the number of entries.

  /**
   * num_deletions - 删除条目数量
   *
   * 功能概述:
   *   - 64 位无符号整数，记录文件中的删除条目数
   *   - 包括单点删除和范围删除
   *   - 用于垃圾回收评估
   *
   * 统计内容:
   *   1. 单点删除:
   *   - 删除单个键
   *   - 对应 ValueType::kTypeDeletion
   *
   *   2. 范围删除:
   *   - 删除一个键范围
   *   - 对应 ValueType::kTypeRangeDeletion
   *
   * 使用场景:
   *   1. 垃圾回收:
   *   - 评估垃圾数据量
   *   - 估计 Compaction 收益
   *
   *   2. Compaction 决策:
   *   - 删除条目多的文件优先 Compaction
   *   - 补偿文件大小计算
   *
   *   3. 空间分析:
   *   - 计算删除数据比例
   *   - 评估存储效率
   *
   * 更新时机:
   *   - Flush/Compaction 时计算
   *   - LogAndApply 线程中更新
   *
   * 线程安全性:
   *   - 只能从单线程的 LogAndApply 线程读写
   *   - 避免并发访问
   *
   * 注意事项:
   *   - 包括单点删除和范围删除
   *   - 与 num_entries 配合计算删除比例
   *   - 用于 Compaction 优先级计算
   */
  // The number of deletion entries, including range deletions.
  uint64_t num_deletions = 0;

  /**
   * raw_key_size - 原始键大小
   *
   * 功能概述:
   *   - 64 位无符号整数，记录文件中所有键的未压缩总大小
   *   - 不包括压缩后的键大小
   *   - 用于计算压缩比和平均大小
   *
   * 统计内容:
   *   - 所有键的未压缩大小之和
   *   - 包括删除条目的键
   *   - 不包括压缩开销
   *
   * 使用场景:
   *   1. 压缩比计算:
   *   - file_size / (raw_key_size + raw_value_size)
   *   - 评估压缩效果
   *
   *   2. 平均键大小:
   *   - raw_key_size / num_entries
   *   - 评估键的平均大小
   *
   *   3. Compaction 决策:
   *   - 估计 Compaction 后的大小
   *
   *   4. 性能分析:
   *   - 分析键的分布
   *   - 优化配置
   *
   * 更新时机:
   *   - Flush/Compaction 时计算
   *   - LogAndApply 线程中更新
   *
   * 线程安全性:
   *   - 只能从单线程的 LogAndApply 线程读写
   *   - 避免并发访问
   *
   * 注意事项:
   *   - 未压缩的大小
   *   - 包括删除条目的键
   *   - 与 raw_value_size 配合使用
   */
  uint64_t raw_key_size = 0;    // total uncompressed key size.

  /**
   * raw_value_size - 原始值大小
   *
   * 功能概述:
   *   - 64 位无符号整数，记录文件中所有值的未压缩总大小
   *   - 不包括压缩后的值大小
   *   - 用于计算压缩比和平均大小
   *
   * 统计内容:
   *   - 所有值的未压缩大小之和
   *   - 不包括删除条目（删除操作没有值）
   *   - 不包括压缩开销
   *
   * 使用场景:
   *   1. 压缩比计算:
   *   - file_size / (raw_key_size + raw_value_size)
   *   - 评估压缩效果
   *
   *   2. 平均值大小:
   *   - raw_value_size / (num_entries - num_deletions)
   *   - 评估非删除条目的平均值大小
   *
   *   3. Compaction 决策:
   *   - 估计 Compaction 后的大小
   *   - 计算补偿文件大小
   *
   *   4. 性能分析:
   *   - 分析值的分布
   *   - 优化配置
   *
   * 更新时机:
   *   - Flush/Compaction 时计算
   *   - LogAndApply 线程中更新
   *
   * 线程安全性:
   *   - 只能从单线程的 LogAndApply 线程读写
   *   - 避免并发访问
   *
   * 注意事项:
   *   - 未压缩的大小
   *   - 不包括删除条目（删除操作没有值）
   *   - 与 raw_key_size 配合使用
   */
  uint64_t raw_value_size = 0;  // total uncompressed value size.

  /**
   * num_range_deletions - 范围删除条目数量
   *
   * 功能概述:
   *   - 64 位无符号整数，记录文件中的范围删除条目数
   *   - 仅统计范围删除（Range Delete）
   *   - 用于评估范围删除的影响
   *
   * 范围删除:
   *   - 删除一个键范围的所有数据
   *   - 对应 ValueType::kTypeRangeDeletion
   *   - 比单点删除更高效
   *
   * 使用场景:
   *   1. 范围删除影响评估:
   *   - 评估范围删除覆盖的数据量
   *   - 估计需要 Compaction 的数据量
   *
   *   2. Compaction 优化:
   *   - 范围删除多的文件可能需要特别处理
   *   - 优化范围删除的合并
   *
   *   3. 性能分析:
   *   - 分析工作负载中的删除模式
   *
   * 更新时机:
   *   - Flush/Compaction 时计算
   *   - LogAndApply 线程中更新
   *
   * 线程安全性:
   *   - 只能从单线程的 LogAndApply 线程读写
   *   - 避免并发访问
   *
   * 注意事项:
   *   - 仅统计范围删除
   *   - 不包括单点删除
   *   - 用于性能分析和优化
   */
  uint64_t num_range_deletions = 0;

  /**
   * compensated_range_deletion_size - 补偿范围删除大小
   *
   * 功能概述:
   *   - 64 位无符号整数，范围删除的补偿大小估计
   *   - 估计本文件的范围墓碑在下一层覆盖的键的大小
   *   - 在 Flush/Compaction 时计算
   *
   * 功能目的:
   *   1. 范围删除影响量化:
   *   - 范围墓碑在下一层覆盖的有效数据量
   *   - 这些数据在 Compaction 时可以删除
   *
   *   2. 补偿大小计算:
   *   - 添加到 compensated_file_size
   *   - 更准确地反映需要 Compaction 的数据量
   *
   *   3. Compaction 优先级:
   *   - 范围删除影响大的文件优先 Compaction
   *
   * 计算方法:
   *   - 基于下一层的文件分布
   *   - 估计范围墓碑覆盖的数据量
   *   - 考虑键重叠情况
   *
   * 使用场景:
   *   1. Compaction 决策:
   *   - 计算文件的 Compaction 优先级
   *   - 评估 Compaction 收益
   *
   *   2. 资源规划:
   *   - 估计需要的 Compaction 工作量
   *
   * 更新时机:
   *   - Flush/Compaction 时计算
   *   - 添加到 compensated_file_size
   *
   * 线程安全性:
   *   - 只能从单线程的 LogAndApply 线程读写
   *   - 避免并发访问
   *
   * 注意事项:
   *   - 估计值，不是精确值
   *   - 与 compensated_file_size 配合
   *   - 用于 Compaction 优先级计算
   */
  // This is computed during Flush/Compaction, and is added to
  // `compensated_file_size`. Currently, this estimates the size of keys in the
  // next level covered by range tombstones in this file.
  uint64_t compensated_range_deletion_size = 0;

  /**
   * refs - 引用计数
   *
   * 功能概述:
   *   - 整数，记录对 FileMetaData 的活跃引用数
   *   - 用于管理 FileMetaData 的生命周期
   *   - 引用计数为 0 时可以删除 FileMetaData
   *
   * 引用来源:
   *   1. Version:
   *   - 每个 Version 包含对 FileMetaData 的引用
   *   - 存活版本持有引用
   *
   *   2. Compaction 任务:
   *   - 正在进行的 Compaction 持有引用
   *   - 防止 Compaction 期间文件被删除
   *
   *   3. 迭代器:
   *   - 读迭代器可能持有引用
   *   - 防止迭代期间文件被删除
   *
   *   4. TableReader:
   *   - table_reader_handle 持有引用
   *   - 文件正在被读取
   *
   * 使用场景:
   *   1. 增加引用:
   *   - 创建 Version 时
   *   - 启动 Compaction 时
   *   - 打开文件读取时
   *
   *   2. 减少引用:
   *   - 销毁 Version 时
   *   - 完成 Compaction 时
   *   - 关闭文件读取时
   *
   *   3. 删除检查:
   *   - refs == 0 时可以删除
   *   - 检查是否还有其他引用
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 删除条件:
   *   - refs == 0
   *   - 从所有 Version 中移除
   *   - Compaction 完成
   *
   * 注意事项:
   *   - 非原子变量（需要锁保护）
   *   - 必须正确平衡增加和减少
   *   - 泄漏引用会导致内存泄漏
   *   - 过早释放会导致 use-after-free
   */
  int refs = 0;  // Reference count

  /**
   * being_compacted - 是否正在被压缩
   *
   * 功能概述:
   *   - 布尔值，标记文件是否正在进行 Compaction
   *   - 防止多个 Compaction 同时处理同一个文件
   *   - 确保文件的一致性
   *
   * 使用场景:
   *   1. Compaction 选择:
   *   - 选择输入文件时检查
   *   - 避免选择正在压缩的文件
   *
   *   2. 并发控制:
   *   - 防止多个 Compaction 任务冲突
   *   - 确保文件状态一致
   *
   *   3. 文件删除:
   *   - 检查文件是否可以删除
   *   - 正在压缩的文件不能删除
   *
   * 生命周期:
   *   - Compaction 开始时设置为 true
   *   - Compaction 完成时设置为 false
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 注意事项:
   *   - Compaction 期间为 true
   *   - 完成后重置为 false
   *   - 防止并发 Compaction
   */
  bool being_compacted = false;       // Is this file undergoing compaction?

  /**
   * init_stats_from_file - 是否已从文件初始化统计
   *
   * 功能概述:
   *   - 布尔值，标记文件的数据条目统计是否已从文件初始化
   *   - 指示是否已从 SSTable 文件加载统计信息
   *   - 用于延迟加载统计信息
   *
   * 含义:
   *   - false: 统计信息未初始化
   *   - true: 统计信息已从文件加载
   *
   * 使用场景:
   *   1. 延迟加载:
   *   - 初次创建时可能不初始化
   *   - 首次访问时加载
   *
   *   2. 统计初始化:
   *   - 从文件读取统计信息
   *   - 填充 stats 字段
   *
   *   3. 恢复:
   *   - 恢复文件元数据时设置
   *
   * 性能优化:
   *   - 避免频繁读取文件元数据
   *   - 按需加载统计信息
   *
   * 更新时机:
   *   - 首次加载文件统计时设置为 true
   *   - 一旦设置，不再改变
   *
   * 线程安全性:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 指示统计信息的初始化状态
   *   - 不影响其他字段
   *   - 用于性能优化
   */
  bool init_stats_from_file = false;  // true if the data-entry stats of this
                                      // file has initialized from file.

  /**
   * marked_for_compaction - 是否标记为需要压缩
   *
   * 功能概述:
   *   - 布尔值，标记文件是否被请求进行 Compaction
   *   - 用户或系统可以标记文件
   *   - Compaction 优先级考虑标记的文件
   *
   * 标记来源:
   *   1. 手动标记:
   *   - CompactFiles() API
   *   - 用户指定要 Compaction 的文件
   *
   *   2. 系统标记:
   *   - TTL 过期
   *   - 周期性 Compaction
   *   - 其他系统条件
   *
   * 使用场景:
   *   1. Compaction 调度:
   *   - 优先处理标记的文件
   *   - 确保标记的文件被处理
   *
   *   2. 用户请求:
   *   - 响应用户的手动 Compaction 请求
   *   - 处理指定的文件
   *
   *   3. 系统维护:
   *   - 处理过期的文件
   *   - 周期性 Compaction
   *
   * 生命周期:
   *   - 标记时设置为 true
   *   - Compaction 后清除标记
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 读取需要在互斥锁保护下
   *
   * 注意事项:
   *   - 用户请求或系统标记
   *   - Compaction 后会清除
   *   - 影响Compaction 优先级
   */
  bool marked_for_compaction = false;  // True if client asked us nicely to
                                       // compact this file.

  /**
   * temperature - 文件温度
   *
   * 功能概述:
   *   - Temperature 枚举，表示文件的热度
   *   - 用于分层存储和温度感知的 Compaction
   *   - 优化不同温度数据的存储策略
   *
   * 温度类型:
   *   1. kUnknown:
   *   - 温度未知
   *   - 初始值
   *
   *   2. kHot:
   *   - 热数据，频繁访问
   *   - 存储在快速存储
   *
   *   3. kWarm:
   *   - 温数据，中等访问频率
   *   - 存储在标准存储
   *
   *   4. kCold:
   *   - 冷数据，很少访问
   *   - 存储在慢速存储
   *
   * 使用场景:
   *   1. 分层存储:
   *   - 不同温度数据存储在不同层级
   *   - 热数据在快存储
   *
   *   2. Compaction:
   *   - 温度感知的 Compaction 策略
   *   - 优化数据布局
   *
   *   3. 性能优化:
   *   - 根据访问模式调整存储
   *   - 提高整体性能
   *
   * 更新机制:
   *   - 根据访问模式更新
   *   - 定期采样访问频率
   *
   * 并发控制:
   *   - 需要在 DB 互斥锁保护下修改
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 用于分层存储优化
   *   - 影响数据布局和 Compaction
   *   - 需要定期更新以反映访问模式
   */
  Temperature temperature = Temperature::kUnknown;

  /**
   * oldest_blob_file_number - 最旧的 Blob 文件号
   *
   * 功能概述:
   *   - 64 位无符号整数，记录此 SST 文件引用的最旧 Blob 文件号
   *   - 仅用于 BlobDB
   *   - 0 表示无效值
   *
   * BlobDB 机制:
   *   - 大值存储在独立的 Blob 文件中
   *   - SSTable 存储 Blob 文件的引用（BlobIndex）
   *   - 一个 SST 文件可能引用多个 Blob 文件
   *
   * 使用场景:
   *   1. Blob GC:
   *   - 确定 SST 文件引用的 Blob 文件
   *   - 评估哪些 Blob 文件可以 GC
   *
   *   2. Blob 文件管理:
   *   - 追踪 SST 文件引用的 Blob 文件
   *   - 确定引用关系
   *
   *   3. 清理:
   *   - SST 文件删除时清理 Blob 引用
   *
   * 值含义:
   *   - 最旧的 Blob 文件号（最小号）
   *   - Blob 文件号从 1 开始
   *   - 0 表示无效
   *
   * 更新时机:
   *   - Flush/Compaction 时设置
   *   - 根据引用的 Blob 文件确定
   *
   * 并发控制:
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项:
   *   - 仅用于 BlobDB
   *   - 记录最旧的 Blob 文件号
   *   - 0 表示无效值
   */
  // Used only in BlobDB. The file number of the oldest blob file this SST file
  // refers to. 0 is an invalid value; BlobDB numbers the files starting from 1.
  uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;

  /**
   * oldest_ancester_time - 最祖先时间
   *
   * 功能概述:
   *   - 64 位无符号整数，Unix 时间戳（秒）
   *   - 记录文件数据来源的最原始文件的创建时间
   *   - 用于准确判断数据是否过期（TTL）
   *
   * 核心概念：
   *   1. 最祖先时间（Oldest Ancestor Time）：
   *      - 定义：文件数据来源的最原始文件的创建时间
   *      - 含义：追溯到数据的最初插入时间，即使经过多次 compaction
   *      - 用途：准确判断数据是否过期（TTL 计算）
   *
   *   2. 为什么需要 oldest_ancester_time：
   *      - file_creation_time 只反映当前文件的创建时间
   *      - 如果文件经过 compaction，file_creation_time 会更新
   *      - 无法准确追踪数据的原始插入时间
   *      - TTL 是基于数据插入时间的生存期
   *      - 需要最初的插入时间来判断是否过期
   *
   *   3. oldest_ancester_time vs file_creation_time：
   *      - oldest_ancester_time：
   *        a. 数据的最初插入时间
   *        b. 在 compaction 期间保持不变
   *        c. 用于 TTL 判断
   *        d. 可以追溯到多个 compaction 之前
   *      - file_creation_time：
   *        a. 当前文件的创建时间
   *        b. 每次 compaction 都会更新
   *        c. 用于周期性压缩判断
   *        d. 只反映最近一次 compaction 的时间
   *
   * 使用场景：
   *   1. TTL 压缩：
   *   - 判断数据是否过期
   *   - 准确的 TTL 计算
   *
   *   2. 数据追踪：
   *   - 追踪数据的历史和来源
   *   - 确保数据一致性
   *
   * 更新规则：
   *   - 新文件：oldest_ancester_time = 自己的创建时间
   *   - Compaction 输出：继承输入文件的最小 oldest_ancester_time
   *
   * 值含义：
   *   - Unix 时间戳（秒）
   *   - 0 表示不可用（kUnknownOldestAncesterTime）
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 准确追踪数据的最初插入时间
   *   - 在 compaction 期间保持不变
   *   - 对 TTL 压缩至关重要
   *   - 0 表示信息不可用
   */
  // The file could be the compaction output from other SST files, which could
  // in turn be outputs for compact older SST files. We track the memtable
  // flush timestamp for the oldest SST file that eventually contribute data
  // to this file. 0 means that information is not available.
  uint64_t oldest_ancester_time = kUnknownOldestAncesterTime;

  /**
   * file_creation_time - 文件创建时间
   *
   * 功能概述:
   *   - 64 位无符号整数，Unix 时间戳（秒）
   *   - 记录 SST 文件的创建时间
   *   - 用于周期性 Compaction 和 TTL 管理
   *
   * 使用场景:
   *   1. 周期性 Compaction:
   *   - 判断文件是否超过周期
   *   - 定期 Compaction 旧文件
   *
   *   2. TTL 管理：
   *   - 与 oldest_ancester_time 配合
   *   - 准确的过期判断
   *
   *   3. 文件管理：
   *   - 管理文件生命周期
   *   - 清理旧文件
   *
   * 更新时机：
   *   - Flush/Compaction 创建文件时设置
   *   - 文件创建后不再改变
   *
   * 值含义：
   *   - Unix 时间戳（秒）
   *   - 0 表示不可用（kUnknownFileCreationTime）
   *
   * 与 oldest_ancester_time 的区别：
   *   - file_creation_time：当前文件的创建时间
   *   - oldest_ancester_time：数据最初的插入时间
   *   - Compaction 后 file_creation_time 更新
   *   - Compaction 后 oldest_ancester_time 不变
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - Unix 时间戳（秒）
   *   - 用于周期性 Compaction
   *   - 0 表示不可用
   */
  // Unix time when the SST file is created.
  uint64_t file_creation_time = kUnknownFileCreationTime;

  /**
   * epoch_number - Epoch 号
   *
   * 功能概述:
   *   - 64 位无符号整数，文件的 Epoch 号
   *   - 表示文件被 Flush 或 Ingest/Import 的顺序
   *   - 用于恢复时验证文件顺序和一致性
   *
   * 功能：
   *   1. 文件顺序：
   *   - 追踪文件的 Flush 顺序
   *   - 确保 L0 文件的正确顺序
   *
   *   2. 恢复验证：
   *   - 恢复时验证文件顺序
   *   - 检测文件损坏或不一致
   *
   *   3. 一致性检查：
   *   - 检查文件的 Epoch 号是否有效
   *   - 确保数据一致性
   *
   * 赋值规则：
   *   - Flush 文件：递增分配 Epoch 号
   *   - Ingest/Import 文件：保持原 Epoch 号
   *   - Compaction 输出：继承输入文件的最小 Epoch 号
   *   - L0 文件：更大的 Epoch 号表示更新的文件
   *
   * 使用场景：
   *   1. 恢复：
   *   - 根据 Epoch 号恢复文件顺序
   *   - 验证文件的一致性
   *
   *   2. 一致性检查：
   *   - 检查文件的 Epoch 号是否有效
   *   - 检测异常
   *
   *   3. L0 管理：
   *   - 确定 L0 文件的顺序
   *   - 优化 L0 读取
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 用于恢复和一致性检查
   *   - L0 文件的 Epoch 号有意义
   *   - kUnknownEpochNumber 表示未知
   */
  // The order of a file being flushed or ingested/imported.
  // Compaction output file will be assigned with the minimum `epoch_number`
  // among input files'.
  // For L0, larger `epoch_number` indicates newer L0 file.
  uint64_t epoch_number = kUnknownEpochNumber;

  /**
   * file_checksum - 文件校验和
   *
   * 功能概述:
   *   - 字符串，文件的校验和值
   *   - 用于验证文件完整性
   *   - 检测文件损坏
   *
   * 功能：
   *   1. 完整性验证：
   *   - 读取文件时验证校验和
   *   - 检测文件损坏
   *
   *   2. 数据一致性：
   *   - 确保文件未被篡改
   *   - 验证数据正确性
   *
   *   3. 恢复：
   *   - 恢复时验证文件
   *   - 拒绝损坏的文件
   *
   * 使用场景：
   *   1. 文件读取：
   *   - 读取时验证校验和
   *   - 检测损坏
   *
   *   2. 恢复：
   *   - 恢复时验证文件
   *   - 确保数据完整性
   *
   *   3. 检查：
   *   - 定期检查文件完整性
   *
   * 计算方法：
   *   - 由 file_checksum_func_name 指定的算法计算
   *   - 文件创建时计算
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 字符串格式
   *   - 与 file_checksum_func_name 配对
   *   - kUnknownFileChecksum 表示未知
   */
  // File checksum
  std::string file_checksum = kUnknownFileChecksum;

  /**
   * file_checksum_func_name - 文件校验和函数名
   *
   * 功能概述:
   *   - 字符串，文件校验和算法的名称
   *   - 指定用于计算 file_checksum 的算法
   *   - 用于验证和恢复
   *
   * 常见算法：
   *   1. CRC32:
   *   - 快速，性能好
   *   - 安全性一般
   *
   *   2. CRC32C:
   *   - CRC32 的一种变体
   *   - 在某些硬件上有加速
   *
   *   3. xxHash:
   *   - 快速，分布好
   *   - 非加密用途
   *
   *   4. 其他：
   *   - 根据配置可能使用其他算法
   *
   * 使用场景：
   *   1. 校验和验证：
   *   - 知道使用哪种算法
   *   - 正确验证文件
   *
   *   2. 恢复：
   *   - 恢复时使用相同算法验证
   *   - 确保一致性
   *
   *   3. 配置：
   *   - 指定校验和算法
   *   - 性能和安全平衡
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 字符串格式
   *   - 与 file_checksum 配对
   *   - kUnknownFileChecksumFuncName 表示未知
   */
  // File checksum function name
  std::string file_checksum_func_name = kUnknownFileChecksumFuncName;

  /**
   * unique_id - SST 唯一标识符
   *
   * 功能概述:
   *   - UniqueId64x2 类型，SST 文件的唯一标识符
   *   - 128 位全局唯一 ID
   *   - 用于跨文件系统和实例识别文件
   *
   * 特性：
   *   1. 唯一性：
   *   - 全局唯一
   *   - 跨文件系统和实例唯一
   *
   *   2. 持久性：
   *   - 文件创建后不变
   *   - 即使文件移动或复制，ID 不变
   *
   *   3. 可追踪性：
   *   - 追踪文件生命周期
   *   - 用于调试和审计
   *
   * 使用场景：
   *   1. 文件识别：
   *   - 在多个实例中识别同一文件
   *   - 文件迁移和复制
   *
   *   2. 调试：
   *   - 追踪文件
   *   - 诊断问题
   *
   *   3. 审计：
   *   - 记录文件历史
   *   - 追踪数据流
   *
   *   4. 去重：
   *   - 检测重复文件
   *
   * 生成规则：
   *   - 文件创建时生成
   *   - 使用随机数或 UUID
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 128 位全局唯一 ID
   *   - 不与文件号混淆
   *   - 用于跨实例识别
   */
  // SST unique id
  UniqueId64x2 unique_id{};

  /**
   * tail_size - SST 文件尾部大小
   *
   * 功能概述:
   *   - 64 位无符号整数，SST 文件"尾部"的大小
   *   - 尾部指数据块之后到文件末尾的所有块
   *   - 包括索引块、元数据块等
   *
   * SST 文件结构：
   *   1. 数据块（Data Blocks）：
   *   - 存储实际的键值对
   *   - 文件的主要部分
   *
   *   2. 尾部（Tail）：
   *   - 索引块（Index Block）
   *   - 过滤器块（Filter Block）
   *   - 元数据块（Meta Block）
   *   - 压缩字典等
   *
   * tail_size 包括：
   *   - 索引块大小
   *   - 过滤器块大小
   *   - 元数据块大小
   *   - 其他元数据结构
   *
   * 使用场景：
   *   1. 性能分析：
   *   - 分析文件结构
   *   - 评估索引和过滤器开销
   *
   *   2. 缓存策略：
   *   - 优化缓存策略
   *   - 区分数据部分和元数据部分
   *
   *   3. 存储优化：
   *   - 评估元数据开销
   *   - 优化文件布局
   *
   * 计算方法：
   *   - tail_size = file_size - data_blocks_size
   *   - file_size 是文件总大小
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 不包括数据块
   *   - 包括索引、过滤器、元数据等
   *   - 用于性能分析和优化
   */
  // Size of the "tail" part of a SST file
  // "Tail" refers to all blocks after data blocks till the end of the SST file
  uint64_t tail_size = 0;

  /**
   * user_defined_timestamps_persisted - 用户定义时间戳是否已持久化
   *
   * 功能概述:
   *   - 布尔值，标记文件创建时持久化时间戳的标志值
   *   - 记录 `AdvancedColumnFamilyOptions.persist_user_defined_timestamps` 的值
   *   - 默认为 true，只有当此标志为 false 时才显式写入 MANIFEST
   *
   * 用户定义时间戳：
   *   - 允许用户在键中附加自定义时间戳
   *   - 用于基于时间的查询和删除
   *   - 独立于 RocksDB 的序列号
   *
   * 标志含义：
   *   - true（默认）：持久化时间戳
   *   - false：不持久化时间戳
   *
   * 使用场景：
   *   1. 时间戳管理：
   *   - 追踪时间戳是否持久化
   *   - 恢复时了解文件特性
   *
   *   2. 兼容性：
   *   - 支持不同的时间戳策略
   *   - 确保恢复正确
   *
   *   3. 配置：
   *   - 记录创建文件时的配置
   *   - 确保行为一致
   *
   * 写入 MANIFEST：
   *   - 默认为 true 时不写入（隐式）
   *   - 为 false 时显式写入
   *
   * 并发控制：
   *   - 文件创建后不可变
   *   - 多线程安全读取
   *
   * 注意事项：
   *   - 默认为 true
   *   - 记录创建时的配置
   *   - 影响恢复行为
   */
  // Value of the `AdvancedColumnFamilyOptions.persist_user_defined_timestamps`
  // flag when the file is created. Default to true, and only when this flag is
  // false, it's explicitly written to Manifest.
  bool user_defined_timestamps_persisted = true;

  FileMetaData() = default;

  FileMetaData(uint64_t file, uint32_t file_path_id, uint64_t file_size,
               const InternalKey& smallest_key, const InternalKey& largest_key,
               const SequenceNumber& smallest_seq,
               const SequenceNumber& largest_seq, bool marked_for_compact,
               Temperature _temperature, uint64_t oldest_blob_file,
               uint64_t _oldest_ancester_time, uint64_t _file_creation_time,
               uint64_t _epoch_number, const std::string& _file_checksum,
               const std::string& _file_checksum_func_name,
               UniqueId64x2 _unique_id,
               const uint64_t _compensated_range_deletion_size,
               uint64_t _tail_size, bool _user_defined_timestamps_persisted)
      : fd(file, file_path_id, file_size, smallest_seq, largest_seq),
        smallest(smallest_key),
        largest(largest_key),
        compensated_range_deletion_size(_compensated_range_deletion_size),
        marked_for_compaction(marked_for_compact),
        temperature(_temperature),
        oldest_blob_file_number(oldest_blob_file),
        oldest_ancester_time(_oldest_ancester_time),
        file_creation_time(_file_creation_time),
        epoch_number(_epoch_number),
        file_checksum(_file_checksum),
        file_checksum_func_name(_file_checksum_func_name),
        unique_id(std::move(_unique_id)),
        tail_size(_tail_size),
        user_defined_timestamps_persisted(_user_defined_timestamps_persisted) {
    TEST_SYNC_POINT_CALLBACK("FileMetaData::FileMetaData", this);
  }

  // REQUIRED: Keys must be given to the function in sorted order (it expects
  // the last key to be the largest).
  Status UpdateBoundaries(const Slice& key, const Slice& value,
                          SequenceNumber seqno, ValueType value_type);

  // Unlike UpdateBoundaries, ranges do not need to be presented in any
  // particular order.
  void UpdateBoundariesForRange(const InternalKey& start,
                                const InternalKey& end, SequenceNumber seqno,
                                const InternalKeyComparator& icmp) {
    if (smallest.size() == 0 || icmp.Compare(start, smallest) < 0) {
      smallest = start;
    }
    if (largest.size() == 0 || icmp.Compare(largest, end) < 0) {
      largest = end;
    }
    assert(icmp.Compare(smallest, largest) <= 0);
    fd.smallest_seqno = std::min(fd.smallest_seqno, seqno);
    fd.largest_seqno = std::max(fd.largest_seqno, seqno);
  }

  /**
   * @brief 尝试获取文件的最祖先时间（Oldest Ancestor Time）
   *
   * 本函数的作用：
   * 1. 从多个来源尝试获取最祖先时间
   * 2. 按优先级顺序检查：成员变量 → table properties
   * 3. 返回 Unix 时间戳（秒），或 0 表示不可用
   *
   * 核心概念：
   *
   * 1. 最祖先时间（Oldest Ancestor Time）：
   *    - 定义：文件数据来源的最原始文件的创建时间
   *    - 含义：追溯到数据的最初插入时间，即使经过多次 compaction
   *    - 用途：
   *      a. 准确判断数据是否过期（TTL 计算）
   *      b. 追踪数据的历史和来源
   *      c. 确保 TTL 压缩的正确性
   *    - 特点：
   *      a. 在 compaction 期间保持不变
   *      b. 新文件的 oldest_ancester_time = 自己的创建时间
   *      c. compaction 输出文件继承输入文件的最小 oldest_ancester_time
   *
   * 2. 为什么需要 oldest_ancester_time：
   *    a. 准确性：
   *       - file_creation_time 只反映当前文件的创建时间
   *       - 如果文件经过 compaction，file_creation_time 会更新
   *       - 无法准确追踪数据的原始插入时间
   *    b. TTL 判断：
   *       - TTL 是基于数据插入时间的生存期
   *       - 需要最初的插入时间来判断是否过期
   *       - oldest_ancester_time 保留了最初的时间
   *    c. 一致性：
   *       - 同一批次的数据有相同的 oldest_ancester_time
   *       - 确保 TTL 判断的一致性
   *
   * 3. oldest_ancester_time vs file_creation_time：
   *    - oldest_ancester_time：
   *      a. 数据的最初插入时间
   *      b. 在 compaction 期间保持不变
   *      c. 用于 TTL 判断
   *      d. 可以追溯到多个 compaction 之前
   *    - file_creation_time：
   *      a. 当前文件的创建时间
   *      b. 每次 compaction 都会更新
   *      c. 用于周期性压缩判断
   *      d. 只反映最近一次 compaction 的时间
   *    - 示例：
   *      - t=0: 文件 A 创建，oldest_ancester_time=0, file_creation_time=0
   *      - t=10: 文件 A 被压缩，生成文件 B
   *               文件 B: oldest_ancester_time=0, file_creation_time=10
   *      - t=20: 文件 B 被压缩，生成文件 C
   *               文件 C: oldest_ancester_time=0, file_creation_time=20
   *      - TTL=15，当前时间=25：
   *        使用 oldest_ancester_time: 25-0=25 > 15，过期，需要压缩
   *        使用 file_creation_time: 25-20=5 < 15，未过期，不压缩
   *        正确的判断应该使用 oldest_ancester_time
   *
   * 4. 数据来源的优先级：
   *    a. 成员变量 oldest_ancester_time：
   *       - 优先级最高
   *       - 已缓存在 FileMetaData 中
   *       - 读取速度快，无需额外 I/O
   *       - 在文件创建或加载时设置
   *    b. table_properties 中的 creation_time：
   *       - 优先级次之
   *       - 存储在 SSTable 文件的元数据中
   *       - 需要通过 table_reader 访问
   *       - 可能耗时（需要读取文件元数据）
   *    c. 如果都不可用，返回 kUnknownOldestAncesterTime（0）
   *
   * 5. 为什么检查 table_reader 和 GetTableProperties()：
   *    a. table_reader：
   *       - TableReader* 类型，指向打开的表读取器
   *       - 如果为 nullptr，说明文件未打开或已关闭
   *       - 只有打开的文件才能访问其 table properties
   *    b. GetTableProperties()：
   *       - 返回 SSTable 文件的属性（TableProperties*）
   *       - 包含 creation_time、file_size、num_entries 等
   *       - 如果为 nullptr，说明属性不可用
   *    c. 双重检查：
   *       - 确保 table_reader 不为空
   *       - 确保 GetTableProperties() 不为空
   *       - 避免空指针解引用导致的崩溃
   *
   * 6. table_reader 的生命周期和 pin 机制：
   *    - table_reader 可能被 cache 管理
   *    - 被 pin 的 table_reader 不会被 cache 淘汰
   *    - 函数注释中的 "pinned" 指的就是这种机制
   *    - 如果 table_reader 被 pin，说明它正在被使用
   *    - 可以安全地访问其 properties
   *
   * 应用场景：
   * 1. TTL 压缩判断：
   *    - ComputeExpiredTtlFiles() 调用此函数
   *    - 判断文件是否包含过期数据
   *    - 根据 oldest_ancester_time 计算是否过期
   *
   * 2. 数据生命周期追踪：
   *    - 追踪数据从最初插入到最终删除的完整生命周期
   *    - 帮助调试和分析数据流动
   *
   * 3. 性能监控：
   *    - 分析数据在系统中停留的时间
   *    - 优化 TTL 配置参数
   *
   * 性能考虑：
   * 1. 优先使用成员变量：
   *    - 避免读取 table properties
   *    - 减少 I/O 操作
   *    - 提高查询速度
   *
   * 2. 缓存机制：
   *    - oldest_ancester_time 被缓存在 FileMetaData 中
   *    - 文件创建或加载时设置
   *    - 后续查询无需 I/O
   *
   * 3. table_reader 检查：
   *    - 避免访问未打开的文件
   *    - 减少不必要的开销
   *    - 提高稳定性
   *
   * 注意事项：
   * 1. 返回值含义：
   *    - 返回 Unix 时间戳（秒）
   *    - 返回 0（kUnknownOldestAncesterTime）表示不可用
   *    - 调用者需要检查返回值是否为 0
   *
   * 2. 数据一致性：
   *    - oldest_ancester_time 在 compaction 期间保持不变
   *    - 新文件继承输入文件的最小 oldest_ancester_time
   *    - 确保数据链的完整性
   *
   * 3. table_reader 的可用性：
   *    - 只有当文件被打开时，table_reader 才有效
   *    - 文件关闭后，table_reader 可能为 nullptr
   *    - 需要处理这种情况
   *
   * 4. kUnknownOldestAncesterTime（0）的特殊含义：
   *    - 0 不是有效的时间戳
   *    - 表示时间信息不可用
   *    - 不应该用于 TTL 计算
   *    - 调用者需要检查并跳过
   *
   * @return Unix 时间戳（秒），或 0（kUnknownOldestAncesterTime）表示不可用
   *
   * @note 此函数尝试从多个来源获取时间信息
   * @note 优先使用缓存的成员变量，避免 I/O
   * @note 返回 0 表示时间信息不可用
   * @see oldest_ancester_time 最祖先时间成员变量
   * @see file_creation_time 文件创建时间成员变量
   * @see TableProperties SSTable 文件属性
   * @see ComputeExpiredTtlFiles() 使用此函数判断 TTL 过期
   */
  // Try to get oldest ancester time from the class itself or table properties
  // if table reader is already pinned.
  // 0 means the information is not available.
  uint64_t TryGetOldestAncesterTime() {
    // 优先级 1：检查成员变量 oldest_ancester_time
    // 如果不等于 kUnknownOldestAncesterTime（0），说明已有缓存值
    // 直接返回缓存的值，无需进一步查询
    // 这种情况下，时间戳已经从 table properties 读取并缓存
    // 可以直接使用，避免重复读取
    if (oldest_ancester_time != kUnknownOldestAncesterTime) {
      return oldest_ancester_time;
    // 优先级 2：从 table properties 中读取 creation_time
    // 如果 table_reader 不为空且 GetTableProperties() 不为空
    // 说明文件已打开且 table properties 可用
    // 可以从中读取 creation_time 作为 oldest_ancester_time
    // 注意：这里使用的是 creation_time 而不是其他字段
    // 因为 creation_time 在 compaction 期间保持不变
    } else if (fd.table_reader != nullptr &&
               fd.table_reader->GetTableProperties() != nullptr) {
      // 从 table properties 中返回 creation_time
      // 这是 SSTable 文件中存储的创建时间
      // 可以追溯到数据的最初插入时间
      return fd.table_reader->GetTableProperties()->creation_time;
    }
    // 如果以上两个条件都不满足，返回 kUnknownOldestAncesterTime（0）
    // 说明：
    // 1. 成员变量 oldest_ancester_time 为 0（未知）
    // 2. table_reader 为空（文件未打开）
    // 3. GetTableProperties() 为空（属性不可用）
    // 这种情况下，时间信息不可用
    // 调用者需要检查返回值是否为 0，并相应处理
    return kUnknownOldestAncesterTime;
  }

  /**
   * @brief 尝试获取文件的创建时间（File Creation Time）
   *
   * 本函数的作用：
   * 1. 从多个来源尝试获取文件的创建时间
   * 2. 按优先级顺序检查：成员变量 → table properties
   * 3. 返回 Unix 时间戳（秒），或 0 表示不可用
   *
   * 核心概念：
   *
   * 1. 文件创建时间（File Creation Time）：
   *    - 定义：SSTable 文件实际被创建的 Unix 时间戳
   *    - 单位：秒（seconds）
   *    - 来源：
   *      a. Flush：memtable 刷盘时创建 SSTable 文件
   *      b. Compaction：压缩输入文件生成新的 SSTable 文件
   *      c. Ingest：外部导入的 SSTable 文件
   *    - 用途：
   *      a. 周期性压缩（Periodic Compaction）判断
   *      b. 文件年龄统计和分析
   *      c. FIFO compaction 的文件淘汰
   *      d. 性能监控和调试
   *
   * 2. file_creation_time vs oldest_ancester_time：
   *    - file_creation_time：
   *      a. 当前文件的创建时间
   *      b. 每次 flush 或 compaction 都会更新
   *      c. 反映文件的物理创建时间
   *      d. 用于周期性压缩判断
   *    - oldest_ancester_time：
   *      a. 数据的最初插入时间
   *      b. 在 compaction 期间保持不变
   *      c. 可以追溯到多个 compaction 之前
   *      d. 用于 TTL 压缩判断
   *    - 关键区别：
   *      file_creation_time 每次都变化，反映文件的新旧
   *      oldest_ancester_time 保持不变，反映数据的原始时间
   *    - 示例：
   *      - t=0: 文件 A 创建（flush）
   *               file_creation_time=0, oldest_ancester_time=0
   *      - t=10: 文件 A 被压缩，生成文件 B
   *               file_creation_time=10, oldest_ancester_time=0
   *      - t=20: 文件 B 被压缩，生成文件 C
   *               file_creation_time=20, oldest_ancester_time=0
   *      - 周期性压缩（30天）：
   *        使用 file_creation_time: 文件 C 年龄=5天，未过期
   *        使用 oldest_ancester_time: 数据年龄=20天，已过期
   *        正确的判断应该使用 file_creation_time
   *
   * 3. 为什么需要 file_creation_time：
   *    a. 周期性压缩：
   *       - 基于文件年龄触发压缩
   *       - 需要知道文件的实际创建时间
   *       - 确保文件定期被重写
   *    b. FIFO Compaction：
   *       - 按文件年龄顺序淘汰文件
   *       - 需要准确的创建时间进行排序
   *       - 确保最老的文件先被删除
   *    c. 性能分析：
   *       - 统计文件的年龄分布
   *       - 分析压缩策略的效果
   *       - 优化配置参数
   *
   * 4. 数据来源的优先级：
   *    a. 成员变量 file_creation_time：
   *       - 优先级最高
   *       - 已缓存在 FileMetaData 中
   *       - 读取速度快，无需额外 I/O
   *       - 在文件创建或加载时设置
   *    b. table_properties 中的 file_creation_time：
   *       - 优先级次之
   *       - 存储在 SSTable 文件的元数据中
   *       - 需要通过 table_reader 访问
   *       - 可能耗时（需要读取文件元数据）
   *    c. 如果都不可用，返回 kUnknownFileCreationTime（0）
   *
   * 5. 为什么检查 table_reader 和 GetTableProperties()：
   *    a. table_reader：
   *       - TableReader* 类型，指向打开的表读取器
   *       - 如果为 nullptr，说明文件未打开或已关闭
   *       - 只有打开的文件才能访问其 table properties
   *    b. GetTableProperties()：
   *       - 返回 SSTable 文件的属性（TableProperties*）
   *       - 包含 file_creation_time、num_entries、data_size 等
   *       - 如果为 nullptr，说明属性不可用
   *    c. 双重检查：
   *       - 确保 table_reader 不为空
   *       - 确保 GetTableProperties() 不为空
   *       - 避免空指针解引用导致的崩溃
   *
   * 6. table_reader 的生命周期和 pin 机制：
   *    - table_reader 可能被 cache 管理
   *    - 被 pin 的 table_reader 不会被 cache 淘汰
   *    - 函数注释中的 "pinned" 指的就是这种机制
   *    - 如果 table_reader 被 pin，说明它正在被使用
   *    - 可以安全地访问其 properties
   *
   * 应用场景：
   * 1. 周期性压缩（Periodic Compaction）：
   *    - ComputeFilesMarkedForPeriodicCompaction() 调用此函数
   *    - 判断文件是否超过周期性压缩间隔
   *    - 根据 file_creation_time 计算文件年龄
   *
   * 2. FIFO Compaction：
   *    - 按文件年龄顺序淘汰最老的文件
   *    - 使用 file_creation_time 进行排序
   *    - 确保先淘汰最老的文件
   *
   * 3. 性能监控：
   *    - 统计文件的年龄分布
   *    - 分析压缩策略的效果
   *    - 优化配置参数（如 periodic_compaction_seconds）
   *
   * 4. 调试和分析：
   *    - 追踪文件的生命周期
   *    - 分析文件流动和压缩行为
   *    - 帮助诊断性能问题
   *
   * 性能考虑：
   * 1. 优先使用成员变量：
   *    - 避免读取 table properties
   *    - 减少 I/O 操作
   *    - 提高查询速度
   *
   * 2. 缓存机制：
   *    - file_creation_time 被缓存在 FileMetaData 中
   *    - 文件创建或加载时设置
   *    - 后续查询无需 I/O
   *
   * 3. table_reader 检查：
   *    - 避免访问未打开的文件
   *    - 减少不必要的开销
   *    - 提高稳定性
   *
   * 注意事项：
   * 1. 返回值含义：
   *    - 返回 Unix 时间戳（秒）
   *    - 返回 0（kUnknownFileCreationTime）表示不可用
   *    - 调用者需要检查返回值是否为 0
   *
   * 2. 与 oldest_ancester_time 的区别：
   *    - file_creation_time：每次 flush/compaction 都更新
   *    - oldest_ancester_time：保持不变，追溯原始时间
   *    - TTL 使用 oldest_ancester_time
   *    - Periodic Compaction 使用 file_creation_time
   *
   * 3. table_reader 的可用性：
   *    - 只有当文件被打开时，table_reader 才有效
   *    - 文件关闭后，table_reader 可能为 nullptr
   *    - 需要处理这种情况
   *
   * 4. kUnknownFileCreationTime（0）的特殊含义：
   *    - 0 不是有效的时间戳
   *    - 表示时间信息不可用
   *    - 不应该用于周期性压缩计算
   *    - 调用者需要检查并跳过
   *
   * 5. 与其他压缩策略的配合：
   *    - 周期性压缩可以与 size-based compaction 配合
   *    - 优先级：size-based > periodic > marked
   *    - 确保多种压缩策略协同工作
   *
   * @return Unix 时间戳（秒），或 0（kUnknownFileCreationTime）表示不可用
   *
   * @note 此函数尝试从多个来源获取时间信息
   * @note 优先使用缓存的成员变量，避免 I/O
   * @note 返回 0 表示时间信息不可用
   * @see file_creation_time 文件创建时间成员变量
   * @see oldest_ancester_time 最祖先时间成员变量
   * @see TableProperties SSTable 文件属性
   * @see ComputeFilesMarkedForPeriodicCompaction() 使用此函数判断周期性压缩
   */
  uint64_t TryGetFileCreationTime() {
    // 优先级 1：检查成员变量 file_creation_time
    // 如果不等于 kUnknownFileCreationTime（0），说明已有缓存值
    // 直接返回缓存的值，无需进一步查询
    // 这种情况下，时间戳已经从 table properties 读取并缓存
    // 可以直接使用，避免重复读取
    if (file_creation_time != kUnknownFileCreationTime) {
      return file_creation_time;
    // 优先级 2：从 table properties 中读取 file_creation_time
    // 如果 table_reader 不为空且 GetTableProperties() 不为空
    // 说明文件已打开且 table properties 可用
    // 可以从中读取 file_creation_time
    // 注意：这里使用的是 file_creation_time 字段
    // 与 TryGetOldestAncesterTime() 使用 creation_time 字段不同
    // file_creation_time 反映当前文件的创建时间
    // 每次 flush 或 compaction 都会更新
    } else if (fd.table_reader != nullptr &&
               fd.table_reader->GetTableProperties() != nullptr) {
      // 从 table properties 中返回 file_creation_time
      // 这是 SSTable 文件中存储的文件创建时间
      // 反映当前文件被创建的 Unix 时间戳
      return fd.table_reader->GetTableProperties()->file_creation_time;
    }
    // 如果以上两个条件都不满足，返回 kUnknownFileCreationTime（0）
    // 说明：
    // 1. 成员变量 file_creation_time 为 0（未知）
    // 2. table_reader 为空（文件未打开）
    // 3. GetTableProperties() 为空（属性不可用）
    // 这种情况下，时间信息不可用
    // 调用者需要检查返回值是否为 0，并相应处理
    return kUnknownFileCreationTime;
  }

  // WARNING: manual update to this function is needed
  // whenever a new string property is added to FileMetaData
  // to reduce approximation error.
  //
  // TODO: eliminate the need of manually updating this function
  // for new string properties
  size_t ApproximateMemoryUsage() const {
    size_t usage = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    usage += malloc_usable_size(const_cast<FileMetaData*>(this));
#else
    usage += sizeof(*this);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    usage += smallest.size() + largest.size() + file_checksum.size() +
             file_checksum_func_name.size();
    return usage;
  }
};

// A compressed copy of file meta data that just contain minimum data needed
// to serve read operations, while still keeping the pointer to full metadata
// of the file in case it is needed.
struct FdWithKeyRange {
  FileDescriptor fd;
  FileMetaData* file_metadata;  // Point to all metadata
  Slice smallest_key;           // slice that contain smallest key
  Slice largest_key;            // slice that contain largest key

  FdWithKeyRange()
      : fd(), file_metadata(nullptr), smallest_key(), largest_key() {}

  FdWithKeyRange(FileDescriptor _fd, Slice _smallest_key, Slice _largest_key,
                 FileMetaData* _file_metadata)
      : fd(_fd),
        file_metadata(_file_metadata),
        smallest_key(_smallest_key),
        largest_key(_largest_key) {}
};

// Data structure to store an array of FdWithKeyRange in one level
// Actual data is guaranteed to be stored closely
struct LevelFilesBrief {
  size_t num_files;
  FdWithKeyRange* files;
  LevelFilesBrief() {
    num_files = 0;
    files = nullptr;
  }
};

// The state of a DB at any given time is referred to as a Version.
// Any modification to the Version is considered a Version Edit. A Version is
// constructed by joining a sequence of Version Edits. Version Edits are written
// to the MANIFEST file.
class VersionEdit {
 public:
  void Clear();

  void SetDBId(const std::string& db_id) {
    has_db_id_ = true;
    db_id_ = db_id;
  }
  bool HasDbId() const { return has_db_id_; }
  const std::string& GetDbId() const { return db_id_; }

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  bool HasComparatorName() const { return has_comparator_; }
  const std::string& GetComparatorName() const { return comparator_; }

  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  bool HasLogNumber() const { return has_log_number_; }
  uint64_t GetLogNumber() const { return log_number_; }

  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  bool HasPrevLogNumber() const { return has_prev_log_number_; }
  uint64_t GetPrevLogNumber() const { return prev_log_number_; }

  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  bool HasNextFile() const { return has_next_file_number_; }
  uint64_t GetNextFile() const { return next_file_number_; }

  void SetMaxColumnFamily(uint32_t max_column_family) {
    has_max_column_family_ = true;
    max_column_family_ = max_column_family;
  }
  bool HasMaxColumnFamily() const { return has_max_column_family_; }
  uint32_t GetMaxColumnFamily() const { return max_column_family_; }

  void SetMinLogNumberToKeep(uint64_t num) {
    has_min_log_number_to_keep_ = true;
    min_log_number_to_keep_ = num;
  }
  bool HasMinLogNumberToKeep() const { return has_min_log_number_to_keep_; }
  uint64_t GetMinLogNumberToKeep() const { return min_log_number_to_keep_; }

  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  bool HasLastSequence() const { return has_last_sequence_; }
  SequenceNumber GetLastSequence() const { return last_sequence_; }

  // Delete the specified table file from the specified level.
  void DeleteFile(int level, uint64_t file) {
    deleted_files_.emplace(level, file);
  }

  // Retrieve the table files deleted as well as their associated levels.
  using DeletedFiles = std::set<std::pair<int, uint64_t>>;
  const DeletedFiles& GetDeletedFiles() const { return deleted_files_; }

  // Add the specified table file at the specified level.
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  // REQUIRES: "oldest_blob_file_number" is the number of the oldest blob file
  // referred to by this file if any, kInvalidBlobFileNumber otherwise.
  void AddFile(int level, uint64_t file, uint32_t file_path_id,
               uint64_t file_size, const InternalKey& smallest,
               const InternalKey& largest, const SequenceNumber& smallest_seqno,
               const SequenceNumber& largest_seqno, bool marked_for_compaction,
               Temperature temperature, uint64_t oldest_blob_file_number,
               uint64_t oldest_ancester_time, uint64_t file_creation_time,
               uint64_t epoch_number, const std::string& file_checksum,
               const std::string& file_checksum_func_name,
               const UniqueId64x2& unique_id,
               const uint64_t compensated_range_deletion_size,
               uint64_t tail_size, bool user_defined_timestamps_persisted) {
    assert(smallest_seqno <= largest_seqno);
    new_files_.emplace_back(
        level,
        FileMetaData(file, file_path_id, file_size, smallest, largest,
                     smallest_seqno, largest_seqno, marked_for_compaction,
                     temperature, oldest_blob_file_number, oldest_ancester_time,
                     file_creation_time, epoch_number, file_checksum,
                     file_checksum_func_name, unique_id,
                     compensated_range_deletion_size, tail_size,
                     user_defined_timestamps_persisted));
    if (!HasLastSequence() || largest_seqno > GetLastSequence()) {
      SetLastSequence(largest_seqno);
    }
  }

  void AddFile(int level, const FileMetaData& f) {
    assert(f.fd.smallest_seqno <= f.fd.largest_seqno);
    new_files_.emplace_back(level, f);
    if (!HasLastSequence() || f.fd.largest_seqno > GetLastSequence()) {
      SetLastSequence(f.fd.largest_seqno);
    }
  }

  // Retrieve the table files added as well as their associated levels.
  using NewFiles = std::vector<std::pair<int, FileMetaData>>;
  const NewFiles& GetNewFiles() const { return new_files_; }

  NewFiles& GetMutableNewFiles() { return new_files_; }

  // Retrieve all the compact cursors
  using CompactCursors = std::vector<std::pair<int, InternalKey>>;
  const CompactCursors& GetCompactCursors() const { return compact_cursors_; }
  void AddCompactCursor(int level, const InternalKey& cursor) {
    compact_cursors_.push_back(std::make_pair(level, cursor));
  }
  void SetCompactCursors(
      const std::vector<InternalKey>& compact_cursors_by_level) {
    compact_cursors_.clear();
    compact_cursors_.reserve(compact_cursors_by_level.size());
    for (int i = 0; i < (int)compact_cursors_by_level.size(); i++) {
      if (compact_cursors_by_level[i].Valid()) {
        compact_cursors_.push_back(
            std::make_pair(i, compact_cursors_by_level[i]));
      }
    }
  }

  // Add a new blob file.
  void AddBlobFile(uint64_t blob_file_number, uint64_t total_blob_count,
                   uint64_t total_blob_bytes, std::string checksum_method,
                   std::string checksum_value) {
    blob_file_additions_.emplace_back(
        blob_file_number, total_blob_count, total_blob_bytes,
        std::move(checksum_method), std::move(checksum_value));
  }

  void AddBlobFile(BlobFileAddition blob_file_addition) {
    blob_file_additions_.emplace_back(std::move(blob_file_addition));
  }

  // Retrieve all the blob files added.
  using BlobFileAdditions = std::vector<BlobFileAddition>;
  const BlobFileAdditions& GetBlobFileAdditions() const {
    return blob_file_additions_;
  }

  void SetBlobFileAdditions(BlobFileAdditions blob_file_additions) {
    assert(blob_file_additions_.empty());
    blob_file_additions_ = std::move(blob_file_additions);
  }

  // Add garbage for an existing blob file.  Note: intentionally broken English
  // follows.
  void AddBlobFileGarbage(uint64_t blob_file_number,
                          uint64_t garbage_blob_count,
                          uint64_t garbage_blob_bytes) {
    blob_file_garbages_.emplace_back(blob_file_number, garbage_blob_count,
                                     garbage_blob_bytes);
  }

  void AddBlobFileGarbage(BlobFileGarbage blob_file_garbage) {
    blob_file_garbages_.emplace_back(std::move(blob_file_garbage));
  }

  // Retrieve all the blob file garbage added.
  using BlobFileGarbages = std::vector<BlobFileGarbage>;
  const BlobFileGarbages& GetBlobFileGarbages() const {
    return blob_file_garbages_;
  }

  void SetBlobFileGarbages(BlobFileGarbages blob_file_garbages) {
    assert(blob_file_garbages_.empty());
    blob_file_garbages_ = std::move(blob_file_garbages);
  }

  // Add a WAL (either just created or closed).
  // AddWal and DeleteWalsBefore cannot be called on the same VersionEdit.
  void AddWal(WalNumber number, WalMetadata metadata = WalMetadata()) {
    assert(NumEntries() == wal_additions_.size());
    wal_additions_.emplace_back(number, std::move(metadata));
  }

  // Retrieve all the added WALs.
  const WalAdditions& GetWalAdditions() const { return wal_additions_; }

  bool IsWalAddition() const { return !wal_additions_.empty(); }

  // Delete a WAL (either directly deleted or archived).
  // AddWal and DeleteWalsBefore cannot be called on the same VersionEdit.
  void DeleteWalsBefore(WalNumber number) {
    assert((NumEntries() == 1) == !wal_deletion_.IsEmpty());
    wal_deletion_ = WalDeletion(number);
  }

  const WalDeletion& GetWalDeletion() const { return wal_deletion_; }

  bool IsWalDeletion() const { return !wal_deletion_.IsEmpty(); }

  bool IsWalManipulation() const {
    size_t entries = NumEntries();
    return (entries > 0) && ((entries == wal_additions_.size()) ||
                             (entries == !wal_deletion_.IsEmpty()));
  }

  // Number of edits
  size_t NumEntries() const {
    return new_files_.size() + deleted_files_.size() +
           blob_file_additions_.size() + blob_file_garbages_.size() +
           wal_additions_.size() + !wal_deletion_.IsEmpty();
  }

  void SetColumnFamily(uint32_t column_family_id) {
    column_family_ = column_family_id;
  }
  uint32_t GetColumnFamily() const { return column_family_; }

  // set column family ID by calling SetColumnFamily()
  void AddColumnFamily(const std::string& name) {
    assert(!is_column_family_drop_);
    assert(!is_column_family_add_);
    assert(NumEntries() == 0);
    is_column_family_add_ = true;
    column_family_name_ = name;
  }

  // set column family ID by calling SetColumnFamily()
  void DropColumnFamily() {
    assert(!is_column_family_drop_);
    assert(!is_column_family_add_);
    assert(NumEntries() == 0);
    is_column_family_drop_ = true;
  }

  bool IsColumnFamilyManipulation() const {
    return is_column_family_add_ || is_column_family_drop_;
  }

  bool IsColumnFamilyAdd() const { return is_column_family_add_; }

  bool IsColumnFamilyDrop() const { return is_column_family_drop_; }

  void MarkAtomicGroup(uint32_t remaining_entries) {
    is_in_atomic_group_ = true;
    remaining_entries_ = remaining_entries;
  }
  bool IsInAtomicGroup() const { return is_in_atomic_group_; }
  uint32_t GetRemainingEntries() const { return remaining_entries_; }

  bool HasFullHistoryTsLow() const { return !full_history_ts_low_.empty(); }
  const std::string& GetFullHistoryTsLow() const {
    assert(HasFullHistoryTsLow());
    return full_history_ts_low_;
  }
  void SetFullHistoryTsLow(std::string full_history_ts_low) {
    assert(!full_history_ts_low.empty());
    full_history_ts_low_ = std::move(full_history_ts_low);
  }

  // return true on success.
  // `ts_sz` is the size in bytes for the user-defined timestamp contained in
  // a user key. This argument is optional because it's only required for
  // encoding a `VersionEdit` with new SST files to add. It's used to handle the
  // file boundaries: `smallest`, `largest` when
  // `FileMetaData.user_defined_timestamps_persisted` is false. When reading
  // the Manifest file, a mirroring change needed to handle
  // file boundaries are not added to the `VersionEdit.DecodeFrom` function
  // because timestamp size is not available at `VersionEdit` decoding time,
  // it's instead added to `VersionEditHandler::OnNonCfOperation`.
  bool EncodeTo(std::string* dst,
                std::optional<size_t> ts_sz = std::nullopt) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString(bool hex_key = false) const;
  std::string DebugJSON(int edit_num, bool hex_key = false) const;

 private:
  friend class ReactiveVersionSet;
  friend class VersionEditHandlerBase;
  friend class ListColumnFamiliesHandler;
  friend class VersionEditHandler;
  friend class VersionEditHandlerPointInTime;
  friend class DumpManifestHandler;
  friend class VersionSet;
  friend class Version;
  friend class AtomicGroupReadBuffer;

  bool GetLevel(Slice* input, int* level, const char** msg);

  const char* DecodeNewFile4From(Slice* input);

  // Encode file boundaries `FileMetaData.smallest` and `FileMetaData.largest`.
  // User-defined timestamps in the user key will be stripped if they shouldn't
  // be persisted.
  void EncodeFileBoundaries(std::string* dst, const FileMetaData& meta,
                            size_t ts_sz) const;

  int max_level_ = 0;
  std::string db_id_;
  std::string comparator_;
  uint64_t log_number_ = 0;
  uint64_t prev_log_number_ = 0;
  uint64_t next_file_number_ = 0;
  uint32_t max_column_family_ = 0;
  // The most recent WAL log number that is deleted
  uint64_t min_log_number_to_keep_ = 0;
  SequenceNumber last_sequence_ = 0;
  bool has_db_id_ = false;
  bool has_comparator_ = false;
  bool has_log_number_ = false;
  bool has_prev_log_number_ = false;
  bool has_next_file_number_ = false;
  bool has_max_column_family_ = false;
  bool has_min_log_number_to_keep_ = false;
  bool has_last_sequence_ = false;

  // Compaction cursors for round-robin compaction policy
  CompactCursors compact_cursors_;

  DeletedFiles deleted_files_;
  NewFiles new_files_;

  BlobFileAdditions blob_file_additions_;
  BlobFileGarbages blob_file_garbages_;

  WalAdditions wal_additions_;
  WalDeletion wal_deletion_;

  // Each version edit record should have column_family_ set
  // If it's not set, it is default (0)
  uint32_t column_family_ = 0;
  // a version edit can be either column_family add or
  // column_family drop. If it's column family add,
  // it also includes column family name.
  bool is_column_family_drop_ = false;
  bool is_column_family_add_ = false;
  std::string column_family_name_;

  bool is_in_atomic_group_ = false;
  uint32_t remaining_entries_ = 0;

  std::string full_history_ts_low_;
};

}  // namespace ROCKSDB_NAMESPACE
