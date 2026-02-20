// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/customizable.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/listener.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/types.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/version.h"
#include "rocksdb/write_buffer_manager.h"

#ifdef max
#undef max
#endif

namespace ROCKSDB_NAMESPACE {

class Cache;
class CompactionFilter;
class CompactionFilterFactory;
class Comparator;
class ConcurrentTaskLimiter;
class Env;
enum InfoLogLevel : unsigned char;
class SstFileManager;
class FilterPolicy;
class Logger;
class MergeOperator;
class Snapshot;
class MemTableRepFactory;
class RateLimiter;
class Slice;
class Statistics;
class InternalKeyComparator;
class WalFilter;
class FileSystem;

struct Options;
struct DbPath;

using FileTypeSet = SmallEnumSet<FileType, FileType::kBlobFile>;

struct ColumnFamilyOptions : public AdvancedColumnFamilyOptions {
  // The function recovers options to a previous version. Only 4.6 or later
  // versions are supported.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  ColumnFamilyOptions* OldDefaults(int rocksdb_major_version = 4,
                                   int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB
  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  // An optional cache object is passed in to be used as the block cache
  ColumnFamilyOptions* OptimizeForSmallDb(
      std::shared_ptr<Cache>* cache = nullptr);

  // Use this if you don't need to keep the data sorted, i.e. you'll never use
  // an iterator, only Put() and Get() API calls
  //
  ColumnFamilyOptions* OptimizeForPointLookup(uint64_t block_cache_size_mb);

  // Default values for some parameters in ColumnFamilyOptions are not
  // optimized for heavy workloads and big datasets, which means you might
  // observe write stalls under some conditions. As a starting point for tuning
  // RocksDB options, use the following two functions:
  // * OptimizeLevelStyleCompaction -- optimizes level style compaction
  // * OptimizeUniversalStyleCompaction -- optimizes universal style compaction
  // Universal style compaction is focused on reducing Write Amplification
  // Factor for big data sets, but increases Space Amplification. You can learn
  // more about the different styles here:
  // https://github.com/facebook/rocksdb/wiki/Rocksdb-Architecture-Guide
  // Make sure to also call IncreaseParallelism(), which will provide the
  // biggest performance gains.
  // Note: we might use more memory than memtable_memory_budget during high
  // write rate period
  ColumnFamilyOptions* OptimizeLevelStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);
  ColumnFamilyOptions* OptimizeUniversalStyleCompaction(
      uint64_t memtable_memory_budget = 512 * 1024 * 1024);

  // -------------------
  // Parameters that affect behavior
  // 影响行为的参数

  // 键比较器
  // 功能：定义表中键的排序顺序，决定键在 MemTable 和 SST 文件中的存储顺序
  // 默认值：BytewiseComparator()（按字节字典序比较）
  // 工作原理：
  //   - 确定键在 MemTable 中的顺序
  //   - 影响 SST 文件中键的排列
  //   - 影响迭代器的遍历方向和顺序
  //   - 影响范围查询和前缀查询的结果
  // 前提条件：
  //   - 必须与同一 DB 之前打开时使用的 comparator 有相同的名称和完全相同的排序规则
  //   - 如果更换 comparator，会导致数据不可读
  //   - Comparator 必须是线程安全的
  //   - 必须保证传递性、自反性等比较器性质
  // 使用场景：
  //   - BytewiseComparator（默认）：按字节字典序，适用于大多数场景
  //   - ReverseBytewiseComparator：反向字典序
  //   - 自定义 Comparator：需要特殊排序规则（如数字、日期、多字段排序）
  // 重要说明：
  //   - 比较器的顺序决定了 RocksDB 的一切数据组织方式
  //   - 一旦使用某个比较器创建 DB，后续打开必须使用相同的比较器
  //   - 自定义比较器需要谨慎实现，确保正确性
  const Comparator* comparator = BytewiseComparator();

  // REQUIRES: The client must provide a merge operator if Merge operation
  // needs to be accessed. Calling Merge on a DB without a merge operator
  // would result in Status::NotSupported. The client must ensure that the
  // merge operator supplied here has the same name and *exactly* the same
  // semantics as the merge operator provided to previous open calls on
  // the same DB. The only exception is reserved for upgrade, where a DB
  // previously without a merge operator is introduced to Merge operation
  // for the first time. It's necessary to specify a merge operator when
  // opening the DB in this case.
  // Default: nullptr
  // 合并操作符
  // 功能：定义如何合并同一键的多个值（用于 Merge() 操作）
  // 默认值：nullptr（不使用合并操作，调用 Merge() 会返回错误）
  // 工作原理：
  //   - 当调用 DB::Merge(key, value) 时，使用 merge_operator 合并新旧值
  //   - 适用于增量更新场景，避免 Read-Modify-Write 的开销
  //   - 合并操作是无序的，merge_operator 必须满足结合律和交换律
  // 前提条件：
  //   - 使用 Merge() 操作必须提供 merge_operator
  //   - 否则返回 Status::NotSupported
  //   - 必须与之前打开 DB 时使用的 merge_operator 具有相同的名称和完全相同的语义
  //   - 唯一的例外：升级场景，DB 之前没有 merge_operator，首次引入 Merge 操作
  // 使用场景：
  //   - 计数器：Merge(key, "delta") 实现计数器
  //   - 累加器：Merge(key, "addend_value") 实现累加
  //   - 集合操作：Merge(key, "new_item") 实现集合合并
  //   - 原子操作：避免读取-修改-写入的竞态条件
  // 示例代码：
  //   // 假设有一个计数器实现
  //   DB::Merge(key, "1")  // counter = 1
  //   DB::Merge(key, "2")  // counter = 3
  //   DB::Merge(key, "4")  // counter = 7
  //   DB::Get(key, &value)    // 读取结果："7"
  // 重要说明：
  //   - 合并操作是无序的，merge_operator 的实现必须是结合律和交换律的
  //   - 并发 Merge 可能导致顺序不确定，应用需要能处理这种情况
  //   - 实现时需要考虑性能，避免每次都读取完整值
  //   - RocksDB 回调不是异常安全的，回调中抛出异常会导致未定义行为
  std::shared_ptr<MergeOperator> merge_operator = nullptr;

  // A single CompactionFilter instance to call into during compaction.
  // Allows an application to modify/delete a key-value during background
  // compaction.
  //
  // If the client requires a new `CompactionFilter` to be used for different
  // compaction runs and/or requires a `CompactionFilter` for table file
  // creations outside of compaction, it can specify compaction_filter_factory
  // instead of this option.  The client should specify only one of the two.
  // compaction_filter takes precedence over compaction_filter_factory if
  // client specifies both.
  //
  // If multithreaded compaction is being used, the supplied CompactionFilter
  // instance may be used from different threads concurrently and so should be
  // thread-safe.
  //
  // Default: nullptr
  // 压缩过滤器（单实例）
  // 功能：允许应用在后台压缩过程中修改或删除键值对
  // 默认值：nullptr（不使用压缩过滤器）
  // 工作原理：
  //   - 在压缩过程中，对每个键值对调用 CompactionFilter
  //   - 可以根据条件决定保留、修改或删除数据
  //   - 过滤器返回 kKeep：保留该键值对
  //   - 过滤器返回 kRemove：删除该键值对
  //   - 过滤器返回 kChangeValue：修改该键值对的值
  //   - 过滤器返回 kRemoveAndSkipUntil：删除并跳过到某个键
  // 使用场景：
  //   - TTL 过期数据清理：删除过期的键值对
  //   - 数据生命周期管理：根据版本号、时间戳等清理旧数据
  //   - 数据脱敏：在压缩过程中修改敏感数据
  //   - 数据归档：将旧数据标记为已归档
  //   - 自定义清理策略：根据业务逻辑实现灵活的清理规则
  // 重要说明：
  //   - 单个实例，可能被多个压缩线程并发调用，必须是线程安全的
  //   - 与 compaction_filter_factory 只能指定一个，compaction_filter 优先级更高
  //   - 过滤器的执行会增加压缩开销，影响压缩性能
  //   - 过滤器返回 kRemove 会导致键值对从 SST 文件中删除，释放空间
  //   - 过滤器不能改变键，只能改变值或删除
  //   - 过滤器中的操作应该是确定性的，避免随机性
  // 线程安全：
  //   - 如果启用了多线程压缩，compaction_filter 可能被多个线程并发调用
  //   - 必须确保过滤器的实现是线程安全的
  //   - 或者使用 compaction_filter_factory，每个线程创建独立的过滤器实例
  // 性能影响：
  //   - 过滤器会为每个键值对调用，会影响压缩速度
  //   - 建议在过滤器内部做快速判断，减少复杂计算
  //   - 可以通过 bloom filter 等方式提前跳过不需要处理的键
  const CompactionFilter* compaction_filter = nullptr;

  // This is a factory that provides `CompactionFilter` objects which allow
  // an application to modify/delete a key-value during table file creation.
  //
  // Unlike the `compaction_filter` option, which is used when compaction
  // creates a table file, this factory allows using a `CompactionFilter` when a
  // table file is created for various reasons. The factory can decide what
  // `TableFileCreationReason`s use a `CompactionFilter`. For compatibility, by
  // default the decision is to use a `CompactionFilter` for
  // `TableFileCreationReason::kCompaction` only.
  //
  // Each thread of work involving creating table files will create a new
  // `CompactionFilter` when it will be used according to the above
  // `TableFileCreationReason`-based decision. This allows the application to
  // know about the different ongoing threads of work and makes it unnecessary
  // for `CompactionFilter` to provide thread-safety.
  //
  // Default: nullptr
  // 压缩过滤器工厂
  // 功能：为创建 SST 文件的线程提供独立的 CompactionFilter 实例
  // 默认值：nullptr（不使用压缩过滤器工厂）
  // 工作原理：
  //   - 工厂模式，为每个创建 SST 文件的线程创建新的 CompactionFilter 实例
  //   - 适用于多种表文件创建场景：压缩、Flush、恢复等
  //   - 工厂可以决定对哪些 TableFileCreationReason 使用过滤器
  //   - 默认只对 TableFileCreationReason::kCompaction 使用过滤器
  // 使用场景：
  //   - 多线程压缩：每个压缩线程有独立的过滤器实例，无需线程安全
  //   - 状态依赖的过滤：根据线程上下文或状态创建不同的过滤器
  //   - 多场景过滤：在 Flush、压缩、恢复等不同场景应用不同的过滤逻辑
  //   - 动态过滤策略：根据运行时条件决定是否使用过滤器
  // 重要说明：
  //   - 与 compaction_filter 只能指定一个，compaction_filter 优先级更高
  //   - 每个线程创建独立的过滤器实例，不需要实现线程安全
  //   - 工厂可以知道正在进行的不同线程工作
  //   - 可以根据 TableFileCreationReason 灵活控制过滤行为
  // 性能影响：
  //   - 创建过滤器实例有少量开销，但通常可以忽略
  //   - 避免了 compaction_filter 的线程同步开销
  //   - 可以在线程内部缓存状态，提高过滤效率
  // TableFileCreationReason 枚举：
  //   - kCompaction：压缩过程中创建 SST 文件
  //   - kFlush：MemTable Flush 创建 SST 文件
  //   - kRecovery：恢复过程中创建 SST 文件
  //   - kMisc：其他原因创建 SST 文件
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;

  // -------------------
  // Parameters that affect performance

  // Amount of data to build up in memory (backed by an unsorted log
  // on disk) before converting to a sorted on-disk file.
  //
  // Larger values increase performance, especially during bulk loads.
  // Up to max_write_buffer_number write buffers may be held in memory
  // at the same time,
  // so you may wish to adjust this parameter to control memory usage.
  // Also, a larger write buffer will result in a longer recovery time
  // the next time the database is opened.
  //
  // Note that write_buffer_size is enforced per column family.
  // See db_write_buffer_size for sharing memory across column families.
  //
  // Default: 64MB
  //
  // Dynamically changeable through SetOptions() API
  // MemTable 缓冲区大小
  // 功能：MemTable 在内存中积累的数据量达到此值后 Flush 到磁盘
  // 默认值：64 MB
  // 工作原理：
  //   - MemTable 是内存中的可写数据结构，接收所有写入操作
  //   - 当 MemTable 大小达到 write_buffer_size 时，会触发 Flush 操作
  //   - Flush 将 MemTable 写入不可变的 SST 文件，并创建新的 MemTable
  //   - 最多可以同时存在 max_write_buffer_number 个 MemTable
  // 内存计算：
  //   - 单个 CF 内存占用 = write_buffer_size * max_write_buffer_number
  //   - 多个 CF 的总内存 = sum(write_buffer_size * max_write_buffer_number for each CF)
  //   - 如果设置了 db_write_buffer_size，所有 CF 共享该内存预算
  // 性能影响：
  //   - 较大的值：减少 Flush 频率，降低写放大，但增加内存使用和恢复时间
  //   - 较小的值：降低内存使用，更快恢复，但增加 Flush 频率和写放大
  // 写入性能：
  //   - 批量加载：可以增大此值（如 256 MB - 1 GB）提高吞吐量
  //   - 低内存场景：减小此值（如 16 MB - 32 MB）降低内存压力
  // 恢复时间：
  //   - 较大的 MemTable 会增加 DB 打开时的恢复时间（需要重放 WAL）
  //   - 如果需要快速恢复，建议设置较小的值
  // 使用建议：
  //   - 写入密集型：256 MB - 512 MB
  //   - 内存受限：16 MB - 64 MB
  //   - 批量加载：512 MB - 1 GB
  //   - 小型数据库：8 MB - 16 MB
  // 重要说明：
  //   - 每个列族独立配置，不共享
  //   - 如果想跨列族共享内存，使用 db_write_buffer_size
  //   - 可以通过 SetOptions() 动态修改
  // 示例：
  //   write_buffer_size = 64 * 1024 * 1024  // 64 MB
  //   max_write_buffer_number = 3
  //   // 单个 CF 最多使用 64 * 3 = 192 MB 内存
  size_t write_buffer_size = 64 << 20;

  // Compress blocks using the specified compression algorithm.
  //
  // Default: kSnappyCompression, if it's supported. If snappy is not linked
  // with the library, the default is kNoCompression.
  //
  // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
  //    ~200-500MB/s compression
  //    ~400-800MB/s decompression
  //
  // Note that these speeds are significantly faster than most
  // persistent storage speeds, and therefore it is typically never
  // worth switching to kNoCompression.  Even if the input data is
  // incompressible, the kSnappyCompression implementation will
  // efficiently detect that and will switch to uncompressed mode.
  //
  // If you do not set `compression_opts.level`, or set it to
  // `CompressionOptions::kDefaultCompressionLevel`, we will attempt to pick the
  // default corresponding to `compression` as follows:
  //
  // - kZSTD: 3
  // - kZlibCompression: Z_DEFAULT_COMPRESSION (currently -1)
  // - kLZ4HCCompression: 0
  // - For all others, we do not specify a compression level
  //
  // Dynamically changeable through SetOptions() API
  // SST 文件压缩算法
  // 功能：指定 SST 文件的压缩算法
  // 默认值：kSnappyCompression（如果支持），否则 kNoCompression
  // 支持的压缩算法：
  //   - kNoCompression：不压缩
  //   - kSnappyCompression：Snappy（默认，平衡压缩比和速度）
  //   - kZlibCompression：Zlib（较高压缩比，较慢）
  //   - kBZip2Compression：BZip2（高压缩比，很慢）
  //   - kLZ4Compression：LZ4（快速，中等压缩比）
  //   - kLZ4HCCompression：LZ4HC（较慢，较高压缩比）
  //   - kXpressCompression：Xpress（Windows 快速压缩）
  //   - kZSTD：Zstandard（高压缩比，可调速度）
  // 压缩性能参考（Intel Core2 2.4GHz）：
  //   - Snappy：压缩 200-500 MB/s，解压 400-800 MB/s
  //   - LZ4：压缩 300-500 MB/s，解压 1-2 GB/s
  //   - ZSTD level 3：压缩 100-200 MB/s，解压 300-600 MB/s
  //   - Zlib：压缩 50-100 MB/s，解压 200-400 MB/s
  // 使用场景：
  //   - kSnappyCompression（默认）：大多数场景，平衡压缩比和速度
  //   - kLZ4Compression：对解压速度要求极高的场景
  //   - kZSTD：对压缩比要求高，且 CPU 资源充足的场景
  //   - kNoCompression：数据本身已压缩或不可压缩（如图片、视频）
  // 性能影响：
  //   - CPU：压缩消耗 CPU 资源，但通常远低于存储 I/O 速度
  //   - 存储：压缩减少存储空间，降低存储成本
  //   - I/O：压缩减少磁盘 I/O，提升读取性能
  //   - 带宽：压缩减少网络传输带宽
  // 重要说明：
  //   - 即使数据不可压缩，Snappy 也会高效检测并切换到非压缩模式
  //   - 压缩级别通过 compression_opts.level 配置
  //   - 默认压缩级别：
  //     - kZSTD: 3
  //     - kZlibCompression: Z_DEFAULT_COMPRESSION (-1)
  //     - kLZ4HCCompression: 0
  // 推荐配置：
  //   - CPU 充裕，存储受限：kZSTD 或 kZlibCompression
  //   - CPU 受限，存储充足：kSnappyCompression 或 kLZ4Compression
  //   - 已压缩数据：kNoCompression
  CompressionType compression;

  // Compression algorithm that will be used for the bottommost level that
  // contain files. The behavior for num_levels = 1 is not well defined.
  // Right now, with num_levels = 1,  all compaction outputs will use
  // bottommost_compression and all flush outputs still use options.compression,
  // but the behavior is subject to change.
  //
  // Default: kDisableCompressionOption (Disabled)
  // 最底层压缩算法
  // 功能：指定最底层（bottommost level）SST 文件的压缩算法
  // 默认值：kDisableCompressionOption（禁用，使用 compression）
  // 工作原理：
  //   - RocksDB 分层存储，最底层（通常是 L6）存储最老的数据
  //   - 最底层的数据不会被再次压缩（除非手动触发）
  //   - 使用更强的压缩算法可以显著减少存储空间
  //   - 禁用（kDisableCompressionOption）时，使用 compression 算法
  // num_levels = 1 的特殊情况：
  //   - 所有压缩输出都使用 bottommost_compression
  //   - 所有 Flush 输出仍使用 compression
  //   - 此行为可能在后续版本中改变
  // 使用场景：
  //   - 冷数据存储：最底层通常是冷数据，使用高压缩比算法（如 kZSTD）
  //   - 存储成本敏感：最底层占据大部分空间，压缩节省更多存储
  //   - 归档场景：对读取性能要求不高，优先考虑存储成本
  // 性能影响：
  //   - 存储：最底层占据约 90% 的数据空间，压缩可节省大量存储
  //   - 读取：最底层数据读取较少，压缩对整体读取性能影响较小
  //   - 压缩：只影响压缩最底层时的一次性能
  // 推荐配置：
  //   - Level Compaction：
  //     - CPU 充裕：kZSTD（压缩级别 3-17）
  //     - 平衡：kZlibCompression
  //     - 禁用：kDisableCompressionOption（使用 compression）
  //   - Universal Compaction：
  //     - 通常保持 kDisableCompressionOption
  // 示例：
  //   compression = kSnappyCompression           // L0-L5 使用 Snappy
  //   bottommost_compression = kZSTD            // L6 使用 ZSTD
  //   bottommost_compression_opts.level = 10     // ZSTD 压缩级别
  CompressionType bottommost_compression = kDisableCompressionOption;

  // different options for compression algorithms used by bottommost_compression
  // if it is enabled. To enable it, please see the definition of
  // CompressionOptions. Behavior for num_levels = 1 is the same as
  // options.bottommost_compression.
  CompressionOptions bottommost_compression_opts;

  // different options for compression algorithms
  CompressionOptions compression_opts;

  // Number of files to trigger level-0 compaction. A value <0 means that
  // level-0 compaction will not be triggered by number of files at all.
  //
  // Default: 4
  //
  // Dynamically changeable through SetOptions() API
  // Level 0 压缩触发阈值
  // 功能：L0 层文件数量达到此值时触发压缩
  // 默认值：4
  // 工作原理：
  //   - Level 0 层的文件是直接从 MemTable Flush 产生的，键范围可能重叠
  //   - 当 L0 文件数量达到此值时，触发 L0 → L1 的压缩
  //   - 压缩会将 L0 文件与 L1 文件合并，消除重叠范围
  //   - 值为负数时，禁用基于文件数量的压缩触发
  // 性能影响：
  //   - 较小的值（如 2-4）：
  //     - 优点：减少读取延迟，L0 文件少，读取需要检查的文件少
  //     - 缺点：增加压缩频率，增加写放大
  //   - 较大的值（如 8-16）：
  //     - 优点：降低压缩频率，减少写放大
  //     - 缺点：增加读取延迟，L0 文件多，读取需要检查多个文件
  // 使用场景：
  //   - 读取密集型：较小的值（2-4）
  //   - 写入密集型：较大的值（8-16）
  //   - 延迟敏感：较小的值（2）
  //   - 吞吐量优先：较大的值（10+）
  // 相关配置：
  //   - level0_slowdown_writes_trigger：写入降级阈值
  //   - level0_stop_writes_trigger：写入停止阈值
  //   - 通常配置为：trigger < slowdown < stop
  // 示例：
  //   level0_file_num_compaction_trigger = 4      // L0 文件数 >= 4 时压缩
  //   level0_slowdown_writes_trigger = 16         // L0 文件数 >= 16 时写入降级
  //   level0_stop_writes_trigger = 24             // L0 文件数 >= 24 时停止写入
  // 重要说明：
  //   - 只对 Level-style compaction 有效
  //   - Universal compaction 不使用此参数
  //   - 可以通过 SetOptions() 动态修改
  int level0_file_num_compaction_trigger = 4;

  // If non-nullptr, use the specified function to put keys in contiguous
  // groups called "prefixes". These prefixes are used to place one
  // representative entry for the group into the Bloom filter
  // rather than an entry for each key (see whole_key_filtering).
  // Under certain conditions, this enables optimizing some range queries
  // (Iterators) in addition to some point lookups (Get/MultiGet).
  //
  // Together `prefix_extractor` and `comparator` must satisfy one essential
  // property for valid prefix filtering of range queries:
  //   If Compare(k1, k2) <= 0 and Compare(k2, k3) <= 0 and
  //      InDomain(k1) and InDomain(k3) and prefix(k1) == prefix(k3),
  //   Then InDomain(k2) and prefix(k2) == prefix(k1)
  //
  // In other words, all keys with the same prefix must be in a contiguous
  // group by comparator order, and cannot be interrupted by keys with no
  // prefix ("out of domain"). (This makes it valid to conclude that no
  // entries within some bounds are present if the upper and lower bounds
  // have a common prefix and no entries with that same prefix are present.)
  //
  // Some other properties are recommended but not strictly required. Under
  // most sensible comparators, the following will need to hold true to
  // satisfy the essential property above:
  // * "Prefix is a prefix": key.starts_with(prefix(key))
  // * "Prefixes preserve ordering": If Compare(k1, k2) <= 0, then
  //   Compare(prefix(k1), prefix(k2)) <= 0
  //
  // The next two properties ensure that seeking to a prefix allows
  // enumerating all entries with that prefix:
  // * "Prefix starts the group": Compare(prefix(key), key) <= 0
  // * "Prefix idempotent": prefix(prefix(key)) == prefix(key)
  //
  // Default: nullptr
  // 前缀提取器
  // 功能：从键中提取前缀，用于前缀过滤和哈希索引优化
  // 默认值：nullptr（不使用前缀过滤）
  // 工作原理：
  //   - 为每个键调用 prefix_extractor 提取前缀
  //   - 在 Bloom filter 中存储前缀而非完整键，减少 filter 大小
  //   - 优化基于前缀的点查询（Get/MultiGet）
  //   - 优化某些范围查询（如果查询范围有共同前缀）
  // 前提条件：
  //   - 具有相同前缀的键必须连续存储（comparator 确保这一点）
  //   - 不能被无前缀的键隔断
  //   - 需满足：Compare(k1, k2) <= 0 且 Compare(k2, k3) <= 0 且
  //     InDomain(k1) 和 InDomain(k3) 且 prefix(k1) == prefix(k3)
  //     则 InDomain(k2) 且 prefix(k2) == prefix(k1)
  // 推荐属性（非必需）：
  //   - "前缀是前缀"：key.starts_with(prefix(key))
  //   - "前缀保持顺序"：如果 Compare(k1, k2) <= 0，则 Compare(prefix(k1), prefix(k2)) <= 0
  //   - "前缀开始分组"：Compare(prefix(key), key) <= 0
  //   - "前缀幂等"：prefix(prefix(key)) == prefix(key)
  // 使用场景：
  //   - 分区键：键格式为 {partition_id}_{sub_key}，提取 partition_id
  //   - 时间分区：键格式为 {timestamp}_{user_id}，提取 timestamp
  //   - 命名空间：键格式为 {namespace}:{key}，提取 namespace
  //   - 文件路径：键格式为 {path}/{filename}，提取 path
  // 性能影响：
  //   - Bloom filter：存储前缀而非完整键，减少 filter 大小
  //   - 点查询：如果查询键有前缀，可以快速跳过不匹配的文件
  //   - 范围查询：如果查询范围有共同前缀，可以优化
  //   - 哈希索引：可以使用 HashLookup 而非 BinarySearch
  // 内置前缀提取器：
  //   - NewFixedPrefixTransform(n)：提取前 n 个字节
  //   - NewCappedPrefixTransform(n)：提取前 n 个字节（如果长度足够）
  //   - NewNoopTransformTransform()：不提取前缀
  // 示例：
  //   // 键格式：user_123:name, user_456:age
  //   prefix_extractor = NewFixedPrefixTransform(5)  // 提取 "user_"
  //
  //   // 键格式：20240118:metric1, 20240118:metric2
  //   prefix_extractor = NewFixedPrefixTransform(8)  // 提取 "20240118"
  //
  //   // 查询优化
  //   // 如果知道查询是 prefix+"foo"，可以跳过前缀不同的文件
  // 重要说明：
  //   - 如果使用，必须在 comparator 中定义 InDomain() 方法
  //   - 不正确的前缀提取器可能导致查询结果不正确
  //   - 配合 whole_key_filtering 使用可以增强过滤能力
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;

  // Control maximum total data size for a level.
  // max_bytes_for_level_base is the max total for level-1.
  // Maximum number of bytes for level L can be calculated as
  // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
  // For example, if max_bytes_for_level_base is 200MB, and if
  // max_bytes_for_level_multiplier is 10, total data size for level-1
  // will be 200MB, total file size for level-2 will be 2GB,
  // and total file size for level-3 will be 20GB.
  //
  // Default: 256MB.
  //
  // Dynamically changeable through SetOptions() API
  // 各层大小基数（Level 1 的目标大小）
  // 功能：控制 Level 1 的目标数据量，其他层根据此基数和乘数计算
  // 默认值：256 MB
  // 工作原理：
  //   - max_bytes_for_level_base 是 Level 1 的目标大小
  //   - Level L 的大小计算公式：
  //     max_bytes_for_level_base * (max_bytes_for_level_multiplier ^ (L-1))
  //   - 例如：base = 200 MB, multiplier = 10
  //     - L1: 200 MB
  //     - L2: 2 GB
  //     - L3: 20 GB
  //     - L4: 200 GB
  //   - 压缩时会尝试将每一层的大小控制在目标值附近
  // 性能影响：
  //   - 较大的值：
  //     - 优点：减少层数，降低读放大，提高读取性能
  //     - 缺点：增加压缩成本，增加写放大，增加内存使用
  //   - 较小的值：
  //     - 优点：减少压缩成本，降低写放大
  //     - 缺点：增加层数，增加读放大，降低读取性能
  // 使用场景：
  //   - 读取密集型：较大的值（512 MB - 1 GB）
  //   - 写入密集型：较小的值（128 MB - 256 MB）
  //   - 大数据量：较大的值（1 GB - 2 GB）
  //   - 小数据量：较小的值（64 MB - 128 MB）
  // 计算示例：
  //   base = 256 MB, multiplier = 10, levels = 7
  //   L0: 不限制
  //   L1: 256 MB
  //   L2: 2.56 GB
  //   L3: 25.6 GB
  //   L4: 256 GB
  //   L5: 2.56 TB
  //   L6: 25.6 TB
  //   总容量约 28 TB
  // 相关配置：
  //   - max_bytes_for_level_multiplier：层级大小乘数（默认 10）
  //   - max_bytes_for_level_multiplier_additional：每层额外乘数
  //   - level_compaction_dynamic_level_bytes：动态调整层级大小
  // 重要说明：
  //   - 只影响 Level 1+，不影响 Level 0
  //   - 可以通过 SetOptions() 动态修改
  //   - 压缩不会立即将每一层调整到目标大小
  //   - 实际大小可能偏离目标值，取决于数据分布
  // 示例：
  //   // 中等规模，读取优先
  //   max_bytes_for_level_base = 512 * 1024 * 1024  // 512 MB
  //   max_bytes_for_level_multiplier = 10
  //
  //   // 大规模，写入优先
  //   max_bytes_for_level_base = 1024 * 1024 * 1024  // 1 GB
  //   max_bytes_for_level_multiplier = 8
  uint64_t max_bytes_for_level_base = 256 * 1048576;

  // Deprecated.
  uint64_t snap_refresh_nanos = 0;

  // Disable automatic compactions. Manual compactions can still
  // be issued on this column family
  //
  // Dynamically changeable through SetOptions() API
  // 禁用自动压缩
  // 功能：禁用后台自动压缩，只允许手动触发压缩
  // 默认值：false（启用自动压缩）
  // 工作原理：
  //   - true 时，不会自动触发压缩
  //   - 可以通过 CompactRange() 手动触发压缩
  //   - Flush 操作仍然正常进行
  //   - L0 文件会不断增长，直到手动压缩或空间耗尽
  // 使用场景：
  //   - 手动压缩控制：完全由应用控制压缩时机
  //   - 批量导入：先导入数据，再手动压缩
  //   - 特定场景：只在业务低峰期执行压缩
  //   - 调试测试：禁用压缩简化测试
  // 性能影响：
  //   - 禁用后：
  //     - 写入性能提升（不消耗 CPU 进行压缩）
  //     - 读取性能下降（需要检查更多文件）
  //     - 空间使用增加（未压缩的数据）
  //   - 启用后：
  //     - 自动清理，保持良好性能
  //     - 消耗 CPU 和 I/O 资源
  // 手动压缩：
  //   - db->CompactRange(options, nullptr, nullptr)  // 压缩整个 DB
  //   - db->CompactRange(options, start_key, end_key)  // 压缩指定范围
  //   - db->CompactFiles(...)  // 压缩指定文件
  // 重要说明：
  //   - 可以通过 SetOptions() 动态修改
  //   - 禁用后，L0 文件数会不断增长
  //   - L0 文件过多会导致读取性能下降
  //   - 最终可能导致磁盘空间不足
  // 风险：
  //   - 长期禁用可能导致：
  //     - 读取性能严重下降
  //     - 磁盘空间耗尽
  //     - L0 文件数超过限制导致写入停止
  // 推荐用法：
  //   - 批量导入：
  //     1. 设置 disable_auto_compactions = true
  //     2. 执行批量导入
  //     3. 调用 CompactRange 手动压缩
  //     4. 设置 disable_auto_compactions = false
  //   - 低峰期压缩：
  //     1. 高峰期：disable_auto_compactions = true
  //     2. 低峰期：调用 CompactRange
  //     3. 恢复：disable_auto_compactions = false
  bool disable_auto_compactions = false;

  // This is a factory that provides TableFactory objects.
  // Default: a block-based table factory that provides a default
  // implementation of TableBuilder and TableReader with default
  // BlockBasedTableOptions.
  // 表工厂
  // 功能：指定 SST 文件（Table）的格式和实现
  // 默认值：BlockBasedTableFactory（基于块的表格式）
  // 支持的表格式：
  //   - BlockBasedTable：默认格式，基于块的存储，支持多种索引和过滤
  //   - PlainTable：扁平表格式，适用于纯内存场景或 RAMFS
  //   - CuckooTable：布谷鸟哈希表格式，仅支持点查询
  // BlockBasedTable（默认）特性：
  //   - 数据分为固定大小的块（Block）
  //   - 支持多种索引类型：BinarySearch、HashSearch、TwoLevelIndex
  //   - 支持 Bloom filter、Ribbon filter 减少磁盘 I/O
  //   - 支持压缩：块级别、表级别、字典压缩
  //   - 适用于大多数场景
  // PlainTable 特性：
  //   - 扁平存储，无块结构
  //   - 仅支持纯内存文件系统（RAMFS）或 MemEnv
  //   - 适用于大量小文件、纯内存场景
  //   - 不支持压缩
  // CuckooTable 特性：
  //   - 基于布谷鸟哈希
  //   - 仅支持点查询，不支持范围查询
  //   - 适用于只读、纯点查询场景
  // 使用场景：
  //   - BlockBasedTable（默认）：大多数通用场景
  //   - PlainTable：纯内存、大量小键值对、不需要压缩
  //   - CuckooTable：只读、纯点查询、超低延迟要求
  // 配置示例：
  //   // BlockBasedTable 配置
  //   BlockBasedTableOptions table_options;
  //   table_options.block_cache = NewLRUCache(1 * 1024 * 1024 * 1024);  // 1 GB
  //   table_options.filter_policy = NewBloomFilterPolicy(10);
  //   table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  //   options.table_factory = NewBlockBasedTableFactory(table_options);
  //
  //   // PlainTable 配置
  //   PlainTableOptions table_options;
  //   table_options.user_key_len = 8;
  //   table_options.bloom_bits_per_key = 10;
  //   options.table_factory = NewPlainTableFactory(table_options);
  //
  //   // CuckooTable 配置
  //   CuckooTableOptions table_options;
  //   table_options.hash_table_ratio = 0.9;
  //   options.table_factory = NewCuckooTableFactory(table_options);
  // 重要说明：
  //   - 一旦 DB 创建，table_factory 不能更改（除非手动重新压缩）
  //   - 不同表格式之间不兼容，需要手动转换
  //   - BlockBasedTable 是最成熟和推荐的格式
  std::shared_ptr<TableFactory> table_factory;

  // A list of paths where SST files for this column family
  // can be put into, with its target size. Similar to db_paths,
  // newer data is placed into paths specified earlier in the
  // vector while older data gradually moves to paths specified
  // later in the vector.
  // Note that, if a path is supplied to multiple column
  // families, it would have files and total size from all
  // the column families combined. User should provision for the
  // total size(from all the column families) in such cases.
  //
  // If left empty, db_paths will be used.
  // Default: empty
  // 列族存储路径
  // 功能：指定此列族的 SST 文件存储路径，实现数据分层存储
  // 默认值：空（使用 db_paths）
  // 工作原理：
  //   - 新数据存储在路径列表前面的路径
  //   - 老数据通过压缩逐渐移动到路径列表后面的路径
  //   - 每个路径可以指定目标大小
  //   - 类似于 db_paths，但仅针对此列族
  // 分层存储策略：
  //   - 快速存储（SSD、NVMe）：存储新数据、热数据
  //   - 慢速存储（HDD、SATA）：存储老数据、冷数据
  //   - 成本优化：使用不同类型的存储降低成本
  // 路径格式：
  //   DbPath { string path, uint64_t target_size }
  //   - path：存储目录路径
  //   - target_size：该路径的目标大小
  // 使用场景：
  //   - SSD + HDD 混合存储：
  //     - 路径 0：/ssd/rocksdb (100 GB)  // 新数据
  //     - 路径 1：/hdd/rocksdb (1 TB)    // 老数据
  //   - NVMe + SATA 混合存储：
  //     - 路径 0：/nvme/hot (200 GB)      // 热数据
  //     - 路径 1：/sata/warm (2 TB)      // 温数据
  //     - 路径 2：/hdd/cold (10 TB)      // 冷数据
  //   - 多 SSD 负载均衡：
  //     - 路径 0：/ssd1/rocksdb (200 GB)
  //     - 路径 1：/ssd2/rocksdb (200 GB)
  //     - 路径 2：/ssd3/rocksdb (200 GB)
  // 示例：
  //   // SSD + HDD 混合存储
  //   std::vector<DbPath> cf_paths;
  //   cf_paths.push_back(DbPath("/ssd/rocksdb", 100 * 1024 * 1024 * 1024));  // 100 GB
  //   cf_paths.push_back(DbPath("/hdd/rocksdb", 1024 * 1024 * 1024 * 1024)); // 1 TB
  //   options.cf_paths = cf_paths;
  // 重要说明：
  //   - 如果为空，使用 db_paths（DB 级别配置）
  //   - 多个列族共享同一路径时，文件和大小会累加
  //   - 需要为所有列族预留足够的存储空间
  //   - 路径必须存在且有写权限
  //   - 压缩时会将文件从前面的路径移动到后面的路径
  // 性能影响：
  //   - 新数据在快速存储上：写入和读取性能更好
  //   - 老数据在慢速存储上：访问性能下降，但存储成本更低
  //   - 压缩会触发跨路径的文件移动，有一定开销
  // 注意事项：
  //   - 确保 target_size 配置合理，避免某个路径过早填满
  //   - 监控每个路径的使用情况，及时调整
  //   - 不同列族的 cf_paths 可以不同，实现差异化存储
  std::vector<DbPath> cf_paths;

  // Compaction concurrent thread limiter for the column family.
  // If non-nullptr, use given concurrent thread limiter to control
  // the max outstanding compaction tasks. Limiter can be shared with
  // multiple column families across db instances.
  //
  // Default: nullptr
  // 压缩线程限制器
  // 功能：限制列族的最大并发压缩任务数量
  // 默认值：nullptr（不限制）
  // 工作原理：
  //   - 限制同时进行的压缩任务数量
  //   - 可以在多个列族或 DB 实例间共享
  //   - 当达到限制时，新的压缩任务需要等待
  //   - 适用于控制 CPU 和 I/O 资源使用
  // 使用场景：
  //   - CPU 受限：限制压缩线程数，避免影响主业务
  //   - I/O 受限：减少并发 I/O，避免磁盘瓶颈
  //   - 多租户：为不同租户的列族分配不同资源
  //   - 资源隔离：控制压缩对其他操作的影响
  // 资源计算：
  //   - 假设 max_background_compactions = 4
  //   - compaction_thread_limiter 限制为 2
  //   - 最多同时有 2 个压缩任务在执行
  //   - 另外 2 个压缩线程需要等待
  // 共享限制器：
  //   - 多个列族共享同一个限制器
  //   - 限制所有列族的压缩任务总数
  //   - 例如：3 个列族共享限制器 4，总共最多 4 个压缩任务
  // 示例：
  //   // 创建限制器，最多 4 个并发压缩任务
  //   auto limiter = NewConcurrentTaskLimiter("compaction_limiter", 4);
  //
  //   // 为多个列族设置相同的限制器
  //   cf_options_1.compaction_thread_limiter = limiter;
  //   cf_options_2.compaction_thread_limiter = limiter;
  //   cf_options_3.compaction_thread_limiter = limiter;
  //   // 三个列族的压缩任务总数最多 4 个
  //
  //   // 单独列族限制
  //   auto limiter_cf1 = NewConcurrentTaskLimiter("cf1", 2);
  //   auto limiter_cf2 = NewConcurrentTaskLimiter("cf2", 4);
  //   cf_options_1.compaction_thread_limiter = limiter_cf1;  // 最多 2 个
  //   cf_options_2.compaction_thread_limiter = limiter_cf2;  // 最多 4 个
  // 性能影响：
  //   - 减少并发压缩任务：
  //     - 优点：降低 CPU 和 I/O 压力，避免影响主业务
  //     - 缺点：压缩速度变慢，可能导致 L0 文件堆积
  //   - 增加并发压缩任务：
  //     - 优点：压缩速度快，及时清理数据
  //     - 缺点：消耗更多 CPU 和 I/O，可能影响其他操作
  // 相关配置：
  //   - max_background_compactions：后台压缩线程池大小
  //   - max_subcompactions：每个压缩任务的子任务数
  //   - 这两个参数控制线程池大小，此参数控制实际并发数
  // 重要说明：
  //   - nullptr 表示不限制，受 max_background_compactions 控制
  //   - 限制值应该 <= max_background_compactions
  //   - 可以动态调整限制值
  //   - 共享限制器时，所有列族的压缩任务共享配额
  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter = nullptr;

  // If non-nullptr, use the specified factory for a function to determine the
  // partitioning of sst files. This helps compaction to split the files
  // on interesting boundaries (key prefixes) to make propagation of sst
  // files less write amplifying (covering the whole key space).
  // THE FEATURE IS STILL EXPERIMENTAL
  //
  // Default: nullptr
  // SST 文件分割器工厂（实验性功能）
  // 功能：根据键前缀分割 SST 文件，减少压缩时的写放大
  // 默认值：nullptr（不使用分割器）
  // 工作原理：
  //   - 在压缩时，根据键前缀将 SST 文件分割成多个文件
  //   - 避免单个 SST 文件跨越整个键空间
  //   - 下次压缩时，可以只选择部分文件，减少写放大
  //   - 类似于分区索引的概念
  // 使用场景：
  //   - 键空间分布不均匀：某些键范围写入频繁，其他范围很少写入
  //   - 压缩优化：减少需要重写的数据量
  //   - 增量更新：只更新特定键范围的数据
  // 性能影响：
  //   - 优点：
  //     - 减少压缩时的写放大
  //     - 只重写需要更新的文件
  //     - 提高压缩效率
  //   - 缺点：
  //     - 增加文件数量
  //     - 增加元数据管理开销
  //     - 可能影响读取性能（需要检查更多文件）
  // 示例：
  //   // 使用固定前缀分割
  //   auto partitioner = NewSstPartitionerFixedPrefixFactory(4);  // 前 4 字节
  //   options.sst_partitioner_factory = partitioner;
  //
  //   // 使用自定义分割器
  //   auto partitioner = std::make_shared<MySstPartitionerFactory>();
  //   options.sst_partitioner_factory = partitioner;
  // 重要说明：
  //   - 实验性功能，可能不稳定
  //   - 只适用于 Level-style compaction
  //   - 需要仔细调优，可能不适合所有场景
  //   - 分割粒度（前缀长度）需要根据数据特征选择
  // 推荐用法：
  //   - 键有明显的前缀结构（如分区 ID、时间戳）
  //   - 键空间分布不均匀
  //   - 写入集中在特定键范围
  // 风险：
  //   - 可能增加文件数量
  //   - 可能增加元数据开销
  //   - 需要仔细测试和调优
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // Create ColumnFamilyOptions with default values for all fields
  ColumnFamilyOptions();
  // Create ColumnFamilyOptions from Options
  explicit ColumnFamilyOptions(const Options& options);

  void Dump(Logger* log) const;
};

enum class WALRecoveryMode : char {
  // Original levelDB recovery
  //
  // We tolerate the last record in any log to be incomplete due to a crash
  // while writing it. Zeroed bytes from preallocation are also tolerated in the
  // trailing data of any log.
  //
  // Use case: Applications for which updates, once applied, must not be rolled
  // back even after a crash-recovery. In this recovery mode, RocksDB guarantees
  // this as long as `WritableFile::Append()` writes are durable. In case the
  // user needs the guarantee in more situations (e.g., when
  // `WritableFile::Append()` writes to page cache, but the user desires this
  // guarantee in face of power-loss crash-recovery), RocksDB offers various
  // mechanisms to additionally invoke `WritableFile::Sync()` in order to
  // strengthen the guarantee.
  //
  // This differs from `kPointInTimeRecovery` in that, in case a corruption is
  // detected during recovery, this mode will refuse to open the DB. Whereas,
  // `kPointInTimeRecovery` will stop recovery just before the corruption since
  // that is a valid point-in-time to which to recover.
  kTolerateCorruptedTailRecords = 0x00,
  // Recover from clean shutdown
  // We don't expect to find any corruption in the WAL
  // Use case : This is ideal for unit tests and rare applications that
  // can require high consistency guarantee
  kAbsoluteConsistency = 0x01,
  // Recover to point-in-time consistency (default)
  // We stop the WAL playback on discovering WAL inconsistency
  // Use case : Ideal for systems that have disk controller cache like
  // hard disk, SSD without super capacitor that store related data
  kPointInTimeRecovery = 0x02,
  // Recovery after a disaster
  // We ignore any corruption in the WAL and try to salvage as much data as
  // possible
  // Use case : Ideal for last ditch effort to recover data or systems that
  // operate with low grade unrelated data
  kSkipAnyCorruptedRecords = 0x03,
};

struct DbPath {
  std::string path;
  uint64_t target_size;  // Target size of total files under the path, in byte.

  DbPath() : target_size(0) {}
  DbPath(const std::string& p, uint64_t t) : path(p), target_size(t) {}
};

extern const char* kHostnameForDbHostId;

enum class CompactionServiceJobStatus : char {
  kSuccess,
  kFailure,
  kUseLocal,
};

struct CompactionServiceJobInfo {
  std::string db_name;
  std::string db_id;
  std::string db_session_id;
  uint64_t job_id;  // job_id is only unique within the current DB and session,
                    // restart DB will reset the job_id. `db_id` and
                    // `db_session_id` could help you build unique id across
                    // different DBs and sessions.

  Env::Priority priority;

  CompactionServiceJobInfo(std::string db_name_, std::string db_id_,
                           std::string db_session_id_, uint64_t job_id_,
                           Env::Priority priority_)
      : db_name(std::move(db_name_)),
        db_id(std::move(db_id_)),
        db_session_id(std::move(db_session_id_)),
        job_id(job_id_),
        priority(priority_) {}
};

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionService : public Customizable {
 public:
  static const char* Type() { return "CompactionService"; }

  // Returns the name of this compaction service.
  const char* Name() const override = 0;

  // Start the remote compaction with `compaction_service_input`, which can be
  // passed to `DB::OpenAndCompact()` on the remote side. `info` provides the
  // information the user might want to know, which includes `job_id`.
  virtual CompactionServiceJobStatus StartV2(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& /*compaction_service_input*/) {
    return CompactionServiceJobStatus::kUseLocal;
  }

  // Wait for remote compaction to finish.
  virtual CompactionServiceJobStatus WaitForCompleteV2(
      const CompactionServiceJobInfo& /*info*/,
      std::string* /*compaction_service_result*/) {
    return CompactionServiceJobStatus::kUseLocal;
  }

  ~CompactionService() override = default;
};

struct DBOptions {
  // The function recovers options to the option as in version 4.6.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  DBOptions* OldDefaults(int rocksdb_major_version = 4,
                         int rocksdb_minor_version = 6);

  // Some functions that make it easier to optimize RocksDB

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  // An optional cache object is passed in for the memory of the
  // memtable to cost to
  DBOptions* OptimizeForSmallDb(std::shared_ptr<Cache>* cache = nullptr);

  // By default, RocksDB uses only one background thread for flush and
  // compaction. Calling this function will set it up such that total of
  // `total_threads` is used. Good value for `total_threads` is the number of
  // cores. You almost definitely want to call this function if your system is
  // bottlenecked by RocksDB.
  DBOptions* IncreaseParallelism(int total_threads = 16);

  // 如果为true，当数据库不存在时将创建数据库
  // 功能：控制打开数据库时是否自动创建数据库
  // 默认值：false
  // 使用场景：
  //   - 首次创建数据库
  //   - 自动化部署
  //   - 测试环境
  // 重要说明：设置为false时，如果数据库不存在会返回错误
  bool create_if_missing = false;

  // 如果为true，缺失的列族将自动创建
  // 功能：控制打开数据库时是否自动创建缺失的列族
  // 默认值：false
  // 使用场景：
  //   - 动态添加列族
  //   - 简化列族管理
  //   - 版本升级时自动迁移
  // 重要说明：需要配合create_if_missing使用
  bool create_missing_column_families = false;

  // 如果为true，当数据库已存在时抛出错误
  // 功能：防止意外覆盖已存在的数据库
  // 默认值：false
  // 使用场景：
  //   - 防止数据覆盖
  //   - 安全检查
  //   - 确保数据库唯一性
  // 重要说明：用于保护现有数据
  bool error_if_exists = false;

  // 如果为true，RocksDB将积极检查数据一致性
  // 功能：启用严格的数据完整性检查
  // 默认值：true
  // 使用场景：
  //   - 生产环境数据保护
  //   - 数据完整性要求高
  //   - 调试和诊断
  // 重要说明：
  //   - 如果任何写入失败，数据库将切换到只读模式
  //   - 可能影响性能
  //   - 建议在生产环境启用
  bool paranoid_checks = true;

  // 如果为true，在memtable flush期间验证条目计数
  // 功能：验证flush时读取的总条目数与插入的计数器是否匹配
  // 默认值：true
  // 使用场景：
  //   - 数据完整性检查
  //   - 调试flush问题
  // 重要说明：
  //   - 用于关闭新验证功能，防止bug影响
  //   - 通常建议启用
  bool flush_verify_memtable_count = true;

  // 如果为true，在MANIFEST中跟踪已同步WAL的日志号和大小
  // 功能：在MANIFEST中记录WAL的元数据，恢复时进行验证
  // 默认值：false
  // 使用场景：
  //   - 防止WAL损坏
  //   - 增强数据恢复能力
  // 重要说明：
  //   - 这是除了per-WAL-entry校验之外的额外保护
  //   - 不支持secondary instance
  //   - 只跟踪已关闭的WAL
  //   - DB::SyncWAL()不会被跟踪
  bool track_and_verify_wals_in_manifest = false;

  // 如果为true，每次打开SST文件时验证MANIFEST和实际文件的唯一ID
  // 功能：确保SST文件没有被覆盖或放错位置
  // 默认值：true
  // 使用场景：
  //   - 文件完整性验证
  //   - 防止文件被误操作
  // 重要说明：
  //   - 只适用于block-based table格式
  //   - 仅当MANIFEST跟踪唯一ID时才有效（从7.3版本开始）
  //   - 设置为false仅用于解决意外问题
  //   - 当max_open_files为-1时，DB::Open()会打开所有文件
  bool verify_sst_unique_id_in_manifest = true;

  // 环境对象，用于与系统环境交互
  // 功能：指定用于文件读写、后台任务调度等的环境对象
  // 默认值：Env::Default()
  // 使用场景：
  //   - 自定义文件系统操作
  //   - 调度和执行后台任务
  //   - 测试环境模拟
  // 重要说明：
  //   - 未来将通过file_system替代env进行存储操作
  //   - 可以自定义实现
  Env* env = Env::Default();

  // 限制内部文件读/写带宽的速率限制器
  // 功能：控制不同操作类型的I/O带宽使用
  // 默认值：nullptr（禁用）
  // 带宽分配：
  //   - Flush请求使用IO_HIGH优先级
  //   - Compaction请求使用IO_LOW优先级（读和写）
  //   - ReadOptions相关读取可以使用ReadOptions::rate_limiter_priority
  //   - WriteOptions相关写入可以使用WriteOptions::rate_limiter_priority
  // 使用场景：
  //   - 控制I/O资源使用
  //   - 防止后台任务影响前台性能
  //   - 多租户环境资源隔离
  // 重要说明：
  //   - 为nullptr时禁用限流
  //   - 启用时，bytes_per_sync默认设置为1MB
  std::shared_ptr<RateLimiter> rate_limiter = nullptr;

  // SST文件管理器，用于跟踪和控制SST文件的删除
  // 功能：管理和监控SST文件的生命周期
  // 默认值：nullptr（禁用）
  // 特性：
  //   - 限制SST文件的删除速率
  //   - 跟踪所有SST文件的总大小
  //   - 设置SST文件的最大空间限制，超过时停止flush和compaction并设置后台错误
  //   - 可在多个数据库实例间共享
  // 限制：
  //   - 只跟踪和限制第一个db_path中的SST文件删除
  // 使用场景：
  //   - 控制磁盘空间使用
  //   - 防止磁盘被填满
  //   - 管理多个数据库的存储
  std::shared_ptr<SstFileManager> sst_file_manager = nullptr;

  // 日志记录器，用于记录数据库的进度和错误信息
  // 功能：输出数据库的内部进度和错误信息
  // 默认值：nullptr（使用数据库目录下的文件）
  // 使用场景：
  //   - 监控数据库运行状态
  //   - 调试和诊断问题
  //   - 审计和日志分析
  // 重要说明：
  //   - 为nullptr时，日志写入到数据库目录下的文件
  //   - 可以自定义实现以输出到其他目标
  std::shared_ptr<Logger> info_log = nullptr;

  // 日志级别
  // 功能：控制日志输出的详细程度
  // 默认值：
  //   - Release模式（NDEBUG）：INFO_LEVEL
  //   - Debug模式：DEBUG_LEVEL
  // 使用场景：
  //   - 生产环境使用INFO_LEVEL
  //   - 开发和调试使用DEBUG_LEVEL
  //   - 性能敏感场景使用WARN_LEVEL
#ifdef NDEBUG
  InfoLogLevel info_log_level = INFO_LEVEL;
#else
  InfoLogLevel info_log_level = DEBUG_LEVEL;
#endif  // NDEBUG

  // 数据库可以同时打开的最大文件数
  // 功能：限制同时打开的文件数量
  // 默认值：-1（不限制）
  // 使用场景：
  //   - 控制文件描述符使用
  //   - 在文件描述符有限的环境下使用
  //   - 大工作集数据库
  // 重要说明：
  //   - -1表示文件一直保持打开
  //   - 可根据target_file_size_base和target_file_size_multiplier估算文件数
  //   - Universal compaction通常设置为-1
  //   - 高值或-1会导致高内存使用
  //   - 可通过SetDBOptions()动态修改
  int max_open_files = -1;

  // 当max_open_files为-1时，DB会在打开时打开所有文件。此选项用于增加打开文件的线程数
  // 功能：控制并发打开文件的线程数量
  // 默认值：16
  // 使用场景：
  //   - 加速数据库打开
  //   - 大量SST文件的数据库
  //   - 并行I/O优化
  // 重要说明：仅当max_open_files为-1时生效
  int max_file_opening_threads = 16;

  // WAL总大小限制，超过此限制时强制flush相关列族
  // 功能：控制WAL文件的总大小
  // 默认值：0（动态计算）
  // 计算方式（设置为0时）：[sum of all write_buffer_size * max_write_buffer_number] * 4
  // 示例：
  //   15个列族，每个write_buffer_size = 128MB, max_write_buffer_number = 6
  //   max_total_wal_size = [15 * 128MB * 6] * 4 = 45GB
  // 使用场景：
  //   - 控制WAL占用磁盘空间
  //   - 强制flush oldest WAL
  //   - 多列族环境
  // 重要说明：
  //   - 只在多列族时生效
  //   - 单列族时WAL大小由write_buffer_size决定
  //   - 会触发支持oldest WAL的列族flush
  //   - 可通过SetDBOptions()动态修改
  uint64_t max_total_wal_size = 0;

  // 统计信息收集器
  // 功能：收集数据库操作的各种指标
  // 默认值：nullptr（禁用）
  // 使用场景：
  //   - 性能监控和分析
  //   - 诊断性能问题
  //   - 容量规划
  // 重要说明：
  //   - 收集的性能指标可用于优化
  //   - 会有一定的性能开销
  std::shared_ptr<Statistics> statistics = nullptr;

  // 是否使用fsync而不是fdatasync来写入稳定存储
  // 功能：控制数据持久化使用的同步方法
  // 默认值：false（使用fdatasync）
  // 使用场景：
  //   - 解决特定内核/文件系统bug
  //   - 特殊存储设备要求
  // 重要说明：
  //   - fdatasync更快且同等安全
  //   - 仅用于解决bug，如ext4在3.7内核之前的问题
  //   - 通常不需要设置
  bool use_fsync = false;

  // 数据库路径列表，指定SST文件存放的位置和目标大小
  // 功能：实现数据分层存储，新数据放在前面的路径，旧数据逐渐移动到后面的路径
  // 默认值：空（只使用db_name指定的路径）
  // 示例：
  //   flash设备10GB + 硬盘2TB：[{"/flash_path", 10GB}, {"/hard_drive", 2TB}]
  // 使用场景：
  //   - 分层存储（热数据和冷数据）
  //   - SSD + HDD混合存储
  //   - 多磁盘存储
  // 重要说明：
  //   - 尝试保证每个路径的数据接近但不大于目标大小
  //   - 基于估算，实际大小可能略超目标
  //   - 用户应预留缓冲空间
  //   - 如果所有路径都不够，文件会放在最后一个路径
  //   - 新数据放在前面的路径是尽力而为
  std::vector<DbPath> db_paths;

  // 日志文件目录
  // 功能：指定日志文件的存放目录
  // 默认值：""（与数据库数据目录相同）
  // 使用场景：
  //   - 分离日志和数据
  //   - 日志集中管理
  //   - 磁盘空间优化
  // 重要说明：
  //   - 为空时，日志文件在数据库目录下
  //   - 非空时，使用数据库目录绝对路径作为日志文件名前缀
  std::string db_log_dir = "";

  // WAL（预写日志）目录
  // 功能：指定WAL文件的绝对路径
  // 默认值：""（与数据库数据目录相同）
  // 使用场景：
  //   - 分离WAL和数据
  //   - WAL放在独立磁盘
  //   - 提高I/O性能
  // 重要说明：
  //   - 为空时，WAL文件在数据库目录下（dbname）
  //   - 销毁数据库时，wal_dir目录及其内容会被删除
  std::string wal_dir = "";

  // 删除过期文件的周期
  // 功能：后台线程定期删除过期文件的间隔时间
  // 默认值：6小时
  // 单位：微秒
  // 使用场景：
  //   - 定期清理不再需要的SST文件和WAL文件
  //   - 控制后台清理频率
  //   - 平衡磁盘空间和清理开销
  // 重要说明：
  //   - 较小值会频繁执行清理，影响性能
  //   - 较大值会导致磁盘空间不能及时释放
  //   - compaction产生的过期文件会在每次compaction时自动删除
  //   - 可通过SetDBOptions()动态修改
  uint64_t delete_obsolete_files_period_micros = 6ULL * 60 * 60 * 1000000;

  // 最大后台任务数（包括compaction和flush）
  // 功能：限制后台线程池中同时执行的任务总数
  // 默认值：2
  // 使用场景：
  //   - 控制后台并发度
  //   - 平衡读写性能和后台处理
  //   - 资源限制
  // 重要说明：
  //   - 包括flush和compaction任务
  //   - 增大值可加快后台处理但增加I/O竞争
  //   - 可通过SetDBOptions()动态修改
  int max_background_jobs = 2;

  // 最大后台压缩任务数（已弃用）
  // 功能：限制同时运行的compaction任务数量
  // 默认值：-1（由max_background_jobs控制）
  // 使用场景：
  //   - 控制compaction资源占用
  //   - 防止compaction影响前台读写
  // 重要说明：
  //   - 已弃用，RocksDB根据max_background_jobs自动决定
  //   - 向后兼容：max_background_jobs = max_background_compactions + max_background_flushes
  //   - 增加此值时应增加LOW优先级线程池线程数
  //   - 可通过SetDBOptions()动态修改
  int max_background_compactions = -1;

  // 每个压缩任务的最大子压缩数
  // 功能：将单个compaction任务拆分为多个并行执行的子任务
  // 默认值：1（不使用子压缩）
  // 使用场景：
  //   - 在多核CPU上加速compaction
  //   - 充分利用多核处理能力
  //   - 提高大compaction的并行度
  // 重要说明：
  //   - 增大值会增加CPU和内存使用
  //   - 对于小compaction可能没有帮助
  //   - 建议根据CPU核心数调整
  //   - 可通过SetDBOptions()动态修改
  uint32_t max_subcompactions = 1;

  // 最大后台flush任务数（已弃用）
  // 功能：限制同时运行的flush任务数量
  // 默认值：-1（由max_background_jobs控制）
  // 使用场景：
  //   - 控制flush资源占用
  //   - 防止flush影响读写性能
  // 重要说明：
  //   - 已弃用，RocksDB根据max_background_jobs自动决定
  //   - 向后兼容：max_background_jobs = max_background_compactions + max_background_flushes
  //   - 默认提交到HIGH优先级线程池
  //   - HIGH优先级线程池为0时，与compaction共享LOW优先级线程池
  //   - 多DB共享Env时很重要，防止compaction阻塞其他DB的flush
  //   - 增加此值时应增加HIGH优先级线程池线程数
  int max_background_flushes = -1;

  // 日志文件最大大小
  // 功能：控制单个日志文件的最大尺寸
  // 默认值：0（不限制）
  // 使用场景：
  //   - 控制单个日志文件大小
  //   - 日志轮转管理
  //   - 便于日志归档
  // 重要说明：
  //   - 0表示所有日志写入一个文件
  //   - 超过限制时会创建新日志文件
  size_t max_log_file_size = 0;

  // 日志文件轮转时间间隔
  // 功能：按时间轮转日志文件
  // 默认值：0（禁用）
  // 单位：秒
  // 使用场景：
  //   - 按时间归档日志
  //   - 定期创建新日志文件
  // 重要说明：
  //   - 0表示禁用基于时间的轮转
  //   - 日志活跃时间超过此值时会轮转
  size_t log_file_time_to_roll = 0;

  // 保留的最大日志文件数
  // 功能：控制保留的日志文件数量
  // 默认值：1000
  // 使用场景：
  //   - 控制磁盘空间使用
  //   - 日志保留策略
  // 重要说明：
  //   - 超过此数量的旧日志会被删除
  size_t keep_log_file_num = 1000;

  // 回收日志文件数
  // 功能：重用之前写入的日志文件，覆盖旧数据
  // 默认值：0（禁用）
  // 使用场景：
  //   - 提高日志写入性能
  //   - 减少文件分配开销
  // 重要说明：
  //   - 值表示保留多少个日志文件供重用
  //   - 更高效，因为块已分配，每次写入后fdatasync不需要更新inode
  size_t recycle_log_file_num = 0;

  // MANIFEST文件最大大小
  // 功能：控制MANIFEST文件的大小，超过时轮转
  // 默认值：1GB
  // 使用场景：
  //   - 防止MANIFEST文件过大
  //   - 控制存储容量使用
  // 重要说明：
  //   - 超过限制时创建新MANIFEST文件
  //   - 旧的MANIFEST文件会被删除
  //   - 1GB是合理的默认值，允许增长但不会达到存储限制
  uint64_t max_manifest_file_size = 1024 * 1024 * 1024;

  // 表缓存分片数（2的幂次方）
  // 功能：将表缓存分成多个分片以减少锁竞争
  // 默认值：6（64个分片）
  // 使用场景：
  //   - 高并发环境
  //   - 减少表缓存的锁竞争
  // 重要说明：
  //   - 分片数为2^table_cache_numshardbits
  //   - 增加分片数可减少竞争但增加内存开销
  int table_cache_numshardbits = 6;

  // WAL TTL（生存时间）
  // 功能：控制WAL文件的存活时间
  // 默认值：0
  // 单位：秒
  // 使用场景：
  //   - 基于时间的WAL清理
  //   - 控制WAL保留策略
  // 重要说明：
  //   - 与WAL_size_limit_MB配合使用
  //   - 两者都为0时尽快删除，不归档
  //   - 只有WAL_ttl_seconds非0且WAL_size_limit_MB为0时，检查间隔为WAL_ttl_seconds/2
  //   - 两者都非0时，每10分钟检查，先检查TTL
  uint64_t WAL_ttl_seconds = 0;

  // WAL大小限制（MB）
  // 功能：控制WAL文件的总大小
  // 默认值：0
  // 单位：MB
  // 使用场景：
  //   - 基于大小的WAL清理
  //   - 控制WAL占用磁盘空间
  // 重要说明：
  //   - 与WAL_ttl_seconds配合使用
  //   - 只有WAL_size_limit_MB非0且WAL_ttl_seconds为0时，每10分钟检查
  //   - 超过限制时从最早的开始删除
  //   - 空文件总是被删除
  uint64_t WAL_size_limit_MB = 0;

  // MANIFEST文件预分配大小
  // 功能：预分配MANIFEST文件空间
  // 默认值：4MB
  // 使用场景：
  //   - 减少随机I/O
  //   - 防止过度分配（如xfs的allocsize选项）
  // 重要说明：
  //   - 4MB是合理的默认值
  //   - 减少文件碎片
  //   - 提高写入性能
  size_t manifest_preallocation_size = 4 * 1024 * 1024;

  // 允许使用mmap读取SST文件
  // 功能：使用内存映射方式读取SST文件
  // 默认值：false
  // 使用场景：
  //   - 在ramfs上运行RocksDB
  //   - 减少内存拷贝
  // 重要说明：
  //   - 不推荐在32位系统上使用
  //   - 禁用压缩时，块不复制，直接从mmap内存读取
  //   - 块不会插入块缓存
  //   - 每次读取都会检查校验和（如果ReadOptions.verify_checksums为true）
  //   - ramfs上通常不需要校验和验证
  bool allow_mmap_reads = false;

  // 允许使用mmap写入文件
  // 功能：使用内存映射方式写入文件
  // 默认值：false
  // 使用场景：
  //   - 特殊文件系统
  // 重要说明：
  //   - 设置为true时，DB::SyncWAL()不工作
  //   - 通常不建议使用
  bool allow_mmap_writes = false;

  // Enable direct I/O mode for read/write
  // they may or may not improve performance depending on the use case
  //
  // Files will be opened in "direct I/O" mode
  // which means that data r/w from the disk will not be cached or
  // buffered. The hardware buffer of the devices may however still
  // be used. Memory mapped files are not impacted by these parameters.

  // 使用O_DIRECT进行用户和compaction读取
  // 功能：绕过操作系统缓存直接读取
  // 默认值：false
  // 使用场景：
  //   - 减少页缓存污染
  //   - 专用存储设备
  // 重要说明：
  //   - 需要文件系统支持O_DIRECT
  //   - 可能影响性能
  bool use_direct_reads = false;

  // 使用O_DIRECT进行flush和compaction写入
  // 功能：后台flush和compaction时绕过操作系统缓存直接写入
  // 默认值：false
  // 使用场景：
  //   - 减少页缓存污染
  //   - 专用存储设备
  //   - 控制I/O性能
  // 重要说明：
  //   - 需要文件系统支持O_DIRECT
  //   - 可能影响性能
  //   - 仅适用于后台写入
  bool use_direct_io_for_flush_and_compaction = false;

  // 允许使用fallocate预分配文件空间
  // 功能：预分配文件空间以提高写入性能
  // 默认值：true
  // 使用场景：
  //   - 提高文件写入性能
  //   - 减少碎片化
  // 重要说明：
  //   - 默认为WAL、SST、Manifest文件预分配空间
  //   - 额外空间在文件写入完成时截断
  //   - 警告：使用btrfs文件系统时建议设为false
  //   - btrfs上预分配的空间无法释放，可能导致显著空间浪费
  bool allow_fallocate = true;

  // 禁用子进程继承打开的文件描述符
  // 功能：设置FD_CLOEXEC标志，防止子进程继承文件描述符
  // 默认值：true
  // 使用场景：
  //   - 防止资源泄漏
  //   - 安全性考虑
  //   - fork/exec场景
  // 重要说明：
  //   - 安全实践建议启用
  //   - 防止意外的文件描述符共享
  bool is_fd_close_on_exec = true;

  // 统计信息转储周期
  // 功能：定期将rocksdb.stats转储到LOG
  // 默认值：600秒（10分钟）
  // 使用场景：
  //   - 定期收集性能指标
  //   - 监控数据库状态
  //   - 问题诊断
  // 重要说明：
  //   - 0表示禁用
  //   - 可通过SetDBOptions()动态修改
  unsigned int stats_dump_period_sec = 600;

  // 统计信息持久化周期
  // 功能：定期将统计信息持久化到数据库
  // 默认值：600秒
  // 使用场景：
  //   - 统计信息持久化
  //   - 历史数据查询
  //   - GetStatsHistory API
  // 重要说明：
  //   - 0表示禁用
  //   - 配合persist_stats_to_disk使用
  unsigned int stats_persist_period_sec = 600;

  // 自动持久化统计信息到磁盘
  // 功能：将统计信息持久化到隐藏的列族（___rocksdb_stats_history___）
  // 默认值：false
  // 使用场景：
  //   - 历史统计信息查询
  //   - GetStatsHistory API
  // 重要说明：
  //   - 每stats_persist_period_sec秒持久化一次
  //   - false时写入内存结构
  //   - 如果创建同名列族会失败
  //   - 统计名称限制为100字节
  bool persist_stats_to_disk = false;

  // 统计历史缓冲区大小
  // 功能：限制内存中统计快照的最大大小
  // 默认值：1MB
  // 使用场景：
  //   - 控制统计信息的内存使用
  //   - 定期获取统计快照
  // 重要说明：
  //   - 0表示禁用
  //   - 超过限制会丢弃旧的快照
  size_t stats_history_buffer_size = 1024 * 1024;

  // 打开SST文件时提示随机访问模式
  // 功能：告诉文件系统文件访问模式是随机的
  // 默认值：true
  // 使用场景：
  //   - 优化文件系统行为
  //   - 提高随机读取性能
  // 重要说明：
  //   - 在SST文件打开时应用
  //   - 可能影响预读行为
  bool advise_random_on_open = true;

  // 所有列族的memtable数据累积量达到此大小时写入磁盘
  // 功能：控制所有列族的memtable总大小
  // 默认值：0（禁用）
  // 使用场景：
  //   - 控制整体memtable内存使用
  //   - 跨列族的内存管理
  // 重要说明：
  //   - 与write_buffer_size不同，write_buffer_size限制单个memtable
  //   - 0表示禁用此功能
  //   - 非零值启用此功能
  size_t db_write_buffer_size = 0;

  // 写缓冲区管理器，跟踪和控制所有memtable的内存使用
  // 功能：管理多个数据库实例的memtable内存使用
  // 默认值：nullptr（禁用）
  // 使用场景：
  //   - 跨多个DB实例共享内存配额
  //   - 集中式memtable内存管理
  // 重要说明：
  //   - 可传递给多个DB，跟踪所有DB的memtable总大小
  //   - 超过限制时触发下一个有写入的DB的flush
  //   - 单个DB时行为与db_write_buffer_size相同
  //   - 设置write_buffer_manager会覆盖db_write_buffer_size
  std::shared_ptr<WriteBufferManager> write_buffer_manager = nullptr;

  // compaction开始时的文件访问模式
  // 功能：指定compaction期间的文件访问模式
  // 默认值：NORMAL
  // 可选值：NONE, NORMAL, SEQUENTIAL, WILLNEED
  // 使用场景：
  //   - 优化文件系统预读策略
  //   - 提高compaction性能
  // 重要说明：
  //   - 应用于compaction的所有输入文件
  enum AccessHint { NONE, NORMAL, SEQUENTIAL, WILLNEED };
  AccessHint access_hint_on_compaction_start = NORMAL;

  // compaction时的预读大小
  // 功能：compaction时进行更大的预读操作
  // 默认值：0
  // 使用场景：
  //   - 在机械硬盘上运行RocksDB
  //   - 将compaction从随机读取改为顺序读取
  // 重要说明：
  //   - 在机械硬盘上建议至少设置为2MB
  //   - 可通过SetDBOptions()动态修改
  size_t compaction_readahead_size = 0;

  // Windows上随机访问文件的最大缓冲区大小
  // 功能：WinMmapReadableFile在非缓冲磁盘I/O模式下的缓冲区大小
  // 默认值：1MB
  // 使用场景：
  //   - Windows平台优化
  // 重要说明：
  //   - 仅在Windows上生效
  //   - 0表示不维护实例缓冲区，避免锁
  //   - 缓冲区增长到指定值，然后分配一次性缓冲区
  size_t random_access_max_buffer_size = 1024 * 1024;

  // WritableFileWriter的最大缓冲区大小
  // 功能：控制写入缓冲区的大小
  // 默认值：1MB
  // 使用场景：
  //   - 直接I/O需要对齐的缓冲区
  //   - 缓冲I/O模式
  // 重要说明：
  //   - 直接I/O时固定缓冲区大小以确保对齐
  //   - 可通过SetDBOptions()动态修改
  size_t writable_file_max_buffer_size = 1024 * 1024;

  // 使用自适应互斥锁
  // 功能：在用户空间自旋，失败后才使用内核互斥锁
  // 默认值：false
  // 使用场景：
  //   - 减少上下文切换
  //   - 低竞争场景
  // 重要说明：
  //   - 热锁时可能浪费自旋时间
  //   - 非重竞争场景效果好
  bool use_adaptive_mutex = false;

  // Create DBOptions with default values for all fields
  DBOptions();
  // Create DBOptions from Options
  explicit DBOptions(const Options& options);

  void Dump(Logger* log) const;

  // 允许操作系统在文件写入时异步增量同步到磁盘
  // 功能：平滑写I/O，在文件写入时定期同步
  // 默认值：0（禁用）
  // 单位：字节
  // 使用场景：
  //   - 平滑写I/O
  //   - 避免文件完成时的大量同步
  // 重要说明：
  //   - 每写入bytes_per_sync字节数触发一次同步
  //   - 0表示关闭
  //   - 不提供持久性保证
  //   - 可配合rate_limiter使用
  //   - 启用rate_limiter时自动将bytes_per_sync设为1MB
  //   - 仅适用于表文件，不适用于WAL（见wal_bytes_per_sync）
  //   - 可通过SetDBOptions()动态修改
  uint64_t bytes_per_sync = 0;

  // WAL文件的增量同步大小
  // 功能：与bytes_per_sync类似，但适用于WAL文件
  // 默认值：0（禁用）
  // 单位：字节
  // 使用场景：
  //   - WAL文件的增量同步
  //   - 平滑WAL写入I/O
  // 重要说明：
  //   - 0表示关闭
  //   - 可通过SetDBOptions()动态修改
  uint64_t wal_bytes_per_sync = 0;

  // 严格限制待同步的字节数
  // 功能：确保任何时刻待同步的字节数不超过限制
  // 默认值：false
  // 使用场景：
  //   - 处理生成速度超过I/O速度的情况
  //   - 避免文件完成时的大量同步
  // 重要说明：
  //   - WAL文件：最多wal_bytes_per_sync字节待写回
  //   - SST文件：最多bytes_per_sync字节待写回
  //   - 如果支持sync_file_range：等待之前的sync_file_range完成
  //   - 如果不支持：使用WritableFile::Sync，总是阻塞
  //   - 不提供额外的持久性保证
  //   - sync_file_range不写元数据
  bool strict_bytes_per_sync = false;

  // 事件监听器列表
  // 功能：注册在特定RocksDB事件发生时调用的回调函数
  // 使用场景：
  //   - 监控数据库事件
  //   - 自定义事件处理
  //   - 审计和日志
  // 重要说明：
  //   - 可以添加多个监听器
  //   - 支持多种事件类型
  std::vector<std::shared_ptr<EventListener>> listeners;

  // 是否启用线程跟踪
  // 功能：跟踪数据库涉及的线程状态
  // 默认值：false
  // 使用场景：
  //   - 调试和诊断
  //   - 监控线程状态
  //   - 性能分析
  // 重要说明：
  //   - 线程状态可通过GetThreadList() API获取
  bool enable_thread_tracking = false;

  // 延迟写入速率
  // 功能：当触发写入限制时限制写入速率
  // 默认值：0（自动推断）
  // 触发条件：
  //   - soft_pending_compaction_bytes_limit被触发
  //   - level0_slowdown_writes_trigger被触发
  //   - 写入最后一个允许的memtable且允许超过3个memtable
  // 计算方式：基于压缩前的用户写入请求大小
  // 自动推断：
  //   - 值为0时，从rate_limiter推断（如果不为空）
  //   - rate_limiter为空时，默认16MB/s
  // 使用场景：
  //   - 防止写入停滞
  //   - 平衡写入和compaction
  //   - 保护系统资源
  // 重要说明：
  //   - RocksDB可能进一步降低速率
  //   - 单位：字节/秒
  //   - DB打开后修改rate_limiter不会调整此值
  //   - 可通过SetDBOptions()动态修改
  uint64_t delayed_write_rate = 0;

  // 启用管道化写入
  // 功能：将WAL写入和memtable写入分离到不同的线程队列中
  // 默认值：false
  // 工作原理：
  //   - 默认（false）：单个写线程队列，队列头部的线程负责写入WAL和memtable
  //   - 启用（true）：维护独立的WAL写队列和memtable写队列
  //   - 写线程先进入WAL写队列，再进入memtable写队列
  //   - WAL写队列中的线程只需等待前一个线程完成WAL写入，不必等待memtable写入
  // 使用场景：
  //   - 提高写入吞吐量
  //   - 减少两阶段提交准备阶段的延迟
  //   - 高并发写入场景
  // 重要说明：
  //   - 可以提高写入性能，特别是在WAL和memtable写入速度差异较大的情况
  //   - 适用于需要高写入吞吐量的工作负载
  bool enable_pipelined_write = false;

  // 无序写入
  // 功能：放松快照不可变性保证以提高写入吞吐量
  // 默认值：false
  // 工作原理：
  //   - false（默认）：快照的不可变性保证
  //     - 只有所有低序号的写入完成，才会为新的快照增加序号
  //     - Iterator和MultiGet提供一致的时间点视图
  //   - true：放松快照不可变性
  //     - 快照仍然看不到获取后发出的写入（高序号）
  //     - 但可能看到获取前仍在进行的写入（低序号）完成后的结果
  // 权衡：
  //   - 优点：更高的写入吞吐量
  //   - 缺点：违反快照的不可变性保证
  //   - 影响：::Get、::MultiGet、Iterator的一致性
  // 使用场景：
  //   - 可以容忍放松的快照保证
  //   - 高吞吐量写入优先
  // 替代方案：
  //   - 使用TransactionDB的WRITE_PREPARED策略
  //   - 配合two_write_queues=true
  // 重要说明：
  //   - Read-Your-Own-Write属性仍然保持
  //   - 应用程序可以自己实现机制来弥补
  bool unordered_write = false;

  // 允许并发写入memtable
  // 功能：允许多个写线程并行更新memtable
  // 默认值：true
  // 支持的memtable工厂：
  //   - SkipListFactory（已实现）
  //   - 其他工厂可能不支持
  // 不兼容：
  //   - inplace_update_support
  //   - filter_deletes
  // 使用场景：
  //   - 高并发写入
  //   - 多核CPU环境
  // 重要说明：
  //   - 强烈建议配合enable_write_thread_adaptive_yield使用
  //   - 可以显著提高写入吞吐量
  bool allow_concurrent_memtable_write = true;

  // 启用写线程自适应让步
  // 功能：写线程在阻塞前先自旋等待
  // 默认值：true
  // 工作原理：
  //   - 与写批处理组 leader 同步的线程
  //   - 在互斥锁上阻塞前最多等待 write_thread_max_yield_usec 微秒
  // 性能提升：
  //   - 显著提高并发工作负载的吞吐量
  //   - 无论是否启用allow_concurrent_memtable_write都有效
  // 使用场景：
  //   - 高并发写入场景
  //   - 多核环境
  // 重要说明：
  //   - 与allow_concurrent_memtable_write配合使用效果更佳
  //   - 通过自旋减少上下文切换
  bool enable_write_thread_adaptive_yield = true;

  // 单个写批处理的最大字节数
  // 功能：限制单次WAL或memtable写入的字节数
  // 默认值：1MB
  // 应用条件：
  //   - 当 leader 写入大小超过限制的1/8时遵循此限制
  // 使用场景：
  //   - 控制批处理大小
  //   - 防止单次写入过大
  //   - 平衡吞吐量和延迟
  // 重要说明：
  //   - 过大的值可能影响延迟
  //   - 过小的值可能降低吞吐量
  uint64_t max_write_batch_group_size_bytes = 1 << 20;

  // 写线程自旋等待的最大时间
  // 功能：写线程在互斥锁阻塞前自旋的最大时间
  // 默认值：100微秒
  // 工作原理：
  //   - 写操作先自旋与其他写线程协调
  //   - 超过此时间后阻塞在互斥锁上
  // 性能权衡：
  //   - 增加此值：可能提高吞吐量，但增加CPU使用
  //   - 减少此值：减少CPU使用，但可能降低吞吐量
  // 使用场景：
  //   - CPU资源充足时可以增加
  //   - 高竞争场景需要调整
  // 重要说明：
  //   - 假设write_thread_slow_yield_usec设置正确
  //   - 需要根据实际工作负载调整
  uint64_t write_thread_max_yield_usec = 100;

  // 慢让步延迟阈值
  // 功能：判断std::this_thread::yield调用是否为慢让步的阈值
  // 默认值：3微秒
  // 工作原理：
  //   - yield调用延迟超过此值表示其他进程/线程想使用当前核心
  // 影响：
  //   - 增加此值：写线程更可能自旋占用CPU
  //     - 表现为非自愿上下文切换增加
  //   - 减少此值：写线程更快让出CPU
  // 使用场景：
  //   - CPU竞争激烈时减小
  //   - CPU资源充足时可以增加
  // 重要说明：
  //   - 影响CPU调度行为
  //   - 需要监控上下文切换次数
  uint64_t write_thread_slow_yield_usec = 3;

  // 跳过数据库打开时的统计更新
  // 功能：DB::Open()时不加载表属性来更新压缩决策统计
  // 默认值：false
  // 使用场景：
  //   - 加速数据库打开
  //   - 磁盘环境
  //   - 大量SST文件
  // 重要说明：
  //   - true会打开更快，但可能影响初始压缩决策
  //   - 统计会在后续操作中逐渐建立
  bool skip_stats_update_on_db_open = false;

  // 跳过数据库打开时的SST文件大小检查
  // 功能：DB::Open()时不获取和检查所有SST文件的大小
  // 默认值：false
  // 使用场景：
  //   - 显著加速启动时间
  //   - 大量SST文件
  //   - 使用昂贵的GetFileSize()的Env
  // 重要说明：
  //   - 仍然会检查所需的SST文件是否存在
  //   - 如果paranoid_checks为false，此选项被忽略
  //   - 不检查任何SST文件
  bool skip_checking_sst_file_sizes_on_db_open = false;

  // WAL恢复模式
  // 功能：控制重放WAL时的一致性级别
  // 默认值：kPointInTimeRecovery
  // 可选值（枚举WALRecoveryMode）：
  //   - kTolerateCorruptedTailRecords（0x00）：容忍尾部损坏
  //     - 原始LevelDB恢复模式
  //     - 容忍最后一条记录因崩溃而不完整
  //     - 零字节（预分配）也被容忍
  //     - 使用场景：更新后绝不能回滚的应用
  //     - 注意：发现损坏会拒绝打开数据库
  //
  //   - kAbsoluteConsistency（0x01）：绝对一致性
  //     - 从干净关闭恢复
  //     - 不期望任何损坏
  //     - 使用场景：单元测试，需要高一致性保证的应用
  //
  //   - kPointInTimeRecovery（0x02）：时间点一致性（默认）
  //     - 发现WAL不一致时停止重放
  //     - 恢复到最后一致的点
  //     - 使用场景：有磁盘控制器缓存的系统（硬盘、无超级电容的SSD）
  //
  //   - kSkipAnyCorruptedRecords（0x03）：跳过损坏记录
  //     - 灾难恢复模式
  //     - 忽略所有损坏，尽力恢复数据
  //     - 使用场景：最后手段恢复，操作低质量无关数据
  // 重要说明：
  //   - 根据应用需求选择合适的模式
  //   - kPointInTimeRecovery是推荐的默认值
  WALRecoveryMode wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  // 允许两阶段提交
  // 功能：控制恢复时是否允许prepared事务
  // 默认值：false
  // 使用场景：
  //   - 使用两阶段提交的应用
  //   - 跨数据库事务
  // 重要说明：
  //   - false时，恢复时遇到prepared事务会失败
  //   - true时，可以恢复prepared事务
  bool allow_2pc = false;

  // 行缓存
  // 功能：表级别的全局行缓存
  // 默认值：nullptr（禁用）
  // 使用场景：
  //   - 缓存查询结果
  //   - 提高点查询性能
  //   - 减少磁盘I/O
  // 重要说明：
  //   - 与block_cache配合使用
  //   - 需要额外的内存
  //   - 适用于热点数据查询
  std::shared_ptr<RowCache> row_cache = nullptr;

  // WAL过滤器
  // 功能：恢复时处理WAL的回调函数
  // 默认值：nullptr（禁用）
  // 功能：
  //   - 检查日志记录
  //   - 忽略特定记录
  //   - 跳过记录重放
  // 使用场景：
  //   - 自定义恢复逻辑
  //   - 数据过滤
  //   - 安全审计
  // 重要说明：
  //   - 在启动时调用
  //   - 当前从单线程调用
  //   - 可以用于跳过某些写入
  WalFilter* wal_filter = nullptr;

  // 选项文件错误时失败
  // 功能：选项文件未正确持久化时操作失败
  // 默认值：false
  // 影响的操作：
  //   - DB::Open
  //   - CreateColumnFamily
  //   - DropColumnFamily
  //   - SetOptions
  // 使用场景：
  //   - 严格模式
  //   - 确保配置正确持久化
  // 重要说明：
  //   - false时，某些错误会被忽略
  //   - true时，选项文件错误会导致操作失败
  bool fail_if_options_file_error = false;

  // 打印malloc统计信息
  // 功能：打印rocksdb.stats时同时打印malloc统计
  // 默认值：false
  // 使用场景：
  //   - 内存分析
  //   - 内存泄漏检测
  //   - 性能调优
  // 重要说明：
  //   - 需要支持malloc_stats的系统
  //   - 输出到LOG
  bool dump_malloc_stats = false;

  // 恢复时避免flush
  // 功能：数据库打开时避免flush WAL日志
  // 默认值：false
  // 默认行为：
  //   - RocksDB重放WAL日志并flush
  //   - 可能创建很小的SST文件
  // 启用后：
  //   - 尝试避免在恢复期间flush（不保证）
  //   - 保留现有的WAL日志
  //   - 如果崩溃发生在flush之前，仍有日志可以恢复
  // 使用场景：
  //   - 加速数据库打开
  //   - 避免创建小的SST文件
  // 重要说明：
  //   - 不保证完全避免flush
  //   - WAL日志会被保留
  bool avoid_flush_during_recovery = false;

  // 关闭时避免flush
  // 功能：数据库关闭时跳过memtable flush以加快关闭速度
  // 默认值：false
  // 默认行为：
  //   - 如果有未持久化的数据（WAL禁用时）
  //   - RocksDB会在关闭时flush所有memtable
  // 启用后：
  //   - 跳过flush以加速关闭
  //   - 未持久化的数据将会丢失
  // 使用场景：
  //   - 快速关闭
  //   - 可以容忍数据丢失
  //   - 测试环境
  // 重要说明：
  //   - 警告：未持久化的数据会丢失
  //   - 可通过SetDBOptions()动态修改
  bool avoid_flush_during_shutdown = false;

  // 允许在后面导入
  // 功能：允许调用IngestExternalFile()时跳过已存在的键
  // 默认值：false
  // 行为：
  //   - IngestExternalFile()跳过已存在的键，而不是覆盖
  //   - 设置为true有以下效果：
  //     1. 禁用SST文件压缩的一些内部优化
  //     2. 保留最后一层仅用于导入的文件
  //     3. 压缩不会包括最后一层的任何文件
  // 使用场景：
  //   - 历史数据回填
  //   - 不覆盖现有数据
  // 重要说明：
  //   - 仅Universal Compaction支持
  //   - num_levels应>= 3
  //   - 不可变选项，创建后不能修改
  bool allow_ingest_behind = false;

  // 两个写入队列
  // 功能：为不同类型的写入使用单独的队列
  // 默认值：false
  // 工作原理：
  //   - false：单队列
  //   - true：两个队列
  //     - 队列1：disable_memtable的写入
  //     - 队列2：写入memtable的写入
  //   - 允许memtable写入不会落后于其他写入
  // 使用场景：
  //   - MySQL 2PC优化
  //   - 只有提交（串行）写入memtable
  //   - 提高写入吞吐量
  // 重要说明：
  //   - 与enable_pipelined_write配合使用
  bool two_write_queues = false;

  // 手动flush WAL
  // 功能：不在每次写入后自动flush WAL
  // 默认值：false
  // 工作原理：
  //   - false：每次写入后自动flush WAL
  //   - true：依赖手动调用FlushWAL写入WAL缓冲区
  // 使用场景：
  //   - 自定义WAL flush策略
  //   - 批量写入优化
  //   - 减少fsync调用
  // 重要说明：
  //   - 增加数据丢失风险
  //   - 需要确保正确调用FlushWAL
  bool manual_wal_flush = false;

  // WAL压缩
  // 功能：写入前压缩WAL记录
  // 默认值：kNoCompression（禁用）
  // 支持的压缩算法：
  //   - zstd
  // 使用场景：
  //   - 减少WAL文件大小
  //   - 节省磁盘空间
  //   - 减少I/O带宽
  // 重要说明：
  //   - 工作中功能（WORK IN PROGRESS）
  //   - 支持的版本会自动读取压缩的WAL记录
  //   - 无论wal_compression设置如何
  CompressionType wal_compression = kNoCompression;

  // 原子flush
  // 功能：原子性地flush多个列族并提交到MANIFEST
  // 默认值：false
  // 说明：
  //   - WAL始终启用时不需要此选项
  //   - WAL允许数据库恢复到WAL中的最后持久化状态
  //   - 此选项适用于有写入不受WAL保护的列族
  // 行为：
  //   - 手动flush：应用程序在DB::Flush中指定要原子flush的列族
  //   - 自动触发flush：RocksDB原子性地flush所有列族
  // 当前限制：
  //   - 原子flush后的WAL启用写入可能独立重放
  //   - 如果进程稍后崩溃并尝试恢复
  // 使用场景：
  //   - 多列族一致性
  //   - 部分列族禁用WAL
  //   - 强一致性要求
  // 重要说明：
  //   - 可能影响性能
  bool atomic_flush = false;

  // 避免不必要的阻塞I/O
  // 功能：避免在执行线程中执行耗时的阻塞I/O操作
  // 默认值：false
  // 工作原理：
  //   - false：工作线程直接执行操作（删除文件等）
  //   - true：调度后台任务执行操作
  //   - 避免的操作：
  //     - 直接删除过期文件
  //     - 删除memtable
  //   - 改为调度后台任务执行
  // 优先级：
  //   - 设置为true时，优先级高于
  //     ReadOptions::background_purge_on_iterator_cleanup
  // 使用场景：
  //   - 延迟敏感的应用
  //   - 保持前台响应
  // 重要说明：
  //   - 可能略微增加后台负载
  //   - 对延迟敏感的场景推荐启用
  bool avoid_unnecessary_blocking_io = false;

  // 将DB ID写入MANIFEST
  // 功能：将DB ID同时写入MANIFEST文件和Identity文件
  // 默认值：false
  // 背景：
  //   - 历史上DB ID只存储在DB文件夹的Identity文件中
  // 设置为true的优点：
  //   1. Identity文件没有校验和，MANIFEST文件有
  //   2. DB的真相来源是MANIFEST文件
  //      - DB ID与真相来源在一起
  //      - 避免之前Identity文件可以独立于MANIFEST复制
  //      - 防止错误的DB ID
  // 使用场景：
  //   - 提高数据完整性
  //   - 防止DB ID不匹配
  //   - 数据库迁移
  // 重要说明：
  //   - 推荐设置为true
  bool write_dbid_to_manifest = false;

  // 日志预读大小
  // 功能：读取日志时预读的字节数
  // 默认值：0（禁用）
  // 使用场景：
  //   - 读取远程日志
  //   - 减少往返次数
  //   - 提高日志读取性能
  // 重要说明：
  //   - 0表示禁用预读
  //   - 远程存储场景特别有用
  size_t log_readahead_size = 0;

  // 文件校验和生成器工厂
  // 功能：为SST文件生成校验和
  // 默认值：nullptr（禁用）
  // 工作原理：
  //   - 未提供时不使用文件校验和
  //   - 创建SST文件时创建新的校验和生成器对象
  //   - 每个生成器只从单线程使用，不需要线程安全
  // 使用场景：
  //   - 数据完整性验证
  //   - 文件损坏检测
  //   - 存储层校验和offload
  // 重要说明：
  //   - 需要文件系统支持
  //   - 略微增加开销
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  // 尽力恢复
  // 功能：尝试恢复到任何有效的点时间状态，而不是返回错误
  // 默认值：false
  // 默认行为：
  //   - RocksDB尝试检测DB文件中的数据丢失或损坏
  //   - 在DB::Open时或后续操作中向用户返回错误
  //   - WAL文件恢复由wal_recovery_mode控制
  // 启用后：
  //   - 优先将DB打开到每个列族的任何有效点时间状态
  //   - 包括空/新状态
  //   - 不将非WAL数据丢失作为错误返回给用户
  //   - 类似于对每个列族应用WALRecoveryMode::kPointInTimeRecovery
  // 设计目的：
  //   - 恢复文件缺失或截断的DB
  //   - 不完整的DB"物理"（FileSystem）复制的结果
  //   - 检测SST文件被替换为相同大小的不同文件
  //     - 假设MANIFEST中跟踪了SST唯一ID
  // 当前限制：
  //   - 不适用于其他DB文件损坏（可被DB::VerifyChecksum()检测）
  //   - 不尝试恢复任何WAL文件
  //   - 需要至少一个有效的MANIFEST
  //   - 与atomic_flush不兼容
  // 示例：
  //   - MANIFEST引用的SST或blob文件缺失
  //   - BER可能找到对应于旧"点时间"版本的一组文件
  //     - 可能来自旧的MANIFEST文件
  //   - 其他DB文件（CURRENT, LOCK, IDENTITY）被忽略或替换或修复
  // 使用场景：
  //   - 不完整的物理复制
  //   - 文件系统恢复
  //   - 容灾恢复
  // 重要说明：
  //   - 与ldb repair不同，需要有效的MANIFEST
  //   - 不保证恢复所有数据
  bool best_efforts_recovery = false;

  // 最大后台错误恢复尝试次数
  // 功能：后台可重试IO错误时调用DB::Resume()的次数
  // 默认值：INT_MAX
  // 工作流程：
  //   1. 后台发生可重试IO错误
  //   2. 调用SetBGError处理错误
  //   3. 如果错误可以自动恢复
  //      - 例如：Flush或WAL写入期间的可重试IO错误
   //      - 在后台调用db resume恢复
  //   4. 重复直到达到最大次数
  // 值含义：
  //   - 0或负数：不自动调用DB::Resume()
  //   - INT_MAX：无限重试
  //   - 其他值：最多重试指定次数
  // 使用场景：
  //   - 可恢复的IO错误
  //   - 网络存储
  //   - 临时故障
  // 重要说明：
  //   - 与bgerror_resume_retry_interval配合使用
  int max_bgerror_resume_count = INT_MAX;

  // 后台错误恢复重试间隔
  // 功能：恢复失败后重试前等待的时间
  // 默认值：1000000微秒（1秒）
  // 使用条件：
  //   - max_bgerror_resume_count >= 2
  //   - 上一次恢复失败
  //   - 满足重做恢复的条件
  // 使用场景：
  //   - 控制重试频率
  //   - 避免频繁重试
  // 重要说明：
  //   - 单位：微秒
  //   - 与max_bgerror_resume_count配合使用
  uint64_t bgerror_resume_retry_interval = 1000000;

  // 允许在错误消息中包含数据
  // 功能：错误消息中包含损坏的键/值
  // 默认值：false
  // 启用后：
  //   - 错误消息/日志/状态中包含损坏的键和值
  //   - 帮助用户了解受影响的数据
  // 禁用原因：
  //   - 防止用户数据暴露在日志/消息中
  //   - 隐私和安全考虑
  // 使用场景：
  //   - 调试和诊断
  //   - 数据恢复
  //   - 问题排查
  // 重要说明：
  //   - 可能暴露敏感数据
  //   - 仅在安全的环境中使用
  bool allow_data_in_errors = false;

  // 数据库主机ID
  // 功能：标识托管数据库的机器
  // 默认值：kHostnameForDbHostId（实际主机名）
  // 存储位置：
  //   - 作为属性写入每个SST文件
  //   - 由DB或离线写入器写入
  //     - SstFileWriter
  //     - RepairDB
  // 使用场景：
  //   - 故障排查
  //   - 追溯写入主机
  //   - 内存损坏诊断
  //   - 主机故障导致文件损坏时
  // 特殊值：
  //   - 默认值：使用实际主机名
  //   - 空字符串：不在SST文件中写入此属性
  // 说明：
  //   - 可能不会通过校验和检测损坏
  //   - 损坏发生在校验和之前
  std::string db_host_id = kHostnameForDbHostId;

  // 校验和移交文件类型
  // 功能：为特定文件类型启用校验和移交
  // 默认值：空（禁用）
  // 前提条件：
  //   - 文件系统支持crc32c校验和验证
  // 当前支持的文件类型：
  //   - kWALFile：WAL文件
  //   - kTableFile：SST表文件
  //   - kDescriptorFile：描述符文件（MANIFEST等）
  // 工作原理：
  //   - RocksDB生成crc32c校验和
  //   - 移交给存储层验证
  //   - 减少重复计算
  // 注意事项：
  //   - RocksDB仅生成基于crc32c的校验和
  //   - 如果存储层有不同的校验和支持：
  //     - 用户应将此设置为空
  //   - 否则可能导致意外的写入失败
  // 使用场景：
  //   - 硬件加速校验和
  //   - 减少CPU开销
  //   - 存储层支持crc32c时
  // 重要说明：
  //   - 确保文件系统支持
  //   - 不支持时设置为空
  FileTypeSet checksum_handoff_file_types;

  // 压缩服务
  // 功能：允许在不同的主机或进程上运行压缩
  // 默认值：nullptr（禁用）
  // 特点：
  //   - 从主主机卸载后台负载
  //   - 将压缩任务分发到远程节点
  // 使用场景：
  //   - 分布式压缩
  //   - 资源卸载
  //   - 集群环境
  // 重要说明：
  //   - 实验性功能
  //   - 接口可能变化
  //   - 不保证向后/向前兼容性
  //   - 已知问题仍在开发中
  std::shared_ptr<CompactionService> compaction_service = nullptr;

  // 使用的最低缓存层
  // 功能：指定数据库使用的最低缓存层
  // 默认值：kNonVolatileBlockTier
  // 支持的缓存层：
  //   - kVolatileTier：易失性层
  //     - 仅使用块缓存（当前实现的易失性层）
  //     - 缓存条目不会溢出到二级缓存
  //     - 块缓存查找失败不会查询二级缓存
  //   - kNonVolatileBlockTier：非易失性块层
  //     - 使用块缓存和二级缓存
  //     - 块缓存未命中时查询二级缓存
  // 架构：
  //   - 分层架构
  //   - volatile_tier和non_volatile_tier是分层关系
  // 使用场景：
  //   - kVolatileTier：纯内存缓存
  //   - kNonVolatileBlockTier：内存+二级缓存
  // 重要说明：
  //   - 设置为kVolatileTier时，二级缓存被禁用
  //   - 影响缓存策略和性能
  CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;

  // 强制SingleDelete契约
  // 功能：控制是否强制执行SingleDelete的使用契约
  // 默认值：true
  // 工作原理：
  //   - true：强制执行契约
  //     - compaction或flush看到SingleDelete后跟同一键的Delete
  //     - compaction任务会失败
  //   - false：不强制执行契约
  //     - 不检查SingleDelete契约
  //     - 允许混合使用Delete和SingleDelete
  // 迁移用途：
  //   - 临时选项，帮助现有用例迁移
  //   - 将在未来的版本中删除
  // 警告：
  //   - 除非迁移现有数据，否则不要设置为false
  //   - 违反契约可能导致未定义行为
  //   - 可能的数据不一致：
  //     - 已删除的旧数据重新可见
  //     - 其他数据一致性问题
  // SingleDelete契约：
  //   - 参见：https://github.com/facebook/rocksdb/wiki/Single-Delete
  //   - 同一用户键不应混合使用Delete和SingleDelete
  // 使用场景：
  //   - 迁移现有数据（契约未强制执行）
  //   - 逐步修复数据模型
  // 重要说明：
  //   - 生产环境应保持为true
  //   - 迁移完成后应移除此选项
  bool enforce_single_del_contracts = true;
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options : public DBOptions, public ColumnFamilyOptions {
  // Create an Options object with default values for all fields.
  Options() : DBOptions(), ColumnFamilyOptions() {}

  Options(const DBOptions& db_options,
          const ColumnFamilyOptions& column_family_options)
      : DBOptions(db_options), ColumnFamilyOptions(column_family_options) {}

  // Change to some default settings from an older version.
  // NOT MAINTAINED: This function has not been and is not maintained.
  // DEPRECATED: This function might be removed in a future release.
  // In general, defaults are changed to suit broad interests. Opting
  // out of a change on upgrade should be deliberate and considered.
  Options* OldDefaults(int rocksdb_major_version = 4,
                       int rocksdb_minor_version = 6);

  void Dump(Logger* log) const;

  void DumpCFOptions(Logger* log) const;

  // Some functions that make it easier to optimize RocksDB

  // Set appropriate parameters for bulk loading.
  // The reason that this is a function that returns "this" instead of a
  // constructor is to enable chaining of multiple similar calls in the future.
  //

  // All data will be in level 0 without any automatic compaction.
  // It's recommended to manually call CompactRange(NULL, NULL) before reading
  // from the database, because otherwise the read can be very slow.
  Options* PrepareForBulkLoad();

  // Use this if your DB is very small (like under 1GB) and you don't want to
  // spend lots of memory for memtables.
  Options* OptimizeForSmallDb();

  // Disable some checks that should not be necessary in the absence of
  // software logic errors or CPU+memory hardware errors. This can improve
  // write speeds but is only recommended for temporary use. Does not
  // change protection against corrupt storage (e.g. verify_checksums).
  Options* DisableExtraChecks();
};

// An application can issue a read request (via Get/Iterators) and specify
// if that read should process data that ALREADY resides on a specified cache
// level. For example, if an application specifies kBlockCacheTier then the
// Get call will process data that is already processed in the memtable or
// the block cache. It will not page in data from the OS cache or data that
// resides in storage.
enum ReadTier {
  kReadAllTier = 0x0,     // data in memtable, block cache, OS cache or storage
  kBlockCacheTier = 0x1,  // data in memtable or block cache
  kPersistedTier = 0x2,   // persisted data.  When WAL is disabled, this option
                          // will skip data in memtable.
                          // Note that this ReadTier currently only supports
                          // Get and MultiGet and does not support iterators.
  kMemtableTier = 0x3     // data in memtable. used for memtable-only iterators.
};

// 读取操作选项结构体
// 该结构体控制 RocksDB 的所有读取操作行为，包括：
// - 点查询（Get、MultiGet）
// - 迭代器操作（Iterator）
// - 扫描操作
struct ReadOptions {
  // *** 适用于点查询和扫描的选项开始 ***

  // 读取快照
  // 功能：指定读取操作的快照版本，提供一致性的数据视图
  // 默认值：nullptr（使用隐式快照）
  // 工作原理：
  //   - nullptr：使用读取操作开始时的隐式快照
  //   - 非空：读取指定的快照版本，快照必须属于当前 DB 且未释放
  // 使用场景：
  //   - 一致性读取：确保多次读取看到相同的数据
  //   - 跨线程读取：多个线程读取同一时间点的数据
  //   - 防止幻读：避免读取过程中看到新写入的数据
  // 重要说明：
  //   - 快照会占用内存和资源，用完需要释放
  //   - 适合需要强一致性的场景
  //   - 过多的快照会增加写放大
  const Snapshot* snapshot = nullptr;

  // 操作时间戳
  // 功能：读取指定时间点可见的最新数据
  // 默认值：nullptr（读取最新数据）
  // 工作原理：
  //   - timestamp：读取时间戳 <= 指定时间戳的最新数据
  //   - iter_start_ts：迭代器的起始时间戳（下界，较旧）
  //   - 对于迭代器，返回在 [iter_start_ts, timestamp] 范围内的版本
  // 前提条件：
  //   - 同一数据库的所有时间戳必须长度和格式相同
  //   - 需要提供自定义的 Comparator 来排序 <key, timestamp> 元组
  // 使用场景：
  //   - 时间旅行查询：读取历史数据
  //   - 数据审计：查看某个时间点的数据状态
  //   - 多版本数据：保留数据的多个时间版本
  // 重要说明：
  //   - 用户指定的时间戳功能仍在开发中，API 可能会变化
  //   - 需要配合 Comparator 一起使用
  const Slice* timestamp = nullptr;
  const Slice* iter_start_ts = nullptr;

  // API 调用截止时间
  // 功能：设置 API 调用（Get/MultiGet/Seek/Next）完成的最大时间
  // 默认值：0（无超时）
  // 单位：微秒（microseconds）
  // 设置方式：
  //   - 使用 env->NowMicros() + 超时时长
  //   - 或使用 gettimeofday() 的返回值 + 超时时长
  // 工作原理：
  //   - RocksDB 会定期检查是否超时
  //   - 超时后立即返回当前结果
  //   - 最佳努力：不保证严格在截止时间完成
  // 超时原因：
  //   - 文件系统不支持截止时间
  //   - 批处理时定期检查而非每个键都检查
  //   - 涉及 I/O 操作且无法中断
  // 使用场景：
  //   - 在线查询：避免长时间阻塞
  //   - SLA 保证：控制最大响应时间
  //   - 超时保护：防止慢查询影响系统
  // 重要说明：
  //   - 调用可能超过截止时间
  //   - 仅影响 API 调用，不影响后台操作
  std::chrono::microseconds deadline = std::chrono::microseconds::zero();

  // 单次读取超时时间
  // 功能：每个文件读取请求的超时时间
  // 默认值：0（无超时）
  // 单位：微秒（microseconds）
  // 与 deadline 的区别：
  //   - deadline：整个 API 调用的总超时时间
  //   - io_timeout：每个文件读取请求的超时时间
  // 工作原理：
  //   - MultiGet/Get/Seek/Next 可能导致多次文件读取
  //   - 每次文件读取都有独立的 io_timeout
  //   - 总时间可能超过 io_timeout，但单次读取不会
  // 使用场景：
  //   - 高延迟存储：避免单个读取阻塞过久
  //   - 网络存储：网络不稳定时的超时控制
  //   - 混合存储：不同层级的存储超时控制
  // 重要说明：
  //   - 需要文件系统支持超时功能
  //   - 适用于每个单独的文件读取操作
  std::chrono::microseconds io_timeout = std::chrono::microseconds::zero();

  // 读取层级
  // 功能：指定读取请求应该从哪个缓存层级处理数据
  // 默认值：kReadAllTier（从所有层级读取）
  // 可选值：
  //   - kReadAllTier：从所有层级读取（缓存 + 磁盘）
  //   - kBlockCacheTier：只从块缓存读取
  //   - kPersistedTier：从持久化存储读取
  // 工作原理：
  //   - kBlockCacheTier：数据不在块缓存时返回 Status::Incomplete
  //   - 可以用于构建多级缓存架构
  // 使用场景：
  //   - kBlockCacheTier：
  //     - 只读取缓存数据，不触发磁盘 I/O
  //     - 低延迟读取，可以容忍未命中
  //   - kReadAllTier：
  //     - 完整的读取功能，包括缓存未命中时从磁盘读取
  // 重要说明：
  //   - kBlockCacheTier 可能导致部分查询失败
  //   - 应用需要处理 Status::Incomplete 状态
  ReadTier read_tier = kReadAllTier;

  // 速率限制器优先级
  // 功能：指定文件读取对内部速率限制器的计费优先级
  // 默认值：Env::IO_TOTAL（禁用速率限制）
  // 工作原理：
  //   - 控制读取对 DBOptions::rate_limiter 的使用方式
  //   - IO_TOTAL：不限制读取速率
  //   - IO_USER：受速率限制器约束
  // 例外情况：
  //   - PlainTable（PlainTableFactory）的文件读取不限制
  //   - CuckooTable（CuckooTableFactory）的文件读取不限制
  //   - 文件头/尾等次要读取不计入速率限制
  // 使用场景：
  //   - 混合负载：限制读取 I/O 不影响写入
  //   - 资源隔离：控制不同类型读取的资源使用
  //   - IO_TOTAL：不受限制的读取（默认）
  // 重要说明：
  //   - 实际计入的字节数可能不完全等于文件读取字节数
  //   - 需要设置 DBOptions::rate_limiter 才生效
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;

  // MultiGet 值大小软限制
  // 功能：限制 MultiGet 批量读取中所有键的累计值大小
  // 默认值：std::numeric_limits<uint64_t>::max()（无限制）
  // 工作原理：
  //   - 在 MultiGet 过程中累计读取的值大小
  //   - 一旦超过此限制，剩余的键返回 Status::Aborted
  //   - 软限制：已读取的键仍然返回结果
  // 使用场景：
  //   - 内存保护：防止 MultiGet 返回过大数据
  //   - 性能控制：避免单次查询占用过多资源
  //   - 流量控制：限制每次查询的数据量
  // 重要说明：
  //   - 只影响 MultiGet，不影响 Get
  //   - 应用需要处理 Status::Aborted 状态
  //   - 已返回的键不受影响
  uint64_t value_size_soft_limit = std::numeric_limits<uint64_t>::max();

  // 验证校验和
  // 功能：读取数据时验证校验和以检测数据损坏
  // 默认值：true
  // 工作原理：
  //   - true：每次读取都验证校验和
  //   - false：不验证校验和，略微提升性能
  // 性能影响：
  //   - 启用：增加 CPU 开销，确保数据完整性
  //   - 禁用：减少开销，但无法检测数据损坏
  // 使用场景：
  //   - true（推荐）：
  //     - 关键数据：需要确保数据完整性
  //     - 生产环境：避免使用损坏的数据
  //   - false：
  //     - 性能优先场景：可以容忍潜在的数据损坏
  //     - 数据已通过其他方式校验
  // 重要说明：
  //   - 通常建议保持启用
  //   - 数据损坏时会导致查询失败
  bool verify_checksums = true;

  // 填充块缓存
  // 功能：控制读取的数据块和索引块是否放入块缓存
  // 默认值：true
  // 工作原理：
  //   - true：读取的数据块放入块缓存
  //   - false：数据不放入块缓存
  // 使用场景：
  //   - true：
  //     - 重复读取相同数据
  //     - 需要缓存提升性能
  //   - false：
  //     - 批量扫描：避免污染缓存
  //     - 一次性读取：数据不会再次访问
  //     - 保持缓存中现有数据的驱逐顺序
  // 重要说明：
  //   - 批量扫描时建议设置为 false
  //   - 可以减少缓存污染，提高缓存命中率
  bool fill_cache = true;

  // 忽略范围删除标记
  // 功能：跳过范围删除标记（range tombstones）的处理
  // 默认值：false
  // 工作原理：
  //   - false：正常处理 range tombstones，正确处理 DeleteRange 操作
  //   - true：跳过 range tombstones，可能导致读取到已删除的旧数据
  // 使用场景：
  //   - true：
  //     - 确定不使用 DeleteRange 的数据库
  //     - 优化读取性能，跳过不必要的检查
  //   - false：
  //     - 正常使用 DeleteRange 的数据库
  //     - 确保正确性
  // 重要说明：
  //   - 如果错误地设置为 true，可能读取到过期数据
  //   - 只在确定不使用 DeleteRange 时启用
  //   - 范围删除标记用于 DeleteRange() 操作
  bool ignore_range_deletions = false;

  // 异步 I/O（实验性功能）
  // 功能：启用异步 I/O 进行数据预取
  // 默认值：false
  // 工作原理：
  //   - false：同步读取数据
  //   - true：RocksDB 异步预取数据
  //     - 在顺序读取时自动启用
  //     - 利用内部自动预取机制
  // 使用场景：
  //   - 顺序扫描：大量数据的顺序读取
  //   - 高吞吐场景：需要最大化 I/O 并行度
  //   - 低延迟要求：减少等待时间
  // 重要说明：
  //   - 实验性功能，API 可能会变化
  //   - 适用于顺序读取模式
  //   - 可能增加一定的资源消耗
  bool async_io = false;

  // 优化 MultiGet 的 I/O（实验性功能）
  // 功能：控制是否在多个层级异步读取 SST 文件
  // 默认值：true
  // 工作原理：
  //   - true（async_io 启用时）：
  //     - 并行读取不同层级的 SST 文件
  //     - 最大化 MultiGet 批量查询的并行度
  //     - 适用于 MultiGet 中的键在不同层级
  //   - false：串行读取
  // 性能影响：
  //   - true：降低 MultiGet 延迟，增加 CPU 开销
  //   - false：降低 CPU 开销，可能增加延迟
  // 使用场景：
  //   - true：MultiGet 键分布在不同层级
  //   - false：CPU 资源受限，可以容忍稍高延迟
  // 重要说明：
  //   - 实验性功能
  //   - 只在 async_io=true 时生效
  //   - 键在同一层级时收益较小
  bool optimize_multiget_for_io = true;

  // *** 适用于点查询和扫描的选项结束 ***
  // *** 仅适用于迭代器或扫描的选项开始 ***

  // 预读大小
  // 功能：控制迭代器的预读大小，提高顺序扫描性能
  // 默认值：0（使用自动预读）
  // 单位：字节
  // 工作原理：
  //   - 0：使用 RocksDB 的自动预读
  //     - 检测到同一 SST 文件超过 2 次读取后启动预读
  //     - 起始大小 8KB，每次额外读取翻倍，最大 256KB
  //   - 非 0：使用指定的预读大小
  //     - 适用于已知的大范围扫描
  // 性能影响：
  //   - 合理设置（> 2MB）：
  //     - 在机械硬盘上显著提高正向迭代性能
  //     - 减少磁盘寻址次数
  //   - 过大：浪费内存和 I/O 带宽
  // 使用场景：
  //   - 大范围扫描：遍历大量连续数据
  //   - 机械硬盘：需要减少 I/O 延迟
  //   - 已知扫描模式：可以准确预测预读大小
  // 重要说明：
  //   - 适用于顺序读取模式
  //   - 对随机访问帮助不大
  //   - 与 readahead_size = 0 相比，自动预读更灵活
  size_t readahead_size = 0;

  // 可跳过内部键的最大数量
  // 功能：限制迭代器 seek 时可跳过的内部键数量
  // 默认值：0（从不因跳过多键而失败）
  // 工作原理：
  //   - 0：不管跳过多少键，都继续操作
  //   - 非 0：跳过超过此数量时返回 Status::Incomplete
  //   - 内部键：包括删除标记、合并操作等元数据
  // 使用场景：
  //   - 0（推荐）：
  //     - 需要确保操作最终完成
  //     - 可以容忍较长等待时间
  //   - 非 0：
  //     - 避免长时间阻塞
  //     - 快速失败，由应用重试
  // 重要说明：
  //   - 只影响迭代器的 seek 操作
  //   - 不影响点查询
  uint64_t max_skippable_internal_keys = 0;

  // 迭代下界（包含）
  // 功能：定义反向迭代器可以返回的最小键
  // 默认值：nullptr（无下界限制）
  // 工作原理：
  //   - nullptr：无限制，可以迭代到最小键
  //   - 非空：
  //     - 边界值是有效的（包含）
  //     - 超过边界后 Valid() 返回 false
  // 前提条件：
  //   - prefix_extractor 非空：
  //     - Seek 目标和 iterate_lower_bound 必须有相同前缀
  //     - 前缀域外不保证顺序
  //   - 使用时间戳：
  //     - 应指向不带时间戳部分的键
  // 使用场景：
  //   - 限制迭代范围：只迭代特定范围内的数据
  //   - 反向迭代：控制反向迭代的起始点
  //   - 分页查询：避免读取不需要的数据
  // 重要说明：
  //   - 只影响反向迭代
  //   - 与 iterate_upper_bound 配合使用可定义完整范围
  const Slice* iterate_lower_bound = nullptr;

  // 迭代上界（不包含）
  // 功能：定义正向迭代器可以返回的最大键
  // 默认值：nullptr（无上界限制）
  // 工作原理：
  //   - nullptr：无限制，可以迭代到最大键
  //   - 非空：
  //     - 边界值是无效的（不包含）
  //     - 到达边界后 Valid() 返回 false
  // 前提条件：
  //   - prefix_extractor 非空：
  //     - auto_prefix_mode = true：
  //       - 通过比较 iterate_upper_bound 和 seek 键推断是否使用前缀迭代
  //       - 可以应用前缀布隆过滤器
  //     - auto_prefix_mode = false：
  //       - 只有与 seek 键相同前缀时才生效
  //       - 超出前缀范围的键返回未定义
  //   - SeekToLast()：
  //     - 将迭代器定位到小于 iterate_upper_bound 的第一个键
  // 使用场景：
  //   - 限制迭代范围：避免读取不需要的数据
  //   - 分页查询：高效获取特定范围
  //   - 前缀查询：优化前缀过滤性能
  // 重要说明：
  //   - 只影响正向迭代
  //   - 与 iterate_lower_bound 配合定义完整范围
  const Slice* iterate_upper_bound = nullptr;

  // 尾部迭代器
  // 功能：创建特殊的尾部迭代器，可以看到新增数据
  // 默认值：false
  // 工作原理：
  //   - false（普通迭代器）：
  //     - 只能看到创建时的数据视图
  //     - 创建后的新数据不可见
  //   - true（尾部迭代器）：
  //     - 可以看到完整的数据库视图
  //     - 可以读取迭代器创建后插入的数据
  //     - 针对顺序读取优化
  // 使用场景：
  //   - 实时监控：持续读取新写入的数据
  //   - 流处理：处理持续写入的数据流
  //   - 增量扫描：只读取新增数据
  // 重要说明：
  //   - 适用于顺序读取
  //   - 可能比普通迭代器有更高的开销
  //   - 不需要重新创建迭代器即可读取新数据
  bool tailing = false;

  // 托管模式（已废弃）
  // 功能：此选项不再使用
  // 默认值：false
  // 重要说明：
  //   - DEPRECATED：已废弃
  //   - 原用于启用已移除的功能
  //   - 保留仅为兼容性
  bool managed = false;

  // 全局顺序查找
  // 功能：无论索引格式（如哈希索引）如何，都启用全局顺序查找
  // 默认值：false
  // 工作原理：
  //   - false：使用表索引的默认查找模式
  //     - 哈希索引：只查找相同前缀的键
  //     - 可以利用前缀优化（如前缀布隆过滤器）
  //   - true：强制全局顺序查找
  //     - 忽略索引格式限制
  //     - Get() 时跳过前缀布隆过滤器
  // 限制：
  //   - 某些表格式（如 PlainTable）可能不支持此选项
  // 使用场景：
  //   - false：
  //     - 需要高效的前缀查询
  //     - 使用哈希索引优化
  //   - true：
  //     - 需要跨前缀的查询
  //     - 不确定前缀结构的查询
  //     - 确保不遗漏任何键
  // 重要说明：
  //   - Get() 时设置为 true 会跳过前缀布隆过滤器
  //   - 可能降低性能但提高查询覆盖率
  bool total_order_seek = false;

  // 自动前缀模式
  // 功能：智能选择前缀查找或全局顺序查找
  // 默认值：false
  // 工作原理：
  //   - false：默认 total_order_seek = false
  //     - 使用前缀查找模式
  //   - true：默认 total_order_seek = true
  //     - RocksDB 根据查找键和迭代器上界智能选择
  //     - 如果不会产生不同结果，启用前缀查找模式
  // 已知 BUG：
  //   - 使用 Comparator::IsSameLengthImmediateSuccessor 和
  //     SliceTransform::FullLengthEnabled 在前缀不一致时启用前缀模式有缺陷
  //   - 可能会遗漏 "短键"（短于 "完整长度" 前缀的键）
  //   - 参见 DBTest2::AutoPrefixMode1 中的 BUG 示例
  // 安全条件：
  //   - 数据库中没有短键，或不期望返回短键
  //   - Comparator::IsSameLengthImmediateSuccessor 的新条件已满足
  // 使用场景：
  //   - true：希望 RocksDB 自动优化查找模式
  //   - false：显式控制查找模式
  // 重要说明：
  //   - 只在前缀提取器非空时有效
  //   - 有已知 BUG，使用时需谨慎
  //   - 如果没有短键，可以安全使用
  bool auto_prefix_mode = false;

  // 前缀与起始相同
  // 功能：强制迭代器只在起始键的前缀范围内迭代
  // 默认值：false
  // 工作原理：
  //   - false：可以迭代超出起始前缀的范围
  //   - true：
  //     - 只迭代与 seek 键相同前缀的键
  //     - 在前缀范围内支持双向迭代（Next 和 Prev）
  // 前提条件：
  //   - 只对前缀查找有效：
  //     - prefix_extractor 非空
  //     - total_order_seek = false
  // 与 iterate_upper_bound 的区别：
  //   - iterate_upper_bound：单向（只限制正向迭代）
  //   - prefix_same_as_start：双向（Next 和 Prev 都受限）
  // 使用场景：
  //   - 前缀范围查询：只查询特定前缀的数据
  //   - 双向前缀遍历：需要前缀内的 Next 和 Prev
  //   - 前缀聚合：计算前缀相关的统计数据
  // 重要说明：
  //   - 只在前缀查找模式下生效
  //   - 确保不读取其他前缀的数据
  bool prefix_same_as_start = false;

  // 固定数据
  // 功能：将迭代器加载的数据块固定在内存中
  // 默认值：false
  // 工作原理：
  //   - false：
  //     - 数据块可以按正常策略被驱逐
  //     - 可能被驱逐到磁盘
  //   - true：
  //     - 迭代器期间数据块保持在内存
  //     - 迭代器删除后才释放
  // 特殊保证：
  //   - 与 BlockBasedTableOptions::use_delta_encoding = false 配合使用
  //   - Iterator::GetProperty("rocksdb.iterator.is-key-pinned") 保证返回 1
  // 使用场景：
  //   - 需要直接访问数据：避免数据被驱逐
  //   - 高频访问：减少重复加载
  //   - 零拷贝访问：直接访问数据而不复制
  // 重要说明：
  //   - 增加内存使用
  //   - 迭代器删除前不会释放内存
  //   - 适用于需要数据指针保持有效的场景
  bool pin_data = false;

  // 自适应预读
  // 功能：启用增强的自适应预读策略
  // 默认值：false
  // 工作原理：
  //   - false：使用基本的自动预读
  //     - 检测到超过 2 次顺序读取后启动
  //     - 起始 8KB，翻倍增长至 256KB
  //     - 每个层级切换文件时重新开始
  //   - true：使用增强的自适应预读
  //     - 更智能的预取策略
  //     - 根据读取模式动态调整
  // 增强功能：
  //   - 更精确的预取大小计算
  //   - 更好的顺序读取检测
  //   - 减少不必要的预取
  // 使用场景：
  //   - true：复杂的顺序扫描模式
  //   - false：简单的顺序扫描
  // 重要说明：
  //   - 与 readahead_size 配合使用
  //   - 适用于迭代器扫描
  //   - 只在读取为顺序时才生效
  bool adaptive_readahead = false;

  // 迭代器清理时后台删除过期文件
  // 功能：在迭代器清理时，将删除过期文件的操作放入后台
  // 默认值：false
  // 工作原理：
  //   - false：
  //     - 清理迭代器时同步删除过期文件
  //     - 可能阻塞清理操作
  //   - true：
  //     - 在 CleanupIteratorState 中调用 PurgeObsoleteFile
  //     - 将删除操作调度到后台任务队列
  //     - 异步删除，不阻塞清理操作
  // 使用场景：
  //   - true：
  //     - 需要快速清理迭代器
  //     - 可以容忍延迟删除
  //   - false：
  //     - 需要立即释放资源
  //     - 清理操作不是性能瓶颈
  // 重要说明：
  //   - 减少清理延迟
  //   - 增加后台任务队列负载
  //   - 使用 flush 任务队列执行
  bool background_purge_on_iterator_cleanup = false;

  // 表过滤回调
  // 功能：根据表的属性决定是否扫描该表
  // 默认值：空（扫描所有表）
  // 工作原理：
  //   - 空：迭代时扫描所有表
  //   - 非空：
  //     - 迭代过程中传递每个表的属性
  //     - 调用回调函数判断是否扫描
  //     - 返回 false：跳过该表
  //   - 应用可以基于自定义逻辑过滤
  // 使用场景：
  //   - 时间范围过滤：跳过不在时间范围内的表
  //   - 自定义属性：基于表的元数据过滤
  //   - 性能优化：跳过不相关的表
  // 重要说明：
  //   - 只影响迭代器扫描
  //   - 不影响点查询（Get、MultiGet）
  //   - 回调会被频繁调用，需要高效实现
  std::function<bool(const TableProperties&)> table_filter;

  // *** END options only relevant to iterators or scans ***

  // ** For RocksDB internal use only **
  Env::IOActivity io_activity = Env::IOActivity::kUnknown;

  ReadOptions() {}
  ReadOptions(bool _verify_checksums, bool _fill_cache);
  explicit ReadOptions(Env::IOActivity _io_activity);
};

// Options that control write operations
struct WriteOptions {
  // 同步写入到磁盘
  // 功能：在写入被视为完成之前，将数据从操作系统缓冲区同步到磁盘
  // 默认值：false
  // 工作原理：
  //   - true：调用 WritableFile::Sync() 确保数据持久化到磁盘
  //   - false：数据写入操作系统缓冲区后即返回，不等待磁盘同步
  // 崩溃语义：
  //   - sync=true：类似 write() 系统调用后跟 fdatasync()，保证持久化
  //   - sync=false：类似 write() 系统调用，可能丢失最近写入
  //   - 机器崩溃：sync=false 可能丢失最近写入
  //   - 进程崩溃（机器不重启）：无论 sync 值如何，都不会丢失数据
  // 使用场景：
  //   - 关键数据：设置为 true 确持久化
  //   - 高吞吐量：设置为 false 提高性能
  //   - 批量导入：批量写入后手动 SyncWAL()
  // 重要说明：
  //   - 设置为 true 会显著降低写入性能
  //   - 通过持久性保证换取性能
  //   - 可以使用 DB::SyncWAL() 手动同步
  bool sync;

  // 禁用 WAL（预写日志）
  // 功能：写入不先进入 WAL，崩溃时可能丢失
  // 默认值：false
  // 工作原理：
  //   - false：写入先写入 WAL，再写入 memtable
  //   - true：直接写入 memtable，跳过 WAL
  // 风险：
  //   - 崩溃后 memtable 中未 flush 的数据会丢失
  //   - 备份引擎依赖 WAL，禁用 WAL 时备份会丢失未 flush 的数据
  // 使用场景：
  //   - 临时数据：可容忍数据丢失
  //   - 批量导入：手动 flush 后禁用 WAL 提高性能
  //   - 特殊场景：备份时设置 flush_before_backup=true
  // 重要说明：
  //   - 禁用 WAL 会提高写入性能
  //   - 数据持久性保证降低
  //   - 备份时必须设置 flush_before_backup=true
  //   - 对于关键数据不建议禁用
  bool disableWAL;

  // 忽略不存在的列族
  // 功能：写入到已删除的列族时不返回错误
  // 默认值：false
  // 工作原理：
  //   - false：写入不存在的列族时返回错误
  //   - true：跳过对不存在列族的写入，继续处理其他写入
  // 批处理行为：
  //   - WriteBatch 中包含多个写入时
  //   - 启用后：只有不存在的列族的写入被跳过，其他写入成功
  // 使用场景：
  //   - 容错写入：允许部分失败
  //   - 迁移场景：列族可能被删除
  //   - 灵活的数据处理
  // 重要说明：
  //   - 可能隐藏错误
  //   - 需要应用层处理部分失败
  //   - 通常用于容错场景
  bool ignore_missing_column_families;

  // 不等待减速
  // 功能：当写入需要等待或减速时，立即失败
  // 默认值：false
  // 工作原理：
  //   - false：当 memtable 满或 compaction 落后时，写入会等待或减速
  //   - true：遇到减速条件时立即返回 Status::Incomplete()
  // 触发条件：
  //   - memtable 数量达到限制
  //   - Level0 文件过多导致需要放缓写入
  //   - pending compaction bytes 超过阈值
  // 使用场景：
  //   - 非关键写入：失败可以重试
  //   - 低延迟要求：不能容忍等待
  //   - 实时系统：快速失败优于慢速响应
  // 重要说明：
  //   - 返回 Status::Incomplete() 表示需要稍后重试
  //   - 应用需要处理此状态并重试
  //   - 与 low_pri 配合使用
  bool no_slowdown;

  // 低优先级写入
  // 功能：此写入请求在 compaction 落后时具有较低优先级
  // 默认值：false
  // 工作原理：
  //   - false：正常优先级写入
  //   - true：低优先级写入
  //     - 如果 no_slowdown=true：立即返回 Status::Incomplete()
  //     - 如果 no_slowdown=false：被 RocksDB 减速
  // 减速策略：
  //   - RocksDB 自动计算减速值
  //   - 确保对高优先级写入的影响最小
  //   - 可能显著降低写入速度
  // 使用场景：
  //   - 后台任务：不影响前台性能
  //   - 非实时写入：可容忍延迟
  //   - 批量操作：可以暂停
  // 重要说明：
  //   - 配合 no_slowdown 使用控制行为
  //   - 适用于混合负载场景
  //   - 高优先级写入不受影响
  bool low_pri;

  // 每批次 memtable 插入提示
  // 功能：在并发写入时维护每个 memtable 的最后插入位置作为提示
  // 默认值：false
  // 工作原理：
  //   - false：每次写入都查找插入位置
  //   - true：在 WriteBatch 中缓存插入位置提示
  //   - 并发写入时从提示位置开始查找
  // 性能影响：
  //   - 顺序键：显著提高并发写入性能
  //   - 随机键：可能没有明显效果
  //   - 非并发写入：此选项被忽略
  // 前提条件：
  //   - allow_concurrent_memtable_write=true
  //   - WriteBatch 中的键是顺序的
  // 使用场景：
  //   - 批量顺序写入：如时间序列数据
  //   - 高并发场景：多个线程同时写入
  //   - 顺序键插入：如自增 ID
  // 重要说明：
  //   - 只在并发 memtable 写入时生效
  //   - 对随机键没有帮助
  //   - 可能增加一些内存开销
  bool memtable_insert_hint_per_batch;

  // 速率限制器优先级
  // 功能：指定此写入对内部速率限制器的计费优先级
  // 默认值：Env::IO_TOTAL
  // 可选值：
  //   - Env::IO_USER：用户 I/O 优先级
  //   - Env::IO_TOTAL：禁用速率限制器计费（默认）
  // 工作原理：
  //   - 控制写入对 DBOptions::rate_limiter 的使用方式
  //   - 影响写入速度和资源分配
  // 支持场景：
  //   - 自动 WAL flush
  //   - 实时更新（Put()、Write()、Delete() 等）
  //   - disableWAL == false
  //   - manual_wal_flush == false
  // 使用场景：
  //   - 混合负载：区分高优先级和低优先级写入
  //   - 资源隔离：控制不同类型写入的资源使用
  //   - IO_TOTAL：不受限制的写入
  // 重要说明：
  //   - 由于实现限制，只允许 IO_USER 和 IO_TOTAL
  //   - 需要设置 DBOptions::rate_limiter 才生效
  Env::IOPriority rate_limiter_priority;

  // 每个键的保护字节
  // 功能：存储每个键条目的保护信息字节数
  // 默认值：0（禁用）
  // 支持值：
  //   - 0：禁用保护（默认）
  //   - 8：启用 8 字节保护
  // 用途：
  //   - 数据完整性验证
  //   - 防篡改保护
  //   - 安全相关场景
  // 性能影响：
  //   - 增加存储开销（每键 8 字节）
  //   - 增加计算开销
  //   - 可能影响写入和读取性能
  // 使用场景：
  //   - 高安全要求：需要完整性保证
  //   - 防篡改：防止恶意修改
  //   - 审计追踪：验证数据完整性
  // 重要说明：
  //   - 目前只支持 0 和 8
  //   - 需要配合其他安全措施使用
  //   - 通常用于特殊安全场景
  size_t protection_bytes_per_key;

  WriteOptions()
      : sync(false),
        disableWAL(false),
        ignore_missing_column_families(false),
        no_slowdown(false),
        low_pri(false),
        memtable_insert_hint_per_batch(false),
        rate_limiter_priority(Env::IO_TOTAL),
        protection_bytes_per_key(0) {}
};

// 控制 flush 操作的选项
struct FlushOptions {
  // 等待 flush 完成
  // 功能：控制 flush 操作是同步等待还是异步执行
  // 默认值：true
  // 工作原理：
  //   - true：调用阻塞直到 flush 操作完成
  //   - false：立即返回，flush 在后台进行
  // 使用场景：
  //   - wait=true：需要确保数据持久化后再继续
  //     - 关键操作后 flush
  //     - 关闭数据库前 flush
  //     - 需要知道 flush 结果
  //   - wait=false：不阻塞主流程
  //     - 后台定期 flush
  //     - 异步处理
  //     - 不需要立即结果
  // 重要说明：
  //   - wait=true 时可能阻塞较长时间
  //   - wait=false 时无法立即知道是否成功
  //   - Flush 完成前数据可能仍然在 memtable
  bool wait;

  // 允许写入停顿
  // 功能：控制 flush 是否立即执行，即使会导致写入停顿
  // 默认值：false
  // 工作原理：
  //   - false：等待直到可以 flush 且不导致写入停顿
  //     - 由其他线程（前台或后台）执行 flush
  //     - 或等待条件满足
  //   - true：立即开始 flush
  //     - 写入在整个 flush 期间会停顿
  //     - 可能影响写入性能
  // 触发写入停顿的条件：
  //   - memtable 数量达到上限
  //   - Level0 文件过多
  //   - 正在执行 flush
  // 使用场景：
  //   - allow_write_stall=true：
  //     - 需要立即 flush（如关闭前）
  //     - 可以容忍写入停顿
  //     - 确保尽快释放 memtable 内存
  //   - allow_write_stall=false：
  //     - 高写入负载：避免停顿影响性能
  //     - 等待后台线程执行 flush
  //     - 保持写入流畅
  // 重要说明：
  //   - 设置为 true 可能显著降低写入吞吐量
  //   - 设置为 false 可能需要等待
  //   - 与 wait 配合使用控制行为
  bool allow_write_stall;

  FlushOptions() : wait(true), allow_write_stall(false) {}
};

// Create a Logger from provided DBOptions
extern Status CreateLoggerFromOptions(const std::string& dbname,
                                      const DBOptions& options,
                                      std::shared_ptr<Logger>* logger);

// CompactionOptions are used in CompactFiles() call.
struct CompactionOptions {
  // Compaction output compression type
  // Default: snappy
  // If set to `kDisableCompressionOption`, RocksDB will choose compression type
  // according to the `ColumnFamilyOptions`, taking into account the output
  // level if `compression_per_level` is specified.
  CompressionType compression;
  // Compaction will create files of size `output_file_size_limit`.
  // Default: MAX, which means that compaction will create a single file
  uint64_t output_file_size_limit;
  // If > 0, it will replace the option in the DBOptions for this compaction.
  uint32_t max_subcompactions;

  CompactionOptions()
      : compression(kSnappyCompression),
        output_file_size_limit(std::numeric_limits<uint64_t>::max()),
        max_subcompactions(0) {}
};

// For level based compaction, we can configure if we want to skip/force
// bottommost level compaction.
enum class BottommostLevelCompaction {
  // Skip bottommost level compaction.
  kSkip,
  // Only compact bottommost level if there is a compaction filter.
  // This is the default option.
  // Similar to kForceOptimized, when compacting bottommost level, avoid
  // double-compacting files
  // created in the same manual compaction.
  kIfHaveCompactionFilter,
  // Always compact bottommost level.
  kForce,
  // Always compact bottommost level but in bottommost level avoid
  // double-compacting files created in the same compaction.
  kForceOptimized,
};

// For manual compaction, we can configure if we want to skip/force garbage
// collection of blob files.
enum class BlobGarbageCollectionPolicy {
  // Force blob file garbage collection.
  kForce,
  // Skip blob file garbage collection.
  kDisable,
  // Inherit blob file garbage collection policy from ColumnFamilyOptions.
  kUseDefault,
};

// CompactRangeOptions is used by CompactRange() call.
// ===== 手动压缩选项结构体 =====
//
// 该结构体用于配置手动压缩操作的行为
// 通过 DB::CompactRange() 或 DB::CompactRangeWithOptions() 调用
//
// 使用场景：
// 1. 手动触发压缩以优化 LSM 树结构
// 2. 压缩特定键范围
// 3. 强制压缩到底层
// 4. 将数据移动到特定层级
// 5. 垃圾回收（GC） Blob 文件
struct CompactRangeOptions {
  // ===== 排他性压缩选项 =====

  // 排他性手动压缩标志
  // If true, no other compaction will run at the same time as this
  // manual compaction.
  //
  // 功能说明：
  // - true：此压缩为排他性压缩，不会与任何其他压缩（包括后台自动压缩）同时执行
  //       系统会等待所有正在进行的压缩完成后才开始此压缩
  // - false：允许与其他压缩并行执行
  //
  // 使用场景：
  // - 需要确保压缩不被干扰时使用
  // - 调试和测试场景
  //
  // 限制：
  // - 与 canceled 选项配合使用时，取消可能被延迟（需要等待后台压缩完成）
  //
  // Default: false
  bool exclusive_manual_compaction = false;

  // ===== 层级控制选项 =====

  // 改变压缩输出层标志
  // If true, compacted files will be moved to the minimum level capable
  // of holding the data or given level (specified non-negative target_level).
  //
  // 功能说明：
  // - false：压缩按照正常规则选择输出层（遵循压缩策略）
  // - true：将压缩后的文件移动到特定层
  //   - 如果 target_level < 0，移动到能够容纳数据的最小层
  //   - 如果 target_level >= 0，移动到指定的 target_level
  //
  // 使用场景：
  // - 将数据强制移动到特定层级
  // - 手动调整 LSM 树结构
  // - 数据分层存储策略
  //
  // 注意：此选项通常与 target_level 配合使用
  bool change_level = false;

  // 目标层号
  // If change_level is true and target_level have non-negative value, compacted
  // files will be moved to target_level.
  //
  // 功能说明：
  // - 当 change_level = true 且 target_level >= 0 时有效
  // - 指定压缩输出文件的目标层号
  // - 目标层必须能够容纳压缩的数据范围
  //
  // 特殊值：
  // - -1：不指定特定层，使用默认行为（通常根据数据大小和压缩策略决定）
  // - 0：移动到 L0 层
  // - 1+：移动到对应层号
  //
  // 使用场景：
  // - 将热点数据移动到上层（提高查询性能）
  // - 将冷数据移动到底层（降低存储成本）
  // - 手动控制数据分布
  //
  // Default: -1
  int target_level = -1;

  // ===== 存储路径选项 =====

  // 目标存储路径 ID
  // Compaction outputs will be placed in options.db_paths[target_path_id].
  // Behavior is undefined if target_path_id is out of range.
  //
  // 功能说明：
  // - 指定压缩输出文件存储在哪个目录
  // - 目录路径通过 ColumnFamilyOptions::db_paths 配置
  // - 支持多磁盘存储场景
  //
  // 多磁盘场景：
  // - 可以将不同层的数据存储在不同磁盘上
  // - 例如：L0-L2 在磁盘 A，L3-L6 在磁盘 B
  // - 通过 target_path_id 选择输出磁盘
  //
  // 注意：
  // - 如果 target_path_id 超出范围，行为未定义
  // - 确保路径存在且有足够的磁盘空间
  //
  // Default: 0（第一个路径）
  uint32_t target_path_id = 0;

  // ===== 底层压缩选项 =====

  // 最底层压缩策略
  // By default level based compaction will only compact the bottommost level
  // if there is a compaction filter
  //
  // 功能说明：
  // 控制是否压缩 LSM 树的最底层（bottommost level）
  // 最底层是数据的最终落地点，压缩后不会再被移动
  //
  // 可选值（枚举类型 BottommostLevelCompaction）：
  // - kSkip：跳过最底层压缩
  //   - 即使有 compaction filter 也不压缩最底层
  //   - 适用于不需要改变历史数据的场景
  //
  // - kIfHaveCompactionFilter：仅当有压缩过滤器时压缩（默认值）
  //   - 如果配置了 CompactionFilter，则压缩最底层
  //   - 通过过滤器可以删除过期数据或修改数据
  //
  // - kForce：强制压缩最底层
  //   - 无论是否有过滤器都压缩最底层
  //   - 适用于需要重写最底层文件的场景
  //
  // 使用场景：
  // - 数据过期/清理（需要过滤器）
  // - 压缩碎片化数据
  // - 更改压缩算法
  // - 迁移数据到新格式
  //
  // 性能影响：
  // - 最底层压缩涉及大量数据 I/O，耗时较长
  // - 谨慎使用 kForce，可能影响写入性能
  //
  // Default: kIfHaveCompactionFilter
  BottommostLevelCompaction bottommost_level_compaction =
      BottommostLevelCompaction::kIfHaveCompactionFilter;

  // ===== 写入控制选项 =====

  // 允许写入停滞标志
  // If true, will execute immediately even if doing so would cause the DB to
  // enter write stall mode. Otherwise, it'll sleep until load is low enough.
  //
  // 功能说明：
  // - true：立即执行压缩，即使会导致数据库进入写入停滞（stall）模式
  // - false：等待系统负载降低后再执行压缩
  //
  // 写入停滞（Write Stall）：
  // - 当系统资源紧张时（如 memtable 满、压缩队列满），RocksDB 会暂停写入
  // - 目的是保护系统，防止资源耗尽
  // - 停滞会显著降低写入性能
  //
  // 使用场景：
  // - 紧急压缩（需要立即执行，即使影响性能）
  // - 非紧急场景（false，避免影响正常写入）
  //
  // 注意：
  // - 设置为 true 可能导致写入延迟增加
  // - 生产环境通常使用 false
  //
  // Default: false
  bool allow_write_stall = false;

  // ===== 并发控制选项 =====

  // 最大子压缩数量
  // If > 0, it will replace the option in the DBOptions for this compaction.
  //
  // 功能说明：
  // - 子压缩（Subcompaction）是将大压缩任务拆分为多个小任务的技术
  // - 每个子压缩独立处理一个键范围
  // - 利用多核 CPU 并行执行
  //
  // 值的含义：
  // - 0：使用 DBOptions::max_subcompactions 的默认值
  // - > 0：覆盖默认值，使用指定值
  //
  // 性能影响：
  // - 增加子压缩数量可以提高 CPU 利用率
  // - 但过多的子压缩会增加线程同步开销
  // - 通常设置为 CPU 核心数或稍少
  //
  // 使用场景：
  // - 大范围压缩（数据量大）
  // - 多核 CPU 服务器
  // - 需要快速完成压缩的场景
  //
  // 注意：
  // - 只对本次压缩生效，不影响后续压缩
  // - 太大的值可能导致内存不足
  //
  // Default: 0（使用全局配置）
  uint32_t max_subcompactions = 0;

  // ===== 时间戳选项 =====

  // 全历史时间戳下界
  // Set user-defined timestamp low bound, the data with older timestamp than
  // low bound maybe GCed by compaction. Default: nullptr
  //
  // 功能说明：
  // - RocksDB 支持用户定义的时间戳（User-Defined Timestamp）
  // - 时间戳用于保留数据的版本信息
  // - 此选项指定时间戳的下界（保留时间戳 >= 此值的键）
  //
  // 垃圾回收（GC）：
  // - 时间戳 < low_bound 的数据可能被压缩删除
  // - 用于清理过期或不再需要的历史数据
  //
  // 使用场景：
  // - 基于时间的 TTL（Time-To-Live）
  // - 数据保留策略
  // - 历史数据归档
  //
  // 注意：
  // - 必须启用时间戳功能（CFOptions::preserve_deletes）
  // - 时间戳格式必须与配置一致
  //
  // Default: nullptr（不设置时间戳下界）
  const Slice* full_history_ts_low = nullptr;

  // ===== 取消控制选项 =====

  // 取消标志（原子指针）
  // Allows cancellation of an in-progress manual compaction.
  //
  // 功能说明：
  // - 指向一个原子布尔变量
  // - 压缩线程会定期检查此变量
  // - 如果设置为 true，压缩会尝试取消
  //
  // 使用方式：
  // ```cpp
  // std::atomic<bool> canceled(false);
  // CompactRangeOptions opts;
  // opts.canceled = &canceled;
  // std::thread t([&]() { db->CompactRange(opts, ...); });
  // // 稍后取消
  // canceled = true;
  // t.join();
  // ```
  //
  // 取消延迟：
  // Cancellation can be delayed waiting on automatic compactions when used
  // together with `exclusive_manual_compaction == true`.
  // - 如果 exclusive_manual_compaction = true，取消可能被延迟
  // - 需要等待所有后台压缩完成才能真正开始压缩
  //
  // 系统覆盖：
  // NOTE: Calling DisableManualCompaction() overwrites the uer-provided
  // canceled variable in CompactRangeOptions.
  // - DisableManualCompaction() 也会设置取消标志
  // - 典型场景：
  //   - 线程 t1 调用 CompactRange(canceled = false)
  //   - 线程 t2 调用 DisableManualCompaction()
  //   - 手动压缩会被正常禁用
  //   - 压缩迭代器可能在取消前扫描一些数据
  //
  // Default: nullptr（不支持取消）
  std::atomic<bool>* canceled = nullptr;

  // ===== Blob 文件垃圾回收选项 =====

  // Blob 垃圾回收策略
  // If set to kForce, RocksDB will override enable_blob_file_garbage_collection
  // to true; if set to kDisable, RocksDB will override it to false, and
  // kUseDefault leaves the setting in effect. This enables customers to both
  // force-enable and force-disable GC when calling CompactRange.
  //
  // Blob 文件简介：
  // - Blob 文件用于存储大值（large values）
  // - 与 SSTable 分离存储，减少 SSTable 大小
  // - 可以独立进行垃圾回收（GC）
  //
  // 可选值（枚举类型 BlobGarbageCollectionPolicy）：
  // - kUseDefault：使用 ColumnFamilyOptions::enable_blob_file_garbage_collection 的默认设置
  //   - 遵全局配置的 enable_blob_file_garbage_collection 开关
  //
  // - kForce：强制启用 Blob GC
  //   - 覆盖全局配置 enable_blob_file_garbage_collection = false
  //   - 在本次压缩中执行 Blob GC
  //
  // - kDisable：强制禁用 Blob GC
  //   - 覆盖全局配置 enable_blob_file_garbage_collection = true
  //   - 本次压缩不执行 Blob GC
  //
  // 使用场景：
  // - kForce：需要立即清理 Blob 文件的无效空间
  // - kDisable：避免在压缩时进行 Blob GC（节省 I/O）
  // - kUseDefault：按配置正常行为
  //
  // 注意：
  // - 只影响本次压缩
  // - 不影响全局配置 enable_blob_file_garbage_collection
  //
  // Default: kUseDefault
  BlobGarbageCollectionPolicy blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kUseDefault;

  // Blob 垃圾回收年龄截止值
  // If set to < 0 or > 1, RocksDB leaves blob_garbage_collection_age_cutoff
  // from ColumnFamilyOptions in effect. Otherwise, it will override the
  // user-provided setting. This enables customers to selectively override the
  // age cutoff.
  //
  // 功能说明：
  // - 控制 Blob GC 时哪些 Blob 文件被处理
  // - 基于文件的创建时间或最古老祖先时间
  // - 值为 0~1 之间的比例，表示文件年龄占最大年龄的比例
  //
  // 值的含义：
  // - < 0 或 > 1：使用 ColumnFamilyOptions::blob_garbage_collection_age_cutoff 的默认值
  // - 0.0：只处理最新的 Blob 文件
  // - 0.5：处理年龄 <= 最大年龄 50% 的文件
  // - 1.0：处理所有 Blob 文件
  //
  // 使用场景：
  // - 0.5~1.0：激进清理，回收更多空间
  // - 0.0~0.5：保守清理，保留较新的 Blob 文件
  // - -1：使用默认行为
  //
  // 性能权衡：
  // - 较高的值（接近 1.0）：回收更多空间，但 I/O 开销大
  // - 较低的值（接近 0.0）：I/O 开销小，但空间回收少
  //
  // 注意：
  // - 只影响本次压缩
  // - 与 blob_garbage_collection_policy 配合使用
  // - 不影响全局配置 blob_garbage_collection_age_cutoff
  //
  // Default: -1（使用全局配置）
  double blob_garbage_collection_age_cutoff = -1;
};

// IngestExternalFileOptions is used by IngestExternalFile()
struct IngestExternalFileOptions {
  // Can be set to true to move the files instead of copying them.
  bool move_files = false;
  // If set to true, ingestion falls back to copy when move fails.
  bool failed_move_fall_back_to_copy = true;
  // If set to false, an ingested file keys could appear in existing snapshots
  // that where created before the file was ingested.
  bool snapshot_consistency = true;
  // If set to false, IngestExternalFile() will fail if the file key range
  // overlaps with existing keys or tombstones or output of ongoing compaction
  // during file ingestion in the DB (the conditions under which a global_seqno
  // must be assigned to the ingested file).
  bool allow_global_seqno = true;
  // If set to false and the file key range overlaps with the memtable key range
  // (memtable flush required), IngestExternalFile will fail.
  bool allow_blocking_flush = true;
  // Set to true if you would like duplicate keys in the file being ingested
  // to be skipped rather than overwriting existing data under that key.
  // Use case: back-fill of some historical data in the database without
  // over-writing existing newer version of data.
  // This option could only be used if the DB has been running
  // with allow_ingest_behind=true since the dawn of time.
  // All files will be ingested at the bottommost level with seqno=0.
  bool ingest_behind = false;
  // DEPRECATED - Set to true if you would like to write global_seqno to
  // the external SST file on ingestion for backward compatibility before
  // RocksDB 5.16.0. Such old versions of RocksDB expect any global_seqno to
  // be written to the SST file rather than recorded in the DB manifest.
  // This functionality was deprecated because (a) random writes might be
  // costly or unsupported on some FileSystems, and (b) the file checksum
  // changes with such a write.
  bool write_global_seqno = false;
  // Set to true if you would like to verify the checksums of each block of the
  // external SST file before ingestion.
  // Warning: setting this to true causes slowdown in file ingestion because
  // the external SST file has to be read.
  bool verify_checksums_before_ingest = false;
  // When verify_checksums_before_ingest = true, RocksDB uses default
  // readahead setting to scan the file while verifying checksums before
  // ingestion.
  // Users can override the default value using this option.
  // Using a large readahead size (> 2MB) can typically improve the performance
  // of forward iteration on spinning disks.
  size_t verify_checksums_readahead_size = 0;
  // Set to TRUE if user wants to verify the sst file checksum of ingested
  // files. The DB checksum function will generate the checksum of each
  // ingested file (if file_checksum_gen_factory is set) and compare the
  // checksum function name and checksum with the ingested checksum information.
  //
  // If this option is set to True: 1) if DB does not enable checksum
  // (file_checksum_gen_factory == nullptr), the ingested checksum information
  // will be ignored; 2) If DB enable the checksum function, we calculate the
  // sst file checksum after the file is moved or copied and compare the
  // checksum and checksum name. If checksum or checksum function name does
  // not match, ingestion will be failed. If the verification is successful,
  // checksum and checksum function name will be stored in Manifest.
  // If this option is set to FALSE, 1) if DB does not enable checksum,
  // the ingested checksum information will be ignored; 2) if DB enable the
  // checksum, we only verify the ingested checksum function name and we
  // trust the ingested checksum. If the checksum function name matches, we
  // store the checksum in Manifest. DB does not calculate the checksum during
  // ingestion. However, if no checksum information is provided with the
  // ingested files, DB will generate the checksum and store in the Manifest.
  bool verify_file_checksum = true;
  // Set to TRUE if user wants file to be ingested to the bottommost level. An
  // error of Status::TryAgain() will be returned if a file cannot fit in the
  // bottommost level when calling
  // DB::IngestExternalFile()/DB::IngestExternalFiles(). The user should clear
  // the bottommost level in the overlapping range before re-attempt.
  //
  // ingest_behind takes precedence over fail_if_not_bottommost_level.
  bool fail_if_not_bottommost_level = false;
};

enum TraceFilterType : uint64_t {
  // Trace all the operations
  kTraceFilterNone = 0x0,
  // Do not trace the get operations
  kTraceFilterGet = 0x1 << 0,
  // Do not trace the write operations
  kTraceFilterWrite = 0x1 << 1,
  // Do not trace the `Iterator::Seek()` operations
  kTraceFilterIteratorSeek = 0x1 << 2,
  // Do not trace the `Iterator::SeekForPrev()` operations
  kTraceFilterIteratorSeekForPrev = 0x1 << 3,
  // Do not trace the `MultiGet()` operations
  kTraceFilterMultiGet = 0x1 << 4,
};

// TraceOptions is used for StartTrace
struct TraceOptions {
  // To avoid the trace file size grows large than the storage space,
  // user can set the max trace file size in Bytes. Default is 64GB
  uint64_t max_trace_file_size = uint64_t{64} * 1024 * 1024 * 1024;
  // Specify trace sampling option, i.e. capture one per how many requests.
  // Default to 1 (capture every request).
  uint64_t sampling_frequency = 1;
  // Note: The filtering happens before sampling.
  uint64_t filter = kTraceFilterNone;
  // When true, the order of write records in the trace will match the order of
  // the corresponding write records in the WAL and applied to the DB. There may
  // be a performance penalty associated with preserving this ordering.
  //
  // Default: false. This means write records in the trace may be in an order
  // different from the WAL's order.
  bool preserve_write_order = false;
};

// ImportColumnFamilyOptions is used by ImportColumnFamily()
struct ImportColumnFamilyOptions {
  // Can be set to true to move the files instead of copying them.
  bool move_files = false;
};

// Options used with DB::GetApproximateSizes()
struct SizeApproximationOptions {
  // Defines whether the returned size should include the recently written
  // data in the memtables. If set to false, include_files must be true.
  bool include_memtables = false;
  // Defines whether the returned size should include data serialized to disk.
  // If set to false, include_memtables must be true.
  bool include_files = true;
  // When approximating the files total size that is used to store a keys range
  // using DB::GetApproximateSizes, allow approximation with an error margin of
  // up to total_files_size * files_size_error_margin. This allows to take some
  // shortcuts in files size approximation, resulting in better performance,
  // while guaranteeing the resulting error is within a reasonable margin.
  // E.g., if the value is 0.1, then the error margin of the returned files size
  // approximation will be within 10%.
  // If the value is non-positive - a more precise yet more CPU intensive
  // estimation is performed.
  double files_size_error_margin = -1.0;
};

struct CompactionServiceOptionsOverride {
  // Currently pointer configurations are not passed to compaction service
  // compaction so the user needs to set it. It will be removed once pointer
  // configuration passing is supported.
  Env* env = Env::Default();
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory = nullptr;

  const Comparator* comparator = BytewiseComparator();
  std::shared_ptr<MergeOperator> merge_operator = nullptr;
  const CompactionFilter* compaction_filter = nullptr;
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory = nullptr;
  std::shared_ptr<const SliceTransform> prefix_extractor = nullptr;
  std::shared_ptr<TableFactory> table_factory;
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory = nullptr;

  // Only subsets of events are triggered in remote compaction worker, like:
  // `OnTableFileCreated`, `OnTableFileCreationStarted`,
  // `ShouldBeNotifiedOnFileIO` `OnSubcompactionBegin`,
  // `OnSubcompactionCompleted`, etc. Worth mentioning, `OnCompactionBegin` and
  // `OnCompactionCompleted` won't be triggered. They will be triggered on the
  // primary DB side.
  std::vector<std::shared_ptr<EventListener>> listeners;

  // statistics is used to collect DB operation metrics, the metrics won't be
  // returned to CompactionService primary host, to collect that, the user needs
  // to set it here.
  std::shared_ptr<Statistics> statistics = nullptr;

  // Only compaction generated SST files use this user defined table properties
  // collector.
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories;
};

struct OpenAndCompactOptions {
  // Allows cancellation of an in-progress compaction.
  std::atomic<bool>* canceled = nullptr;
};

struct LiveFilesStorageInfoOptions {
  // Whether to populate FileStorageInfo::file_checksum* or leave blank
  bool include_checksum_info = false;
  // Flushes memtables if total size in bytes of live WAL files is >= this
  // number (and DB is not read-only).
  // Default: always force a flush without checking sizes.
  uint64_t wal_size_for_flush = 0;
};

struct WaitForCompactOptions {
  // A boolean to abort waiting in case of a pause (PauseBackgroundWork()
  // called) If true, Status::Aborted will be returned immediately. If false,
  // ContinueBackgroundWork() must be called to resume the background jobs.
  // Otherwise, jobs that were queued, but not scheduled yet may never finish
  // and WaitForCompact() may wait indefinitely.
  bool abort_on_pause = false;

  // A boolean to flush all column families before starting to wait.
  bool flush = false;
};

}  // namespace ROCKSDB_NAMESPACE
