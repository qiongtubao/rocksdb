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
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/dbformat.h"
#include "db/kv_checksum.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/version_edit.h"
#include "memory/allocator.h"
#include "memory/concurrent_arena.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "table/multiget_context.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

struct FlushJobInfo;
class Mutex;
class MemTableIterator;
class MergeContext;
class SystemClock;

// ImmutableMemTableOptions 是 MemTable 的不可变配置选项
// 这些选项在 MemTable 创建后不能修改，贯穿 MemTable 的整个生命周期
// 从 ImmutableOptions 和 MutableCFOptions 中提取相关配置组合而成
struct ImmutableMemTableOptions {
  explicit ImmutableMemTableOptions(const ImmutableOptions& ioptions,
                                    const MutableCFOptions& mutable_cf_options);

  // Arena 块大小
  // 功能：指定 Arena 内存分配器每次分配的块大小
  // 默认值：4096 字节（4 KB）或根据系统页大小调整
  // 工作原理：
  //   - Arena 是 MemTable 使用的内存分配器，用于高效管理内存
  //   - 当当前块用完时，分配新的 arena_block_size 大小的块
  //   - 内存只能分配，不能释放，直到整个 Arena 被销毁
  //   - 适用于大量小对象分配的场景
  // 使用场景：
  //   - 小数据量：4 KB（默认）
  //   - 中等数据量：8 KB - 16 KB
  //   - 大数据量：32 KB - 64 KB
  // 性能影响：
  //   - 较小的值（如 4 KB）：
  //     - 优点：内存使用更精确，浪费少
  //     - 缺点：频繁分配新块，分配开销大
  //   - 较大的值（如 64 KB）：
  //     - 优点：减少分配次数，分配开销小
  //     - 缺点：可能浪费内存，碎片化
  // 计算示例：
  //   - arena_block_size = 4096
  //   - 分配 100 字节：使用 4096 字节块
  //   - 分配 5000 字节：使用 8192 字节块（2 * 4096）
  // 重要说明：
  //   - 值应该是 2 的幂或系统页大小的倍数
  //   - 过大会浪费内存，过小会影响性能
  //   - MemTable 创建后不能修改（Immutable）
  // 系统对齐：
  //   - 通常设置为系统页大小（如 4 KB、8 KB）
  //   - 避免跨页分配，提高内存访问效率
  size_t arena_block_size;

  // MemTable 前缀 Bloom filter 位数
  // 功能：指定 MemTable 前缀 Bloom filter 的位数（每个键）
  // 默认值：0（不使用前缀 Bloom filter）
  // 工作原理：
  //   - Bloom filter 是概率数据结构，用于快速判断键是否可能存在
  //   - 前缀 Bloom filter 只存储键的前缀，而非完整键
  //   - 需要配合 prefix_extractor 使用
  //   - 位数越多，假阳性率越低，但内存占用越大
  // 假阳性率计算：
  //   - 假阳性率 ≈ (1 - e^(-n*m/k))^k
  //   - n = 键的数量
  //   - m = bits_per_key（此参数）
  //   - k = 哈希函数数量（通常为 6）
  // 使用场景：
  //   - 禁用（0）：不使用前缀 Bloom filter
  //   - 低精度（10-15）：内存敏感场景
  //   - 中等精度（16-20）：平衡场景（推荐）
  //   - 高精度（21-30）：对读性能要求极高的场景
  // 性能影响：
  //   - 较小的值：
  //     - 优点：内存占用小
  //     - 缺点：假阳性率高，可能不必要地查询 SST
  //   - 较大的值：
  //     - 优点：假阳性率低，减少不必要的 SST 查询
  //     - 缺点：内存占用大
  // 推荐值与假阳性率对照表：
  //   bits_per_key | 假阳性率
  //   ------------|----------
  //   10          | ~2.0%
  //   12          | ~1.2%
  //   14          | ~0.7%
  //   16          | ~0.4%
  //   18          | ~0.2%
  //   20          | ~0.1%
  // 重要说明：
  //   - 0 表示不使用前缀 Bloom filter
  //   - 需要配合 prefix_extractor 使用
  //   - 只对前缀查询有效，对完整键查询无效
  //   - MemTable 创建后不能修改（Immutable）
  // 示例：
  //   memtable_prefix_bloom_bits = 10  // 约 2% 假阳性率
  //   memtable_prefix_bloom_bits = 20  // 约 0.1% 假阳性率
  uint32_t memtable_prefix_bloom_bits;

  // MemTable 大页大小
  // 功能：指定 MemTable 使用的大页（Huge Page）大小
  // 默认值：0（不使用大页）
  // 工作原理：
  //   - 大页是操作系统提供的超大内存页（如 2 MB、1 GB）
  //   - 减少页表项数量，降低 TLB 缺失
  //   - 提高内存访问性能，特别是对于大内存数据库
  //   - 需要操作系统预分配大页（hugetlbfs）
  // 使用场景：
  //   - 禁用（0）：使用标准页（4 KB）
  //   - 2 MB：常见的大页大小
  //   - 1 GB：超大型数据库
  // 性能影响：
  //   - 启用大页：
  //     - 优点：减少 TLB 缺失，提高内存访问速度
  //     - 缺点：需要预分配，内存碎片化，不易释放
  //   - 不启用大页：
  //     - 优点：灵活，内存管理简单
  //     - 缺点：TLB 缺失较多
  // 前提条件：
  //   - 操作系统支持大页（Linux hugetlbfs）
  //   - 预分配足够的大页
  //   - 有足够的权限使用大页
  // 重要说明：
  //   - 0 表示不使用大页
  //   - 需要操作系统配置配合
  //   - 不适合小内存场景
  //   - MemTable 创建后不能修改（Immutable）
  // 示例：
  //   // Linux 2 MB 大页
  //   memtable_huge_page_size = 2 * 1024 * 1024  // 2 MB
  //
  //   // Linux 1 GB 大页
  //   memtable_huge_page_size = 1024 * 1024 * 1024  // 1 GB
  size_t memtable_huge_page_size;

  // MemTable 完整键过滤
  // 功能：控制 MemTable 的 Bloom filter 是否过滤完整键
  // 默认值：false（只过滤前缀）
  // 工作原理：
  //   - true：Bloom filter 存储完整键
  //   - false：Bloom filter 只存储前缀（需要 prefix_extractor）
  //   - 完整键过滤可以精确过滤，但内存占用更大
  // 使用场景：
  //   - false（默认）：
  //     - 使用前缀过滤
  //     - 需要 prefix_extractor
  //     - 适用于有前缀结构的键
  //   - true：
  //     - 使用完整键过滤
  //     - 不需要 prefix_extractor
  //     - 适用于随机键或无前缀结构
  // 性能影响：
  //   - false（前缀过滤）：
  //     - 优点：内存占用小，过滤粒度较粗
  //     - 缺点：假阳性率较高
  //   - true（完整键过滤）：
  //     - 优点：假阳性率低，过滤精确
  //     - 缺点：内存占用大
  // 计算示例：
  //   - 假设有 100 万个键
  //   - 前缀过滤（prefix = 4 字节）：100 万 * 4 字节 = 4 MB
  //   - 完整键（avg = 16 字节）：100 万 * 16 字节 = 16 MB
  // 重要说明：
  //   - MemTable 创建后不能修改（Immutable）
  //   - 与 memtable_prefix_bloom_bits 配合使用
  //   - 建议根据键的分布选择
  // 示例：
  //   // 键有前缀结构（如 user_id:timestamp）
  //   memtable_whole_key_filtering = false
  //   prefix_extractor = NewFixedPrefixTransform(8)  // 提取 user_id
  //
  //   // 键无前缀结构（如 UUID）
  //   memtable_whole_key_filtering = true
  bool memtable_whole_key_filtering;

  // 支持原地更新
  // 功能：允许 MemTable 支持原地更新操作
  // 默认值：false（不支持）
  // 工作原理：
  //   - 原地更新：在已存在的值上直接修改，不创建新版本
  //   - 节省内存和存储空间
  //   - 需要配合 inplace_callback 使用
  //   - 只在 MemTable 中生效，不适用于 SST 文件
  // 使用场景：
  //   - 频繁更新同一键的值
  //   - 新值大小不大于旧值大小
  //   - 计数器、累加器等场景
  // 性能影响：
  //   - true：
  //     - 优点：减少内存使用，减少 Flush 数据量
  //     - 缺点：增加锁竞争，需要实现回调
  //   - false：
  //     - 优点：简单可靠，无锁竞争
  //     - 缺点：每次更新创建新版本，内存使用高
  // 前提条件：
  //   - 必须实现 inplace_callback
  //   - 新值大小 <= 旧值大小
  //   - MemTable 支持原地更新（如 SkipList 支持）
  // 重要说明：
  //   - MemTable 创建后不能修改（Immutable）
  //   - 实验性功能，可能不稳定
  //   - 原地更新不会持久化，Flush 后失效
  // 示例：
  //   // 计数器场景
  //   inplace_update_support = true
  //   inplace_callback = &CounterCallback
  bool inplace_update_support;

  // 原地更新锁数量
  // 功能：指定原地更新使用的锁的数量
  // 默认值：10000
  // 工作原理：
  //   - 使用分片锁减少锁竞争
  //   - 每个键通过哈希映射到一个锁
  //   - 更新同一键的线程竞争同一个锁
  //   - 更新不同键的线程竞争不同的锁
  // 使用场景：
  //   - 低并发：1000 - 5000
  //   - 中等并发：10000（默认）
  //   - 高并发：20000 - 50000
  // 性能影响：
  //   - 较小的值：
  //     - 优点：锁占用少
  //     - 缺点：锁竞争高，并发性能差
  //   - 较大的值：
  //     - 优点：锁竞争低，并发性能好
  //     - 缺点：锁占用多
  // 计算示例：
  //   - inplace_update_num_locks = 10000
  //   - 哈希函数：hash(key) % 10000
  //   - 假设均匀分布，每个锁平均承担 1/10000 的更新操作
  // 重要说明：
  //   - 应该是质数或 2 的幂
  //   - 值应该 >= 预期的并发更新线程数
  //   - MemTable 创建后不能修改（Immutable）
  //   - 只在 inplace_update_support = true 时有效
  // 示例：
  //   // 高并发场景
  //   inplace_update_num_locks = 10000
  //
  //   // 超高并发场景
  //   inplace_update_num_locks = 50000
  size_t inplace_update_num_locks;

  // 原地更新回调函数
  // 功能：定义如何执行原地更新的回调函数
  // 默认值：nullptr
  // 函数签名：
  //   UpdateStatus (*inplace_callback)(
  //     char* existing_value,              // 已存在的值（可修改）
  //     uint32_t* existing_value_size,    // 已存在的值大小（可修改）
  //     Slice delta_value,                // 新值（增量）
  //     std::string* merged_value          // 合并后的值（如果需要扩展）
  //   )
  // 返回值：
  //   - UPDATE_OK：原地更新成功
  //   - UPDATE_FAILED：原地更新失败
  //   - UPDATE_SKIPPED：跳过更新
  // 工作原理：
  //   - RocksDB 调用此回调执行原地更新
  //   - 如果新值能放入现有值，原地修改
  //   - 如果新值太大，写入 merged_value，RocksDB 创建新版本
  //   - 回调函数必须是线程安全的
  // 使用场景：
  //   - 计数器：existing_value += delta_value
  //   - 拼接：strcat(existing_value, delta_value)
  //   - 自定义逻辑：根据业务需求实现
  // 示例代码：
  //   UpdateStatus CounterInplaceCallback(
  //       char* existing_value, uint32_t* existing_value_size,
  //       Slice delta_value, std::string* merged_value) {
  //     int64_t* counter = reinterpret_cast<int64_t*>(existing_value);
  //     int64_t delta = *reinterpret_cast<const int64_t*>(delta_value.data());
  //     *counter += delta;
  //     return UPDATE_OK;
  //   }
  //
  //   UpdateStatus StringAppendInplaceCallback(
  //       char* existing_value, uint32_t* existing_value_size,
  //       Slice delta_value, std::string* merged_value) {
  //     if (delta_value.size_ > *existing_value_size) {
  //       *merged_value = std::string(existing_value, *existing_value_size);
  //       merged_value->append(delta_value.data_, delta_value.size_);
  //       return UPDATE_FAILED;
  //     }
  //     memcpy(existing_value + *existing_value_size,
  //            delta_value.data_, delta_value.size_);
  //     *existing_value_size += delta_value.size_;
  //     return UPDATE_OK;
  //   }
  // 重要说明：
  //   - 必须配合 inplace_update_support 使用
  //   - 回调函数必须是线程安全的
  //   - MemTable 创建后不能修改（Immutable）
  //   - 回调抛出异常会导致未定义行为
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  // 最大连续合并次数
  // 功能：限制连续调用 Merge 操作的最大次数
  // 默认值：0（无限制）
  // 工作原理：
  //   - 当读取键时，如果键有多个未合并的值
  //   - 需要连续调用 merge_operator 合并这些值
  //   - 此参数限制合并的最大次数
  //   - 超过限制时，返回 Incomplete 状态
  // 使用场景：
  //   - 无限制（0）：适用于大多数场景
  //   - 限制合并（100-1000）：防止恶意键影响性能
  // 性能影响：
  //   - 无限制（0）：
  //     - 优点：总是能读取到最新值
  //     - 缺点：可能被恶意键利用，影响性能
  //   - 限制合并（N）：
  //     - 优点：防止性能问题
  //     - 缺点：可能无法读取到最新值
  // 重要说明：
  //   - 0 表示无限制
  //   - MemTable 创建后不能修改（Immutable）
  //   - 只在使用 merge_operator 时有效
  //   - 建议根据业务场景设置合理值
  // 示例：
  //   // 防止恶意键
  //   max_successive_merges = 1000
  //
  //   // 无限制
  //   max_successive_merges = 0
  size_t max_successive_merges;

  // 统计信息收集器
  // 功能：用于收集和记录各种操作统计信息
  // 默认值：nullptr（不收集统计信息）
  // 工作原理：
  //   - 收集各种操作的统计信息（读、写、压缩等）
  //   - 包括计数、延迟、大小等
  //   - 可以通过 Statistics API 查询
  // 收集的统计信息包括：
  //   - 读操作：Get、MultiGet、Iterator 等
  //   - 写操作：Put、Delete、Merge 等
  //   - 压缩操作：Flush、Compaction 等
  //   - 内存操作：MemTable、Block Cache 等
  //   - 延迟统计：各种操作的延迟分布
  // 使用场景：
  //   - 性能监控：收集统计信息用于监控
  //   - 性能调优：分析瓶颈和热点
  //   - 问题诊断：定位性能问题
  //   - 容量规划：预测资源需求
  // 性能影响：
  //   - 启用统计：
  //     - 优点：提供丰富的性能数据
  //     - 缺点：轻微的性能开销（约 1-3%）
  //   - 不启用统计：
  //     - 优点：性能最佳
  //     - 缺点：无法获取统计信息
  // 重要说明：
  //   - nullptr 表示不收集统计信息
  //   - MemTable 创建后不能修改（Immutable）
  //   - 统计信息是全局的，共享同一 Statistics 对象
  // 示例：
  //   // 创建统计对象
  //   auto statistics = CreateDBStatistics();
  //
  //   // 查询统计信息
  //   std::string stats;
  //   statistics->ToString(&stats);
  Statistics* statistics;

  // 合并操作符
  // 功能：定义如何合并同一键的多个值（用于 Merge() 操作）
  // 默认值：nullptr（不使用合并操作）
  // 工作原理：
  //   - 当调用 DB::Merge(key, value) 时，使用 merge_operator 合并
  //   - 避免读-修改-写的开销，实现原子增量更新
  //   - 合并操作是无序的，必须满足结合律和交换律
  // 使用场景：
  //   - 计数器：Merge(key, "delta")
  //   - 累加器：Merge(key, "addend_value")
  //   - 集合：Merge(key, "new_item")
  // 内置合并操作符：
  //   - UInt64AddOperator：无符号 64 位整数加法
  //   - StringAppendOperator：字符串拼接
  //   - 自定义 MergeOperator：根据业务需求实现
  // 重要说明：
  //   - nullptr 表示不使用合并操作
  //   - MemTable 创建后不能修改（Immutable）
  //   - 合并操作符必须是线程安全的
  // 示例：
  //   // 使用内置加法操作符
  //   options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  //
  //   // 使用自定义操作符
  //   class MyMergeOperator : public MergeOperator {
  //     // 实现 Merge 方法
  //   };
  //   options.merge_operator = std::make_shared<MyMergeOperator>();
  MergeOperator* merge_operator;

  // 信息日志记录器
  // 功能：用于记录 RocksDB 的各种信息和警告
  // 默认值：nullptr（不记录日志）
  // 工作原理：
  //   - 记录 RocksDB 的各种事件和状态
  //   - 包括 INFO、WARN、ERROR 等级别
  //   - 可以输出到文件、控制台等
  // 记录的内容包括：
  //   - 启动和关闭信息
  //   - 压缩和 Flush 信息
  //   - 错误和警告
  //   - 性能和统计信息
  // 使用场景：
  //   - 问题诊断：记录错误和警告
  //   - 性能分析：记录压缩和 Flush 信息
  //   - 运维监控：记录关键事件
  // 性能影响：
  //   - 启用日志：
  //     - 优点：提供丰富的诊断信息
  //     - 缺点：轻微的 I/O 和字符串格式化开销
  //   - 不启用日志：
  //     - 优点：性能最佳
  //     - 缺点：难以诊断问题
  // 重要说明：
  //   - nullptr 表示不记录日志
  //   - MemTable 创建后不能修改（Immutable）
  //   - 建议生产环境启用 INFO 级别日志
  // 示例：
  //   // 创建日志记录器
  //   auto env = Env::Default();
  //   auto info_log = NewLogger(env, "/path/to/rocksdb.log");
  //
  //   // 配置日志级别
  //   options.info_log_level = INFO_LEVEL;
  Logger* info_log;

  // 允许在错误消息中包含数据
  // 功能：控制是否在错误消息中包含敏感数据
  // 默认值：false（不包含数据）
  // 工作原理：
  //   - true：错误消息中包含键值对数据
  //   - false：错误消息中不包含数据，只包含位置信息
  // 使用场景：
  //   - 开发和测试：设置为 true，便于调试
  //   - 生产环境：设置为 false，保护数据安全
  // 安全考虑：
  //   - true：
  //     - 优点：便于调试和问题诊断
  //     - 缺点：错误消息可能包含敏感数据
  //   - false：
  //     - 优点：保护数据安全
  //     - 缺点：调试困难
  // 重要说明：
  //   - MemTable 创建后不能修改（Immutable）
  //   - 生产环境建议设置为 false
  // 示例：
  //   // 开发环境
  //   allow_data_in_errors = true
  //
  //   // 生产环境
  //   allow_data_in_errors = false
  bool allow_data_in_errors;

  // 每个键的保护字节数
  // 功能：指定为每个键添加的保护字节数（用于校验）
  // 默认值：0（不使用保护）
  // 工作原理：
  //   - 为每个键值对添加额外的保护字节
  //   - 保护字节用于检测内存损坏或篡改
  //   - 通常使用校验和或哈希
  // 使用场景：
  //   - 不使用（0）：不需要保护
  //   - 基本保护（4-8 字节）：检测常见错误
  //   - 强保护（16-32 字节）：高安全性要求
  // 性能影响：
  //   - 启用保护：
  //     - 优点：检测内存损坏和篡改
  //     - 缺点：内存占用增加，计算开销增加
  //   - 不启用保护：
  //     - 优点：内存占用小，无计算开销
  //     - 缺点：无法检测内存损坏
  // 重要说明：
  //   - 0 表示不使用保护
  //   - MemTable 创建后不能修改（Immutable）
  //   - 增加内存占用：每个键值对增加 protection_bytes_per_key 字节
  // 示例：
  //   // 基本保护
  //   protection_bytes_per_key = 8
  //
  //   // 强保护
  //   protection_bytes_per_key = 16
  //
  //   // 不使用保护
  //   protection_bytes_per_key = 0
  uint32_t protection_bytes_per_key;
};

// Batched counters to updated when inserting keys in one write batch.
// In post process of the write batch, these can be updated together.
// Only used in concurrent memtable insert case.
struct MemTablePostProcessInfo {
  uint64_t data_size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletes = 0;
};

using MultiGetRange = MultiGetContext::Range;
// Note:  Many of the methods in this class have comments indicating that
// external synchronization is required as these methods are not thread-safe.
// It is up to higher layers of code to decide how to prevent concurrent
// invocation of these methods.  This is usually done by acquiring either
// the db mutex or the single writer thread.
//
// Some of these methods are documented to only require external
// synchronization if this memtable is immutable.  Calling MarkImmutable() is
// not sufficient to guarantee immutability.  It is up to higher layers of
// code to determine if this MemTable can still be modified by other threads.
// Eg: The Superversion stores a pointer to the current MemTable (that can
// be modified) and a separate list of the MemTables that can no longer be
// written to (aka the 'immutable memtables').
class MemTable {
 public:
  struct KeyComparator : public MemTableRep::KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    virtual int operator()(const char* prefix_len_key1,
                           const char* prefix_len_key2) const override;
    virtual int operator()(const char* prefix_len_key,
                           const DecodedType& key) const override;
  };

  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  //
  // earliest_seq should be the current SequenceNumber in the db such that any
  // key inserted into this memtable will have an equal or larger seq number.
  // (When a db is first created, the earliest sequence number will be 0).
  // If the earliest sequence number is not known, kMaxSequenceNumber may be
  // used, but this may prevent some transactions from succeeding until the
  // first key is inserted into the memtable.
  explicit MemTable(const InternalKeyComparator& comparator,
                    const ImmutableOptions& ioptions,
                    const MutableCFOptions& mutable_cf_options,
                    WriteBufferManager* write_buffer_manager,
                    SequenceNumber earliest_seq, uint32_t column_family_id);
  // No copying allowed
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // Do not delete this MemTable unless Unref() indicates it not in use.
  ~MemTable();

  // Increase reference count.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void Ref() { ++refs_; }

  // Drop reference count.
  // If the refcount goes to zero return this memtable, otherwise return null.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  MemTable* Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      return this;
    }
    return nullptr;
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  size_t ApproximateMemoryUsage();

  // As a cheap version of `ApproximateMemoryUsage()`, this function doesn't
  // require external synchronization. The value may be less accurate though
  size_t ApproximateMemoryUsageFast() const {
    return approximate_memory_usage_.load(std::memory_order_relaxed);
  }

  // used by MemTableListVersion::MemoryAllocatedBytesExcludingLast
  size_t MemoryAllocatedBytes() const {
    return table_->ApproximateMemoryUsage() +
           range_del_table_->ApproximateMemoryUsage() +
           arena_.MemoryAllocatedBytes();
  }

  // Returns a vector of unique random memtable entries of size 'sample_size'.
  //
  // Note: the entries are stored in the unordered_set as length-prefixed keys,
  //       hence their representation in the set as "const char*".
  // Note2: the size of the output set 'entries' is not enforced to be strictly
  //        equal to 'target_sample_size'. Its final size might be slightly
  //        greater or slightly less than 'target_sample_size'
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  // REQUIRES: SkipList memtable representation. This function is not
  // implemented for any other type of memtable representation (vectorrep,
  // hashskiplist,...).
  void UniqueRandomSample(const uint64_t& target_sample_size,
                          std::unordered_set<const char*>* entries) {
    // TODO(bjlemaire): at the moment, only supported by skiplistrep.
    // Extend it to all other memtable representations.
    table_->UniqueRandomSample(num_entries(), target_sample_size, entries);
  }

  // This method heuristically determines if the memtable should continue to
  // host more data.
  bool ShouldScheduleFlush() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_REQUESTED;
  }

  // Returns true if a flush should be scheduled and the caller should
  // be the one to schedule it
  bool MarkFlushScheduled() {
    auto before = FLUSH_REQUESTED;
    return flush_state_.compare_exchange_strong(before, FLUSH_SCHEDULED,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed);
  }

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/dbformat.{h,cc} module.
  //
  // By default, it returns an iterator for prefix seek if prefix_extractor
  // is configured in Options.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        Calling ~Iterator of the iterator will destroy all the states but
  //        those allocated in arena.
  InternalIterator* NewIterator(const ReadOptions& read_options, Arena* arena);

  // Returns an iterator that yields the range tombstones of the memtable.
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.
  // @param immutable_memtable Whether this memtable is an immutable memtable.
  // This information is not stored in memtable itself, so it needs to be
  // specified by the caller. This flag is used internally to decide whether a
  // cached fragmented range tombstone list can be returned. This cached version
  // is constructed when a memtable becomes immutable. Setting the flag to false
  // will always yield correct result, but may incur performance penalty as it
  // always creates a new fragmented range tombstone list.
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable);

  Status VerifyEncodedEntry(Slice encoded,
                            const ProtectionInfoKVOS64& kv_prot_info);

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  //
  // REQUIRES: if allow_concurrent = false, external synchronization to prevent
  // simultaneous operations on the same MemTable.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  Status Add(SequenceNumber seq, ValueType type, const Slice& key,
             const Slice& value, const ProtectionInfoKVOS64* kv_prot_info,
             bool allow_concurrent = false,
             MemTablePostProcessInfo* post_process_info = nullptr,
             void** hint = nullptr);

  // Used to Get value associated with key or Get Merge Operands associated
  // with key.
  // If do_merge = true the default behavior which is Get value for key is
  // executed. Expected behavior is described right below.
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // If memtable contains Merge operation as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operand to *operands.
  //   store MergeInProgress in s, and return false.
  // Else, return false.
  // If any operation was found, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  // On success, *s may be set to OK, NotFound, or MergeInProgress.  Any other
  // status returned indicates a corruption or other unexpected error.
  // If do_merge = false then any Merge Operands encountered for key are simply
  // stored in merge_context.operands_list and never actually merged to get a
  // final value. The raw Merge Operands are eventually returned to the user.
  // @param immutable_memtable Whether this memtable is immutable. Used
  // internally by NewRangeTombstoneIterator(). See comment above
  // NewRangeTombstoneIterator() for more detail.
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true);

  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true) {
    SequenceNumber seq;
    return Get(key, value, columns, timestamp, s, merge_context,
               max_covering_tombstone_seq, &seq, read_opts, immutable_memtable,
               callback, is_blob_index, do_merge);
  }

  // @param immutable_memtable Whether this memtable is immutable. Used
  // internally by NewRangeTombstoneIterator(). See comment above
  // NewRangeTombstoneIterator() for more detail.
  void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                ReadCallback* callback, bool immutable_memtable);

  // If `key` exists in current memtable with type value_type and the existing
  // value is at least as large as the new value, updates it in-place. Otherwise
  // adds the new value to the memtable out-of-place.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  Status Update(SequenceNumber seq, ValueType value_type, const Slice& key,
                const Slice& value, const ProtectionInfoKVOS64* kv_prot_info);

  // If `key` exists in current memtable with type `kTypeValue` and the existing
  // value is at least as large as the new value, updates it in-place. Otherwise
  // if `key` exists in current memtable with type `kTypeValue`, adds the new
  // value to the memtable out-of-place.
  //
  // Returns `Status::NotFound` if `key` does not exist in current memtable or
  // the latest version of `key` does not have `kTypeValue`.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  Status UpdateCallback(SequenceNumber seq, const Slice& key,
                        const Slice& delta,
                        const ProtectionInfoKVOS64* kv_prot_info);

  // Returns the number of successive merge entries starting from the newest
  // entry for the key up to the last non-merge entry or last entry for the
  // key in the memtable.
  size_t CountSuccessiveMergeEntries(const LookupKey& key);

  // Update counters and flush status after inserting a whole write batch
  // Used in concurrent memtable inserts.
  void BatchPostProcess(const MemTablePostProcessInfo& update_counters) {
    num_entries_.fetch_add(update_counters.num_entries,
                           std::memory_order_relaxed);
    data_size_.fetch_add(update_counters.data_size, std::memory_order_relaxed);
    if (update_counters.num_deletes != 0) {
      num_deletes_.fetch_add(update_counters.num_deletes,
                             std::memory_order_relaxed);
    }
    UpdateFlushState();
  }

  // Get total number of entries in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  uint64_t num_entries() const {
    return num_entries_.load(std::memory_order_relaxed);
  }

  // Get total number of deletes in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  uint64_t num_deletes() const {
    return num_deletes_.load(std::memory_order_relaxed);
  }

  uint64_t get_data_size() const {
    return data_size_.load(std::memory_order_relaxed);
  }

  // Dynamically change the memtable's capacity. If set below the current usage,
  // the next key added will trigger a flush. Can only increase size when
  // memtable prefix bloom is disabled, since we can't easily allocate more
  // space.
  void UpdateWriteBufferSize(size_t new_write_buffer_size) {
    if (bloom_filter_ == nullptr ||
        new_write_buffer_size < write_buffer_size_) {
      write_buffer_size_.store(new_write_buffer_size,
                               std::memory_order_relaxed);
    }
  }

  // Returns the edits area that is needed for flushing the memtable
  VersionEdit* GetEdits() { return &edit_; }

  // Returns if there is no entry inserted to the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  bool IsEmpty() const { return first_seqno_ == 0; }

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  SequenceNumber GetFirstSequenceNumber() {
    return first_seqno_.load(std::memory_order_relaxed);
  }

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  void SetFirstSequenceNumber(SequenceNumber first_seqno) {
    return first_seqno_.store(first_seqno, std::memory_order_relaxed);
  }

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  SequenceNumber GetEarliestSequenceNumber() {
    return earliest_seqno_.load(std::memory_order_relaxed);
  }

  // Sets the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  // Used only for MemPurge operation
  void SetEarliestSequenceNumber(SequenceNumber earliest_seqno) {
    return earliest_seqno_.store(earliest_seqno, std::memory_order_relaxed);
  }

  // DB's latest sequence ID when the memtable is created. This number
  // may be updated to a more recent one before any key is inserted.
  SequenceNumber GetCreationSeq() const { return creation_seq_; }

  void SetCreationSeq(SequenceNumber sn) { creation_seq_ = sn; }

  // Returns the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  uint64_t GetNextLogNumber() { return mem_next_logfile_number_; }

  // Sets the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void SetNextLogNumber(uint64_t num) { mem_next_logfile_number_ = num; }

  // if this memtable contains data from a committed
  // two phase transaction we must take note of the
  // log which contains that data so we can know
  // when to relese that log
  void RefLogContainingPrepSection(uint64_t log);
  uint64_t GetMinLogContainingPrepSection();

  // Notify the underlying storage that no more items will be added.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  // After MarkImmutable() is called, you should not attempt to
  // write anything to this MemTable().  (Ie. do not call Add() or Update()).
  void MarkImmutable() {
    table_->MarkReadOnly();
    mem_tracker_.DoneAllocating();
  }

  // Notify the underlying storage that all data it contained has been
  // persisted.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void MarkFlushed() { table_->MarkFlushed(); }

  // return true if the current MemTableRep supports merge operator.
  bool IsMergeOperatorSupported() const {
    return table_->IsMergeOperatorSupported();
  }

  // return true if the current MemTableRep supports snapshots.
  // inplace update prevents snapshots,
  bool IsSnapshotSupported() const {
    return table_->IsSnapshotSupported() && !moptions_.inplace_update_support;
  }

  struct MemTableStats {
    uint64_t size;
    uint64_t count;
  };

  MemTableStats ApproximateStats(const Slice& start_ikey,
                                 const Slice& end_ikey);

  // Get the lock associated for the key
  port::RWMutex* GetLock(const Slice& key);

  const InternalKeyComparator& GetInternalKeyComparator() const {
    return comparator_.comparator;
  }

  const ImmutableMemTableOptions* GetImmutableMemTableOptions() const {
    return &moptions_;
  }

  uint64_t ApproximateOldestKeyTime() const {
    return oldest_key_time_.load(std::memory_order_relaxed);
  }

  // REQUIRES: db_mutex held.
  void SetID(uint64_t id) { id_ = id; }

  uint64_t GetID() const { return id_; }

  void SetFlushCompleted(bool completed) { flush_completed_ = completed; }

  uint64_t GetFileNumber() const { return file_number_; }

  void SetFileNumber(uint64_t file_num) { file_number_ = file_num; }

  void SetFlushInProgress(bool in_progress) {
    flush_in_progress_ = in_progress;
  }

  void SetFlushJobInfo(std::unique_ptr<FlushJobInfo>&& info) {
    flush_job_info_ = std::move(info);
  }

  std::unique_ptr<FlushJobInfo> ReleaseFlushJobInfo() {
    return std::move(flush_job_info_);
  }

  // Returns a heuristic flush decision
  bool ShouldFlushNow();

  void ConstructFragmentedRangeTombstones();

  // Returns whether a fragmented range tombstone list is already constructed
  // for this memtable. It should be constructed right before a memtable is
  // added to an immutable memtable list. Note that if a memtable does not have
  // any range tombstone, then no range tombstone list will ever be constructed.
  // @param allow_empty Specifies whether a memtable with no range tombstone is
  // considered to have its fragmented range tombstone list constructed.
  bool IsFragmentedRangeTombstonesConstructed(bool allow_empty = true) const {
    if (allow_empty) {
      return fragmented_range_tombstone_list_.get() != nullptr ||
             is_range_del_table_empty_;
    } else {
      return fragmented_range_tombstone_list_.get() != nullptr;
    }
  }

  // Returns Corruption status if verification fails.
  static Status VerifyEntryChecksum(const char* entry,
                                    uint32_t protection_bytes_per_key,
                                    bool allow_data_in_errors = false);

 private:
  // Flush状态枚举：表示当前memtable的flush状态
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED, FLUSH_SCHEDULED };

  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class MemTableList;

  // 键比较器
  // 功能：用于比较memtable中的键，基于InternalKeyComparator实现
  // 使用场景：在memtable中插入、查找、遍历数据时都需要使用比较器
  // 线程安全：本身是只读的，使用时需要外部同步
  // 重要说明：比较器决定了memtable中数据的有序性
  KeyComparator comparator_;

  // 不可变的memtable选项
  // 功能：存储memtable创建时确定的不可变配置选项
  // 包含内容：arena块大小、bloom filter配置、inplace update支持、merge operator等
  // 使用场景：在memtable的整个生命周期中用于控制行为
  // 线程安全：常量对象，线程安全
  const ImmutableMemTableOptions moptions_;

  // 引用计数
  // 功能：跟踪有多少地方正在使用这个memtable
  // 使用场景：
  //   - MemTableList引用活跃的memtable
  //   - Flush任务引用正在被flush的memtable
  //   - Iterator引用正在遍历的memtable
  // 线程安全：原子操作，需要外部同步
  // 生命周期：当引用计数降为0时，memtable可以被销毁
  int refs_;

  // Arena内存块大小
  // 功能：指定ConcurrentArena每次分配内存块的大小
  // 使用场景：初始化时设置，用于优化内存分配性能
  // 线程安全：常量，线程安全
  // 重要说明：较大的值可以减少内存碎片，但可能增加内存使用
  const size_t kArenaBlockSize;

  // 内存分配跟踪器
  // 功能：跟踪memtable的内存使用情况，与WriteBufferManager协作
  // 使用场景：
  //   - 分配内存时通知WriteBufferManager
  //   - 解除引用时释放内存配额
  //   - 监控整体内存使用
  // 线程安全：内部使用原子操作
  // 重要说明：用于实现基于内存的flush触发机制
  AllocTracker mem_tracker_;

  // 并发内存分配器
  // 功能：为memtable中的键值对提供高效的内存分配
  // 使用场景：
  //   - 存储键值数据
  //   - 存储内部数据结构
  //   - 支持并发插入
  // 线程安全：支持并发分配，无需外部同步
  // 重要说明：memtable销毁时arena内存会被一起释放
  ConcurrentArena arena_;

  // 主数据表的表示
  // 功能：存储memtable中的所有常规键值对
  // 数据结构：可以是skiplist、hashskiplist、vector等不同实现
  // 使用场景：
  //   - 存储Put、Merge、Delete等操作的数据
  //   - 支持Get、NewIterator、Add等操作
  // 线程安全：根据配置支持并发插入
  // 生命周期：随memtable一起创建和销毁
  std::unique_ptr<MemTableRep> table_;

  // 范围删除表的表示
  // 功能：专门存储范围删除操作（RangeTombstone）
  // 使用场景：
  //   - 存储StartKey到EndKey之间的删除标记
  //   - 查询时检查数据是否被范围删除覆盖
  //   - 支持范围删除的重叠和合并
  // 线程安全：读取时需要外部同步
  // 重要说明：与table_分开存储以优化范围删除查询
  std::unique_ptr<MemTableRep> range_del_table_;

  // 范围删除表是否为空的标志
  // 功能：快速判断range_del_table_中是否有数据
  // 使用场景：
  //   - 查询时优化，避免空范围删除表的检查
  //   - 构造FragmentedRangeTombstoneList前的判断
  // 线程安全：原子操作
  // 性能优化：避免频繁查询range_del_table_是否为空
  std::atomic_bool is_range_del_table_empty_;

  // 所有插入数据的总大小
  // 功能：记录memtable中所有键值对的总数据量
  // 使用场景：
  //   - 判断是否需要触发flush
  //   - 统计信息收集
  //   - 内存使用监控
  // 线程安全：原子操作，支持并发更新
  // 重要说明：用于flush决策，与write_buffer_size_比较
  std::atomic<uint64_t> data_size_;

  // memtable中的条目数量
  // 功能：记录存储的键值对总数（包括Put、Merge、Delete等）
  // 使用场景：
  //   - 统计信息
  //   - 性能分析
  //   - 调试和监控
  // 线程安全：原子操作，支持并发更新
  // 重要说明：包括所有类型的操作，不仅仅是Put
  std::atomic<uint64_t> num_entries_;

  // 删除操作的条目数量
  // 功能：记录memtable中的Delete和SingleDelete操作数量
  // 使用场景：
  //   - 统计删除率
  //   - 优化flush决策
  //   - 压缩策略选择
  // 线程安全：原子操作，支持并发更新
  // 重要说明：高删除率可能影响flush和compaction策略
  std::atomic<uint64_t> num_deletes_;

  // 动态可变的写缓冲区大小
  // 功能：存储当前memtable的容量限制，可以动态调整
  // 使用场景：
  //   - 动态调整memtable大小
  //   - 实现自动调优
  //   - 响应内存压力
  // 线程安全：原子操作
  // 重要说明：
  //   - 如果设置了bloom filter，只能减小不能增大
  //   - 用于与data_size_比较判断是否flush
  std::atomic<size_t> write_buffer_size_;

  // Flush是否正在进行
  // 功能：标记flush任务是否已开始执行
  // 使用场景：
  //   - 防止重复调度flush
  //   - 状态检查和监控
  //   - 协调memtable状态转换
  // 线程安全：需要外部同步（通常使用db_mutex）
  // 生命周期：从flush开始时设置为true，完成时设置为false
  bool flush_in_progress_;

  // Flush是否已完成
  // 功能：标记flush任务是否已完成
  // 使用场景：
  //   - 判断memtable是否可以释放
  //   - 状态检查和监控
  //   - 协调memtable生命周期
  // 线程安全：需要外部同步（通常使用db_mutex）
  // 重要说明：flush完成后，memtable变为不可变状态
  bool flush_completed_;

  // flush完成后生成的SSTable文件编号
  // 功能：记录flush操作生成的文件编号
  // 使用场景：
  //   - 更新VersionEdit
  //   - 记录日志文件删除边界
  //   - Version管理
  // 线程安全：需要外部同步
  // 重要说明：flush完成前为0，完成后设置为实际文件编号
  uint64_t file_number_;

  // VersionEdit对象
  // 功能：记录flush操作需要应用到Manifest的变更
  // 使用场景：
  //   - Flush时记录新增文件信息
  //   - 记录删除的文件
  //   - 记录其他元数据变更
  // 线程安全：flush期间单线程访问
  // 重要说明：flush完成后会被应用到Manifest
  VersionEdit edit_;

  // 第一个插入的键值对的序列号
  // 功能：记录memtable中最早的序列号
  // 使用场景：
  //   - 判断memtable是否为空（0表示空）
  //   - 日志文件回收决策
  //   - 快照隔离判断
  // 线程安全：原子操作
  // 重要说明：用于确定哪些日志文件可以删除
  std::atomic<SequenceNumber> first_seqno_;

  // 创建时的数据库序列号
  // 功能：记录创建memtable时的全局序列号
  // 使用场景：
  //   - 事务处理
  //   - 快照判断
  //   - 事务冲突检测
  // 线程安全：原子操作
  // 重要说明：用于判断写操作是否在这个memtable中
  std::atomic<SequenceNumber> earliest_seqno_;

  // 创建时的序列号
  // 功能：记录创建memtable时的当前序列号
  // 使用场景：
  //   - 追踪memtable创建时间
  //   - 调试和诊断
  //   - 性能分析
  // 线程安全：需要外部同步
  // 重要说明：与earliest_seqno_相关但不完全相同
  SequenceNumber creation_seq_;

  // 下一个活跃日志文件的编号
  // 功能：记录flush后应该使用的日志文件编号
  // 使用场景：
  //   - 日志文件回收
  //   - 确定哪些日志可以删除
  //   - 恢复时确定日志边界
  // 线程安全：需要外部同步
  // 重要说明：小于此编号的日志文件可以被安全删除
  uint64_t mem_next_logfile_number_;

  // 引用的最早包含prepared section的日志编号
  // 功能：记录两阶段事务中prepared日志的最小编号
  // 使用场景：
  //   - 两阶段事务管理
  //   - 确定日志文件何时可以删除
  //   - 事务恢复
  // 线程安全：原子操作
  // 重要说明：只有在事务提交前不能删除相关日志
  std::atomic<uint64_t> min_prep_log_referenced_;

  // 就地更新（inplace update）使用的读写锁数组
  // 功能：为不同的键提供读写锁以支持并发更新
  // 使用场景：
  //   - 支持inplace_update功能
  //   - 保护对同一键的并发修改
  //   - 提高并发性能
  // 线程安全：每个锁保护特定的键集合
  // 重要说明：通过hash将键映射到锁数组
  std::vector<port::RWMutex> locks_;

  // 前缀提取器
  // 功能：从键中提取前缀用于前缀搜索和hash索引
  // 使用场景：
  //   - 基于前缀的memtable查询
  //   - 前缀bloom filter
  //   - 前缀迭代器
  // 线程安全：只读，线程安全
  // 重要说明：用于优化前缀相关操作
  const SliceTransform* const prefix_extractor_;

  // 动态布隆过滤器
  // 功能：基于前缀的布隆过滤器，快速判断前缀是否存在
  // 使用场景：
  //   - 前缀查询优化
  //   - 减少不必要的memtable查找
  //   - 提高读取性能
  // 线程安全：读取时需要外部同步
  // 重要说明：仅当memtable_prefix_bloom_bits配置时创建
  std::unique_ptr<DynamicBloom> bloom_filter_;

  // Flush状态
  // 功能：表示当前memtable的flush状态
  // 状态转换：
  //   - FLUSH_NOT_REQUESTED：不需要flush
  //   - FLUSH_REQUESTED：需要flush但未调度
  //   - FLUSH_SCHEDULED：flush已调度
  // 使用场景：
  //   - ShouldScheduleFlush()检查
  //   - MarkFlushScheduled()调度flush
  //   - 防止重复调度
  // 线程安全：原子操作，支持CAS
  std::atomic<FlushStateEnum> flush_state_;

  // 系统时钟
  // 功能：获取系统时间用于时间戳相关操作
  // 使用场景：
  //   - 记录oldest_key_time_
  //   - TTL过期检查
  //   - 性能统计
  // 线程安全：只读，线程安全
  // 重要说明：用于记录键的插入时间
  SystemClock* clock_;

  // 插入提示的前缀提取器
  // 功能：为支持插入提示（insert hint）而提取前缀
  // 使用场景：
  //   - 优化顺序插入性能
  //   - 跟踪每个前缀的插入位置
  //   - 减少skiplist查找开销
  // 线程安全：只读，线程安全
  // 重要说明：与prefix_extractor_可能不同，专门用于insert hint
  const SliceTransform* insert_with_hint_prefix_extractor_;

  // 每个前缀的插入提示映射
  // 功能：缓存每个前缀的最后插入位置，加速后续插入
  // 数据结构：基于hash的unordered_map
  // 使用场景：
  //   - 顺序插入优化
  //   - 提供InsertHint参数给Add()
  //   - 减少skiplist遍历
  // 线程安全：需要外部同步
  // 重要说明：仅在insert_with_hint_prefix_extractor_配置时使用
  UnorderedMapH<Slice, void*, SliceHasher32> insert_hints_;

  // 最旧键的时间戳
  // 功能：记录memtable中最早插入的键的时间戳
  // 使用场景：
  //   - 基于时间的flush策略
  //   - TTL管理
  //   - QoS控制
  // 线程安全：原子操作
  // 重要说明：用于触发基于时间的flush
  std::atomic<uint64_t> oldest_key_time_;

  // Memtable唯一标识符
  // 功能：唯一标识一个memtable实例
  // 使用场景：
  //   - 追踪flush进度
  //   - 调试和日志
  //   - 监控和分析
  // 线程安全：需要外部同步（通常在创建时设置）
  // 重要说明：全局唯一，由系统分配
  uint64_t id_ = 0;

  // 原子flush的序列号
  // 功能：记录负责flush此memtable的原子flush操作的序列号
  // 定义：
  //   - 所有序列号小于此值的数据已被flush
  //   - 所有序列号大于等于此值的数据未flush
  // 使用场景：
  //   - 原子flush协调
  //   - 多列族同步flush
  //   - 恢复和一致性检查
  // 线程安全：需要外部同步
  // 重要说明：用于支持跨列族的原子flush操作
  SequenceNumber atomic_flush_seqno_;

  // 近似内存使用量
  // 功能：缓存table_、arena_和range_del_table_的内存使用总和
  // 使用场景：
  //   - 快速获取内存使用信息
  //   - 避免频繁计算
  //   - Flush决策
  // 线程安全：原子操作
  // 更新时机：ApproximateMemoryUsage()或ShouldFlushNow()时刷新
  // 重要说明：是近似值，不是精确值
  std::atomic<uint64_t> approximate_memory_usage_;

  // Flush任务信息
  // 功能：存储与当前memtable flush相关的详细信息
  // 使用场景：
  //   - 记录flush统计信息
  //   - 监控flush性能
  //   - 事件监听器回调
  // 线程安全：flush期间单线程访问
  // 重要说明：flush完成后可以释放
  std::unique_ptr<FlushJobInfo> flush_job_info_;

  // 根据ShouldFlushNow()更新flush_state_
  // 功能：检查内存使用情况并更新flush状态
  // 使用场景：
  //   - 插入数据后检查是否需要flush
  //   - BatchPostProcess中调用
  //   - 触发状态转换
  // 线程安全：调用者需要确保外部同步
  // 重要说明：实现flush的自动触发逻辑
  void UpdateFlushState();

  // 更新最旧键的时间戳
  // 功能：检查并更新oldest_key_time_
  // 使用场景：
  //   - 首次插入时设置
  //   - 定期检查是否需要更新
  //   - 支持TTL相关功能
  // 线程安全：调用者需要确保外部同步
  // 重要说明：只在必要时更新以减少开销
  void UpdateOldestKeyTime();

  // 从table_中获取数据
  // 功能：从主表中查找指定键的数据
  // 参数：
  //   - key：要查找的键
  //   - max_covering_tombstone_seq：最大覆盖范围删除序列号
  //   - do_merge：是否执行merge操作
  //   - callback：读取回调（用于校验）
  //   - is_blob_index：输出参数，是否为blob引用
  //   - value：输出的值
  //   - columns：输出的列数据
  //   - timestamp：输出的时间戳
  //   - s：输出的状态
  //   - merge_context：merge操作上下文
  //   - seq：输出的序列号
  //   - found_final_value：输出参数，是否找到最终值
  //   - merge_in_progress：输出参数，merge是否在进行中
  // 使用场景：
  //   - Get操作的内部实现
  //   - 处理merge操作
  //   - 查找键值对
  // 线程安全：需要外部同步（除非memtable为不可变）
  // 重要说明：核心数据获取逻辑
  void GetFromTable(const LookupKey& key,
                    SequenceNumber max_covering_tombstone_seq, bool do_merge,
                    ReadCallback* callback, bool* is_blob_index,
                    std::string* value, PinnableWideColumns* columns,
                    std::string* timestamp, Status* s,
                    MergeContext* merge_context, SequenceNumber* seq,
                    bool* found_final_value, bool* merge_in_progress);

  // 创建范围删除迭代器的内部实现
  // 功能：创建用于遍历范围删除的迭代器
  // 前置条件：
  //   - 必须先检查is_range_del_table_empty_
  //   - memtable必须仍然存活
  // 参数：
  //   - read_options：读取选项
  //   - read_seq：读取序列号（用作范围删除的上界）
  //   - immutable_memtable：memtable是否不可变
  // 返回：非空的FragmentedRangeTombstoneIterator指针
  // 使用场景：
  //   - NewRangeTombstoneIterator的内部实现
  //   - 范围删除查询
  //   - Get操作时检查范围删除
  // 线程安全：调用者需要确保外部同步
  // 重要说明：仅在memtable生命周期内有效
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIteratorInternal(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable);

  // 分片的范围删除列表
  // 功能：存储已分片的范围删除信息
  // 创建时机：当memtable变为不可变时（如果!is_range_del_table_empty_）
  // 使用场景：
  //   - 范围删除查询优化
  //   - 避免重复分片计算
  //   - 加速Get和Iterator操作
  // 线程安全：创建后只读，线程安全
  // 重要说明：只创建一次，用于不可变memtable
  std::unique_ptr<FragmentedRangeTombstoneList>
      fragmented_range_tombstone_list_;

  // 范围删除互斥锁
  // 功能：确保只有一个范围删除写入者可以失效缓存
  // 使用场景：
  //   - 保护fragmented_range_tombstone_list_的创建
  //   - 保护cached_range_tombstone_的更新
  //   - 协调并发访问
  // 线程安全：提供互斥访问
  // 重要说明：用于控制对范围删除缓存的并发修改
  std::mutex range_del_mutex_;

  // 缓存的范围删除列表（核心本地数组）
  // 功能：为每个CPU核心缓存范围删除列表
  // 数据结构：CoreLocalArray，每个核心有自己的缓存
  // 使用场景：
  //   - 读取范围删除时的性能优化
  //   - 减少缓存竞争
  //   - 支持并发读取
  // 线程安全：
  //   - 读取：线程安全，每个核心访问自己的缓存
  //   - 写入：需要range_del_mutex_保护
  // 重要说明：显著提高多核环境下的读取性能
  CoreLocalArray<std::shared_ptr<FragmentedRangeTombstoneListCache>>
      cached_range_tombstone_;

  // 更新条目的校验和
  // 功能：为键值对计算并设置校验和
  // 参数：
  //   - kv_prot_info：保护信息
  //   - key：键
  //   - value：值
  //   - type：值类型
  //   - s：序列号
  //   - checksum_ptr：校验和写入位置
  // 使用场景：
  //   - 数据完整性验证
  //   - 灾难恢复
  //   - 错误检测
  // 线程安全：调用者需要确保外部同步
  // 重要说明：用于防止数据损坏
  void UpdateEntryChecksum(const ProtectionInfoKVOS64* kv_prot_info,
                           const Slice& key, const Slice& value, ValueType type,
                           SequenceNumber s, char* checksum_ptr);
};

extern const char* EncodeKey(std::string* scratch, const Slice& target);

}  // namespace ROCKSDB_NAMESPACE
