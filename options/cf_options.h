// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "options/db_options.h"
#include "rocksdb/options.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

// ImmutableCFOptions is a data struct used by RocksDB internal. It contains a
// subset of Options that should not be changed during the entire lifetime
// of DB. Raw pointers defined in this struct do not have ownership to the data
// they point to. Options contains std::shared_ptr to these data.
// ImmutableCFOptions 是 RocksDB 内部使用的数据结构，包含在 DB 整个生命周期中
// 不应该改变的 Options 子集。此结构中定义的原始指针不拥有其指向数据的所有权。
// Options 中包含指向这些数据的 std::shared_ptr。
// 不可变 CF 选项：这些选项在 DB 打开后不能动态修改
// 与 MutableCFOptions 相对，MutableCFOptions 可以通过 SetOptions() 动态修改
struct ImmutableCFOptions {
 public:
  static const char* kName() { return "ImmutableCFOptions"; }
  explicit ImmutableCFOptions();
  explicit ImmutableCFOptions(const ColumnFamilyOptions& cf_options);

  // 压缩风格
  // 功能：指定压缩算法的类型，决定数据如何在层次结构中组织
  // 支持的压缩风格：
  //   - kCompactionStyleLevel：Level-style compaction（默认）
  //     数据分层存储，每层的数据量呈指数增长
  //     L0 层可能有重叠，L1+ 层不重叠
  //     适用于读写均衡的场景
  //   - kCompactionStyleUniversal：Universal-style compaction
  //     所有文件都可能在同一层，通过时间排序
  //     减少写放大，但增加读放大和空间放大
  //     适用于写入密集型场景
  //   - kCompactionStyleFIFO：FIFO-style compaction
  //     先进先出，按文件创建时间删除旧文件
  //     适用于时序数据、日志数据
  //     数据有 TTL（生存时间）
  // 重要说明：
  //   - 压缩风格在 DB 创建后不能更改（immutable）
  //   - 不同风格对性能和资源消耗影响很大
  //   - 根据读写模式选择合适的压缩风格
  CompactionStyle compaction_style;

  // 压缩优先级
  // 功能：指定压缩时文件的选择策略，影响压缩性能
  // 支持的优先级：
  //   - kByCompensatedSize：基于补偿大小的优先级（默认）
  //     考虑文件大小、层数、重叠度等因素
  //     平衡压缩选择，适用于大多数场景
  //   - kOldestLargestSeqFirst：优先选择最旧的文件
  //     减少压缩延迟，加快旧数据清理
  //     可能增加写放大
  //   - kMinOverlappingRatio：最小重叠比例
  //     优先压缩与上层重叠少的文件
  //     减少读放大
  //   - kOldestSmallestSeqFirst：优先选择最旧且序列号最小的文件
  // 性能影响：
  //   - 不同的优先级会影响压缩效率和写放大
  //   - 建议根据具体场景测试选择
  CompactionPri compaction_pri;

  // 用户键比较器
  // 功能：定义用户键的排序方式，影响数据组织和查询
  // 默认值：BytewiseComparator()（按字节字典序）
  // 工作原理：
  //   - 用于比较用户提供的键（不包含序列号）
  //   - 与 internal_comparator 一起工作
  //   - 决定键在 MemTable 和 SST 文件中的顺序
  // 重要说明：
  //   - 指针不拥有数据所有权，数据由 Options 中的 shared_ptr 管理
  //   - DB 打开后不能更改（immutable）
  //   - 必须与创建 DB 时使用的 comparator 完全一致
  //   - 常用比较器：
  //     - BytewiseComparator：按字节字典序
  //     - ReverseBytewiseComparator：反向字典序
  //     - 自定义 Comparator：根据业务需求实现
  const Comparator* user_comparator;

  // 内部键比较器（仅存在于 ImmutableCFOptions）
  // 功能：基于 user_comparator 的包装，用于比较内部键（包含序列号）
  // 内部键格式：[user_key][sequence_number][type]
  // 工作原理：
  //   - 在比较内部键时，先比较 user_key
  //   - 如果 user_key 相同，再比较 sequence_number（降序）
  //   - 如果 sequence_number 相同，再比较 type
  //   - 降序比较 sequence_number 确保新版本排在前面
  // 重要说明：
  //   - 只在 ImmutableCFOptions 中存在，不在 ColumnFamilyOptions 中
  //   - 由 user_comparator 自动构造
  //   - DB 打开后不能更改（immutable）
  // 使用场景：
  //   - 所有内部键的比较（MemTable、SST 文件、迭代器）
  //   - 版本控制：新版本（更大 sequence_number）优先返回
  InternalKeyComparator internal_comparator;  // Only in Immutable

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
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 指针由 shared_ptr 管理，不拥有所有权
  std::shared_ptr<MergeOperator> merge_operator;

  // 压缩过滤器（单实例）
  // 功能：允许应用在后台压缩过程中修改或删除键值对
  // 默认值：nullptr（不使用压缩过滤器）
  // 工作原理：
  //   - 在压缩过程中，对每个键值对调用 CompactionFilter
  //   - 可以根据条件决定保留、修改或删除数据
  //   - 返回 kKeep：保留，kRemove：删除，kChangeValue：修改值
  // 使用场景：
  //   - TTL 过期数据清理
  //   - 数据生命周期管理（根据版本号、时间戳）
  //   - 数据脱敏
  //   - 自定义清理策略
  // 重要说明：
  //   - 原始指针，不拥有数据所有权
  //   - 可能被多个压缩线程并发调用，必须线程安全
  //   - 与 compaction_filter_factory 只能指定一个
  //   - DB 打开后不能更改（immutable）
  const CompactionFilter* compaction_filter;

  // 压缩过滤器工厂
  // 功能：为创建 SST 文件的线程提供独立的 CompactionFilter 实例
  // 默认值：nullptr（不使用压缩过滤器工厂）
  // 工作原理：
  //   - 工厂模式，为每个线程创建新的 CompactionFilter 实例
  //   - 适用于多线程压缩场景，每个线程有独立实例
  //   - 可以根据 TableFileCreationReason 决定是否使用过滤器
  // 使用场景：
  //   - 多线程压缩：避免线程安全问题
  //   - 状态依赖的过滤：根据线程上下文创建不同过滤器
  //   - 多场景过滤：Flush、压缩、恢复等不同场景
  // 重要说明：
  //   - 指针由 shared_ptr 管理
  //   - DB 打开后不能更改（immutable）
  std::shared_ptr<CompactionFilterFactory> compaction_filter_factory;

  // 最小合并写缓冲区数量
  // 功能：指定 Flush 时最少需要合并的 MemTable 数量
  // 默认值：1
  // 工作原理：
  //   - 当活跃 MemTable 数量达到 min_write_buffer_number_to_merge 时
  //   - 将这些 MemTable 合并为一个进行 Flush
  //   - 减少小文件数量，提高压缩效率
  // 使用场景：
  //   - 减少小文件：设置较大的值（2-4）
  //   - 快速 Flush：设置较小的值（1）
  // 性能影响：
  //   - 较大的值：
  //     - 优点：减少文件数量，提高压缩效率
  //     - 缺点：增加 Flush 延迟，增加内存使用
  //   - 较小的值：
  //     - 优点：快速释放内存
  //     - 缺点：产生更多小文件，增加压缩开销
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 应该 <= max_write_buffer_number
  int min_write_buffer_number_to_merge;

  // 最大维护的写缓冲区数量
  // 功能：指定允许在内存中维护的最大 MemTable 数量（包括不可变和活跃的）
  // 默认值：0（由 max_write_buffer_number 决定）
  // 工作原理：
  //   - 当 MemTable 数量超过此值时，多余的会被释放
  //   - 用于控制内存使用，防止 MemTable 堆积
  //   - 0 表示不限制，使用 max_write_buffer_number
  // 使用场景：
  //   - 内存受限：设置较小的值（2-3）
  //   - 写入密集：设置较大的值（4-6）
  // 性能影响：
  //   - 较小的值：
  //     - 优点：减少内存使用
  //     - 缺点：增加 Flush 频率，可能影响读取性能
  //   - 较大的值：
  //     - 优点：提高读取性能（缓存更多数据）
  //     - 缺点：增加内存使用
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 设置为 0 时，实际限制为 max_write_buffer_number
  int max_write_buffer_number_to_maintain;

  // 最大维护的写缓冲区大小
  // 功能：指定允许在内存中维护的最大 MemTable 总大小
  // 默认值：0（不限制）
  // 工作原理：
  //   - 当 MemTable 总大小超过此值时，会释放最老的 MemTable
  //   - 用于控制内存使用
  //   - 0 表示不限制
  // 使用场景：
  //   - 内存受限：设置合理的上限（如 1 GB）
  //   - 写入密集：不限制（0）
  // 性能影响：
  //   - 设置限制：
  //     - 优点：严格控制内存使用
  //     - 缺点：可能影响读取性能
  //   - 不限制：
  //     - 优点：最大化读取性能
  //     - 缺点：内存使用可能很高
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 与 max_write_buffer_number_to_maintain 一起控制内存
  int64_t max_write_buffer_size_to_maintain;

  // 支持原地更新
  // 功能：允许在已存在的值上直接更新，避免创建新版本
  // 默认值：false（不支持原地更新）
  // 工作原理：
  //   - 当新值比旧值小时，可以原地更新
  //   - 节省内存和存储空间
  //   - 需要配合 inplace_callback 使用
  // 使用场景：
  //   - 频繁更新同一键的值
  //   - 新值不大于旧值（如计数器）
  // 性能影响：
  //   - true：
  //     - 优点：减少内存和存储使用
  //     - 缺点：增加代码复杂度，需要实现回调
  //   - false：
  //     - 优点：简单可靠
  //     - 缺点：每次更新创建新版本
  // 前提条件：
  //   - 必须实现 inplace_callback
  //   - 新值大小必须 <= 旧值大小
  //   - 不支持所有 MemTable 类型（如 SkipList 支持，HashSkipList 不支持）
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 实验性功能，可能不稳定
  bool inplace_update_support;

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
  // 使用场景：
  //   - 计数器：existing_value += delta_value
  //   - 拼接：strcat(existing_value, delta_value)
  //   - 自定义逻辑：根据业务需求实现
  // 示例：
  //   UpdateStatus MyInplaceCallback(
  //       char* existing_value, uint32_t* existing_value_size,
  //       Slice delta_value, std::string* merged_value) {
  //     if (delta_value.size_ > *existing_value_size) {
  //       *merged_value = std::string(existing_value, *existing_value_size);
  //       merged_value->append(delta_value.data_, delta_value.size_);
  //       return UPDATE_FAILED;  // 需要创建新版本
  //     }
  //     memcpy(existing_value, delta_value.data_, delta_value.size_);
  //     *existing_value_size = delta_value.size_;
  //     return UPDATE_OK;  // 原地更新成功
  //   }
  // 重要说明：
  //   - 必须配合 inplace_update_support 使用
  //   - 回调函数必须是线程安全的
  //   - DB 打开后不能更改（immutable）
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);

  // MemTable 表示工厂
  // 功能：指定 MemTable 的数据结构和实现方式
  // 默认值：SkipListFactory（基于跳表的 MemTable）
  // 支持的 MemTable 类型：
  //   - SkipListFactory：跳表（默认）
  //     支持范围查询，适用于大多数场景
  //   - VectorRepFactory：向量
  //     适合小数据量，纯内存场景
  //   - HashSkipListRepFactory：哈希跳表
  //     适合纯点查询场景
  //   - HashLinkListRepFactory：哈希链表
  //     适合纯点查询场景
  //   - cuckoo_hashing：布谷鸟哈希
  //     适合纯点查询，超低延迟
  // 使用场景：
  //   - SkipListFactory（默认）：
  //     - 支持范围查询和点查询
  //     - 适用于大多数通用场景
  //   - HashSkipListRepFactory：
  //     - 只有点查询
  //     - 不需要范围查询
  //     - 内存充足
  //   - VectorRepFactory：
  //     - 小数据量
  //     - 纯内存，需要快速启动和关闭
  // 性能影响：
  //   - SkipList：
  //     - 读写：O(log N)
  //     - 内存：约 1.5 倍数据大小
  //   - HashSkipList：
  //     - 读：O(1)，写：O(log N)
  //     - 内存：约 2 倍数据大小（哈希表 + 跳表）
  //   - Vector：
  //     - 读写：O(log N)
  //     - 内存：约 1 倍数据大小
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 选择后不能轻易更换
  std::shared_ptr<MemTableRepFactory> memtable_factory;

  // 表工厂
  // 功能：指定 SST 文件的格式和实现
  // 默认值：BlockBasedTableFactory（基于块的表格式）
  // 支持的表格式：
  //   - BlockBasedTable（默认）：
  //     数据分为固定大小的块
  //     支持多种索引类型和过滤器
  //     适用于大多数场景
  //   - PlainTable：
  //     扁平存储，无块结构
  //     仅适用于纯内存文件系统（RAMFS）
  //   - CuckooTable：
  //     基于布谷鸟哈希
  //     仅支持点查询，不支持范围查询
  // 使用场景：
  //   - BlockBasedTable（默认）：大多数通用场景
  //   - PlainTable：纯内存、大量小键值对
  //   - CuckooTable：只读、纯点查询、超低延迟
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 不同表格式不兼容
  std::shared_ptr<TableFactory> table_factory;

  // 表属性收集器工厂列表
  // 功能：指定用于收集和报告 SST 文件统计信息的收集器
  // 默认值：空列表
  // 工作原理：
  //   - 在构建 SST 文件时，收集器收集各种统计信息
  //   - 包括：键值对数量、数据大小、压缩比等
  //   - 信息可以用于监控、调优、分析
  // 常见的表属性收集器：
  //   - SizePropertiesCollector：收集大小信息
  //   - TimestampedPropertiesCollector：收集时间戳信息
  //   - CustomPropertiesCollector：自定义收集器
  // 使用场景：
  //   - 监控：收集文件大小、键值对数量等
  //   - 调优：分析压缩比、热点数据等
  //   - 分析：统计访问模式、数据分布等
  // 重要说明：
  //   - 每个收集器会增加少量开销
  //   - DB 打开后不能更改（immutable）
  Options::TablePropertiesCollectorFactories
      table_properties_collector_factories;

  // Bloom 过滤器局部性
  // 功能：指定 Bloom 过滤器的局部性，用于优化 PlainTable 的 Bloom filter
  // 默认值：0（不使用局部性）
  // 工作原理：
  //   - 将 Bloom filter 分成多个部分
  //   - 减少内存访问，提高查询性能
  //   - 适用于 PlainTable 格式
  // 使用场景：
  //   - PlainTable 格式：设置合理的值（如 1-6）
  //   - BlockBasedTable 格式：不需要设置此参数
  // 性能影响：
  //   - 较大的值：
  //     - 优点：减少内存访问
  //     - 缺点：增加 Bloom filter 大小
  //   - 0：
  //     - 不使用局部性，简单但可能性能较差
  // 重要说明：
  //   - 主要用于 PlainTableReader
  //   - 可能需要移动到 PlainTableOptions
  //   - DB 打开后不能更改（immutable）
  uint32_t bloom_locality;

  // 动态层级大小（Level-style compaction）
  // 功能：启用动态调整各层目标大小，优化层级大小分布
  // 默认值：false（不启用）
  // 工作原理：
  //   - 根据实际数据量动态调整每层的目标大小
  //   - 避免某些层过小或过大
  //   - 优化压缩性能和空间使用
  // 使用场景：
  //   - 数据量快速增长：启用动态调整
  //   - 数据量稳定：可以不启用
  // 性能影响：
  //   - 启用：
  //     - 优点：更合理的层级分布，更好的性能
  //     - 缺点：需要一定的计算开销
  //   - 不启用：
  //     - 优点：简单，开销小
  //     - 缺点：可能产生不合理的层级分布
  // 重要说明：
  //   - 只对 Level-style compaction 有效
  //   - DB 打开后不能更改（immutable）
  bool level_compaction_dynamic_level_bytes;

  // 动态文件大小（Level-style compaction）
  // 功能：启用动态调整目标文件大小，优化文件大小分布
  // 默认值：false（不启用）
  // 工作原理：
  //   - 根据层数和数据量动态调整目标文件大小
  //   - 优化文件数量和大小
  // 使用场景：
  //   - 多层 Level compaction：启用动态调整
  //   - 少层 Level compaction：可以不启用
  // 性能影响：
  //   - 启用：
  //     - 优点：更合理的文件大小分布
  //     - 缺点：增加复杂性
  //   - 不启用：
  //     - 优点：简单
  //     - 缺点：可能产生不合理的文件大小
  // 重要说明：
  //   - 只对 Level-style compaction 有效
  //   - DB 打开后不能更改（immutable）
  bool level_compaction_dynamic_file_size;

  // 层数
  // 功能：指定压缩的层数
  // 默认值：7
  // 工作原理：
  //   - 数据分层存储，从 L0 到 L(num_levels-1)
  //   - L0 层可能有重叠，L1+ 层不重叠
  //   - 每层的数据量呈指数增长
  // 使用场景：
  //   - Level-style compaction：
  //     - 小数据量：3-5 层
  //     - 中等数据量：5-7 层
  //     - 大数据量：7-10 层
  //   - Universal-style compaction：
  //     - 通常设置为 1 层或更少
  //   - FIFO-style compaction：
  //     - 通常设置为 1 层
  // 性能影响：
  //   - 较少的层数：
  //     - 优点：读放大小，读取性能好
  //     - 缺点：写放大大
  //   - 较多的层数：
  //     - 优点：写放大小
  //     - 缺点：读放大大，读取性能差
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - Level-style 推荐使用 7 层
  //   - Universal/FIFO 推荐使用 1 层
  int num_levels;

  // 优化过滤器以提高命中性能
  // 功能：优先优化 Bloom filter 以提高命中率场景的性能
  // 默认值：false（优先优化未命中场景）
  // 工作原理：
  //   - false：优先优化未命中场景（键不存在的情况）
  //     Bloom filter 更积极，增加过滤能力
  //   - true：优先优化命中场景（键存在的情况）
  //     Bloom filter 较保守，减少假阳性
  // 使用场景：
  //   - 命中率高：设置为 true
  //   - 未命中率高：设置为 false（默认）
  // 性能影响：
  //   - true：
  //     - 优点：减少命中场景的磁盘 I/O
  //     - 缺点：增加未命中场景的磁盘 I/O
  //   - false：
  //     - 优点：减少未命中场景的磁盘 I/O
  //     - 缺点：增加命中场景的磁盘 I/O
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 根据实际命中率选择
  bool optimize_filters_for_hits;

  // 强制一致性检查
  // 功能：启用额外的内部一致性检查，帮助发现 Bug
  // 默认值：false（不启用）
  // 工作原理：
  //   - 在关键路径上添加断言和验证
  //   - 发现不一致时触发断言失败
  // 使用场景：
  //   - 开发和测试：启用以发现问题
  //   - 生产环境：禁用以提高性能
  // 性能影响：
  //   - 启用：
  //     - 优点：帮助发现 Bug
  //     - 缺点：显著降低性能
  //   - 不启用：
  //     - 优点：性能最佳
  //     - 缺点：难以发现内部 Bug
  // 重要说明：
  //   - 仅用于开发和测试
  //   - DB 打开后不能更改（immutable）
  //   - 生产环境应该禁用
  bool force_consistency_checks;

  // 排除最底层数据的秒数
  // 功能：指定数据在最底层可以停留的最长时间
  // 默认值：0（不限制）
  // 工作原理：
  //   - 数据在最底层停留超过此时间后，会触发压缩
  //   - 用于避免数据长期停留在最底层
  // 使用场景：
  //   - 时序数据：设置合理的 TTL
  //   - 日志数据：定期清理旧数据
  // 重要说明：
  //   - 0 表示不限制
  //   - DB 打开后不能更改（immutable）
  uint64_t preclude_last_level_data_seconds;

  // 保留内部时间的秒数
  // 功能：指定保留内部时间戳的持续时间
  // 默认值：0（不保留）
  // 工作原理：
  //   - 内部时间戳用于版本控制和快照
  //   - 超过此时间的旧版本数据会被清理
  // 使用场景：
  //   - 快照管理：控制快照的保留时间
  //   - 版本管理：控制旧版本的保留时间
  // 重要说明：
  //   - 0 表示不限制
  //   - DB 打开后不能更改（immutable）
  uint64_t preserve_internal_time_seconds;

  // MemTable 插入提示前缀提取器
  // 功能：为 MemTable 插入操作提供前缀提取器，用于优化插入性能
  // 默认值：nullptr（不使用）
  // 工作原理：
  //   - 在插入键时，提取前缀用于提示 MemTable 的位置
  //   - 减少 MemTable 的查找时间
  //   - 适用于某些 MemTable 实现（如 SkipList）
  // 使用场景：
  //   - 写入密集型：可以提高插入性能
  //   - 键有明显前缀结构
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 与 prefix_extractor 不同，此提取器仅用于插入优化
  std::shared_ptr<const SliceTransform>
      memtable_insert_with_hint_prefix_extractor;

  // 列族存储路径
  // 功能：指定此列族的 SST 文件存储路径，实现数据分层存储
  // 默认值：空（使用 db_paths）
  // 工作原理：
  //   - 新数据存储在路径列表前面的路径
  //   - 老数据通过压缩逐渐移动到后面的路径
  //   - 每个路径可以指定目标大小
  // 使用场景：
  //   - SSD + HDD 混合存储：
  //     - 路径 0：/ssd/rocksdb（新数据）
  //     - 路径 1：/hdd/rocksdb（老数据）
  //   - 分层存储：
  //     - 路径 0：/fast/storage（热数据）
  //     - 路径 1：/slow/storage（冷数据）
  // 重要说明：
  //   - 如果为空，使用 db_paths
  //   - DB 打开后不能更改（immutable）
  std::vector<DbPath> cf_paths;

  // 压缩线程限制器
  // 功能：限制列族的最大并发压缩任务数量
  // 默认值：nullptr（不限制）
  // 工作原理：
  //   - 限制同时进行的压缩任务数量
  //   - 可以在多个列族或 DB 实例间共享
  //   - 当达到限制时，新的压缩任务需要等待
  // 使用场景：
  //   - CPU 受限：限制压缩线程数，避免影响主业务
  //   - I/O 受限：减少并发 I/O，避免磁盘瓶颈
  //   - 多租户：为不同租户的列族分配不同资源
  // 重要说明：
  //   - nullptr 表示不限制
  //   - DB 打开后不能更改（immutable）
  std::shared_ptr<ConcurrentTaskLimiter> compaction_thread_limiter;

  // SST 文件分割器工厂（实验性功能）
  // 功能：根据键前缀分割 SST 文件，减少压缩时的写放大
  // 默认值：nullptr（不使用）
  // 工作原理：
  //   - 在压缩时，根据键前缀将 SST 文件分割成多个文件
  //   - 避免单个 SST 文件跨越整个键空间
  //   - 下次压缩时，可以只选择部分文件
  // 使用场景：
  //   - 键空间分布不均匀
  //   - 压缩优化
  // 重要说明：
  //   - 实验性功能，可能不稳定
  //   - DB 打开后不能更改（immutable）
  std::shared_ptr<SstPartitionerFactory> sst_partitioner_factory;

  // Blob 缓存
  // 功能：指定 Blob 文件的缓存
  // 默认值：nullptr（不使用）
  // 工作原理：
  //   - 缓存 Blob 文件中的大值
  //   - 减少 Blob 文件的磁盘 I/O
  //   - 与 block_cache 不同，block_cache 缓存的是数据块
  // 使用场景：
  //   - 大值存储：存储大量大值（如图片、文档）
  //   - 读取密集：频繁读取 Blob 值
  // 重要说明：
  //   - 仅当启用 Blob 文件时有效
  //   - DB 打开后不能更改（immutable）
  std::shared_ptr<Cache> blob_cache;

  // 持久化用户定义的时间戳
  // 功能：控制是否将用户定义的时间戳持久化到 SST 文件
  // 默认值：false（不持久化）
  // 工作原理：
  //   - 如果启用，用户时间戳会存储在 SST 文件中
  //   - 时间戳可以用于时序数据管理、TTL 等
  // 使用场景：
  //   - 时序数据：需要存储时间戳
  //   - TTL 管理：根据时间戳过期数据
  // 重要说明：
  //   - DB 打开后不能更改（immutable）
  //   - 需要配合 comparator 支持
  bool persist_user_defined_timestamps;
};

struct ImmutableOptions : public ImmutableDBOptions, public ImmutableCFOptions {
  explicit ImmutableOptions();
  explicit ImmutableOptions(const Options& options);

  ImmutableOptions(const DBOptions& db_options,
                   const ColumnFamilyOptions& cf_options);

  ImmutableOptions(const ImmutableDBOptions& db_options,
                   const ImmutableCFOptions& cf_options);

  ImmutableOptions(const DBOptions& db_options,
                   const ImmutableCFOptions& cf_options);

  ImmutableOptions(const ImmutableDBOptions& db_options,
                   const ColumnFamilyOptions& cf_options);
};

struct MutableCFOptions {
  static const char* kName() { return "MutableCFOptions"; }
  explicit MutableCFOptions(const ColumnFamilyOptions& options)
      : write_buffer_size(options.write_buffer_size),
        max_write_buffer_number(options.max_write_buffer_number),
        arena_block_size(options.arena_block_size),
        memtable_prefix_bloom_size_ratio(
            options.memtable_prefix_bloom_size_ratio),
        memtable_whole_key_filtering(options.memtable_whole_key_filtering),
        memtable_huge_page_size(options.memtable_huge_page_size),
        max_successive_merges(options.max_successive_merges),
        inplace_update_num_locks(options.inplace_update_num_locks),
        prefix_extractor(options.prefix_extractor),
        experimental_mempurge_threshold(
            options.experimental_mempurge_threshold),
        disable_auto_compactions(options.disable_auto_compactions),
        soft_pending_compaction_bytes_limit(
            options.soft_pending_compaction_bytes_limit),
        hard_pending_compaction_bytes_limit(
            options.hard_pending_compaction_bytes_limit),
        level0_file_num_compaction_trigger(
            options.level0_file_num_compaction_trigger),
        level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
        level0_stop_writes_trigger(options.level0_stop_writes_trigger),
        max_compaction_bytes(options.max_compaction_bytes),
        ignore_max_compaction_bytes_for_input(
            options.ignore_max_compaction_bytes_for_input),
        target_file_size_base(options.target_file_size_base),
        target_file_size_multiplier(options.target_file_size_multiplier),
        max_bytes_for_level_base(options.max_bytes_for_level_base),
        max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
        ttl(options.ttl),
        periodic_compaction_seconds(options.periodic_compaction_seconds),
        max_bytes_for_level_multiplier_additional(
            options.max_bytes_for_level_multiplier_additional),
        compaction_options_fifo(options.compaction_options_fifo),
        compaction_options_universal(options.compaction_options_universal),
        enable_blob_files(options.enable_blob_files),
        min_blob_size(options.min_blob_size),
        blob_file_size(options.blob_file_size),
        blob_compression_type(options.blob_compression_type),
        enable_blob_garbage_collection(options.enable_blob_garbage_collection),
        blob_garbage_collection_age_cutoff(
            options.blob_garbage_collection_age_cutoff),
        blob_garbage_collection_force_threshold(
            options.blob_garbage_collection_force_threshold),
        blob_compaction_readahead_size(options.blob_compaction_readahead_size),
        blob_file_starting_level(options.blob_file_starting_level),
        prepopulate_blob_cache(options.prepopulate_blob_cache),
        max_sequential_skip_in_iterations(
            options.max_sequential_skip_in_iterations),
        check_flush_compaction_key_order(
            options.check_flush_compaction_key_order),
        paranoid_file_checks(options.paranoid_file_checks),
        report_bg_io_stats(options.report_bg_io_stats),
        compression(options.compression),
        bottommost_compression(options.bottommost_compression),
        compression_opts(options.compression_opts),
        bottommost_compression_opts(options.bottommost_compression_opts),
        last_level_temperature(options.last_level_temperature ==
                                       Temperature::kUnknown
                                   ? options.bottommost_temperature
                                   : options.last_level_temperature),
        memtable_protection_bytes_per_key(
            options.memtable_protection_bytes_per_key),
        block_protection_bytes_per_key(options.block_protection_bytes_per_key),
        sample_for_compression(
            options.sample_for_compression),  // TODO: is 0 fine here?
        compression_per_level(options.compression_per_level) {
    RefreshDerivedOptions(options.num_levels, options.compaction_style);
  }

  MutableCFOptions()
      : write_buffer_size(0),
        max_write_buffer_number(0),
        arena_block_size(0),
        memtable_prefix_bloom_size_ratio(0),
        memtable_whole_key_filtering(false),
        memtable_huge_page_size(0),
        max_successive_merges(0),
        inplace_update_num_locks(0),
        prefix_extractor(nullptr),
        experimental_mempurge_threshold(0.0),
        disable_auto_compactions(false),
        soft_pending_compaction_bytes_limit(0),
        hard_pending_compaction_bytes_limit(0),
        level0_file_num_compaction_trigger(0),
        level0_slowdown_writes_trigger(0),
        level0_stop_writes_trigger(0),
        max_compaction_bytes(0),
        ignore_max_compaction_bytes_for_input(true),
        target_file_size_base(0),
        target_file_size_multiplier(0),
        max_bytes_for_level_base(0),
        max_bytes_for_level_multiplier(0),
        ttl(0),
        periodic_compaction_seconds(0),
        compaction_options_fifo(),
        enable_blob_files(false),
        min_blob_size(0),
        blob_file_size(0),
        blob_compression_type(kNoCompression),
        enable_blob_garbage_collection(false),
        blob_garbage_collection_age_cutoff(0.0),
        blob_garbage_collection_force_threshold(0.0),
        blob_compaction_readahead_size(0),
        blob_file_starting_level(0),
        prepopulate_blob_cache(PrepopulateBlobCache::kDisable),
        max_sequential_skip_in_iterations(0),
        check_flush_compaction_key_order(true),
        paranoid_file_checks(false),
        report_bg_io_stats(false),
        compression(Snappy_Supported() ? kSnappyCompression : kNoCompression),
        bottommost_compression(kDisableCompressionOption),
        last_level_temperature(Temperature::kUnknown),
        memtable_protection_bytes_per_key(0),
        block_protection_bytes_per_key(0),
        sample_for_compression(0) {}

  explicit MutableCFOptions(const Options& options);

  // Must be called after any change to MutableCFOptions
  void RefreshDerivedOptions(int num_levels, CompactionStyle compaction_style);

  void RefreshDerivedOptions(const ImmutableCFOptions& ioptions) {
    RefreshDerivedOptions(ioptions.num_levels, ioptions.compaction_style);
  }

  int MaxBytesMultiplerAdditional(int level) const {
    if (level >=
        static_cast<int>(max_bytes_for_level_multiplier_additional.size())) {
      return 1;
    }
    return max_bytes_for_level_multiplier_additional[level];
  }

  void Dump(Logger* log) const;

  // Memtable related options
  size_t write_buffer_size;
  int max_write_buffer_number;
  size_t arena_block_size;
  double memtable_prefix_bloom_size_ratio;
  bool memtable_whole_key_filtering;
  size_t memtable_huge_page_size;
  size_t max_successive_merges;
  size_t inplace_update_num_locks;
  std::shared_ptr<const SliceTransform> prefix_extractor;
  // [experimental]
  // Used to activate or deactive the Mempurge feature (memtable garbage
  // collection). (deactivated by default). At every flush, the total useful
  // payload (total entries minus garbage entries) is estimated as a ratio
  // [useful payload bytes]/[size of a memtable (in bytes)]. This ratio is then
  // compared to this `threshold` value:
  //     - if ratio<threshold: the flush is replaced by a mempurge operation
  //     - else: a regular flush operation takes place.
  // Threshold values:
  //   0.0: mempurge deactivated (default).
  //   1.0: recommended threshold value.
  //   >1.0 : aggressive mempurge.
  //   0 < threshold < 1.0: mempurge triggered only for very low useful payload
  //   ratios.
  // [experimental]
  double experimental_mempurge_threshold;

  // Compaction related options
  bool disable_auto_compactions;
  uint64_t soft_pending_compaction_bytes_limit;
  uint64_t hard_pending_compaction_bytes_limit;
  int level0_file_num_compaction_trigger;
  int level0_slowdown_writes_trigger;
  int level0_stop_writes_trigger;
  uint64_t max_compaction_bytes;
  bool ignore_max_compaction_bytes_for_input;
  uint64_t target_file_size_base;
  int target_file_size_multiplier;
  uint64_t max_bytes_for_level_base;
  double max_bytes_for_level_multiplier;
  uint64_t ttl;
  uint64_t periodic_compaction_seconds;
  std::vector<int> max_bytes_for_level_multiplier_additional;
  CompactionOptionsFIFO compaction_options_fifo;
  CompactionOptionsUniversal compaction_options_universal;

  // Blob file related options
  bool enable_blob_files;
  uint64_t min_blob_size;
  uint64_t blob_file_size;
  CompressionType blob_compression_type;
  bool enable_blob_garbage_collection;
  double blob_garbage_collection_age_cutoff;
  double blob_garbage_collection_force_threshold;
  uint64_t blob_compaction_readahead_size;
  int blob_file_starting_level;
  PrepopulateBlobCache prepopulate_blob_cache;

  // Misc options
  uint64_t max_sequential_skip_in_iterations;
  bool check_flush_compaction_key_order;
  bool paranoid_file_checks;
  bool report_bg_io_stats;
  CompressionType compression;
  CompressionType bottommost_compression;
  CompressionOptions compression_opts;
  CompressionOptions bottommost_compression_opts;
  Temperature last_level_temperature;
  uint32_t memtable_protection_bytes_per_key;
  uint8_t block_protection_bytes_per_key;

  uint64_t sample_for_compression;
  std::vector<CompressionType> compression_per_level;

  // Derived options
  // Per-level target file size.
  std::vector<uint64_t> max_file_size;
};

uint64_t MultiplyCheckOverflow(uint64_t op1, double op2);

// Get the max file size in a given level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options,
    int level, CompactionStyle compaction_style, int base_level = 1,
    bool level_compaction_dynamic_level_bytes = false);

// Get the max size of an L0 file for which we will pin its meta-blocks when
// `pin_l0_filter_and_index_blocks_in_cache` is set.
size_t MaxFileSizeForL0MetaPin(const MutableCFOptions& cf_options);

Status GetStringFromMutableCFOptions(const ConfigOptions& config_options,
                                     const MutableCFOptions& mutable_opts,
                                     std::string* opt_string);

Status GetMutableOptionsFromStrings(
    const MutableCFOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    Logger* info_log, MutableCFOptions* new_options);

}  // namespace ROCKSDB_NAMESPACE
