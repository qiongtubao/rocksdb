// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

struct ImmutableDBOptions {
  static const char* kName() { return "ImmutableDBOptions"; }
  ImmutableDBOptions();
  explicit ImmutableDBOptions(const DBOptions& options);

  void Dump(Logger* log) const;

  // === 数据库创建和打开选项 ===

  // 如果数据库不存在，则创建一个新的数据库
  // true: 打开不存在的数据库时自动创建
  // false: 打开不存在的数据库返回错误
  bool create_if_missing;

  // 当指定的列族不存在时自动创建缺失的列族
  // 适用于在打开数据库时指定多个列族名称的场景
  bool create_missing_column_families;

  // 如果数据库已存在，则报错
  // 与 create_if_missing 配合使用，确保不会意外覆盖现有数据库
  bool error_if_exists;

  // 启用偏执检查（paranoid checks）
  // 为提高数据完整性，会对数据库进行额外的检查：
  // - 强制检查压缩后的文件
  // - 检查 SST 文件的校验和
  // 注意：这会显著影响性能，通常只用于调试
  bool paranoid_checks;

  // 在 flush 时验证 memtable 的数量
  // 用于调试目的，确保 flush 的一致性
  bool flush_verify_memtable_count;

  // 在 manifest 中跟踪和验证 WAL 文件
  // 确保所有 WAL 文件都被正确记录和引用
  bool track_and_verify_wals_in_manifest;

  // 在 manifest 中验证 SST 文件的唯一 ID
  // 确保 SST 文件标识符的唯一性，防止重复或错误引用
  bool verify_sst_unique_id_in_manifest;

  // === 环境和资源管理 ===

  // RocksDB 使用的环境接口
  // 封装了文件系统操作、线程创建、时间获取等系统调用
  // 默认使用 Env::Default()
  Env* env;

  // 速率限制器
  // 用于限制写入和压缩的 I/O 速率，防止磁盘 I/O 成为瓶颈
  // 可以限制 Flush 和 Compaction 的带宽使用
  std::shared_ptr<RateLimiter> rate_limiter;

  // SST 文件管理器
  // 跟踪和管理 SST 文件的总大小，当超过限制时删除最旧的文件
  // 用于控制 SST 文件的磁盘使用量
  std::shared_ptr<SstFileManager> sst_file_manager;

  // 信息日志记录器
  // RocksDB 用于输出信息性消息（如压缩进度、统计信息等）的日志
  // 默认输出到标准输出
  std::shared_ptr<Logger> info_log;

  // 信息日志的级别
  // 控制日志输出的详细程度
  // 可选值：DEBUG_LEVEL, INFO_LEVEL, WARN_LEVEL, ERROR_LEVEL, FATAL_LEVEL, HEADER_LEVEL
  InfoLogLevel info_log_level;

  // 最大并发打开文件的线程数
  // 用于数据库打开时并发加载 SST 文件
  // 较大的值可以加快打开速度，但会增加资源消耗
  int max_file_opening_threads;

  // 统计信息收集器
  // 收集和存储数据库运行时的各种统计数据（如读写延迟、压缩次数等）
  // 用于性能分析和监控
  std::shared_ptr<Statistics> statistics;

  // === 文件系统和 I/O 选项 ===

  // 使用 fsync 而非 fdatasync
  // fsync: 同步数据和元数据，确保数据完全写入磁盘
  // fdatasync: 仅同步数据，性能更好但安全性稍差
  // true: 更安全但较慢
  bool use_fsync;

  // 数据库文件存储路径列表
  // 支持将数据文件分布在多个目录中，突破单目录的 inode 限制
  // 第一个路径作为主目录，其他路径用于存放 SST 文件
  std::vector<DbPath> db_paths;

  // 数据库日志文件的存储目录
  // 如果为空，则与数据库文件在同一目录
  std::string db_log_dir;

  // WAL 文件的存储目录（从配置文件读取的原始值）
  // 注意：要确定实际使用的目录，应使用 GetWalDir 或 IsWalDirSameAsDBPath 方法
  // 而不是直接访问此变量
  // The wal_dir option from the file.  To determine the
  // directory in use, the GetWalDir or IsWalDirSameAsDBPath
  // methods should be used instead of accessing this variable directly.
  std::string wal_dir;

  // === 日志管理选项 ===

  // 单个日志文件的最大大小（字节）
  // 超过此大小后创建新的日志文件
  size_t max_log_file_size;

  // 日志文件滚动的时间间隔（秒）
  // 每隔指定时间创建新的日志文件，与 max_log_file_size 配合使用
  size_t log_file_time_to_roll;

  // 保留的日志文件数量
  // 超过此数量时删除最旧的日志文件
  size_t keep_log_file_num;

  // 可回收的日志文件数量
  // 这些日志文件会被重命名而不是删除，可以减少文件创建开销
  size_t recycle_log_file_num;

  // === Manifest 文件管理 ===

  // MANIFEST 文件的最大大小（字节）
  // MANIFEST 文件记录数据库的元数据（列族、文件列表等）
  // 超过此大小会创建新的 MANIFEST 文件
  uint64_t max_manifest_file_size;

  // === 缓存选项 ===

  // 表缓存的分片数量（以 2 的幂次方表示）
  // 用于分片表缓存以减少锁争用
  // 默认值 6 表示 64 个分片
  int table_cache_numshardbits;

  // === WAL（写前日志）选项 ===

  // WAL 文件的存活时间（秒）
  // 超过此时间的 WAL 文件会被删除（即使可能包含未恢复的数据）
  // 用于清理旧的 WAL 文件
  uint64_t WAL_ttl_seconds;

  // WAL 文件的总大小限制（MB）
  // 当 WAL 文件总大小超过此限制时，删除最旧的 WAL 文件
  uint64_t WAL_size_limit_MB;

  // === 写入优化选项 ===

  // 单个写入批次的最大字节数
  // 多个小写入会被合并为一个批次写入，以提高吞吐量
  // 较大的值可以提高吞吐量，但会增加延迟
  uint64_t max_write_batch_group_size_bytes;

  // === 存储优化选项 ===

  // MANIFEST 文件的预分配大小（字节）
  // 预分配空间可以减少文件系统碎片和分配开销
  size_t manifest_preallocation_size;

  // 允许使用 mmap 进行读操作
  // 将 SST 文件映射到内存空间读取
  // true: 可减少数据拷贝，但可能受地址空间限制
  bool allow_mmap_reads;

  // 允许使用 mmap 进行写操作
  // 通过 mmap 方式写入日志和数据文件
  // true: 可提高写入性能，但在某些文件系统上可能不稳定
  bool allow_mmap_writes;

  // 使用直接 I/O 进行读操作（O_DIRECT）
  // 跳过操作系统的页缓存，直接从磁盘读取
  // true: 减少内存拷贝，但需要应用程序自行管理缓存对齐
  bool use_direct_reads;

  // 在 flush 和 compaction 时使用直接 I/O
  // 跳过页缓存直接读写磁盘
  // 适合 SSD 存储和大文件场景
  bool use_direct_io_for_flush_and_compaction;

  // 允许使用 fallocate 预分配文件空间
  // 预先分配空间可以减少文件系统碎片
  // true: 减少写入时的文件分配开销
  bool allow_fallocate;

  // 在执行 exec 调用时关闭文件描述符
  // 防止子进程继承数据库的文件描述符
  // true: 更安全，避免文件描述符泄露
  bool is_fd_close_on_exec;

  // 打开文件时给出随机访问的建议
  // 使用 posix_fadvise(POSIX_FADV_RANDOM) 提示系统文件将被随机访问
  // true: 优化随机访问模式
  bool advise_random_on_open;

  // === 内存和缓冲区管理 ===

  // 数据库总写缓冲区大小（字节）
  // 所有列族的 memtable 总大小上限
  // 超过此限制时会触发 flush
  size_t db_write_buffer_size;

  // 写缓冲区管理器
  // 跟踪和控制所有列族的 memtable 内存使用
  // 当内存使用超过限制时触发 flush 或停止写入
  std::shared_ptr<WriteBufferManager> write_buffer_manager;

  // === 压缩访问提示 ===

  // 压缩开始时的访问提示
  // 提示系统即将顺序访问数据，用于预读优化
  // 类型包括：NORMAL, SEQUENTIAL, WILLNEED, NONE
  DBOptions::AccessHint access_hint_on_compaction_start;

  // 随机访问的最大缓冲区大小（字节）
  // 用于预读优化的缓冲区大小
  size_t random_access_max_buffer_size;

  // === 并发和线程管理 ===

  // 使用自适应互斥锁
  // 根据竞争情况动态选择互斥锁的实现方式
  // true: 在低竞争时使用更轻量的实现
  bool use_adaptive_mutex;

  // 事件监听器列表
  // 注册的监听器会在特定事件（如 flush、compaction）发生时被调用
  // 用于监控、审计和自定义处理
  std::vector<std::shared_ptr<EventListener>> listeners;

  // 启用线程跟踪
  // 记录线程的创建和销毁信息
  // 用于调试和分析线程使用情况
  bool enable_thread_tracking;

  // 启用流水线写入
  // 允许写入流水线化，提高并发写入性能
  // true: 将写入分为多个阶段并行处理
  bool enable_pipelined_write;

  // 允许无序写入
  // 写入顺序可能与写入请求的提交顺序不同
  // true: 可提高性能，但需要应用能处理乱序
  bool unordered_write;

  // 允许并发写入 memtable
  // 多个写入线程可以并发修改 memtable
  // true: 提高并发写入性能
  bool allow_concurrent_memtable_write;

  // 启用写入线程自适应让出 CPU
  // 写入线程在等待锁时自适应地让出 CPU 时间片
  // true: 在竞争时减少 CPU 占用，false: 立即自旋等待
  bool enable_write_thread_adaptive_yield;

  // 写入线程最大让出时间（微秒）
  // 自适应让出 CPU 的最大时间阈值
  uint64_t write_thread_max_yield_usec;

  // 写入线程慢速让出时间（微秒）
  // 超过此时间后认为让出是慢速的，可能需要调整策略
  uint64_t write_thread_slow_yield_usec;

  // === 数据库打开选项 ===

  // 打开数据库时跳过统计信息更新
  // 跳过打开时计算统计信息的过程
  // true: 加快数据库打开速度，但统计数据可能不准确
  bool skip_stats_update_on_db_open;

  // 打开数据库时跳过 SST 文件大小检查
  // 不验证 SST 文件的大小是否与 MANIFEST 记录一致
  // true: 加快打开速度，但可能忽略文件损坏
  bool skip_checking_sst_file_sizes_on_db_open;

  // === WAL 恢复模式 ===

  // WAL 恢复模式
  // 控制数据库启动时从 WAL 恢复数据的策略
  // 可选值：
  // - kRecoveryPointConsistent: 恢复到一致性点（默认，最快）
  // - kAbsoluteConsistency: 绝对一致性（可能更慢）
  // - kPointInTimeRecovery: 时间点恢复
  // - kSkipAnyCorruptedRecords: 跳过损坏的记录
  WALRecoveryMode wal_recovery_mode;

  // === 两阶段提交 (2PC) 选项 ===

  // 允许使用两阶段提交协议
  // 支持跨数据库或跨列族的原子性事务
  // true: 启用 WriteBatchWithIndex 和事务的两阶段提交
  bool allow_2pc;

  // === 行缓存选项 ===

  // 行缓存
  // 缓存从表中读取的行数据
  // 与 block cache 配合使用，适用于热点数据的查询场景
  std::shared_ptr<Cache> row_cache;

  // === WAL 过滤器 ===

  // WAL 过滤器
  // 在 WAL 恢复时调用，允许过滤或修改 WAL 记录
  // 用于自定义 WAL 处理逻辑
  WalFilter* wal_filter;

  // === 配置文件选项 ===

  // 如果 OPTIONS 文件存在错误则失败
  // 打开数据库时检查 OPTIONS 文件的完整性
  // true: 配置错误时直接返回错误
  bool fail_if_options_file_error;

  // 导出内存分配统计信息
  // 将内存分配器的统计信息写入日志
  // 用于分析内存使用情况
  bool dump_malloc_stats;

  // === 恢复选项 ===

  // 恢复期间避免 flush
  // 在从 WAL 恢复数据时不执行 flush 操作
  // true: 加快恢复速度，但可能增加恢复后的 flush 压力
  bool avoid_flush_during_recovery;

  // 允许在数据库末尾导入数据
  // 允许使用 IngestExternalFile 在数据库末尾添加 SST 文件
  // 适用于数据导入和归档场景
  bool allow_ingest_behind;

  // 使用两个写入队列
  // 主写入队列和非 WAL 写入队列分离
  // true: 可以提高没有 WAL 的写入性能
  bool two_write_queues;

  // 手动 WAL 刷新
  // 禁用自动 WAL 刷新，需要显式调用 FlushWAL
  // true: 提供更精细的 WAL 控制权
  bool manual_wal_flush;

  // === WAL 压缩选项 ===

  // WAL 压缩类型
  // 压缩 WAL 文件以减少磁盘空间使用
  // 可选值：kNoCompression, kSnappyCompression, kZlibCompression, kBZip2Compression 等
  CompressionType wal_compression;

  // === Flush 选项 ===

  // 原子性 flush
  // 确保 flush 所有列族时的一致性
  // true: 要么全部成功，要么全部失败
  bool atomic_flush;

  // === I/O 优化选项 ===

  // 避免不必要的阻塞 I/O
  // 使用非阻塞或异步 I/O 减少线程阻塞
  // true: 提高并发性能
  bool avoid_unnecessary_blocking_io;

  // === 统计持久化选项 ===

  // 将统计信息持久化到磁盘
  // 定期将统计信息写入磁盘
  // true: 数据库重启后可以恢复之前的统计数据
  bool persist_stats_to_disk;

  // 将 DBID 写入 MANIFEST 文件
  // 在 MANIFEST 中记录数据库的唯一标识符
  // 用于区分不同的数据库实例
  bool write_dbid_to_manifest;

  // === 日志预读大小 ===

  // 日志文件预读大小（字节）
  // 读取 WAL 文件时的预读缓冲区大小
  // 较大的值可以减少 I/O 次数，但会增加内存使用
  size_t log_readahead_size;

  // === 文件校验选项 ===

  // 文件校验和生成器工厂
  // 用于生成文件校验和的工厂类
  // 确保文件的完整性和正确性
  std::shared_ptr<FileChecksumGenFactory> file_checksum_gen_factory;

  // === 错误恢复选项 ===

  // 尽最大努力恢复
  // 在遇到错误时尝试尽可能多的恢复数据
  // true: 可能包含部分损坏的数据，但最大化恢复率
  bool best_efforts_recovery;

  // 后台错误恢复的最大尝试次数
  // 在后台线程遇到错误后的最大恢复尝试次数
  // 0 表示无限重试
  int max_bgerror_resume_count;

  // 后台错误恢复的重试间隔（微秒）
  // 每次恢复失败后等待的时间
  uint64_t bgerror_resume_retry_interval;

  // 允许错误消息中包含数据
  // 在错误消息中包含部分数据内容
  // true: 方便调试，但可能泄露敏感信息
  bool allow_data_in_errors;

  // === 多实例管理选项 ===

  // 数据库主机 ID
  // 标识运行此数据库实例的主机
  // 用于多实例部署和管理
  std::string db_host_id;

  // 文件类型集合，启用了校验和移交
  // 指定哪些文件类型需要校验和验证
  FileTypeSet checksum_handoff_file_types;

  // 最低使用的缓存层
  // 指定最低优先级的缓存层（如 volatile, non_volatile, persistent）
  // 用于分层存储策略
  CacheTier lowest_used_cache_tier;

  // === 便利/辅助对象（不属于基础 DBOptions）===
  // Convenience/Helper objects that are not part of the base DBOptions

  // 文件系统接口
  // 封装所有文件系统操作的接口
  // 替代传统的 Env::FileSystem，提供更灵活的文件系统访问
  std::shared_ptr<FileSystem> fs;

  // 系统时钟
  // 提供时间获取功能
  // 用于时间相关的操作，如计时器、超时等
  SystemClock* clock;

  // 统计信息指针（非共享指针）
  // 直接指向统计信息对象
  // 与 std::shared_ptr<Statistics> statistics 重复，用于内部优化
  Statistics* stats;

  // 日志记录器指针（非共享指针）
  // 直接指向日志记录器对象
  // 与 std::shared_ptr<Logger> info_log 重复，用于内部优化
  Logger* logger;

  // 压缩服务
  // 将压缩任务卸载到远程服务或独立进程
  // 用于分布式压缩或专用压缩服务
  std::shared_ptr<CompactionService> compaction_service;

  // 强制执行 SingleDelete 契约
  // 确保 SingleDelete 操作的正确性
  // true: 严格执行 SingleDelete 的语义保证
  bool enforce_single_del_contracts;

  // === WAL 目录查询方法 ===

  // 检查 WAL 目录是否与数据库路径相同
  bool IsWalDirSameAsDBPath() const;

  // 检查 WAL 目录是否与指定路径相同
  bool IsWalDirSameAsDBPath(const std::string& path) const;

  // 获取 WAL 目录
  const std::string& GetWalDir() const;

  // 获取指定路径的 WAL 目录
  const std::string& GetWalDir(const std::string& path) const;
};

struct MutableDBOptions {
  static const char* kName() { return "MutableDBOptions"; }
  MutableDBOptions();
  explicit MutableDBOptions(const DBOptions& options);

  void Dump(Logger* log) const;

  // 最大后台任务数
  // 功能：控制后台线程池中可以同时执行的任务总数（包括flush和compaction）
  // 默认值：2
  // 使用场景：
  //   - 限制后台并发度，控制资源使用
  //   - 平衡读写性能和后台处理速度
  //   - 在SSD上可以设置较大值，HDD上设置较小值
  // 重要说明：
  //   - 如果设置了max_background_compactions和max_background_flushes，
  //     则此值不应小于两者之和
  //   - 增大此值可以加快后台处理，但会增加I/O竞争
  int max_background_jobs;

  // 最大后台压缩任务数
  // 功能：限制同时运行的compaction任务数量
  // 默认值：-1（表示不限制，由max_background_jobs控制）
  // 使用场景：
  //   - 控制compaction对系统资源的占用
  //   - 防止compaction影响前台读写性能
  //   - 在高写入负载下限制compactivity
  // 重要说明：
  //   - 如果设置为-1，则实际限制由max_background_jobs决定
  //   - 在SSD上通常设置为较大的值
  //   - 在HDD上通常设置为较小的值（如1-2）
  int max_background_compactions;

  // 每个压缩任务的最大子压缩数
  // 功能：控制单个compaction任务中可以并行执行的子compaction数量
  // 默认值：1
  // 取值范围：1 ~ 4（典型值）
  // 使用场景：
  //   - 在多核CPU上加速compaction
  //   - 充分利用多核处理能力
  //   - 提高大compaction的并行度
  // 重要说明：
  //   - 增大此值会增加CPU和内存使用
  //   - 对于小compaction可能没有帮助
  //   - 建议根据CPU核心数调整
  uint32_t max_subcompactions;

  // 关闭时避免flush
  // 功能：控制数据库关闭时是否等待所有memtable flush完成
  // 默认值：false
  // 使用场景：
  //   - 快速关闭数据库，允许崩溃恢复
  //   - 在某些场景下优先保证快速关闭而非数据持久化
  // 重要说明：
  //   - 设置为true时，关闭速度更快，但下次打开需要恢复WAL
  //   - 设置为false时，关闭较慢，但保证所有数据已持久化
  //   - 建议在生产环境使用false以确保数据安全
  bool avoid_flush_during_shutdown;

  // 可写文件最大缓冲区大小
  // 功能：设置WAL文件和数据文件写入时的缓冲区大小
  // 默认值：1MB（1024 * 1024）
  // 使用场景：
  //   - 平衡写入延迟和吞吐量
  //   - 大缓冲区减少系统调用次数，提高吞吐量
  //   - 小缓冲区降低延迟，但增加系统调用开销
  // 重要说明：
  //   - 增大此值会提高写入性能，但增加内存使用
  //   - 在高并发写入时，较大的缓冲区效果明显
  //   - 建议根据硬件性能和网络条件调整
  size_t writable_file_max_buffer_size;

  // 延迟写入速率
  // 功能：当需要限流时，每秒最多写入的字节数
  // 默认值：2MB/s
  // 单位：字节/秒
  // 使用场景：
  //   - 当memtable数量过多时限制写入速率
  //   - 防止写入速度过快导致系统资源耗尽
  //   - 在存储带宽有限的情况下限流
  // 重要说明：
  //   - 设置为0表示不限流
  //   - 当pending_compaction_bytes达到阈值时生效
  //   - 可以动态调整以应对负载变化
  uint64_t delayed_write_rate;

  // 最大WAL总大小
  // 功能：限制所有WAL文件的总大小
  // 默认值：0（表示不限制）
  // 单位：字节
  // 使用场景：
  //   - 限制WAL文件占用磁盘空间
  //   - 防止WAL堆积过多
  //   - 控制恢复时间
  // 重要说明：
  //   - 当总大小超过限制时，会强制触发flush
  //   - 设置为0表示只根据memtable大小决定何时flush
  //   - 建议根据磁盘空间和恢复需求设置
  uint64_t max_total_wal_size;

  // 删除过期文件的周期
  // 功能：后台线程定期删除过期文件的间隔时间
  // 默认值：6小时
  // 单位：微秒
  // 使用场景：
  //   - 定期清理不再需要的SST文件和WAL文件
  //   - 控制后台清理的频率
  //   - 平衡磁盘空间和清理开销
  // 重要说明：
  //   - 较小的值会频繁执行清理，影响性能
  //   - 较大的值会导致磁盘空间不能及时释放
  //   - 建议设置为几小时
  uint64_t delete_obsolete_files_period_micros;

  // 统计信息转储周期
  // 功能：定期将统计信息输出到日志的间隔时间
  // 默认值：600秒（10分钟）
  // 单位：秒
  // 使用场景：
  //   - 监控数据库运行状态
  //   - 定期记录性能指标
  //   - 诊断问题
  // 重要说明：
  //   - 设置为0表示不自动转储
  //   - 统计信息会输出到INFO级别日志
  //   - 生产环境建议启用以便监控
  unsigned int stats_dump_period_sec;

  // 统计信息持久化周期
  // 功能：定期将统计信息保存到磁盘的间隔时间
  // 默认值：600秒（10分钟）
  // 单位：秒
  // 使用场景：
  //   - 在数据库重启后保留统计信息
  //   - 长期监控和分析
  //   - 历史数据分析
  // 重要说明：
  //   - 设置为0表示不持久化
  //   - 统计信息存储在stats目录
  //   - 重启后可以恢复之前的统计信息
  unsigned int stats_persist_period_sec;

  // 统计信息历史缓冲区大小
  // 功能：存储历史统计信息的缓冲区大小
  // 默认值：1MB
  // 单位：字节
  // 使用场景：
  //   - 保存最近的统计信息快照
  //   - 支持统计信息查询和分析
  //   - 用于监控和告警
  // 重要说明：
  //   - 较大的值可以保存更多历史数据
  //   - 会占用更多内存
  //   - 设置为0表示不保存历史信息
  size_t stats_history_buffer_size;

  // 最大打开文件数
  // 功能：限制同时打开的文件数量
  // 默认值：-1（表示不限制，使用系统限制）
  // 使用场景：
  //   - 控制文件描述符使用
  //   - 在文件描述符有限的环境下使用
  //   - 限制表缓存大小
  // 重要说明：
  //   - 设置为-1使用系统允许的最大值
  //   - 较小的值会触发更频繁的文件打开/关闭
  //   - 影响table_cache的缓存效率
  int max_open_files;

  // 每次同步的字节数
  // 功能：在写入数据文件时，每积累这么多字节就调用一次sync
  // 默认值：0（表示禁用，由OS决定何时刷盘）
  // 单位：字节
  // 使用场景：
  //   - 平衡数据持久化和性能
  //   - 控制数据持久化的粒度
  //   - 在某些存储上提高持久性
  // 重要说明：
  //   - 较小的值会导致频繁sync，影响性能
  //   - 较大的值增加数据丢失风险
  //   - 建议根据可靠性和性能需求设置
  uint64_t bytes_per_sync;

  // WAL文件每次同步的字节数
  // 功能：在写入WAL文件时，每积累这么多字节就调用一次sync
  // 默认值：0（表示禁用，由OS决定何时刷盘）
  // 单位：字节
  // 使用场景：
  //   - 控制WAL的持久化频率
  //   - 平衡数据持久化和性能
  //   - 提高崩溃恢复能力
  // 重要说明：
  //   - 较小的值提高持久性，但影响写入性能
  //   - 较大的值增加数据丢失风险
  //   - 对于关键数据建议设置较小的值
  uint64_t wal_bytes_per_sync;

  // 严格按字节数同步
  // 功能：严格按bytes_per_sync和wal_bytes_per_sync指定的字节数执行sync
  // 默认值：false
  // 使用场景：
  //   - 确保数据持久化的精确控制
  //   - 某些特殊存储设备需要
  // 重要说明：
  //   - 设置为true时会更严格地控制sync时机
  //   - 可能影响性能
  //   - 通常不需要设置
  bool strict_bytes_per_sync;

  // 压缩预读大小
  // 功能：在compaction时预读的数据大小
  // 默认值：0（表示禁用预读）
  // 单位：字节
  // 使用场景：
  //   - 在HDD上提高compaction性能
  //   - 减少I/O等待时间
  //   - 利用顺序读优势
  // 重要说明：
  //   - 在SSD上通常不需要设置
  //   - 在HDD上建议设置为较大值（如2MB）
  //   - 较大的值会增加内存使用
  size_t compaction_readahead_size;

  // 最大后台flush任务数
  // 功能：限制同时运行的flush任务数量
  // 默认值：-1（表示不限制，由max_background_jobs控制）
  // 使用场景：
  //   - 控制flush对系统资源的占用
  //   - 防止过多flush影响读写性能
  //   - 在高写入负载下限制flush并发度
  // 重要说明：
  //   - 如果设置为-1，则实际限制由max_background_jobs决定
  //   - flush优先级通常高于compaction
  //   - 建议根据memtable数量和flush速度需求设置
  int max_background_flushes;
};

Status GetStringFromMutableDBOptions(const ConfigOptions& config_options,
                                     const MutableDBOptions& mutable_opts,
                                     std::string* opt_string);

Status GetMutableDBOptionsFromStrings(
    const MutableDBOptions& base_options,
    const std::unordered_map<std::string, std::string>& options_map,
    MutableDBOptions* new_options);

bool MutableDBOptionsAreEqual(const MutableDBOptions& this_options,
                              const MutableDBOptions& that_options);

}  // namespace ROCKSDB_NAMESPACE
