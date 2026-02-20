//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cinttypes>  // 包含 C 标准库的整数类型定义（如 int64_t、uint64_t 等）

#include "db/db_impl/db_impl.h"  // DB 实现的主头文件
#include "db/error_handler.h"  // 错误处理相关头文件
#include "db/event_helpers.h"  // 事件处理辅助工具
#include "logging/logging.h"  // 日志记录功能
#include "monitoring/perf_context_imp.h"  // 性能监控上下文实现
#include "options/options_helper.h"  // 选项辅助工具
#include "test_util/sync_point.h"  // 同步点测试工具
#include "util/cast_util.h"  // 类型转换工具

namespace ROCKSDB_NAMESPACE {  // 进入 RocksDB 命名空间
// 便捷方法：在指定的列族中执行 Put 操作（写入键值对）
Status DBImpl::Put(const WriteOptions& o, ColumnFamilyHandle* column_family,  // DBImpl::Put: 在列族中写入键值对
                   const Slice& key, const Slice& val) {  // 键和值的切片
  // 检查列族是否有时间戳，如果有则返回错误
  const Status s = FailIfCfHasTs(column_family);  // FailIfCfHasTs: 检查列族是否配置了时间戳
  if (!s.ok()) {  // 如果检查失败（有错误）
    return s;  // 直接返回错误状态
  }
  // 调用基类 DB 的 Put 方法执行实际的写入
  return DB::Put(o, column_family, key, val);  // 调用基类 Put 方法完成写入
}

// 带时间戳的 Put 操作：在指定的列族中执行 Put 操作，同时提供时间戳
Status DBImpl::Put(const WriteOptions& o, ColumnFamilyHandle* column_family,  // DBImpl::Put: 带时间戳的写入
                   const Slice& key, const Slice& ts, const Slice& val) {  // 键、时间戳、值
  // 检查列族的时间戳是否与提供的时间戳匹配
  const Status s = FailIfTsMismatchCf(column_family, ts, /*ts_for_read=*/false);  // FailIfTsMismatchCf: 检查时间戳是否匹配（用于写入）
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 Put 方法执行实际的写入（带时间戳）
  return DB::Put(o, column_family, key, ts, val);  // 调用基类 Put 方法
}

// PutEntity 操作：写入一个实体（包含多个列）到指定的列族
Status DBImpl::PutEntity(const WriteOptions& options,  // DBImpl::PutEntity: 写入实体（多列）
                         ColumnFamilyHandle* column_family, const Slice& key,  // 列族和键
                         const WideColumns& columns) {  // 宽列（多列数据）
  // 检查列族是否有时间戳，如果有则返回错误
  const Status s = FailIfCfHasTs(column_family);  // FailIfCfHasTs: 检查列族是否有时间戳
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }

  // 调用基类 DB 的 PutEntity 方法执行实际的实体写入
  return DB::PutEntity(options, column_family, key, columns);  // 调用基类 PutEntity 方法
}

// Merge 操作：对指定的键执行合并操作（需要配置 merge_operator）
Status DBImpl::Merge(const WriteOptions& o, ColumnFamilyHandle* column_family,  // DBImpl::Merge: 合并操作
                     const Slice& key, const Slice& val) {  // 键和值
  // 检查列族是否有时间戳，如果有则返回错误
  const Status s = FailIfCfHasTs(column_family);  // FailIfCfHasTs: 检查列族是否有时间戳
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 检查列族是否配置了 merge_operator
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);  // 将列族句柄转换为具体实现类型
  if (!cfh->cfd()->ioptions()->merge_operator) {  // 如果未配置 merge_operator
    return Status::NotSupported("Provide a merge_operator when opening DB");  // 返回不支持错误
  } else {  // 如果配置了 merge_operator
    // 调用基类 DB 的 Merge 方法执行实际的合并
    return DB::Merge(o, column_family, key, val);  // 调用基类 Merge 方法
  }
}

// 带时间戳的 Merge 操作：对指定的键执行合并操作，同时提供时间戳
Status DBImpl::Merge(const WriteOptions& o, ColumnFamilyHandle* column_family,  // DBImpl::Merge: 带时间戳的合并操作
                     const Slice& key, const Slice& ts, const Slice& val) {  // 键、时间戳、值
  // 检查列族的时间戳是否与提供的时间戳匹配
  const Status s = FailIfTsMismatchCf(column_family, ts, /*ts_for_read=*/false);  // FailIfTsMismatchCf: 检查时间戳匹配
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 Merge 方法执行实际的合并（带时间戳）
  return DB::Merge(o, column_family, key, ts, val);  // 调用基类 Merge 方法
}

// Delete 操作：删除指定列族中的单个键
Status DBImpl::Delete(const WriteOptions& write_options,  // DBImpl::Delete: 删除操作
                      ColumnFamilyHandle* column_family, const Slice& key) {  // 列族和键
  // 检查列族是否有时间戳，如果有则返回错误
  const Status s = FailIfCfHasTs(column_family);  // FailIfCfHasTs: 检查列族是否有时间戳
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 Delete 方法执行实际的删除
  return DB::Delete(write_options, column_family, key);  // 调用基类 Delete 方法
}

// 带时间戳的 Delete 操作：删除指定列族中的单个键，同时提供时间戳
Status DBImpl::Delete(const WriteOptions& write_options,  // DBImpl::Delete: 带时间戳的删除
                      ColumnFamilyHandle* column_family, const Slice& key,  // 列族和键
                      const Slice& ts) {  // 时间戳
  // 检查列族的时间戳是否与提供的时间戳匹配
  const Status s = FailIfTsMismatchCf(column_family, ts, /*ts_for_read=*/false);  // FailIfTsMismatchCf: 检查时间戳匹配
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 Delete 方法执行实际的删除（带时间戳）
  return DB::Delete(write_options, column_family, key, ts);  // 调用基类 Delete 方法
}

// SingleDelete 操作：执行单次删除（比 Delete 更高效，但只能删除一个版本的键）
Status DBImpl::SingleDelete(const WriteOptions& write_options,  // DBImpl::SingleDelete: 单次删除
                            ColumnFamilyHandle* column_family,  // 列族
                            const Slice& key) {  // 键
  // 检查列族是否有时间戳，如果有则返回错误
  const Status s = FailIfCfHasTs(column_family);  // FailIfCfHasTs: 检查列族是否有时间戳
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 SingleDelete 方法执行实际的删除
  return DB::SingleDelete(write_options, column_family, key);  // 调用基类 SingleDelete 方法
}

// 带时间戳的 SingleDelete 操作：执行单次删除，同时提供时间戳
Status DBImpl::SingleDelete(const WriteOptions& write_options,  // DBImpl::SingleDelete: 带时间戳的单次删除
                            ColumnFamilyHandle* column_family, const Slice& key,  // 列族和键
                            const Slice& ts) {  // 时间戳
  // 检查列族的时间戳是否与提供的时间戳匹配
  const Status s = FailIfTsMismatchCf(column_family, ts, /*ts_for_read=*/false);  // FailIfTsMismatchCf: 检查时间戳匹配
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 SingleDelete 方法执行实际的删除（带时间戳）
  return DB::SingleDelete(write_options, column_family, key, ts);  // 调用基类 SingleDelete 方法
}

// DeleteRange 操作：删除指定列族中的键范围 [begin_key, end_key)
Status DBImpl::DeleteRange(const WriteOptions& write_options,  // DBImpl::DeleteRange: 范围删除
                           ColumnFamilyHandle* column_family,  // 列族
                           const Slice& begin_key, const Slice& end_key) {  // 开始键和结束键
  // 检查列族是否有时间戳，如果有则返回错误
  const Status s = FailIfCfHasTs(column_family);  // FailIfCfHasTs: 检查列族是否有时间戳
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 DeleteRange 方法执行实际的删除
  return DB::DeleteRange(write_options, column_family, begin_key, end_key);  // 调用基类 DeleteRange 方法
}

// 带时间戳的 DeleteRange 操作：删除键范围，同时提供时间戳
Status DBImpl::DeleteRange(const WriteOptions& write_options,  // DBImpl::DeleteRange: 带时间戳的范围删除
                           ColumnFamilyHandle* column_family,  // 列族
                           const Slice& begin_key, const Slice& end_key,  // 开始键和结束键
                           const Slice& ts) {  // 时间戳
  // 检查列族的时间戳是否与提供的时间戳匹配
  const Status s = FailIfTsMismatchCf(column_family, ts, /*ts_for_read=*/false);  // FailIfTsMismatchCf: 检查时间戳匹配
  if (!s.ok()) {  // 如果检查失败
    return s;  // 返回错误
  }
  // 调用基类 DB 的 DeleteRange 方法执行实际的删除（带时间戳）
  return DB::DeleteRange(write_options, column_family, begin_key, end_key, ts);  // 调用基类 DeleteRange 方法
}

// 设置可恢复状态的 PreRelease 回调函数
// 此回调在 WAL 写入之后、memtable 写入之前被调用
void DBImpl::SetRecoverableStatePreReleaseCallback(  // DBImpl::SetRecoverableStatePreReleaseCallback: 设置回调
    PreReleaseCallback* callback) {  // PreRelease 回调指针
  recoverable_state_pre_release_callback_.reset(callback);  // 使用智能指针管理回调对象
}

Status DBImpl::Write(const WriteOptions& write_options, WriteBatch* my_batch) {  // DBImpl::Write: 执行写入操作
  Status s;  // 声明状态变量
  if (write_options.protection_bytes_per_key > 0) {  // 如果写入配置设置了 protection_bytes_per_key，则更新 batch 的 protection info
    s = WriteBatchInternal::UpdateProtectionInfo(  // 更新 WriteBatch 的保护信息
        my_batch, write_options.protection_bytes_per_key);  // 传入 batch 和每个键的保护字节数
  }
  if (s.ok()) {  // 如果更新保护信息成功
    s = WriteImpl(write_options, my_batch, /*callback=*/nullptr,  // 调用 WriteImpl 执行实际写入，无回调
                  /*log_used=*/nullptr);  // 不需要返回使用的日志号
  }
  return s;  // 返回写入状态
}

Status DBImpl::WriteWithCallback(const WriteOptions& write_options,  // DBImpl::WriteWithCallback: 带回调的写入
                                 WriteBatch* my_batch,  // WriteBatch 对象
                                 WriteCallback* callback) {  // 写入回调
  Status s;  // 声明状态变量
  if (write_options.protection_bytes_per_key > 0) {  // 如果需要保护信息
    s = WriteBatchInternal::UpdateProtectionInfo(  // 更新保护信息
        my_batch, write_options.protection_bytes_per_key);  // 传入参数
  }
  if (s.ok()) {  // 如果更新成功
    s = WriteImpl(write_options, my_batch, callback, nullptr);  // 调用 WriteImpl 执行写入，带回调
  }
  return s;  // 返回写入状态
}

// The main write queue. This is the only write queue that updates LastSequence.
// When using one write queue, the same sequence also indicates the last
// published sequence.
// DBImpl::WriteImpl - RocksDB 的主要写入实现函数
//
// 功能概述：
// 这是 RocksDB 写入路径的核心入口，负责将数据写入 WAL（Write-Ahead Log）和 MemTable。
// 支持多种写入模式：
//   - 标准 WAL + MemTable 写入
//   - WAL-Only 写入（用于两阶段提交的 prepare 阶段）
//   - 无序写入（Unordered Write）
//   - 流水线写入（Pipelined Write）
//   - 并行 MemTable 写入（Concurrent MemTable Write）
//
// 写入流程（标准模式）：
// 1. 参数校验和前置检查
// 2. 低优先级写入限流
// 3. 加入写入队列，成为组 leader 或 follower
// 4. 预处理（PreprocessWrite）：准备 WAL writer、切换 WAL、调度 flush
// 5. 组建写入批次组
// 6. 写入 WAL
// 7. 调用 PreReleaseCallback
// 8. 写入 MemTable（串行或并行）
// 9. 调用 PostMemTableCallback
// 10. 更新序列号并退出
//
// 参数说明：
// - write_options: 写入选项（是否同步、是否禁用 WAL、优先级等）
// - my_batch: 写入批次（包含 Put/Delete/Merge 等操作）
// - callback: 写入前的回调函数，用于验证和准备
// - log_used: 输出参数，返回使用的 WAL 文件号
// - log_ref: WAL 日志引用，用于两阶段提交
// - disable_memtable: 是否禁用 MemTable 写入（仅写 WAL）
// - seq_used: 输出参数，返回分配的序列号
// - batch_cnt: 批次计数（用于 seq_per_batch 模式）
// - pre_release_callback: 写入 WAL 后、MemTable 前的回调
// - post_memtable_callback: 写入 MemTable 后的回调
Status DBImpl::WriteImpl(const WriteOptions& write_options,  // 写入选项
                         WriteBatch* my_batch, WriteCallback* callback,  // 写入批次和回调
                         uint64_t* log_used, uint64_t log_ref,  // 使用的日志号和日志引用
                         bool disable_memtable, uint64_t* seq_used,  // 是否禁用 memtable 和使用的序列号
                         size_t batch_cnt,  // 批次计数
                         PreReleaseCallback* pre_release_callback,  // PreRelease 回调
                         PostMemTableCallback* post_memtable_callback) {  // PostMemTable 回调
  // ============================================================================
  // 第一阶段：参数校验和前置检查
  // ============================================================================

  // 断言：如果启用了 seq_per_batch_ 模式，batch_cnt 必须非零
  // seq_per_batch_ 模式下，每个写批次消耗一个序列号，而不是每个键消耗一个序列号
  assert(!seq_per_batch_ || batch_cnt != 0);

  // 断言：验证 protection_bytes_per_key 参数的一致性
  // protection_bytes_per_key 用于每键保护（如校验和），必须是以下之一：
  // 1. batch 为空或计数为 0
  // 2. protection_bytes_per_key 为 0（不启用保护）
  // 3. protection_bytes_per_key 等于 batch 中设置的保护字节数（必须一致）
  assert(my_batch == nullptr || my_batch->Count() == 0 ||
         write_options.protection_bytes_per_key == 0 ||
         write_options.protection_bytes_per_key ==
             my_batch->GetProtectionBytesPerKey());

  // 检查 1：WriteBatch 指针有效性
  // WriteBatch 为空指针是无效参数，直接返回错误
  if (my_batch == nullptr) {
    return Status::InvalidArgument("Batch is nullptr!");
  }

  // 检查 2：时间戳设置验证
  // 如果需要写入 memtable（非 disable_memtable），则必须设置时间戳
  // 时间戳 RocksDB 的特性，用于支持用户定义的时间戳，实现 TTL、时间范围查询等功能
  // 注意：如果只写 WAL（disable_memtable=true），则时间戳可以不设置
  //      例如：WriteCommitted 策略的两阶段提交，在 prepare 阶段只写 WAL
  //      此时不需要设置时间戳，因为只有在 commit 时才会有包含提交时间戳的 commit marker
  else if (!disable_memtable &&
           WriteBatchInternal::TimestampsUpdateNeeded(*my_batch)) {
    // If writing to memtable, then we require the caller to set/update the
    // timestamps for the keys in the write batch.
    // Otherwise, it means we are just writing to the WAL, and we allow
    // timestamps unset for the keys in the write batch. This can happen if we
    // use TransactionDB with write-committed policy, and we currently do not
    // support user-defined timestamp with other policies.
    // In the prepare phase, a transaction can write the batch to the WAL
    // without inserting to memtable. The keys in the batch do not have to be
    // assigned timestamps because they will be used only during recovery if
    // there is a commit marker which includes their commit timestamp.
    return Status::InvalidArgument("write batch must have timestamp(s) set");
  }

  // 检查 3：rate_limiter_priority 参数验证
  // rate_limiter_priority 用于指定写入在速率限制器中的优先级
  // 当前实现只支持两个值：
  // - Env::IO_TOTAL: 默认优先级，所有 I/O 共享限制
  // - Env::IO_USER: 用户优先级，用于区分用户 I/O 和后台 I/O
  else if (write_options.rate_limiter_priority != Env::IO_TOTAL &&
           write_options.rate_limiter_priority != Env::IO_USER) {
    return Status::InvalidArgument(
        "WriteOptions::rate_limiter_priority only allows "
        "Env::IO_TOTAL and Env::IO_USER due to implementation constraints");
  }

  // 检查 4：rate_limiter_priority 与 WAL 模式的兼容性
  // 当前实现：rate_limiter_priority 只用于限制自动 WAL 刷新
  // 因此要求：
  // - 不能禁用 WAL（disableWAL == false）
  // - 不能手动刷新 WAL（manual_wal_flush_ == false）
  else if (write_options.rate_limiter_priority != Env::IO_TOTAL &&
           (write_options.disableWAL || manual_wal_flush_)) {
    return Status::InvalidArgument(
        "WriteOptions::rate_limiter_priority currently only supports "
        "rate-limiting automatic WAL flush, which requires "
        "`WriteOptions::disableWAL` and "
        "`DBOptions::manual_wal_flush` both set to false");
  }

  // 检查 5：protection_bytes_per_key 参数验证
  // protection_bytes_per_key 用于每键保护（如 CRC 校验）
  // 当前只支持两个值：
  // - 0: 不启用保护
  // - 8: 每键 8 字节保护（通常用于 CRC64）
  else if (write_options.protection_bytes_per_key != 0 &&
           write_options.protection_bytes_per_key != 8) {
    return Status::InvalidArgument(
        "`WriteOptions::protection_bytes_per_key` must be zero or eight");
  }

  // 检查 6：Trace 记录
  // 如果启用了 tracer 且 tracer 不需要保留写入顺序，则在此处记录 trace
  // 这样可以避免在后续阶段增加延迟
  // TODO: this use of operator bool on `tracer_` can avoid unnecessary lock
  // grabs but does not seem thread-safe.
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_ && !tracer_->IsWriteOrderPreserved()) {
      // We don't have to preserve write order so can trace anywhere. It's more
      // efficient to trace here than to add latency to a phase of the log/apply
      // pipeline.
      // TODO: maybe handle the tracing status?
      tracer_->Write(my_batch).PermitUncheckedError();
    }
  }

  // 检查 7：同步写入与 WAL 的兼容性
  // 同步写入（sync=true）意味着每次写入后调用 fsync/fdatasync
  // 如果禁用 WAL，则无法保证数据持久化，因此不允许
  if (write_options.sync && write_options.disableWAL) {
    return Status::InvalidArgument("Sync writes has to enable WAL.");
  }

  // 检查 8：双队列与流水线写入的兼容性
  // two_write_queues_: 用于两阶段提交，分为 WAL 队列和非 WAL 队列
  // enable_pipelined_write: 流水线写入，将写入分为多个阶段并行执行
  // 这两个特性不兼容，因为流水线写入假设单队列顺序
  if (two_write_queues_ && immutable_db_options_.enable_pipelined_write) {
    return Status::NotSupported(
        "pipelined_writes is not compatible with concurrent prepares");
  }

  // 检查 9：seq_per_batch 与流水线写入的兼容性
  // seq_per_batch_: 每个批次一个序列号（而非每个键一个）
  // 当前实现尚未支持两者同时使用
  // TODO(yiwu): update pipeline write with seq_per_batch and batch_cnt
  if (seq_per_batch_ && immutable_db_options_.enable_pipelined_write) {
    return Status::NotSupported(
        "pipelined_writes is not compatible with seq_per_batch");
  }

  // 检查 10：无序写入与流水线写入的兼容性
  // unordered_write: 写入顺序可能与提交顺序不同
  // 流水线写入依赖有序的写入流水线，两者不兼容
  if (immutable_db_options_.unordered_write &&
      immutable_db_options_.enable_pipelined_write) {
    return Status::NotSupported(
        "pipelined_writes is not compatible with unordered_write");
  }

  // 检查 11：流水线写入与 post_memtable_callback 的兼容性
  // post_memtable_callback: 写入 memtable 后的回调
  // 流水线写入的异步特性使得回调难以支持
  if (immutable_db_options_.enable_pipelined_write &&
      post_memtable_callback != nullptr) {
    return Status::NotSupported(
        "pipelined write currently does not honor post_memtable_callback");
  }

  // 检查 12：seq_per_batch 与 post_memtable_callback 的兼容性
  // seq_per_batch 当前不支持 post_memtable_callback
  if (seq_per_batch_ && post_memtable_callback != nullptr) {
    return Status::NotSupported(
        "seq_per_batch currently does not honor post_memtable_callback");
  }

  // 断言：IsLatestPersistentState 优化只适用于禁用 memtable 的情况
  // IsLatestPersistentState: 标记批次是最新的持久化状态，无需写入
  // 这个优化只在 WAL-Only 场景下有意义
  // Otherwise IsLatestPersistentState optimization does not make sense
  assert(!WriteBatchInternal::IsLatestPersistentState(my_batch) ||
         disable_memtable);

  // ============================================================================
  // 第二阶段：低优先级写入限流
  // ============================================================================

  // 如果设置了 low_pri 标志，表示这是低优先级写入
  // 在压缩落后时，对低优先级写入进行限流，以加快压缩速度
  // 这样可以优先处理压缩任务，避免压缩严重落后导致写入停止
  if (write_options.low_pri) {
    // 必要时限制低优先级写入
    // 如果返回非 OK，表示需要拒绝本次写入（如设置了 no_slowdown）
    Status s = ThrottleLowPriWritesIfNeeded(write_options, my_batch);
    if (!s.ok()) {
      return s;
    }
  }

  // ============================================================================
  // 第三阶段：根据写入模式选择不同的实现路径
  // ============================================================================

  // 路径 1：双队列 WAL-Only 模式
  // 适用场景：
  // - two_write_queues_: 启用了双队列（用于两阶段提交）
  // - disable_memtable: 只写 WAL，不写 memtable（两阶段提交的 prepare 阶段）
  //
  // 注意：WAL-Only 模式用于 WriteCommitted 策略的 prepare 阶段
  //      这些批次只写 WAL，不消耗序列号（commit 阶段才消耗）
  if (two_write_queues_ && disable_memtable) {
    // 确定是否分配序列号
    // seq_per_batch_ 模式下需要分配，否则不分配
    AssignOrder assign_order =
        seq_per_batch_ ? kDoAssignOrder : kDontAssignOrder;
    // Otherwise it is WAL-only Prepare batches in WriteCommitted policy and
    // they don't consume sequence.
    return WriteImplWALOnly(&nonmem_write_thread_, write_options, my_batch,
                            callback, log_used, log_ref, seq_used, batch_cnt,
                            pre_release_callback, assign_order,
                            kDontPublishLastSeq, disable_memtable);
  }

  // 路径 2：无序写入模式
  // 适用场景：
  // - unordered_write: 启用了无序写入
  //
  // 无序写入的流程：
  // 1. 先写入 WAL（有序，通过 WriteImplWALOnly）
  // 2. 再写入 MemTable（无序，通过 UnorderedWriteMemtable）
  //
  // 这样可以提高并发性能，因为写入 MemTable 时不需要严格按顺序
  if (immutable_db_options_.unordered_write) {
    // 计算子批次计数
    // 如果指定了 batch_cnt，使用它
    // 否则使用 batch 中的键数（每个键是一个子批次）
    const size_t sub_batch_cnt =
        batch_cnt != 0
            ? batch_cnt
            // every key is a sub-batch consuming a seq
            : WriteBatchInternal::Count(my_batch);
    uint64_t seq = 0;

    // Use a write thread to i) optimize for WAL write, ii) publish last
    // sequence in in increasing order, iii) call pre_release_callback serially
    //
    // 先写 WAL（有序）
    Status status = WriteImplWALOnly(
        &write_thread_, write_options, my_batch, callback, log_used, log_ref,
        &seq, sub_batch_cnt, pre_release_callback, kDoAssignOrder,
        kDoPublishLastSeq, disable_memtable);

    // 测试同步点：用于单元测试
    TEST_SYNC_POINT("DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL");

    if (!status.ok()) {
      return status;
    }

    // 保存序列号
    if (seq_used) {
      *seq_used = seq;
    }

    // 再写入 MemTable（无序）
    if (!disable_memtable) {
      TEST_SYNC_POINT("DBImpl::WriteImpl:BeforeUnorderedWriteMemtable");
      status = UnorderedWriteMemtable(write_options, my_batch, callback,
                                      log_ref, seq, sub_batch_cnt);
    }

    return status;
  }

  // 路径 3：流水线写入模式
  // 适用场景：
  // - enable_pipelined_write: 启用了流水线写入
  //
  // 流水线写入的特点：
  // - 将写入分为多个阶段并行执行（WAL 写入、MemTable 写入等）
  // - 提高吞吐量，但可能增加延迟
  if (immutable_db_options_.enable_pipelined_write) {
    return PipelinedWriteImpl(write_options, my_batch, callback, log_used,
                              log_ref, disable_memtable, seq_used);
  }

  // ============================================================================
  // 第四阶段：标准写入模式（串行或并行 MemTable 写入）
  // ============================================================================

  // 如果没有走上述特殊路径，则使用标准写入模式
  // 标准模式支持：
  // - 串行 MemTable 写入（默认）
  // - 并行 MemTable 写入（allow_concurrent_memtable_write）

  // 创建性能计时器：记录写入前后的处理时间
  // 包括：队列等待、预处理、后处理等非核心写入时间
  PERF_TIMER_GUARD(write_pre_and_post_process_time);

  // 创建 Writer 对象，表示当前写入请求
  // Writer 是 WriteThread 中的基本单位，包含：
  // - 写入选项（write_options）
  // - 写入批次（my_batch）
  // - 回调函数（callback）
  // - 其他状态信息（序列号、状态等）
  WriteThread::Writer w(write_options, my_batch, callback, log_ref,
                        disable_memtable, batch_cnt, pre_release_callback,
                        post_memtable_callback);

  // 创建写入计时器：记录整个写入操作的耗时
  StopWatch write_sw(immutable_db_options_.clock, stats_, DB_WRITE);

  // ============================================================================
  // 第五阶段：加入写入队列
  // ============================================================================

  // 调用 JoinBatchGroup 将当前 writer 加入写入队列
  // 有两种可能的结果：
  // 1. 成为 GROUP_LEADER：负责执行实际的 WAL 和 MemTable 写入
  // 2. 成为 PARALLEL_MEMTABLE_WRITER：并行写入模式下，成为 follower
  // 3. 被其他 leader 组批：等待完成后状态变为 STATE_COMPLETED
  write_thread_.JoinBatchGroup(&w);

  // ============================================================================
  // 情况 1：并行模式下的 Follower Writer
  // ============================================================================

  // 如果状态是 STATE_PARALLEL_MEMTABLE_WRITER，说明当前线程是并行组中的非 leader
  // 这发生在 allow_concurrent_memtable_write=true 且写批次包含多个 writer 时
  // Leader 负责：
  //   - 写入 WAL
  //   - 分配序列号
  //   - 分发任务给 follower 并行写入 MemTable
  // Follower 负责：
  //   - 并行写入自己的批次到 MemTable
  //   - 最后一个完成的 follower 负责更新序列号和退出
  if (w.state == WriteThread::STATE_PARALLEL_MEMTABLE_WRITER) {
    // we are a non-leader in a parallel group

    // 如果需要写入 MemTable（非 disable_memtable）
    if (w.ShouldWriteToMemtable()) {
      // 停止前后处理计时器（开始记录 MemTable 写入时间）
      PERF_TIMER_STOP(write_pre_and_post_process_time);

      // 启动 MemTable 写入计时器
      PERF_TIMER_GUARD(write_memtable_time);

      // 创建列族 MemTable 实现
      // 用于访问所有列族的 MemTable
      ColumnFamilyMemTablesImpl column_family_memtables(
          versions_->GetColumnFamilySet());

      // 并行写入 MemTable
      // InsertInto 会将 WriteBatch 中的所有操作写入到对应的 MemTable
      w.status = WriteBatchInternal::InsertInto(
          &w, w.sequence, &column_family_memtables, &flush_scheduler_,
          &trim_history_scheduler_,
          write_options.ignore_missing_column_families, 0 /*log_number*/, this,
          true /*concurrent_memtable_writes*/, seq_per_batch_, w.batch_cnt,
          batch_per_txn_, write_options.memtable_insert_hint_per_batch);

      // 恢复前后处理计时器
      PERF_TIMER_START(write_pre_and_post_process_time);
    }

    // 检查是否是最后一个完成的并行 writer
    // CompleteParallelMemTableWriter 返回 true 表示当前线程负责退出批处理组
    if (write_thread_.CompleteParallelMemTableWriter(&w)) {
      // we're responsible for exit batch group
      // TODO(myabandeh): propagate status to write_group

      // 获取最后序列号
      auto last_sequence = w.write_group->last_sequence;

      // 调用所有 writer 的 post_memtable_callback
      for (auto* tmp_w : *(w.write_group)) {
        assert(tmp_w);
        if (tmp_w->post_memtable_callback) {
          // 调用回调，传入最后序列号和是否禁用 memtable
          Status tmp_s =
              (*tmp_w->post_memtable_callback)(last_sequence, disable_memtable);
          // TODO: propagate the execution status of post_memtable_callback to
          // caller.
          assert(tmp_s.ok());
        }
      }

      // 更新数据库的最后序列号
      versions_->SetLastSequence(last_sequence);

      // 检查 MemTable 写入状态
      MemTableInsertStatusCheck(w.status);

      // 退出批处理组，通知其他等待的 writer
      write_thread_.ExitAsBatchGroupFollower(&w);
    }

    // 断言：状态已完成
    assert(w.state == WriteThread::STATE_COMPLETED);

    // STATE_COMPLETED conditional below handles exit
  }

  // ============================================================================
  // 情况 2：写入已完成（被其他 leader 组批）
  // ============================================================================

  // 如果状态是 STATE_COMPLETED，说明当前 writer 已经被其他 leader 完成了写入
  // 这通常发生在：
  // - 多个小写入被合并到一个批次中
  // - Leader 已经完成了 WAL 写入、MemTable 写入和序列号更新
  // 当前线程只需要返回结果即可
  // STATE_COMPLETED: 写入已完成，由 leader 处理完成
  if (w.state == WriteThread::STATE_COMPLETED) {
    // 返回使用的 WAL 文件号
    if (log_used != nullptr) {
      *log_used = w.log_used;
    }

    // 返回分配的序列号
    if (seq_used != nullptr) {
      *seq_used = w.sequence;
    }

    // 写入已完成，leader 已更新序列号
    // 直接返回最终状态
    return w.FinalStatus();
  }

  // ============================================================================
  // 情况 3：成为 GROUP_LEADER
  // ============================================================================

  // 断言：当前状态必须是 GROUP_LEADER
  // 到达此点，说明当前线程成为了批处理组的 leader
  // Leader 的职责：
  // 1. 执行 PreprocessWrite（准备 WAL writer、切换 WAL、调度 flush）
  // 2. 组建写入批处理组（包括后续等待的 writer）
  // 3. 写入 WAL
  // 4. 分配序列号
  // 5. 写入 MemTable（串行或并行）
  // 6. 更新最后序列号
  // 7. 调用回调函数
  // 8. 退出批处理组，唤醒等待的 writer
  // STATE_GROUP_LEADER: 当前 writer 是写入批处理组的 leader
  assert(w.state == WriteThread::STATE_GROUP_LEADER);

  Status status;

  // 创建写入上下文
  // WriteContext 包含 flush 上下文和版本上下文等信息
  WriteContext write_context;

  // 创建日志上下文
  // LogContext 包含 WAL writer、是否需要同步等信息
  LogContext log_context(write_options.sync);

  // 创建写入批处理组
  // WriteGroup 是一组一起写入的 writer 集合
  WriteThread::WriteGroup write_group;

  // 标记是否在并行组中
  // 如果是并行组，leader 也要写入自己的 MemTable
  bool in_parallel_group = false;

  // 最后序列号，初始为最大值（表示未分配）
  uint64_t last_sequence = kMaxSequenceNumber;

  assert(!two_write_queues_ || !disable_memtable);
  {
    // 使用并发写入时，只在负责写入 memtable 的写入线程中进行预处理
    // 以避免与其他线程共享数据结构上的同步问题

    // PreprocessWrite 有自己的性能计时。
    PERF_TIMER_STOP(write_pre_and_post_process_time);

    // 预处理：准备 WAL writer、处理 RecoverableState、刷新 memtable
    status = PreprocessWrite(write_options, &log_context, &write_context);
    if (!two_write_queues_) {
      // 在 ::PreprocessWrite 之后分配，因为序列号可能会在内部
      // 通过 WriteRecoverableState 推进
      last_sequence = versions_->LastSequence();
    }

    PERF_TIMER_START(write_pre_and_post_process_time);
  }

  // 添加到日志并应用到 memtable。
  // 我们可以在这一阶段释放锁，因为 &w 当前负责日志记录
  // 并防止并发日志记录器和并发写入到 memtables

  TEST_SYNC_POINT("DBImpl::WriteImpl:BeforeLeaderEnters");
  // 组建写入批处理组
  last_batch_group_size_ =
      write_thread_.EnterAsBatchGroupLeader(&w, &write_group);

  IOStatus io_s;
  Status pre_release_cb_status;
  if (status.ok()) {
    // TODO: this use of operator bool on `tracer_` can avoid unnecessary lock
    // grabs but does not seem thread-safe.
    if (tracer_) {
      InstrumentedMutexLock lock(&trace_mutex_);
      if (tracer_ && tracer_->IsWriteOrderPreserved()) {
        for (auto* writer : write_group) {
          // TODO: maybe handle the tracing status?
          tracer_->Write(writer->batch).PermitUncheckedError();
        }
      }
    }
    // Rules for when we can update the memtable concurrently
    // 1. supported by memtable
    // 2. Puts are not okay if inplace_update_support
    // 3. Merges are not okay
    //
    // Rules 1..2 are enforced by checking the options
    // during startup (CheckConcurrentWritesSupported), so if
    // options.allow_concurrent_memtable_write is true then they can be
    // assumed to be true.  Rule 3 is checked for each batch.  We could
    // relax rules 2 if we could prevent write batches from referring
    // more than once to a particular key.
    bool parallel = immutable_db_options_.allow_concurrent_memtable_write &&
                    write_group.size > 1;
    size_t total_count = 0;
    size_t valid_batches = 0;
    size_t total_byte_size = 0;
    size_t pre_release_callback_cnt = 0;
    for (auto* writer : write_group) {
      assert(writer);
      if (writer->CheckCallback(this)) {
        valid_batches += writer->batch_cnt;
        if (writer->ShouldWriteToMemtable()) {
          total_count += WriteBatchInternal::Count(writer->batch);
          parallel = parallel && !writer->batch->HasMerge();
        }
        total_byte_size = WriteBatchInternal::AppendedByteSize(
            total_byte_size, WriteBatchInternal::ByteSize(writer->batch));
        if (writer->pre_release_callback) {
          pre_release_callback_cnt++;
        }
      }
    }
    // Note about seq_per_batch_: either disableWAL is set for the entire write
    // group or not. In either case we inc seq for each write batch with no
    // failed callback. This means that there could be a batch with
    // disalbe_memtable in between; although we do not write this batch to
    // memtable it still consumes a seq. Otherwise, if !seq_per_batch_, we inc
    // the seq per valid written key to mem.
    size_t seq_inc = seq_per_batch_ ? valid_batches : total_count;

    const bool concurrent_update = two_write_queues_;
  // Update stats while we are an exclusive group leader, so we know
  // that nobody else can be writing to these particular stats.
  // We're optimistic, updating the stats before we successfully
  // commit.  That lets us release our leader status early.
  auto stats = default_cf_internal_stats_;  // 获取默认列族的内部统计对象
  stats->AddDBStats(InternalStats::kIntStatsNumKeysWritten, total_count,  // 统计写入的键数量
                    concurrent_update);  // 支持并发更新
  RecordTick(stats_, NUMBER_KEYS_WRITTEN, total_count);  // 记录写入键的数量
  stats->AddDBStats(InternalStats::kIntStatsBytesWritten, total_byte_size,  // 统计写入的字节数
                    concurrent_update);  // 支持并发更新
  RecordTick(stats_, BYTES_WRITTEN, total_byte_size);  // 记录写入字节数
  stats->AddDBStats(InternalStats::kIntStatsWriteDoneBySelf, 1,  // 统计自己完成的写入
                    concurrent_update);  // 支持并发更新
  RecordTick(stats_, WRITE_DONE_BY_SELF);  // 记录自己完成的写入
  auto write_done_by_other = write_group.size - 1;  // 计算其他写入者完成的写入数
  if (write_done_by_other > 0) {  // 如果有其他写入者
    stats->AddDBStats(InternalStats::kIntStatsWriteDoneByOther,  // 统计其他写入者完成的写入
                      write_done_by_other, concurrent_update);  // 支持并发更新
    RecordTick(stats_, WRITE_DONE_BY_OTHER, write_done_by_other);  // 记录其他写入者完成的写入
  }
  RecordInHistogram(stats_, BYTES_PER_WRITE, total_byte_size);  // 记录每次写入的字节分布

  if (write_options.disableWAL) {  // 如果禁用了 WAL
    has_unpersisted_data_.store(true, std::memory_order_relaxed);  // 标记存在未持久化的数据
  }

  PERF_TIMER_STOP(write_pre_and_post_process_time);  // 停止前后处理计时器

  if (!two_write_queues_) {  // 如果不使用双写入队列
    if (status.ok() && !write_options.disableWAL) {  // 状态正常且启用了 WAL
      assert(log_context.log_file_number_size);  // 断言日志文件号大小存在
      LogFileNumberSize& log_file_number_size =  // 获取日志文件号大小引用
          *(log_context.log_file_number_size);  // 解引用
      PERF_TIMER_GUARD(write_wal_time);  // 启动 WAL 写入计时器
      io_s =  // 调用 WAL 写入
          WriteToWAL(write_group, log_context.writer, log_used,  // 写入到 WAL
                     log_context.need_log_sync, log_context.need_log_dir_sync,  // 是否需要同步
                     last_sequence + 1, log_file_number_size);  // 序列号和文件大小
    }
  } else {  // 使用双写入队列
    if (status.ok() && !write_options.disableWAL) {  // 状态正常且启用了 WAL
      PERF_TIMER_GUARD(write_wal_time);  // 启动 WAL 写入计时器
      // LastAllocatedSequence is increased inside WriteToWAL under
      // wal_write_mutex_ to ensure ordered events in WAL
      io_s = ConcurrentWriteToWAL(write_group, log_used, &last_sequence,  // 并发写入到 WAL
                                  seq_inc);  // 序列号增量
    } else {  // 否则只增加序列号
      // Otherwise we inc seq number for memtable writes
      last_sequence = versions_->FetchAddLastAllocatedSequence(seq_inc);  // 获取并增加序列号
    }
  }
    status = io_s;  // 将 IO 状态赋值给状态变量
    assert(last_sequence != kMaxSequenceNumber);  // 断言序列号未达到最大值
    const SequenceNumber current_sequence = last_sequence + 1;  // 当前序列号
    last_sequence += seq_inc;  // 增加序列号

    // PreReleaseCallback is called after WAL write and before memtable write
    if (status.ok()) {  // 如果状态正常
      SequenceNumber next_sequence = current_sequence;  // 下一个序列号
      size_t index = 0;  // 索引
      // Note: the logic for advancing seq here must be consistent with the
      // logic in WriteBatchInternal::InsertInto(write_group...) as well as
      // with WriteBatchInternal::InsertInto(write_batch...) that is called on
      // the merged batch during recovery from the WAL.
      for (auto* writer : write_group) {  // 遍历写入组中的所有写入者
        if (writer->CallbackFailed()) {  // 如果回调失败
          continue;  // 跳过
        }
        writer->sequence = next_sequence;  // 分配序列号
        if (writer->pre_release_callback) {  // 如果有 PreRelease 回调
          Status ws = writer->pre_release_callback->Callback(  // 调用回调
              writer->sequence, disable_memtable, writer->log_used, index++,  // 传入参数
              pre_release_callback_cnt);  // 回调计数
          if (!ws.ok()) {  // 如果回调失败
            status = pre_release_cb_status = ws;  // 保存错误状态
            break;  // 跳出循环
          }
        }
        if (seq_per_batch_) {  // 如果每批次一个序列号
          assert(writer->batch_cnt);  // 断言批次计数非零
          next_sequence += writer->batch_cnt;  // 增加序列号
        } else if (writer->ShouldWriteToMemtable()) {  // 如果需要写入 memtable
          next_sequence += WriteBatchInternal::Count(writer->batch);  // 增加键的数量
        }
      }
    }

    if (status.ok()) {  // 如果状态正常
      PERF_TIMER_GUARD(write_memtable_time);  // 启动 memtable 写入计时器

      if (!parallel) {  // 如果不是并行模式
        // w.sequence will be set inside InsertInto
        w.status = WriteBatchInternal::InsertInto(  // 插入到 memtable
            write_group, current_sequence, column_family_memtables_.get(),  // 写入组和列族 memtable
            &flush_scheduler_, &trim_history_scheduler_,  // flush 和修剪调度器
            write_options.ignore_missing_column_families,  // 忽略缺失的列族
            0 /*recovery_log_number*/, this, parallel, seq_per_batch_,  // 恢复日志号等参数
            batch_per_txn_);  // 每事务批次数
      } else {  // 并行模式
        write_group.last_sequence = last_sequence;  // 设置最后一个序列号
        write_thread_.LaunchParallelMemTableWriters(&write_group);  // 启动并行 memtable 写入者
        in_parallel_group = true;

        // Each parallel follower is doing each own writes. The leader should
        // also do its own.
        if (w.ShouldWriteToMemtable()) {  // 如果需要写入 memtable
          ColumnFamilyMemTablesImpl column_family_memtables(  // 创建列族 memtable 实现
              versions_->GetColumnFamilySet());  // 获取列族集合
          assert(w.sequence == current_sequence);  // 断言序列号匹配
          w.status = WriteBatchInternal::InsertInto(  // 插入到 memtable
              &w, w.sequence, &column_family_memtables, &flush_scheduler_,  // 写入器和参数
              &trim_history_scheduler_,
              write_options.ignore_missing_column_families, 0 /*log_number*/,
              this, true /*concurrent_memtable_writes*/, seq_per_batch_,
              w.batch_cnt, batch_per_txn_,
              write_options.memtable_insert_hint_per_batch);
        }
      }
      if (seq_used != nullptr) {
        *seq_used = w.sequence;
      }
    }
  }
  PERF_TIMER_START(write_pre_and_post_process_time);

  if (!io_s.ok()) {
    // Check WriteToWAL status
    IOStatusCheck(io_s);
  }
  if (!w.CallbackFailed()) {
    if (!io_s.ok()) {
      assert(pre_release_cb_status.ok());
    } else {
      WriteStatusCheck(pre_release_cb_status);
    }
  } else {
    assert(pre_release_cb_status.ok());
  }

  // ============================================================================
  // 第十三阶段：WAL 同步（如果需要）
  // ============================================================================

  // 如果需要同步 WAL（write_options.sync = true）
  if (log_context.need_log_sync) {
    VersionEdit synced_wals;  // 已同步的 WAL 列表（用于更新 MANIFEST）

    // 加锁 WAL 写入互斥量
    // 保护日志文件状态和 MANIFEST 更新
    log_write_mutex_.Lock();

    if (status.ok()) {
      // 标记 WAL 已同步
      // 参数：
      // - logfile_number_: 当前 WAL 文件号
      // - log_context.need_log_dir_sync: 是否需要同步 WAL 目录
      // - &synced_wals: 输出参数，记录已同步的 WAL
      MarkLogsSynced(logfile_number_, log_context.need_log_dir_sync,
                     &synced_wals);
    } else {
      // 标记 WAL 未同步
      // 用于错误恢复时跳过这些 WAL 文件
      MarkLogsNotSynced(logfile_number_);
    }

    // 解锁 WAL 写入互斥量
    log_write_mutex_.Unlock();

    // 如果成功且有 WAL 需要添加到 MANIFEST
    // synced_wals.IsWalAddition() 返回 true 表示有新的 WAL 需要记录
    if (status.ok() && synced_wals.IsWalAddition()) {
      // 加锁数据库互斥量
      InstrumentedMutexLock l(&mutex_);

      // 将 WAL 信息应用到 MANIFEST
      // TODO: plumb Env::IOActivity
      const ReadOptions read_options;
      status = ApplyWALToManifest(read_options, &synced_wals);
    }

    // 双队列模式下的 WAL 同步处理
    // Requesting sync with two_write_queues_ is expected to be very rare. We
    // hence provide a simple implementation that is not necessarily efficient.
    // 双队列模式下的同步请求很少见，因此使用简单的实现（可能不够高效）
    if (two_write_queues_) {
      if (manual_wal_flush_) {
        // 手动 WAL 刷新模式
        status = FlushWAL(true);
      } else {
        // 自动 WAL 同步模式
        status = SyncWAL();
      }
    }
  }

  // ============================================================================
  // 第十四阶段：退出批处理组
  // ============================================================================

  // 决定是否由当前线程退出批处理组
  bool should_exit_batch_group = true;

  if (in_parallel_group) {
    // 并行模式：检查是否是最后一个完成的 writer
    // CompleteParallelMemTableWriter 返回 true 表示当前线程负责退出
    // CompleteParallelWorker returns true if this thread should
    // handle exit, false means somebody else did
    should_exit_batch_group = write_thread_.CompleteParallelMemTableWriter(&w);
  }

  // 如果当前线程负责退出批处理组
  if (should_exit_batch_group) {
    if (status.ok()) {
      // ----------------------------------------------------------------------
      // 子阶段 14.1：调用 PostMemTableCallback
      // ----------------------------------------------------------------------
      // PostMemTableCallback 在 MemTable 写入完成后调用
      // 用途：
      // - 通知外部系统数据已写入 MemTable（可见）
      // - 清理临时资源
      // - 触发后续操作
      for (auto* tmp_w : write_group) {
        assert(tmp_w);

        if (tmp_w->post_memtable_callback) {
          // 调用回调，传入最后序列号和是否禁用 memtable
          Status tmp_s =
              (*tmp_w->post_memtable_callback)(last_sequence, disable_memtable);

          // TODO: propagate the execution status of post_memtable_callback to
          // caller.
          // TODO: 将 post_memtable_callback 的执行状态传播给调用者
          assert(tmp_s.ok());
        }
      }

      // ----------------------------------------------------------------------
      // 子阶段 14.2：更新最后序列号
      // ----------------------------------------------------------------------
      // Note: if we are to resume after non-OK statuses we need to revisit how
      // we reacts to non-OK statuses here.
      // 注意：如果要在非 OK 状态后恢复，需要重新考虑这里对非 OK 状态的处理
      versions_->SetLastSequence(last_sequence);
    }

    // 检查 MemTable 写入状态
    MemTableInsertStatusCheck(w.status);

    // ----------------------------------------------------------------------
    // 子阶段 14.3：退出批处理组
    // ----------------------------------------------------------------------
    // 退出批处理组，唤醒等待的 writer
    // 这会：
    // 1. 释放 leader 状态
    // 2. 将状态标记为 completed
    // 3. 唤醒所有等待的 follower
    // 4. 通知下一个 leader 开始处理
    write_thread_.ExitAsBatchGroupLeader(write_group, status);
  }

  // ============================================================================
  // 第十五阶段：返回最终状态
  // ============================================================================

  // 如果状态正常，返回 writer 的最终状态
  // FinalStatus 可能是：
  // - OK: 写入成功
  // - 错误: 写入失败（WAL 错误、MemTable 错误、回调错误等）
  if (status.ok()) {
    status = w.FinalStatus();
  }

  // 返回最终状态
  return status;
}

Status DBImpl::PipelinedWriteImpl(const WriteOptions& write_options,
                                  WriteBatch* my_batch, WriteCallback* callback,
                                  uint64_t* log_used, uint64_t log_ref,
                                  bool disable_memtable, uint64_t* seq_used) {
  PERF_TIMER_GUARD(write_pre_and_post_process_time);
  StopWatch write_sw(immutable_db_options_.clock, stats_, DB_WRITE);

  WriteContext write_context;

  WriteThread::Writer w(write_options, my_batch, callback, log_ref,
                        disable_memtable, /*_batch_cnt=*/0,
                        /*_pre_release_callback=*/nullptr);
  write_thread_.JoinBatchGroup(&w);
  TEST_SYNC_POINT("DBImplWrite::PipelinedWriteImpl:AfterJoinBatchGroup");
  if (w.state == WriteThread::STATE_GROUP_LEADER) {
    WriteThread::WriteGroup wal_write_group;
    if (w.callback && !w.callback->AllowWriteBatching()) {
      write_thread_.WaitForMemTableWriters();
    }
    LogContext log_context(!write_options.disableWAL && write_options.sync);
    // PreprocessWrite does its own perf timing.
    PERF_TIMER_STOP(write_pre_and_post_process_time);
    w.status = PreprocessWrite(write_options, &log_context, &write_context);
    PERF_TIMER_START(write_pre_and_post_process_time);

    // This can set non-OK status if callback fail.
    last_batch_group_size_ =
        write_thread_.EnterAsBatchGroupLeader(&w, &wal_write_group);
    const SequenceNumber current_sequence =
        write_thread_.UpdateLastSequence(versions_->LastSequence()) + 1;
    size_t total_count = 0;
    size_t total_byte_size = 0;

    if (w.status.ok()) {
      // TODO: this use of operator bool on `tracer_` can avoid unnecessary lock
      // grabs but does not seem thread-safe.
      if (tracer_) {
        InstrumentedMutexLock lock(&trace_mutex_);
        if (tracer_ != nullptr && tracer_->IsWriteOrderPreserved()) {
          for (auto* writer : wal_write_group) {
            // TODO: maybe handle the tracing status?
            tracer_->Write(writer->batch).PermitUncheckedError();
          }
        }
      }
      SequenceNumber next_sequence = current_sequence;
      for (auto* writer : wal_write_group) {
        assert(writer);
        if (writer->CheckCallback(this)) {
          if (writer->ShouldWriteToMemtable()) {
            writer->sequence = next_sequence;
            size_t count = WriteBatchInternal::Count(writer->batch);
            next_sequence += count;
            total_count += count;
          }
          total_byte_size = WriteBatchInternal::AppendedByteSize(
              total_byte_size, WriteBatchInternal::ByteSize(writer->batch));
        }
      }
      if (w.disable_wal) {
        has_unpersisted_data_.store(true, std::memory_order_relaxed);
      }
      write_thread_.UpdateLastSequence(current_sequence + total_count - 1);
    }

    auto stats = default_cf_internal_stats_;
    stats->AddDBStats(InternalStats::kIntStatsNumKeysWritten, total_count);
    RecordTick(stats_, NUMBER_KEYS_WRITTEN, total_count);
    stats->AddDBStats(InternalStats::kIntStatsBytesWritten, total_byte_size);
    RecordTick(stats_, BYTES_WRITTEN, total_byte_size);
    RecordInHistogram(stats_, BYTES_PER_WRITE, total_byte_size);

    PERF_TIMER_STOP(write_pre_and_post_process_time);

    IOStatus io_s;
    io_s.PermitUncheckedError();  // Allow io_s to be uninitialized

    if (w.status.ok() && !write_options.disableWAL) {
      PERF_TIMER_GUARD(write_wal_time);
      stats->AddDBStats(InternalStats::kIntStatsWriteDoneBySelf, 1);
      RecordTick(stats_, WRITE_DONE_BY_SELF, 1);
      if (wal_write_group.size > 1) {
        stats->AddDBStats(InternalStats::kIntStatsWriteDoneByOther,
                          wal_write_group.size - 1);
        RecordTick(stats_, WRITE_DONE_BY_OTHER, wal_write_group.size - 1);
      }
      assert(log_context.log_file_number_size);
      LogFileNumberSize& log_file_number_size =
          *(log_context.log_file_number_size);
      io_s =
          WriteToWAL(wal_write_group, log_context.writer, log_used,
                     log_context.need_log_sync, log_context.need_log_dir_sync,
                     current_sequence, log_file_number_size);
      w.status = io_s;
    }

    if (!io_s.ok()) {
      // Check WriteToWAL status
      IOStatusCheck(io_s);
    } else if (!w.CallbackFailed()) {
      WriteStatusCheck(w.status);
    }

    VersionEdit synced_wals;
    if (log_context.need_log_sync) {
      InstrumentedMutexLock l(&log_write_mutex_);
      if (w.status.ok()) {
        MarkLogsSynced(logfile_number_, log_context.need_log_dir_sync,
                       &synced_wals);
      } else {
        MarkLogsNotSynced(logfile_number_);
      }
    }
    if (w.status.ok() && synced_wals.IsWalAddition()) {
      InstrumentedMutexLock l(&mutex_);
      // TODO: plumb Env::IOActivity
      const ReadOptions read_options;
      w.status = ApplyWALToManifest(read_options, &synced_wals);
    }
    write_thread_.ExitAsBatchGroupLeader(wal_write_group, w.status);
  }

  // NOTE: the memtable_write_group is declared before the following
  // `if` statement because its lifetime needs to be longer
  // that the inner context  of the `if` as a reference to it
  // may be used further below within the outer _write_thread
  WriteThread::WriteGroup memtable_write_group;

  if (w.state == WriteThread::STATE_MEMTABLE_WRITER_LEADER) {
    PERF_TIMER_GUARD(write_memtable_time);
    assert(w.ShouldWriteToMemtable());
    write_thread_.EnterAsMemTableWriter(&w, &memtable_write_group);
    if (memtable_write_group.size > 1 &&
        immutable_db_options_.allow_concurrent_memtable_write) {
      write_thread_.LaunchParallelMemTableWriters(&memtable_write_group);
    } else {
      memtable_write_group.status = WriteBatchInternal::InsertInto(
          memtable_write_group, w.sequence, column_family_memtables_.get(),
          &flush_scheduler_, &trim_history_scheduler_,
          write_options.ignore_missing_column_families, 0 /*log_number*/, this,
          false /*concurrent_memtable_writes*/, seq_per_batch_, batch_per_txn_);
      versions_->SetLastSequence(memtable_write_group.last_sequence);
      write_thread_.ExitAsMemTableWriter(&w, memtable_write_group);
    }
  } else {
    // NOTE: the memtable_write_group is never really used,
    // so we need to set its status to pass ASSERT_STATUS_CHECKED
    memtable_write_group.status.PermitUncheckedError();
  }

  if (w.state == WriteThread::STATE_PARALLEL_MEMTABLE_WRITER) {
    assert(w.ShouldWriteToMemtable());
    ColumnFamilyMemTablesImpl column_family_memtables(
        versions_->GetColumnFamilySet());
    w.status = WriteBatchInternal::InsertInto(
        &w, w.sequence, &column_family_memtables, &flush_scheduler_,
        &trim_history_scheduler_, write_options.ignore_missing_column_families,
        0 /*log_number*/, this, true /*concurrent_memtable_writes*/,
        false /*seq_per_batch*/, 0 /*batch_cnt*/, true /*batch_per_txn*/,
        write_options.memtable_insert_hint_per_batch);
    if (write_thread_.CompleteParallelMemTableWriter(&w)) {
      MemTableInsertStatusCheck(w.status);
      versions_->SetLastSequence(w.write_group->last_sequence);
      write_thread_.ExitAsMemTableWriter(&w, *w.write_group);
    }
  }
  if (seq_used != nullptr) {
    *seq_used = w.sequence;
  }

  assert(w.state == WriteThread::STATE_COMPLETED);
  return w.FinalStatus();
}

Status DBImpl::UnorderedWriteMemtable(const WriteOptions& write_options,
                                      WriteBatch* my_batch,
                                      WriteCallback* callback, uint64_t log_ref,
                                      SequenceNumber seq,
                                      const size_t sub_batch_cnt) {
  PERF_TIMER_GUARD(write_pre_and_post_process_time);
  StopWatch write_sw(immutable_db_options_.clock, stats_, DB_WRITE);

  WriteThread::Writer w(write_options, my_batch, callback, log_ref,
                        false /*disable_memtable*/);

  if (w.CheckCallback(this) && w.ShouldWriteToMemtable()) {
    w.sequence = seq;
    size_t total_count = WriteBatchInternal::Count(my_batch);
    InternalStats* stats = default_cf_internal_stats_;
    stats->AddDBStats(InternalStats::kIntStatsNumKeysWritten, total_count);
    RecordTick(stats_, NUMBER_KEYS_WRITTEN, total_count);

    ColumnFamilyMemTablesImpl column_family_memtables(
        versions_->GetColumnFamilySet());
    w.status = WriteBatchInternal::InsertInto(
        &w, w.sequence, &column_family_memtables, &flush_scheduler_,
        &trim_history_scheduler_, write_options.ignore_missing_column_families,
        0 /*log_number*/, this, true /*concurrent_memtable_writes*/,
        seq_per_batch_, sub_batch_cnt, true /*batch_per_txn*/,
        write_options.memtable_insert_hint_per_batch);
    if (write_options.disableWAL) {
      has_unpersisted_data_.store(true, std::memory_order_relaxed);
    }
  }

  size_t pending_cnt = pending_memtable_writes_.fetch_sub(1) - 1;
  if (pending_cnt == 0) {
    // switch_cv_ waits until pending_memtable_writes_ = 0. Locking its mutex
    // before notify ensures that cv is in waiting state when it is notified
    // thus not missing the update to pending_memtable_writes_ even though it is
    // not modified under the mutex.
    std::lock_guard<std::mutex> lck(switch_mutex_);
    switch_cv_.notify_all();
  }
  WriteStatusCheck(w.status);

  if (!w.FinalStatus().ok()) {
    return w.FinalStatus();
  }
  return Status::OK();
}

// The 2nd write queue. If enabled it will be used only for WAL-only writes.
// This is the only queue that updates LastPublishedSequence which is only
// applicable in a two-queue setting.
Status DBImpl::WriteImplWALOnly(
    WriteThread* write_thread, const WriteOptions& write_options,
    WriteBatch* my_batch, WriteCallback* callback, uint64_t* log_used,
    const uint64_t log_ref, uint64_t* seq_used, const size_t sub_batch_cnt,
    PreReleaseCallback* pre_release_callback, const AssignOrder assign_order,
    const PublishLastSeq publish_last_seq, const bool disable_memtable) {
  PERF_TIMER_GUARD(write_pre_and_post_process_time);
  WriteThread::Writer w(write_options, my_batch, callback, log_ref,
                        disable_memtable, sub_batch_cnt, pre_release_callback);
  StopWatch write_sw(immutable_db_options_.clock, stats_, DB_WRITE);

  write_thread->JoinBatchGroup(&w);
  assert(w.state != WriteThread::STATE_PARALLEL_MEMTABLE_WRITER);
  if (w.state == WriteThread::STATE_COMPLETED) {
    if (log_used != nullptr) {
      *log_used = w.log_used;
    }
    if (seq_used != nullptr) {
      *seq_used = w.sequence;
    }
    return w.FinalStatus();
  }
  // else we are the leader of the write batch group
  assert(w.state == WriteThread::STATE_GROUP_LEADER);

  if (publish_last_seq == kDoPublishLastSeq) {
    Status status;

    // Currently we only use kDoPublishLastSeq in unordered_write
    assert(immutable_db_options_.unordered_write);
    WriteContext write_context;
    if (error_handler_.IsDBStopped()) {
      status = error_handler_.GetBGError();
    }
    // TODO(myabandeh): Make preliminary checks thread-safe so we could do them
    // without paying the cost of obtaining the mutex.
    if (status.ok()) {
      LogContext log_context;
      status = PreprocessWrite(write_options, &log_context, &write_context);
      WriteStatusCheckOnLocked(status);
    }
    if (!status.ok()) {
      WriteThread::WriteGroup write_group;
      write_thread->EnterAsBatchGroupLeader(&w, &write_group);
      write_thread->ExitAsBatchGroupLeader(write_group, status);
      return status;
    }
  } else {
    InstrumentedMutexLock lock(&mutex_);
    Status status =
        DelayWrite(/*num_bytes=*/0ull, *write_thread, write_options);
    if (!status.ok()) {
      WriteThread::WriteGroup write_group;
      write_thread->EnterAsBatchGroupLeader(&w, &write_group);
      write_thread->ExitAsBatchGroupLeader(write_group, status);
      return status;
    }
  }

  WriteThread::WriteGroup write_group;
  uint64_t last_sequence;
  write_thread->EnterAsBatchGroupLeader(&w, &write_group);
  // Note: no need to update last_batch_group_size_ here since the batch writes
  // to WAL only
  // TODO: this use of operator bool on `tracer_` can avoid unnecessary lock
  // grabs but does not seem thread-safe.
  if (tracer_) {
    InstrumentedMutexLock lock(&trace_mutex_);
    if (tracer_ != nullptr && tracer_->IsWriteOrderPreserved()) {
      for (auto* writer : write_group) {
        // TODO: maybe handle the tracing status?
        tracer_->Write(writer->batch).PermitUncheckedError();
      }
    }
  }

  size_t pre_release_callback_cnt = 0;
  size_t total_byte_size = 0;
  for (auto* writer : write_group) {
    assert(writer);
    if (writer->CheckCallback(this)) {
      total_byte_size = WriteBatchInternal::AppendedByteSize(
          total_byte_size, WriteBatchInternal::ByteSize(writer->batch));
      if (writer->pre_release_callback) {
        pre_release_callback_cnt++;
      }
    }
  }

  const bool concurrent_update = true;
  // Update stats while we are an exclusive group leader, so we know
  // that nobody else can be writing to these particular stats.
  // We're optimistic, updating the stats before we successfully
  // commit.  That lets us release our leader status early.
  auto stats = default_cf_internal_stats_;
  stats->AddDBStats(InternalStats::kIntStatsBytesWritten, total_byte_size,
                    concurrent_update);
  RecordTick(stats_, BYTES_WRITTEN, total_byte_size);
  stats->AddDBStats(InternalStats::kIntStatsWriteDoneBySelf, 1,
                    concurrent_update);
  RecordTick(stats_, WRITE_DONE_BY_SELF);
  auto write_done_by_other = write_group.size - 1;
  if (write_done_by_other > 0) {
    stats->AddDBStats(InternalStats::kIntStatsWriteDoneByOther,
                      write_done_by_other, concurrent_update);
    RecordTick(stats_, WRITE_DONE_BY_OTHER, write_done_by_other);
  }
  RecordInHistogram(stats_, BYTES_PER_WRITE, total_byte_size);

  PERF_TIMER_STOP(write_pre_and_post_process_time);

  PERF_TIMER_GUARD(write_wal_time);
  // LastAllocatedSequence is increased inside WriteToWAL under
  // wal_write_mutex_ to ensure ordered events in WAL
  size_t seq_inc = 0 /* total_count */;
  if (assign_order == kDoAssignOrder) {
    size_t total_batch_cnt = 0;
    for (auto* writer : write_group) {
      assert(writer->batch_cnt || !seq_per_batch_);
      if (!writer->CallbackFailed()) {
        total_batch_cnt += writer->batch_cnt;
      }
    }
    seq_inc = total_batch_cnt;
  }
  Status status;
  if (!write_options.disableWAL) {
    IOStatus io_s =
        ConcurrentWriteToWAL(write_group, log_used, &last_sequence, seq_inc);
    status = io_s;
    // last_sequence may not be set if there is an error
    // This error checking and return is moved up to avoid using uninitialized
    // last_sequence.
    if (!io_s.ok()) {
      IOStatusCheck(io_s);
      write_thread->ExitAsBatchGroupLeader(write_group, status);
      return status;
    }
  } else {
    // Otherwise we inc seq number to do solely the seq allocation
    last_sequence = versions_->FetchAddLastAllocatedSequence(seq_inc);
  }

  size_t memtable_write_cnt = 0;
  auto curr_seq = last_sequence + 1;
  for (auto* writer : write_group) {
    if (writer->CallbackFailed()) {
      continue;
    }
    writer->sequence = curr_seq;
    if (assign_order == kDoAssignOrder) {
      assert(writer->batch_cnt || !seq_per_batch_);
      curr_seq += writer->batch_cnt;
    }
    if (!writer->disable_memtable) {
      memtable_write_cnt++;
    }
    // else seq advances only by memtable writes
  }
  if (status.ok() && write_options.sync) {
    assert(!write_options.disableWAL);
    // Requesting sync with two_write_queues_ is expected to be very rare. We
    // hance provide a simple implementation that is not necessarily efficient.
    if (manual_wal_flush_) {
      status = FlushWAL(true);
    } else {
      status = SyncWAL();
    }
  }
  PERF_TIMER_START(write_pre_and_post_process_time);

  if (!w.CallbackFailed()) {
    WriteStatusCheck(status);
  }
  if (status.ok()) {
    size_t index = 0;
    for (auto* writer : write_group) {
      if (!writer->CallbackFailed() && writer->pre_release_callback) {
        assert(writer->sequence != kMaxSequenceNumber);
        Status ws = writer->pre_release_callback->Callback(
            writer->sequence, disable_memtable, writer->log_used, index++,
            pre_release_callback_cnt);
        if (!ws.ok()) {
          status = ws;
          break;
        }
      }
    }
  }
  if (publish_last_seq == kDoPublishLastSeq) {
    versions_->SetLastSequence(last_sequence + seq_inc);
    // Currently we only use kDoPublishLastSeq in unordered_write
    assert(immutable_db_options_.unordered_write);
  }
  if (immutable_db_options_.unordered_write && status.ok()) {
    pending_memtable_writes_ += memtable_write_cnt;
  }
  write_thread->ExitAsBatchGroupLeader(write_group, status);
  if (status.ok()) {
    status = w.FinalStatus();
  }
  if (seq_used != nullptr) {
    *seq_used = w.sequence;
  }
  return status;
}

// WriteStatusCheckOnLocked: 检查写入状态（在已持有锁的情况下）
// 如果状态非 OK 且不是 Busy/Incomplete，则设置后台错误，停止压缩和后续写入
void DBImpl::WriteStatusCheckOnLocked(const Status& status) {
  // 设置 bg_error_ 就足够了吗？这至少会停止压缩并失败任何后续写入。
  InstrumentedMutexLock l(&mutex_);
  assert(!status.IsIOFenced() || !error_handler_.GetBGError().ok());
  if (immutable_db_options_.paranoid_checks && !status.ok() &&
      !status.IsBusy() && !status.IsIncomplete()) {
    // 可能将返回状态改为 void？
    error_handler_.SetBGError(status, BackgroundErrorReason::kWriteCallback);
  }
}

// WriteStatusCheck: 检查写入状态（自动获取锁）
// 如果状态非 OK 且不是 Busy/Incomplete，则设置后台错误，停止压缩和后续写入
void DBImpl::WriteStatusCheck(const Status& status) {
  // 设置 bg_error_ 就足够了吗？这至少会停止压缩并失败任何后续写入。
  assert(!status.IsIOFenced() || !error_handler_.GetBGError().ok());
  if (immutable_db_options_.paranoid_checks && !status.ok() &&
      !status.IsBusy() && !status.IsIncomplete()) {
    mutex_.Lock();
    // 可能将返回状态改为 void？
    error_handler_.SetBGError(status, BackgroundErrorReason::kWriteCallback);
    mutex_.Unlock();
  }
}

// IOStatusCheck: 检查 IO 状态
// 如果状态非 OK 且不是 Busy/Incomplete，或者是 IOFenced 错误，则设置后台错误
void DBImpl::IOStatusCheck(const IOStatus& io_status) {
  // 设置 bg_error_ 就足够了吗？这至少会停止压缩并失败任何后续写入。
  if ((immutable_db_options_.paranoid_checks && !io_status.ok() &&
       !io_status.IsBusy() && !io_status.IsIncomplete()) ||
      io_status.IsIOFenced()) {
    mutex_.Lock();
    // 可能将返回状态改为 void？
    error_handler_.SetBGError(io_status, BackgroundErrorReason::kWriteCallback);
    mutex_.Unlock();
  } else {
    // 强制可写文件继续可写
    logs_.back().writer->file()->reset_seen_error();
  }
}

// MemTableInsertStatusCheck: 检查 memtable 插入状态
// 非 OK 状态表示 WAL 暗示的状态与内存状态已分离
// 这可能是由于 write_batch 损坏（非常糟糕），或者客户端指定了无效的列族
// 且没有指定 ignore_missing_column_families
void DBImpl::MemTableInsertStatusCheck(const Status& status) {
  // 非 OK 状态表示 WAL 暗示的状态与内存中的状态已分离。这可能是：
  // - 由于 write_batch 损坏（非常糟糕）
  // - 或者客户端指定了无效的列族且未指定 ignore_missing_column_families
  if (!status.ok()) {
    mutex_.Lock();
    assert(!error_handler_.IsBGWorkStopped());
    // 可能将返回状态改为 void？
    error_handler_.SetBGError(status, BackgroundErrorReason::kMemTable)
        .PermitUncheckedError();
    mutex_.Unlock();
  }
}

// PreprocessWrite: 写入预处理函数
// 在实际写入前执行：检查数据库状态、处理 WAL 切换、处理缓冲区刷新、调度 flush 等
Status DBImpl::PreprocessWrite(const WriteOptions& write_options,
                               LogContext* log_context,
                               WriteContext* write_context) {
  assert(write_context != nullptr && log_context != nullptr);
  Status status;

  // === 步骤 1：检查数据库状态 ===
  // 检查数据库是否因后台错误而停止，如果停止则返回错误
  // 当发生严重错误（如磁盘损坏、IO错误等）时，数据库会进入停止状态
  if (error_handler_.IsDBStopped()) {
    InstrumentedMutexLock l(&mutex_);
    status = error_handler_.GetBGError();
  }

  PERF_TIMER_GUARD(write_scheduling_flushes_compactions_time);

  // === 步骤 2：处理 WAL 切换 ===
  // 当 WAL（Write-Ahead Log）总大小超过最大限制时，需要切换到新的 WAL 文件
  // UNLIKELY：分支预测优化，表示此条件不太常见
  // WAL 是预写日志，用于在崩溃恢复时重放未持久化的写入
  if (UNLIKELY(status.ok() && total_log_size_ > GetMaxTotalWalSize())) {
    assert(versions_);
    InstrumentedMutexLock l(&mutex_);
    const ColumnFamilySet* const column_families =
        versions_->GetColumnFamilySet();
    assert(column_families);
    size_t num_cfs = column_families->NumberOfColumnFamilies();
    assert(num_cfs >= 1);
    // 只有多列族时才需要主动切换 WAL
    // 单列族情况下，WAL 会在 flush 时自动处理，不需要额外切换
    if (num_cfs > 1) {
      // 等待所有待处理的写入完成，确保切换 WAL 时没有写入正在进行
      // 这样可以保证 WAL 切换的原子性和数据一致性
      WaitForPendingWrites();
      status = SwitchWAL(write_context);
    }
  }

  // 写缓冲区需要刷新时，触发 flush
  if (UNLIKELY(status.ok() && write_buffer_manager_->ShouldFlush())) {
    // 在 SwitchMemtable() 中添加新 memtable 之前，
    // write_buffer_manager_->ShouldFlush() 会持续返回 true。
    // 如果另一个线程正在使用相同的写入缓冲区写入另一个 DB，它们也可能被刷新。
    // 我们最终可能会刷新比需要更多的 DB。这是次优的，但仍然是正确的。
    InstrumentedMutexLock l(&mutex_);
    WaitForPendingWrites();
    status = HandleWriteBufferManagerFlush(write_context);
  }

  if (UNLIKELY(status.ok() && !trim_history_scheduler_.Empty())) {
    // === 步骤 4：处理 Memtable 历史修剪 ===
    // 如果有需要修剪的 memtable 历史记录，则执行修剪
    // 这通常是为了释放不再需要的旧版本数据
    InstrumentedMutexLock l(&mutex_);
    status = TrimMemtableHistory(write_context);
  }

  if (UNLIKELY(status.ok() && !flush_scheduler_.Empty())) {
    // === 步骤 5：调度 Flush 操作 ===
    // 如果有待 flush 的列族，调度 flush 操作
    // flush 会将 memtable 中的数据持久化到磁盘 SST 文件
    InstrumentedMutexLock l(&mutex_);
    WaitForPendingWrites();
    status = ScheduleFlushes(write_context);
  }

  PERF_TIMER_STOP(write_scheduling_flushes_compactions_time);
  PERF_TIMER_GUARD(write_pre_and_post_process_time);

  if (UNLIKELY(status.ok() && (write_controller_.IsStopped() ||
                               write_controller_.NeedsDelay()))) {
    // === 步骤 6：处理写入限流 ===
    // 如果写入控制器要求停止或延迟写入，则执行相应的限流操作
    // 这是由 RecalculateWriteStallConditions 函数计算得出的
    // 限流机制包括：停止写入（kStopped）和延迟写入（kDelayed）
    PERF_TIMER_STOP(write_pre_and_post_process_time);
    PERF_TIMER_GUARD(write_delay_time);
    // We don't know size of curent batch so that we always use the size
    // for previous one. It might create a fairness issue that expiration
    // might happen for smaller writes but larger writes can go through.
    // Can optimize it if it is an issue.
    // 由于不知道当前批次的准确大小，我们使用上一个批次的大小
    // 这可能导致公平性问题：较小的写入可能超时，而较大的写入可能通过
    // 如果这是一个问题，可以进行优化
    InstrumentedMutexLock l(&mutex_);
    status = DelayWrite(last_batch_group_size_, write_thread_, write_options);
    PERF_TIMER_START(write_pre_and_post_process_time);
  }

  // === 步骤 7：处理写缓冲区限流 ===
  // If memory usage exceeded beyond a certain threshold,
  // write_buffer_manager_->ShouldStall() returns true to all threads writing to
  // all DBs and writers will be stalled.
  // It does soft checking because WriteBufferManager::buffer_limit_ has already
  // exceeded at this point so no new write (including current one) will go
  // through until memory usage is decreased.
  // 如果内存使用超过某个阈值，write_buffer_manager_->ShouldStall() 会返回 true
  // 所有向所有 DB 写入的线程都会被阻塞
  // 这是软检查，因为 WriteBufferManager::buffer_limit_ 已经超过，
  // 所以在内存使用减少之前，没有新的写入（包括当前写入）能够通过
  if (UNLIKELY(status.ok() && write_buffer_manager_->ShouldStall())) {
    default_cf_internal_stats_->AddDBStats(
        InternalStats::kIntStatsWriteBufferManagerLimitStopsCounts, 1,
        true /* concurrent */);
    // 如果用户设置了 no_slowdown 选项，则不阻塞，直接返回 Incomplete 状态
    // 这样用户可以快速失败并重试，而不是长时间阻塞
    if (write_options.no_slowdown) {
      status = Status::Incomplete("Write stall");
    } else {
      InstrumentedMutexLock l(&mutex_);
      // 阻塞等待内存使用下降到安全水平
      // 这会等待 flush 操作完成，释放内存
      WriteBufferManagerStallWrites();
    }
  }
  // === 步骤 8：准备 WAL 同步上下文 ===
  // 获取日志写入互斥锁，确保 WAL 操作的线程安全
  InstrumentedMutexLock l(&log_write_mutex_);
  if (status.ok() && log_context->need_log_sync) {
    // 等待所有并行的 WAL 同步操作完成
    // Wait until the parallel syncs are finished. Any sync process has to sync
    // the front log too so it is enough to check the status of front()
    // We do a while loop since log_sync_cv_ is signalled when any sync is
    // finished
    // 任何同步进程都需要同步最前面的日志，所以只需要检查 front() 的状态
    // 使用 while 循环是因为 log_sync_cv_ 在任何同步完成时都会被通知
    // Note: there does not seem to be a reason to wait for parallel sync at
    // this early step but it is not important since parallel sync (SyncWAL) and
    // need_log_sync are usually not used together.
    // 注意：在这个早期步骤等待并行同步似乎没有意义，但由于并行同步（SyncWAL）
    // 和 need_log_sync 通常不会一起使用，所以这不是问题
    while (logs_.front().IsSyncing()) {
      log_sync_cv_.Wait();
    }
    for (auto& log : logs_) {
      // This is just to prevent the logs to be synced by a parallel SyncWAL
      // call. We will do the actual syncing later after we will write to the
      // WAL.
      // Note: there does not seem to be a reason to set this early before we
      // actually write to the WAL
      // 这只是为了防止日志被并行的 SyncWAL 调用同步
      // 我们将在写入 WAL 之后进行实际的同步
      // 注意：在实际写入 WAL 之前设置这个标志似乎没有特别的原因
      log.PrepareForSync();
    }
  } else {
    log_context->need_log_sync = false;
  }
  // 设置日志写入器为当前活跃的日志写入器
  log_context->writer = logs_.back().writer;
  // 确定是否需要同步日志目录
  log_context->need_log_dir_sync =
      log_context->need_log_dir_sync && !log_dir_synced_;
  // 设置日志文件编号大小的地址
  log_context->log_file_number_size = std::addressof(alive_log_files_.back());

  return status;
}

Status DBImpl::MergeBatch(const WriteThread::WriteGroup& write_group,
                          WriteBatch* tmp_batch, WriteBatch** merged_batch,
                          size_t* write_with_wal,
                          WriteBatch** to_be_cached_state) {
  assert(write_with_wal != nullptr);
  assert(tmp_batch != nullptr);
  assert(*to_be_cached_state == nullptr);
  *write_with_wal = 0;
  auto* leader = write_group.leader;
  assert(!leader->disable_wal);  // Same holds for all in the batch group
  if (write_group.size == 1 && !leader->CallbackFailed() &&
      leader->batch->GetWalTerminationPoint().is_cleared()) {
    // we simply write the first WriteBatch to WAL if the group only
    // contains one batch, that batch should be written to the WAL,
    // and the batch is not wanting to be truncated
    *merged_batch = leader->batch;
    if (WriteBatchInternal::IsLatestPersistentState(*merged_batch)) {
      *to_be_cached_state = *merged_batch;
    }
    *write_with_wal = 1;
  } else {
    // WAL needs all of the batches flattened into a single batch.
    // We could avoid copying here with an iov-like AddRecord
    // interface
    *merged_batch = tmp_batch;
    for (auto writer : write_group) {
      if (!writer->CallbackFailed()) {
        Status s = WriteBatchInternal::Append(*merged_batch, writer->batch,
                                              /*WAL_only*/ true);
        if (!s.ok()) {
          tmp_batch->Clear();
          return s;
        }
        if (WriteBatchInternal::IsLatestPersistentState(writer->batch)) {
          // We only need to cache the last of such write batch
          *to_be_cached_state = writer->batch;
        }
        (*write_with_wal)++;
      }
    }
  }
  // return merged_batch;
  return Status::OK();
}

// When two_write_queues_ is disabled, this function is called from the only
// write thread. Otherwise this must be called holding log_write_mutex_.
IOStatus DBImpl::WriteToWAL(const WriteBatch& merged_batch,
                            log::Writer* log_writer, uint64_t* log_used,
                            uint64_t* log_size,
                            Env::IOPriority rate_limiter_priority,
                            LogFileNumberSize& log_file_number_size) {
  assert(log_size != nullptr);

  Slice log_entry = WriteBatchInternal::Contents(&merged_batch);
  TEST_SYNC_POINT_CALLBACK("DBImpl::WriteToWAL:log_entry", &log_entry);
  auto s = merged_batch.VerifyChecksum();
  if (!s.ok()) {
    return status_to_io_status(std::move(s));
  }
  *log_size = log_entry.size();
  // When two_write_queues_ WriteToWAL has to be protected from concurretn calls
  // from the two queues anyway and log_write_mutex_ is already held. Otherwise
  // if manual_wal_flush_ is enabled we need to protect log_writer->AddRecord
  // from possible concurrent calls via the FlushWAL by the application.
  const bool needs_locking = manual_wal_flush_ && !two_write_queues_;
  // Due to performance cocerns of missed branch prediction penalize the new
  // manual_wal_flush_ feature (by UNLIKELY) instead of the more common case
  // when we do not need any locking.
  if (UNLIKELY(needs_locking)) {
    log_write_mutex_.Lock();
  }
  IOStatus io_s = log_writer->MaybeAddUserDefinedTimestampSizeRecord(
      versions_->GetColumnFamiliesTimestampSizeForRecord(),
      rate_limiter_priority);
  if (!io_s.ok()) {
    return io_s;
  }
  io_s = log_writer->AddRecord(log_entry, rate_limiter_priority);

  if (UNLIKELY(needs_locking)) {
    log_write_mutex_.Unlock();
  }
  if (log_used != nullptr) {
    *log_used = logfile_number_;
  }
  total_log_size_ += log_entry.size();
  log_file_number_size.AddSize(*log_size);
  log_empty_ = false;
  return io_s;
}

IOStatus DBImpl::WriteToWAL(const WriteThread::WriteGroup& write_group,
                            log::Writer* log_writer, uint64_t* log_used,
                            bool need_log_sync, bool need_log_dir_sync,
                            SequenceNumber sequence,
                            LogFileNumberSize& log_file_number_size) {
  IOStatus io_s;
  assert(!two_write_queues_);
  assert(!write_group.leader->disable_wal);
  // Same holds for all in the batch group
  size_t write_with_wal = 0;
  WriteBatch* to_be_cached_state = nullptr;
  WriteBatch* merged_batch;
  io_s = status_to_io_status(MergeBatch(write_group, &tmp_batch_, &merged_batch,
                                        &write_with_wal, &to_be_cached_state));
  if (UNLIKELY(!io_s.ok())) {
    return io_s;
  }

  if (merged_batch == write_group.leader->batch) {
    write_group.leader->log_used = logfile_number_;
  } else if (write_with_wal > 1) {
    for (auto writer : write_group) {
      writer->log_used = logfile_number_;
    }
  }

  WriteBatchInternal::SetSequence(merged_batch, sequence);

  uint64_t log_size;
  io_s = WriteToWAL(*merged_batch, log_writer, log_used, &log_size,
                    write_group.leader->rate_limiter_priority,
                    log_file_number_size);
  if (to_be_cached_state) {
    cached_recoverable_state_ = *to_be_cached_state;
    cached_recoverable_state_empty_ = false;
  }

  if (io_s.ok() && need_log_sync) {
    StopWatch sw(immutable_db_options_.clock, stats_, WAL_FILE_SYNC_MICROS);
    // It's safe to access logs_ with unlocked mutex_ here because:
    //  - we've set getting_synced=true for all logs,
    //    so other threads won't pop from logs_ while we're here,
    //  - only writer thread can push to logs_, and we're in
    //    writer thread, so no one will push to logs_,
    //  - as long as other threads don't modify it, it's safe to read
    //    from std::deque from multiple threads concurrently.
    //
    // Sync operation should work with locked log_write_mutex_, because:
    //   when DBOptions.manual_wal_flush_ is set,
    //   FlushWAL function will be invoked by another thread.
    //   if without locked log_write_mutex_, the log file may get data
    //   corruption

    const bool needs_locking = manual_wal_flush_ && !two_write_queues_;
    if (UNLIKELY(needs_locking)) {
      log_write_mutex_.Lock();
    }

    for (auto& log : logs_) {
      io_s = log.writer->file()->Sync(immutable_db_options_.use_fsync);
      if (!io_s.ok()) {
        break;
      }
    }

    if (UNLIKELY(needs_locking)) {
      log_write_mutex_.Unlock();
    }

    if (io_s.ok() && need_log_dir_sync) {
      // We only sync WAL directory the first time WAL syncing is
      // requested, so that in case users never turn on WAL sync,
      // we can avoid the disk I/O in the write code path.
      io_s = directories_.GetWalDir()->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
  }

  if (merged_batch == &tmp_batch_) {
    tmp_batch_.Clear();
  }
  if (io_s.ok()) {
    auto stats = default_cf_internal_stats_;
    if (need_log_sync) {
      stats->AddDBStats(InternalStats::kIntStatsWalFileSynced, 1);
      RecordTick(stats_, WAL_FILE_SYNCED);
    }
    stats->AddDBStats(InternalStats::kIntStatsWalFileBytes, log_size);
    RecordTick(stats_, WAL_FILE_BYTES, log_size);
    stats->AddDBStats(InternalStats::kIntStatsWriteWithWal, write_with_wal);
    RecordTick(stats_, WRITE_WITH_WAL, write_with_wal);
  }
  return io_s;
}

IOStatus DBImpl::ConcurrentWriteToWAL(
    const WriteThread::WriteGroup& write_group, uint64_t* log_used,
    SequenceNumber* last_sequence, size_t seq_inc) {
  IOStatus io_s;

  assert(two_write_queues_ || immutable_db_options_.unordered_write);
  assert(!write_group.leader->disable_wal);
  // Same holds for all in the batch group
  WriteBatch tmp_batch;
  size_t write_with_wal = 0;
  WriteBatch* to_be_cached_state = nullptr;
  WriteBatch* merged_batch;
  io_s = status_to_io_status(MergeBatch(write_group, &tmp_batch, &merged_batch,
                                        &write_with_wal, &to_be_cached_state));
  if (UNLIKELY(!io_s.ok())) {
    return io_s;
  }

  // We need to lock log_write_mutex_ since logs_ and alive_log_files might be
  // pushed back concurrently
  log_write_mutex_.Lock();
  if (merged_batch == write_group.leader->batch) {
    write_group.leader->log_used = logfile_number_;
  } else if (write_with_wal > 1) {
    for (auto writer : write_group) {
      writer->log_used = logfile_number_;
    }
  }
  *last_sequence = versions_->FetchAddLastAllocatedSequence(seq_inc);
  auto sequence = *last_sequence + 1;
  WriteBatchInternal::SetSequence(merged_batch, sequence);

  log::Writer* log_writer = logs_.back().writer;
  LogFileNumberSize& log_file_number_size = alive_log_files_.back();

  assert(log_writer->get_log_number() == log_file_number_size.number);

  uint64_t log_size;
  io_s = WriteToWAL(*merged_batch, log_writer, log_used, &log_size,
                    write_group.leader->rate_limiter_priority,
                    log_file_number_size);
  if (to_be_cached_state) {
    cached_recoverable_state_ = *to_be_cached_state;
    cached_recoverable_state_empty_ = false;
  }
  log_write_mutex_.Unlock();

  if (io_s.ok()) {
    const bool concurrent = true;
    auto stats = default_cf_internal_stats_;
    stats->AddDBStats(InternalStats::kIntStatsWalFileBytes, log_size,
                      concurrent);
    RecordTick(stats_, WAL_FILE_BYTES, log_size);
    stats->AddDBStats(InternalStats::kIntStatsWriteWithWal, write_with_wal,
                      concurrent);
    RecordTick(stats_, WRITE_WITH_WAL, write_with_wal);
  }
  return io_s;
}

/**
 * @brief 将缓存的可恢复状态写入 memtable，确保持久化
 *
 * 该函数用于将延迟的可恢复状态（如 WAL 回收点、事务状态等）写入 memtable，
 * 确保这些状态在 memtable flush 后能够持久化到 SST 文件中。
 * 由于 WAL 可能在 memtable 切换后被删除，可恢复状态必须提前写入 memtable。
 *
 * @return Status 操作状态，OK 表示写入成功
 *
 * @note 调用此函数时必须持有 mutex_（db_mutex）
 *
 * 设计背景：
 * - **缓存可恢复状态**：某些操作（如 WAL 回收）需要记录状态以便恢复
 * - **延迟写入策略**：为了性能，状态首先缓存在 cached_recoverable_state_ 中
 * - **持久化时机**：在 memtable 切换前将缓存状态写入 memtable
 * - **双队列支持**：支持 two_write_queues_ 模式的序列号管理
 *
 * 关键数据结构：
 * - cached_recoverable_state_: 延迟写入的可恢复状态（WriteBatch 格式）
 * - cached_recoverable_state_empty_: 标记缓存是否为空
 * - recoverable_state_pre_release_callback_: 状态写入前的回调（如 WAL 回收点记录）
 *
 * 函数执行流程：
 * 1. 检查是否有待写入的缓存状态
 * 2. 获取当前序列号（考虑双队列模式）
 * 3. 将序列号写入可恢复状态
 * 4. 将状态插入到 memtable（写入 WAL，但标记为恢复状态）
 * 5. 更新全局序列号
 * 6. 调用预释放回调（如果注册）
 * 7. 清空缓存并标记为空
 *
 * 调用时机：
 * - DBImpl::SwitchMemtable：memtable 切换前
 * - 确保 WAL 回收点等信息能够持久化
 *
 * 性能考虑：
 * - 使用原子操作避免锁竞争（two_write_queues_ 模式）
 * - 批量插入减少系统调用
 * - 锁释放期间允许其他操作继续
 */
Status DBImpl::WriteRecoverableState() {
  mutex_.AssertHeld();

  // 步骤 1: 检查是否有待写入的缓存状态
  // cached_recoverable_state_empty_ 为 true 表示没有待持久化的可恢复状态
  if (!cached_recoverable_state_empty_) {
    bool dont_care_bool;
    SequenceNumber next_seq;

    // 步骤 2: 获取当前序列号（考虑双队列模式）
    //
    // 双队列模式（two_write_queues_）说明：
    // - RocksDB 支持两个写入队列以优化并发性能
    // - 第一个队列：普通写入操作（需要访问 memtable）
    // - 第二个队列：后台任务（flush、compaction，不需要访问 memtable）
    // - FetchAddLastAllocatedSequence: 原子操作获取并递增已分配序列号
    if (two_write_queues_) {
      log_write_mutex_.Lock();
    }

    SequenceNumber seq;
    if (two_write_queues_) {
      seq = versions_->FetchAddLastAllocatedSequence(0);
    } else {
      seq = versions_->LastSequence();
    }

    // 步骤 3: 将序列号写入可恢复状态
    // seq + 1 表示新状态的序列号（当前最大序列号 + 1）
    WriteBatchInternal::SetSequence(&cached_recoverable_state_, seq + 1);

    // 步骤 4: 将状态插入到 memtable
    //
    // 参数说明：
    // - column_family_memtables_.get(): 列族 memtable 映射
    // - flush_scheduler_: flush 调度器引用
    // - trim_history_scheduler_: memtable 历史清理调度器引用
    // - 第三个参数 true: 标记为恢复状态（与普通写入区分）
    // - 0 /*recovery_log_number*/: 不需要关联到特定 WAL（这是内部状态）
    // - this: DBImpl 实例引用
    // - false /*concurrent_memtable_writes*/: 当前线程在写入队列头部，无并发
    auto status = WriteBatchInternal::InsertInto(
        &cached_recoverable_state_, column_family_memtables_.get(),
        &flush_scheduler_, &trim_history_scheduler_, true,
        0 /*recovery_log_number*/, this, false /* concurrent_memtable_writes */,
        &next_seq, &dont_care_bool, seq_per_batch_);

    // 计算实际写入的最后一个序列号
    auto last_seq = next_seq - 1;

    // 步骤 5: 更新全局序列号
    if (two_write_queues_) {
      // 双队列模式需要分别更新已分配和已发布序列号
      // last_seq - seq: 本次操作使用的序列号数量
      versions_->FetchAddLastAllocatedSequence(last_seq - seq);
      versions_->SetLastPublishedSequence(last_seq);
    }
    versions_->SetLastSequence(last_seq);

    if (two_write_queues_) {
      log_write_mutex_.Unlock();
    }

    // 步骤 6: 调用预释放回调（如果注册）
    //
    // PreReleaseCallback 用途：
    // - 在写入 WAL 之后、写入 memtable 之前调用
    // - 用于在写入对读取器可见之前执行某些操作
    // - 用于减少锁开销（在写入线程上顺序更新某些内容）
    //
    // 典型使用场景：
    // - WAL 回收点记录（AddCommitted -> AdvanceMaxEvictedSeq）
    // - 事务状态更新
    // - 快照相关操作
    if (status.ok() && recoverable_state_pre_release_callback_) {
      const bool DISABLE_MEMTABLE = true;  // 表示不写入 memtable（已写入）

      // 为每个子批量调用回调
      // 子批量：WriteBatch 可能包含多个操作，每个操作一个序列号
      for (uint64_t sub_batch_seq = seq + 1;
           sub_batch_seq < next_seq && status.ok(); sub_batch_seq++) {
        uint64_t const no_log_num = 0;  // 不关联到特定 WAL

        // 释放互斥锁，因为回调可能会重新获取锁
        // 示例调用链：AddCommitted -> AdvanceMaxEvictedSeq -> GetSnapshotListFromDB
        // 避免在回调期间持有主锁，减少锁竞争
        mutex_.Unlock();
        status = recoverable_state_pre_release_callback_->Callback(
            sub_batch_seq, !DISABLE_MEMTABLE, no_log_num, 0, 1);
        mutex_.Lock();
      }
    }

    // 步骤 7: 清空缓存并标记为空
    if (status.ok()) {
      cached_recoverable_state_.Clear();
      cached_recoverable_state_empty_ = true;
    }

    return status;
  }

  // 没有待写入的可恢复状态，直接返回成功
  return Status::OK();
}

void DBImpl::SelectColumnFamiliesForAtomicFlush(
    autovector<ColumnFamilyData*>* selected_cfds,
    const autovector<ColumnFamilyData*>& provided_candidate_cfds) {
  mutex_.AssertHeld();
  assert(selected_cfds);

  autovector<ColumnFamilyData*> candidate_cfds;

  // Generate candidate cfds if not provided
  if (provided_candidate_cfds.empty()) {
    for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
      if (!cfd->IsDropped() && cfd->initialized()) {
        cfd->Ref();
        candidate_cfds.push_back(cfd);
      }
    }
  } else {
    candidate_cfds = provided_candidate_cfds;
  }

  for (ColumnFamilyData* cfd : candidate_cfds) {
    if (cfd->IsDropped()) {
      continue;
    }
    if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
        !cached_recoverable_state_empty_.load()) {
      selected_cfds->push_back(cfd);
    }
  }

  // Unref the newly generated candidate cfds (when not provided) in
  // `candidate_cfds`
  if (provided_candidate_cfds.empty()) {
    for (auto candidate_cfd : candidate_cfds) {
      candidate_cfd->UnrefAndTryDelete();
    }
  }
}

// Assign sequence number for atomic flush.
void DBImpl::AssignAtomicFlushSeq(const autovector<ColumnFamilyData*>& cfds) {
  assert(immutable_db_options_.atomic_flush);
  auto seq = versions_->LastSequence();
  for (auto cfd : cfds) {
    cfd->imm()->AssignAtomicFlushSeq(seq);
  }
}

Status DBImpl::SwitchWAL(WriteContext* write_context) {
  mutex_.AssertHeld();
  assert(write_context != nullptr);
  Status status;

  if (alive_log_files_.begin()->getting_flushed) {
    return status;
  }

  auto oldest_alive_log = alive_log_files_.begin()->number;
  bool flush_wont_release_oldest_log = false;
  if (allow_2pc()) {
    auto oldest_log_with_uncommitted_prep =
        logs_with_prep_tracker_.FindMinLogContainingOutstandingPrep();

    assert(oldest_log_with_uncommitted_prep == 0 ||
           oldest_log_with_uncommitted_prep >= oldest_alive_log);
    if (oldest_log_with_uncommitted_prep > 0 &&
        oldest_log_with_uncommitted_prep == oldest_alive_log) {
      if (unable_to_release_oldest_log_) {
        // we already attempted to flush all column families dependent on
        // the oldest alive log but the log still contained uncommitted
        // transactions so there is still nothing that we can do.
        return status;
      } else {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Unable to release oldest log due to uncommitted transaction");
        unable_to_release_oldest_log_ = true;
        flush_wont_release_oldest_log = true;
      }
    }
  }
  if (!flush_wont_release_oldest_log) {
    // we only mark this log as getting flushed if we have successfully
    // flushed all data in this log. If this log contains outstanding prepared
    // transactions then we cannot flush this log until those transactions are
    // commited.
    unable_to_release_oldest_log_ = false;
    alive_log_files_.begin()->getting_flushed = true;
  }

  ROCKS_LOG_INFO(
      immutable_db_options_.info_log,
      "Flushing all column families with data in WAL number %" PRIu64
      ". Total log size is %" PRIu64 " while max_total_wal_size is %" PRIu64,
      oldest_alive_log, total_log_size_.load(), GetMaxTotalWalSize());
  // no need to refcount because drop is happening in write thread, so can't
  // happen while we're in the write thread
  autovector<ColumnFamilyData*> cfds;
  if (immutable_db_options_.atomic_flush) {
    SelectColumnFamiliesForAtomicFlush(&cfds);
  } else {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (cfd->OldestLogToKeep() <= oldest_alive_log) {
        cfds.push_back(cfd);
      }
    }
    MaybeFlushStatsCF(&cfds);
  }
  WriteThread::Writer nonmem_w;
  if (two_write_queues_) {
    nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
  }

  for (const auto cfd : cfds) {
    cfd->Ref();
    status = SwitchMemtable(cfd, write_context);
    cfd->UnrefAndTryDelete();
    if (!status.ok()) {
      break;
    }
  }
  if (two_write_queues_) {
    nonmem_write_thread_.ExitUnbatched(&nonmem_w);
  }

  if (status.ok()) {
    if (immutable_db_options_.atomic_flush) {
      AssignAtomicFlushSeq(cfds);
    }
    for (auto cfd : cfds) {
      cfd->imm()->FlushRequested();
      if (!immutable_db_options_.atomic_flush) {
        FlushRequest flush_req;
        GenerateFlushRequest({cfd}, FlushReason::kWalFull, &flush_req);
        SchedulePendingFlush(flush_req);
      }
    }
    if (immutable_db_options_.atomic_flush) {
      FlushRequest flush_req;
      GenerateFlushRequest(cfds, FlushReason::kWalFull, &flush_req);
      SchedulePendingFlush(flush_req);
    }
    MaybeScheduleFlushOrCompaction();
  }
  return status;
}

Status DBImpl::HandleWriteBufferManagerFlush(WriteContext* write_context) {
  mutex_.AssertHeld();
  assert(write_context != nullptr);
  Status status;

  // Before a new memtable is added in SwitchMemtable(),
  // write_buffer_manager_->ShouldFlush() will keep returning true. If another
  // thread is writing to another DB with the same write buffer, they may also
  // be flushed. We may end up with flushing much more DBs than needed. It's
  // suboptimal but still correct.
  // no need to refcount because drop is happening in write thread, so can't
  // happen while we're in the write thread
  autovector<ColumnFamilyData*> cfds;
  if (immutable_db_options_.atomic_flush) {
    SelectColumnFamiliesForAtomicFlush(&cfds);
  } else {
    ColumnFamilyData* cfd_picked = nullptr;
    SequenceNumber seq_num_for_cf_picked = kMaxSequenceNumber;

    for (auto cfd : *versions_->GetColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (!cfd->mem()->IsEmpty() && !cfd->imm()->IsFlushPendingOrRunning()) {
        // We only consider flush on CFs with bytes in the mutable memtable,
        // and no immutable memtables for which flush has yet to finish. If
        // we triggered flush on CFs already trying to flush, we would risk
        // creating too many immutable memtables leading to write stalls.
        uint64_t seq = cfd->mem()->GetCreationSeq();
        if (cfd_picked == nullptr || seq < seq_num_for_cf_picked) {
          cfd_picked = cfd;
          seq_num_for_cf_picked = seq;
        }
      }
    }
    if (cfd_picked != nullptr) {
      cfds.push_back(cfd_picked);
    }
    MaybeFlushStatsCF(&cfds);
  }
  if (!cfds.empty()) {
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log,
        "Flushing triggered to alleviate write buffer memory usage. Write "
        "buffer is using %" ROCKSDB_PRIszt
        " bytes out of a total of %" ROCKSDB_PRIszt ".",
        write_buffer_manager_->memory_usage(),
        write_buffer_manager_->buffer_size());
  }

  WriteThread::Writer nonmem_w;
  if (two_write_queues_) {
    nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
  }
  for (const auto cfd : cfds) {
    if (cfd->mem()->IsEmpty()) {
      continue;
    }
    cfd->Ref();
    status = SwitchMemtable(cfd, write_context);
    cfd->UnrefAndTryDelete();
    if (!status.ok()) {
      break;
    }
  }
  if (two_write_queues_) {
    nonmem_write_thread_.ExitUnbatched(&nonmem_w);
  }

  if (status.ok()) {
    if (immutable_db_options_.atomic_flush) {
      AssignAtomicFlushSeq(cfds);
    }
    for (const auto cfd : cfds) {
      cfd->imm()->FlushRequested();
      if (!immutable_db_options_.atomic_flush) {
        FlushRequest flush_req;
        GenerateFlushRequest({cfd}, FlushReason::kWriteBufferManager,
                             &flush_req);
        SchedulePendingFlush(flush_req);
      }
    }
    if (immutable_db_options_.atomic_flush) {
      FlushRequest flush_req;
      GenerateFlushRequest(cfds, FlushReason::kWriteBufferManager, &flush_req);
      SchedulePendingFlush(flush_req);
    }
    MaybeScheduleFlushOrCompaction();
  }
  return status;
}

uint64_t DBImpl::GetMaxTotalWalSize() const {
  uint64_t max_total_wal_size =
      max_total_wal_size_.load(std::memory_order_acquire);
  if (max_total_wal_size > 0) {
    return max_total_wal_size;
  }
  return 4 * max_total_in_memory_state_.load(std::memory_order_acquire);
}

// 延迟写入函数：根据写入控制器的状态延迟或停止写入
//
// 功能说明：
// 1. 处理两种写入停顿模式：
//    - kDelayed: 定时延迟，用于节流写入速率
//    - kStopped: 完全停止，用于严重情况下暂停所有写入
// 2. 支持 no_slowdown 选项，用于快速失败场景
// 3. 更新写入停顿统计信息
//
// REQUIRES: mutex_ is held
// 要求：调用此函数时必须持有互斥锁 mutex_
//
// REQUIRES: this thread is currently at the leader for write_thread
// 要求：当前线程必须是 write_thread 的领导者（leader）
Status DBImpl::DelayWrite(uint64_t num_bytes, WriteThread& write_thread,
                          const WriteOptions& write_options) {
  // 断言：验证当前线程确实持有 mutex_ 互斥锁
  mutex_.AssertHeld();

  // time_delayed: 记录延迟的总时间（单位：微秒），用于统计
  uint64_t time_delayed = 0;

  // delayed: 标记是否发生了延迟或暂停
  bool delayed = false;

  {
    // 创建一个计时器对象 StopWatch，用于统计停顿时间
    // 参数：
    // - immutable_db_options_.clock: 系统时钟
    // - stats_: 统计信息对象
    // - WRITE_STALL: 写入停顿事件的统计类型
    // - Histograms::HISTOGRAM_ENUM_MAX: 直方图的最大值
    // - &time_delayed: 输出参数，用于记录延迟时间
    StopWatch sw(immutable_db_options_.clock, stats_, WRITE_STALL,
                 Histograms::HISTOGRAM_ENUM_MAX, &time_delayed);

    // 注释：为了避免并行的定时延迟（这样会导致节流效果不佳），
    // 仅在主写入队列（primary write queue）上支持定时延迟
    // To avoid parallel timed delays (bad throttling), only support them
    // on the primary write queue.
    uint64_t delay;  // 存储需要延迟的时间（单位：微秒）

    // 判断当前的 write_thread 是否是主写入队列 write_thread_
    // 只有主写入队列才进行定时延迟处理（基于 num_bytes 计算延迟）
    if (&write_thread == &write_thread_) {
      // 调用 write_controller_ 获取需要延迟的时间
      // 参数：
      // - immutable_db_options_.clock: 系统时钟
      // - num_bytes: 本次写入的字节数，用于计算需要的延迟时间
      // 返回：需要延迟的微秒数
      delay =
          write_controller_.GetDelay(immutable_db_options_.clock, num_bytes);
    } else {
      // 如果不是主写入队列（例如非 memtable 写入），则 num_bytes 必须为 0
      // 断言验证：非主队列不应该有需要计字节数的写入
      assert(num_bytes == 0);
      delay = 0;  // 不进行延迟
    }

    // 测试同步点：用于单元测试，在延迟开始前插入一个钩子点
    TEST_SYNC_POINT("DBImpl::DelayWrite:Start");

    // 如果计算出的延迟时间大于 0，说明需要进行延迟处理（kDelayed 状态）
    if (delay > 0) {
      // 检查写入选项中的 no_slowdown 标志
      // 如果设置为 true，表示调用者不希望等待（用于快速失败场景）
      if (write_options.no_slowdown) {
        // 直接返回 Incomplete 状态，告知调用者因为写入停顿而无法完成
        return Status::Incomplete("Write stall");
      }

      // 测试同步点：用于单元测试，在睡眠前插入钩子点
      TEST_SYNC_POINT("DBImpl::DelayWrite:Sleep");

      // 通知 write_thread 线程即将发生停顿
      // 这样 write_thread 可以设置一个屏障（barrier），
      // 并让所有设置了 no_slowdown 标志的等待写入者失败返回
      // Notify write_thread about the stall so it can setup a barrier and
      // fail any pending writers with no_slowdown
      write_thread.BeginWriteStall();

      // 释放互斥锁 mutex_
      // 原因：睡眠时不应该持有锁，否则会阻塞其他操作（如后台压缩）
      mutex_.Unlock();

      // 测试同步点：用于单元测试，在开始写入停顿完成后插入钩子点
      TEST_SYNC_POINT("DBImpl::DelayWrite:BeginWriteStallDone");

      // 我们将延迟写入，直到：
      // 1. 已经睡眠了 delay 微秒，或者
      // 2. 不再需要延迟了（NeedsDelay() 返回 false）
      //
      // 每次检查间隔设置为 1ms（1001 微秒，略长于 1ms 是因为
      // WriteController 的最小延迟是 1ms，考虑到睡眠精度、取整等因素）
      //
      // We will delay the write until we have slept for `delay` microseconds
      // or we don't need a delay anymore. We check for cancellation every 1ms
      // (slightly longer because WriteController minimum delay is 1ms, in
      // case of sleep imprecision, rounding, etc.)
      const uint64_t kDelayInterval = 1001;  // 检查间隔：1001 微秒

      // 计算停顿结束时间点：当前开始时间 + 需要延迟的总时间
      uint64_t stall_end = sw.start_time() + delay;

      // 循环检查是否需要继续延迟
      while (write_controller_.NeedsDelay()) {
        // 如果当前时间已经达到或超过预设的停顿结束时间
        // 说明已经延迟了足够的时间，可以退出循环
        if (immutable_db_options_.clock->NowMicros() >= stall_end) {
          // 我们已经延迟了这个写入 delay 微秒，可以退出循环
          // We already delayed this write `delay` microseconds
          break;
        }

        // 标记确实发生了延迟
        delayed = true;

        // 睡眠 0.001 秒（即 1001 微秒）
        // Sleep for 0.001 seconds
        immutable_db_options_.clock->SleepForMicroseconds(kDelayInterval);
      }

      // 重新获取互斥锁 mutex_
      // 原因：睡眠结束后，恢复正常的同步保护
      mutex_.Lock();

      // 通知 write_thread 写入停顿已经结束
      // 清除之前设置的屏障，恢复正常写入流程
      write_thread.EndWriteStall();
    }

    // 处理"停止写入"状态（kStopped）
    //
    // 如果后台出现错误，则不等待，即使这是一个软错误。
    // 原因：我们可能会在这里无限期等待，因为后台压缩可能永远无法成功完成，
    // 导致停顿条件无限期持续
    //
    // Don't wait if there's a background error, even if its a soft error. We
    // might wait here indefinitely as the background compaction may never
    // finish successfully, resulting in the stall condition lasting
    // indefinitely
    while (error_handler_.GetBGError().ok() &&  // 后台错误状态正常
           write_controller_.IsStopped() &&    // 写入控制器处于停止状态
           !shutting_down_.load(std::memory_order_relaxed)) {  // 数据库未关闭
      // 检查写入选项中的 no_slowdown 标志
      if (write_options.no_slowdown) {
        // 直接返回 Incomplete 状态
        return Status::Incomplete("Write stall");
      }

      // 标记发生了延迟/停顿
      delayed = true;

      // 通知 write_thread 即将发生停顿
      // 这样 write_thread 可以设置屏障并让所有设置了 no_slowdown 的写入者失败
      // Notify write_thread about the stall so it can setup a barrier and
      // fail any pending writers with no_slowdown
      write_thread.BeginWriteStall();

      // 如果是主写入队列，插入测试同步点
      if (&write_thread == &write_thread_) {
        TEST_SYNC_POINT("DBImpl::DelayWrite:Wait");
      } else {
        // 如果是非 memtable 写入队列，插入不同的测试同步点
        TEST_SYNC_POINT("DBImpl::DelayWrite:NonmemWait");
      }

      // 在条件变量 bg_cv_ 上等待
      // 等待条件（满足任一即被唤醒）：
      // - 后台错误出现（GetBGError() 不返回 ok）
      // - 写入控制器不再是停止状态（IsStopped() 返回 false）
      // - 数据库正在关闭（shutting_down_ 为 true）
      // 其他线程（如后台压缩线程）会在状态改变后通过 bg_cv_.NotifyAll() 唤醒这里
      bg_cv_.Wait();

      // 测试同步点回调：用于单元测试，在等待完成后执行
      TEST_SYNC_POINT_CALLBACK("DBImpl::DelayWrite:AfterWait", &mutex_);

      // 通知 write_thread 停顿已经结束
      write_thread.EndWriteStall();
    }
  }  // StopWatch 析构时自动更新统计数据

  // 断言验证：如果发生了延迟，则不能设置 no_slowdown 标志
  // 这是因为如果设置了 no_slowdown，应该在延迟之前就已经返回了
  assert(!delayed || !write_options.no_slowdown);

  // 如果确实发生了延迟，更新统计信息
  if (delayed) {
    // 将延迟时间添加到默认列族的内部统计中
    // InternalStats::kIntStatsWriteStallMicros: 写入停顿的微秒数统计
    default_cf_internal_stats_->AddDBStats(
        InternalStats::kIntStatsWriteStallMicros, time_delayed);

    // 记录一个 tick 计数器，用于监控
    // STALL_MICROS: 停顿微秒数
    RecordTick(stats_, STALL_MICROS, time_delayed);
  }

  // 如果数据库不是只读模式，且 write_controller 没有停止写入，
  // 我们可以忽略任何后台错误，允许写入继续进行
  // If DB is not in read-only mode and write_controller is not stopping
  // writes, we can ignore any background errors and allow the write to
  // proceed
  Status s;  // 初始化状态为 OK

  // 检查写入控制器是否仍然处于停止状态
  if (write_controller_.IsStopped()) {
    // 如果数据库没有正在关闭
    if (!shutting_down_.load(std::memory_order_relaxed)) {
      // 如果写入仍然停止且数据库未关闭，说明我们是因为后台错误而退出
      // 返回 Incomplete 状态，携带后台错误信息
      // If writes are still stopped and db not shutdown, it means we bailed
      // due to a background error
      s = Status::Incomplete(error_handler_.GetBGError().ToString());
    } else {
      // 数据库正在关闭，返回 ShutdownInProgress 状态
      s = Status::ShutdownInProgress("stalled writes");
    }
  }

  // 如果错误处理器标记数据库已停止，直接返回后台错误
  // 这个检查可能覆盖前面的状态（如果数据库已停止，即使 IsStopped() 为 false）
  if (error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }

  // 返回最终的状态
  // 可能是：
  // - Status::OK()：写入可以继续
  // - Status::Incomplete()：写入停顿或后台错误
  // - Status::ShutdownInProgress()：数据库正在关闭
  return s;
}

// DBImpl::WriteBufferManagerStallWrites - 写入缓冲区管理器停顿写入
//
// 功能概述：
// 当内存使用超过阈值时，开始停止写入操作。这是 WriteBufferManager 触发的
// 写入停顿机制的一部分，用于防止内存过度使用。
//
// 前置条件：
// 1. REQUIRES: mutex_ is held - 必须持有 DB 的主互斥量
// 2. REQUIRES: this thread is currently at the front of the writer queue
//    - 当前线程必须在写入队列的头部
//    - 这意味着当前线程是 leader，可以安全地启动停顿
//
// 工作流程：
// 1. 开始写入停顿：通过 WriteThread::BeginWriteStall() 阻止新的写入加入队列
// 2. 释放 DB mutex：允许其他线程继续运行（如 flush 线程）
// 3. 通知 WriteBufferManager：将 DB 实例添加到 WriteBufferManager 的队列
// 4. 等待停顿结束：通过 WBMStallInterface::Block() 阻塞当前线程
// 5. 重新获取 DB mutex：准备结束停顿
// 6. 结束写入停顿：通过 WriteThread::EndWriteStall() 允许新的写入
//
// 停顿条件：
// WriteBufferManager 监控以下条件：
// - MemTable 的总内存使用超过 limit
// - Write Buffer 的总大小超过 limit
// 当条件满足时，WriteBufferManager 会调用此函数
//
// 解除停顿条件：
// - Flush 操作完成，释放 MemTable 内存
// - Write Buffer 大小下降到安全水平
// - WriteBufferManager 检测到内存使用下降，唤醒等待的线程
//
// 设计考虑：
// 1. 为什么需要释放 mutex？
//    - Flush 线程需要获取 mutex_ 来进行 flush 操作
//    - 如果不释放 mutex，flush 线程无法运行，导致死锁
//    - 释放 mutex_ 允许后台线程继续工作，释放内存
//
// 2. 为什么在释放 mutex 之前调用 BeginWriteStall()？
//    - BeginWriteStall() 需要在互斥量保护下调用
//    - 确保新的写入不会在停顿开始前加入队列
//    - 保持操作的原子性
//
// 3. WBMStallInterface 的作用：
//    - WriteBufferManager 通过此接口与 DB 实例交互
//    - SetState(BLOCKED): 标记 DB 处于阻塞状态
//    - BeginWriteStall(): 将 DB 添加到 WriteBufferManager 的队列
//    - Block(): 阻塞当前线程，直到内存使用下降
//    - 接口设计允许 WriteBufferManager 不直接依赖 DB 实现细节
//
// 4. 为什么重新获取 mutex？
//    - EndWriteStall() 需要在互斥量保护下调用
//    - 确保新的写入不会在停顿结束前加入队列
//    - 保持一致性
//
// 线程安全：
// - 此函数由 leader 线程调用，其他 writer 线程会等待
// - 释放 mutex 期间，其他线程可能获取 mutex 并执行操作
// - 但写入线程会在 LinkOne 中等待停顿清除
// - 重新获取 mutex 后，调用 EndWriteStall() 安全地结束停顿
//
// 性能影响：
// - 停顿期间，写入操作会被阻塞
// - Flush 线程可以继续运行，释放内存
// - 停顿时间取决于 flush 操作的速度
// - 可能影响写入吞吐量，但防止内存溢出
//
// 错误处理：
// - 此函数不会返回错误状态
// - 如果 DB 正在关闭，WriteBufferManager 会处理清理
// - 线程可能会在 Block() 中无限期等待，直到 flush 完成
//
// 与其他组件的交互：
// - WriteThread: 通过 BeginWriteStall/EndWriteStall 控制写入队列
// - WriteBufferManager: 通过 WBMStallInterface 监控内存使用并唤醒线程
// - Flush 线程: 释放 mutex 期间可以运行，释放内存
// - Writer 线程: 在 LinkOne 中等待停顿清除
void DBImpl::WriteBufferManagerStallWrites() {
  // ============================================================================
  // 前置条件检查
  // ============================================================================
  // 断言：当前线程必须持有 DB 的主互斥量
  // 这是一个重要的不变量，确保操作的线程安全
  mutex_.AssertHeld();

  // ============================================================================
  // 步骤 1：开始写入停顿（阻止新的写入加入队列）
  // ============================================================================
  // First block future writer threads who want to add themselves to the queue
  // 首先阻塞未来想要加入 WriteThread 队列的写入线程
  // of WriteThread.
  //
  // BeginWriteStall() 的作用：
  // 1. 在 WriteThread 队列头部插入 write_stall_dummy_
  // 2. 新的写入尝试链接队列时会看到 dummy
  // 3. 对于设置了 no_slowdown 的写入，立即返回失败
  // 4. 对于其他写入，在 LinkOne 中等待停顿清除
  //
  // 注意：此调用必须在 mutex_ 保护下进行
  write_thread_.BeginWriteStall();

  // ============================================================================
  // 步骤 2：释放 DB mutex
  // ============================================================================
  // 释放 mutex_ 允许其他线程继续运行
  // 特别是 flush 线程需要获取 mutex_ 来执行 flush 操作
  // 如果不释放 mutex，flush 线程无法运行，会导致死锁
  mutex_.Unlock();

  // ============================================================================
  // 步骤 3：通知 WriteBufferManager 状态变更
  // ============================================================================
  // Change the state to State::Blocked.
  // 将状态更改为 BLOCKED
  //
  // WBMStallInterface 是 WriteBufferManager 和 DB 实例之间的接口
  // SetState(BLOCKED) 标记 DB 处于阻塞状态
  // WriteBufferManager 使用此状态来管理多个 DB 实例
  static_cast<WBMStallInterface*>(wbm_stall_.get())
      ->SetState(WBMStallInterface::State::BLOCKED);

  // ============================================================================
  // 步骤 4：将 DB 实例添加到 WriteBufferManager 的队列并阻塞
  // ============================================================================
  // Then WriteBufferManager will add DB instance to its queue
  // WriteBufferManager 会将 DB 实例添加到其队列
  // and block this thread by calling WBMStallInterface::Block().
  // 并通过调用 WBMStallInterface::Block() 阻塞此线程
  //
  // BeginWriteStall() 的作用：
  // 1. 将 DB 实例添加到 WriteBufferManager 的停顿队列
  // 2. WriteBufferManager 会定期检查内存使用情况
  // 3. 当内存使用下降到安全水平时，唤醒等待的 DB 实例
  //
  // Block() 的作用：
  // 1. 阻塞当前线程（leader 线程）
  // 2. 等待 WriteBufferManager 的唤醒信号
  // 3. 当 flush 操作完成，内存释放后被唤醒
  write_buffer_manager_->BeginWriteStall(wbm_stall_.get());
  wbm_stall_->Block();

  // ============================================================================
  // 步骤 5：重新获取 DB mutex
  // ============================================================================
  // Block() 返回表示内存使用已下降到安全水平
  // 重新获取 mutex_ 以准备结束停顿
  // 注意：此时 flush 操作已经完成或正在进行
  mutex_.Lock();

  // ============================================================================
  // 步骤 6：结束写入停顿（允许新的写入）
  // ============================================================================
  // Stall has ended. Signal writer threads so that they can add
  // 停顿已经结束。通知写入线程，使它们可以将自己
  // themselves to the WriteThread queue for writes.
  // 添加到 WriteThread 队列进行写入
  //
  // EndWriteStall() 的作用：
  // 1. 从 WriteThread 队列中移除 write_stall_dummy_
  // 2. 解除对 WriteThread 队列的阻塞
  // 3. 唤醒所有等待停顿清除的写入线程
  // 4. 新的写入可以重新加入队列
  //
  // 注意：此调用必须在 mutex_ 保护下进行
  write_thread_.EndWriteStall();
}

// ThrottleLowPriWritesIfNeeded: 必要时限制低优先级写入
// 当压缩落后时，对低优先级写入进行限流，以加快压缩速度
Status DBImpl::ThrottleLowPriWritesIfNeeded(const WriteOptions& write_options,  // 写入选项
                                            WriteBatch* my_batch) {  // 写入批次
  assert(write_options.low_pri);  // 断言：低优先级写入
  // This is called outside the DB mutex. Although it is safe to make the call,
  // the consistency condition is not guaranteed to hold. It's OK to live with
  // it in this case.
  // 此函数在 DB 互斥锁之外调用，虽然调用是安全的，但不能保证一致性条件成立，这种情况是可以接受的
  // If we need to speed compaction, it means the compaction is left behind
  // and we start to limit low pri writes to a limit.
  // 如果需要加速压缩，说明压缩已经落后，开始限制低优先级写入到一定限度
  if (write_controller_.NeedSpeedupCompaction()) {  // 如果需要加速压缩
    if (allow_2pc() && (my_batch->HasCommit() || my_batch->HasRollback())) {  // 如果允许2PC且是提交或回滚
      // For 2PC, we only rate limit prepare, not commit.
      // 对于两阶段提交，只对 prepare 阶段限流，不对 commit 阶段限流
      return Status::OK();  // 返回成功
    }
    if (write_options.no_slowdown) {  // 如果不允许减速
      return Status::Incomplete("Low priority write stall");  // 返回未完成错误
    } else {  // 允许减速
      assert(my_batch != nullptr);  // 断言批次非空
      // Rate limit those writes. The reason that we don't completely wait
      // is that in case the write is heavy, low pri writes may never have
      // a chance to run. Now we guarantee we are still slowly making
      // progress.
      // 限流这些写入。不完全等待的原因是：如果写入负载很重，低优先级写入可能永远没有机会执行。
      // 现在我们保证仍然在缓慢地取得进展。
      PERF_TIMER_GUARD(write_delay_time);  // 启动写入延迟计时器
      write_controller_.low_pri_rate_limiter()->Request(  // 请求低优先级限流器
          my_batch->GetDataSize(), Env::IO_HIGH, nullptr /* stats */,  // 数据大小和优先级
          RateLimiter::OpType::kWrite);  // 操作类型：写入
    }
  }
  return Status::OK();  // 返回成功
}

void DBImpl::MaybeFlushStatsCF(autovector<ColumnFamilyData*>* cfds) {
  assert(cfds != nullptr);
  if (!cfds->empty() && immutable_db_options_.persist_stats_to_disk) {
    ColumnFamilyData* cfd_stats =
        versions_->GetColumnFamilySet()->GetColumnFamily(
            kPersistentStatsColumnFamilyName);
    if (cfd_stats != nullptr && !cfd_stats->mem()->IsEmpty()) {
      for (ColumnFamilyData* cfd : *cfds) {
        if (cfd == cfd_stats) {
          // stats CF already included in cfds
          return;
        }
      }
      // force flush stats CF when its log number is less than all other CF's
      // log numbers
      bool force_flush_stats_cf = true;
      for (auto* loop_cfd : *versions_->GetColumnFamilySet()) {
        if (loop_cfd == cfd_stats) {
          continue;
        }
        if (loop_cfd->GetLogNumber() <= cfd_stats->GetLogNumber()) {
          force_flush_stats_cf = false;
        }
      }
      if (force_flush_stats_cf) {
        cfds->push_back(cfd_stats);
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "Force flushing stats CF with automated flush "
                       "to avoid holding old logs");
      }
    }
  }
}

// DBImpl::TrimMemtableHistory - 修剪 MemTable 历史记录
//
// 功能概述：
// 修剪 MemTable 历史记录，释放不再需要的 immutable memtable。
// 这是为了控制内存使用，特别是在配置了 `allow_concurrent_memtable_write` 时。
//
// 背景知识：
// MemTable 历史记录（History）：
// - 当配置了 `allow_concurrent_memtable_write` 时，RocksDB 会保留多个 immutable memtable
// - 这些 memtable 可以用于读取，以支持时间旅行（point-in-time reads）
// - 但保留太多 memtable 会消耗大量内存
//
// 修剪策略：
// - 只有当 immutable memtable 不再被任何活动查询使用时，才能被释放
// - 调度器（trim_history_scheduler_）跟踪哪些 memtable 可以安全释放
// - 修剪是按需进行的，不会过于激进
//
// 参数说明：
// - context: 写入上下文
//   - memtables_to_free: 待释放的 memtable 列表
//   - superversion_context: 用于安装新的 SuperVersion
//
// 调用时机：
// - 在 WriteImpl 的步骤 4 中调用
// - 在写入完成后，检查是否需要修剪历史
// - 与 flush 操作配合，及时释放已 flush 的 memtable
//
// 工作流程：
// 1. 从调度器获取所有需要修剪的列族
// 2. 对每个列族调用 imm()->TrimHistory()
// 3. 如果有 memtable 被释放，安装新的 SuperVersion
// 4. 尝试删除不再使用的列族
//
// TrimHistory 的逻辑：
// - 检查当前活跃 memtable 的内存使用
// - 释放不再需要的 immutable memtable
// - 判断标准：memtable 不被任何快照或迭代器引用
// - 返回是否发生了修剪
//
// 为什么需要安装新的 SuperVersion？
// - imm 列表发生变化（部分 memtable 被释放）
// - 读取操作需要看到最新的 imm 列表
// - SuperVersion 聚合了 mem/imm/current，必须保持同步
//
// 性能考虑：
// - 修剪操作在 mutex_ 保护下进行
// - 应该快速完成，避免阻塞写入
// - 调度器确保公平性，不会过度修剪某个列族
//
// 内存管理：
// - 被释放的 memtable 添加到 context->memtables_to_free_
// - 这些 memtable 会在退出 WriteImpl 后被统一释放
// - 避免在互斥量内部进行复杂的内存释放操作
//
// 并发安全：
// - 调用时必须持有 mutex_（由 WriteImpl 保证）
// - TrimHistory 内部正确处理引用计数
// - 不会释放仍被使用的 memtable
Status DBImpl::TrimMemtableHistory(WriteContext* context) {
  // ============================================================================
  // 步骤 1：从调度器获取所有需要修剪的列族
  // ============================================================================
  // 创建列族列表
  autovector<ColumnFamilyData*> cfds;

  // 从 trim_history_scheduler_ 获取所有需要修剪的列族
  // 调度器（TrimHistoryScheduler）跟踪哪些列族需要修剪
  // TakeNextColumnFamily() 返回 nullptr 表示没有更多需要修剪的列族
  ColumnFamilyData* tmp_cfd;
  while ((tmp_cfd = trim_history_scheduler_.TakeNextColumnFamily()) !=
         nullptr) {
    // 将列族添加到待处理列表
    cfds.push_back(tmp_cfd);
  }

  // ============================================================================
  // 步骤 2：对每个列族进行修剪
  // ============================================================================
  // 遍历所有需要修剪的列族
  for (auto& cfd : cfds) {
    // 创建待删除的 memtable 列表（虽然这里没有使用）
    // 这个变量是 TrimHistory 的接口要求，用于接收被释放的 memtable
    autovector<MemTable*> to_delete;

    // 调用 immutable memtable 列表的修剪函数
    // 参数说明：
    // 1. &context->memtables_to_free_: 被释放的 memtable 添加到此列表
    // 2. cfd->mem()->MemoryAllocatedBytes(): 当前活跃 memtable 的内存使用
    //    - 用于计算可以释放多少内存
    //    - 确保释放后仍有足够的内存供当前 memtable 使用
    //
    // TrimHistory 的逻辑：
    // - 遍历 imm 列表中的所有 memtable
    // - 检查每个 memtable 是否仍被引用（快照、迭代器等）
    // - 释放不再被引用的 memtable
    // - 返回是否发生了修剪（trimmed）
    bool trimmed = cfd->imm()->TrimHistory(&context->memtables_to_free_,
                                           cfd->mem()->MemoryAllocatedBytes());

    // ============================================================================
    // 步骤 3：如果有 memtable 被释放，安装新的 SuperVersion
    // ============================================================================
    // 如果发生了修剪（trimmed == true），需要更新 SuperVersion
    if (trimmed) {
      // 创建新的 SuperVersion
      // NewSuperVersion() 会分配一个新的 SuperVersion 对象
      context->superversion_context.NewSuperVersion();

      // 断言：新 SuperVersion 必须成功创建
      assert(context->superversion_context.new_superversion.get() != nullptr);

      // 安装新的 SuperVersion
      // 因为 imm 列表发生了变化（部分 memtable 被释放）
      // SuperVersion 必须更新以反映最新的 imm 列表状态
      cfd->InstallSuperVersion(&context->superversion_context, &mutex_);
    }

    // ============================================================================
    // 步骤 4：尝试删除不再使用的列族
    // ============================================================================
    // UnrefAndTryDecrease(): 减少列族的引用计数
    // 如果引用计数归零，删除列族（通常发生在 DropColumnFamily 后）
    // 这确保了列族资源的正确清理
    if (cfd->UnrefAndTryDelete()) {
      // 列族已被删除，将指针设为 nullptr
      cfd = nullptr;
    }
  }

  // 返回成功状态
  // TrimMemtableHistory 不会失败，总是返回 OK
  return Status::OK();
}

/**
 * @brief 调度 flush 操作，将活跃 memtables 切换为不可变并触发后台 flush
 *
 * 该函数是 RocksDB flush 调度的核心入口点，负责：
 * 1. 选择需要 flush 的列族（单列族或原子多列族模式）
 * 2. 将活跃 memtables 切换为不可变 memtables
 * 3. 生成 flush 请求并调度后台 flush 任务
 *
 * 支持两种 flush 模式：
 * - **原子 flush (atomic_flush)**: 多个列族的 memtables 作为一个单元同时 flush
 * - **独立 flush**: 每个列族独立进行 flush
 *
 * @param context 写入上下文，用于存储 memtable 切换相关的资源（如 SuperVersion）
 *
 * @return Status 操作状态，OK 表示调度成功，否则返回失败原因
 *
 * @note 调用此函数时必须持有 mutex_（db_mutex）
 *
 * 函数执行流程：
 * 1. 根据 atomic_flush 配置选择需要 flush 的列族
 * 2. 如果启用双写队列（two_write_queues_），进入非内存写入队列
 * 3. 遍历选中的列族，将非空的活跃 memtables 切换为不可变
 * 4. 退出非内存写入队列（如果之前进入）
 * 5. 如果所有 memtable 切换成功：
 *    - 原子 flush: 为所有列族分配统一的序列号，生成单个 flush 请求
 *    - 独立 flush: 为每个列族生成独立的 flush 请求
 * 6. 调度后台 flush 任务
 *
 * 关键设计要点：
 * - **原子 flush 一致性**: 多列族使用相同的序列号，确保恢复时的状态一致性
 * - **双写队列支持**: two_write_queues_ 模式下通过非内存写入队列避免死锁
 * - **引用计数管理**: 对选中的列族增加引用，避免 flush 期间被删除
 * - **失败回滚**: 如果某个列族的 memtable 切换失败，停止后续操作
 *
 * 调用时机：
 * - 写入 buffer 满时（write_buffer_size 或 max_write_buffer_number）
 * - WAL 大小超过限制（max_total_wal_size）
 * - 写入 buffer manager 触发的内存压力
 * - 用户手动触发 flush
 *
 * 性能考虑：
 * - 使用 autovector 避免动态内存分配（小规模场景）
 * - 原子 flush 减少 MANIFEST 写入次数（多列族共享一个版本编辑）
 * - 延迟 flush 请求生成到 memtable 切换之后，避免锁内开销
 */
Status DBImpl::ScheduleFlushes(WriteContext* context) {
  autovector<ColumnFamilyData*> cfds;  // 需要执行 flush 的列族列表

  // 步骤 1: 选择需要 flush 的列族
  if (immutable_db_options_.atomic_flush) {
    // 原子 flush 模式
    // 从所有列族中选择有数据需要 flush 的列族：
    // - 有不可变 memtables 待 flush（cfd->imm()->NumNotFlushed() != 0）
    // - 活跃 memtable 非空（!cfd->mem()->IsEmpty()）
    // - 有待持久化的恢复状态（cached_recoverable_state_）
    SelectColumnFamiliesForAtomicFlush(&cfds);

    // 为所有选中的列族增加引用计数，防止在 flush 过程中被删除
    for (auto cfd : cfds) {
      cfd->Ref();
    }

    // 清空 flush 调度器，避免重复调度
    flush_scheduler_.Clear();
  } else {
    // 独立 flush 模式
    // 从 flush 调度器中逐个取出需要 flush 的列族
    // flush 调度器在需要 flush 时通过 AddColumnFamily() 添加列族
    ColumnFamilyData* tmp_cfd;
    while ((tmp_cfd = flush_scheduler_.TakeNextColumnFamily()) != nullptr) {
      cfds.push_back(tmp_cfd);
    }

    // 检查是否需要 flush 统计信息列族（如果有配置）
    MaybeFlushStatsCF(&cfds);
  }

  Status status;
  WriteThread::Writer nonmem_w;

  // 步骤 2: 如果启用双写队列，进入非内存写入队列
  //
  // 双写队列设计说明：
  // - RocksDB 支持两个写入队列：内存写入队列和非内存写入队列
  // - 内存写入队列：处理需要访问 memtable 的操作（如普通写入）
  // - 非内存写入队列：处理不需要访问 memtable 的后台任务（如 flush、compaction）
  //
  // 为什么要使用非内存写入队列？
  // - flush 和 compaction 需要长时间持有 db_mutex
  // - 如果在内存写入队列中执行，会阻塞所有写入操作
  // - 使用独立队列允许并发写入继续进行
  //
  // EnterUnbatched: 以非批量模式进入队列，确保该线程独立调度
  if (two_write_queues_) {
    nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
  }

  // 步骤 3: 将活跃 memtables 切换为不可变
  for (auto& cfd : cfds) {
    // 只有活跃 memtable 非空时才需要切换
    // 如果 memtable 为空，不需要 flush，跳过
    if (!cfd->mem()->IsEmpty()) {
      // SwitchMemtable 会：
      // 1. 将当前活跃 memtable 标记为不可变，添加到 imm 列表
      // 2. 创建新的活跃 memtable 接收后续写入
      // 3. 可能创建新的 WAL 文件（如果当前 WAL 需要切换）
      // 4. 更新 context 中的 superversion_context
      status = SwitchMemtable(cfd, context);
    }

    // 释放之前增加的引用计数
    // 如果引用计数降为 0，删除列族对象（如果已标记为删除）
    if (cfd->UnrefAndTryDelete()) {
      cfd = nullptr;
    }

    // 如果某个列族的 memtable 切换失败，停止后续处理
    // 部分列族已切换成功的状态将被保留
    if (!status.ok()) {
      break;
    }
  }

  // 步骤 4: 退出非内存写入队列
  if (two_write_queues_) {
    nonmem_write_thread_.ExitUnbatched(&nonmem_w);
  }

  // 步骤 5: 如果所有 memtable 切换成功，调度 flush 任务
  if (status.ok()) {
    if (immutable_db_options_.atomic_flush) {
      // 原子 flush 模式：所有列族作为整体 flush
      // 分配统一的序列号用于原子性
      AssignAtomicFlushSeq(cfds);

      // 生成单个 flush 请求，包含所有列族
      FlushRequest flush_req;
      GenerateFlushRequest(cfds, FlushReason::kWriteBufferFull, &flush_req);

      // 调度 flush 任务（批量处理所有列族）
      SchedulePendingFlush(flush_req);
    } else {
      // 独立 flush 模式：每个列族独立 flush
      for (auto* cfd : cfds) {
        FlushRequest flush_req;
        // 为每个列族生成独立的 flush 请求
        GenerateFlushRequest({cfd}, FlushReason::kWriteBufferFull, &flush_req);

        // 分别调度每个列族的 flush 任务
        SchedulePendingFlush(flush_req);
      }
    }

    // 触发后台 flush 或 compaction 线程开始处理任务
    MaybeScheduleFlushOrCompaction();
  }
  return status;
}

void DBImpl::NotifyOnMemTableSealed(ColumnFamilyData* /*cfd*/,
                                    const MemTableInfo& mem_table_info) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  mutex_.Unlock();
  for (auto listener : immutable_db_options_.listeners) {
    listener->OnMemTableSealed(mem_table_info);
  }
  mutex_.Lock();
}

/**
 * @brief 将列族的活跃 memtable 切换为不可变，并创建新的活跃 memtable
 *
 * 该函数是 RocksDB memtable 管理的核心操作，在 flush 前将当前活跃的 memtable
 * 标记为不可变（添加到 imm 列表），并创建新的 memtable 接收后续写入。
 * 同时可能创建新的 WAL 文件，确保数据持久化。
 *
 * @param cfd 目标列族数据指针，指定要执行 memtable 切换的列族
 * @param context 写入上下文，用于存储切换过程中的资源（如待释放的 memtables、SuperVersion 等）
 *
 * @return Status 操作状态，OK 表示切换成功
 *
 * 前置条件：
 * - 必须持有 mutex_（db_mutex）
 * - 调用线程必须在写入队列头部（确保无并发写入）
 * - 如果启用 two_write_queues_，线程必须在第二个写入队列头部
 *
 * 函数执行流程：
 * 1. 将可恢复状态写入 memtable（即将持久化）
 * 2. 判断是否需要创建新的 WAL 文件
 * 3. 准备 memtable 信息（用于回调通知）
 * 4. 释放 db_mutex，在锁外执行：
 *    - 创建新的 WAL 文件（如果需要）
 *    - 创建新的 memtable
 *    - 创建 SuperVersion（用于后续更新）
 * 5. 重新获取 db_mutex
 * 6. 处理 WAL 回收（如果启用回收）
 * 7. 切换 WAL 文件，刷新旧 WAL 缓冲区
 * 8. 失败时清理已分配资源
 * 9. 更新空列族的 log number（优化 WAL 清理）
 * 10. 将当前 memtable 添加到 imm 列表，安装新的 memtable
 * 11. 更新 SuperVersion 并调度后台任务
 * 12. 通知监听器 memtable 已封存
 *
 * 关键设计要点：
 * - **锁释放策略**：在锁外执行耗时操作（创建 WAL、创建 memtable）以减少锁竞争
 * - **可恢复状态持久化**：WriteRecoverableState 确保状态不会因 WAL 删除而丢失
 * - **WAL 回收机制**：recycle_log_file_num 允许重用旧的 WAL 文件以减少文件创建开销
 * - **空列族优化**：更新空列族的 log number 以加速旧 WAL 清理
 * - **错误恢复**：失败时清理已分配资源，避免内存泄漏
 *
 * 调用时机：
 * - memtable 大小达到 write_buffer_size
 * - WAL 大小超过限制需要切换
 * - 用户手动触发 flush
 * - 原子 flush 前的准备阶段
 *
 * 性能考虑：
 * - 锁释放期间允许并发写入继续进行
 * - WAL 预分配（preallocate_block_size）减少文件系统调用
 * - 空列族的 log number 更新避免不必要的 MANIFEST 写入
 */
// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
// REQUIRES: this thread is currently at the front of the 2nd writer queue if
// two_write_queues_ is true (This is to simplify the reasoning.)
Status DBImpl::SwitchMemtable(ColumnFamilyData* cfd, WriteContext* context) {
  mutex_.AssertHeld();
  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;
  log::Writer* new_log = nullptr;
  MemTable* new_mem = nullptr;
  IOStatus io_s;

  // 步骤 1: 将可恢复状态写入 memtable
  //
  // 设计说明：
  // - 可恢复状态（如 WAL 回收点、事务状态等）通常持久化在 WAL 中
  // - memtable 切换后，旧的 WAL 可能会被删除
  // - 为了确保状态不丢失，将状态写入 memtable（后续 flush 到 SST 文件）
  Status s = WriteRecoverableState();
  if (!s.ok()) {
    return s;
  }

  // 步骤 2: 尝试切换到新的 memtable 并触发旧 memtable 的 flush
  // 注意：大部分操作在 db_mutex 释放后执行，以减少锁持有时间
  assert(versions_->prev_log_number() == 0);

  // 判断是否需要创建新的 WAL 文件
  // log_empty_ 为 true 表示当前 WAL 文件没有写入，可以复用
  if (two_write_queues_) {
    log_write_mutex_.Lock();
  }
  bool creating_new_log = !log_empty_;
  if (two_write_queues_) {
    log_write_mutex_.Unlock();
  }

  // WAL 回收机制：重用旧的 WAL 文件以减少文件创建开销
  uint64_t recycle_log_number = 0;
  if (creating_new_log && immutable_db_options_.recycle_log_file_num &&
      !log_recycle_files_.empty()) {
    recycle_log_number = log_recycle_files_.front();
  }

  // 分配新的 WAL 文件号
  uint64_t new_log_number =
      creating_new_log ? versions_->NewFileNumber() : logfile_number_;
  const MutableCFOptions mutable_cf_options = *cfd->GetLatestMutableCFOptions();

  // 步骤 3: 准备 memtable 信息（用于回调通知）
  MemTableInfo memtable_info;
  memtable_info.cf_name = cfd->GetName();
  memtable_info.first_seqno = cfd->mem()->GetFirstSequenceNumber();
  memtable_info.earliest_seqno = cfd->mem()->GetEarliestSequenceNumber();
  memtable_info.num_entries = cfd->mem()->num_entries();
  memtable_info.num_deletes = cfd->mem()->num_deletes();
  // 记录待 flush 的不可变 memtable 数量（稍后记录日志）
  int num_imm_unflushed = cfd->imm()->NumNotFlushed();

  // 计算 WAL 预分配块大小
  // 预分配可以减少文件系统调用，提高写入性能
  const auto preallocate_block_size =
      GetWalPreallocateBlockSize(mutable_cf_options.write_buffer_size);

  // 步骤 4: 释放 db_mutex，在锁外执行耗时操作
  mutex_.Unlock();

  // 创建新的 WAL 文件（如果需要）
  if (creating_new_log) {
    // TODO: 写入缓冲区大小应该是所有列族的最大值，而不是单个列族的
    io_s = CreateWAL(new_log_number, recycle_log_number, preallocate_block_size,
                     &new_log);
    if (s.ok()) {
      s = io_s;
    }
  }

  // 创建新的 memtable（仅在所有前置操作成功后）
  if (s.ok()) {
    SequenceNumber seq = versions_->LastSequence();
    new_mem = cfd->ConstructNewMemtable(mutable_cf_options, seq);
    context->superversion_context.NewSuperVersion();
  }

  // 记录日志信息
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] New memtable created with log file: #%" PRIu64
                 ". Immutable memtables: %d.\n",
                 cfd->GetName().c_str(), new_log_number, num_imm_unflushed);

  // 构造碎片化的范围删除标记
  // 由于线程在写入队列头部，此时没有并发写入，可以安全地操作
  cfd->mem()->ConstructFragmentedRangeTombstones();

  // 步骤 5: 重新获取 db_mutex
  mutex_.Lock();

  // 步骤 6: 处理 WAL 回收
  if (recycle_log_number != 0) {
    // Since renaming the file is done outside DB mutex, we need to ensure
    // concurrent full purges don't delete the file while we're recycling it.
    // To achieve that we hold the old log number in the recyclable list until
    // after it has been renamed.
    assert(log_recycle_files_.front() == recycle_log_number);
    log_recycle_files_.pop_front();
  }

  // 步骤 7: 切换 WAL 文件
  if (s.ok() && creating_new_log) {
    InstrumentedMutexLock l(&log_write_mutex_);
    assert(new_log != nullptr);
    if (!logs_.empty()) {
      // Alway flush the buffer of the last log before switching to a new one
      log::Writer* cur_log_writer = logs_.back().writer;
      if (error_handler_.IsRecoveryInProgress()) {
        // In recovery path, we force another try of writing WAL buffer.
        cur_log_writer->file()->reset_seen_error();
      }
      io_s = cur_log_writer->WriteBuffer();
      if (s.ok()) {
        s = io_s;
      }
      if (!s.ok()) {
        ROCKS_LOG_WARN(immutable_db_options_.info_log,
                       "[%s] Failed to switch from #%" PRIu64 " to #%" PRIu64
                       "  WAL file\n",
                       cfd->GetName().c_str(), cur_log_writer->get_log_number(),
                       new_log_number);
      }
    }
    if (s.ok()) {
      logfile_number_ = new_log_number;
      log_empty_ = true;
      log_dir_synced_ = false;
      logs_.emplace_back(logfile_number_, new_log);
      alive_log_files_.push_back(LogFileNumberSize(logfile_number_));
    }
  }

  // 步骤 8: 失败时清理已分配资源
  if (!s.ok()) {
    // how do we fail if we're not creating new log?
    assert(creating_new_log);
    delete new_mem;
    delete new_log;
    context->superversion_context.new_superversion.reset();
    // We may have lost data from the WritableFileBuffer in-memory buffer for
    // the current log, so treat it as a fatal error and set bg_error
    if (!io_s.ok()) {
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kMemTable);
    } else {
      error_handler_.SetBGError(s, BackgroundErrorReason::kMemTable);
    }
    // Read back bg_error in order to get the right severity
    s = error_handler_.GetBGError();
    return s;
  }

  // 步骤 9: 更新空列族的 log number（优化 WAL 清理）
  bool empty_cf_updated = false;
  if (immutable_db_options_.track_and_verify_wals_in_manifest &&
      !immutable_db_options_.allow_2pc && creating_new_log) {
    // In non-2pc mode, WALs become obsolete if they do not contain unflushed
    // data. Updating the empty CF's log number might cause some WALs to become
    // obsolete. So we should track the WAL obsoletion event before actually
    // updating the empty CF's log number.
    uint64_t min_wal_number_to_keep =
        versions_->PreComputeMinLogNumberWithUnflushedData(logfile_number_);
    if (min_wal_number_to_keep >
        versions_->GetWalSet().GetMinWalNumberToKeep()) {
      // Get a snapshot of the empty column families.
      // LogAndApply may release and reacquire db
      // mutex, during that period, column family may become empty (e.g. its
      // flush succeeds), then it affects the computed min_log_number_to_keep,
      // so we take a snapshot for consistency of column family data
      // status. If a column family becomes non-empty afterwards, its active log
      // should still be the created new log, so the min_log_number_to_keep is
      // not affected.
      autovector<ColumnFamilyData*> empty_cfs;
      for (auto cf : *versions_->GetColumnFamilySet()) {
        if (cf->IsEmpty()) {
          empty_cfs.push_back(cf);
        }
      }

      VersionEdit wal_deletion;
      wal_deletion.DeleteWalsBefore(min_wal_number_to_keep);
      s = versions_->LogAndApplyToDefaultColumnFamily(
          read_options, &wal_deletion, &mutex_, directories_.GetDbDir());
      if (!s.ok() && versions_->io_status().IsIOError()) {
        s = error_handler_.SetBGError(versions_->io_status(),
                                      BackgroundErrorReason::kManifestWrite);
      }
      if (!s.ok()) {
        return s;
      }

      for (auto cf : empty_cfs) {
        if (cf->IsEmpty()) {
          cf->SetLogNumber(logfile_number_);
          // MEMPURGE: No need to change this, because new adds
          // should still receive new sequence numbers.
          cf->mem()->SetCreationSeq(versions_->LastSequence());
        }  // cf may become non-empty.
      }
      empty_cf_updated = true;
    }
  }
  if (!empty_cf_updated) {
    for (auto cf : *versions_->GetColumnFamilySet()) {
      // all this is just optimization to delete logs that
      // are no longer needed -- if CF is empty, that means it
      // doesn't need that particular log to stay alive, so we just
      // advance the log number. no need to persist this in the manifest
      if (cf->IsEmpty()) {
        if (creating_new_log) {
          cf->SetLogNumber(logfile_number_);
        }
        cf->mem()->SetCreationSeq(versions_->LastSequence());
      }
    }
  }

  cfd->mem()->SetNextLogNumber(logfile_number_);
  assert(new_mem != nullptr);
  cfd->imm()->Add(cfd->mem(), &context->memtables_to_free_);
  new_mem->Ref();
  cfd->SetMemtable(new_mem);
  InstallSuperVersionAndScheduleWork(cfd, &context->superversion_context,
                                     mutable_cf_options);

  // 步骤 11: 通知监听器 memtable 已封存
  // 在成功安装新 memtable 后通知客户端
  NotifyOnMemTableSealed(cfd, memtable_info);
  // 步骤 12: 忽略未检查的 IO 错误
  // 可能到这里没有检查 i_os 的值，但这没关系
  // 如果检查过，很可能 s 已经是错误状态
  // 无论如何，在这里忽略 i_os 的任何未检查错误
  io_s.PermitUncheckedError();
  return s;
}

size_t DBImpl::GetWalPreallocateBlockSize(uint64_t write_buffer_size) const {
  mutex_.AssertHeld();
  size_t bsize =
      static_cast<size_t>(write_buffer_size / 10 + write_buffer_size);
  // Some users might set very high write_buffer_size and rely on
  // max_total_wal_size or other parameters to control the WAL size.
  if (mutable_db_options_.max_total_wal_size > 0) {
    bsize = std::min<size_t>(
        bsize, static_cast<size_t>(mutable_db_options_.max_total_wal_size));
  }
  if (immutable_db_options_.db_write_buffer_size > 0) {
    bsize = std::min<size_t>(bsize, immutable_db_options_.db_write_buffer_size);
  }
  if (immutable_db_options_.write_buffer_manager &&
      immutable_db_options_.write_buffer_manager->enabled()) {
    bsize = std::min<size_t>(
        bsize, immutable_db_options_.write_buffer_manager->buffer_size());
  }

  return bsize;
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) {
  // Pre-allocate size of write batch conservatively.
  // 8 bytes are taken by header, 4 bytes for count, 1 byte for type,
  // and we allocate 11 extra bytes for key length, as well as value length.
  WriteBatch batch(key.size() + value.size() + 24, 0 /* max_bytes */,
                   opt.protection_bytes_per_key, 0 /* default_cf_ts_sz */);
  Status s = batch.Put(column_family, key, value);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::Put(const WriteOptions& opt, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& ts, const Slice& value) {
  ColumnFamilyHandle* default_cf = DefaultColumnFamily();
  assert(default_cf);
  const Comparator* const default_cf_ucmp = default_cf->GetComparator();
  assert(default_cf_ucmp);
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key,
                   default_cf_ucmp->timestamp_size());
  Status s = batch.Put(column_family, key, ts, value);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::PutEntity(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const WideColumns& columns) {
  const ColumnFamilyHandle* const default_cf = DefaultColumnFamily();
  assert(default_cf);

  const Comparator* const default_cf_ucmp = default_cf->GetComparator();
  assert(default_cf_ucmp);

  WriteBatch batch(/* reserved_bytes */ 0, /* max_bytes */ 0,
                   options.protection_bytes_per_key,
                   default_cf_ucmp->timestamp_size());

  const Status s = batch.PutEntity(column_family, key, columns);
  if (!s.ok()) {
    return s;
  }

  return Write(options, &batch);
}

Status DB::Delete(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                  const Slice& key) {
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key, 0 /* default_cf_ts_sz */);
  Status s = batch.Delete(column_family, key);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                  const Slice& key, const Slice& ts) {
  ColumnFamilyHandle* default_cf = DefaultColumnFamily();
  assert(default_cf);
  const Comparator* const default_cf_ucmp = default_cf->GetComparator();
  assert(default_cf_ucmp);
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key,
                   default_cf_ucmp->timestamp_size());
  Status s = batch.Delete(column_family, key, ts);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::SingleDelete(const WriteOptions& opt,
                        ColumnFamilyHandle* column_family, const Slice& key) {
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key, 0 /* default_cf_ts_sz */);
  Status s = batch.SingleDelete(column_family, key);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::SingleDelete(const WriteOptions& opt,
                        ColumnFamilyHandle* column_family, const Slice& key,
                        const Slice& ts) {
  ColumnFamilyHandle* default_cf = DefaultColumnFamily();
  assert(default_cf);
  const Comparator* const default_cf_ucmp = default_cf->GetComparator();
  assert(default_cf_ucmp);
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key,
                   default_cf_ucmp->timestamp_size());
  Status s = batch.SingleDelete(column_family, key, ts);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::DeleteRange(const WriteOptions& opt,
                       ColumnFamilyHandle* column_family,
                       const Slice& begin_key, const Slice& end_key) {
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key, 0 /* default_cf_ts_sz */);
  Status s = batch.DeleteRange(column_family, begin_key, end_key);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::DeleteRange(const WriteOptions& opt,
                       ColumnFamilyHandle* column_family,
                       const Slice& begin_key, const Slice& end_key,
                       const Slice& ts) {
  ColumnFamilyHandle* default_cf = DefaultColumnFamily();
  assert(default_cf);
  const Comparator* const default_cf_ucmp = default_cf->GetComparator();
  assert(default_cf_ucmp);
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key,
                   default_cf_ucmp->timestamp_size());
  Status s = batch.DeleteRange(column_family, begin_key, end_key, ts);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::Merge(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                 const Slice& key, const Slice& value) {
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key, 0 /* default_cf_ts_sz */);
  Status s = batch.Merge(column_family, key, value);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

Status DB::Merge(const WriteOptions& opt, ColumnFamilyHandle* column_family,
                 const Slice& key, const Slice& ts, const Slice& value) {
  ColumnFamilyHandle* default_cf = DefaultColumnFamily();
  assert(default_cf);
  const Comparator* const default_cf_ucmp = default_cf->GetComparator();
  assert(default_cf_ucmp);
  WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                   opt.protection_bytes_per_key,
                   default_cf_ucmp->timestamp_size());
  Status s = batch.Merge(column_family, key, ts, value);
  if (!s.ok()) {
    return s;
  }
  return Write(opt, &batch);
}

}  // namespace ROCKSDB_NAMESPACE
