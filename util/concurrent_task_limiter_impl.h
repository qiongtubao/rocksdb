//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// 本文件实现并发任务限流器（ConcurrentTaskLimiter）。
//
// 背景：
//   RocksDB 有多种后台任务（Flush、Compaction 等），这些任务共享有限的
//   线程池资源。在高并发写入时，可能产生大量并发 Compaction，耗尽 I/O 带宽。
//   ConcurrentTaskLimiter 提供了一种基于令牌（Token）的流量控制机制，
//   限制某类任务的最大并发数量。
//
// 工作原理（令牌桶）：
//   - 每次提交任务前，调用 GetToken() 申请一个令牌
//   - 若当前并发任务数已达上限，GetToken() 返回 nullptr（被限流）
//   - 令牌（TaskLimiterToken）持有期间计为"outstanding task"
//   - 令牌析构时自动归还（outstanding_tasks_ 递减）
//   - 使用原子操作实现，无需加锁（高性能）
//
// 强制模式：
//   GetToken(force=true) 可绕过限制强制获取令牌，
//   用于某些必须执行的关键任务（如 L0 文件过多时的强制 Compaction）

#pragma once
#include <atomic>
#include <memory>

#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class TaskLimiterToken;

// ConcurrentTaskLimiterImpl —— 并发任务限流器的具体实现
//
// 使用两个原子整数追踪限流状态：
//   max_outstanding_tasks_: 最大允许的并发任务数（-1 表示不限制）
//   outstanding_tasks_:     当前正在执行的任务数
//
// 线程安全：所有操作基于 std::atomic，无需加锁
class ConcurrentTaskLimiterImpl : public ConcurrentTaskLimiter {
 public:
  // 构造函数
  // name:                  限流器名称（用于日志和监控）
  // max_outstanding_task:  最大并发任务数（-1 表示不限制）
  explicit ConcurrentTaskLimiterImpl(const std::string& name,
                                     int32_t max_outstanding_task);

  // 禁止拷贝（每个限流器独立管理计数状态）
  ConcurrentTaskLimiterImpl(const ConcurrentTaskLimiterImpl&) = delete;
  ConcurrentTaskLimiterImpl& operator=(const ConcurrentTaskLimiterImpl&) =
      delete;

  virtual ~ConcurrentTaskLimiterImpl();

  // 返回限流器名称
  virtual const std::string& GetName() const override;

  // 动态调整最大并发任务数
  // limit: 新的上限值（-1 表示不限制）
  // 原子写入，立即生效（不影响已在执行的任务）
  virtual void SetMaxOutstandingTask(int32_t limit) override;

  // 重置为不限制模式（等效于 SetMaxOutstandingTask(-1)）
  virtual void ResetMaxOutstandingTask() override;

  // 返回当前正在执行的任务数
  // 使用原子 relaxed 读，允许轻微滞后（用于监控统计）
  virtual int32_t GetOutstandingTask() const override;

  // 申请一个任务执行令牌
  //
  // 令牌（TaskLimiterToken）采用 RAII 设计：
  //   - 创建时：outstanding_tasks_ 原子递增
  //   - 析构时：outstanding_tasks_ 原子递减（自动归还）
  //
  // force: 是否强制获取（忽略上限限制）
  //   true  - 绕过限制，始终返回有效令牌（用于关键路径）
  //   false - 若 outstanding_tasks_ >= max_outstanding_tasks_，返回 nullptr
  //
  // 返回：
  //   unique_ptr<TaskLimiterToken>  成功获取令牌
  //   nullptr                       被限流（仅在 force=false 时）
  virtual std::unique_ptr<TaskLimiterToken> GetToken(bool force);

 private:
  friend class TaskLimiterToken;  // 允许令牌访问 outstanding_tasks_（归还时递减）

  std::string name_;                           // 限流器名称
  std::atomic<int32_t> max_outstanding_tasks_; // 最大并发任务数上限（-1 不限）
  std::atomic<int32_t> outstanding_tasks_;     // 当前正在执行的任务数
};

// TaskLimiterToken —— 并发任务执行令牌（RAII）
//
// 持有此令牌期间，限流器的 outstanding_tasks_ 保持递增状态。
// 令牌析构时自动将 outstanding_tasks_ 递减，无需手动归还。
//
// 典型用法：
//   auto token = limiter->GetToken(false);
//   if (!token) {
//     // 被限流，跳过本次任务
//     return;
//   }
//   // 执行任务...
//   // token 析构时自动归还
class TaskLimiterToken {
 public:
  // 构造函数：记录所属的限流器（此时 outstanding_tasks_ 已在 GetToken() 中递增）
  explicit TaskLimiterToken(ConcurrentTaskLimiterImpl* limiter)
      : limiter_(limiter) {}

  // 析构函数：自动归还令牌（递减 outstanding_tasks_）
  ~TaskLimiterToken();

 private:
  ConcurrentTaskLimiterImpl* limiter_;  // 所属的限流器（不拥有所有权）

  // 禁止拷贝，防止令牌被复制导致计数错乱
  TaskLimiterToken(const TaskLimiterToken&) = delete;
  void operator=(const TaskLimiterToken&) = delete;
};

}  // namespace ROCKSDB_NAMESPACE
