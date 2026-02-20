//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/concurrent_task_limiter_impl.h"

#include "rocksdb/concurrent_task_limiter.h"

namespace ROCKSDB_NAMESPACE {

ConcurrentTaskLimiterImpl::ConcurrentTaskLimiterImpl(
    const std::string& name, int32_t max_outstanding_task)
    : name_(name),
      max_outstanding_tasks_{max_outstanding_task},
      outstanding_tasks_{0} {}

ConcurrentTaskLimiterImpl::~ConcurrentTaskLimiterImpl() {
  assert(outstanding_tasks_ == 0);
}

const std::string& ConcurrentTaskLimiterImpl::GetName() const { return name_; }

void ConcurrentTaskLimiterImpl::SetMaxOutstandingTask(int32_t limit) {
  max_outstanding_tasks_.store(limit, std::memory_order_relaxed);
}

void ConcurrentTaskLimiterImpl::ResetMaxOutstandingTask() {
  max_outstanding_tasks_.store(-1, std::memory_order_relaxed);
}

int32_t ConcurrentTaskLimiterImpl::GetOutstandingTask() const {
  return outstanding_tasks_.load(std::memory_order_relaxed);
}

/**
 * @brief 获取任务限制令牌
 *
 * 该函数尝试获取任务限制令牌，用于控制并发任务的数量。
 * 如果成功获取到令牌，则允许执行任务；如果获取失败，则表示已达到并发限制。
 *
 * @param force 是否强制获取令牌（忽略限制）
 *            - true：强制获取令牌，不检查并发限制
 *              即使超出限制也允许执行任务
 *              用于紧急任务或手动触发任务
 *            - false：遵守并发限制，只有当前任务数 < 限制时才获取令牌
 *              用于正常的自动任务调度
 *              这是最常用的模式
 *
 * @return std::unique_ptr<TaskLimiterToken> 任务限制令牌
 *         - 成功：返回有效的令牌指针
 *           令牌的析构函数会自动释放计数（--outstanding_tasks_）
 *           使用 RAII（Resource Acquisition Is Initialization）模式管理资源
 *         - 失败：返回 nullptr
 *           表示已达到并发限制，不应执行任务
 *           调用者应稍后重试或放弃任务
 *
 * @note 线程安全：使用原子操作，可以在多线程环境中安全调用
 *
 * @brief 限制器的行为：
 * - 限制值（max_outstanding_tasks_）：
 *   - 正数（如 4）：最多允许 4 个并发任务
 *   - -1：无限制，允许任意数量的并发任务
 *   - 0：不允许任何任务（特殊配置，很少使用）
 *
 * - 当前任务数（outstanding_tasks_）：
 *   - 原子整数，表示当前正在执行的任务数量
 *   - 使用 memory_order_relaxed 内存序，适合计数器场景
 *   - 令牌析构时自动递减
 *
 * @brief 强制模式（force = true）：
 * - 不检查并发限制，总是成功获取令牌
 * - 使用场景：
 *   - 手动触发的压缩（用户要求立即执行）
 *   - 紧急任务（不能等待）
 *   - 调试和测试（需要突破限制）
 * - 风险：
 *   - 可能超出系统资源限制
 *   - 导致性能下降（锁竞争、I/O 竞争）
 *   - 甚至可能 OOM（内存不足）
 *
 * @brief 非强制模式（force = false）：
 * - 检查并发限制，只有未超限时才获取令牌
 * - 使用场景：
 *   - 自动后台压缩（默认行为）
 *   - 正常的 flush 任务
 *   - 避免系统过载
 * - 行为：
 *   - 如果 tasks < limit：通过 CAS 原子操作递增计数
 *   - 如果 tasks >= limit：返回 nullptr，获取失败
 *   - 如果 CAS 失败（多线程竞争）：重新检查并重试
 *
 * @brief compare_exchange_weak 的作用：
 * - 原子操作：比较并交换（Compare-And-Swap）
 * - 参数：(expected, desired) -> (old, succeeded)
 * - 行为：
 *   1. 如果 outstanding_tasks_ == expected：
 *      - 设置 outstanding_tasks_ = desired
 *      - 返回 true（成功）
 *   2. 如果 outstanding_tasks_ != expected：
 *      - 不修改 outstanding_tasks_
 *      - 将当前值赋给 expected
 *      - 返回 false（失败，需要重试）
 * - weak 版本：
 *   - 可能在成功时返回 false（spurious failure）
 *   - 必须在循环中重试
 *   - 比强版本（compare_exchange_strong）更高效
 *
 * @brief 循环条件解释：
 * while (force || limit < 0 || tasks < limit)
 * - force = true：总是进入循环（忽略限制）
 * - force = false && limit < 0：无限制模式，总是进入循环
 * - force = false && tasks < limit：未超限，进入循环
 * - force = false && tasks >= limit：已超限，不进入循环，返回 nullptr
 *
 * @brief 竞争情况处理：
 * - 多个线程同时调用 GetToken()：
 *   - 线程 A：读取 tasks=2, limit=4，尝试 CAS(2,3) -> 成功
 *   - 线程 B：读取 tasks=2, limit=4，尝试 CAS(2,3) -> 失败
 *   - 线程 B：tasks 更新为 3，重新检查 tasks=3 < limit=4 -> 成功
 * - CAS 失败的线程会：
 *   1. 重新加载最新的 tasks 值
 *   2. 重新检查循环条件
 *   3. 重新尝试 CAS 操作
 * - 这种自旋（spin-wait）方式在竞争不激烈时效率高
 */
std::unique_ptr<TaskLimiterToken> ConcurrentTaskLimiterImpl::GetToken(
    bool force) {
  // 1. 读取当前限制值
  // 使用 memory_order_relaxed：不保证与其他操作的顺序
  // 适用于计数器场景，性能更好
  int32_t limit = max_outstanding_tasks_.load(std::memory_order_relaxed);

  // 2. 读取当前任务数
  // 同样使用 memory_order_relaxed
  int32_t tasks = outstanding_tasks_.load(std::memory_order_relaxed);

  // force = true, bypass the throttle.
  // limit < 0 means unlimited tasks.
  // 循环尝试获取令牌：
  // - force = true：总是进入循环（强制模式）
  // - force = false && limit < 0：无限制模式，总是进入循环
  // - force = false && tasks < limit：未超限，进入循环尝试获取
  // - force = false && tasks >= limit：已超限，跳出循环，返回 nullptr
  while (force || limit < 0 || tasks < limit) {
    // 3. 尝试通过 CAS 原子操作递增任务计数
    // outstanding_tasks_ 是 std::atomic<int32_t> 类型
    // compare_exchange_weak 是无锁编程的核心原语
    //
    // 操作逻辑：
    // - 如果 outstanding_tasks_ == tasks（当前值）：
    //   - 设置 outstanding_tasks_ = tasks + 1
    //   - 返回 true（CAS 成功）
    //   - tasks 被更新为最新的 outstanding_tasks_ 值
    // - 如果 outstanding_tasks_ != tasks（其他线程修改了）：
    //   - 不修改 outstanding_tasks_
    //   - 将最新的 outstanding_tasks_ 值赋给 tasks
    //   - 返回 false（CAS 失败）
    //
    // weak 版本说明：
    // - 可能会"虚假失败"（spurious failure）
    // - 即使值未改变也可能返回 false
    // - 必须在循环中重试
    // - 比强版本更高效（不要求严格排序）
    if (outstanding_tasks_.compare_exchange_weak(tasks, tasks + 1)) {
      // CAS 成功：成功获取到令牌
      // 创建并返回令牌对象
      // TaskLimiterToken 的析构函数会自动调用 --outstanding_tasks_
      // 实现 RAII 语义，防止资源泄漏
      return std::unique_ptr<TaskLimiterToken>(new TaskLimiterToken(this));
    }
    // CAS 失败：tasks 已被其他线程更新
    // tasks 现在包含最新的 outstanding_tasks_ 值
    // 继续循环，重新检查条件并重试
  }

  // 退出循环，说明：
  // - force = false 且 tasks >= limit
  // - 已达到并发限制，无法获取令牌
  // 返回 nullptr 表示获取失败
  return nullptr;
}

ConcurrentTaskLimiter* NewConcurrentTaskLimiter(const std::string& name,
                                                int32_t limit) {
  return new ConcurrentTaskLimiterImpl(name, limit);
}

TaskLimiterToken::~TaskLimiterToken() {
  --limiter_->outstanding_tasks_;
  assert(limiter_->outstanding_tasks_ >= 0);
}

}  // namespace ROCKSDB_NAMESPACE
