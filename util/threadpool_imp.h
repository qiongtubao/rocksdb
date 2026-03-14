//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// ThreadPoolImpl —— RocksDB 后台线程池实现
//
// 职责：
//   管理一组后台线程，负责执行 Flush、Compaction 等后台任务。
//   RocksDB 按优先级维护多个线程池（HIGH、LOW、BOTTOM、USER），
//   本类是每个优先级线程池的具体实现。
//
// 线程池生命周期：
//   1. SetBackgroundThreads(n)  → 创建 n 个后台线程
//   2. Schedule()/SubmitJob()   → 将任务加入队列，唤醒等待线程
//   3. BGThread()               → 后台线程循环取任务执行
//   4. JoinAllThreads()         → 等待所有线程退出（丢弃未执行的任务）
//   5. WaitForJobsAndJoinAllThreads() → 等待所有任务完成后再退出
//
// 线程数动态调整：
//   - SetBackgroundThreads(n)  可增减线程数
//   - 减少线程时，多余线程在完成当前任务后自行退出（按创建逆序）
//   - IncBackgroundThreadsIfNeeded(n) 仅扩容，不缩减
//
// 线程预留机制（ReserveThreads/ReleaseThreads）：
//   允许某些任务"预留"线程，使其他任务无法占用这些线程，
//   用于保证关键路径（如紧急 Compaction）有线程可用。
//
// 实现模式：pimpl（Pointer to Implementation）
//   头文件只暴露公共接口，具体实现隐藏在 Impl 结构体中，
//   便于未来替换底层实现而不影响头文件。

#pragma once

#include <functional>
#include <memory>

#include "rocksdb/env.h"
#include "rocksdb/threadpool.h"

namespace ROCKSDB_NAMESPACE {

// ThreadPoolImpl —— 线程池公共接口的具体实现类
//
// 继承自 ThreadPool（rocksdb/threadpool.h 中定义的纯虚接口）。
// 内部通过 pimpl 模式将实现细节封装在 Impl 结构体中。
class ThreadPoolImpl : public ThreadPool {
 public:
  ThreadPoolImpl();
  ~ThreadPoolImpl();

  // 禁止移动（线程资源不能被转移）
  ThreadPoolImpl(ThreadPoolImpl&&) = delete;
  ThreadPoolImpl& operator=(ThreadPoolImpl&&) = delete;

  // -----------------------------------------------------------------------
  // 线程生命周期管理
  // -----------------------------------------------------------------------

  // 等待所有线程退出
  // 行为：丢弃队列中尚未开始执行的任务，等待正在执行的任务完成后退出
  // 注意：调用后不能再提交新任务，直到重新 SetBackgroundThreads
  void JoinAllThreads() override;

  // 设置后台线程数量（允许增加或减少）
  // num: 目标线程数
  //   - 增加：立即启动新线程
  //   - 减少：多余线程在完成当前任务后按创建逆序自行退出
  void SetBackgroundThreads(int num) override;

  // 获取当前配置的后台线程数（非实际运行线程数）
  int GetBackgroundThreads() override;

  // 获取当前任务队列长度（原子读取，用于监控统计）
  unsigned int GetQueueLen() const override;

  // 等待所有排队和执行中的任务完成后，再等待所有线程退出
  // 与 JoinAllThreads() 的区别：会等待队列中未开始的任务也执行完毕
  void WaitForJobsAndJoinAllThreads() override;

  // -----------------------------------------------------------------------
  // 线程优先级调整（仅 Linux 有效）
  // -----------------------------------------------------------------------

  // 降低线程的内核 I/O 优先级（使用 ioprio_set 系统调用设置 IDLE 级别）
  // 适用场景：BOTTOM 优先级的 Compaction，避免与前台 I/O 竞争
  void LowerIOPriority();

  // 降低线程的 CPU 调度优先级
  // pri: 目标 CPU 优先级（如 CpuPriority::kIdle）
  void LowerCPUPriority(CpuPriority pri);

  // -----------------------------------------------------------------------
  // 线程数动态扩容
  // -----------------------------------------------------------------------

  // 确保线程池中至少有 num 个线程，但不减少现有线程数
  // 与 SetBackgroundThreads 的区别：只扩容，不缩容
  void IncBackgroundThreadsIfNeeded(int num);

  // -----------------------------------------------------------------------
  // 任务提交接口
  // -----------------------------------------------------------------------

  // 提交一个 fire-and-forget 任务（无法取消）
  // 拷贝版本：复制 job 函数对象后提交
  void SubmitJob(const std::function<void()>&) override;

  // 提交一个 fire-and-forget 任务（无法取消）
  // 移动版本：转移 job 函数对象所有权（更高效）
  void SubmitJob(std::function<void()>&&) override;

  // 提交一个带标签的可取消任务
  // function:        任务函数（C 函数指针形式）
  // arg:             传递给 function 的参数
  // tag:             任务标签，用于 UnSchedule() 批量取消同标签任务
  // unschedFunction: 任务被取消时的清理回调（可为 nullptr）
  //                  注意：unschedFunction 在互斥锁外执行，避免死锁
  void Schedule(void (*function)(void* arg1), void* arg, void* tag,
                void (*unschedFunction)(void* arg));

  // 取消队列中所有匹配指定标签的待执行任务
  // 注意：只能取消尚未开始执行的任务，正在执行的任务不受影响
  // tag:    要取消的任务标签
  // 返回：  成功取消的任务数量
  int UnSchedule(void* tag);

  // -----------------------------------------------------------------------
  // 环境与优先级配置
  // -----------------------------------------------------------------------

  // 设置宿主环境（用于线程状态注册等）
  void SetHostEnv(Env* env);

  // 获取宿主环境
  Env* GetHostEnv() const;

  // 获取本线程池的任务优先级（HIGH/LOW/BOTTOM/USER）
  // 后台线程可通过此方法查询自己的优先级类型
  Env::Priority GetThreadPriority() const;

  // 设置本线程池的任务优先级
  void SetThreadPriority(Env::Priority priority);

  // -----------------------------------------------------------------------
  // 线程预留机制
  // -----------------------------------------------------------------------

  // 预留指定数量的线程，使其不执行普通任务
  // 实际预留数可能少于请求数（取决于当前空闲线程数）
  // threads_to_be_reserved: 期望预留的线程数
  // 返回：实际成功预留的线程数
  int ReserveThreads(int threads_to_be_reserved) override;

  // 释放之前预留的线程，使其可重新执行普通任务
  // threads_to_be_released: 期望释放的线程数
  // 返回：实际成功释放的线程数
  int ReleaseThreads(int threads_to_be_released) override;

  // pthread 调用的错误检查辅助函数
  // label:  操作名称（用于错误日志）
  // result: pthread 函数的返回值（0 表示成功）
  // 若 result != 0，打印错误信息并调用 abort()
  static void PthreadCall(const char* label, int result);

  // pimpl 实现结构体（前向声明，具体定义在 .cc 文件中）
  struct Impl;

 private:
  // pimpl 指针：将实现细节隐藏在独立的 Impl 结构体中
  // 优点：
  //   1. 减少头文件依赖（不需要暴露 std::deque、std::condition_variable 等）
  //   2. 便于未来替换实现（如改用不同的线程库）
  //   3. ABI 稳定性（实现变化不影响头文件）
  std::unique_ptr<Impl> impl_;
};

}  // namespace ROCKSDB_NAMESPACE
