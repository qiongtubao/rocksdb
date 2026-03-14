//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// ThreadPoolImpl 实现文件
//
// 本文件包含线程池的核心实现：
//   - Impl 结构体：封装所有内部状态（互斥锁、条件变量、任务队列、线程列表）
//   - BGThread()：后台线程主循环
//   - Submit()：任务入队
//   - StartBGThreads()：启动后台线程
//   - JoinThreads()：等待线程退出
//   - UnSchedule()：取消待执行任务
//
// 并发控制机制：
//   - std::mutex mu_：保护任务队列、线程列表及所有状态变量
//   - std::condition_variable bgsignal_：用于后台线程的等待/唤醒
//   - std::atomic_uint queue_len_：无锁读取队列长度（统计用）
//
// 线程退出策略：
//   多余线程（bgthreads_.size() > total_threads_limit_）按创建逆序退出：
//   最后创建的线程负责 detach 自己并从 bgthreads_ 中移除，
//   若还有更多多余线程，则唤醒所有线程继续此过程。

#include "util/threadpool_imp.h"

#ifndef OS_WIN
#include <unistd.h>
#endif

#ifdef OS_LINUX
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// PthreadCall —— pthread API 错误检查辅助函数
// 若 result != 0，打印错误信息并 abort（不可恢复的系统错误）
void ThreadPoolImpl::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, errnoStr(result).c_str());
    abort();
  }
}

// ============================================================
// ThreadPoolImpl::Impl —— 线程池的内部实现结构体
// ============================================================
struct ThreadPoolImpl::Impl {
  Impl();
  ~Impl();

  // 线程退出逻辑
  // wait_for_jobs_to_complete: true  → 等待所有排队任务执行完毕再退出
  //                            false → 丢弃未执行任务，立即退出
  void JoinThreads(bool wait_for_jobs_to_complete);

  // 设置后台线程数（内部实现）
  // num:          目标线程数
  // allow_reduce: true → 允许减少线程数；false → 只能增加（不缩减）
  void SetBackgroundThreadsInternal(int num, bool allow_reduce);

  // 获取当前配置的线程数上限
  int GetBackgroundThreads();

  // 获取当前任务队列长度（原子 relaxed 读，用于统计）
  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed);
  }

  // 降低线程 I/O 优先级（需在 mu_ 保护下修改 low_io_priority_ 标志）
  void LowerIOPriority();

  // 降低线程 CPU 优先级（需在 mu_ 保护下修改 cpu_priority_）
  void LowerCPUPriority(CpuPriority pri);

  // 唤醒所有等待中的后台线程
  // 使用 notify_all() 通知全部等待线程，用于：
  //   - 有新任务入队时（有多余线程时需唤醒全部，确保非退出线程被唤醒）
  //   - 调整线程数时（需要多余线程自行退出）
  //   - 释放预留线程时
  void WakeUpAllThreads() { bgsignal_.notify_all(); }

  // 后台线程主循环函数
  // 每个后台线程都执行此函数，循环从任务队列取任务并执行，
  // 直到收到退出信号（exit_all_threads_ == true）或自身是多余线程。
  // thread_id: 线程在 bgthreads_ 中的索引（用于判断是否为多余线程）
  void BGThread(size_t thread_id);

  // 启动后台线程（若当前线程数不足 total_threads_limit_）
  // 在持有 mu_ 的情况下调用
  // 为每个新线程设置名称（格式："rocksdb:low"、"rocksdb:high" 等）
  void StartBGThreads();

  // 提交任务到线程池（核心入队函数）
  // schedule:    要执行的任务函数（移动语义）
  // unschedule:  取消时的清理函数（移动语义，可为空）
  // tag:         任务标签（用于 UnSchedule 批量取消）
  void Submit(std::function<void()>&& schedule,
              std::function<void()>&& unschedule, void* tag);

  // 取消队列中所有匹配 arg 标签的任务
  // 返回：成功取消的任务数
  int UnSchedule(void* arg);

  void SetHostEnv(Env* env) { env_ = env; }
  Env* GetHostEnv() const { return env_; }

  // 判断当前是否有多余线程（实际线程数 > 配置上限）
  bool HasExcessiveThread() const {
    return static_cast<int>(bgthreads_.size()) > total_threads_limit_;
  }

  // 判断指定线程是否是"最后一个多余线程"（需要优先退出的线程）
  // 策略：总是让最后创建的多余线程先退出（LIFO 顺序）
  // 排除 exit_all_threads_ 时的情况（此时由 JoinThreads 统一处理）
  bool IsLastExcessiveThread(size_t thread_id) const {
    return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
  }

  // 判断指定线程是否为多余线程（thread_id >= total_threads_limit_）
  bool IsExcessiveThread(size_t thread_id) const {
    return static_cast<int>(thread_id) >= total_threads_limit_;
  }

  // 获取/设置线程优先级
  Env::Priority GetThreadPriority() const { return priority_; }
  void SetThreadPriority(Env::Priority priority) { priority_ = priority; }

  // 预留指定数量的线程（阻止这些线程执行普通任务）
  // 实际预留数 = min(可预留数, 请求数)
  // 可预留数 = max(0, num_waiting_threads_ - reserved_threads_)
  // 需要持有 mu_
  int ReserveThreads(int threads_to_be_reserved) {
    std::unique_lock<std::mutex> lock(mu_);
    // 可预留的线程数不超过当前空闲等待线程数中未被预留的部分
    int reserved_threads_in_success =
        std::min(std::max(num_waiting_threads_ - reserved_threads_, 0),
                 threads_to_be_reserved);
    reserved_threads_ += reserved_threads_in_success;
    return reserved_threads_in_success;
  }

  // 释放之前预留的线程，使其可重新执行普通任务
  // 释放后唤醒所有等待线程，让被预留的线程重新参与任务调度
  int ReleaseThreads(int threads_to_be_released) {
    std::unique_lock<std::mutex> lock(mu_);
    // 不能释放超过已预留数量的线程
    int released_threads_in_success =
        std::min(reserved_threads_, threads_to_be_released);
    reserved_threads_ -= released_threads_in_success;
    // 唤醒所有等待线程，让被预留的线程重新检查调度条件
    WakeUpAllThreads();
    return released_threads_in_success;
  }

 private:
  // BGThreadWrapper —— 后台线程的入口函数（静态，符合 pthread 接口）
  // arg: 指向 BGThreadMetadata 的指针（含线程池指针和线程 ID）
  static void BGThreadWrapper(void* arg);

  bool low_io_priority_;          // 是否已降低 I/O 优先级
  CpuPriority cpu_priority_;      // 当前 CPU 优先级设置
  Env::Priority priority_;        // 线程池优先级（HIGH/LOW/BOTTOM/USER）
  Env* env_;                      // 宿主环境（用于线程状态注册）

  int total_threads_limit_;       // 配置的最大线程数上限
  std::atomic_uint queue_len_;    // 任务队列长度（原子，用于无锁统计读取）

  // 当前预留的线程数（由 ReserveThreads/ReleaseThreads 管理）
  // 当 num_waiting_threads_ <= reserved_threads_ 时，
  // 等待中的线程不会被普通任务唤醒（保留给预留用途）
  int reserved_threads_;

  // 当前处于等待状态的线程数（即可被预留的最大线程数）
  // 在极少数情况下（SetBackgroundThreadInternal 或多余线程退出时），
  // 可能小于 reserved_threads_
  int num_waiting_threads_;

  bool exit_all_threads_;           // 全局退出标志（JoinThreads 时设置）
  bool wait_for_jobs_to_complete_;  // 是否等待所有任务完成后再退出

  // BGItem —— 任务队列中的单个任务项
  struct BGItem {
    void* tag = nullptr;                      // 任务标签（用于 UnSchedule）
    std::function<void()> function;           // 要执行的任务函数
    std::function<void()> unschedFunction;    // 取消时的清理回调
  };

  using BGQueue = std::deque<BGItem>;
  BGQueue queue_;  // 任务队列（FIFO，受 mu_ 保护）

  std::mutex mu_;                          // 保护所有状态的互斥锁
  std::condition_variable bgsignal_;       // 后台线程等待/唤醒的条件变量
  std::vector<port::Thread> bgthreads_;    // 所有后台线程的句柄列表
};

// ============================================================
// Impl 构造/析构
// ============================================================

inline ThreadPoolImpl::Impl::Impl()
    : low_io_priority_(false),
      cpu_priority_(CpuPriority::kNormal),
      priority_(Env::LOW),
      env_(nullptr),
      total_threads_limit_(0),
      queue_len_(),
      reserved_threads_(0),
      num_waiting_threads_(0),
      exit_all_threads_(false),
      wait_for_jobs_to_complete_(false),
      queue_(),
      mu_(),
      bgsignal_(),
      bgthreads_() {}

// 析构时断言所有线程已退出（调用者必须先调用 JoinThreads）
inline ThreadPoolImpl::Impl::~Impl() { assert(bgthreads_.size() == 0U); }

// ============================================================
// JoinThreads —— 等待所有后台线程退出
// ============================================================

void ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete) {
  std::unique_lock<std::mutex> lock(mu_);
  assert(!exit_all_threads_);

  wait_for_jobs_to_complete_ = wait_for_jobs_to_complete;
  exit_all_threads_ = true;

  // 将线程限制清零，防止在 join 过程中用户并发提交任务时重新创建线程
  total_threads_limit_ = 0;
  reserved_threads_ = 0;
  num_waiting_threads_ = 0;

  lock.unlock();

  // 唤醒所有等待线程，让它们检查 exit_all_threads_ 标志并退出
  bgsignal_.notify_all();

  // 等待所有线程完成
  for (auto& th : bgthreads_) {
    th.join();
  }

  bgthreads_.clear();

  // 重置退出标志，允许后续重新使用线程池
  exit_all_threads_ = false;
  wait_for_jobs_to_complete_ = false;
}

// ============================================================
// 优先级调整
// ============================================================

// 降低 I/O 优先级（加锁修改标志，BGThread 在下次循环时生效）
inline void ThreadPoolImpl::Impl::LowerIOPriority() {
  std::lock_guard<std::mutex> lock(mu_);
  low_io_priority_ = true;
}

// 降低 CPU 优先级（加锁修改，BGThread 在下次循环时生效）
inline void ThreadPoolImpl::Impl::LowerCPUPriority(CpuPriority pri) {
  std::lock_guard<std::mutex> lock(mu_);
  cpu_priority_ = pri;
}

// ============================================================
// BGThread —— 后台线程主循环
// ============================================================

void ThreadPoolImpl::Impl::BGThread(size_t thread_id) {
  bool low_io_priority = false;                         // 当前线程的 I/O 优先级状态
  CpuPriority current_cpu_priority = CpuPriority::kNormal; // 当前线程的 CPU 优先级状态

  while (true) {
    // 加锁，进入等待循环
    std::unique_lock<std::mutex> lock(mu_);

    // 线程进入等待状态，递增等待线程计数
    num_waiting_threads_++;

    TEST_SYNC_POINT("ThreadPoolImpl::BGThread::WaitingThreadsInc");
    TEST_IDX_SYNC_POINT("ThreadPoolImpl::BGThread::Start:th", thread_id);

    // 等待条件：线程阻塞，直到满足以下任一条件时退出等待：
    //   1. exit_all_threads_ == true：收到全局退出信号
    //   2. IsLastExcessiveThread(thread_id)：自己是最后一个多余线程，需要退出
    //   3. 队列非空 && 自己不是多余线程 && 有足够空闲线程（未被完全预留）
    //      即：!queue_.empty() && !IsExcessiveThread(thread_id)
    //          && num_waiting_threads_ > reserved_threads_
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id) ||
            num_waiting_threads_ <= reserved_threads_)) {
      bgsignal_.wait(lock);  // 释放锁并阻塞，被唤醒后重新获取锁
    }

    // 退出等待，递减等待线程计数
    num_waiting_threads_--;

    if (exit_all_threads_) {
      // 收到全局退出信号
      if (!wait_for_jobs_to_complete_ || queue_.empty()) {
        // 不需要等待任务完成，或者队列已空 → 直接退出
        break;
      }
      // 否则继续执行队列中剩余的任务
    } else if (IsLastExcessiveThread(thread_id)) {
      // 当前线程是最后一个多余线程，需要自行退出（缩减线程数时触发）
      // 注意：exit_all_threads_ == false 时才走此分支，
      // 避免与 JoinThreads() 的 join() 调用冲突（不能 join 一个 detach 的线程）
      auto& terminating_thread = bgthreads_.back();
      terminating_thread.detach();  // 分离线程，允许其自然退出
      bgthreads_.pop_back();         // 从线程列表中移除

      if (HasExcessiveThread()) {
        // 仍有多余线程需要退出，唤醒所有线程继续处理
        WakeUpAllThreads();
      }
      TEST_IDX_SYNC_POINT("ThreadPoolImpl::BGThread::Termination:th",
                          thread_id);
      TEST_SYNC_POINT("ThreadPoolImpl::BGThread::Termination");
      break;  // 当前线程退出主循环
    }

    // 从队列头部取出一个任务（FIFO）
    auto func = std::move(queue_.front().function);
    queue_.pop_front();

    // 更新队列长度（原子写，relaxed 顺序，供统计用）
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    // 检查是否需要调整优先级（持锁期间读取最新配置）
    bool decrease_io_priority = (low_io_priority != low_io_priority_);
    CpuPriority cpu_priority = cpu_priority_;
    lock.unlock();  // 释放锁，执行任务期间不持锁

    // 若需要降低 CPU 优先级，调用系统调用调整（仅降级，不升级）
    if (cpu_priority < current_cpu_priority) {
      TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::BGThread::BeforeSetCpuPriority",
                               &current_cpu_priority);
      // 参数 0 表示当前线程
      port::SetCpuPriority(0, cpu_priority);
      current_cpu_priority = cpu_priority;
      TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::BGThread::AfterSetCpuPriority",
                               &current_cpu_priority);
    }

#ifdef OS_LINUX
    if (decrease_io_priority) {
      // 通过 ioprio_set 系统调用将当前线程的 I/O 调度类设置为 IDLE（最低优先级）
      // 效果仅在使用支持 I/O 优先级的调度器（如 CFQ）时生效
      // 可通过以下命令切换 I/O 调度器：
      //   echo cfq > /sys/block/<device>/queue/scheduler
#define IOPRIO_CLASS_SHIFT (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
      syscall(SYS_ioprio_set, 1,  // IOPRIO_WHO_PROCESS（按进程/线程设置）
              0,                  // 0 表示当前线程
              IOPRIO_PRIO_VALUE(3, 0));  // 3 = IOPRIO_CLASS_IDLE（最低 I/O 优先级）
      low_io_priority = true;
    }
#else
    (void)decrease_io_priority;  // 非 Linux 平台，抑制"未使用变量"警告
#endif

    TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::Impl::BGThread:BeforeRun",
                             &priority_);

    // 执行任务（不持锁，允许任务内部提交新任务）
    func();
  }
}

// BGThreadMetadata —— 创建后台线程时传递参数的辅助结构
// 通过 BGThreadWrapper 传递到新线程，使其能找到所属线程池和自己的 ID
struct BGThreadMetadata {
  ThreadPoolImpl::Impl* thread_pool_;  // 所属线程池
  size_t thread_id_;                   // 该线程在 bgthreads_ 中的索引
  BGThreadMetadata(ThreadPoolImpl::Impl* thread_pool, size_t thread_id)
      : thread_pool_(thread_pool), thread_id_(thread_id) {}
};

// BGThreadWrapper —— 后台线程的静态入口函数
// 负责：
//   1. 从 BGThreadMetadata 中提取参数
//   2. 注册线程状态（用于监控）
//   3. 调用 BGThread() 主循环
//   4. 线程退出时注销线程状态
void ThreadPoolImpl::Impl::BGThreadWrapper(void* arg) {
  BGThreadMetadata* meta = reinterpret_cast<BGThreadMetadata*>(arg);
  size_t thread_id = meta->thread_id_;
  ThreadPoolImpl::Impl* tp = meta->thread_pool_;
#ifdef ROCKSDB_USING_THREAD_STATUS
  // 根据线程池优先级，将线程注册到线程状态监控系统
  // 初始化为 NUM_THREAD_TYPES 以便编译器检测到任何遗漏的 case
  ThreadStatus::ThreadType thread_type = ThreadStatus::NUM_THREAD_TYPES;
  switch (tp->GetThreadPriority()) {
    case Env::Priority::HIGH:
      thread_type = ThreadStatus::HIGH_PRIORITY;
      break;
    case Env::Priority::LOW:
      thread_type = ThreadStatus::LOW_PRIORITY;
      break;
    case Env::Priority::BOTTOM:
      thread_type = ThreadStatus::BOTTOM_PRIORITY;
      break;
    case Env::Priority::USER:
      thread_type = ThreadStatus::USER;
      break;
    case Env::Priority::TOTAL:
      assert(false);
      return;
  }
  assert(thread_type != ThreadStatus::NUM_THREAD_TYPES);
  ThreadStatusUtil::RegisterThread(tp->GetHostEnv(), thread_type);
#endif
  delete meta;          // BGThreadMetadata 已不再需要，释放内存
  tp->BGThread(thread_id);  // 进入主循环
#ifdef ROCKSDB_USING_THREAD_STATUS
  ThreadStatusUtil::UnregisterThread();  // 线程退出时注销监控状态
#endif
  return;
}

// ============================================================
// SetBackgroundThreadsInternal —— 设置线程数（内部实现）
// ============================================================

void ThreadPoolImpl::Impl::SetBackgroundThreadsInternal(int num,
                                                        bool allow_reduce) {
  std::lock_guard<std::mutex> lock(mu_);
  if (exit_all_threads_) {
    return;  // 正在退出时，不允许调整线程数
  }
  if (num > total_threads_limit_ ||
      (num < total_threads_limit_ && allow_reduce)) {
    total_threads_limit_ = std::max(0, num);
    WakeUpAllThreads();   // 唤醒所有线程：多余的线程会自行退出
    StartBGThreads();     // 不足的线程会被补充创建
  }
}

int ThreadPoolImpl::Impl::GetBackgroundThreads() {
  std::unique_lock<std::mutex> lock(mu_);
  return total_threads_limit_;
}

// ============================================================
// StartBGThreads —— 启动后台线程（需持有 mu_）
// ============================================================

void ThreadPoolImpl::Impl::StartBGThreads() {
  // 循环直到线程数达到上限
  while ((int)bgthreads_.size() < total_threads_limit_) {
    // 创建新线程，传入线程池指针和线程 ID（即当前 bgthreads_ 的大小）
    port::Thread p_t(&BGThreadWrapper,
                     new BGThreadMetadata(this, bgthreads_.size()));

// 设置线程名称（仅在 GNU C Library 2.12+ 上支持）
// 格式："rocksdb:<priority>"，例如 "rocksdb:high"、"rocksdb:low"
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    auto th_handle = p_t.native_handle();
    std::string thread_priority = Env::PriorityToString(GetThreadPriority());
    std::ostringstream thread_name_stream;
    thread_name_stream << "rocksdb:";
    for (char c : thread_priority) {
      thread_name_stream << static_cast<char>(tolower(c));
    }
    pthread_setname_np(th_handle, thread_name_stream.str().c_str());
#endif
#endif
    bgthreads_.push_back(std::move(p_t));
  }
}

// ============================================================
// Submit —— 任务入队（核心提交逻辑）
// ============================================================

void ThreadPoolImpl::Impl::Submit(std::function<void()>&& schedule,
                                  std::function<void()>&& unschedule,
                                  void* tag) {
  std::lock_guard<std::mutex> lock(mu_);

  if (exit_all_threads_) {
    return;  // 线程池正在退出，拒绝新任务
  }

  // 若线程数不足，先补充线程
  StartBGThreads();

  // 将新任务追加到队列尾部（FIFO）
  queue_.push_back(BGItem());

  TEST_SYNC_POINT("ThreadPoolImpl::Submit::Enqueue");

  auto& item = queue_.back();
  item.tag = tag;                           // 任务标签（用于 UnSchedule）
  item.function = std::move(schedule);       // 任务函数（移动所有权）
  item.unschedFunction = std::move(unschedule); // 取消回调（移动所有权）

  // 更新原子队列长度（供无锁统计读取）
  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);

  if (!HasExcessiveThread()) {
    // 线程数正常：唤醒一个等待线程即可（避免惊群效应）
    bgsignal_.notify_one();
  } else {
    // 有多余线程时：notify_one 可能唤醒需要退出的多余线程
    // 改用 notify_all 确保至少有一个正常线程被唤醒来处理新任务
    WakeUpAllThreads();
  }
}

// ============================================================
// UnSchedule —— 取消指定标签的待执行任务
// ============================================================

int ThreadPoolImpl::Impl::UnSchedule(void* arg) {
  int count = 0;

  // 在锁内从队列中移除匹配的任务，收集其 unschedFunction
  std::vector<std::function<void()>> candidates;
  {
    std::lock_guard<std::mutex> lock(mu_);

    BGQueue::iterator it = queue_.begin();
    while (it != queue_.end()) {
      if (arg == (*it).tag) {
        // 找到匹配标签的任务
        if (it->unschedFunction) {
          candidates.push_back(std::move(it->unschedFunction));
        }
        it = queue_.erase(it);
        count++;
      } else {
        ++it;
      }
    }
    // 更新队列长度统计
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);
  }

  // 在锁外执行 unschedFunction，避免在锁内回调导致死锁
  for (auto& f : candidates) {
    f();
  }

  return count;
}

// ============================================================
// ThreadPoolImpl 公共接口实现（委托给 Impl）
// ============================================================

ThreadPoolImpl::ThreadPoolImpl() : impl_(new Impl()) {}

ThreadPoolImpl::~ThreadPoolImpl() {}

// 等待所有线程退出（丢弃未执行任务）
void ThreadPoolImpl::JoinAllThreads() { impl_->JoinThreads(false); }

// 设置后台线程数（允许增减）
void ThreadPoolImpl::SetBackgroundThreads(int num) {
  impl_->SetBackgroundThreadsInternal(num, true);
}

int ThreadPoolImpl::GetBackgroundThreads() {
  return impl_->GetBackgroundThreads();
}

unsigned int ThreadPoolImpl::GetQueueLen() const {
  return impl_->GetQueueLen();
}

// 等待所有任务（含排队中的）完成后再退出
void ThreadPoolImpl::WaitForJobsAndJoinAllThreads() {
  impl_->JoinThreads(true);
}

void ThreadPoolImpl::LowerIOPriority() { impl_->LowerIOPriority(); }

void ThreadPoolImpl::LowerCPUPriority(CpuPriority pri) {
  impl_->LowerCPUPriority(pri);
}

// 仅扩容，不缩减线程数
void ThreadPoolImpl::IncBackgroundThreadsIfNeeded(int num) {
  impl_->SetBackgroundThreadsInternal(num, false);
}

// 提交任务（拷贝版本：复制 job 后提交，无标签，不可取消）
void ThreadPoolImpl::SubmitJob(const std::function<void()>& job) {
  auto copy(job);
  impl_->Submit(std::move(copy), std::function<void()>(), nullptr);
}

// 提交任务（移动版本：转移 job 所有权，更高效，无标签，不可取消）
void ThreadPoolImpl::SubmitJob(std::function<void()>&& job) {
  impl_->Submit(std::move(job), std::function<void()>(), nullptr);
}

// 提交带标签的可取消任务（C 函数指针形式）
void ThreadPoolImpl::Schedule(void (*function)(void* arg1), void* arg,
                              void* tag, void (*unschedFunction)(void* arg)) {
  if (unschedFunction == nullptr) {
    impl_->Submit(std::bind(function, arg), std::function<void()>(), tag);
  } else {
    impl_->Submit(std::bind(function, arg), std::bind(unschedFunction, arg),
                  tag);
  }
}

int ThreadPoolImpl::UnSchedule(void* arg) { return impl_->UnSchedule(arg); }

void ThreadPoolImpl::SetHostEnv(Env* env) { impl_->SetHostEnv(env); }

Env* ThreadPoolImpl::GetHostEnv() const { return impl_->GetHostEnv(); }

Env::Priority ThreadPoolImpl::GetThreadPriority() const {
  return impl_->GetThreadPriority();
}

void ThreadPoolImpl::SetThreadPriority(Env::Priority priority) {
  impl_->SetThreadPriority(priority);
}

int ThreadPoolImpl::ReserveThreads(int threads_to_be_reserved) {
  return impl_->ReserveThreads(threads_to_be_reserved);
}

int ThreadPoolImpl::ReleaseThreads(int threads_to_be_released) {
  return impl_->ReleaseThreads(threads_to_be_released);
}

// ============================================================
// NewThreadPool —— 工厂函数：创建并初始化一个新的线程池
// ============================================================

// 创建一个拥有 num_threads 个线程的线程池
// 返回：ThreadPool* 接口指针（调用者负责管理生命周期）
ThreadPool* NewThreadPool(int num_threads) {
  ThreadPoolImpl* thread_pool = new ThreadPoolImpl();
  thread_pool->SetBackgroundThreads(num_threads);
  return thread_pool;
}

}  // namespace ROCKSDB_NAMESPACE
