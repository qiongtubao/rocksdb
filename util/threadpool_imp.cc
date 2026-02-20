//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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

void ThreadPoolImpl::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, errnoStr(result).c_str());
    abort();
  }
}

struct ThreadPoolImpl::Impl {
  Impl();
  ~Impl();

  void JoinThreads(bool wait_for_jobs_to_complete);

  void SetBackgroundThreadsInternal(int num, bool allow_reduce);
  int GetBackgroundThreads();

  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed);
  }

  void LowerIOPriority();

  void LowerCPUPriority(CpuPriority pri);

  // 唤醒所有等待中的后台线程
  // 使用条件变量的 notify_all 方法通知所有等待的线程
  // 通常用于任务队列有新任务、需要调整线程数或需要清理多余线程时
  void WakeUpAllThreads() { bgsignal_.notify_all(); }

  // 后台线程主函数
  // 每个后台线程都会执行这个函数，从任务队列中取出任务并执行
  // thread_id: 线程的唯一标识符，用于区分不同的后台线程
  void BGThread(size_t thread_id);

  // 启动后台线程
  // 根据当前设置的线程限制，启动所需数量的后台线程
  // 如果已经存在足够的线程，则不创建新线程
  void StartBGThreads();

  // 提交任务到线程池
  // schedule: 要执行的任务函数（右值引用，支持移动语义）
  // unschedule: 取消任务时调用的清理函数（右值引用）
  // tag: 任务的标签，用于标识和取消特定任务
  void Submit(std::function<void()>&& schedule,
              std::function<void()>&& unschedule, void* tag);

  int UnSchedule(void* arg);

  void SetHostEnv(Env* env) { env_ = env; }

  Env* GetHostEnv() const { return env_; }

  bool HasExcessiveThread() const {
    return static_cast<int>(bgthreads_.size()) > total_threads_limit_;
  }

  // Return true iff the current thread is the excessive thread to terminate.
  // Always terminate the running thread that is added last, even if there are
  // more than one thread to terminate.
  bool IsLastExcessiveThread(size_t thread_id) const {
    return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
  }

  bool IsExcessiveThread(size_t thread_id) const {
    return static_cast<int>(thread_id) >= total_threads_limit_;
  }

  // Return the thread priority.
  // This would allow its member-thread to know its priority.
  Env::Priority GetThreadPriority() const { return priority_; }

  // Set the thread priority.
  void SetThreadPriority(Env::Priority priority) { priority_ = priority; }

  int ReserveThreads(int threads_to_be_reserved) {
    std::unique_lock<std::mutex> lock(mu_);
    // We can reserve at most num_waiting_threads_ in total so the number of
    // threads that can be reserved might be fewer than the desired one. In
    // rare cases, num_waiting_threads_ could be less than reserved_threads
    // due to SetBackgroundThreadInternal or last excessive threads. If that
    // happens, we cannot reserve any other threads.
    int reserved_threads_in_success =
        std::min(std::max(num_waiting_threads_ - reserved_threads_, 0),
                 threads_to_be_reserved);
    reserved_threads_ += reserved_threads_in_success;
    return reserved_threads_in_success;
  }

  int ReleaseThreads(int threads_to_be_released) {
    std::unique_lock<std::mutex> lock(mu_);
    // We cannot release more than reserved_threads_
    int released_threads_in_success =
        std::min(reserved_threads_, threads_to_be_released);
    reserved_threads_ -= released_threads_in_success;
    WakeUpAllThreads();
    return released_threads_in_success;
  }

 private:
  static void BGThreadWrapper(void* arg);

  bool low_io_priority_;
  CpuPriority cpu_priority_;
  Env::Priority priority_;
  Env* env_;

  int total_threads_limit_;
  std::atomic_uint queue_len_;  // Queue length. Used for stats reporting
  // Number of reserved threads, managed by ReserveThreads(..) and
  // ReleaseThreads(..), if num_waiting_threads_ is no larger than
  // reserved_threads_, its thread will be blocked to ensure the reservation
  // mechanism
  int reserved_threads_;
  // Number of waiting threads (Maximum number of threads that can be
  // reserved), in rare cases, num_waiting_threads_ could be less than
  // reserved_threads due to SetBackgroundThreadInternal or last
  // excessive threads.
  int num_waiting_threads_;
  bool exit_all_threads_;
  bool wait_for_jobs_to_complete_;

  // Entry per Schedule()/Submit() call
  struct BGItem {
    void* tag = nullptr;
    std::function<void()> function;
    std::function<void()> unschedFunction;
  };

  using BGQueue = std::deque<BGItem>;
  BGQueue queue_;

  std::mutex mu_;
  std::condition_variable bgsignal_;
  std::vector<port::Thread> bgthreads_;
};

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

inline ThreadPoolImpl::Impl::~Impl() { assert(bgthreads_.size() == 0U); }

void ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete) {
  std::unique_lock<std::mutex> lock(mu_);
  assert(!exit_all_threads_);

  wait_for_jobs_to_complete_ = wait_for_jobs_to_complete;
  exit_all_threads_ = true;
  // prevent threads from being recreated right after they're joined, in case
  // the user is concurrently submitting jobs.
  total_threads_limit_ = 0;
  reserved_threads_ = 0;
  num_waiting_threads_ = 0;

  lock.unlock();

  bgsignal_.notify_all();

  for (auto& th : bgthreads_) {
    th.join();
  }

  bgthreads_.clear();

  exit_all_threads_ = false;
  wait_for_jobs_to_complete_ = false;
}

inline void ThreadPoolImpl::Impl::LowerIOPriority() {
  std::lock_guard<std::mutex> lock(mu_);
  low_io_priority_ = true;
}

inline void ThreadPoolImpl::Impl::LowerCPUPriority(CpuPriority pri) {
  std::lock_guard<std::mutex> lock(mu_);
  cpu_priority_ = pri;
}

void ThreadPoolImpl::Impl::BGThread(size_t thread_id) {
  bool low_io_priority = false;
  CpuPriority current_cpu_priority = CpuPriority::kNormal;

  while (true) {
    // Wait until there is an item that is ready to run
    std::unique_lock<std::mutex> lock(mu_);
    // Stop waiting if the thread needs to do work or needs to terminate.
    // Increase num_waiting_threads_ once this task has started waiting
    num_waiting_threads_++;

    TEST_SYNC_POINT("ThreadPoolImpl::BGThread::WaitingThreadsInc");
    TEST_IDX_SYNC_POINT("ThreadPoolImpl::BGThread::Start:th", thread_id);
    // When not exist_all_threads and the current thread id is not the last
    // excessive thread, it may be blocked due to 3 reasons: 1) queue is empty
    // 2) it is the excessive thread (not the last one)
    // 3) the number of waiting threads is not greater than reserved threads
    // (i.e, no available threads due to full reservation")
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id) ||
            num_waiting_threads_ <= reserved_threads_)) {
      bgsignal_.wait(lock);
    }
    // Decrease num_waiting_threads_ once the thread is not waiting
    num_waiting_threads_--;

    if (exit_all_threads_) {  // mechanism to let BG threads exit safely

      if (!wait_for_jobs_to_complete_ || queue_.empty()) {
        break;
      }
    } else if (IsLastExcessiveThread(thread_id)) {
      // Current thread is the last generated one and is excessive.
      // We always terminate excessive thread in the reverse order of
      // generation time. But not when `exit_all_threads_ == true`,
      // otherwise `JoinThreads()` could try to `join()` a `detach()`ed
      // thread.
      auto& terminating_thread = bgthreads_.back();
      terminating_thread.detach();
      bgthreads_.pop_back();
      if (HasExcessiveThread()) {
        // There is still at least more excessive thread to terminate.
        WakeUpAllThreads();
      }
      TEST_IDX_SYNC_POINT("ThreadPoolImpl::BGThread::Termination:th",
                          thread_id);
      TEST_SYNC_POINT("ThreadPoolImpl::BGThread::Termination");
      break;
    }

    auto func = std::move(queue_.front().function);
    queue_.pop_front();

    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    bool decrease_io_priority = (low_io_priority != low_io_priority_);
    CpuPriority cpu_priority = cpu_priority_;
    lock.unlock();

    if (cpu_priority < current_cpu_priority) {
      TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::BGThread::BeforeSetCpuPriority",
                               &current_cpu_priority);
      // 0 means current thread.
      port::SetCpuPriority(0, cpu_priority);
      current_cpu_priority = cpu_priority;
      TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::BGThread::AfterSetCpuPriority",
                               &current_cpu_priority);
    }

#ifdef OS_LINUX
    if (decrease_io_priority) {
#define IOPRIO_CLASS_SHIFT (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
      // Put schedule into IOPRIO_CLASS_IDLE class (lowest)
      // These system calls only have an effect when used in conjunction
      // with an I/O scheduler that supports I/O priorities. As at
      // kernel 2.6.17 the only such scheduler is the Completely
      // Fair Queuing (CFQ) I/O scheduler.
      // To change scheduler:
      //  echo cfq > /sys/block/<device_name>/queue/schedule
      // Tunables to consider:
      //  /sys/block/<device_name>/queue/slice_idle
      //  /sys/block/<device_name>/queue/slice_sync
      syscall(SYS_ioprio_set, 1,  // IOPRIO_WHO_PROCESS
              0,                  // current thread
              IOPRIO_PRIO_VALUE(3, 0));
      low_io_priority = true;
    }
#else
    (void)decrease_io_priority;  // avoid 'unused variable' error
#endif

    TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::Impl::BGThread:BeforeRun",
                             &priority_);

    func();
  }
}

// Helper struct for passing arguments when creating threads.
struct BGThreadMetadata {
  ThreadPoolImpl::Impl* thread_pool_;
  size_t thread_id_;  // Thread count in the thread.
  BGThreadMetadata(ThreadPoolImpl::Impl* thread_pool, size_t thread_id)
      : thread_pool_(thread_pool), thread_id_(thread_id) {}
};

void ThreadPoolImpl::Impl::BGThreadWrapper(void* arg) {
  BGThreadMetadata* meta = reinterpret_cast<BGThreadMetadata*>(arg);
  size_t thread_id = meta->thread_id_;
  ThreadPoolImpl::Impl* tp = meta->thread_pool_;
#ifdef ROCKSDB_USING_THREAD_STATUS
  // initialize it because compiler isn't good enough to see we don't use it
  // uninitialized
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
  delete meta;
  tp->BGThread(thread_id);
#ifdef ROCKSDB_USING_THREAD_STATUS
  ThreadStatusUtil::UnregisterThread();
#endif
  return;
}

void ThreadPoolImpl::Impl::SetBackgroundThreadsInternal(int num,
                                                        bool allow_reduce) {
  std::lock_guard<std::mutex> lock(mu_);
  if (exit_all_threads_) {
    return;
  }
  if (num > total_threads_limit_ ||
      (num < total_threads_limit_ && allow_reduce)) {
    total_threads_limit_ = std::max(0, num);
    WakeUpAllThreads();
    StartBGThreads();
  }
}

int ThreadPoolImpl::Impl::GetBackgroundThreads() {
  std::unique_lock<std::mutex> lock(mu_);
  return total_threads_limit_;
}

void ThreadPoolImpl::Impl::StartBGThreads() {
  // 启动后台线程（如果必要）
  // 循环直到当前线程数量达到或超过线程限制
  while ((int)bgthreads_.size() < total_threads_limit_) {
    // 创建新的后台线程
    // 参数：线程函数包装器、线程元数据（包含线程池指针和线程 ID）
    port::Thread p_t(&BGThreadWrapper,
                     new BGThreadMetadata(this, bgthreads_.size()));

// 设置线程名称以辅助调试
// 仅在使用 GNU C Library 2.12 或更高版本时支持
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    // 获取线程的底层原生句柄
    auto th_handle = p_t.native_handle();

    // 将线程优先级枚举值转换为字符串
    std::string thread_priority = Env::PriorityToString(GetThreadPriority());

    // 构建线程名称
    // 格式为 "rocksdb:" 后跟小写的优先级名称（如 "rocksdb:high"）
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

void ThreadPoolImpl::Impl::Submit(std::function<void()>&& schedule,
                                  std::function<void()>&& unschedule,
                                  void* tag) {
  std::lock_guard<std::mutex> lock(mu_);

  if (exit_all_threads_) {
    return;
  }

  StartBGThreads();

  // 将任务添加到任务队列
  queue_.push_back(BGItem());

  // 测试同步点：在任务入队后
  TEST_SYNC_POINT("ThreadPoolImpl::Submit::Enqueue");

  // 获取刚添加的任务引用
  auto& item = queue_.back();

  // 设置任务标签（用于识别和取消任务）
  item.tag = tag;

  // 设置要执行的任务函数（使用移动语义转移所有权）
  item.function = std::move(schedule);

  // 设置取消任务时的清理函数（使用移动语义）
  item.unschedFunction = std::move(unschedule);

  // 更新队列长度（原子操作，用于统计）
  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);

  // 如果没有多余的线程（线程数量未超过限制）
  if (!HasExcessiveThread()) {
    // 唤醒至少一个等待的线程来执行新任务
    bgsignal_.notify_one();
  } else {
    // 需要唤醒所有线程，确保被唤醒的不是需要终止的线程
    // 因为有多余线程，有些线程可能需要退出，需要确保有非退出的线程被唤醒
    WakeUpAllThreads();
  }
}

int ThreadPoolImpl::Impl::UnSchedule(void* arg) {
  int count = 0;

  std::vector<std::function<void()>> candidates;
  {
    std::lock_guard<std::mutex> lock(mu_);

    // Remove from priority queue
    BGQueue::iterator it = queue_.begin();
    while (it != queue_.end()) {
      if (arg == (*it).tag) {
        if (it->unschedFunction) {
          candidates.push_back(std::move(it->unschedFunction));
        }
        it = queue_.erase(it);
        count++;
      } else {
        ++it;
      }
    }
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);
  }

  // Run unschedule functions outside the mutex
  for (auto& f : candidates) {
    f();
  }

  return count;
}

ThreadPoolImpl::ThreadPoolImpl() : impl_(new Impl()) {}

ThreadPoolImpl::~ThreadPoolImpl() {}

void ThreadPoolImpl::JoinAllThreads() { impl_->JoinThreads(false); }

void ThreadPoolImpl::SetBackgroundThreads(int num) {
  impl_->SetBackgroundThreadsInternal(num, true);
}

int ThreadPoolImpl::GetBackgroundThreads() {
  return impl_->GetBackgroundThreads();
}

unsigned int ThreadPoolImpl::GetQueueLen() const {
  return impl_->GetQueueLen();
}

void ThreadPoolImpl::WaitForJobsAndJoinAllThreads() {
  impl_->JoinThreads(true);
}

void ThreadPoolImpl::LowerIOPriority() { impl_->LowerIOPriority(); }

void ThreadPoolImpl::LowerCPUPriority(CpuPriority pri) {
  impl_->LowerCPUPriority(pri);
}

void ThreadPoolImpl::IncBackgroundThreadsIfNeeded(int num) {
  impl_->SetBackgroundThreadsInternal(num, false);
}

void ThreadPoolImpl::SubmitJob(const std::function<void()>& job) {
  auto copy(job);
  impl_->Submit(std::move(copy), std::function<void()>(), nullptr);
}

void ThreadPoolImpl::SubmitJob(std::function<void()>&& job) {
  impl_->Submit(std::move(job), std::function<void()>(), nullptr);
}

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

// Return the thread priority.
// This would allow its member-thread to know its priority.
Env::Priority ThreadPoolImpl::GetThreadPriority() const {
  return impl_->GetThreadPriority();
}

// Set the thread priority.
void ThreadPoolImpl::SetThreadPriority(Env::Priority priority) {
  impl_->SetThreadPriority(priority);
}

// Reserve a specific number of threads, prevent them from running other
// functions The number of reserved threads could be fewer than the desired one
int ThreadPoolImpl::ReserveThreads(int threads_to_be_reserved) {
  return impl_->ReserveThreads(threads_to_be_reserved);
}

// Release a specific number of threads
int ThreadPoolImpl::ReleaseThreads(int threads_to_be_released) {
  return impl_->ReleaseThreads(threads_to_be_released);
}

ThreadPool* NewThreadPool(int num_threads) {
  ThreadPoolImpl* thread_pool = new ThreadPoolImpl();
  thread_pool->SetBackgroundThreads(num_threads);
  return thread_pool;
}

}  // namespace ROCKSDB_NAMESPACE
