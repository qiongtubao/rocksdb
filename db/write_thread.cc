//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_thread.h"

#include <chrono>
#include <thread>

#include "db/column_family.h"
#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

WriteThread::WriteThread(const ImmutableDBOptions& db_options)
    : max_yield_usec_(db_options.enable_write_thread_adaptive_yield
                          ? db_options.write_thread_max_yield_usec
                          : 0),
      slow_yield_usec_(db_options.write_thread_slow_yield_usec),
      allow_concurrent_memtable_write_(
          db_options.allow_concurrent_memtable_write),
      enable_pipelined_write_(db_options.enable_pipelined_write),
      max_write_batch_group_size_bytes(
          db_options.max_write_batch_group_size_bytes),
      newest_writer_(nullptr),
      newest_memtable_writer_(nullptr),
      last_sequence_(0),
      write_stall_dummy_(),
      stall_mu_(),
      stall_cv_(&stall_mu_) {}

uint8_t WriteThread::BlockingAwaitState(Writer* w, uint8_t goal_mask) {
  // We're going to block.  Lazily create the mutex.  We guarantee
  // propagation of this construction to the waker via the
  // STATE_LOCKED_WAITING state.  The waker won't try to touch the mutex
  // or the condvar unless they CAS away the STATE_LOCKED_WAITING that
  // we install below.
  w->CreateMutex();

  auto state = w->state.load(std::memory_order_acquire);
  assert(state != STATE_LOCKED_WAITING);
  if ((state & goal_mask) == 0 &&
      w->state.compare_exchange_strong(state, STATE_LOCKED_WAITING)) {
    // we have permission (and an obligation) to use StateMutex
    std::unique_lock<std::mutex> guard(w->StateMutex());
    w->StateCV().wait(guard, [w] {
      return w->state.load(std::memory_order_relaxed) != STATE_LOCKED_WAITING;
    });
    state = w->state.load(std::memory_order_relaxed);
  }
  // else tricky.  Goal is met or CAS failed.  In the latter case the waker
  // must have changed the state, and compare_exchange_strong has updated
  // our local variable with the new one.  At the moment WriteThread never
  // waits for a transition across intermediate states, so we know that
  // since a state change has occurred the goal must have been met.
  assert((state & goal_mask) != 0);
  return state;
}

// WriteThread::AwaitState - 等待 Writer 达到目标状态
//
// 功能概述：
// 这是 WriteThread 的核心等待函数，用于等待 Writer 达到指定的目标状态。
// 函数采用自适应的自旋-让出-阻塞策略，根据历史性能数据动态选择等待方式。
//
// 参数说明：
// - w: 要等待的 Writer 指针
// - goal_mask: 目标状态掩码，可以是以下状态的组合：
//   * STATE_GROUP_LEADER: 成为批处理组 leader
//   * STATE_MEMTABLE_WRITER_LEADER: 成为 MemTable writer 组 leader
//   * STATE_PARALLEL_MEMTABLE_WRITER: 成为并行 MemTable writer
//   * STATE_COMPLETED: 写入已完成
// - ctx: 自适应上下文，用于跟踪和调整等待策略
//
// 返回值：
// - Writer 的当前状态（必定满足 goal_mask 条件）
//
// 等待策略（三阶段）：
// 1. 忙等待（Busy Loop）: 使用 pause 指令自旋约 1 微秒
//    - 适合极短的等待，避免上下文切换开销
//    - pause 指令可以减少 CPU 流水线冲突
//
// 2. 让出自旋（Yield Loop）: 调用 std::this_thread::yield() 自旋最多 max_yield_usec_ 微秒
//    - 适合中等长度的等待（通常 < 10 微秒）
//    - yield 让出 CPU 时间片给其他线程
//    - 如果有足够空闲核心，yield 不会导致上下文切换
//
// 3. 阻塞等待（Blocking Wait）: 使用条件变量和互斥量阻塞
//    - 适合长时间等待
//    - 避免浪费 CPU 资源
//    - 但有较高的上下文切换开销（FUTEX 延迟通常 > 2.7 微秒）
//
// 自适应机制：
// yield_credit: 用于跟踪 yield 策略的有效性
// - 正值: yield 通常是有效的，会继续使用 yield 策略
// - 负值: yield 效果不佳，会更快地切换到阻塞等待
// - 指数衰减: 每次更新时，credit 乘以 1023/1024，使历史数据逐渐失效
// - 采样率: 默认为 1/256，即只有约 0.4% 的等待会更新 credit
//
// 性能优化：
// 现代 Xeon CPU 上，每个 pause 循环约 7 纳秒，200 次约 1 微秒
// 这样可以摊销访问时钟和 yield 调用的开销
//
// 分类决策：
// 将等待分为三类：
// 1. short-uncontended: 短等待且无竞争，应该自旋
// 2. short-contended: 短等待但有竞争，取决于优化目标（吞吐量 vs 资源公平）
// 3. long: 长等待，应该阻塞
//
// 判断慢 yield 的标准：
// - 如果 yield 耗时超过 slow_yield_usec_（默认 100 微秒），认为是慢 yield
// - 连续 3 次慢 yield 立即切换到阻塞等待
// - 这表明存在其他可运行线程，继续自旋会浪费 CPU
uint8_t WriteThread::AwaitState(Writer* w, uint8_t goal_mask,
                                AdaptationContext* ctx) {
  // 存储当前状态
  uint8_t state = 0;

  // ============================================================================
  // 等待策略说明：
  // 1. 忙等待：使用 "pause" 指令自旋约 1 微秒（200 次循环）
  // 2. 让出自旋：SOMETIMES 使用 "yield" 自旋约 100 微秒（可配置 max_yield_usec_）
  // 3. 阻塞等待：使用互斥量和条件变量阻塞
  // ============================================================================

  // ============================================================================
  // 阶段 1：忙等待（Busy Loop）
  // ============================================================================
  // 在现代 Xeon CPU 上，每次循环约 7 纳秒（主要是 pause 指令的开销）
  // 200 次迭代略多于 1 微秒。这个长度足够摊销访问时钟和 yield 的成本
  for (uint32_t tries = 0; tries < 200; ++tries) {
    // 原子地加载 Writer 的当前状态
    // memory_order_acquire: 确保后续操作能看到状态改变前的所有内存写入
    state = w->state.load(std::memory_order_acquire);
    if ((state & goal_mask) != 0) {
      // 目标状态已满足，立即返回
      return state;
    }
    // 执行 CPU pause 指令
    // 提示 CPU 当前线程在自旋等待，可以优化流水线性能
    port::AsmVolatilePause();
  }

  // ============================================================================
  // 阶段 1.5：性能统计
  // ============================================================================
  // 这个统计在快路径之后，所以当所有写入都来自同一个线程时，计数为 0
  PERF_TIMER_GUARD(write_thread_wait_nanos);

  // ============================================================================
  // 自适应等待策略的详细设计文档
  // ============================================================================
  // 如果只需要等待短时间，在循环中调用 std::this_thread::yield() 比在
  // StateMutex() 中阻塞要高效得多。参考数据：
  // 在 4.0 SELinux 测试服务器上（启用了系统调用审计支持）：
  // - FUTEX_WAKE 到 FUTEX_WAIT 返回的最小延迟: 2.7 微秒
  // - 平均延迟: 约 10 微秒
  //
  // 这对 RocksDB 的单写入者设计影响很大。当然，如果其他线程正在等待运行
  // 或者需要等待很长时间，自旋是个坏主意。我们如何决策？
  //
  // 我们将等待分为 3 类：short-uncontended、short-contended 和 long
  // 如果我们有预言机，那么我们会：
  // - 总是对 short-uncontended 自旋
  // - 总是对 long 阻塞
  // - 对 short-contended 取决于我们优化 RocksDB 吞吐量还是避免贪婪使用系统资源
  //
  // 区分 short 和 long 很简单：测量经过的时间。
  // 区分 short-uncontended 和 short-contended 稍微复杂一点：
  // 我们可以使用 getrusage(RUSAGE_THREAD, ..) 检测非自愿上下文切换，
  // 但更简单的方法（可移植性代码和 CPU 开销更少）是查找耗时超过预期的 yield。
  // 如果当前核心没有其他可运行进程，sched_yield() 不会产生上下文切换开销，
  // 在这种情况下它通常少于 1 微秒。
  //
  // 这里的主要可调参数：
  // 1. "short" 和 "long" 等待之间的阈值（max_yield_usec_）
  // 2. 怀疑 yield 足够慢以至于应该阻塞的阈值（slow_yield_usec_ 和 kMaxSlowYieldsWhileSpinning）
  //
  // 如果这些阈值选择得当：
  // - CPU 密集型工作负载且线程数不超过核心数时，会经历很少的上下文切换（自愿或非自愿）
  // - 总上下文切换数（自愿和非自愿）不会比 --max_yield_wait_micros=0 时的自愿切换数
  //   大太多（可能约 2 倍）
  //
  // 还有一个常数：在反转之前的决定之前我们会容忍的慢 yield 次数
  // 孤立的慢 yield 很常见（低优先级的小任务准备运行），所以至少应该为 2
  // 我们保守地设置为 3，这样我们也可以立即安排 ctx 更新，而不是等待下一次 update_ctx

  // 自旋时容忍的最大慢 yield 次数
  const size_t kMaxSlowYieldsWhileSpinning = 3;

  // yield_credit: yield 方法在这个上下文中的信誉度
  // - 信誉度通过 yield 在超时前成功而增加
  // - 否则减少
  // 引用自适应上下文中的值
  auto& yield_credit = ctx->value;

  // 是否更新自适应上下文
  // 基于采样运行或在硬失败后立即更新
  bool update_ctx = false;

  // 是否应该加强 yield 信誉度
  // 如果 yield 成功且快，设置为 true
  bool would_spin_again = false;

  // 采样基准：采样率为 1/sampling_base
  // 即每 256 次等待，只有 1 次会更新 yield_credit
  // 这样可以减少竞争并平滑数据
  const int sampling_base = 256;

  // ============================================================================
  // 阶段 2：让出自旋（Yield Loop）- 自适应策略
  // ============================================================================
  if (max_yield_usec_ > 0) {
    // 随机决定是否更新上下文（采样率 1/256）
    update_ctx = Random::GetTLSInstance()->OneIn(sampling_base);

    // 如果满足以下任一条件，尝试 yield 自旋：
    // 1. 本次需要更新统计信息（update_ctx == true）
    // 2. 之前 yield 自旋成功的概率 > 50%（yield_credit >= 0）
    if (update_ctx || yield_credit.load(std::memory_order_relaxed) >= 0) {
      // 我们正在更新自适应统计信息，或者自旋比 max_yield_usec_ 短的概率 > 50%
      // 且不会导致非自愿上下文切换

      // 记录自旋开始时间
      auto spin_begin = std::chrono::steady_clock::now();

      // 慢 yield 计数器（不包括最终导致目标满足的那个 yield，如果有）
      size_t slow_yield_count = 0;

      // 每次迭代的开始时间
      auto iter_begin = spin_begin;

      // 循环：yield 自旋，直到超时或目标状态满足
      while ((iter_begin - spin_begin) <=
             std::chrono::microseconds(max_yield_usec_)) {
        // 让出 CPU 时间片给其他线程
        // 如果没有其他可运行线程，这只会短暂暂停，不会导致上下文切换
        std::this_thread::yield();

        // 检查目标状态是否已满足
        state = w->state.load(std::memory_order_acquire);
        if ((state & goal_mask) != 0) {
          // 成功：目标状态满足
          would_spin_again = true;
          break;
        }

        // 记录当前时间
        auto now = std::chrono::steady_clock::now();

        // 检查这次 yield 是否是"慢"的
        // 条件：now == iter_begin（时钟精度不够）或 yield 耗时 >= slow_yield_usec_
        if (now == iter_begin ||
            now - iter_begin >= std::chrono::microseconds(slow_yield_usec_)) {
          // 保守地将其计为慢 yield（如果我们的时钟不够精确无法测量 yield 持续时间）
          ++slow_yield_count;

          // 如果慢 yield 次数过多，立即切换到阻塞等待
          if (slow_yield_count >= kMaxSlowYieldsWhileSpinning) {
            // 不仅一次非自愿上下文切换，而是多次。立即更新 yield_credit
            // 并退回到阻塞等待
            update_ctx = true;
            break;
          }
        }
        // 更新迭代开始时间
        iter_begin = now;
      }
    }
  }

  // ============================================================================
  // 阶段 3：阻塞等待（Blocking Wait）
  // ============================================================================
  // 如果目标状态仍未满足，使用互斥量和条件变量阻塞等待
  if ((state & goal_mask) == 0) {
    // 测试同步点：用于单元测试
    TEST_SYNC_POINT_CALLBACK("WriteThread::AwaitState:BlockingWaiting", w);

    // 调用阻塞等待函数
    state = BlockingAwaitState(w, goal_mask);
  }

  // ============================================================================
  // 更新自适应上下文
  // ============================================================================
  if (update_ctx) {
    // 由于我们的更新是基于采样的，所以一个线程覆盖其他线程的更新是可以的
    // 因此更新不需要是原子的

    // 读取当前的 yield_credit
    auto v = yield_credit.load(std::memory_order_relaxed);

    // 固定点指数衰减，衰减常数为 1/1024，+1 和 -1 按比例缩放以避免 int32_t 溢出
    //
    // 每次更新时：
    // - 正信誉度衰减 1/1024（即 0.1%）
    // - 如果采样的 yield 成功，信誉度增加 X
    // - 设置 X=2^17=131072 确保信誉度永远不超过 2^17*2^10=2^27
    //   这低于 int32_t 的上限 2^31
    // - 负信誉度同理
    //
    // 公式：new_credit = old_credit - old_credit/1024 + (would_spin_again ? +X : -X)
    v = v - (v / 1024) + (would_spin_again ? 1 : -1) * 131072;

    // 存储更新后的信誉度
    yield_credit.store(v, std::memory_order_relaxed);
  }

  // 断言：状态必须满足目标掩码
  assert((state & goal_mask) != 0);
  return state;
}

// WriteThread::SetState - 设置 Writer 的状态
//
// 功能概述：
// 原子地设置 Writer 的新状态，并可能唤醒正在等待该 Writer 的线程。
// 函数处理两种情况：
// 1. 简单状态转换：使用 CAS 原子操作直接更新状态
// 2. 阻塞等待状态：需要使用互斥量和条件变量唤醒等待线程
//
// 参数说明：
// - w: 要设置状态的 Writer 指针
// - new_state: 新的状态值
//
// 设计原理：
// 这个函数是 AwaitState 的配对函数。当某个线程改变 Writer 的状态时，
// 它调用 SetState 来通知可能正在等待该状态变化的线程。
//
// 状态类型：
// - 非等待状态（如 STATE_INIT, STATE_GROUP_LEADER, STATE_COMPLETED）：
//   Writer 处于活动或完成状态，可以安全地使用 CAS 更新
//
// - 等待状态（STATE_LOCKED_WAITING）：
//   Writer 处于阻塞等待状态，另一个线程正持有互斥量等待条件变量
//   必须通过条件变量唤醒，而不是简单的 CAS
//
// 执行流程：
// 1. 读取当前状态
// 2. 尝试使用 CAS 更新状态
// 3. 如果 CAS 失败（可能是等待状态），使用互斥量和条件变量更新
// 4. 通过条件变量唤醒等待线程
//
// 内存序说明：
// - load(acquire): 确保读取到最新的状态值
// - store(relaxed): 在互斥量保护下，不需要额外的内存序保证
// - compare_exchange_strong: 强 CAS，保证正确性
void WriteThread::SetState(Writer* w, uint8_t new_state) {
  // 断言：Writer 指针必须有效
  assert(w);

  // ============================================================================
  // 步骤 1：读取当前状态
  // ============================================================================
  // 原子地加载 Writer 的当前状态
  // memory_order_acquire: 确保能看到状态改变前的所有内存写入
  auto state = w->state.load(std::memory_order_acquire);

  // ============================================================================
  // 步骤 2：检查是否需要特殊处理等待状态
  // ============================================================================
  // 如果满足以下任一条件，需要使用互斥量和条件变量：
  // 1. 当前状态是 STATE_LOCKED_WAITING：Writer 正在阻塞等待
  // 2. CAS 失败：状态已被其他线程改变（通常也意味着有等待）
  if (state == STATE_LOCKED_WAITING ||
      !w->state.compare_exchange_strong(state, new_state)) {
    // 断言：当前状态必须是 STATE_LOCKED_WAITING
    // 这意味着另一个线程正在 BlockingAwaitState 中等待
    assert(state == STATE_LOCKED_WAITING);

    // ============================================================================
    // 步骤 3：使用互斥量保护状态更新
    // ============================================================================
    // 加锁 Writer 的状态互斥量
    // 与 BlockingAwaitState 中的同一个互斥量配对
    std::lock_guard<std::mutex> guard(w->StateMutex());

    // 断言：确认状态还没有被更新为 new_state
    // 防止重复更新
    assert(w->state.load(std::memory_order_relaxed) != new_state);

    // ============================================================================
    // 步骤 4：更新状态并唤醒等待线程
    // ============================================================================
    // 在互斥量保护下更新状态
    // memory_order_relaxed: 由于互斥量已提供同步，不需要额外的内存序
    w->state.store(new_state, std::memory_order_relaxed);

    // 唤醒正在条件变量上等待的线程
    // notify_one: 只唤醒一个等待线程（WriteThread 的设计是单生产者-单消费者）
    // 与 BlockingAwaitState 中的 wait() 配对
    w->StateCV().notify_one();
  }

  // ============================================================================
  // 步骤 2 的另一种情况：CAS 成功
  // ============================================================================
  // 如果 CAS 成功，状态已经原子地更新为 new_state
  // 不需要额外的操作，因为：
  // 1. 如果 Writer 正在自旋等待，它会立即看到新状态
  // 2. 如果 Writer 没有等待，这次状态转换只是状态机的正常推进
}

// WriteThread::LinkOne - 将单个 Writer 链接到写入队列
//
// 功能概述：
// 尝试将 Writer 链接到写入队列的末尾，成为新的最新 writer。
// 这是无锁链表插入的原子操作，用于构建写入队列。
//
// 参数说明：
// - w: 要链接的 Writer 指针
// - newest_writer: 指向队列中最新 writer 的原子指针
//
// 返回值：
// - true: 成功成为 leader（队列为空或之前只有一个 writer）
// - false: 队列非空，成为 follower
//
// 写入停顿机制：
// 当数据库处于写入停顿状态（kStopped 或 kDelayed）时，
// newest_writer_ 会指向 write_stall_dummy_，阻止新的写入加入队列。
// - 如果 w->no_slowdown = true: 立即返回失败（快速失败）
// - 如果 w->no_slowdown = false: 等待停顿清除
//
// 链表结构：
//    old newest_writer_    w (new newest_writer_)
//           │                 │
//           ▼                 ▼
//     WriterA -----------> WriterB
//     link_newer         link_newer
//         │                 │
//         ▼                 ▼
//     WriterC -----------> WriterD
//                          link_older
//
// 状态转换：
// STATE_INIT -> (被 leader 选中) -> STATE_GROUP_LEADER
// STATE_INIT -> (等待 leader 处理) -> STATE_COMPLETED
// STATE_INIT -> (写入停顿) -> STATE_COMPLETED (如果 no_slowdown)
bool WriteThread::LinkOne(Writer* w, std::atomic<Writer*>* newest_writer) {
  // 断言：newest_writer 必须有效
  assert(newest_writer != nullptr);

  // 断言：Writer 必须处于初始状态
  // 只有未链接的 writer 才能调用此函数
  assert(w->state == STATE_INIT);

  // ============================================================================
  // 步骤 1：读取当前最新的 writer
  // ============================================================================

  // 原子地读取当前的最新 writer
  // memory_order_relaxed: 不需要强一致性，会在后续 CAS 中重新检查
  Writer* writers = newest_writer->load(std::memory_order_relaxed);

  // ============================================================================
  // 步骤 2：尝试原子地链接到队列（CAS 循环）
  // ============================================================================

  while (true) {
    // 断言：不能将 writer 链接到自己
    // 防止循环链表
    assert(writers != w);

    // ----------------------------------------------------------------------
    // 子步骤 2.1：检查写入停顿状态
    // ----------------------------------------------------------------------
    // If write stall in effect, and w->no_slowdown is not true,
    // block here until stall is cleared. If its true, then return
    // immediately
    //
    // 如果当前 newest_writer 指向 write_stall_dummy_，说明正在写入停顿
    // write_stall_dummy_ 是一个特殊的 writer，用于标记队列处于停顿状态
    if (writers == &write_stall_dummy_) {
      // 情况 A：当前 writer 不愿意等待停顿（快速失败）
      if (w->no_slowdown) {
        // 设置错误状态：写入停顿
        w->status = Status::Incomplete("Write stall");

        // 设置状态为已完成（失败）
        SetState(w, STATE_COMPLETED);

        // 返回 false，表示没有成功成为 leader
        return false;
      }

      // 情况 B：当前 writer 愿意等待停顿清除
      // Since no_slowdown is false, wait here to be notified of the write
      // stall clearing
      {
        // 加锁停顿互斥量
        // 保护停顿状态和条件变量
        MutexLock lock(&stall_mu_);

        // 重新读取 newest_writer_，因为可能在加锁前已经改变
        writers = newest_writer->load(std::memory_order_relaxed);

        // 如果仍然处于停顿状态
        if (writers == &write_stall_dummy_) {
          // 测试同步点：用于单元测试
          TEST_SYNC_POINT_CALLBACK("WriteThread::WriteStall::Wait", w);

          // 等待停顿清除的信号
          // 其他线程会在停顿清除后调用 stall_cv_.NotifyAll()
          stall_cv_.Wait();

          // Load newest_writers_ again since it may have changed
          // 停顿清除后，重新读取 newest_writer_
          // 并继续循环尝试链接
          writers = newest_writer->load(std::memory_order_relaxed);
          continue;  // 继续下一次循环
        }
      }
    }

    // ----------------------------------------------------------------------
    // 子步骤 2.2：设置链表指针
    // ----------------------------------------------------------------------
    // 将当前 writer 的 link_older 指针指向当前的最新 writer
    // 这样 w 就成为了新的链表末尾
    //
    // 链表结构：
    //   writers (old newest) -> w (new newest)
    //                link_older
    w->link_older = writers;

    // ----------------------------------------------------------------------
    // 子步骤 2.3：原子地更新 newest_writer_
    // ----------------------------------------------------------------------
    // 使用 CAS (Compare-And-Swap) 原子操作更新 newest_writer_
    // - 如果 newest_writer_ 仍然是 writers，则将其更新为 w
    // - 如果 newest_writer_ 已经被其他线程修改，则 CAS 失败
    // - CAS 失败后，继续循环重新读取和尝试
    //
    // compare_exchange_weak: 弱 CAS，可能在某些情况下假失败
    // 但性能更好，失败后会重试
    if (newest_writer->compare_exchange_weak(writers, w)) {
      // CAS 成功：成功链接到队列

      // 返回是否成为 leader
      // - writers == nullptr: 原来队列为空，当前 writer 成为 leader
      // - writers != nullptr: 队列非空，当前 writer 成为 follower
      return (writers == nullptr);
    }

    // CAS 失败：newest_writer_ 被其他线程修改
    // 继续循环，重新读取 writers 并再次尝试
    // 这是无锁队列的核心机制，保证并发安全
  }
}

bool WriteThread::LinkGroup(WriteGroup& write_group,
                            std::atomic<Writer*>* newest_writer) {
  assert(newest_writer != nullptr);
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;
  Writer* w = last_writer;
  while (true) {
    // Unset link_newer pointers to make sure when we call
    // CreateMissingNewerLinks later it create all missing links.
    w->link_newer = nullptr;
    w->write_group = nullptr;
    if (w == leader) {
      break;
    }
    w = w->link_older;
  }
  Writer* newest = newest_writer->load(std::memory_order_relaxed);
  while (true) {
    leader->link_older = newest;
    if (newest_writer->compare_exchange_weak(newest, last_writer)) {
      return (newest == nullptr);
    }
  }
}

// WriteThread::CreateMissingNewerLinks - 补全 Writer 链表的 link_newer 指针
//
// 功能概述：
// 此函数用于补全 Writer 双向链表中缺失的 link_newer 指针。
// 在某些情况下（如 LinkGroup 操作后），link_newer 指针可能被设置为 nullptr，
// 本函数遍历链表并重建这些指针，使其成为完整的双向链表。
//
// 链表结构说明：
// - link_older: 指向链表中较早的 writer（向前指针）
// - link_newer: 指向链表中较新的 writer（向后指针）
// - 链表是从旧到新的顺序：WriterA → WriterB → WriterC
//   - WriterA.link_older = nullptr (队首)
//   - WriterA.link_newer = WriterB
//   - WriterB.link_older = WriterA
//   - WriterB.link_newer = WriterC
//   - WriterC.link_older = WriterB
//   - WriterC.link_newer = nullptr (队尾)
//
// 参数说明：
// - head: 链表的起始节点（可能是 leader、newest_writer_ 或其他节点）
//
// 调用时机：
// 1. EnterAsBatchGroupLeader: 在构建批处理组前，确保可以向后遍历链表
// 2. LinkGroup: 在链接 WriteGroup 到 newest_memtable_writer_ 后
// 3. ExitAsBatchGroupLeader: 在流水线模式下，需要查找新的 leader
// 4. ExitUnbatched: 在退出非批处理模式后
//
// 懒加载设计：
// link_newer 指针采用懒加载策略：
// - 优点：减少指针更新操作，提高 LinkOne 的性能
// - 缺点：需要专门的函数补全 link_newer 指针
// - 实现：LinkOne 只设置 link_older，不设置 link_newer
//
// 算法逻辑：
// 1. 从 head 开始向后遍历（通过 link_older 指针）
// 2. 对于每个节点，设置其下一个节点的 link_newer 指向当前节点
// 3. 停止条件：
//    - link_older 为 nullptr（到达链表末尾）
//    - link_newer 已非空且指向当前节点（双向链接已存在）
//
// 示例场景：
//
// 场景 1: 完整的链表（link_newer 已设置）
//   head → WriterA → WriterB → WriterC → nullptr
//   无需操作，直接返回
//
// 场景 2: link_newer 全部缺失（需要补全）
//   初始状态：
//     head = WriterA
//     WriterA.link_older = nullptr
//     WriterA.link_newer = nullptr
//     WriterB.link_older = WriterA
//     WriterB.link_newer = nullptr
//     WriterC.link_older = WriterB
//     WriterC.link_newer = nullptr
//
//   执行过程：
//     1) head = WriterA, next = WriterA.link_older = nullptr
//        break (到达末尾)
//
//   最终状态（不变）：
//     仍为单向链表
//
// 场景 3: 部分链表已双向连接（最常见的情况）
//   初始状态（LinkGroup 后）：
//     head = WriterA
//     WriterA.link_older = nullptr, WriterA.link_newer = nullptr
//     WriterB.link_older = WriterA, WriterB.link_newer = WriterC
//     WriterC.link_older = WriterB, WriterC.link_newer = WriterD
//     WriterD.link_older = WriterC, WriterD.link_newer = nullptr
//
//   执行过程：
//     1) head = WriterA, next = WriterA.link_older = nullptr
//        break (到达末尾)
//
//   注意：这种情况下，函数不会补全 WriterA 的 link_newer
//   因为它是链表的起始节点
//
// 场景 4: 从中间节点开始补全
//   初始状态：
//     head = WriterB
//     WriterB.link_older = WriterA, WriterB.link_newer = nullptr
//     WriterC.link_older = WriterB, WriterC.link_newer = nullptr
//
//   执行过程：
//     1) head = WriterB, next = WriterB.link_older = WriterA
//        next != nullptr, next.link_newer == nullptr
//        WriterA.link_newer = WriterB
//        head = WriterA
//     2) head = WriterA, next = WriterA.link_older = nullptr
//        break
//
//   最终状态：
//     WriterA.link_newer = WriterB (已补全)
//     WriterB.link_older = WriterA, WriterB.link_newer = nullptr
//
// 线程安全：
// - 函数只读取 link_older，不修改它
// - 只写入 link_newer，且采用单向遍历，不会产生竞争
// - 通常在持有 db_mutex 或确认无并发修改时调用
//
// 性能考虑：
// - 时间复杂度：O(n)，n 为链表长度
// - 空间复杂度：O(1)，只使用常量空间
// - 遍历方向：从新到旧（通过 link_older）
//
// 注意事项：
// 1. 此函数不会创建 link_newer 指向链表末尾节点
// 2. 此函数不会修改链表结构，只补全 link_newer 指针
// 3. 调用此函数后，可以通过 link_newer 向后遍历已补全的部分
// 4. 断言确保已双向连接的节点不会出现不一致
void WriteThread::CreateMissingNewerLinks(Writer* head) {
  // 循环遍历链表，补全 link_newer 指针
  while (true) {
    // 获取当前 head 节点的 link_older 指针（指向链表中的前一个节点）
    Writer* next = head->link_older;

    // 检查遍历终止条件：
    // 1. next == nullptr: 到达链表头部（最早加入的 writer）
    //    说明已经遍历完整个链表，停止补全
    // 2. next->link_newer != nullptr: link_newer 已被设置
    //    说明这部分链表已经被其他操作双向连接，停止补全
    if (next == nullptr || next->link_newer != nullptr) {
      // 断言验证：
      // - 如果 next 为 nullptr，说明到达链表头部，直接断言通过
      // - 如果 next 不为 nullptr 且 link_newer 不为 nullptr，
      //   断言 link_newer 必须指向 head，保证双向链表的一致性
      // 这个断言用于捕获数据竞争或逻辑错误
      assert(next == nullptr || next->link_newer == head);
      // 终止遍历
      break;
    }

    // 设置下一个节点的 link_newer 指向当前节点
    // 这样就建立了从 next 到 head 的反向链接（从旧到新）
    // 示例：
    //   补全前: next.link_newer = nullptr
    //   补全后: next.link_newer = head
    //   结果: next ←→ head (双向连接)
    next->link_newer = head;

    // 将 head 移动到下一个节点（继续向前遍历链表）
    // 这样可以补全更早节点的 link_newer 指针
    head = next;
  }
}

// WriteThread::CompleteLeader - 完成 Leader Writer 并从批处理组中移除
//
// 功能概述：
// 此函数用于完成一个 Leader Writer，将其从 WriteGroup 中移除，
// 并将其状态设置为 STATE_COMPLETED，唤醒等待中的线程。
// Leader 完成后，如果批处理组中还有其他 Writer，会选择新的 Leader。
//
// 参数说明：
// - write_group: 引用类型，包含批处理组的状态信息
//   * leader: 批处理组的 Leader（将被完成）
//   * last_writer: 批处理组的最后一个 Writer
//   * size: 批处理组中的 Writer 数量
//
// 调用时机：
// 1. ExitAsBatchGroupLeader: Leader 完成 WAL 和 MemTable 写入后
// 2. ExitAsMemTableWriter: MemTable writer leader 完成写入后
// 3. 流水线模式下 Leader 完成记账和 WAL 写入后
//
// 链表结构变化：
// 移除 Leader 需要考虑两种情况：
//
// 情况 A: 批处理组只有 Leader 一个 Writer（size == 1）
//   移除前：
//     leader (既是 leader 又是 last_writer)
//     link_older = nullptr
//     link_newer = nullptr
//
//   移除后：
//     批处理组清空
//     leader = nullptr
//     last_writer = nullptr
//     size = 0
//
// 情况 B: 批处理组有多个 Writer（size > 1）
//   移除前：
//     leader ←→ next ←→ ... ←→ last_writer
//     link_older   link_older  link_older
//     link_newer   link_newer  link_newer
//
//   移除后：
//     next (新 leader) ←→ ... ←→ last_writer
//     link_older = nullptr
//     link_newer
//     leader 已被移除
//
// 新 Leader 的选择：
// - 如果 size > 1，next (leader->link_newer) 成为新的 leader
// - 新 leader 的 link_older 被设置为 nullptr（成为链表头部）
//
// 断言检查：
// - write_group.size > 0: 确保至少有一个 Writer
// - leader->link_newer != nullptr: 确保 size > 1 时有下一个 Writer
//
// 线程安全：
// - 通常在持有 db_mutex 或确认无并发修改时调用
// - SetState 会原子地设置状态并唤醒等待线程
//
// 状态转换：
// leader: STATE_GROUP_LEADER 或 STATE_MEMTABLE_WRITER_LEADER → STATE_COMPLETED
// 这会唤醒在 JoinBatchGroup 或 AwaitState 中等待的线程
//
// 与 CompleteFollower 的区别：
// - CompleteLeader: 处理 Leader，可能需要选择新的 Leader
// - CompleteFollower: 处理 Follower，不移除链表头部
void WriteThread::CompleteLeader(WriteGroup& write_group) {
  // 断言：批处理组必须包含至少 1 个 Writer
  // Leader 必须存在才能完成
  assert(write_group.size > 0);

  // 获取批处理组的 Leader 指针
  // Leader 是批处理组的第一个 Writer，link_older == nullptr
  Writer* leader = write_group.leader;

  // 判断批处理组的 Writer 数量，采取不同的处理策略
  if (write_group.size == 1) {
    // ========================================================================
    // 情况 A: 批处理组只有 Leader 一个 Writer（size == 1）
    // ========================================================================
    //
    // 链表结构：
    //   leader (既是 leader 又是 last_writer)
    //   link_older = nullptr
    //   link_newer = nullptr
    //
    // 处理逻辑：
    // 1. 将 leader 和 last_writer 都设置为 nullptr
    // 2. 批处理组变为空组（size 将递减为 0）
    //
    // 示例：
    //   完成前：
    //     write_group.leader = WriterA
    //     write_group.last_writer = WriterA
    //     write_group.size = 1
    //     链表: WriterA
    //
    //   完成后：
    //     write_group.leader = nullptr
    //     write_group.last_writer = nullptr
    //     write_group.size = 0
    //     链表: 空

    // 将批处理组的 leader 指针设置为 nullptr
    // 表示批处理组没有 leader（空组）
    write_group.leader = nullptr;

    // 将批处理组的 last_writer 指针设置为 nullptr
    // 表示批处理组没有 writer（空组）
    // 与 leader 一起设置为 nullptr，保持一致性
    write_group.last_writer = nullptr;
  } else {
    // ========================================================================
    // 情况 B: 批处理组有多个 Writer（size > 1）
    // ========================================================================
    //
    // 链表结构：
    //   leader ←→ next ←→ ... ←→ last_writer
    //   link_older   link_older  link_older
    //   link_newer   link_newer  link_newer
    //
    // 处理逻辑：
    // 1. 移除 leader，使其下一个 Writer 成为新的 leader
    // 2. 将 next.link_older 设置为 nullptr（链表头部）
    // 3. 更新 write_group.leader 指向 next
    //
    // 示例：
    //   完成前：
    //     write_group.leader = WriterA
    //     write_group.last_writer = WriterC
    //     write_group.size = 3
    //     链表: WriterA ←→ WriterB ←→ WriterC
    //
    //   完成后：
    //     write_group.leader = WriterB (新 leader)
    //     write_group.last_writer = WriterC
    //     write_group.size = 2
    //     链表: WriterB ←→ WriterC
    //     WriterA 已被移除

    // 断言：Leader 必须有下一个 Writer（link_newer != nullptr）
    // 因为 size > 1，所以至少还有一个 Writer 在 leader 后面
    assert(leader->link_newer != nullptr);

    // 将下一个 Writer 的 link_older 设置为 nullptr
    // 这样下一个 Writer 就成为新的链表头部
    // leader->link_newer: 下一个 Writer（新 leader）
    // 设置 link_older = nullptr 使其成为新的头部
    leader->link_newer->link_older = nullptr;

    // 更新批处理组的 leader 指针
    // 使其指向原 leader 的下一个 Writer
    // 这个 Writer 成为新的批处理组 leader
    write_group.leader = leader->link_newer;
  }

  // 递减批处理组的大小
  // 表示 Leader 已被移除，批处理组中剩余的 Writer 数量减 1
  // 示例：
  // - size = 1: 移除 Leader 后，size = 0（空组）
  // - size = 4: 移除 Leader 后，size = 3（还有 3 个 Follower）
  write_group.size -= 1;

  // 设置 Leader 的状态为 STATE_COMPLETED
  // SetState 的作用：
  // 1. 原子地设置 Writer 的状态为 STATE_COMPLETED
  // 2. 如果 Writer 处于等待状态（STATE_LOCKED_WAITING），通过条件变量唤醒它
  // 3. 通知 Writer 它可以返回结果给调用者
  //
  // 状态转换：
  // leader: STATE_GROUP_LEADER 或 STATE_MEMTABLE_WRITER_LEADER → STATE_COMPLETED
  //
  // 唤醒机制：
  // - 如果 Leader 在某个等待点等待（虽然不太可能），会被唤醒
  // - 更多情况下，Leader 线程在调用此函数后继续执行
  // - Leader 的状态设置为 COMPLETED 后，不应再被访问
  //
  // 注意：
  // - Leader 完成后，新 leader（如果存在）可能需要开始工作
  // - 新 leader 的状态由其他函数设置（如 ExitAsBatchGroupLeader）
  // - 这里只是完成当前 leader，不影响新 leader 的状态
  SetState(leader, STATE_COMPLETED);
}

// WriteThread::CompleteFollower - 完成 Follower Writer 并从批处理组中移除
//
// 功能概述：
// 此函数用于完成一个 Follower Writer（非 Leader），将其从 WriteGroup 中移除，
// 并将其状态设置为 STATE_COMPLETED，唤醒等待中的线程。
//
// 参数说明：
// - w: 要完成的 Writer 指针（必须是 Follower，不能是 Leader）
// - write_group: 引用类型，包含批处理组的状态信息
//   * leader: 批处理组的 Leader
//   * last_writer: 批处理组的最后一个 Writer
//   * size: 批处理组中的 Writer 数量
//
// 调用时机：
// 1. ExitAsBatchGroupLeader: 在非流水线模式下，逐个完成 Follower
// 2. ExitAsMemTableWriter: 完成 MemTable 写入组的 Follower
// 3. 流水线模式下的完成操作
//
// 链表结构变化：
// 移除操作需要考虑两种情况：
//
// 情况 A: w 是批处理组的最后一个 Writer（last_writer）
//   移除前：
//     leader ←→ ... ←→ prev ←→ w (last_writer)
//     link_older     link_older  link_older
//     link_newer     link_newer  link_newer
//
//   移除后：
//     leader ←→ ... ←→ prev (new last_writer)
//                                link_newer = nullptr
//     w 已被移除
//
// 情况 B: w 不是最后一个 Writer（中间的 Writer）
//   移除前：
//     leader ←→ ... ←→ prev ←→ w ←→ next ←→ ... ←→ last_writer
//     link_older     link_older  link_older  link_older
//     link_newer     link_newer  link_newer  link_newer
//
//   移除后：
//     leader ←→ ... ←→ prev ←→ next ←→ ... ←→ last_writer
//     link_older     link_older  link_older  link_older
//     link_newer     link_newer  link_newer  link_newer
//     w 已被移除
//
// 断言检查：
// - write_group.size > 1: 确保至少还有一个 Writer（Leader）
// - w != write_group.leader: 确保 w 不是 Leader（Leader 应使用 CompleteLeader）
//
// 线程安全：
// - 通常在持有 db_mutex 或确认无并发修改时调用
// - SetState 会原子地设置状态并唤醒等待线程
//
// 状态转换：
// w: STATE_PARALLEL_MEMTABLE_WRITER 或其他状态 → STATE_COMPLETED
// 这会唤醒在 JoinBatchGroup 或 AwaitState 中等待的线程
void WriteThread::CompleteFollower(Writer* w, WriteGroup& write_group) {
  // 断言：批处理组必须包含至少 2 个 Writer（Leader + 1 个 Follower）
  // 因为 Follower 不能单独存在，必须有一个 Leader
  assert(write_group.size > 1);

  // 断言：当前 Writer 不能是 Leader
  // Leader 应该使用 CompleteLeader 函数完成，而不是 CompleteFollower
  assert(w != write_group.leader);

  // 判断当前 Writer 的位置，采取不同的移除策略
  if (w == write_group.last_writer) {
    // ========================================================================
    // 情况 A: 当前 Writer 是批处理组的最后一个 Writer（last_writer）
    // ========================================================================
    //
    // 链表结构：
    //   ... ←→ prev ←→ w (last_writer)
    //           ↑       ↑
    //        link_older link_older
    //        link_newer link_newer = nullptr
    //
    // 移除操作：
    // 1. 设置 prev.link_newer = nullptr（使 prev 成为新的 last_writer）
    // 2. 更新 write_group.last_writer = prev
    //
    // 示例：
    //   移除前：
    //     Leader ←→ Writer2 ←→ Writer3 (last_writer)
    //   移除 Writer3 后：
    //     Leader ←→ Writer2 (new last_writer)
    //     Writer3 已被移除，link_newer = nullptr

    // 将前一个 Writer 的 link_newer 设置为 nullptr
    // 这样前一个 Writer 就成为新的批处理组尾部
    // w->link_older 指向 w 的前一个节点
    w->link_older->link_newer = nullptr;

    // 更新批处理组的 last_writer 指针
    // 使其指向 w 的前一个 Writer
    // 这样新的批处理组尾部就是 w->link_older
    write_group.last_writer = w->link_older;
  } else {
    // ========================================================================
    // 情况 B: 当前 Writer 不是最后一个 Writer（中间的 Writer）
    // ========================================================================
    //
    // 链表结构：
    //   ... ←→ prev ←→ w ←→ next ←→ ...
    //           ↑       ↑       ↑
    //        link_older link_older link_older
    //        link_newer link_newer link_newer
    //
    // 移除操作：
    // 1. 设置 prev.link_newer = next（跳过 w）
    // 2. 设置 next.link_older = prev（跳过 w）
    //
    // 示例：
    //   移除前：
    //     Leader ←→ Writer2 ←→ Writer3 ←→ Writer4
    //   移除 Writer3 后：
    //     Leader ←→ Writer2 ←→ Writer4
    //     Writer3 已被移除，链表跳过 Writer3
    //
    // 双向链表删除节点标准操作：
    // 1. prev.link_newer = next
    // 2. next.link_older = prev

    // 将前一个 Writer 的 link_newer 指向当前 Writer 的下一个 Writer
    // 这样前一个 Writer 就跳过当前 Writer，直接连接到下一个 Writer
    // w->link_older: 前一个 Writer
    // w->link_newer: 下一个 Writer
    w->link_older->link_newer = w->link_newer;

    // 将下一个 Writer 的 link_older 指向当前 Writer 的前一个 Writer
    // 这样下一个 Writer 就跳过当前 Writer，直接连接到前一个 Writer
    // 与上一行一起完成了双向链表的节点移除操作
    w->link_newer->link_older = w->link_older;
  }

  // 递减批处理组的大小
  // 表示当前 Writer 已被移除，批处理组中剩余的 Writer 数量减 1
  // 例如：size = 4，移除 1 个 Follower 后，size = 3
  write_group.size -= 1;

  // 设置当前 Writer 的状态为 STATE_COMPLETED
  // SetState 的作用：
  // 1. 原子地设置 Writer 的状态为 STATE_COMPLETED
  // 2. 如果 Writer 处于等待状态（STATE_LOCKED_WAITING），通过条件变量唤醒它
  // 3. 通知 Writer 它可以返回结果给调用者
  //
  // 状态转换：
  // w: STATE_PARALLEL_MEMTABLE_WRITER 或其他等待状态 → STATE_COMPLETED
  //
  // 唤醒机制：
  // - 如果 Writer 在 JoinBatchGroup 中等待，会被唤醒
  // - 唤醒后，Writer 会检查状态并返回结果
  // - 结果存储在 w->status 中
  //
  // 注意：
  // - SetState 会唤醒等待中的线程（如果有）
  // - 被唤醒的线程会检查 w->status 获取执行结果
  // - 状态设置为 COMPLETED 后，Writer 不应再被访问
  SetState(w, STATE_COMPLETED);
}

// WriteThread::BeginWriteStall - 开始写入停顿
//
// 功能概述：
// 启动写入停顿机制，阻止新的写入操作进入队列，并处理已进入队列的写入。
// 这是一个无锁操作，通过在写入队列头部插入一个特殊的 dummy writer 来实现。
//
// 写入停顿的两种模式：
// 1. kStopped（完全停止）：写入操作会被拒绝
// 2. kDelayed（延迟写入）：写入操作会被限速
//
// 核心机制：
// 1. 在写入队列头部插入 write_stall_dummy_，使其成为 newest_writer_
// 2. 新的写入尝试链接队列时会看到 dummy，进入等待或快速失败
// 3. 遍历已进入队列的写入，处理那些设置了 no_slowdown 的写入
//
// 处理逻辑：
// - 对于 w->no_slowdown == true 的写入：
//   - 立即标记为失败（Status::Incomplete("Write stall")）
//   - 从队列中移除
//   - 设置状态为 STATE_COMPLETED
//   - 让线程快速返回，避免阻塞
//
// - 对于 w->no_slowdown == false 的写入：
//   - 保留在队列中
//   - 会在 LinkOne 中等待停顿清除
//
// 队列结构变化：
// Before BeginWriteStall:
//   newest_writer_ -> WriterD -> WriterC -> WriterB -> WriterA
//
// After BeginWriteStall:
//   newest_writer_ -> write_stall_dummy_
//                      link_older -> WriterD -> WriterC -> WriterB -> WriterA
//
// 如果 WriterD.no_slowdown == true，WriterD 会被移除：
//   newest_writer_ -> write_stall_dummy_
//                      link_older -> WriterC -> WriterB -> WriterA
//
// 设计考虑：
// 1. 为什么使用 dummy writer？
//    - 无锁操作：通过 CAS 原子地将 dummy 插入队列
//    - 简单高效：不需要维护复杂的数据结构
//    - 复用现有机制：LinkOne 已经处理了 write_stall_dummy_ 的逻辑
//
// 2. 为什么只处理 write_group == nullptr 的写入？
//    - write_group != nullptr 表示写入已经被 leader 处理或正在处理
//    - 当前的 write group 不会混合 slowdown/no_slowdown 写入
//    - 处理已经分组的写入可能会破坏批处理的原子性
//
// 3. 链表指针的维护：
//    - link_older: 从 dummy 到队列中的写入（单向链表）
//    - link_newer: 从写入到更新的节点（双向链表，但可能为空）
//    - CreateMissingNewerLinks() 会在需要时填充 link_newer
//
// 4. 为什么只在 link_newer 已设置时才更新它？
//    - CreateMissingNewerLinks() 假设第一个非空 link_newer 是最后一个空链接
//    - 如果在这里设置 link_newer，可能提前终止 CreateMissingNewerLinks()
//    - 只在 link_newer 已设置时才更新，保持一致性
//
// 线程安全：
// - stall_begun_count_ 的递增是原子的
// - LinkOne 是无锁的，使用 CAS 原子操作
// - 遍历队列时不需要锁，因为：
//   1. 此时 dummy 已经在队列头部
//   2. 新的写入会在 LinkOne 中等待
//   3. leader 完成当前组后才会处理新的写入
//
// 状态转换：
// - 正常写入: STATE_INIT -> STATE_GROUP_LEADER -> STATE_COMPLETED
// - 停顿写入 (no_slowdown): STATE_INIT -> STATE_COMPLETED (失败)
// - 停顿写入 (!no_slowdown): STATE_INIT -> STATE_GROUP_LEADER (等待停顿清除)
//
// 性能影响：
// - BeginWriteStall 是轻量级的，主要是队列遍历
// - no_slowdown 写入会快速失败，不占用资源
// - 其他写入会等待，但不阻塞系统
void WriteThread::BeginWriteStall() {
  // 增加停顿开始计数器
  // 用于跟踪停顿的次数和状态
  ++stall_begun_count_;

  // ============================================================================
  // 步骤 1：在队列头部插入 write_stall_dummy_
  // ============================================================================
  // 将特殊的 dummy writer 链接到队列头部
  // 这样新的写入会看到 dummy 并等待或快速失败
  // LinkOne 会处理 write_stall_dummy_ 的特殊逻辑
  LinkOne(&write_stall_dummy_, &newest_writer_);

  // ============================================================================
  // 步骤 2：遍历队列，处理设置了 no_slowdown 的写入
  // ============================================================================
  // Walk writer list until w->write_group != nullptr.
  // 遍历队列，直到遇到已被分组的写入（write_group != nullptr）
  // The current write group will not have a mix of slowdown/no_slowdown.
  // 当前的写入组不会混合 slowdown 和 no_slowdown 写入
  // 所以停在 write_group != nullptr 的位置是安全的
  //
  // 队列结构：
  // write_stall_dummy_ -> Writer1 -> Writer2 -> Writer3 -> ...
  //                      link_older
  //
  // 从 write_stall_dummy_.link_older 开始遍历
  Writer* w = write_stall_dummy_.link_older;
  Writer* prev = &write_stall_dummy_;

  // 遍历队列，处理所有未被分组的写入
  while (w != nullptr && w->write_group == nullptr) {
    // ----------------------------------------------------------------------
    // 情况 A：写入设置了 no_slowdown 标志
    // ----------------------------------------------------------------------
    // no_slowdown = true 表示写入不能容忍延迟
    // 需要立即失败，让调用者快速返回
    if (w->no_slowdown) {
      // 从队列中移除该写入
      // 将 prev->link_older 直接指向 w->link_older，跳过 w
      prev->link_older = w->link_older;

      // 设置错误状态：写入停顿
      w->status = Status::Incomplete("Write stall");

      // 设置状态为已完成（失败状态）
      // 等待中的线程会被唤醒并返回错误
      SetState(w, STATE_COMPLETED);

      // ----------------------------------------------------------------------
      // 更新 link_newer 指针（如果已设置）
      // ----------------------------------------------------------------------
      // Only update `link_newer` if it's already set.
      // 只在 link_newer 已设置时才更新它
      // `CreateMissingNewerLinks()` will update the nullptr `link_newer` later,
      // which assumes the first non-nullptr `link_newer` is the last
      // nullptr link in the writer list.
      // CreateMissingNewerLinks() 稍后会更新空的 link_newer
      // 它假设第一个非空的 link_newer 是最后一个空链接
      //
      // If `link_newer` is set here, `CreateMissingNewerLinks()` may stop
      // updating the whole list when it sees the first non-nullptr link.
      // 如果在这里设置 link_newer，CreateMissingNewerLinks() 可能在看到
      // 第一个非空链接时停止更新整个列表
      //
      // 只在以下情况下更新 link_newer：
      // 1. prev->link_older 存在（即 prev 不是 dummy）
      // 2. prev->link_older->link_newer 已设置
      // 这样可以避免破坏 CreateMissingNewerLinks() 的假设
      if (prev->link_older && prev->link_older->link_newer) {
        // 更新 link_newer 指针，使其指向 prev
        // 保持链表的双向一致性
        prev->link_older->link_newer = prev;
      }

      // 继续处理下一个写入
      // 注意：w 不更新为 w->link_older，因为已经被移除
      w = prev->link_older;
    } else {
      // ----------------------------------------------------------------------
      // 情况 B：写入没有设置 no_slowdown 标志
      // ----------------------------------------------------------------------
      // no_slowdown = false 表示写入可以容忍延迟
      // 保留该写入在队列中，它会在 LinkOne 中等待停顿清除

      // 更新 prev 指针
      prev = w;

      // 移动到下一个写入
      w = w->link_older;
    }
  }
}

void WriteThread::EndWriteStall() {
  MutexLock lock(&stall_mu_);

  // Unlink write_stall_dummy_ from the write queue. This will unblock
  // pending write threads to enqueue themselves
  assert(newest_writer_.load(std::memory_order_relaxed) == &write_stall_dummy_);
  // write_stall_dummy_.link_older can be nullptr only if LockWAL() has been
  // called.
  if (write_stall_dummy_.link_older) {
    write_stall_dummy_.link_older->link_newer = write_stall_dummy_.link_newer;
  }
  newest_writer_.exchange(write_stall_dummy_.link_older);

  ++stall_ended_count_;

  // Wake up writers
  stall_cv_.SignalAll();
}

uint64_t WriteThread::GetBegunCountOfOutstandingStall() {
  if (stall_begun_count_ > stall_ended_count_) {
    // Oustanding stall in queue
    assert(newest_writer_.load(std::memory_order_relaxed) ==
           &write_stall_dummy_);
    return stall_begun_count_;
  } else {
    // No stall in queue
    assert(newest_writer_.load(std::memory_order_relaxed) !=
           &write_stall_dummy_);
    return 0;
  }
}

void WriteThread::WaitForStallEndedCount(uint64_t stall_count) {
  MutexLock lock(&stall_mu_);

  while (stall_ended_count_ < stall_count) {
    stall_cv_.Wait();
  }
}

// WriteThread::JoinBatchGroup - 加入写入批处理组
//
// 功能概述：
// 这是 WriteThread 的核心入口，用于将 Writer 加入到写入队列并等待被处理。
// Writer 有多种可能的命运：
// 1. 成为 GROUP_LEADER：负责执行实际的 WAL 和 MemTable 写入
// 2. 成为 MEMTABLE_WRITER_LEADER：在流水线模式下负责 MemTable 写入
// 3. 成为 PARALLEL_MEMTABLE_WRITER：在并行模式下成为 follower
// 4. 等待其他 writer 完成后变为 STATE_COMPLETED
//
// 写入队列的组织结构：
// - Writers 以链表形式组织，通过 link_older 和 link_newer 指针连接
// - newest_writer_ 指向最新加入的 writer
// - leader 负责将多个 writer 组成一个批处理组一起处理
//
// 等待状态说明：
// 非 leader 的 writer 会等待直到以下任一情况：
// 1. 现有的 leader 在完成后选择它作为新的 leader（STATE_GROUP_LEADER）
// 2. 现有的 leader 选择它作为 follower 并：
//    2.1 代表它完成 MemTable 写入（串行模式）
//    2.2 或告诉它并行完成 MemTable 写入（并行模式）
// 3. 流水线模式下，leader 完成记账和 WAL 写入，将其加入待处理队列
//    3.1 它成为 MemTable writer 组的 leader
//    3.2 或现有的 MemTable writer 组 leader 告诉它并行完成写入
static WriteThread::AdaptationContext jbg_ctx("JoinBatchGroup");
void WriteThread::JoinBatchGroup(Writer* w) {
  // 测试同步点：用于单元测试，在 JoinBatchGroup 开始时调用
  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Start", w);

  // 断言：Writer 必须有一个有效的 WriteBatch
  assert(w->batch != nullptr);

  // ============================================================================
  // 步骤 1：将 Writer 链接到写入队列
  // ============================================================================

  // 将当前 writer 链接到写入队列的末尾
  // LinkOne 尝试将 writer 作为 leader 链接
  // 返回 true 表示成功成为 leader（队列为空或之前只有一个 writer）
  // 返回 false 表示队列非空，成为 follower
  bool linked_as_leader = LinkOne(w, &newest_writer_);

  // 如果成功链接为 leader，设置状态为 GROUP_LEADER
  // GROUP_LEADER: 当前 writer 将负责执行批处理组的所有写入
  if (linked_as_leader) {
    SetState(w, STATE_GROUP_LEADER);
  }

  // 测试同步点：用于单元测试
  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Wait", w);
  TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:Wait2", w);

  // ============================================================================
  // 步骤 2：如果不是 leader，等待被处理
  // ============================================================================

  if (!linked_as_leader) {
    /**
     * Follower 等待直到以下任一情况发生：
     *
     * 1) 现有的 leader 在完成后选择我们作为新的 leader
     *    - 原来的 leader 完成，当前 writer 成为下一轮的 leader
     *    - 状态变为 STATE_GROUP_LEADER
     *
     * 2) 现有的 leader 选择我们作为 follower 并：
     * 2.1) 代表我们完成 MemTable 写入（串行模式）
     *    - Leader 将 follower 的批次一起写入 MemTable
     *    - 状态变为 STATE_COMPLETED
     *
     * 2.2) 或告诉我们要并行完成 MemTable 写入（并行模式）
     *    - Leader 分配任务给 follower 并行写入
     *    - 状态变为 STATE_PARALLEL_MEMTABLE_WRITER
     *
     * 3) 流水线写入模式：现有的 leader 选择我们作为 follower 并完成记账和 WAL 写入
     *    - Leader 完成 WAL 写入和记账工作
     *    - 将当前 writer 加入待处理的 MemTable writer 队列
     *
     * 3.1) 我们成为 MemTable writer 组的 leader
     *    - 当前 writer 负责组织 MemTable 写入批处理组
     *    - 状态变为 STATE_MEMTABLE_WRITER_LEADER
     *
     * 3.2) 或现有的 MemTable writer 组 leader 告诉我们要并行完成 MemTable 写入
     *    - MemTable leader 将当前 writer 作为 follower
     *    - 状态变为 STATE_PARALLEL_MEMTABLE_WRITER
     */
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:BeganWaiting", w);

    // 等待状态变为以下任一状态：
    // - STATE_GROUP_LEADER: 成为 leader，需要执行完整的写入流程
    // - STATE_MEMTABLE_WRITER_LEADER: 流水线模式下的 MemTable leader
    // - STATE_PARALLEL_MEMTABLE_WRITER: 并行模式下的 follower
    // - STATE_COMPLETED: 写入已完成，可以返回
    //
    // &jbg_ctx: 自适应上下文，用于记录等待时间和调整让出策略
    AwaitState(w,
               STATE_GROUP_LEADER | STATE_MEMTABLE_WRITER_LEADER |
                   STATE_PARALLEL_MEMTABLE_WRITER | STATE_COMPLETED,
               &jbg_ctx);

    // 测试同步点：用于单元测试
    TEST_SYNC_POINT_CALLBACK("WriteThread::JoinBatchGroup:DoneWaiting", w);
  }
}

// 构建 Write Batch Group（写批处理组）
//
// 功能说明：
// 此函数由 WriteThread 的 Batch Group Leader 调用，用于从等待队列中收集 Writer，
// 组合成一个批处理组（WriteGroup）。批处理组中的所有 Writer 将由 Leader 一起处理，
// 包括 WAL 写入、序列号分配和 MemTable 写入，从而提高吞吐量。
//
// 调用时机：
// - 当前 Writer 成为 Batch Group Leader（状态从 STATE_INIT 变为 STATE_GROUP_LEADER）
// - 在 Leader 开始执行实际的 WAL 和 MemTable 写入之前
//
// 批处理组的优势：
// 1. 减少 WAL 写入次数：多个 WriteBatch 合并成一次 WAL 写入
// 2. 减少序列号分配开销：批量分配序列号
// 3. 提高吞吐量：合并小写入，降低每字节的开销
// 4. 减少锁竞争：一次性处理多个写入
//
// 批处理策略：
// - 从 Leader 开始，遍历等待队列中的 Writer（通过 link_newer 指针）
// - 根据以下条件决定是否将 Writer 加入批处理组：
//   1. 配置兼容性（sync、disable_wal、protection_bytes_per_key 等）
//   2. 批处理大小限制（max_size）
//   3. 回调是否允许批处理
// - 满足所有条件的 Writer 会被加入批处理组
// - 遇到不满足条件的 Writer 时停止收集
//
// 返回值：
// - 返回批处理组的总字节数（所有 Writer 的 WriteBatch 大小之和）
//
// 参数说明：
// - leader: 当前 Writer，已经处于 STATE_GROUP_LEADER 状态，link_older 必须为 nullptr
// - write_group: 输出参数，用于存储构建的批处理组信息
//
// 相关配置：
// - max_write_batch_group_size_bytes: 批处理组的最大字节数（默认 16MB）
//
// 注意事项：
// 1. Leader 的 link_older 必须为 nullptr（链表头部）
// 2. 遍历从 Leader 开始，通过 link_newer 向后遍历（从旧到新）
// 3. 遍历时 leader 是 exclusive（不包含），newest_writer 是 inclusive（包含）
// 4. 批处理组构建后，所有被选中的 Writer 的 write_group 指针都指向同一个 write_group
size_t WriteThread::EnterAsBatchGroupLeader(Writer* leader,
                                            WriteGroup* write_group) {
  assert(leader->link_older == nullptr);  // 断言：Leader 必须是链表头部（没有 link_older）
  assert(leader->batch != nullptr);       // 断言：Leader 必须有 WriteBatch
  assert(write_group != nullptr);         // 断言：write_group 不能为空

  // 获取 Leader 的 WriteBatch 大小，作为批处理组的初始大小
  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the  允许批处理组增长到最大大小，
  // original write is small, limit the growth so we do not slow  但如果原始写入很小，
  // down the small write too much.  则限制增长，以免让小写入太慢
  //
  // 批处理组大小限制策略：
  // - 对于小写入（size <= min_batch_size_bytes），max_size = size + min_batch_size_bytes
  //   这确保小写入不会被批处理过度拖慢
  // - 对于大写入（size > min_batch_size_bytes），max_size = max_write_batch_group_size_bytes
  //   这允许批处理组增长到最大限制
  //
  // 示例：
  // - max_write_batch_group_size_bytes = 16MB
  // - min_batch_size_bytes = 2MB (16MB / 8)
  // - Leader 大小 = 1KB（小写入）
  //   → max_size = 1KB + 2MB = 2MB + 1KB
  // - Leader 大小 = 5MB（大写入）
  //   → max_size = 16MB
  size_t max_size = max_write_batch_group_size_bytes;  // 获取配置的最大批处理组大小
  const uint64_t min_batch_size_bytes = max_write_batch_group_size_bytes / 8;  // 最小批处理组大小是最大值的 1/8
  if (size <= min_batch_size_bytes) {  // 如果 Leader 的写入很小
    max_size = size + min_batch_size_bytes;  // 限制最大大小为当前大小 + 最小批处理组大小
  }

  // 初始化批处理组信息
  leader->write_group = write_group;      // Leader 的 write_group 指针指向 write_group
  write_group->leader = leader;          // write_group 的 leader 指针指向 Leader
  write_group->last_writer = leader;     // 初始时，last_writer 就是 Leader
  write_group->size = 1;                 // 批处理组大小初始为 1（只有 Leader）
  Writer* newest_writer = newest_writer_.load(std::memory_order_acquire);  // 原子加载最新的 Writer（链表尾部）

  // This is safe regardless of any db mutex status of the caller. Previous  无论调用者的数据库互斥锁状态如何，这都是安全的。
  // calls to ExitAsGroupLeader either didn't call CreateMissingNewerLinks  之前的 ExitAsGroupLeader 调用要么没有调用 CreateMissingNewerLinks
  // (they emptied the list and then we added ourself as leader) or had to  （它们清空了列表，然后我们将自己添加为 leader）
  // explicitly wake us up (the list was non-empty when we added ourself,  要么必须显式唤醒我们（当我们添加自己时列表非空，
  // so we have already received our MarkJoined).  所以我们已经收到了 MarkJoined）。
  //
  // CreateMissingNewerLinks 的作用：
  // - 确保 link_newer 指针被正确设置（有些情况下 link_newer 是懒加载的）
  // - 遍历从 newest_writer 向后到 leader 的链表，补全 link_newer 指针
  // - 这使得后续可以通过 link_newer 指针遍历整个批处理组
  //
  // 为什么这里是安全的：
  // - 如果之前的 leader 清空了列表，那么我们现在是唯一的 writer，不需要补全 link_newer
  // - 如果之前的 leader 没有清空列表，那么我们已经被唤醒（收到 MarkJoined），说明 link_newer 已设置
  CreateMissingNewerLinks(newest_writer);  // 补全 link_newer 指针

  // Tricky. Iteration start (leader) is exclusive and finish  关键点：遍历起点（leader）是独占的（不包含），
  // (newest_writer) is inclusive. Iteration goes from old to new.  终点（newest_writer）是包含的。遍历从旧到新。
  //
  // 遍历顺序：leader → writer1 → writer2 → ... → newest_writer
  //
  // 注意：
  // - 起点从 leader 开始（不包含 leader，因为我们已经处理了 leader）
  // - 通过 link_newer 指针向后遍历（从旧到新）
  // - 终点是 newest_writer（包含）
  Writer* w = leader;  // 从 leader 开始遍历
  while (w != newest_writer) {  // 当未到达最新 writer 时继续遍历
    assert(w->link_newer);  // 断言：link_newer 必须存在（因为 w != newest_writer）
    w = w->link_newer;  // 移动到下一个更新的 writer

    // 检查配置兼容性，如果不兼容则停止收集
    //
    // 不兼容的情况包括：
    // 1. sync 配置不同（sync vs 不 sync）
    // 2. no_slowdown 配置不同（允许延迟 vs 不允许延迟）
    // 3. disable_wal 配置不同（启用 WAL vs 禁用 WAL）
    // 4. protection_bytes_per_key 不同（完整性保护级别不同）
    // 5. rate_limiter_priority 不同（速率限制优先级不同）
    // 6. batch 为 nullptr（非写入操作）
    // 7. 回调不允许批处理
    // 8. 批处理组大小超过限制

    if (w->sync && !leader->sync) {  // 如果当前 writer 需要 sync，但 Leader 不需要
      // Do not include a sync write into a batch handled by a non-sync write.  不要将 sync 写入加入到由非 sync 写入处理的批处理组中。
      // 原因：sync 写入需要 WAL 持久化，而非 sync 写入不需要，混合会导致语义混乱
      break;  // 停止收集
    }

    if (w->no_slowdown != leader->no_slowdown) {  // 如果 no_slowdown 配置不同
      // Do not mix writes that are ok with delays with the ones that  不要混合可以接受延迟的写入和
      // request fail on delays.  请求延迟时失败的写入。
      // 原因：no_slowdown=true 的写入期望立即执行，不应该被延迟的写入拖慢
      break;  // 停止收集
    }

    if (w->disable_wal != leader->disable_wal) {  // 如果 disable_wal 配置不同
      // Do not mix writes that enable WAL with the ones whose  不要混合启用 WAL 的写入和
      // WAL disabled.  禁用 WAL 的写入。
      // 原因：WAL 的启用/禁用影响数据持久化语义，不能混合
      break;  // 停止收集
    }

    if (w->protection_bytes_per_key != leader->protection_bytes_per_key) {  // 如果完整性保护级别不同
      // Do not mix writes with different levels of integrity protection.  不要混合不同完整性保护级别的写入。
      // 原因：protection_bytes_per_key 控制每条目的校验和长度，不同级别的写入不能混合
      break;  // 停止收集
    }

    if (w->rate_limiter_priority != leader->rate_limiter_priority) {  // 如果速率限制优先级不同
      // Do not mix writes with different rate limiter priorities.  不要混合不同速率限制优先级的写入。
      // 原因：不同优先级的写入应该分开处理，以实现流量控制
      break;  // 停止收集
    }

    if (w->batch == nullptr) {  // 如果当前 writer 的 WriteBatch 为空
      // Do not include those writes with nullptr batch. Those are not writes,  不要包含那些 batch 为 nullptr 的写入。
      // those are something else. They want to be alone  那些不是写入操作，而是其他操作。它们希望单独处理。
      // 原因：nullptr batch 可能表示特殊操作（如 flush、compaction），不应该与普通写入混合
      break;  // 停止收集
    }

    if (w->callback != nullptr && !w->callback->AllowWriteBatching()) {  // 如果有回调且回调不允许批处理
      // don't batch writes that don't want to be batched  不要批处理那些不希望被批处理的写入
      // 原因：某些操作需要原子性，不能与其他操作混合
      break;  // 停止收集
    }

    // 获取当前 writer 的 WriteBatch 大小
    auto batch_size = WriteBatchInternal::ByteSize(w->batch);
    // 检查加入当前 writer 后，批处理组是否超过最大大小
    if (size + batch_size > max_size) {  // 如果当前大小 + batch_size 超过 max_size
      // Do not make batch too big  不要让批处理组太大
      // 原因：过大的批处理组会增加延迟，影响小写入的响应时间
      break;  // 停止收集
    }

    // 所有检查都通过，将当前 writer 加入批处理组
    w->write_group = write_group;        // 设置 writer 的 write_group 指针
    size += batch_size;                  // 累加批处理组总大小
    write_group->last_writer = w;        // 更新批处理组的最后一个 writer
    write_group->size++;                // 增加批处理组大小
  }
  // 测试同步点：用于单元测试，在批处理组构建完成后注入测试逻辑
  TEST_SYNC_POINT_CALLBACK("WriteThread::EnterAsBatchGroupLeader:End", w);
  // 返回批处理组的总字节数
  return size;
}

void WriteThread::EnterAsMemTableWriter(Writer* leader,
                                        WriteGroup* write_group) {
  assert(leader != nullptr);
  assert(leader->link_older == nullptr);
  assert(leader->batch != nullptr);
  assert(write_group != nullptr);

  size_t size = WriteBatchInternal::ByteSize(leader->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = max_write_batch_group_size_bytes;
  const uint64_t min_batch_size_bytes = max_write_batch_group_size_bytes / 8;
  if (size <= min_batch_size_bytes) {
    max_size = size + min_batch_size_bytes;
  }

  leader->write_group = write_group;
  write_group->leader = leader;
  write_group->size = 1;
  Writer* last_writer = leader;

  if (!allow_concurrent_memtable_write_ || !leader->batch->HasMerge()) {
    Writer* newest_writer = newest_memtable_writer_.load();
    CreateMissingNewerLinks(newest_writer);

    Writer* w = leader;
    while (w != newest_writer) {
      assert(w->link_newer);
      w = w->link_newer;

      if (w->batch == nullptr) {
        break;
      }

      if (w->batch->HasMerge()) {
        break;
      }

      if (!allow_concurrent_memtable_write_) {
        auto batch_size = WriteBatchInternal::ByteSize(w->batch);
        if (size + batch_size > max_size) {
          // Do not make batch too big
          break;
        }
        size += batch_size;
      }

      w->write_group = write_group;
      last_writer = w;
      write_group->size++;
    }
  }

  write_group->last_writer = last_writer;
  write_group->last_sequence =
      last_writer->sequence + WriteBatchInternal::Count(last_writer->batch) - 1;
}

void WriteThread::ExitAsMemTableWriter(Writer* /*self*/,
                                       WriteGroup& write_group) {
  Writer* leader = write_group.leader;
  Writer* last_writer = write_group.last_writer;

  Writer* newest_writer = last_writer;
  if (!newest_memtable_writer_.compare_exchange_strong(newest_writer,
                                                       nullptr)) {
    CreateMissingNewerLinks(newest_writer);
    Writer* next_leader = last_writer->link_newer;
    assert(next_leader != nullptr);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_MEMTABLE_WRITER_LEADER);
  }
  Writer* w = leader;
  while (true) {
    if (!write_group.status.ok()) {
      w->status = write_group.status;
    }
    Writer* next = w->link_newer;
    if (w != leader) {
      SetState(w, STATE_COMPLETED);
    }
    if (w == last_writer) {
      break;
    }
    assert(next);
    w = next;
  }
  // Note that leader has to exit last, since it owns the write group.
  SetState(leader, STATE_COMPLETED);
}

// 启动并行 MemTable 写入器
//
// 功能说明：
// 此函数由 Leader 调用，用于启动 Write Group 中所有 Writer 的并行 MemTable 写入。
// 它通过设置每个 Writer 的状态为 STATE_PARALLEL_MEMTABLE_WRITER，并原子地初始化
// 运行计数器，从而让所有 Writer（包括 Leader）开始并行写入 MemTable。
//
// 调用时机：
// - Leader 已完成 WAL 写入和序列号分配
// - Leader 决定使用并行模式（allow_concurrent_memtable_write=true 且 write_group.size > 1）
// - 在 Leader 开始自己写入 MemTable 之前
//
// 并行写入机制：
// 1. 原子设置 write_group->running = write_group->size
//    - running 是原子计数器，跟踪当前正在运行的 writer 数量
//    - 初始化为批处理组的大小
//
// 2. 遍历批处理组中的所有 Writer
//    - 通过 WriteGroup 的迭代器遍历（从 leader 开始，通过 link_newer）
//    - 包括 Leader 和所有 Follower
//
// 3. 设置每个 Writer 的状态为 STATE_PARALLEL_MEMTABLE_WRITER
//    - SetState 会唤醒等待中的 Writer（通过条件变量）
//    - 被唤醒的 Writer 立即开始写入自己的 MemTable
//
// 4. 所有 Writer 并行执行
//    - Leader 调用此函数后，也会开始自己的 MemTable 写入
//    - Follower 被唤醒后，也开始自己的 MemTable 写入
//    - 所有 Writer 使用已分配的序列号独立写入
//
// 执行流程：
// Leader
//   ↓
// 调用 LaunchParallelMemTableWriters()
//   ├─ running = size (原子设置)
//   └─ SetState(all, STATE_PARALLEL_MEMTABLE_WRITER)
//         ├─ Leader: 开始写入 MemTable
//         ├─ Follower1: 被唤醒，开始写入 MemTable
//         ├─ Follower2: 被唤醒，开始写入 MemTable
//         └─ ...
//   ↓
// 所有 Writer 调用 WriteBatchInternal::InsertInto()
//   ├─ Leader: InsertInto(sequence=1000, concurrent_memtable_writes=true)
//   ├─ Follower1: InsertInto(sequence=1005, concurrent_memtable_writes=true)
//   └─ ...
//   ↓
// 每个 Writer 完成后调用 CompleteParallelMemTableWriter()
//   ├─ running-- (原子递减)
//   ├─ 如果不是最后一个，AwaitState(STATE_COMPLETED)
//   └─ 如果是最后一个，执行退出逻辑
//         ├─ 调用 post_memtable_callback
//         ├─ SetLastSequence(last_sequence)
//         └─ SetState(all, STATE_COMPLETED)
//
// 线程安全：
// - write_group->running 是原子变量，多线程安全
// - SetState 会设置 Writer 状态并唤醒等待者
// - 每个 Writer 独立写入自己的 MemTable，使用并发插入机制
//
// 性能考虑：
// - 使用原子计数器（running）协调完成状态，无需显式锁
// - 所有 Writer 并行执行，充分利用多核 CPU
// - 最后一个完成的 Writer 负责更新序列号和唤醒其他 Writer
//
// 参数说明：
// - write_group: 已构建的 Write Group，包含 leader 和所有 follower
//              write_group->size: 批处理组中的 writer 数量
//              write_group->leader: 批处理组的 leader
//
// 相关配置：
// - allow_concurrent_memtable_write: 是否允许并发 MemTable 写入
// - enable_pipelined_write: 是否启用流水线写入模式
//
// 注意事项：
// 1. 此函数调用后，Leader 应该立即开始自己的 MemTable 写入
// 2. 所有 Writer（包括 Leader）都会被设置为 STATE_PARALLEL_MEMTABLE_WRITER
// 3. 运行计数器从 write_group->size 开始递减，达到 1 时最后一个 writer 完成
// 4. 最后一个完成的 writer 负责更新全局 LastSequence 和调用回调
void WriteThread::LaunchParallelMemTableWriters(WriteGroup* write_group) {
  assert(write_group != nullptr);  // 断言：write_group 不能为空

  // 原子设置运行计数器为批处理组的大小
  //
  // write_group->running 的作用：
  // - 跟踪当前正在运行的 writer 数量
  // - 每个 writer 完成后递减此计数器
  // - 当 running 减到 1 时，表示当前 writer 是最后一个完成的
  //
  // 为什么是原子操作：
  // - 多个 writer 会并发调用 CompleteParallelMemTableWriter
  // - 需要保证 running-- 的原子性
  // - 使用 std::atomic::store 初始化，保证多线程可见性
  //
  // 初始值：
  // - running = write_group->size
  // - 例如：size = 4（1 leader + 3 followers），则 running = 4
  //
  // 递减过程：
  // - Writer 1 完成: running-- (4→3)，不是最后一个，等待
  // - Writer 2 完成: running-- (3→2)，不是最后一个，等待
  // - Writer 3 完成: running-- (2→1)，最后一个！执行退出逻辑
  // - Leader: running-- (1→0)，但此时已经退出
  //
  // 注意：
  // - 后缀递减（running--）返回原值，所以原值 = 1 表示当前是最后一个
  write_group->running.store(write_group->size);

  // 遍历批处理组中的所有 writer
  //
  // 遍历顺序：
  // - WriteGroup 的迭代器从 leader 开始
  // - 通过 link_newer 指针向后遍历
  // - 顺序：leader → follower1 → follower2 → ... → last_writer
  //
  // 遍历内容：
  // - 包括 Leader 和所有 Follower
  // - 所有 writer 都会被设置为 STATE_PARALLEL_MEMTABLE_WRITER
  //
  // for (auto w : *write_group) 语法：
  // - 使用 WriteGroup 的迭代器
  // - 等价于：
  //   for (auto it = write_group->begin(); it != write_group->end(); ++it) {
  //     auto w = *it;
  //     SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);
  //   }
  for (auto w : *write_group) {
    // 设置 writer 的状态为 STATE_PARALLEL_MEMTABLE_WRITER
    //
    // SetState 的作用：
    // 1. 原子地设置 Writer 的状态
    // 2. 如果 Writer 处于等待状态（STATE_LOCKED_WAITING），通过条件变量唤醒它
    // 3. 通知 Writer 它可以开始并行写入 MemTable
    //
    // Leader 的处理：
    // - Leader 的状态会被设置为 STATE_PARALLEL_MEMTABLE_WRITER
    // - 但 Leader 不会被唤醒（因为 Leader 已经是活动状态）
    // - Leader 在调用此函数后，直接开始自己的 MemTable 写入
    //
    // Follower 的处理：
    // - Follower 之前在 JoinBatchGroup 中等待
    // - 状态从 STATE_INIT 或 STATE_LOCKED_WAITING 变为 STATE_PARALLEL_MEMTABLE_WRITER
    // - Follower 被唤醒后，开始执行自己的 MemTable 写入逻辑
    //
    // 并发写入：
    // - 所有 writer 被唤醒后，会并发执行 MemTable 写入
    // - 每个 writer 使用自己的序列号和 WriteBatch
    // - MemTable 使用并发插入机制（concurrent_memtable_writes=true）
    //
    // 状态转换：
    // Leader: STATE_GROUP_LEADER → STATE_PARALLEL_MEMTABLE_WRITER
    // Follower: STATE_INIT → STATE_PARALLEL_MEMTABLE_WRITER
    //
    // 后续流程：
    // 所有 writer 在写入完成后调用 CompleteParallelMemTableWriter
    SetState(w, STATE_PARALLEL_MEMTABLE_WRITER);
  }
}

// 创建自适应等待上下文，用于优化 AwaitState 的等待策略
//
// AdaptationContext 的作用：
// - 跟踪历史等待时间
// - 根据历史数据选择最优的等待策略（自旋、yield 或阻塞）
// - 减少不必要的上下文切换
//
// cpmtw_ctx 的命名：
// - cpmtw = Complete Parallel MemTable Writer
// - 这个上下文专门用于 CompleteParallelMemTableWriter 中的 AwaitState 调用
//
// 自适应机制：
// - 记录每次等待的耗时
// - 如果等待时间短，倾向于使用自旋（避免上下文切换开销）
// - 如果等待时间长，倾向于使用阻塞（节省 CPU 资源）
//
// 性能优化：
// - 避免在短等待时使用阻塞（上下文切换通常 > 2.7 微秒）
// - 避免在长等待时使用自旋（浪费 CPU 资源）
static WriteThread::AdaptationContext cpmtw_ctx(
    "CompleteParallelMemTableWriter");

// 完成并行 MemTable 写入的协调函数
//
// 功能说明：
// 此函数由 Leader 和所有 Follower 调用，用于协调并行 MemTable 写入的完成。
// 它通过原子计数器（write_group->running）判断当前 Writer 是否是最后一个完成的，
// 从而决定是等待还是执行退出逻辑。
//
// 调用时机：
// - Writer 完成 MemTable 写入后（WriteBatchInternal::InsertInto 返回）
// - 由 Leader 和所有 Follower 都调用
//
// 两种角色：
// 1. Leader: 在完成自己的 MemTable 写入后调用
// 2. Follower: 在完成自己的 MemTable 写入后调用
//
// 两种结果：
// 1. 不是最后一个: 返回 false，调用者需要等待 STATE_COMPLETED
// 2. 是最后一个: 返回 true，调用者需要执行退出逻辑（更新序列号、调用回调等）
//
// 错误处理：
// - 如果当前 Writer 的写入失败，将错误传播到 write_group->status
// - 最后一个完成的 Writer 会将组状态传播给所有 Writer
//
// 线程安全：
// - write_group->running 是原子变量，后缀递减是原子的
// - write_group->status 的更新使用 leader 的 StateMutex() 保护
//
// 返回值：
// - true: 当前 Writer 是最后一个完成的，需要执行退出逻辑
// - false: 当前 Writer 不是最后一个，需要等待
//
// 参数说明：
// - w: 当前 Writer 指针（可能是 Leader 或 Follower）
//
// 注意事项：
// 1. 调用此函数前，Writer 必须已完成 MemTable 写入
// 2. 如果返回 false，调用者必须调用 AwaitState(STATE_COMPLETED)
// 3. 如果返回 true，调用者必须：
//    - 调用所有 writer 的 post_memtable_callback
//    - 更新全局 LastSequence
//    - 调用 ExitAsBatchGroupFollower 唤醒其他 Writer
// 4. 最后一个完成的 Writer 负责所有退出逻辑
// This method is called by both the leader and parallel followers  此方法由 leader 和并行 followers 都会调用
bool WriteThread::CompleteParallelMemTableWriter(Writer* w) {
  // 获取当前 Writer 所属的 Write Group
  //
  // write_group 包含的信息：
  // - leader: 批处理组的 leader
  // - last_writer: 批处理组的最后一个 writer
  // - size: 批处理组中的 writer 数量
  // - running: 当前正在运行的 writer 数量（原子计数器）
  // - status: 批处理组的整体状态
  // - last_sequence: 批处理组的最后一个序列号
  //
  // 为什么需要 write_group：
  // - 需要访问 write_group->running（运行计数器）
  // - 需要访问 write_group->status（组状态）
  // - 需要访问 write_group->leader（用于获取锁）
  auto* write_group = w->write_group;

  // 错误处理：如果当前 Writer 的写入失败，传播错误到整个组
  //
  // 检查条件：!w->status.ok()
  // - 如果 w->status 不为 OK，表示 MemTable 写入失败
  // - 失败原因可能是：内存不足、MemTable 满等
  //
  // 为什么需要锁保护：
  // - 多个 Writer 可能并发调用此函数
  // - write_group->status 是共享状态
  // - 需要保证写入操作的原子性
  //
  // 为什么使用 leader 的 StateMutex()：
  // - StateMutex() 是每个 Writer 独立的互斥量
  // - 使用 leader 的互斥量可以避免多个 Writer 竞争不同的锁
  // - 简化了锁的层次结构
  //
  // lock_guard 的作用：
  // - 构造时自动加锁
  // - 析构时自动解锁（RAII）
  // - 即使异常发生也能保证锁被释放
  //
  // 错误传播机制：
  // - 任何一个 Writer 失败，整个组都失败
  // - write_group->status 保存第一个失败的状态
  // - 后续 Writer 会看到这个错误并传播给调用者
  if (!w->status.ok()) {
    // 获取 leader 的状态互斥量（RAII，自动解锁）
    std::lock_guard<std::mutex> guard(write_group->leader->StateMutex());
    // 将当前 Writer 的错误状态传播到整个批处理组
    // 其他 Writer 会通过 w->status = write_group->status 获取这个错误
    write_group->status = w->status;
  }

  // 检查是否是最后一个完成的 Writer
  //
  // write_group->running-- 的作用：
  // - 后缀递减，返回递减前的原值
  // - 例如：running = 3，执行 running-- 后，running = 2，返回 3
  //
  // 判断逻辑：write_group->running-- > 1
  // - 如果原值 > 1，说明还有其他 Writer 在运行
  // - 如果原值 <= 1（即原值为 1），说明当前是最后一个
  //
  // 示例场景（4 个 Writer）：
  // 初始: running = 4
  // Writer1 完成: running-- (4→3), 返回 4 > 1 → 不是最后一个，等待
  // Writer2 完成: running-- (3→2), 返回 3 > 1 → 不是最后一个，等待
  // Writer3 完成: running-- (2→1), 返回 2 > 1 → 不是最后一个，等待
  // Writer4 完成: running-- (1→0), 返回 1 > 1 → false! 最后一个！
  //
  // 为什么不是 >= 1：
  // - 如果 running = 0（所有 Writer 都已完成），不应该发生
  // - 正常情况下，至少有一个 Writer 会调用此函数
  // - 使用 > 1 可以提前发现异常情况
  //
  // 原子操作保证：
  // - running 是 std::atomic<size_t>
  // - 后缀递减是原子的（fetch_sub）
  // - 多个线程并发调用也能正确计数
  if (write_group->running-- > 1) {
    // we're not the last one  我们不是最后一个完成的

    // 等待最后一个 Writer 设置 STATE_COMPLETED 状态
    //
    // AwaitState 的作用：
    // - 等待当前 Writer 的状态变为 STATE_COMPLETED
    // - 使用自适应的自旋-让出-阻塞策略
    // - 被最后一个 Writer 的 SetState 调用唤醒
    //
    // 参数说明：
    // - w: 要等待的 Writer（当前 Writer）
    // - STATE_COMPLETED: 目标状态
    // - &cpmtw_ctx: 自适应等待上下文（用于优化等待策略）
    //
    // 等待策略（三阶段）：
    // 1. 忙等待（约 1 微秒）：使用 pause 指令自旋
    // 2. 让出自旋（最多 max_yield_usec_ 微秒）：调用 yield()
    // 3. 阻塞等待：使用条件变量和互斥量阻塞
    //
    // 谁来唤醒：
    // - 最后一个完成的 Writer 调用 ExitAsBatchGroupFollower
    // - ExitAsBatchGroupFollower 调用 SetState(w, STATE_COMPLETED)
    // - SetState 会唤醒等待中的 Writer（通过条件变量）
    //
    // 为什么需要等待：
    // - 非最后一个 Writer 不应该执行退出逻辑
    // - 退出逻辑只需要执行一次（由最后一个 Writer 执行）
    // - 其他 Writer 需要等待退出逻辑完成后才能返回
    AwaitState(w, STATE_COMPLETED, &cpmtw_ctx);

    // 返回 false，表示当前 Writer 不是最后一个
    // 调用者（db_impl_write.cc）需要：
    // - 不执行退出逻辑
    // - 直接检查 w->status 并返回
    return false;
  }

  // else we're the last parallel worker and should perform exit duties.  否则我们是最后一个并行 worker，应该执行退出职责。

  // 将批处理组的整体状态复制到当前 Writer 的状态
  //
  // write_group->status 包含：
  // - 如果所有 Writer 都成功：Status::OK()
  // - 如果有 Writer 失败：第一个失败的状态
  //
  // 为什么需要复制：
  // - 当前 Writer 需要返回正确的状态给调用者
  // - 调用者会检查 w->status 来判断写入是否成功
  // - 如果有 Writer 失败，当前 Writer 也应该返回失败
  //
  // 调用者如何使用：
  // - 在 db_impl_write.cc:608 调用 MemTableInsertStatusCheck(w.status)
  // - 检查状态，如果失败则处理错误
  w->status = write_group->status;

  // 允许 write_group->status 包含的 Status 对象被静默销毁
  //
  // PermitUncheckedError 的作用：
  // - 标记 Status 对象的错误已经被检查过
  // - 避免 Status 析构函数的断言失败（"status was not checked"）
  // - 在错误已经传播给调用者后调用
  //
  // 为什么需要调用：
  // - RocksDB 的 Status 对象有严格的使用规则
  // - 如果 Status 包含错误但未被检查，析构时会触发断言
  // - 这里错误已经传播给 w->status，可以安全地销毁
  //
  // 谁来检查 write_group->status：
  // - 实际上没有其他地方检查 write_group->status
  // - 只是通过 w->status 传播给调用者
  // - 所以此处调用 PermitUncheckedError 避免断言失败
  //
  // Callers of this function must ensure w->status is checked.  调用此函数的代码必须确保检查了 w->status。
  write_group->status.PermitUncheckedError();

  // 返回 true，表示当前 Writer 是最后一个完成的
  // 调用者（db_impl_write.cc）需要：
  // 1. 调用所有 writer 的 post_memtable_callback（589-602行）
  // 2. 更新全局 LastSequence（605行）
  // 3. 调用 MemTableInsertStatusCheck 检查状态（608行）
  // 4. 调用 ExitAsBatchGroupFollower 唤醒其他 Writer（611行）
  return true;
}

void WriteThread::ExitAsBatchGroupFollower(Writer* w) {
  auto* write_group = w->write_group;

  assert(w->state == STATE_PARALLEL_MEMTABLE_WRITER);
  assert(write_group->status.ok());
  ExitAsBatchGroupLeader(*write_group, write_group->status);
  assert(w->status.ok());
  assert(w->state == STATE_COMPLETED);
  SetState(write_group->leader, STATE_COMPLETED);
}

// 创建自适应等待上下文，用于 ExitAsBatchGroupLeader 中的 AwaitState 调用
//
// eabgl_ctx 的命名：
// - eabgl = Exit As Batch Group Leader
// - 这个上下文专门用于 ExitAsBatchGroupLeader 中的 AwaitState 调用
static WriteThread::AdaptationContext eabgl_ctx("ExitAsBatchGroupLeader");

// 退出批处理组（Batch Group Leader 的退出函数）
//
// 功能说明：
// 此函数由 Batch Group Leader 调用，用于完成批处理组的退出工作。
// 它负责：
// 1. 处理错误状态的传播
// 2. 根据 enable_pipelined_write_ 选择不同的退出策略
// 3. 完成（唤醒）所有 Follower Writer
// 4. 选择下一个 Leader
// 5. 清理链表和资源
//
// 参数说明：
// - write_group: 要退出的批处理组（包含 leader、last_writer、size、status 等）
// - status: 传入的状态引用，可能被 write_group.status 覆盖
void WriteThread::ExitAsBatchGroupLeader(WriteGroup& write_group,
                                         Status& status) {
  // 测试同步点：用于单元测试，在函数开始时注入测试逻辑
  TEST_SYNC_POINT_CALLBACK("WriteThread::ExitAsBatchGroupLeader:Start",
                           &write_group);

  // 获取批处理组的 leader 指针（第一个 writer，最早到达）
  Writer* leader = write_group.leader;
  // 获取批处理组的最后一个 writer 指针（最新到达）
  Writer* last_writer = write_group.last_writer;
  // 断言：leader 必须是链表头部（link_older == nullptr）
  assert(leader->link_older == nullptr);

  // If status is non-ok already, then write_group.status won't have the chance  如果 status 已经非 OK，则 write_group.status 不会有传播给调用者的机会。
  // of being propagated to caller.
  if (!status.ok()) {
    write_group.status.PermitUncheckedError();
  }

  // Propagate memtable write error to the whole group.  将 MemTable 写入错误传播到整个组。
  if (status.ok() && !write_group.status.ok()) {
    status = write_group.status;  // 将批处理组的错误传播到 status 参数
  }

  // 根据 enable_pipelined_write_ 选择不同的退出策略
  if (enable_pipelined_write_) {
    // ============================================================================
    // 流水线写入模式的退出逻辑
    // ============================================================================
    //
    // 流水线模式的特点：
    // - WAL 写入和 MemTable 写入可以并行
    // - Leader 完成 WAL 写入后，立即可以处理下一批
    // - MemTable 写入由专门的 memtable writer leader 处理

    // We insert a dummy Writer right before our current write_group. This  我们在当前的批处理组之前插入一个 dummy Writer。
    // allows us to unlink our write_group without the risk that a subsequent  这允许我们安全地断开我们的批处理组，
    // writer becomes a new leader and might overtake us and add itself to the  而不用担心后续的 writer 成为新 leader
    // memtable-writer-list before we can do so. This ensures that writers are  并在我们之前将其添加到 memtable-writer-list。
    // added to the memtable-writer-list in the exact same order in which they  这确保了 writer 按照在 newest_writer 列表中的确切顺序
    // were in the newest_writer list.  被添加到 memtable-writer-list。
    // This must happen before completing the writers from our group to prevent  这必须在完成我们组中的 writer 之前发生，以防止
    // a race where the owning thread of one of these writers can start a new  一种竞争：这些 writer 的拥有线程可以开始新的写入操作。
    Writer dummy;  // 创建占位符 writer

    // 原子加载最新的 writer（newest_writer_ 的当前值）
    // memory_order_acquire: 确保后续操作能看到最新的链表状态
    Writer* head = newest_writer_.load(std::memory_order_acquire);

    // 尝试使用 CAS 将 dummy 插入到 newest_writer_ 链表
    // 条件：head == last_writer（即 newest_writer_ 就是我们的 last_writer）
    if (head != last_writer ||
        !newest_writer_.compare_exchange_strong(head, &dummy)) {
      // CAS 失败：有新的 writer 到达，需要特殊处理

      // Either last_writer wasn't the head during the load(), or it was the  要么 last_writer 在 load() 期间不是 head，
      // head during the load() but somebody else pushed onto the list before  要么在 load() 期间是 head，
      // we did the compare_exchange_strong (causing it to fail). In the latter  但在我们执行 compare_exchange_strong 之前其他人推入了列表
      // case compare_exchange_strong has the effect of re-reading its first  （导致 CAS 失败）。在后一种情况下，
      // param (head). No need to retry a failing CAS, because only a departing  compare_exchange_strong 会重新读取第一个参数（head）。
      // leader (which we are at the moment) can remove nodes from the list.  不需要重试失败的 CAS，因为只有离任的 leader（我们）可以移除节点。
      assert(head != last_writer);

      // After walking link_older starting from head (if not already done) we  从 head 开始沿着 link_older 遍历后（如果还没做），
      // will be able to traverse w->link_newer below.  我们将能够遍历下面的 w->link_newer。
      CreateMissingNewerLinks(head);  // 补全 link_newer 指针

      // 断言：last_writer 必须有 link_newer（因为有新的 writer 到达）
      assert(last_writer->link_newer != nullptr);

      // 将 last_writer 的下一个 writer 的 link_older 指向 dummy
      // 这样 dummy 就成为新的链表分割点
      last_writer->link_newer->link_older = &dummy;

      // 设置 dummy 的 link_newer 指向 last_writer 的下一个 writer
      dummy.link_newer = last_writer->link_newer;
    }

    // Complete writers that don't write to memtable  完成不写入 MemTable 的 writer
    //
    // 遍历范围：从 last_writer 开始，向前遍历到 leader（不包含 leader）
    //
    // 为什么需要单独处理：
    // - 不写入 MemTable 的 writer 不需要等待 MemTable 写入
    // - 可以直接唤醒，让它们的线程返回
    for (Writer* w = last_writer; w != leader;) {
      Writer* next = w->link_older;  // 保存下一个 writer，防止 SetState 后 w 被释放

      // 将传入的 status 传播给 writer
      w->status = status;

      // 如果当前 writer 不写入 MemTable，完成这个 follower
      if (!w->ShouldWriteToMemtable()) {
        CompleteFollower(w, write_group);  // 设置状态为 STATE_COMPLETED 并唤醒
      }

      // 移动到下一个 writer
      w = next;
    }

    // 如果 leader 也不写入 MemTable，完成 leader
    if (!leader->ShouldWriteToMemtable()) {
      CompleteLeader(write_group);
    }

    // 测试同步点：用于单元测试
    TEST_SYNC_POINT_CALLBACK(
        "WriteThread::ExitAsBatchGroupLeader:AfterCompleteWriters",
        &write_group);

    // Link the remaining of the group to memtable writer list.  将批处理组的剩余部分链接到 memtable writer 列表。
    // We have to link our group to memtable writer queue before wake up  我们必须在唤醒下一个 leader 或设置 newest_writer_ 为 null 之前，
    // next leader or set newest_writer_ to null, otherwise the next leader  将我们的组链接到 memtable writer 队列。
    // can run ahead of us and link to memtable writer queue before we do.  否则下一个 leader 可能会在我们之前运行并链接到 memtable writer 队列。
    if (write_group.size > 0) {  // 如果还有 writer 需要写入 MemTable
      if (LinkGroup(write_group, &newest_memtable_writer_)) {  // 尝试链接到 memtable writer 列表
        // The leader can now be different from current writer.  Leader 现在可能已经不同于当前的 writer。
        // 设置批处理组的 leader 状态为 STATE_MEMTABLE_WRITER_LEADER
        SetState(write_group.leader, STATE_MEMTABLE_WRITER_LEADER);
      }
    }

    // Unlink the dummy writer from the list and identify the new leader
    head = newest_writer_.load(std::memory_order_acquire);
    if (head != &dummy ||
        !newest_writer_.compare_exchange_strong(head, nullptr)) {
      CreateMissingNewerLinks(head);
      Writer* new_leader = dummy.link_newer;
      assert(new_leader != nullptr);
      new_leader->link_older = nullptr;
      SetState(new_leader, STATE_GROUP_LEADER);
    }

    AwaitState(leader,
               STATE_MEMTABLE_WRITER_LEADER | STATE_PARALLEL_MEMTABLE_WRITER |
                   STATE_COMPLETED,
               &eabgl_ctx);
  } else {
    Writer* head = newest_writer_.load(std::memory_order_acquire);
    if (head != last_writer ||
        !newest_writer_.compare_exchange_strong(head, nullptr)) {
      // Either last_writer wasn't the head during the load(), or it was the
      // head during the load() but somebody else pushed onto the list before
      // we did the compare_exchange_strong (causing it to fail).  In the
      // latter case compare_exchange_strong has the effect of re-reading
      // its first param (head).  No need to retry a failing CAS, because
      // only a departing leader (which we are at the moment) can remove
      // nodes from the list.
      assert(head != last_writer);

      // After walking link_older starting from head (if not already done)
      // we will be able to traverse w->link_newer below. This function
      // can only be called from an active leader, only a leader can
      // clear newest_writer_, we didn't, and only a clear newest_writer_
      // could cause the next leader to start their work without a call
      // to MarkJoined, so we can definitely conclude that no other leader
      // work is going on here (with or without db mutex).
      CreateMissingNewerLinks(head);
      assert(last_writer->link_newer != nullptr);
      assert(last_writer->link_newer->link_older == last_writer);
      last_writer->link_newer->link_older = nullptr;

      // Next leader didn't self-identify, because newest_writer_ wasn't
      // nullptr when they enqueued (we were definitely enqueued before them
      // and are still in the list).  That means leader handoff occurs when
      // we call MarkJoined
      SetState(last_writer->link_newer, STATE_GROUP_LEADER);
    }
    // else nobody else was waiting, although there might already be a new
    // leader now

    while (last_writer != leader) {
      assert(last_writer);
      last_writer->status = status;
      // we need to read link_older before calling SetState, because as soon
      // as it is marked committed the other thread's Await may return and
      // deallocate the Writer.
      auto next = last_writer->link_older;
      SetState(last_writer, STATE_COMPLETED);

      last_writer = next;
    }
  }
}

// 用于 EnterUnbatched 中等待时的自适应休眠统计上下文
static WriteThread::AdaptationContext eu_ctx("EnterUnbatched");

// 以「非批处理」方式加入写队列：当前线程独占成为 leader，不与其它写请求合并。
// 用于需要单独跑完再让出队列的操作（如 FlushMemTable、SwitchMemtable 前的逻辑）。
// 调用前需保证 w->batch == nullptr，这样前面的 leader 不会把本 writer 选为 follower。
void WriteThread::EnterUnbatched(Writer* w, InstrumentedMutex* mu) {
  // 必须传入有效 writer，且 batch 为空，表示非普通写请求、不参与批处理
  assert(w != nullptr && w->batch == nullptr);
  // 先释放外部传入的互斥锁，避免在 LinkOne / 等待时长时间持锁阻塞其它线程
  mu->Unlock();
  // 将本 writer 链接到 newest_writer_ 队列末尾；返回 true 表示当前没有更新的 writer，本 writer 已是队头（即成为 leader）
  bool linked_as_leader = LinkOne(w, &newest_writer_);
  if (!linked_as_leader) { //返回 true 表示当前没有更新的 writer，本 writer 已是队头（即已成为 leader）。
    TEST_SYNC_POINT("WriteThread::EnterUnbatched:Wait");
    // 队列中已有更新的 writer，本 writer 暂时不是 leader；因 batch 为 nullptr，前面的 leader 不会选我们为 follower，只能等前面的人全部完成后我们才会被置为 STATE_GROUP_LEADER
    AwaitState(w, STATE_GROUP_LEADER, &eu_ctx);
  }
  // 若开启流水线写，还需等待所有已提交的 memtable 写完成，保证与 MemTable 写的顺序一致
  if (enable_pipelined_write_) {
    WaitForMemTableWriters();
  }
  // 重新获取外部互斥锁，与调用方持锁约定一致
  mu->Lock();
}

void WriteThread::ExitUnbatched(Writer* w) {
  assert(w != nullptr);
  Writer* newest_writer = w;
  if (!newest_writer_.compare_exchange_strong(newest_writer, nullptr)) {
    CreateMissingNewerLinks(newest_writer);
    Writer* next_leader = w->link_newer;
    assert(next_leader != nullptr);
    next_leader->link_older = nullptr;
    SetState(next_leader, STATE_GROUP_LEADER);
  }
}

static WriteThread::AdaptationContext wfmw_ctx("WaitForMemTableWriters");
void WriteThread::WaitForMemTableWriters() {
  assert(enable_pipelined_write_);
  if (newest_memtable_writer_.load() == nullptr) {
    return;
  }
  Writer w;
  if (!LinkOne(&w, &newest_memtable_writer_)) {
    AwaitState(&w, STATE_MEMTABLE_WRITER_LEADER, &wfmw_ctx);
  }
  newest_memtable_writer_.store(nullptr);
}

}  // namespace ROCKSDB_NAMESPACE
