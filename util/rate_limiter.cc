//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>

#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {
size_t RateLimiter::RequestToken(size_t bytes, size_t alignment,
                                 Env::IOPriority io_priority, Statistics* stats,
                                 RateLimiter::OpType op_type) {
  if (io_priority < Env::IO_TOTAL && IsRateLimited(op_type)) {
    bytes = std::min(bytes, static_cast<size_t>(GetSingleBurstBytes()));

    if (alignment > 0) {
      // Here we may actually require more than burst and block
      // as we can not write/read less than one page at a time on direct I/O
      // thus we do not want to be strictly constrained by burst
      bytes = std::max(alignment, TruncateToPageBoundary(alignment, bytes));
    }
    Request(bytes, io_priority, stats, op_type);
  }
  return bytes;
}

// Pending request: 待处理的请求结构体
struct GenericRateLimiter::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu)  // 构造函数：请求数据和互斥量
      : request_bytes(_bytes), bytes(_bytes), cv(_mu) {}  // 初始化请求字节数、原始字节数和条件变量
  int64_t request_bytes;  // 请求的字节数（可能被部分批准）
  int64_t bytes;  // 原始请求字节数（保持不变）
  port::CondVar cv;  // 条件变量：用于等待唤醒
};

// GenericRateLimiter 构造函数：初始化通用限流器
// 参数说明：
// - rate_bytes_per_sec: 速率限制（字节/秒）
// - refill_period_us: 补充周期（微秒）
// - fairness: 公平性参数（1-100），值越大越公平
// - mode: 限流器模式（只读或读写）
// - clock: 系统时钟
// - auto_tuned: 是否启用自动调优
GenericRateLimiter::GenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us, int32_t fairness,  // 速率、周期、公平性参数
    RateLimiter::Mode mode, const std::shared_ptr<SystemClock>& clock,  // 模式和时钟
    bool auto_tuned)  // 是否自动调优
    : RateLimiter(mode),  // 调用基类构造函数
      refill_period_us_(refill_period_us),  // 初始化补充周期（微秒）
      rate_bytes_per_sec_(auto_tuned ? rate_bytes_per_sec / 2  // 如果自动调优，从半速率开始
                                     : rate_bytes_per_sec),  // 否则使用指定速率
      refill_bytes_per_period_(  // 计算并初始化每周期补充的字节数
          CalculateRefillBytesPerPeriodLocked(rate_bytes_per_sec_)),  // 调用计算函数
      clock_(clock),  // 初始化系统时钟
      stop_(false),  // 初始化停止标志为 false
      exit_cv_(&request_mutex_),  // 初始化退出条件变量（关联请求互斥量）
      requests_to_wait_(0),  // 初始化待等待请求数为 0
      available_bytes_(0),  // 初始化可用字节数为 0
      next_refill_us_(NowMicrosMonotonicLocked()),  // 设置下次补充时间为当前时间
      fairness_(fairness > 100 ? 100 : fairness),  // 初始化公平性参数（限制最大值为 100）
      rnd_((uint32_t)time(nullptr)),  // 用当前时间初始化随机数生成器
      wait_until_refill_pending_(false),  // 初始化等待补充标志为 false
      auto_tuned_(auto_tuned),  // 初始化自动调优标志
      num_drains_(0),  // 初始化排空计数为 0
      max_bytes_per_sec_(rate_bytes_per_sec),  // 保存最大速率（字节/秒）
      tuned_time_(NowMicrosMonotonicLocked()) {  // 初始化上次调优时间为当前时间
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {  // 遍历所有 IO 优先级
    total_requests_[i] = 0;  // 初始化每个优先级的总请求数为 0
    total_bytes_through_[i] = 0;  // 初始化每个优先级的通过字节数为 0
  }
}

GenericRateLimiter::~GenericRateLimiter() {
  MutexLock g(&request_mutex_);
  stop_ = true;
  std::deque<Req*>::size_type queues_size_sum = 0;
  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {
    queues_size_sum += queue_[i].size();
  }
  requests_to_wait_ = static_cast<int32_t>(queues_size_sum);

  for (int i = Env::IO_TOTAL - 1; i >= Env::IO_LOW; --i) {
    std::deque<Req*> queue = queue_[i];
    for (auto& r : queue) {
      r->cv.Signal();
    }
  }

  while (requests_to_wait_ > 0) {
    exit_cv_.Wait();
  }
}

// This API allows user to dynamically change rate limiter's bytes per second.
void GenericRateLimiter::SetBytesPerSecond(int64_t bytes_per_second) {
  MutexLock g(&request_mutex_);
  SetBytesPerSecondLocked(bytes_per_second);
}

void GenericRateLimiter::SetBytesPerSecondLocked(int64_t bytes_per_second) {
  assert(bytes_per_second > 0);
  rate_bytes_per_sec_.store(bytes_per_second, std::memory_order_relaxed);
  refill_bytes_per_period_.store(
      CalculateRefillBytesPerPeriodLocked(bytes_per_second),
      std::memory_order_relaxed);
}

// GenericRateLimiter::Request: 请求指定字节数的配额
// bytes: 请求的字节数
// pri: IO优先级
// stats: 统计信息对象
void GenericRateLimiter::Request(int64_t bytes, const Env::IOPriority pri,  // 请求参数：字节数和优先级
                                 Statistics* stats) {  // 统计信息
  assert(bytes <= refill_bytes_per_period_.load(std::memory_order_relaxed));  // 断言：请求字节数不超过每周期补充字节数
  bytes = std::max(static_cast<int64_t>(0), bytes);  // 确保字节数非负
  TEST_SYNC_POINT("GenericRateLimiter::Request");  // 测试同步点
  TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:1",  // 测试同步点回调
                           &rate_bytes_per_sec_);  // 传入速率参数
  MutexLock g(&request_mutex_);  // 加锁请求互斥量

  if (auto_tuned_) {  // 如果启用了自动调优
    static const int kRefillsPerTune = 100;  // 每100次补充进行一次调优
    std::chrono::microseconds now(NowMicrosMonotonicLocked());  // 获取当前时间
    if (now - tuned_time_ >=  // 如果距离上次调优的时间超过阈值
        kRefillsPerTune * std::chrono::microseconds(refill_period_us_)) {  // 100个补充周期
      Status s = TuneLocked();  // 执行调优
      s.PermitUncheckedError();  //**TODO: What to do on error? 错误处理待定
    }
  }

  if (stop_) {  // 如果限流器已停止
    // It is now in the clean-up of ~GenericRateLimiter().
    // Therefore any new incoming request will exit from here
    // and not get satiesfied.
    // 正在清理 GenericRateLimiter 析构函数，因此任何新的传入请求将从此处退出，不会被满足
    return;  // 直接返回
  }

  ++total_requests_[pri];  // 增加该优先级的总请求数

  if (available_bytes_ > 0) {  // 如果有可用字节数
    int64_t bytes_through = std::min(available_bytes_, bytes);  // 计算可通过的字节数
    total_bytes_through_[pri] += bytes_through;  // 统计该优先级的通过字节数
    available_bytes_ -= bytes_through;  // 减少可用字节数
    bytes -= bytes_through;  // 减少剩余需要的字节数
  }

  if (bytes == 0) {  // 如果请求已被完全满足
    return;  // 直接返回
  }

  // Request cannot be satisfied at this moment, enqueue
  // 请求无法立即满足，加入队列
  Req r(bytes, &request_mutex_);  // 创建请求对象
  queue_[pri].push_back(&r);  // 将请求加入对应优先级的队列
  TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:PostEnqueueRequest",  // 测试同步点：入队后
                           &request_mutex_);  // 传入互斥量
  // A thread representing a queued request coordinates with other such threads.
  // There are two main duties.
  //
  // (1) Waiting for the next refill time.
  // (2) Refilling the bytes and granting requests.
  // 代表排队请求的线程与其他线程协调。有两个主要职责：
  // (1) 等待下一次补充时间
  // (2) 补充字节并批准请求
  do {  // 循环处理请求
    int64_t time_until_refill_us = next_refill_us_ - NowMicrosMonotonicLocked();  // 计算距离下次补充的微秒数
    if (time_until_refill_us > 0) {  // 如果还未到补充时间
      if (wait_until_refill_pending_) {  // 如果已有线程在等待补充
        // Somebody is performing (1). Trust we'll be woken up when our request
        // is granted or we are needed for future duties.
        // 已有线程在执行(1)，信任会在请求被批准或需要执行未来职责时被唤醒
        r.cv.Wait();  // 等待条件变量通知
      } else {  // 如果没有线程在等待补充
        // Whichever thread reaches here first performs duty (1) as described
        // above.
        // 第一个到达此处的线程执行上述职责(1)
        int64_t wait_until = clock_->NowMicros() + time_until_refill_us;  // 计算等待截止时间
        RecordTick(stats, NUMBER_RATE_LIMITER_DRAINS);  // 记录限流器排空次数
        ++num_drains_;  // 增加排空计数
        wait_until_refill_pending_ = true;  // 标记正在等待补充
        r.cv.TimedWait(wait_until);  // 定时等待
        TEST_SYNC_POINT_CALLBACK("GenericRateLimiter::Request:PostTimedWait",  // 测试同步点：定时等待后
                                 &time_until_refill_us);  // 传入等待时间
        wait_until_refill_pending_ = false;  // 清除等待标记
      }
    } else {  // 已到补充时间
      // Whichever thread reaches here first performs duty (2) as described
      // above.
      // 第一个到达此处的线程执行上述职责(2)
      RefillBytesAndGrantRequestsLocked();  // 补充字节并批准请求
    }
    if (r.request_bytes == 0) {  // 如果请求已被完全满足
      // If there is any remaining requests, make sure there exists at least
      // one candidate is awake for future duties by signaling a front request
      // of a queue.
      // 如果还有剩余请求，确保至少有一个候选者处于唤醒状态以执行未来职责，通过信号通知队列的前端请求
      for (int i = Env::IO_TOTAL - 1; i >= Env::IO_LOW; --i) {  // 从高到低优先级遍历
        auto& queue = queue_[i];  // 获取队列引用
        if (!queue.empty()) {  // 如果队列不为空
          queue.front()->cv.Signal();  // 通知队列前端的请求
          break;  // 跳出循环
        }
      }
    }
    // Invariant: non-granted request is always in one queue, and granted
    // request is always in zero queues.
    // 不变式：未批准的请求总是在一个队列中，已批准的请求不在任何队列中
#ifndef NDEBUG
    int num_found = 0;  // 找到的次数
    for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {  // 遍历所有优先级
      if (std::find(queue_[i].begin(), queue_[i].end(), &r) !=  // 在队列中查找请求
          queue_[i].end()) {  // 如果找到
        ++num_found;  // 增加计数
      }
    }
    if (r.request_bytes == 0) {  // 如果请求已完成
      assert(num_found == 0);  // 断言：不应在队列中
    } else {  // 如果请求未完成
      assert(num_found == 1);  // 断言：应在队列中
    }
#endif  // NDEBUG
  } while (!stop_ && r.request_bytes > 0);  // 循环直到停止或请求完成

  if (stop_) {  // 如果限流器已停止
    // It is now in the clean-up of ~GenericRateLimiter().
    // Therefore any woken-up request will have come out of the loop and then
    // exit here. It might or might not have been satisfied.
    // 正在清理 GenericRateLimiter，因此任何被唤醒的请求将退出循环，然后在此处退出。
    // 请求可能已被满足，也可能未被满足。
    --requests_to_wait_;  // 减少待等待的请求数
    exit_cv_.Signal();  // 通知退出条件变量
  }
}

std::vector<Env::IOPriority>
GenericRateLimiter::GeneratePriorityIterationOrderLocked() {
  std::vector<Env::IOPriority> pri_iteration_order(Env::IO_TOTAL /* 4 */);
  // We make Env::IO_USER a superior priority by always iterating its queue
  // first
  pri_iteration_order[0] = Env::IO_USER;

  bool high_pri_iterated_after_mid_low_pri = rnd_.OneIn(fairness_);
  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
      "PostRandomOneInFairnessForHighPri",
      &high_pri_iterated_after_mid_low_pri);
  bool mid_pri_itereated_after_low_pri = rnd_.OneIn(fairness_);
  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
      "PostRandomOneInFairnessForMidPri",
      &mid_pri_itereated_after_low_pri);

  if (high_pri_iterated_after_mid_low_pri) {
    pri_iteration_order[3] = Env::IO_HIGH;
    pri_iteration_order[2] =
        mid_pri_itereated_after_low_pri ? Env::IO_MID : Env::IO_LOW;
    pri_iteration_order[1] =
        (pri_iteration_order[2] == Env::IO_MID) ? Env::IO_LOW : Env::IO_MID;
  } else {
    pri_iteration_order[1] = Env::IO_HIGH;
    pri_iteration_order[3] =
        mid_pri_itereated_after_low_pri ? Env::IO_MID : Env::IO_LOW;
    pri_iteration_order[2] =
        (pri_iteration_order[3] == Env::IO_MID) ? Env::IO_LOW : Env::IO_MID;
  }

  TEST_SYNC_POINT_CALLBACK(
      "GenericRateLimiter::GeneratePriorityIterationOrderLocked::"
      "PreReturnPriIterationOrder",
      &pri_iteration_order);
  return pri_iteration_order;
}

// RefillBytesAndGrantRequestsLocked: 补充配额并批准请求（已持有锁）
// 在每个补充周期到来时，补充可用字节数并批准队列中的等待请求
void GenericRateLimiter::RefillBytesAndGrantRequestsLocked() {
  TEST_SYNC_POINT_CALLBACK(  // 测试同步点回调
      "GenericRateLimiter::RefillBytesAndGrantRequestsLocked", &request_mutex_);  // 传入互斥量
  next_refill_us_ = NowMicrosMonotonicLocked() + refill_period_us_;  // 设置下次补充的时间
  // Carry over the left over quota from the last period
  // 结转上一个周期的剩余配额
  auto refill_bytes_per_period =  // 获取每周期应补充的字节数
      refill_bytes_per_period_.load(std::memory_order_relaxed);  // 原子加载
  assert(available_bytes_ == 0);  // 断言：可用字节数应该为0（因为已被完全使用）
  available_bytes_ = refill_bytes_per_period;  // 补充可用字节数

  std::vector<Env::IOPriority> pri_iteration_order =  // 生成优先级遍历顺序
      GeneratePriorityIterationOrderLocked();  // 按公平性原则生成优先级顺序

  for (int i = Env::IO_LOW; i < Env::IO_TOTAL; ++i) {  // 遍历所有优先级
    assert(!pri_iteration_order.empty());  // 断言：遍历顺序不为空
    Env::IOPriority current_pri = pri_iteration_order[i];  // 获取当前处理的优先级
    auto* queue = &queue_[current_pri];  // 获取对应优先级的队列
    while (!queue->empty()) {  // 遍历队列中的所有请求
      auto* next_req = queue->front();  // 获取队列前端的请求
      if (available_bytes_ < next_req->request_bytes) {  // 如果可用字节不足以满足请求
        // Grant partial request_bytes to avoid starvation of requests
        // that become asking for more bytes than available_bytes_
        // due to dynamically reduced rate limiter's bytes_per_second that
        // leads to reduced refill_bytes_per_period hence available_bytes_
        // 部分批准请求字节数，以避免请求饥饿。
        // 这些请求可能因为动态降低的限流器速率导致请求字节数超过可用字节数，
        // 进而导致 reduced refill_bytes_per_period 和 available_bytes_
        next_req->request_bytes -= available_bytes_;  // 从请求中扣除可用字节数
        available_bytes_ = 0;  // 将可用字节清零
        break;  // 退出循环，等待下次补充
      }
      available_bytes_ -= next_req->request_bytes;  // 减少可用字节数
      next_req->request_bytes = 0;  // 标记请求已完成
      total_bytes_through_[current_pri] += next_req->bytes;  // 统计该优先级的通过字节数
      queue->pop_front();  // 从队列中移除已完成的请求

      // Quota granted, signal the thread to exit
      // 配额已批准，通知线程退出等待
      next_req->cv.Signal();  // 唤醒请求对应的线程
    }
  }
}

int64_t GenericRateLimiter::CalculateRefillBytesPerPeriodLocked(
    int64_t rate_bytes_per_sec) {
  if (std::numeric_limits<int64_t>::max() / rate_bytes_per_sec <
      refill_period_us_) {
    // Avoid unexpected result in the overflow case. The result now is still
    // inaccurate but is a number that is large enough.
    return std::numeric_limits<int64_t>::max() / 1000000;
  } else {
    return rate_bytes_per_sec * refill_period_us_ / 1000000;
  }
}

// GenericRateLimiter::TuneLocked: 调整限流速率（已持有锁）
// 根据实际使用情况动态调整限流速率，以在吞吐量和系统资源之间取得平衡
Status GenericRateLimiter::TuneLocked() {
  const int kLowWatermarkPct = 50;  // 低水位标记：50%的排空率
  const int kHighWatermarkPct = 90;  // 高水位标记：90%的排空率
  const int kAdjustFactorPct = 5;  // 调整因子：每次调整5%
  // computed rate limit will be in
  // `[max_bytes_per_sec_ / kAllowedRangeFactor, max_bytes_per_sec_]`.
  // 计算的速率限制将在 [最大速率/20, 最大速率] 范围内
  const int kAllowedRangeFactor = 20;  // 允许的范围因子：20倍

  std::chrono::microseconds prev_tuned_time = tuned_time_;  // 保存上次调优时间
  tuned_time_ = std::chrono::microseconds(NowMicrosMonotonicLocked());  // 更新当前调优时间

  int64_t elapsed_intervals = (tuned_time_ - prev_tuned_time +  // 计算经过的补充周期数
                               std::chrono::microseconds(refill_period_us_) -  // 加上补充周期
                               std::chrono::microseconds(1)) /  // 减去1微秒（向上取整）
                              std::chrono::microseconds(refill_period_us_);  // 除以补充周期
  // We tune every kRefillsPerTune intervals, so the overflow and division-by-
  // zero conditions should never happen.
  // 我们每 kRefillsPerTune 个周期调优一次，因此溢出和除零条件不应该发生
  assert(num_drains_ <= std::numeric_limits<int64_t>::max() / 100);  // 断言：防止计算溢出
  assert(elapsed_intervals > 0);  // 断言：至少经过1个周期
  int64_t drained_pct = num_drains_ * 100 / elapsed_intervals;  // 计算排空百分比

  int64_t prev_bytes_per_sec = GetBytesPerSecond();  // 获取当前速率（字节/秒）
  int64_t new_bytes_per_sec;  // 新的速率（字节/秒）
  if (drained_pct == 0) {  // 如果排空率为0（没有等待的情况）
    new_bytes_per_sec = max_bytes_per_sec_ / kAllowedRangeFactor;  // 降到最低速率：最大速率的1/20
  } else if (drained_pct < kLowWatermarkPct) {  // 如果排空率低于50%（资源充足）
    // sanitize to prevent overflow
    // 清理输入以防止溢出
    int64_t sanitized_prev_bytes_per_sec =  // 清理后的当前速率
        std::min(prev_bytes_per_sec, std::numeric_limits<int64_t>::max() / 100);  // 防止乘法溢出
    new_bytes_per_sec =  // 降低速率：减少5%
        std::max(max_bytes_per_sec_ / kAllowedRangeFactor,  // 不低于最低速率
                 sanitized_prev_bytes_per_sec * 100 / (100 + kAdjustFactorPct));  // 新速率 = 原速率 * 100/105
  } else if (drained_pct > kHighWatermarkPct) {  // 如果排空率高于90%（资源紧张）
    // sanitize to prevent overflow
    // 清理输入以防止溢出
    int64_t sanitized_prev_bytes_per_sec =  // 清理后的当前速率
        std::min(prev_bytes_per_sec, std::numeric_limits<int64_t>::max() /  // 防止乘法溢出
                                         (100 + kAdjustFactorPct));
    new_bytes_per_sec =  // 提高速率：增加5%
        std::min(max_bytes_per_sec_,  // 不超过最大速率
                 sanitized_prev_bytes_per_sec * (100 + kAdjustFactorPct) / 100);  // 新速率 = 原速率 * 105/100
  } else {  // 排空率在50%-90%之间（正常范围）
    new_bytes_per_sec = prev_bytes_per_sec;  // 保持当前速率不变
  }
  if (new_bytes_per_sec != prev_bytes_per_sec) {  // 如果速率发生变化
    SetBytesPerSecondLocked(new_bytes_per_sec);  // 更新速率
  }
  num_drains_ = 0;  // 重置排空计数器
  return Status::OK();  // 返回成功
}

RateLimiter* NewGenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us /* = 100 * 1000 */,
    int32_t fairness /* = 10 */,
    RateLimiter::Mode mode /* = RateLimiter::Mode::kWritesOnly */,
    bool auto_tuned /* = false */) {
  assert(rate_bytes_per_sec > 0);
  assert(refill_period_us > 0);
  assert(fairness > 0);
  std::unique_ptr<RateLimiter> limiter(
      new GenericRateLimiter(rate_bytes_per_sec, refill_period_us, fairness,
                             mode, SystemClock::Default(), auto_tuned));
  return limiter.release();
}

}  // namespace ROCKSDB_NAMESPACE
