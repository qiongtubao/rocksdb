//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/write_controller.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <ratio>

#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<WriteControllerToken> WriteController::GetStopToken() {
  ++total_stopped_; //添加停止token个数
  return std::unique_ptr<WriteControllerToken>(new StopWriteToken(this)); //添加停止token对象
}

std::unique_ptr<WriteControllerToken> WriteController::GetDelayToken(
    uint64_t write_rate) {
  if (0 == total_delayed_++) {
    // Starting delay, so reset counters.
    next_refill_time_ = 0;
    credit_in_bytes_ = 0;
  }
  // NOTE: for simplicity, any current credit_in_bytes_ or "debt" in
  // next_refill_time_ will be based on an old rate. This rate will apply
  // for subsequent additional debts and for the next refill.
  set_delayed_write_rate(write_rate);
  return std::unique_ptr<WriteControllerToken>(new DelayWriteToken(this));
}

std::unique_ptr<WriteControllerToken>
WriteController::GetCompactionPressureToken() {
  ++total_compaction_pressure_;
  return std::unique_ptr<WriteControllerToken>(
      new CompactionPressureToken(this));
}

bool WriteController::IsStopped() const {
  return total_stopped_.load(std::memory_order_relaxed) > 0; //当有任何停止写入的令牌存在时 就是停止状态
}
// WriteController::GetDelay - 计算写入延迟时间
//
// 功能概述：
// 根据当前的写入速率限制，计算需要延迟多少微秒才能执行指定字节数的写入。
// 这是一个令牌桶（Token Bucket）算法的实现，用于平滑地限制写入速率。
//
// 参数说明：
// - clock: 系统时钟，用于获取当前时间（单调时钟）
// - num_bytes: 要写入的字节数
//
// 返回值：
// - 需要延迟的微秒数。调用者应该在这个时间范围内休眠。
// - 返回 0 表示可以立即写入，无需延迟。
//
// 令牌桶算法原理：
// 1. 令牌桶以固定的速率（delayed_write_rate_）填充字节信用额度（credit_in_bytes_）
// 2. 写入操作消耗信用额度
// 3. 如果信用额度不足，需要等待桶被重新填充
// 4. 桶有最大容量限制（通过 refill 间隔隐式限制）
//
// 状态变量：
// - total_stopped_: 停止写入的 token 计数器。如果 > 0，写入完全停止
// - total_delayed_: 延迟写入的 token 计数器。如果 == 0，不进行速率限制
// - credit_in_bytes_: 当前可用的写入字节信用额度
// - next_refill_time_: 下一次补充信用额度的时间戳
// - delayed_write_rate_: 目标写入速率（字节/秒）
//
// 填充策略：
// - 填充间隔：每 1 毫秒（kMicrosPerRefill = 1000）
// - 填充量：delayed_write_rate_ * elapsed_time / 1000000
// - 向上取整：+0.999999 确保不因浮点数精度问题而少填充
//
// 延迟计算：
// - 如果有足够的信用额度（credit_in_bytes_ >= num_bytes）：立即写入
// - 如果信用额度不足：计算需要等待多长时间才能积累足够的信用额度
// - 延迟时间 = bytes_over_budget / delayed_write_rate_ * 1000000
// - 最小延迟：至少 1 毫秒（kMicrosPerRefill），减少 DB mutex 竞争
//
// 设计考虑：
// 1. 在 DB mutex 内部调用，不能休眠，需要最小化获取时间的频率
// 2. 每次填充间隔内最多获取一次时间，减少开销
// 3. 假设调用者会按照返回的时间休眠（在 mutex 外部）
// 4. 信用额度可能有负值（debt），通过延长 next_refill_time_ 来偿还
//
// 性能优化：
// - 快速路径：如果停止或没有延迟，立即返回 0
// - 快速路径：如果信用额度足够，立即返回 0
// - 减少时间获取：只在需要填充时才获取时间
// - 减少(mutex)竞争：最小延迟为 1 毫秒，减少 DB mutex 释放和获取的频率
//
// 边界情况：
// - total_stopped_ > 0: 返回 0，上层会处理停止状态
// - total_delayed_ == 0: 返回 0，没有速率限制
// - credit_in_bytes_ 可能很大：避免溢出（uint64_t）
// - 时间获取成本：在 DB mutex 内部调用系统时钟，需要最小化调用次数
uint64_t WriteController::GetDelay(SystemClock* clock, uint64_t num_bytes) {
  // ============================================================================
  // 快速路径 1：检查是否停止写入
  // ============================================================================
  // 如果有任何停止写入的 token 存在，则不需要计算延迟
  // 上层会处理停止状态（在 DelayWrite 中检查 IsStopped()）
  // 返回 0 是为了简化调用逻辑，上层会检查 IsStopped() 并停止写入
  if (total_stopped_.load(std::memory_order_relaxed) > 0) {
    return 0;
  }

  // ============================================================================
  // 快速路径 2：检查是否需要速率限制
  // ============================================================================
  // 如果没有延迟令牌，则不需要延迟
  // 这表示当前没有配置写入速率限制
  if (total_delayed_.load(std::memory_order_relaxed) == 0) {
    return 0;
  }

  // ============================================================================
  // 快速路径 3：检查信用额度是否足够
  // ============================================================================
  // 以下是速率限制的核心逻辑：令牌桶算法
  // 如果有足够的信用额度，则不需要延迟
  // 直接从信用额度中扣除 num_bytes，返回 0 表示可以立即写入
  if (credit_in_bytes_ >= num_bytes) {
    credit_in_bytes_ -= num_bytes;
    return 0;
  }

  // ============================================================================
  // 慢速路径：需要补充信用额度
  // ============================================================================
  // 获取当前时间（单调时钟，确保时间不会倒退）
  // 注意：在 DB mutex 内部调用，需要最小化调用频率
  // 每次填充间隔内最多获取一次时间，减少开销
  auto time_now = NowMicrosMonotonic(clock);

  // 时间常量定义
  const uint64_t kMicrosPerSecond = 1000000;  // 每秒的微秒数
  // 填充间隔：每 1 毫秒填充一次
  // 较小的填充间隔可以更精确地控制速率，但会增加开销
  const uint64_t kMicrosPerRefill = 1000;

  // ============================================================================
  // 初始化填充时间
  // ============================================================================
  // 如果 next_refill_time_ 为 0，表示这是第一次调用
  // 初始化填充时间为当前时间，给予一个初始的信用额度
  if (next_refill_time_ == 0) {
    // Start with an initial allotment of bytes for one interval
    // 首次调用时，立即开始填充周期
    next_refill_time_ = time_now;
  }

  // ============================================================================
  // 检查是否需要填充信用额度
  // ============================================================================
  // 如果当前时间已经达到或超过下一次填充时间，需要进行填充
  if (next_refill_time_ <= time_now) {
    // Refill based on time interval plus any extra elapsed
    // 计算已经经过的时间（包括完整的填充间隔和额外的碎片时间）
    uint64_t elapsed = time_now - next_refill_time_ + kMicrosPerRefill;

    // 计算应该填充的字节数
    // 公式：elapsed / kMicrosPerSecond * delayed_write_rate_
    // 加上 0.999999 是为了向上取整，确保不因浮点数精度问题而少填充
    credit_in_bytes_ += static_cast<uint64_t>(
        1.0 * elapsed / kMicrosPerSecond * delayed_write_rate_ + 0.999999);

    // 更新下一次填充时间
    // 下一次填充在 kMicrosPerRefill 微秒后
    next_refill_time_ = time_now + kMicrosPerRefill;

    // ============================================================================
    // 填充后再次检查信用额度是否足够
    // ============================================================================
    // Avoid delay if possible, to reduce DB mutex release & re-aquire.
    // 如果填充后信用额度足够，立即写入，无需延迟
    // 这是一个优化，减少 DB mutex 的释放和获取次数
    if (credit_in_bytes_ >= num_bytes) {
      credit_in_bytes_ -= num_bytes;
      return 0;
    }
  }

  // ============================================================================
  // 计算延迟时间
  // ============================================================================
  // We need to delay to avoid exceeding write rate.
  // 到达这里表示信用额度仍然不足，需要计算延迟时间
  // 确保信用额度确实不足
  assert(num_bytes > credit_in_bytes_);

  // 计算超出预算的字节数
  uint64_t bytes_over_budget = num_bytes - credit_in_bytes_;

  // 计算需要的延迟时间
  // 公式：bytes_over_budget / delayed_write_rate_ * kMicrosPerSecond
  // 这表示以当前速率，需要多少微秒才能积累 enough 的信用额度
  uint64_t needed_delay = static_cast<uint64_t>(
      1.0 * bytes_over_budget / delayed_write_rate_ * kMicrosPerSecond);

  // 清空信用额度（全部用于本次写入）
  credit_in_bytes_ = 0;

  // 延长下一次填充时间
  // 这样在下次填充时，会"还清"这次的债务
  // 通过将延迟"借"给未来的填充操作来保持平均速率
  next_refill_time_ += needed_delay;

  // ============================================================================
  // 返回延迟时间
  // ============================================================================
  // Minimum delay of refill interval, to reduce DB mutex contention.
  // 最小延迟为填充间隔（1 毫秒），减少 DB mutex 竞争
  // 这样可以避免过于频繁地获取时间并重新填充
  // 返回 max(next_refill_time_ - time_now, kMicrosPerRefill)
  // 注意：如果 needed_delay 很小，返回 kMicrosPerRefill 确保至少等待 1 毫秒
  return std::max(next_refill_time_ - time_now, kMicrosPerRefill);
}

uint64_t WriteController::NowMicrosMonotonic(SystemClock* clock) {
  return clock->NowNanos() / std::milli::den;
}

StopWriteToken::~StopWriteToken() {
  assert(controller_->total_stopped_ >= 1);
  --controller_->total_stopped_;
}

DelayWriteToken::~DelayWriteToken() {
  controller_->total_delayed_--;
  assert(controller_->total_delayed_.load() >= 0);
}

CompactionPressureToken::~CompactionPressureToken() {
  controller_->total_compaction_pressure_--;
  assert(controller_->total_compaction_pressure_ >= 0);
}

}  // namespace ROCKSDB_NAMESPACE
