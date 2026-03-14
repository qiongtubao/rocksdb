//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// 本文件提供 DMutex（分布式互斥锁）的跨平台包装。
//
// 设计思路：
//   - 若编译时启用了 Folly 库（USE_FOLLY 宏），使用 folly::DistributedMutex，
//     它在高竞争下性能优于传统 pthread_mutex。
//   - 否则退回到标准的 port::Mutex（基于 pthread_mutex）。
//
// folly::DistributedMutex 的优点（见官方文档）：
//   - 无中心化的等待队列，锁的所有权直接在线程间传递（减少上下文切换）
//   - 在高竞争场景下比 std::mutex 快约 2-5 倍
//   - 支持组合（combining）：多个线程的写操作可被合并成一次执行
//   - 无偏向锁持有线程（公平调度）
//
// 限制：
//   - 目前仅支持通过 DMutexLock RAII 包装器进行作用域加锁，
//     不支持裸露的 lock()/unlock() 调用（因为两种实现的接口不同）。

#pragma once

#include "rocksdb/rocksdb_namespace.h"

// This file declares a wrapper around the efficient folly DistributedMutex
// that falls back on a standard mutex when not available. See
// https://github.com/facebook/folly/blob/main/folly/synchronization/DistributedMutex.h
// for benefits and limitations.

// At the moment, only scoped locking is supported using DMutexLock
// RAII wrapper, because lock/unlock APIs will vary.

#ifdef USE_FOLLY
// ============================================================
// Folly 路径：使用高性能分布式互斥锁
// ============================================================

#include <folly/synchronization/DistributedMutex.h>

namespace ROCKSDB_NAMESPACE {

// DMutex —— Folly DistributedMutex 的 RocksDB 封装
//
// 继承自 folly::DistributedMutex，附加：
//   - kName()：返回实现名称（用于日志和诊断）
//   - AssertHeld()：当前为空操作（folly 暂不支持该断言）
//   - adaptive 参数：被忽略（folly 内部自适应，无需外部配置）
class DMutex : public folly::DistributedMutex {
 public:
  // 返回互斥锁的实现名称，用于日志输出和运行时诊断
  static const char* kName() { return "folly::DistributedMutex"; }

  // 构造函数
  // IGNORED_adaptive: adaptive 参数被忽略（folly 内部自动处理）
  explicit DMutex(bool IGNORED_adaptive = false) { (void)IGNORED_adaptive; }

  // 断言当前线程持有该锁（当前为空操作，folly 不支持此功能）
  // TODO: 未来版本的 folly 可能支持，届时可填充实现
  void AssertHeld() {}
};

// DMutexLock —— folly::DistributedMutex 的 RAII 锁包装
// 使用 std::lock_guard 在作用域内自动加锁/解锁
using DMutexLock = std::lock_guard<folly::DistributedMutex>;

}  // namespace ROCKSDB_NAMESPACE

#else
// ============================================================
// 标准路径：使用 port::Mutex（基于 pthread_mutex 或等价实现）
// ============================================================

#include <mutex>

#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

// DMutex —— 退化为标准 port::Mutex
// port::Mutex 在各平台（Linux/macOS/Windows）均有对应实现
using DMutex = port::Mutex;

// DMutexLock —— port::Mutex 的 RAII 锁包装
// 使用 std::lock_guard 在作用域内自动加锁/解锁
using DMutexLock = std::lock_guard<DMutex>;

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_FOLLY
