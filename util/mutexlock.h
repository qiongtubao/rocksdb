//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// 本文件提供多种 RAII 风格的锁辅助类，用于简化 RocksDB 中的并发控制。
// 主要包含：
//   - MutexLock      : 互斥锁的 RAII 包装
//   - ReadLock       : 读写锁读锁的 RAII 包装
//   - ReadUnlock     : 读写锁读锁的自动解锁包装
//   - WriteLock      : 读写锁写锁的 RAII 包装
//   - SpinMutex      : 基于 CAS 的自旋锁（低竞争场景下开销极小）
//   - CacheAlignedWrapper : 防止伪共享的缓存行对齐包装
//   - Striped        : 条带化锁（参考 Guava Striped，减少锁竞争）

#pragma once
#include <assert.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "port/port.h"
#include "util/fastrange.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

// MutexLock —— 互斥锁的 RAII 包装类
//
// 在构造时自动加锁，在析构时自动解锁，保证无论何种退出方式（正常返回、
// 异常抛出等）锁都能被正确释放，避免死锁。
//
// 典型用法：
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ 是成员变量互斥锁
//     ... 复杂逻辑，可能有多个 return 路径 ...
//   }
class MutexLock {
 public:
  // 构造函数：获取互斥锁
  // mu: 指向要加锁的 port::Mutex 对象的指针，不能为 nullptr
  explicit MutexLock(port::Mutex *mu) : mu_(mu) { this->mu_->Lock(); }

  // 禁止拷贝构造和拷贝赋值，防止锁被意外复制导致重复解锁
  MutexLock(const MutexLock &) = delete;
  void operator=(const MutexLock &) = delete;

  // 析构函数：自动释放互斥锁
  ~MutexLock() { this->mu_->Unlock(); }

 private:
  port::Mutex *const mu_;  // 持有的互斥锁指针（const 防止被重新绑定）
};

// ReadLock —— 读写锁（读锁）的 RAII 包装类
//
// 在构造时获取读锁，在析构时自动释放读锁。
// 多个线程可同时持有读锁，但写锁是独占的。
// 适用于多读少写的场景以提高并发度。
class ReadLock {
 public:
  // 构造函数：获取读锁
  // mu: 指向读写锁对象的指针
  explicit ReadLock(port::RWMutex *mu) : mu_(mu) { this->mu_->ReadLock(); }

  // 禁止拷贝，防止重复释放读锁
  ReadLock(const ReadLock &) = delete;
  void operator=(const ReadLock &) = delete;

  // 析构函数：自动释放读锁
  ~ReadLock() { this->mu_->ReadUnlock(); }

 private:
  port::RWMutex *const mu_;  // 持有的读写锁指针
};

// ReadUnlock —— 读锁的自动解锁 RAII 包装类
//
// 与 ReadLock 相反：构造时断言锁已被持有，析构时自动解锁。
// 适用于需要在持锁状态下进入某个作用域，并在离开时自动解锁的场景。
// 注意：构造时不会加锁，调用者必须在构造前已持有读锁。
class ReadUnlock {
 public:
  // 构造函数：断言读锁已被持有（用于调试验证）
  // mu: 指向已持有读锁的读写锁对象
  explicit ReadUnlock(port::RWMutex *mu) : mu_(mu) { mu->AssertHeld(); }

  // 禁止拷贝
  ReadUnlock(const ReadUnlock &) = delete;
  ReadUnlock &operator=(const ReadUnlock &) = delete;

  // 析构函数：自动释放读锁
  ~ReadUnlock() { mu_->ReadUnlock(); }

 private:
  port::RWMutex *const mu_;  // 持有的读写锁指针
};

// WriteLock —— 读写锁（写锁）的 RAII 包装类
//
// 在构造时获取写锁（独占锁），在析构时自动释放。
// 写锁持有期间，其他线程无法获取读锁或写锁。
class WriteLock {
 public:
  // 构造函数：获取写锁（独占）
  // mu: 指向读写锁对象的指针
  explicit WriteLock(port::RWMutex *mu) : mu_(mu) { this->mu_->WriteLock(); }

  // 禁止拷贝，防止重复释放写锁
  WriteLock(const WriteLock &) = delete;
  void operator=(const WriteLock &) = delete;

  // 析构函数：自动释放写锁
  ~WriteLock() { this->mu_->WriteUnlock(); }

 private:
  port::RWMutex *const mu_;  // 持有的读写锁指针
};

// SpinMutex —— 基于原子操作的自旋锁
//
// 在低竞争场景下开销极小，因为不会触发系统调用（如 futex）。
// 实现策略：
//   1. 使用 CAS（Compare-And-Swap）原子操作尝试获取锁
//   2. 若 CAS 失败，执行 CPU 暂停指令（pause/yield）后重试
//   3. 超过 100 次尝试后，调用 std::this_thread::yield() 让出 CPU
//      以避免在高竞争时饿死其他线程
//
// 接口命名兼容 std::unique_lock 和 std::lock_guard，可直接配合使用。
//
// 注意：高竞争场景下建议使用 port::Mutex（基于 OS futex），
// 自旋锁在持锁时间短、线程数不超过 CPU 核心数时效果最好。
class SpinMutex {
 public:
  // 构造函数：初始化为未锁定状态
  SpinMutex() : locked_(false) {}

  // 非阻塞尝试加锁
  // 返回 true 表示成功获取锁，false 表示锁已被其他线程持有
  // 使用 memory_order_relaxed 读取（减少开销）和 memory_order_acquire 获取
  // 语义（确保后续操作不会被重排序到加锁前）
  bool try_lock() {
    auto currently_locked = locked_.load(std::memory_order_relaxed);
    return !currently_locked &&
           locked_.compare_exchange_weak(currently_locked, true,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

  // 阻塞加锁（自旋等待直到获取锁）
  // 前 100 次失败：执行 CPU pause 指令（x86 上降低功耗并避免流水线乱序）
  // 超过 100 次后：调用 yield() 让出 CPU 时间片，避免饿死其他线程
  void lock() {
    for (size_t tries = 0;; ++tries) {
      if (try_lock()) {
        // 成功获取锁，退出自旋
        break;
      }
      // 执行 CPU 级别的暂停指令，减少总线争用和功耗
      port::AsmVolatilePause();
      if (tries > 100) {
        // 自旋次数过多，主动让出 CPU，避免影响其他线程
        std::this_thread::yield();
      }
    }
  }

  // 解锁
  // 使用 memory_order_release 确保锁释放前的所有写操作对其他线程可见
  void unlock() { locked_.store(false, std::memory_order_release); }

 private:
  // 原子布尔标志：true 表示锁已被持有，false 表示空闲
  std::atomic<bool> locked_;
};

// CacheAlignedWrapper —— 缓存行对齐包装器，用于防止伪共享（False Sharing）
//
// 伪共享问题：当多个线程分别访问同一缓存行中的不同变量时，
// CPU 缓存一致性协议（如 MESI）会导致缓存行频繁失效，
// 严重影响多核并发性能。
//
// 解决方案：将对象强制对齐到缓存行边界（通常 64 字节），
// 确保每个对象独占至少一个完整缓存行。
//
// 注意：若 mutex 尺寸小于缓存行的一半，更合理的做法是将多个 mutex
// 打包进一个缓存行（见下方 Striped 的设计说明）。
// 但 mutex 通常约为 40 字节（64 字节缓存行的大半），所以单独对齐是合理的。
template <class T>
struct ALIGN_AS(CACHE_LINE_SIZE) CacheAlignedWrapper {
  T obj_;  // 被包装的对象，已按缓存行对齐
};

// Unwrap —— 辅助模板：从 CacheAlignedWrapper 中提取内部对象的引用
//
// 提供统一接口，无论 T 是否被 CacheAlignedWrapper 包装，
// 都能通过 Unwrap<T>::Go() 获取到真实对象的引用。
template <class T>
struct Unwrap {
  using type = T;
  // 对于普通类型，直接返回自身引用
  static type &Go(T &t) { return t; }
};
template <class T>
struct Unwrap<CacheAlignedWrapper<T>> {
  using type = T;
  // 对于 CacheAlignedWrapper，提取内部 obj_ 的引用
  static type &Go(CacheAlignedWrapper<T> &t) { return t.obj_; }
};

// Striped —— 条带化锁（Striped Locking）
//
// 灵感来源：Guava Striped（https://github.com/google/guava/wiki/StripedExplained）
// 以及 Java ConcurrentHashMap 的分段锁设计。
//
// 核心思想：将一把全局锁拆分为多个"条带"（stripe），
// 不同的键哈希到不同的条带，从而允许对不同键的操作并发执行，
// 而不是全部串行等待同一把锁。
//
// 典型用法（使用缓存行对齐的 SpinMutex）：
//   Striped<CacheAlignedWrapper<SpinMutex>> lock_stripes(256);
//   lock_stripes.Get(key).lock();   // 按 key 哈希选择对应条带加锁
//   // ... 临界区操作 ...
//   lock_stripes.Get(key).unlock();
//
// 模板参数：
//   T    - 锁类型（如 SpinMutex、port::Mutex、CacheAlignedWrapper<SpinMutex>）
//   Key  - 键类型，默认为 Slice
//   Hash - 哈希函数类型，默认为 SliceNPHasher64
template <class T, class Key = Slice, class Hash = SliceNPHasher64>
class Striped {
 public:
  // 构造函数：创建指定数量的锁条带
  // stripe_count: 条带数量，建议为 2 的幂以优化取模运算
  explicit Striped(size_t stripe_count)
      : stripe_count_(stripe_count), data_(new T[stripe_count]) {}

  using Unwrapped = typename Unwrap<T>::type;

  // 根据键获取对应条带的锁引用
  // key:  用于哈希定位的键
  // seed: 哈希种子（可用于避免哈希碰撞攻击）
  // 返回：对应条带锁的引用（已解包 CacheAlignedWrapper）
  Unwrapped &Get(const Key &key, uint64_t seed = 0) {
    // FastRangeGeneric：将哈希值快速映射到 [0, stripe_count_) 范围
    // 比取模（%）更快，且对非 2 幂的 stripe_count 也能均匀分布
    size_t index = FastRangeGeneric(hash_(key, seed), stripe_count_);
    return Unwrap<T>::Go(data_[index]);
  }

  // 估算该 Striped 对象的内存占用（字节数）
  // 注意：不使用 malloc_usable_size() 以避免统计未映射页面
  size_t ApproximateMemoryUsage() const {
    return sizeof(*this) + stripe_count_ * sizeof(T);
  }

 private:
  size_t stripe_count_;          // 条带总数
  std::unique_ptr<T[]> data_;    // 存储所有条带锁的数组
  Hash hash_;                    // 哈希函数实例
};

}  // namespace ROCKSDB_NAMESPACE
