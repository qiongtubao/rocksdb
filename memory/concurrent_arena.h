//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// ConcurrentArena —— 线程安全的并发内存分配器
//
// 设计目标与核心思路：
//   RocksDB 的 MemTable 写入路径对内存分配性能极为敏感。普通的加全局锁
//   分配方案在高并发写入时会产生严重竞争。ConcurrentArena 通过以下机制
//   解决这一问题：
//
//   1. 【Per-Core 分片缓存（Shard）】
//      每个 CPU 核心拥有一个专属的小型分配缓存（Shard）。小分配优先从
//      当前线程所在 CPU 对应的 Shard 中取出，完全避免竞争。
//
//   2. 【内联自旋锁保护】
//      每个 Shard 用一个轻量 SpinMutex 保护，持锁时间极短（仅指针运算），
//      远低于 OS mutex 的开销。
//
//   3. 【延迟实例化】
//      Shard 仅在 ConcurrentArena 检测到真正的并发使用时才激活，
//      避免单线程场景的额外开销。
//
//   4. 【Shard 大小自适应】
//      Shard 块大小根据硬件并发度（CPU 核心数）动态计算，
//      确保 Shard 块从底层 Arena 分配时不产生内存碎片浪费。
//
//   5. 【大分配直通 Arena】
//      超过 shard_block_size_/4 的分配直接加全局锁走底层 Arena，
//      避免 Shard 被大块占满导致碎片。
//
// 继承关系：ConcurrentArena -> Allocator（虚接口）
// 底层依赖：包含一个 Arena 实例作为真正的内存来源

#pragma once
#include <atomic>
#include <memory>
#include <utility>

#include "memory/allocator.h"
#include "memory/arena.h"
#include "port/lang.h"
#include "port/likely.h"
#include "util/core_local.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

// 仅对 padding 数组生成"字段未使用"警告的抑制，
// 或在 GCC 4.8.1 下构建时需要此宏，否则会编译失败。
#ifdef __clang__
#define ROCKSDB_FIELD_UNUSED __attribute__((__unused__))
#else
#define ROCKSDB_FIELD_UNUSED
#endif  // __clang__

namespace ROCKSDB_NAMESPACE {

class Logger;

// ConcurrentArena —— 线程安全的并发内存竞技场（Arena）
//
// 对底层 Arena 进行线程安全封装，通过以下机制降低多线程分配开销：
//   - 使用内联 SpinMutex 保护底层 Arena（替代重量级 OS 互斥锁）
//   - 为每个 CPU 核心维护一个小型分配缓存（Shard），避免小分配的锁竞争
//   - Shard 按需懒初始化（仅在检测到真正并发时才激活）
//   - Shard 块大小自适应，避免向底层 Arena 申请时产生碎片
class ConcurrentArena : public Allocator {
 public:
  // 构造函数
  // block_size:      底层 Arena 的块大小（同时影响 Shard 块大小），
  //                  默认为 Arena::kMinBlockSize
  // tracker:         可选的内存分配追踪器（用于监控/限制内存使用）
  // huge_page_size:  若非 0，底层 Arena 使用大页内存（减少 TLB miss）
  //
  // Per-Core Shard 的 shard_block_size 计算方式：
  //   shard_block_size = min(128KB, block_size / 8)
  // 这样在多核场景下，各 Shard 合计预留的内存不超过一个 Arena 块大小。
  explicit ConcurrentArena(size_t block_size = Arena::kMinBlockSize,
                           AllocTracker* tracker = nullptr,
                           size_t huge_page_size = 0);

  // 分配 bytes 字节的非对齐内存
  // 线程安全：优先从当前 CPU 的 Shard 分配（无全局锁），
  // 仅当 Shard 容量不足或分配量过大时才加全局锁访问底层 Arena
  char* Allocate(size_t bytes) override {
    return AllocateImpl(bytes, false /*force_arena*/,
                        [this, bytes]() { return arena_.Allocate(bytes); });
  }

  // 分配 bytes 字节的对齐内存
  // rounded_up: 将 bytes 向上对齐到 sizeof(void*)，保证指针对齐
  // huge_page_size 非 0 时强制走底层 Arena（大页内存不适合 Shard 缓存）
  char* AllocateAligned(size_t bytes, size_t huge_page_size = 0,
                        Logger* logger = nullptr) override {
    // 将 bytes 向上取整到 sizeof(void*) 的倍数，确保内存对齐
    size_t rounded_up = ((bytes - 1) | (sizeof(void*) - 1)) + 1;
    assert(rounded_up >= bytes && rounded_up < bytes + sizeof(void*) &&
           (rounded_up % sizeof(void*)) == 0);

    // huge_page_size != 0 时 force_arena=true，强制走底层 Arena
    // 因为大页内存由 Arena 统一管理，不应放入 Shard 缓存
    return AllocateImpl(rounded_up, huge_page_size != 0 /*force_arena*/,
                        [this, rounded_up, huge_page_size, logger]() {
                          return arena_.AllocateAligned(rounded_up,
                                                        huge_page_size, logger);
                        });
  }

  // 返回 Arena 当前实际使用的内存量（字节）
  // 计算方式：底层 Arena 内存使用量 - 所有 Shard 已分配但未使用的量
  // 需要持有 arena_mutex_ 以获得一致的快照
  size_t ApproximateMemoryUsage() const {
    std::unique_lock<SpinMutex> lock(arena_mutex_, std::defer_lock);
    lock.lock();
    return arena_.ApproximateMemoryUsage() - ShardAllocatedAndUnused();
  }

  // 返回底层 Arena 向操作系统申请的总内存量（字节）
  // 使用原子 relaxed 读取，无需加锁（允许轻微的非最新值）
  size_t MemoryAllocatedBytes() const {
    return memory_allocated_bytes_.load(std::memory_order_relaxed);
  }

  // 返回已分配但尚未使用的总内存量（字节）
  // = 底层 Arena 未使用量 + 所有 Shard 缓存的未使用量
  size_t AllocatedAndUnused() const {
    return arena_allocated_and_unused_.load(std::memory_order_relaxed) +
           ShardAllocatedAndUnused();
  }

  // 返回不规则块（大小不等于 block_size 的块）的数量
  // 用于内存碎片统计
  size_t IrregularBlockNum() const {
    return irregular_block_num_.load(std::memory_order_relaxed);
  }

  // 返回底层 Arena 的块大小配置
  size_t BlockSize() const override { return arena_.BlockSize(); }

 private:
  // Shard —— 每个 CPU 核心的私有小型内存分配缓存
  //
  // 每个 Shard 持有从底层 Arena 预先批量申请的一块内存，
  // 小分配直接在 Shard 内完成（仅需 Shard 级别的 SpinMutex，无需全局锁）。
  //
  // 内存布局策略：
  //   - 对齐分配（bytes % sizeof(void*) == 0）: 从 free_begin_ 头部取出
  //   - 非对齐分配：从 free_begin_ + avail - bytes 尾部取出
  //   这样对齐和非对齐分配各自从两端增长，互不干扰。
  //
  // padding[40]：填充至 64 字节缓存行边界，防止相邻 Shard 产生伪共享
  struct Shard {
    char padding[40] ROCKSDB_FIELD_UNUSED;  // 缓存行填充，防止伪共享
    mutable SpinMutex mutex;                // 保护本 Shard 的自旋锁
    char* free_begin_;                      // 指向 Shard 可用内存的起始位置
    std::atomic<size_t> allocated_and_unused_;  // 当前 Shard 中可用的字节数（原子）

    Shard() : free_begin_(nullptr), allocated_and_unused_(0) {}
  };

  // 线程局部 CPU ID 缓存
  // 用于快速定位当前线程应使用哪个 Shard，避免频繁调用 getcpu() 系统调用
  // 值为 0 表示尚未选过 Shard（或 CPU 0 且未经过 Repick）
  static thread_local size_t tls_cpuid;

  // 填充字节，确保以下成员与 arena_mutex_ 不在同一缓存行
  // 避免 tls_cpuid 更新与 shard_block_size_ 读取产生伪共享
  char padding0[56] ROCKSDB_FIELD_UNUSED;

  // 每个 Shard 向底层 Arena 申请的块大小
  // = min(128KB, block_size / 8)
  // 上限 128KB 防止多核同时申请导致内存激增（64 核 × 1MB = 64MB）
  size_t shard_block_size_;

  // Per-Core Shard 数组（大小 = CPU 核心数，2 的幂）
  // CoreLocalArray 确保每个 Shard 按缓存行对齐，防止伪共享
  CoreLocalArray<Shard> shards_;

  // 底层 Arena：真正持有所有内存块的分配器
  Arena arena_;

  // 保护底层 Arena 的全局自旋锁（相比 OS mutex 更轻量）
  mutable SpinMutex arena_mutex_;

  // 底层 Arena 中可用（已分配块中未使用的）字节数的原子缓存
  // 实际值由 Fixup() 在每次操作 arena_ 后更新
  std::atomic<size_t> arena_allocated_and_unused_;

  // 底层 Arena 向 OS 申请的总字节数（原子缓存，由 Fixup() 维护）
  std::atomic<size_t> memory_allocated_bytes_;

  // 底层 Arena 中不规则块（非标准 block_size）数量（原子缓存）
  std::atomic<size_t> irregular_block_num_;

  // 再次填充，确保上方原子成员不与其他数据共享缓存行
  char padding1[56] ROCKSDB_FIELD_UNUSED;

  // 重新为当前线程选择一个可用的 Shard
  // 当当前 CPU 对应的 Shard 被其他线程持有（try_lock 失败）时调用。
  // 随机选择另一个未被持有的 Shard，并更新 tls_cpuid 记录。
  // 返回：选中的 Shard 指针（已确保可以 lock）
  Shard* Repick();

  // 计算所有 Shard 缓存中已分配但未使用的字节总量
  // 遍历所有 Shard，累加各自的 allocated_and_unused_
  // 用于 ApproximateMemoryUsage() 和 AllocatedAndUnused() 的计算
  size_t ShardAllocatedAndUnused() const {
    size_t total = 0;
    for (size_t i = 0; i < shards_.Size(); ++i) {
      total += shards_.AccessAtCore(i)->allocated_and_unused_.load(
          std::memory_order_relaxed);
    }
    return total;
  }

  // AllocateImpl —— 核心分配逻辑（模板函数，统一处理对齐与非对齐分配）
  //
  // 分配策略（按优先级）：
  //
  //   【快速路径 1 - 直通 Arena】满足以下任一条件时直接加全局锁走 Arena：
  //     a) bytes > shard_block_size_ / 4：分配量超过 Shard 容量的 1/4，
  //        放入 Shard 会产生严重碎片
  //     b) force_arena == true：大页内存等特殊场景
  //     c) tls_cpuid == 0（未经过 Repick）且 Shard[0] 为空
  //        且 Arena 全局锁可立即获取（无等待）：
  //        说明这是早期单线程阶段，零碎片开销地走 Arena
  //
  //   【慢速路径 - Shard 分配】
  //     1. 根据 tls_cpuid 选择当前线程的 Shard
  //     2. try_lock Shard：若失败，调用 Repick() 换一个
  //     3. 若 Shard 剩余空间 >= bytes，直接从 Shard 中切出
  //     4. 若 Shard 空间不足（avail < bytes），先加全局锁从 Arena 为 Shard
  //        补充内存（避免 Arena 块内碎片：若 Arena 剩余量接近 shard_block_size_
  //        则直接用 Arena 剩余量作为新的 avail，否则申请一个完整 shard_block_size_）
  //
  // bytes:       要分配的字节数
  // force_arena: true 时强制走底层 Arena（不经过 Shard 缓存）
  // func:        实际调用底层 Arena 分配的 lambda 函数
  template <typename Func>
  char* AllocateImpl(size_t bytes, bool force_arena, const Func& func) {
    size_t cpu;

    // 快速路径：直接走底层 Arena（加全局锁）
    // 触发条件：
    //   1. bytes 超过 shard 块大小的 1/4（避免大分配浪费 shard 空间）
    //   2. force_arena 为 true（如大页内存分配）
    //   3. 首次分配且全局锁可立即获取（无并发竞争的早期阶段）
    std::unique_lock<SpinMutex> arena_lock(arena_mutex_, std::defer_lock);
    if (bytes > shard_block_size_ / 4 || force_arena ||
        ((cpu = tls_cpuid) == 0 &&
         !shards_.AccessAtCore(0)->allocated_and_unused_.load(
             std::memory_order_relaxed) &&
         arena_lock.try_lock())) {
      if (!arena_lock.owns_lock()) {
        arena_lock.lock();
      }
      auto rv = func();   // 调用底层 Arena 分配
      Fixup();            // 更新原子缓存的统计数据
      return rv;
    }

    // 慢速路径：从 Per-Core Shard 分配
    // 根据 tls_cpuid 选择对应的 Shard（位掩码确保 index 在范围内）
    Shard* s = shards_.AccessAtCore(cpu & (shards_.Size() - 1));
    if (!s->mutex.try_lock()) {
      // 当前 Shard 被占用，重新选择一个空闲的 Shard
      s = Repick();
      s->mutex.lock();
    }
    // adopt_lock：接管已通过 try_lock/lock 获取的锁（避免重复加锁）
    std::unique_lock<SpinMutex> lock(s->mutex, std::adopt_lock);

    size_t avail = s->allocated_and_unused_.load(std::memory_order_relaxed);
    if (avail < bytes) {
      // Shard 剩余空间不足，需要从底层 Arena 补充内存
      std::lock_guard<SpinMutex> reload_lock(arena_mutex_);

      // 获取 Arena 当前剩余的精确可用字节数
      auto exact = arena_allocated_and_unused_.load(std::memory_order_relaxed);
      assert(exact == arena_.AllocatedAndUnused());

      if (exact >= bytes && arena_.IsInInlineBlock()) {
        // Arena 的内联块（初始小块）还有足够空间时，直接从 Arena 分配。
        // 目的：避免小 MemTable 浪费一整个大 Arena 块。
        // 背景：MemTable 创建时约分配 1KB 内存，若立即分配一个完整 Arena 块
        // （通常数 MB），对于有大量空 MemTable 的场景（如数千个列族）会造成
        // 严重内存浪费。
        auto rv = func();
        Fixup();
        return rv;
      }

      // 根据 Arena 当前剩余量智能决定 Shard 补充大小：
      //   若 Arena 剩余量在 [shard_block_size_/2, shard_block_size_*2) 范围内，
      //   直接用 Arena 剩余量作为本次 Shard 块大小（避免浪费 Arena 当前块末尾）。
      //   否则，申请一个标准的 shard_block_size_ 大小的块。
      avail = exact >= shard_block_size_ / 2 && exact < shard_block_size_ * 2
                  ? exact
                  : shard_block_size_;
      s->free_begin_ = arena_.AllocateAligned(avail);  // 从 Arena 申请对齐内存
      Fixup();  // 更新 Arena 统计缓存
    }
    // 更新 Shard 剩余量（减去本次分配的字节）
    s->allocated_and_unused_.store(avail - bytes, std::memory_order_relaxed);

    char* rv;
    if ((bytes % sizeof(void*)) == 0) {
      // 对齐分配：从 Shard 缓冲区头部取出
      rv = s->free_begin_;
      s->free_begin_ += bytes;
    } else {
      // 非对齐分配：从 Shard 缓冲区尾部取出
      // 避免破坏 free_begin_ 的对齐性（对后续对齐分配友好）
      rv = s->free_begin_ + avail - bytes;
    }
    return rv;
  }

  // Fixup —— 同步更新底层 Arena 统计信息到原子缓存
  //
  // 在每次操作底层 Arena 后调用（持有 arena_mutex_ 时）。
  // 将 arena_ 的最新统计值写入原子变量，供其他线程无锁读取。
  void Fixup() {
    arena_allocated_and_unused_.store(arena_.AllocatedAndUnused(),
                                      std::memory_order_relaxed);
    memory_allocated_bytes_.store(arena_.MemoryAllocatedBytes(),
                                  std::memory_order_relaxed);
    irregular_block_num_.store(arena_.IrregularBlockNum(),
                               std::memory_order_relaxed);
  }

  // 禁止拷贝构造和赋值（Arena 语义不可复制）
  ConcurrentArena(const ConcurrentArena&) = delete;
  ConcurrentArena& operator=(const ConcurrentArena&) = delete;
};

}  // namespace ROCKSDB_NAMESPACE
