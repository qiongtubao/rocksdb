//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// ConcurrentArena 实现文件
//
// 本文件包含 ConcurrentArena 的构造函数和 Repick() 的实现。
// 核心分配逻辑（AllocateImpl）在头文件中以内联模板形式实现。

#include "memory/concurrent_arena.h"

#include <thread>

#include "port/port.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// tls_cpuid —— 线程局部 CPU ID 缓存
//
// 每个线程独立维护此变量，记录上次使用的 Shard 索引（经过编码）。
// 初始值为 0，表示尚未选过 Shard。
// 编码规则（由 Repick() 设置）：
//   tls_cpuid = shard_index | shards_.Size()
// 通过 OR 操作加入 shards_.Size() 标志位，确保 tls_cpuid > 0，
// 从而与"尚未初始化（值为 0）"的状态区分开。
thread_local size_t ConcurrentArena::tls_cpuid = 0;

namespace {
// kMaxShardBlockSize —— Shard 块大小上限（128KB）
//
// 限制原因：防止多核并发场景下内存激增。
// 极端情况：若每个核心都分配一个 Shard 块但未填满：
//   64 核 × 1MB/块 = 64MB 的预留内存，可能意外触发 Flush 操作。
// 设置 128KB 上限后：
//   64 核 × 128KB = 8MB，处于可接受范围。
const size_t kMaxShardBlockSize = size_t{128 * 1024};
}  // namespace

// 构造函数：初始化 ConcurrentArena
//
// 参数说明：
//   block_size:     底层 Arena 的内存块大小
//   tracker:        内存使用追踪器（nullptr 表示不追踪）
//   huge_page_size: 大页内存大小（0 表示不使用大页）
//
// shard_block_size_ 计算：
//   = min(kMaxShardBlockSize, block_size / 8)
//   即：不超过 128KB，且不超过 Arena 块大小的 1/8。
//   这样即使所有 Shard 同时填满，总预留也不超过一个 Arena 块的大小。
ConcurrentArena::ConcurrentArena(size_t block_size, AllocTracker* tracker,
                                 size_t huge_page_size)
    : shard_block_size_(std::min(kMaxShardBlockSize, block_size / 8)),
      shards_(),
      arena_(block_size, tracker, huge_page_size) {
  // 初始化原子统计缓存（将 arena_ 的初始状态同步到原子变量）
  Fixup();
}

// Repick —— 为当前线程重新选择一个 Shard
//
// 调用时机：当前线程尝试 try_lock 当前 CPU 对应的 Shard 失败时。
// 说明当前 Shard 正被另一线程（可能在同一核上运行）持有，
// 需要换一个空闲的 Shard 以减少竞争。
//
// 实现细节：
//   AccessElementAndIndex() 由 CoreLocalArray 提供，返回
//   { Shard*, shard_index }，内部通过随机或轮询策略选择一个未被占用的 Shard。
//
// tls_cpuid 更新策略：
//   tls_cpuid = shard_index | shards_.Size()
//   即使选中的是 CPU 0（index == 0），OR 上 shards_.Size() 后值也非零，
//   从而和"未经过 Repick（tls_cpuid == 0）"的状态明确区分。
//   这样 AllocateImpl 中的快速路径判断 (tls_cpuid == 0) 只会在真正的
//   首次分配时触发，而不会在 Repick 后误触发。
//
// 返回：选中的 Shard 指针（调用者负责加锁）
ConcurrentArena::Shard* ConcurrentArena::Repick() {
  auto shard_and_index = shards_.AccessElementAndIndex();
  // 即使选中的是 index 0，也通过 OR shards_.Size() 使 tls_cpuid 非零，
  // 避免后续误走 AllocateImpl 的快速路径（该路径假设 tls_cpuid==0 表示未选过）
  tls_cpuid = shard_and_index.second | shards_.Size();
  return shard_and_index.first;
}

}  // namespace ROCKSDB_NAMESPACE
