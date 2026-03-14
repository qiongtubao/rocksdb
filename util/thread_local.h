//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// 本文件实现了 RocksDB 自定义的线程局部存储（Thread-Local Storage, TLS）。
//
// 背景与动机：
//   C++ 标准的 thread_local 关键字存在一个局限：同一个 thread_local 变量
//   在同一进程的所有同类对象实例之间是共享的。例如：
//     class DBImpl { thread_local int x; };
//   两个 DBImpl 实例会共享同一个 x，无法区分来自哪个实例。
//
//   ThreadLocalPtr 解决了这个问题：它不仅能区分来自不同线程的数据，
//   还能区分来自不同 ThreadLocalPtr 实例的数据，实现真正的"实例级"
//   线程局部存储。
//
// 内存模型：
//   内存占用为 O(线程数 × ThreadLocalPtr 实例数)，以二维表格形式组织：
//     ---------------------------------------------------
//     |          | 实例 1   | 实例 2   | 实例 3   |
//     ---------------------------------------------------
//     | 线程 1   |  void*  |  void*  |  void*  |  <- ThreadData
//     ---------------------------------------------------
//     | 线程 2   |  void*  |  void*  |  void*  |  <- ThreadData
//     ---------------------------------------------------
//     | 线程 3   |  void*  |  void*  |  void*  |  <- ThreadData
//     ---------------------------------------------------
//
// 生命周期管理：
//   支持注册 UnrefHandler 清理回调，在以下情况自动调用：
//   (1) 线程退出时
//   (2) ThreadLocalPtr 实例被销毁时

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "port/port.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// UnrefHandler —— 线程局部指针的清理回调函数类型
//
// 当以下事件发生时，若存储的指针非 nullptr，此回调会被调用：
//   (1) 持有该指针的线程退出
//   (2) 对应的 ThreadLocalPtr 实例被销毁
//
// 重要警告：
//   此函数在持有全局互斥锁的情况下被调用！
//   该全局锁同时被 ThreadLocalPtr 的大多数方法使用，且所有实例共享。
//   因此，handler 函数内部：
//     - 不得加任何互斥锁（可能死锁）
//     - 不得调用任何 ThreadLocalPtr 实例的方法（除非确保安全）
using UnrefHandler = void (*)(void* ptr);

// ThreadLocalPtr —— 实例级线程局部存储指针
//
// 相比 thread_local 的优势：
//   - 可区分同类的不同对象实例
//   - 支持跨线程遍历所有线程的值（Scrape/Fold）
//   - 支持线程退出时的自定义清理（UnrefHandler）
//
// 只能存储指针类型（void*）。
// 线程安全：单个 ThreadLocalPtr 实例的 Get/Reset 操作无需加锁（原子操作），
// 但 Scrape/Fold 等全局操作需要持有全局锁。
class ThreadLocalPtr {
 public:
  // 构造函数：分配一个全局唯一的实例 ID
  // handler: 可选的清理回调，当线程退出或实例销毁时调用
  explicit ThreadLocalPtr(UnrefHandler handler = nullptr);

  // 禁止拷贝（每个实例有唯一 ID，不能复制）
  ThreadLocalPtr(const ThreadLocalPtr&) = delete;
  ThreadLocalPtr& operator=(const ThreadLocalPtr&) = delete;

  // 析构函数：回收实例 ID，并对所有线程中存储的非 nullptr 指针调用 UnrefHandler
  ~ThreadLocalPtr();

  // 获取当前线程存储的指针值
  // 无锁操作（原子 load，memory_order_acquire）
  // 若当前线程尚未存储值，返回 nullptr
  void* Get() const;

  // 设置当前线程存储的指针值
  // 无锁操作（原子 store，memory_order_release）
  // ptr: 要存储的指针（可以为 nullptr）
  void Reset(void* ptr);

  // 原子地交换指针值，返回旧值
  // 相当于原子的 { old = Get(); Reset(ptr); return old; }
  // ptr: 新指针值
  // 返回：被替换的旧指针值
  void* Swap(void* ptr);

  // 原子 CAS（比较并交换）操作
  // 仅当当前值等于 expected 时，才将值替换为 ptr
  // ptr:      成功时要写入的新值
  // expected: [输入] 期望的当前值；[输出] 若失败，返回实际当前值
  // 返回：true 表示交换成功，false 表示当前值与 expected 不符（expected 已更新为实际值）
  bool CompareAndSwap(void* ptr, void*& expected);

  // 批量收集所有线程的值，并将它们替换为 replacement
  // 需要持有全局锁。
  // ptrs:        [输出] 收集到的所有非 nullptr 旧值
  // replacement: 替换值（通常为 nullptr）
  // 注意：仅收集非 nullptr 的值
  void Scrape(autovector<void*>* ptrs, void* const replacement);

  using FoldFunc = std::function<void(void*, void*)>;

  // 对所有线程的当前值应用聚合函数
  // 在持有全局锁期间调用（防止 UnrefHandler 并发运行），
  // 但调用者仍需提供外部同步，因为持有值的线程可以在不加锁的情况下
  // 通过 Get()/Reset() 访问自己的值。
  // func: 聚合函数，签名为 void(void* value, void* res)
  // res:  聚合结果的累积缓冲区（由调用者管理）
  void Fold(FoldFunc func, void* res);

  // 仅用于测试：查看下一个可用 ID（不实际分配）
  static uint32_t TEST_PeekId();

  // 初始化全局单例（StaticMeta）
  //
  // 若不调用此函数，单例会在首次使用时自动初始化。
  // 主动调用此函数可以控制初始化时机（例如在 Env::Default() 中调用，
  // 以确保正确的构造顺序）。
  // 多次调用是安全的（幂等操作）。
  static void InitSingletons();

  // 内部实现类（前向声明，供 .cc 文件使用）
  class StaticMeta;

 private:
  // 获取全局单例 StaticMeta 的指针
  // 使用函数内静态变量（而非全局静态变量）以控制初始化顺序
  static StaticMeta* Instance();

  // 该 ThreadLocalPtr 实例在全局表中的唯一 ID
  // ID 由 StaticMeta 分配，销毁时回收复用
  const uint32_t id_;
};

}  // namespace ROCKSDB_NAMESPACE
