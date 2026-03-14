//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// ThreadLocalPtr 实现文件
//
// 整体架构：
//   ThreadLocalPtr 通过一个全局单例 StaticMeta 管理所有实例和所有线程的
//   线程局部数据，形成一张二维表：
//
//     行 = 线程（ThreadData，每线程一个，通过双向链表串联）
//     列 = ThreadLocalPtr 实例（通过 uint32_t ID 索引）
//
//   每个单元格（ThreadData::entries[id]）存储一个原子指针。
//
// 数据结构：
//   StaticMeta（单例）
//     ├── head_（双向循环链表头，串联所有 ThreadData）
//     ├── handler_map_（ID → UnrefHandler 映射）
//     ├── mutex_（全局互斥锁，保护链表、handler_map_、entries 扩容）
//     ├── next_instance_id_（下一个可用 ID 的计数器）
//     ├── free_instance_ids_（已回收的 ID 池，供复用）
//     └── tls_（thread_local ThreadData* 指针，每线程各一份）
//
// 读写路径的无锁优化：
//   Get()/Reset() 操作直接读写 entries[id].ptr（原子操作），不需要加锁。
//   只有以下操作需要全局锁：
//     - entries 向量扩容（ReclaimId 期间可能并发调整大小）
//     - Scrape/Fold（需要遍历所有线程）
//     - GetId/ReclaimId（管理 ID 池）
//     - AddThreadData/RemoveThreadData（维护线程链表）
//
// 跨平台线程退出处理：
//   - Linux/macOS：通过 pthread_key_create 注册 OnThreadExit 回调
//   - Windows：通过 DLL_THREAD_DETACH 机制（TLS 回调）触发清理
//   - 主线程：通过函数内静态变量的析构函数（非 Windows）触发清理

#include "util/thread_local.h"

#include <stdlib.h>

#include "port/likely.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// Entry —— 线程局部数据表中的单个单元格
// 使用原子指针以支持 Get/Reset/Swap/CAS 的无锁操作
struct Entry {
  Entry() : ptr(nullptr) {}
  // 拷贝构造：relaxed 读取，因为拷贝通常在持锁状态下进行
  Entry(const Entry& e) : ptr(e.ptr.load(std::memory_order_relaxed)) {}
  std::atomic<void*> ptr;  // 实际存储的指针值（原子，支持无锁读写）
};

class StaticMeta;

// ThreadData —— 单个线程的所有 ThreadLocalPtr 数据的容器
//
// 结构说明：
//   entries: 按 ID 索引的原子指针向量，entries[id] 对应 ID 为 id 的 ThreadLocalPtr
//   next/prev: 双向链表指针，将所有线程的 ThreadData 串联起来（受全局 mutex_ 保护）
//   inst: 指向全局 StaticMeta 单例（用于线程退出时访问，避免 Instance() 可能失效）
//
// 每个线程的 ThreadData 在首次访问时懒创建，线程退出时由 OnThreadExit 销毁。
struct ThreadData {
  explicit ThreadData(ThreadLocalPtr::StaticMeta* _inst)
      : entries(), next(nullptr), prev(nullptr), inst(_inst) {}
  std::vector<Entry> entries;              // ID → 原子指针的映射表
  ThreadData* next;                        // 双向链表：下一个线程节点
  ThreadData* prev;                        // 双向链表：上一个线程节点
  ThreadLocalPtr::StaticMeta* inst;        // 所属的 StaticMeta 单例（缓存，防止析构顺序问题）
};

// ============================================================
// StaticMeta —— ThreadLocalPtr 的全局管理类（单例）
// ============================================================

class ThreadLocalPtr::StaticMeta {
 public:
  StaticMeta();

  // 分配一个新的唯一 ID（从空闲池取，池空则递增计数器）
  // 需要持有 mutex_
  uint32_t GetId();

  // 查看下一个可用 ID，但不实际分配（用于测试）
  // 需要持有 mutex_
  uint32_t PeekId() const;

  // 回收指定 ID：对所有线程中该 ID 对应的非 nullptr 指针调用 UnrefHandler，
  // 然后将 ID 放回空闲池供复用
  // 需要持有 mutex_
  void ReclaimId(uint32_t id);

  // 获取当前线程中 id 对应的指针值（无锁，原子 acquire 读）
  void* Get(uint32_t id) const;

  // 设置当前线程中 id 对应的指针值（无锁，原子 release 写）
  // 若 entries 需要扩容，加锁扩容后再写入
  void Reset(uint32_t id, void* ptr);

  // 原子交换当前线程中 id 对应的指针值，返回旧值
  void* Swap(uint32_t id, void* ptr);

  // 原子 CAS：仅当当前值 == expected 时写入 ptr
  // 失败时 expected 被更新为实际当前值
  bool CompareAndSwap(uint32_t id, void* ptr, void*& expected);

  // 批量收集所有线程中 id 对应的非 nullptr 值，并替换为 replacement
  // 需要持有全局 mutex_
  void Scrape(uint32_t id, autovector<void*>* ptrs, void* const replacement);

  // 对所有线程中 id 对应的值应用聚合函数
  // 持有全局 mutex_（防止 UnrefHandler 并发）
  void Fold(uint32_t id, FoldFunc func, void* res);

  // 注册指定 id 的 UnrefHandler（需持锁）
  void SetHandler(uint32_t id, UnrefHandler handler);

  // 获取全局互斥锁（函数内静态变量，确保初始化顺序正确）
  //
  // 设计说明：使用函数内静态变量而非全局静态变量，原因是：
  //   C++ 对不同编译单元中全局静态变量的构造顺序无保证，
  //   而函数内静态变量在首次调用时初始化，可通过控制首次调用时机
  //   保证正确的构造顺序（例如在 Env::Default() 中调用 InitSingletons()）。
  static port::Mutex* Mutex();

  // 获取本实例的 mutex_（成员变量版本）
  // 通常应使用 Mutex() 静态方法，但在 StaticMeta 自身可能已析构的情况下
  // （例如 OnThreadExit 在主线程析构后被调用），应使用此方法
  port::Mutex* MemberMutex() { return &mutex_; }

 private:
  // 获取指定 id 的 UnrefHandler（需持锁）
  UnrefHandler GetHandler(uint32_t id);

  // 线程退出时的清理回调
  // 遍历该线程所有 entries，对非 nullptr 值调用对应的 UnrefHandler，
  // 然后从全局链表中移除 ThreadData 并释放内存
  static void OnThreadExit(void* ptr);

  // 将当前线程的 ThreadData 插入全局双向链表（需持锁）
  void AddThreadData(ThreadData* d);

  // 从全局双向链表中移除当前线程的 ThreadData（需持锁）
  void RemoveThreadData(ThreadData* d);

  // 获取当前线程的 ThreadData（懒创建）
  // 首次调用时创建 ThreadData，注册到全局链表，并通过 pthread_setspecific
  // 注册线程退出回调
  static ThreadData* GetThreadLocal();

  uint32_t next_instance_id_;                    // 下一个可分配的 ID（单调递增）
  autovector<uint32_t> free_instance_ids_;       // 已回收的空闲 ID 池（供复用，避免 entries 无限增长）

  // 双向循环链表的哨兵头节点，串联所有线程的 ThreadData
  // 通过此链表，Scrape/Fold/ReclaimId 可以遍历所有线程
  ThreadData head_;

  std::unordered_map<uint32_t, UnrefHandler> handler_map_;  // ID → 清理回调

  // 私有全局互斥锁
  // 保护：head_（线程链表）、next_instance_id_、free_instance_ids_、
  //       handler_map_、所有 ThreadData::entries 的扩容操作
  // 注意：开发者应通过 Mutex() 访问，而非直接使用此成员变量
  port::Mutex mutex_;

  // 线程局部指针：指向当前线程的 ThreadData
  // 每个线程独立持有，通过 GetThreadLocal() 懒创建
  static thread_local ThreadData* tls_;

  // pthread key：用于注册 OnThreadExit 线程退出回调
  // 在非 macOS 平台上，也用于存储 ThreadData 指针（通过 pthread_setspecific）
  pthread_key_t pthread_key_;
};

// tls_ —— 线程局部 ThreadData 指针（每线程一份，初始为 nullptr）
thread_local ThreadData* ThreadLocalPtr::StaticMeta::tls_ = nullptr;

// ============================================================
// Windows 平台特殊处理
// ============================================================
// Windows 的 TLS 原语不支持每线程析构回调，因此需要通过
// DLL TLS 回调机制（.CRT$XLB 段）手动触发 OnThreadExit。
// 参考：
//   http://www.codeproject.com/Articles/8113/Thread-Local-Storage-The-C-Way
//   http://www.nynaeve.net/?p=183
//
// 注：使用 TLS 配合线程池时需谨慎，因为线程在现代使用中没有固定身份，
// 但在单次请求范围内是安全的。

#ifdef OS_WIN

namespace wintlscleanup {

// Windows 平台下的 TLS 清理例程指针（在 StaticMeta 构造时设置）
UnrefHandler thread_local_inclass_routine = nullptr;
// pthread_key（兼容 Windows 的 TLS key）
pthread_key_t thread_local_key = pthread_key_t(-1);

// Windows DLL TLS 回调：在线程分离（DLL_THREAD_DETACH）时触发
// 通过检查 TLS 值是否非 nullptr 来决定是否调用清理函数
void NTAPI WinOnThreadExit(PVOID module, DWORD reason, PVOID reserved) {
  // 忽略进程退出（PROCESS_EXIT），只处理线程分离（DLL_THREAD_DETACH）
  if (DLL_THREAD_DETACH == reason) {
    if (thread_local_key != pthread_key_t(-1) &&
        thread_local_inclass_routine != nullptr) {
      void* tls = TlsGetValue(thread_local_key);
      if (tls != nullptr) {
        thread_local_inclass_routine(tls);
      }
    }
  }
}

}  // namespace wintlscleanup

// extern "C" 抑制 C++ 名称修饰，使链接器可以通过 /INCLUDE 指令找到此符号
extern "C" {

#ifdef _MSC_VER
// 强制链接器保留 p_thread_callback_on_exit 变量（防止被优化删除）
// 通过 /INCLUDE 链接器指令强制引用此变量

#ifndef _X86_
// x64 平台：.CRT 段与 .rdata 合并，必须是 const
#pragma const_seg(".CRT$XLB")
extern const PIMAGE_TLS_CALLBACK p_thread_callback_on_exit;
const PIMAGE_TLS_CALLBACK p_thread_callback_on_exit =
    wintlscleanup::WinOnThreadExit;
#pragma const_seg()

#pragma comment(linker, "/include:_tls_used")
#pragma comment(linker, "/include:p_thread_callback_on_exit")

#else  // _X86_
// x86 平台：数据段
#pragma data_seg(".CRT$XLB")
PIMAGE_TLS_CALLBACK p_thread_callback_on_exit = wintlscleanup::WinOnThreadExit;
#pragma data_seg()

#pragma comment(linker, "/INCLUDE:__tls_used")
#pragma comment(linker, "/INCLUDE:_p_thread_callback_on_exit")

#endif  // _X86_

#else
// 非 MSVC 编译器（如 MinGW）：通过 DllMain 触发
// 参考：https://github.com/couchbase/gperftools/blob/master/src/windows/port.cc
BOOL WINAPI DllMain(HINSTANCE h, DWORD dwReason, PVOID pv) {
  if (dwReason == DLL_THREAD_DETACH)
    wintlscleanup::WinOnThreadExit(h, dwReason, pv);
  return TRUE;
}
#endif

}  // extern "C"

#endif  // OS_WIN

// ============================================================
// 单例初始化
// ============================================================

// 主动初始化单例（控制初始化时机，通常在 Env::Default() 中调用）
void ThreadLocalPtr::InitSingletons() { ThreadLocalPtr::Instance(); }

// 获取全局 StaticMeta 单例
//
// 设计要点：
//   1. 使用函数内静态指针（而非静态对象），避免主线程析构后子线程
//      使用时触发 use-after-destroy 问题
//   2. 意图泄漏（intentional leak）：不在程序结束时 delete inst，
//      因为子线程可能在主线程死后才退出，此时仍需访问 StaticMeta
//      （thread_local tls_ 的析构会调用 OnThreadExit，需要访问 inst）
//   3. 通过 thread_local tls_ 的构造/析构顺序保证正确性：
//      tls_ 的析构在子线程退出时发生，此时 inst 指针仍有效
ThreadLocalPtr::StaticMeta* ThreadLocalPtr::Instance() {
  static ThreadLocalPtr::StaticMeta* inst = new ThreadLocalPtr::StaticMeta();
  return inst;
}

// ============================================================
// StaticMeta 方法实现
// ============================================================

port::Mutex* ThreadLocalPtr::StaticMeta::Mutex() { return &Instance()->mutex_; }

// OnThreadExit —— 线程退出清理回调
//
// 触发时机：
//   - Linux/macOS：通过 pthread_key_create 注册，线程退出时自动调用
//   - Windows：通过 WinOnThreadExit 间接调用
//
// 清理步骤：
//   1. 重置 pthread key（防止重复调用）
//   2. 加全局锁，从链表中移除本线程的 ThreadData
//   3. 遍历所有 entries，对非 nullptr 指针调用对应的 UnrefHandler
//   4. 释放 ThreadData 内存
//
// 注意：使用缓存的 tls->inst 而非 Instance()，因为此时 StaticMeta 单例
// 可能已通过其他途径析构（主线程先于子线程退出的极端情况）
void ThreadLocalPtr::StaticMeta::OnThreadExit(void* ptr) {
  auto* tls = static_cast<ThreadData*>(ptr);
  assert(tls != nullptr);

  // 使用缓存的 inst，而非重新调用 Instance()（防止主线程析构后访问失效）
  auto* inst = tls->inst;
  // 清除 pthread key 的值，防止递归触发（某些平台会重复调用）
  pthread_setspecific(inst->pthread_key_, nullptr);

  MutexLock l(inst->MemberMutex());
  inst->RemoveThreadData(tls);  // 从全局链表摘除

  // 对本线程所有非 nullptr 的 entries 调用对应的 UnrefHandler
  uint32_t id = 0;
  for (auto& e : tls->entries) {
    void* raw = e.ptr.load();
    if (raw != nullptr) {
      auto unref = inst->GetHandler(id);
      if (unref != nullptr) {
        unref(raw);  // 调用清理回调（在持锁状态下！handler 不能再加锁）
      }
    }
    ++id;
  }
  // 释放 ThreadData 本身（无论平台，都在此统一删除）
  delete tls;
}

// StaticMeta 构造函数
//
// 初始化：
//   1. 创建 pthread key，注册 OnThreadExit 回调
//   2. 注册主线程的静态析构（非 Windows）：通过函数内静态对象 A 的析构，
//      在主线程退出时调用 OnThreadExit（主线程不会触发 pthread key 回调）
//   3. 初始化双向循环链表（head_ 指向自身）
//   4. Windows：将回调和 key 共享给 wintlscleanup
ThreadLocalPtr::StaticMeta::StaticMeta()
    : next_instance_id_(0), head_(this), pthread_key_(0) {
  // 创建 pthread key，关联 OnThreadExit 为线程退出回调
  if (pthread_key_create(&pthread_key_, &OnThreadExit) != 0) {
    abort();  // 系统资源耗尽，不可恢复
  }

  // 主线程退出时 OnThreadExit 不会被 pthread key 触发，
  // 通过函数内静态变量 A 的析构（在 ~StaticMeta 之后执行）来处理主线程清理。
  //
  // 注意事项：
  //   - ~A() 在 ~StaticMeta 之后调用（静态变量析构逆序），
  //     因此 StaticMeta 析构后不能访问其成员
  //   - 此机制在编译器处理内存回收的方式上可能有脆弱性，
  //     atexit(3) 可能是更稳健的替代方案
#if !defined(OS_WIN)
  static struct A {
    ~A() {
      if (tls_) {
        OnThreadExit(tls_);  // 处理主线程的 ThreadData 清理
      }
    }
  } a;
#endif  // !defined(OS_WIN)

  // 初始化双向循环链表：head_ 的 next 和 prev 均指向自身（空链表）
  head_.next = &head_;
  head_.prev = &head_;

#ifdef OS_WIN
  // 将 OnThreadExit 函数指针和 pthread_key 共享给 Windows TLS 清理模块
  wintlscleanup::thread_local_inclass_routine = OnThreadExit;
  wintlscleanup::thread_local_key = pthread_key_;
#endif
}

// AddThreadData —— 将新的 ThreadData 插入双向循环链表尾部（需持锁）
void ThreadLocalPtr::StaticMeta::AddThreadData(ThreadData* d) {
  Mutex()->AssertHeld();
  // 插入到 head_ 之前（即链表尾部）
  d->next = &head_;
  d->prev = head_.prev;
  head_.prev->next = d;
  head_.prev = d;
}

// RemoveThreadData —— 从双向循环链表中移除指定节点（需持锁）
void ThreadLocalPtr::StaticMeta::RemoveThreadData(ThreadData* d) {
  Mutex()->AssertHeld();
  d->next->prev = d->prev;
  d->prev->next = d->next;
  // 将 d 自身设为自环，防止悬空指针访问
  d->next = d->prev = d;
}

// GetThreadLocal —— 获取当前线程的 ThreadData（懒创建）
//
// 首次调用时：
//   1. 创建新的 ThreadData
//   2. 加锁，将其插入全局链表（必须在注册 pthread key 前，
//      确保 OnThreadExit 触发时链表已包含此节点）
//   3. 通过 pthread_setspecific 注册，使线程退出时自动触发 OnThreadExit
//   4. 若 pthread_setspecific 失败，回滚并 abort
ThreadData* ThreadLocalPtr::StaticMeta::GetThreadLocal() {
  if (UNLIKELY(tls_ == nullptr)) {
    auto* inst = Instance();
    tls_ = new ThreadData(inst);
    {
      // 先注册到全局链表，再设置 pthread key
      // 确保 OnThreadExit 触发时可以正确找到此 ThreadData
      MutexLock l(Mutex());
      inst->AddThreadData(tls_);
    }
    // 注册 pthread key，使线程退出时触发 OnThreadExit
    // 即使不是 macOS，也需要注册，以便 OnThreadExit 被调用
    if (pthread_setspecific(inst->pthread_key_, tls_) != 0) {
      // 注册失败（极罕见），回滚并终止进程
      {
        MutexLock l(Mutex());
        inst->RemoveThreadData(tls_);
      }
      delete tls_;
      abort();
    }
  }
  return tls_;
}

// ============================================================
// 核心读写操作（无锁路径）
// ============================================================

// Get —— 获取当前线程中 id 对应的指针（原子 acquire 读）
void* ThreadLocalPtr::StaticMeta::Get(uint32_t id) const {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // entries 不够大，说明该线程从未设置过此 id，返回 nullptr
    return nullptr;
  }
  // acquire 语义：确保后续内存操作不会被重排序到此读之前
  return tls->entries[id].ptr.load(std::memory_order_acquire);
}

// Reset —— 设置当前线程中 id 对应的指针（原子 release 写）
void ThreadLocalPtr::StaticMeta::Reset(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // 需要扩容 entries，加锁保护（ReclaimId 期间可能并发读取 entries.size()）
    MutexLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  // release 语义：确保前面的写操作对通过 Get() acquire 读取的线程可见
  tls->entries[id].ptr.store(ptr, std::memory_order_release);
}

// Swap —— 原子交换（原子 acquire 读旧值 + 写新值）
void* ThreadLocalPtr::StaticMeta::Swap(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    MutexLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  // exchange：原子地将 ptr 写入，并返回旧值
  return tls->entries[id].ptr.exchange(ptr, std::memory_order_acquire);
}

// CompareAndSwap —— 原子 CAS
// 成功（当前值 == expected）：写入 ptr，返回 true
// 失败（当前值 != expected）：将 expected 更新为实际值，返回 false
bool ThreadLocalPtr::StaticMeta::CompareAndSwap(uint32_t id, void* ptr,
                                                void*& expected) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    MutexLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  // compare_exchange_strong：成功用 release 写，失败用 relaxed 读
  return tls->entries[id].ptr.compare_exchange_strong(
      expected, ptr, std::memory_order_release, std::memory_order_relaxed);
}

// ============================================================
// 全局遍历操作（需持锁）
// ============================================================

// Scrape —— 收集所有线程中 id 对应的非 nullptr 值，并替换为 replacement
// 典型用途：MemTable 切换时，收集所有线程持有的读引用
void ThreadLocalPtr::StaticMeta::Scrape(uint32_t id, autovector<void*>* ptrs,
                                        void* const replacement) {
  MutexLock l(Mutex());
  // 遍历双向链表，访问每个线程的 ThreadData
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      // 原子交换：将旧值替换为 replacement，取出旧值
      void* ptr =
          t->entries[id].ptr.exchange(replacement, std::memory_order_acquire);
      if (ptr != nullptr) {
        ptrs->push_back(ptr);
      }
    }
  }
}

// Fold —— 对所有线程中 id 对应的非 nullptr 值应用聚合函数
// 典型用途：统计所有线程的某个计数器总和
void ThreadLocalPtr::StaticMeta::Fold(uint32_t id, FoldFunc func, void* res) {
  MutexLock l(Mutex());
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.load();
      if (ptr != nullptr) {
        func(ptr, res);  // 调用聚合函数（在持锁状态下！不能再加锁）
      }
    }
  }
}

// TEST_PeekId —— 测试辅助：查看下一个可用 ID（不分配）
uint32_t ThreadLocalPtr::TEST_PeekId() { return Instance()->PeekId(); }

// SetHandler —— 注册指定 id 的 UnrefHandler
void ThreadLocalPtr::StaticMeta::SetHandler(uint32_t id, UnrefHandler handler) {
  MutexLock l(Mutex());
  handler_map_[id] = handler;
}

// GetHandler —— 获取指定 id 的 UnrefHandler（需持锁）
UnrefHandler ThreadLocalPtr::StaticMeta::GetHandler(uint32_t id) {
  Mutex()->AssertHeld();
  auto iter = handler_map_.find(id);
  if (iter == handler_map_.end()) {
    return nullptr;
  }
  return iter->second;
}

// GetId —— 分配一个唯一 ID
// 优先从空闲池取（复用已回收的 ID），池空则递增计数器
uint32_t ThreadLocalPtr::StaticMeta::GetId() {
  MutexLock l(Mutex());
  if (free_instance_ids_.empty()) {
    return next_instance_id_++;
  }

  uint32_t id = free_instance_ids_.back();
  free_instance_ids_.pop_back();
  return id;
}

// PeekId —— 查看下一个可用 ID（不实际分配，仅用于测试）
uint32_t ThreadLocalPtr::StaticMeta::PeekId() const {
  MutexLock l(Mutex());
  if (!free_instance_ids_.empty()) {
    return free_instance_ids_.back();
  }
  return next_instance_id_;
}

// ReclaimId —— 回收一个 ID，清理所有线程中该 ID 的数据
//
// 步骤：
//   1. 遍历所有线程的 ThreadData，对非 nullptr 的 entries[id] 调用 UnrefHandler
//   2. 清空 handler_map_[id]
//   3. 将 id 放回空闲池（free_instance_ids_）
void ThreadLocalPtr::StaticMeta::ReclaimId(uint32_t id) {
  MutexLock l(Mutex());
  auto unref = GetHandler(id);
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      // 原子置 nullptr，取出旧值
      void* ptr = t->entries[id].ptr.exchange(nullptr);
      if (ptr != nullptr && unref != nullptr) {
        unref(ptr);  // 调用 UnrefHandler（持锁，handler 不能再加锁）
      }
    }
  }
  handler_map_[id] = nullptr;          // 清除 handler 注册
  free_instance_ids_.push_back(id);    // 放回空闲池
}

// ============================================================
// ThreadLocalPtr 公共接口实现
// ============================================================

// 构造：分配唯一 ID，可选注册 UnrefHandler
ThreadLocalPtr::ThreadLocalPtr(UnrefHandler handler)
    : id_(Instance()->GetId()) {
  if (handler != nullptr) {
    Instance()->SetHandler(id_, handler);
  }
}

// 析构：回收 ID，清理所有线程中该 ID 的数据
ThreadLocalPtr::~ThreadLocalPtr() { Instance()->ReclaimId(id_); }

// 获取当前线程的指针值（无锁，原子 acquire）
void* ThreadLocalPtr::Get() const { return Instance()->Get(id_); }

// 设置当前线程的指针值（无锁，原子 release）
void ThreadLocalPtr::Reset(void* ptr) { Instance()->Reset(id_, ptr); }

// 原子交换当前线程的指针值
void* ThreadLocalPtr::Swap(void* ptr) { return Instance()->Swap(id_, ptr); }

// 原子 CAS
bool ThreadLocalPtr::CompareAndSwap(void* ptr, void*& expected) {
  return Instance()->CompareAndSwap(id_, ptr, expected);
}

// 批量收集所有线程的值并替换（需持全局锁）
void ThreadLocalPtr::Scrape(autovector<void*>* ptrs, void* const replacement) {
  Instance()->Scrape(id_, ptrs, replacement);
}

// 对所有线程的值应用聚合函数（需持全局锁）
void ThreadLocalPtr::Fold(FoldFunc func, void* res) {
  Instance()->Fold(id_, func, res);
}

}  // namespace ROCKSDB_NAMESPACE
