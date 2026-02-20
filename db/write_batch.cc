//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeSingleDeletion varstring
//    kTypeRangeDeletion varstring varstring
//    kTypeMerge varstring varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring
//    kTypeColumnFamilySingleDeletion varint32 varstring
//    kTypeColumnFamilyRangeDeletion varint32 varstring varstring
//    kTypeColumnFamilyMerge varint32 varstring varstring
//    kTypeBeginPrepareXID
//    kTypeEndPrepareXID varstring
//    kTypeCommitXID varstring
//    kTypeCommitXIDAndTimestamp varstring varstring
//    kTypeRollbackXID varstring
//    kTypeBeginPersistedPrepareXID
//    kTypeBeginUnprepareXID
//    kTypeWideColumnEntity varstring varstring
//    kTypeColumnFamilyWideColumnEntity varint32 varstring varstring
//    kTypeNoop
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "rocksdb/write_batch.h"

#include <algorithm>
#include <limits>
#include <map>
#include <stack>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/kv_checksum.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/snapshot_impl.h"
#include "db/trim_history_scheduler.h"
#include "db/wide/wide_column_serialization.h"
#include "db/write_batch_internal.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/system_clock.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/duplicate_detector.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// anon namespace for file-local types
namespace {

enum ContentFlags : uint32_t {
  DEFERRED = 1 << 0,
  HAS_PUT = 1 << 1,
  HAS_DELETE = 1 << 2,
  HAS_SINGLE_DELETE = 1 << 3,
  HAS_MERGE = 1 << 4,
  HAS_BEGIN_PREPARE = 1 << 5,
  HAS_END_PREPARE = 1 << 6,
  HAS_COMMIT = 1 << 7,
  HAS_ROLLBACK = 1 << 8,
  HAS_DELETE_RANGE = 1 << 9,
  HAS_BLOB_INDEX = 1 << 10,
  HAS_BEGIN_UNPREPARE = 1 << 11,
  HAS_PUT_ENTITY = 1 << 12,
};

struct BatchContentClassifier : public WriteBatch::Handler {
  uint32_t content_flags = 0;

  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_PUT;
    return Status::OK();
  }

  Status PutEntityCF(uint32_t /* column_family_id */, const Slice& /* key */,
                     const Slice& /* entity */) override {
    content_flags |= ContentFlags::HAS_PUT_ENTITY;
    return Status::OK();
  }

  Status DeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE;
    return Status::OK();
  }

  Status SingleDeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_SINGLE_DELETE;
    return Status::OK();
  }

  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE_RANGE;
    return Status::OK();
  }

  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_MERGE;
    return Status::OK();
  }

  Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_BLOB_INDEX;
    return Status::OK();
  }

  Status MarkBeginPrepare(bool unprepare) override {
    content_flags |= ContentFlags::HAS_BEGIN_PREPARE;
    if (unprepare) {
      content_flags |= ContentFlags::HAS_BEGIN_UNPREPARE;
    }
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice&) override {
    content_flags |= ContentFlags::HAS_END_PREPARE;
    return Status::OK();
  }

  Status MarkCommit(const Slice&) override {
    content_flags |= ContentFlags::HAS_COMMIT;
    return Status::OK();
  }

  Status MarkCommitWithTimestamp(const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_COMMIT;
    return Status::OK();
  }

  Status MarkRollback(const Slice&) override {
    content_flags |= ContentFlags::HAS_ROLLBACK;
    return Status::OK();
  }
};

}  // anonymous namespace

struct SavePoints {
  std::stack<SavePoint, autovector<SavePoint>> stack;
};

// ============================================================================
// WriteBatch::WriteBatch(size_t reserved_bytes, size_t max_bytes,
//                         size_t protection_bytes_per_key, size_t default_cf_ts_sz)
//
// 功能描述:
//   创建一个新的空 WriteBatch 对象，用于批量存储数据库更新操作。
//   这是 WriteBatch 的主要构造函数，支持预分配空间、最大字节数限制、
//   保护字节和时间戳大小配置。
//
// 参数说明:
//   reserved_bytes (size_t):
//     - 预留的字节数，用于避免频繁的内存重新分配
//     - 如果知道预计的批次大小，设置此值可以提高性能
//     - 如果设置为 0，则使用默认的最小头部大小（12 字节）
//     - 实际预留空间至少为 WriteBatchInternal::kHeader（12 字节）
//     - 预留空间不占用实际内存，只是设置 rep_ 的容量（capacity）
//
//   max_bytes (size_t):
//     - WriteBatch 的最大允许字节数限制
//     - 当批次大小超过此限制时，添加操作会返回 Status::Incomplete() 错误
//     - 用于防止单个批次过大，影响系统性能
//     - 如果设置为 0，则不限制大小（默认）
//     - 包括序列号、计数和所有操作记录的总大小
//
//   protection_bytes_per_key (size_t):
//     - 为每个条目（key）分配的保护字节数
//     - 目前支持两种值：0（禁用）或 8（启用）
//     - 用于存储校验和信息，确保数据完整性
//     - 启用后会创建 ProtectionInfo 对象，每个 key 保存 8 字节保护信息
//     - 断言检查：如果不是 0 或 8，程序会终止（assert 失败）
//
//   default_cf_ts_sz (size_t):
//     - 默认列族（column family）的时间戳大小
//     - 当使用用户定义时间戳（user-defined timestamp）功能时使用
//     - 如果列族启用了时间戳，每个 key 末尾会附加固定长度的时间戳
//     - 时间戳大小通常为 0、8 或其他固定值（取决于列族配置）
//     - 用于 UpdateTimestamps() 等函数中判断是否需要更新时间戳
//
// 初始化行为:
//   1. 设置 content_flags_ = 0:
//      - 表示内容标志位未初始化，需要延迟计算（lazy evaluation）
//      - 用于快速查询批次中是否包含特定类型的操作（如 HasPut()、HasDelete()）
//      - 实际计算在首次调用 ComputeContentFlags() 时进行
//
//   2. 设置 max_bytes_ = max_bytes:
//      - 存储最大字节限制
//      - 在添加操作时检查：if (rep_.size() > max_bytes_) return Status::Incomplete()
//
//   3. 设置 default_cf_ts_sz_ = default_cf_ts_sz:
//      - 存储默认列族的时间戳大小
//      - 用于判断是否需要更新时间戳（needs_in_place_update_ts_ 标志）
//
//   4. 初始化 rep_（内部缓冲区）:
//      - rep_ 是存储所有序列化操作的字符串缓冲区
//      - 格式：[sequence: 8字节] [count: 4字节] [data: 操作记录]
//      - 预留空间（reserve）：避免频繁的重新分配和复制
//        * 如果 reserved_bytes > 12，则预留 reserved_bytes
//        * 否则，预留最小头部大小 12 字节（WriteBatchInternal::kHeader）
//      - 调整大小（resize）：设置 rep_ 的初始大小为 12 字节
//        * 前 8 字节：sequence（初始为 0，写入时由 DB 分配）
//        * 后 4 字节：count（初始为 0，表示没有操作记录）
//        * 从第 12 字节开始：操作记录数据区
//
//   5. 创建保护信息（如果启用）:
//      - 如果 protection_bytes_per_key != 0，创建 ProtectionInfo 对象
//      - ProtectionInfo 存储每个 key 的校验和信息（8 字节）
//      - 校验和信息用于 VerifyChecksum() 验证数据完整性
//
// 使用示例:
//
//   1. 创建默认 WriteBatch（最常用）:
//   WriteBatch batch;  // 等同于 WriteBatch(0, 0, 0, 0)
//   batch.Put("key1", "value1");
//   batch.Delete("key2");
//
//   2. 预分配空间（提高性能）:
//   WriteBatch batch(1024);  // 预分配 1KB，避免频繁扩容
//   for (int i = 0; i < 100; i++) {
//     batch.Put("key" + std::to_string(i), "value" + std::to_string(i));
//   }
//
//   3. 限制批次大小:
//   WriteBatch batch(0, 1024 * 1024);  // 最大 1MB
//   Status s = batch.Put("large_key", large_value);
//   if (s.IsIncomplete()) {
//     // 批次已满，需要分批写入
//   }
//
//   4. 启用保护（校验和）:
//   WriteBatch batch(0, 0, 8);  // 每个条目 8 字节保护信息
//   batch.Put("key1", "value1");
//   Status s = batch.VerifyChecksum();  // 验证完整性
//
//   5. 配置时间戳大小:
//   WriteBatch batch(0, 0, 0, 8);  // 默认列族时间戳大小为 8 字节
//   batch.Put("key1", "value1");  // key 末尾会附加 8 字节时间戳
//
// 内存布局（构造后）:
//
//   rep_ 的初始状态（12 字节）:
//   [0-7]   sequence (8 字节固定整数): 0x0000000000000000
//   [8-11]  count (4 字节固定整数): 0x00000000
//   [12-]   data: 空（等待添加操作）
//
// 线程安全性:
//   - 构造函数本身是线程安全的（初始化阶段）
//   - 但构造后的 WriteBatch 对象不是线程安全的
//   - 多个线程调用 Put/Delete 等方法需要外部同步
//   - 多个线程调用 const 方法（如 GetDataSize()）是安全的
//
// 性能考虑:
//   1. 预分配空间（reserved_bytes）:
//      - 避免多次内存重新分配和数据复制
//      - std::string 的扩容通常是按倍增策略（如 2 倍）
//      - 预分配后，添加操作只需要追加数据，无需重新分配
//      - 建议：根据预期的批次大小预留合理空间
//
//   2. 最大字节限制（max_bytes）:
//      - 防止单个批次过大导致内存压力
//      - 过大的批次会影响 WAL 写入和 MemTable 插入性能
//      - 建议：根据系统资源设置合理上限（如 1MB、10MB）
//
//   3. 保护字节（protection_bytes_per_key）:
//      - 启用后会增加内存消耗（每个 key +8 字节）
//      - 同时增加 CPU 开销（计算和验证校验和）
//      - 仅在需要强数据完整性保证时启用
//
//   4. 延迟计算（content_flags_）:
//      - 标志位按需计算，避免遍历所有操作
//      - 首次调用 HasPut() 等函数时才计算
//      - 适用于只查询一次或几次的场景
//
// 错误处理:
//   - 如果 protection_bytes_per_key 不是 0 或 8，触发 assert 失败
//   - 如果添加操作导致超过 max_bytes_，返回 Status::Incomplete()
//   - 内存分配失败会抛出 std::bad_alloc 异常
//
// 相关函数:
//   - WriteBatch::Put/Delete/Merge(): 添加操作到批次
//   - WriteBatch::Clear(): 清空批次（重置为初始状态）
//   - WriteBatch::GetDataSize(): 获取当前数据大小
//   - WriteBatch::Release(): 释放序列化数据并清空批次
//   - WriteBatch::VerifyChecksum(): 验证校验和（如果启用保护）
//
// 注意事项:
//   1. reserved_bytes 和 max_bytes 的区别:
//      - reserved_bytes: 预分配容量（capacity），优化性能
//      - max_bytes: 实际大小限制（size），防止过大
//      - 示例: WriteBatch(1024, 4096) 表示：
//        * 预留 1KB 空间（至少 12 字节）
//        * 但实际大小限制为 4KB
//
//   2. protection_bytes_per_key 的限制:
//      - 当前只支持 0 或 8，未来可能支持其他值
//      - 值为 8 时，ProtectionInfo 会为每个 key 存储 8 字节校验和
//      - 校验和信息独立于 rep_，不包含在序列化数据中
//
//   3. 默认构造函数行为:
//      - WriteBatch() 等同于 WriteBatch(0, 0, 0, 0)
//      - 使用简化的重载构造函数（委托给这个主构造函数）
//      - 大多数情况下使用默认参数即可
//
//   4. 时间戳大小的影响:
//      - default_cf_ts_sz 不影响 rep_ 的格式
//      - 时间戳存储在 key 的末尾（作为 varstring 的一部分）
//      - UpdateTimestamps() 函数使用此参数判断是否需要更新
//   ============================================================================
WriteBatch::WriteBatch(size_t reserved_bytes, size_t max_bytes,
                       size_t protection_bytes_per_key, size_t default_cf_ts_sz)
    : content_flags_(0),
      max_bytes_(max_bytes),
      default_cf_ts_sz_(default_cf_ts_sz),
      rep_() {
  // 当前 `protection_bytes_per_key` 只能为每个条目启用 8 字节的保护信息。
  // 断言检查：如果不是 0 或 8，程序会在调试模式下终止。
  // 0 表示禁用保护（不计算和存储校验和），8 表示启用保护（每个 key 保存 8 字节校验和）。
  assert(protection_bytes_per_key == 0 || protection_bytes_per_key == 8);

  // 如果启用了保护信息（protection_bytes_per_key != 0），则创建 ProtectionInfo 对象。
  // ProtectionInfo 用于存储每个 key 的校验和信息（8 字节/条目）。
  // 校验和信息用于后续的 VerifyChecksum() 调用来验证数据完整性。
  if (protection_bytes_per_key != 0) {
    prot_info_.reset(new WriteBatch::ProtectionInfo());
  }

  // 预留 rep_ 的内存空间（capacity），避免频繁的内存重新分配和数据复制。
  // WriteBatchInternal::kHeader = 12，表示 WriteBatch 头部固定为 12 字节（8 字节 sequence + 4 字节 count）。
  // 逻辑：如果用户指定的 reserved_bytes > 12，则预留 reserved_bytes 字节；
  //       否则，预留最小头部大小 12 字节。
  // 这样确保 rep_ 至少有 12 字节的容量，避免后续添加操作时立即触发扩容。
  rep_.reserve((reserved_bytes > WriteBatchInternal::kHeader)
                   ? reserved_bytes
                   : WriteBatchInternal::kHeader);

  // 调整 rep_ 的大小（size）为 WriteBatchInternal::kHeader（12 字节）。
  // 这会初始化前 12 个字节为 0x00（根据 C++ 标准，std::string::resize() 会将新增字符初始化为 '\0'）。
  // 初始化后的内存布局：
  //   [0-7]   sequence: 8 字节固定整数（初始为 0，写入时由 DB 分配）
  //   [8-11]  count: 4 字节固定整数（初始为 0，表示没有操作记录）
  //   [12-]   data: 操作记录数据区（当前为空，等待添加操作）
  // 注意：resize() 会改变 size，但不会改变 capacity（capacity 已经在 reserve() 中设置）。
  rep_.resize(WriteBatchInternal::kHeader);
}

// ============================================================================
// WriteBatch::WriteBatch(const std::string& rep)
//
// 功能描述:
//   从已序列化的字符串数据创建 WriteBatch 对象。
//   用于从 WAL（Write-Ahead Log）、备份或其他存储中恢复 WriteBatch。
//
// 参数说明:
//   rep (const std::string&):
//     - 已序列化的 WriteBatch 数据（包含头部和操作记录）
//     - 格式：[sequence: 8字节] [count: 4字节] [data: 操作记录]
//     - 必须符合 WriteBatch 的序列化格式（参考 write_batch.cc:10-37）
//     - rep 会被复制到新的 WriteBatch 对象的 rep_ 成员
//     - 调用方负责确保 rep 的格式正确
//
// 初始化行为:
//   1. content_flags_ = ContentFlags::DEFERRED:
//      - 标志位设置为延迟计算状态
//      - 表示尚未解析 rep_ 的内容
//      - 首次调用 HasPut() 等函数时，会遍历 rep_ 计算标志位
//      - 避免在构造时就遍历整个数据（提高性能）
//
//   2. max_bytes_ = 0:
//      - 不限制批次大小
//      - 因为 rep 已经是完整的数据，无需再限制
//      - 如果需要限制，应该在添加操作时控制
//
//   3. rep_ = rep (复制):
//      - 深度复制传入的字符串
//      - 创建新的字符串对象，与输入 rep 独立
//      - 适用于需要保留原始数据的场景
//
// 使用示例:
//
//   1. 从 WAL 中恢复 WriteBatch:
//   std::string wal_data = ReadFromWAL();  // 从 WAL 文件读取
//   WriteBatch batch(wal_data);             // 从序列化数据创建
//   batch.Iterate(&handler);                // 遍历并恢复操作
//
//   2. 从备份中恢复:
//   std::string backup_data = ReadFromBackup();
//   WriteBatch batch(backup_data);
//   Status s = db->Write(WriteOptions(), &batch);
//
//   3. 网络传输后重建:
//   std::string received = ReceiveFromNetwork();
//   WriteBatch batch(received);
//   batch.VerifyChecksum();  // 验证完整性
//
//   4. 复制 WriteBatch:
//   WriteBatch original;
//   original.Put("key1", "value1");
//   WriteBatch copy(original.Data());  // 从序列化数据创建副本
//
// 性能考虑:
//   - 构造时只复制字符串，不解析内容（O(n) 时间，n = rep.size()）
//   - 标志位延迟计算，避免不必要的遍历
//   - 如果已知内容，可以预先设置标志位（需要访问内部接口）
//   - 大数据量的批次可能占用较多内存（完整复制）
//
// 线程安全性:
//   - 构造函数是线程安全的（单次操作）
//   - 构造后的对象不是线程安全的
//   - 输入 rep 在构造期间不应被修改
//
// 错误处理:
//   - 如果 rep 格式不正确，在 Iterate() 或其他操作时会失败
//   - 内存分配失败会抛出 std::bad_alloc 异常
//   - 不会在构造时验证 rep 的格式（延迟到实际使用）
//
// 注意事项:
//   1. 与移动构造函数的区别:
//      - 此构造函数复制数据（深拷贝）
//      - WriteBatch(std::string&& rep) 移动数据（零拷贝）
//      - 如果不需要保留原始数据，优先使用移动构造函数
//
//   2. 格式验证:
//      - 构造时不验证 rep 的格式
//      - 调用 Iterate()、Count() 等函数时才会解析
//      - 如果格式错误，解析函数会返回错误
//
//   3. 性能优化:
//      - 如果只是临时使用，优先使用移动语义
//      - 如果需要长期保存，复制构造是合适的
//      - 可以考虑使用共享指针避免多次复制
//
//   4. 时间戳和保护信息:
//      - rep 包含所有操作数据（包括时间戳，如果有的话）
//      - 但不会恢复 ProtectionInfo（校验和需要重新计算）
//      - 需要手动调用 VerifyChecksum() 验证完整性
// ============================================================================
WriteBatch::WriteBatch(const std::string& rep)
    : content_flags_(ContentFlags::DEFERRED), max_bytes_(0), rep_(rep) {}

// ============================================================================
// WriteBatch::WriteBatch(std::string&& rep)
//
// 功能描述:
//   从已序列化的字符串数据创建 WriteBatch 对象（移动语义）。
//   这是高效的构造方式，适用于不需要保留原始字符串的场景。
//
// 参数说明:
//   rep (std::string&&):
//     - 已序列化的 WriteBatch 数据（右值引用）
//     - 使用移动语义，避免数据复制
//     - 输入 rep 会被清空（或移出），不应再使用
//     - 必须符合 WriteBatch 的序列化格式
//
// 初始化行为:
//   1. content_flags_ = ContentFlags::DEFERRED:
//      - 标志位设置为延迟计算状态
//      - 尚未解析 rep_ 的内容
//
//   2. max_bytes_ = 0:
//      - 不限制批次大小
//
//   3. rep_ = std::move(rep) (移动):
//      - 直接转移 rep 的内部缓冲区所有权
//      - 不分配新内存，不复制数据（O(1) 时间）
//      - 原始 rep 被清空（或处于未定义状态）
//
// 使用示例:
//
//   1. 从临时字符串创建（最高效）:
//   std::string wal_data = ReadFromWAL();  // 读取 WAL
//   WriteBatch batch(std::move(wal_data)); // 移动构造（零拷贝）
//   // wal_data 现在为空或未定义，不应再使用
//   batch.Iterate(&handler);
//
//   2. 从函数返回值创建:
//   std::string SerializeBatch() {
//     std::string data;
//     // ... 填充数据 ...
//     return data;  // 返回值优化（RVO）
//   }
//   WriteBatch batch(SerializeBatch());  // 隐式移动
//
//   3. 链式操作:
//   WriteBatch batch(CombineMultipleBatches(batches));  // 移动临时对象
//
//   4. 使用 std::move 明确移动:
//   std::string data = ...;
//   WriteBatch batch(std::move(data));  // 明确表达移动意图
//
// 性能考虑:
//   - 移动构造是 O(1) 操作（仅转移指针）
//   - 不分配内存，不复制数据
//   - 比复制构造快得多（尤其是大数据量）
//   - 编译器优化时可能自动使用移动语义
//
// 线程安全性:
//   - 构造函数是线程安全的（单次操作）
//   - 但移动后，原始 rep 不应被其他线程访问
//   - 构造后的对象不是线程安全的
//
// 错误处理:
//   - 如果 rep 格式不正确，在后续操作时会失败
//   - 内存操作失败的可能性极低（只移动指针）
//
// 注意事项:
//   1. 移动后原始字符串的状态:
//      - rep 被移出后，处于有效但未指定的状态
//      - 可以安全地重新赋值或析构
//      - 但不应假设其内容或大小
//
//   2. 与复制构造的选择:
//      - 如果不需要保留原始数据，优先使用移动构造
//      - 如果需要原始数据，使用复制构造 WriteBatch(const std::string&)
//
//   3. 返回值优化（RVO/NRVO）:
//      - 函数返回 std::string 时，编译器可能自动优化
//      - 显式使用 std::move 可能阻止优化
//      - 但在大多数情况下，移动构造是明确的选择
//
//   4. 格式验证:
//      - 与复制构造一样，不验证 rep 的格式
//      - 延迟到 Iterate()、Count() 等函数时解析
//
// 相关构造函数:
//   - WriteBatch(const std::string&): 复制构造（深拷贝）
//   - WriteBatch(const WriteBatch&): 从另一个 WriteBatch 复制
//   - WriteBatch(WriteBatch&&): 从另一个 WriteBatch 移动
//   - WriteBatch(size_t, size_t, size_t, size_t): 创建空批次
// ============================================================================
WriteBatch::WriteBatch(std::string&& rep)
    : content_flags_(ContentFlags::DEFERRED),
      max_bytes_(0),
      rep_(std::move(rep)) {}

// ============================================================================
// WriteBatch::WriteBatch(const WriteBatch& src)
//
// 功能描述:
//   拷贝构造函数：从另一个 WriteBatch 对象创建副本。
//   深度复制所有数据和状态，生成完全独立的 WriteBatch 对象。
//
// 参数说明:
//   src (const WriteBatch&):
//     - 源 WriteBatch 对象，用于复制
//     - 包括所有操作记录、状态标志、保存点等信息
//     - 不会被修改（const 引用）
//
// 初始化行为:
//   1. wal_term_point_ = src.wal_term_point_:
//      - 复制 WAL 终止点（SavePoint 对象）
//      - SavePoint 包含 size、count、content_flags 三个字段
//      - 用于控制哪些记录写入 WAL
//
//   2. content_flags_ = src.content_flags_.load(std::memory_order_relaxed):
//      - 原子加载源对象的标志位
//      - 使用 relaxed 内存序（保证原子性，但不保证顺序）
//      - 复制已计算的内容标志（如 HAS_PUT、HAS_DELETE 等）
//      - 避免延迟计算，直接使用已知的标志位
//
//   3. max_bytes_ = src.max_bytes_:
//      - 复制最大字节限制
//      - 新对象具有相同的大小限制
//
//   4. default_cf_ts_sz_ = src.default_cf_ts_sz_:
//      - 复制默认列族的时间戳大小
//      - 用于判断是否需要更新时间戳
//
//   5. rep_ = src.rep_ (深度复制):
//      - 复制序列化数据（std::string 的拷贝构造）
//      - 包含头部（sequence + count）和所有操作记录
//      - 新对象与源对象完全独立
//
//   6. save_points_ (条件复制):
//      - 如果 src.save_points_ != nullptr:
//        * 创建新的 SavePoints 对象
//        * 复制保存点栈（src.save_points_->stack）
//        * SavePoint 是一个简单的结构体，可以直接赋值
//      - 否则，save_points_ 保持为 nullptr（无保存点）
//
//   7. prot_info_ (条件复制):
//      - 如果 src.prot_info_ != nullptr:
//        * 创建新的 ProtectionInfo 对象
//        * 复制保护信息条目（src.prot_info_->entries_）
//        * entries_ 存储每个 key 的校验和（8 字节）
//      - 否则，prot_info_ 保持为 nullptr（无保护信息）
//
// 使用示例:
//
//   1. 简单复制:
//   WriteBatch original;
//   original.Put("key1", "value1");
//   original.Delete("key2");
//
//   WriteBatch copy(original);  // 深度复制
//   copy.Put("key3", "value3"); // 不影响 original
//
//   2. 函数参数传递:
//   void ProcessBatch(WriteBatch batch) {  // 值传递，调用拷贝构造
//     batch.Iterate(&handler);
//   }
//
//   WriteBatch batch;
//   batch.Put("key1", "value1");
//   ProcessBatch(batch);  // 复制一份传递
//
//   3. 容器存储:
//   std::vector<WriteBatch> batches;
//   WriteBatch batch;
//   batch.Put("key1", "value1");
//   batches.push_back(batch);  // 复制并存储
//
//   4. 备份原始批次:
//   WriteBatch original = ...;
//   WriteBatch backup(original);  // 备份
//   original.Put("key", "value"); // 修改 original
//   // backup 保持不变
//
// 性能考虑:
//   - 复制 rep_ 是 O(n) 操作（n = rep_.size()）
//   - 复制 save_points_ 和 prot_info_ 是 O(m) 操作（m = 栈深度 / 条目数）
//   - 原子加载 content_flags_ 是 O(1) 操作
//   - 对于大数据量的批次，复制可能较慢
//
// 线程安全性:
//   - 构造函数本身是线程安全的
//   - 但在复制期间，src 不应被其他线程修改
//   - 使用 std::memory_order_relaxed 读取 content_flags_
//   - 假设调用方已确保 src 在复制期间的线程安全
//
// 错误处理:
//   - 内存分配失败会抛出 std::bad_alloc 异常
//   - 对象可能处于部分构造状态（需要 RAII 保证）
//   - RocksDB 的异常处理策略：避免异常，使用 Status 返回
//
// 注意事项:
//   1. 深度复制的含义:
//      - rep_、save_points_、prot_info_ 都是独立副本
//      - 修改副本不会影响源对象
//      - 反之亦然
//
//   2. content_flags_ 的复制:
//      - 直接复制已计算的标志位（原子加载）
//      - 避免延迟计算（直接使用 src 的结果）
//      - 如果 src 的标志位是 DEFERRED，副本也是 DEFERRED
//
//   3. save_points_ 的语义:
//      - SavePoint 用于回滚（RollbackToSavePoint）
//      - 复制后，副本和源对象有相同的保存点历史
//      - 但可以独立调用 SetSavePoint() 和 RollbackToSavePoint()
//
//   4. prot_info_ 的语义:
//      - ProtectionInfo 存储校验和信息
//      - 复制后，副本可以独立调用 VerifyChecksum()
//      - 如果源对象未验证，副本也不会自动验证
//
//   5. 性能优化:
//      - 如果不需要完整的副本，考虑使用移动语义
//      - 如果只是读取，考虑使用 const 引用
//      - 对于函数参数，使用 const WriteBatch& 避免复制
//
// 相关函数:
//   - WriteBatch::operator=(const WriteBatch&): 拷贝赋值运算符
//   - WriteBatch(WriteBatch&&): 移动构造函数（更高效）
//   - WriteBatch::operator=(WriteBatch&&): 移动赋值运算符
//
// 实现细节:
//   - wal_term_point_: SavePoint 结构体，包含 size、count、content_flags
//   - content_flags_: std::atomic<uint32_t>，需要原子加载
//   - save_points_: std::unique_ptr<SavePoints>，包含 std::stack<SavePoint>
//   - prot_info_: std::unique_ptr<ProtectionInfo>，包含 entries_ 数组
//   - rep_: std::string，存储序列化数据
// ============================================================================
WriteBatch::WriteBatch(const WriteBatch& src)
    : wal_term_point_(src.wal_term_point_),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      max_bytes_(src.max_bytes_),
      default_cf_ts_sz_(src.default_cf_ts_sz_),
      rep_(src.rep_) {
  if (src.save_points_ != nullptr) {
    save_points_.reset(new SavePoints());
    save_points_->stack = src.save_points_->stack;
  }
  if (src.prot_info_ != nullptr) {
    prot_info_.reset(new WriteBatch::ProtectionInfo());
    prot_info_->entries_ = src.prot_info_->entries_;
  }
}

// ============================================================================
// WriteBatch::WriteBatch(WriteBatch&& src) noexcept
//
// 功能描述:
//   移动构造函数：从另一个 WriteBatch 对象转移所有权。
//   零拷贝操作，直接转移源对象的所有资源到新对象。
//   这是创建 WriteBatch 副本的最高效方式。
//
// 参数说明:
//   src (WriteBatch&&):
//     - 源 WriteBatch 对象（右值引用）
//     - 移动后，src 对象处于有效但未指定的状态
//     - 不应再使用 src（可以安全地重新赋值或析构）
//
// 初始化行为（按成员初始化顺序）:
//
//   1. save_points_ = std::move(src.save_points_):
//      - 转移保存点栈的所有权
//      - 直接转移 std::unique_ptr 指针（O(1) 操作）
//      - src.save_points_ 被置为 nullptr
//      - 新对象获得所有保存点历史
//
//   2. wal_term_point_ = std::move(src.wal_term_point_):
//      - 转移 WAL 终止点（SavePoint 结构体）
//      - SavePoint 是简单的结构体（POD 类型）
//      - 按字节复制（实际上等价于普通赋值）
//      - src.wal_term_point_ 处于未定义状态（但可以重新赋值）
//
//   3. content_flags_ = src.content_flags_.load(std::memory_order_relaxed):
//      - 原子加载源对象的标志位
//      - 使用 relaxed 内存序（保证原子性，但不保证顺序）
//      - 新对象继承 src 的标志位状态
//      - 如果 src 是 DEFERRED，新对象也是 DEFERRED
//
//   4. max_bytes_ = src.max_bytes_:
//      - 复制最大字节限制（简单赋值）
//      - 新对象具有相同的大小限制
//
//   5. prot_info_ = std::move(src.prot_info_):
//      - 转移保护信息对象的所有权
//      - 直接转移 std::unique_ptr 指针（O(1) 操作）
//      - src.prot_info_ 被置为 nullptr
//      - 新对象获得所有校验和信息
//
//   6. default_cf_ts_sz_ = src.default_cf_ts_sz_:
//      - 复制默认列族的时间戳大小（简单赋值）
//      - 新对象具有相同的时间戳配置
//
//   7. rep_ = std::move(src.rep_):
//      - 转移序列化数据缓冲区的所有权
//      - 直接转移 std::string 的内部指针（O(1) 操作）
//      - src.rep_ 被清空（或处于未定义状态）
//      - 新对象获得所有操作数据（无需复制）
//
// noexcept 规范:
//   - 函数声明为 noexcept，保证不会抛出异常
//   - 因为只转移指针，不分配内存或复制数据
//   - 允许编译器优化（如容器的重新分配）
//   - 符合标准库容器的移动语义要求
//
// 使用示例:
//
//   1. 从临时对象创建:
//   WriteBatch CreateBatch() {
//     WriteBatch batch;
//     batch.Put("key1", "value1");
//     return batch;  // 返回值优化（RVO/NRVO）
//   }
//   WriteBatch batch = CreateBatch();  // 可能使用移动构造
//
//   2. 显式移动:
//   WriteBatch src;
//   src.Put("key1", "value1");
//   WriteBatch dest(std::move(src));  // 显式移动
//   // src 现在不应再使用
//
//   3. 容器操作（高效）:
//   std::vector<WriteBatch> batches;
//   batches.emplace_back(std::move(temp_batch));  // 就地构造（零拷贝）
//
//   4. 交换对象:
//   WriteBatch a, b;
//   a.Put("key1", "value1");
//   b.Put("key2", "value2");
//   WriteBatch temp(std::move(a));  // 暂存 a
//   a = std::move(b);               // a 获得 b 的内容
//   b = std::move(temp);            // b 获得 a 的原始内容
//
// 性能考虑:
//   - 移动构造是 O(1) 操作（只转移指针）
//   - 不分配内存，不复制数据
//   - 比拷贝构造快得多（尤其是大数据量的批次）
//   - 适合大规模使用（如容器的重新分配）
//
// 线程安全性:
//   - 构造函数本身是线程安全的（单次操作）
//   - 但在移动期间，src 不应被其他线程访问
//   - 使用 std::memory_order_relaxed 读取 content_flags_
//   - 移动后，src 对象不应再被任何线程使用
//
// 注意事项:
//   1. 移动后源对象的状态:
//      - src.save_points_ 被置为 nullptr
//      - src.prot_info_ 被置为 nullptr
//      - src.rep_ 被清空（size = 0）
//      - 其他成员（wal_term_point_、max_bytes_ 等）可能保持原值
//      - src 处于有效但未指定的状态
//      - 可以安全地重新赋值或析构
//
//   2. noexcept 的意义:
//      - 保证不会抛出异常
//      - 允许编译器优化（如容器的强异常保证）
//      - 标准库容器要求移动构造是 noexcept 的
//
//   3. 与拷贝构造的选择:
//      - 如果不需要保留 src，优先使用移动构造
//      - 如果需要保留 src，使用拷贝构造 WriteBatch(const WriteBatch&)
//      - 现代编译器会自动使用移动语义（如返回值优化）
//
//   4. 智能指针的移动语义:
//      - std::unique_ptr 只能移动，不能复制
//      - std::move 后，源指针变为 nullptr
//      - 新指针获得对象的所有权
//
//   5. std::string 的移动语义:
//      - 如果 src.rep_ 使用短字符串优化（SSO），可能复制数据
//      - 否则，直接转移内部缓冲区指针
//      - 大多数情况下，移动是 O(1) 操作
//
//   6. content_flags_ 的原子加载:
//      - 需要使用 load() 读取原子变量
//      - relaxed 内存序是合适的（只读取值，不涉及同步）
//      - 不需要 acquire/release 语义（因为 src 不再被使用）
//
// 相关函数:
//   - WriteBatch::operator=(WriteBatch&&): 移动赋值运算符
//   - WriteBatch(const WriteBatch&): 拷贝构造函数（深拷贝）
//   - WriteBatch::operator=(const WriteBatch&): 拷贝赋值运算符
//
// 实现细节:
//   - 列表初始化顺序必须与成员声明顺序一致
//   - 所有成员都使用 std::move 转移（除了普通类型的赋值）
//   - noexcept 规范：不抛出异常（不分配内存）
//   - 原子加载使用 relaxed 内存序（不涉及同步）
//
// 编译器优化:
//   - RVO (Return Value Optimization): 可能省略移动构造
//   - NRVO (Named RVO): 可能省略移动构造
//   - 容器的emplace_back(): 可以直接构造，避免移动
//   - 移动构造本身已经非常高效（O(1)）
// ============================================================================
WriteBatch::WriteBatch(WriteBatch&& src) noexcept
    : save_points_(std::move(src.save_points_)),
      wal_term_point_(std::move(src.wal_term_point_)),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      max_bytes_(src.max_bytes_),
      prot_info_(std::move(src.prot_info_)),
      default_cf_ts_sz_(src.default_cf_ts_sz_),
      rep_(std::move(src.rep_)) {}

WriteBatch& WriteBatch::operator=(const WriteBatch& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(src);
  }
  return *this;
}

WriteBatch& WriteBatch::operator=(WriteBatch&& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(std::move(src));
  }
  return *this;
}

WriteBatch::~WriteBatch() {}

WriteBatch::Handler::~Handler() {}

void WriteBatch::Handler::LogData(const Slice& /*blob*/) {
  // If the user has not specified something to do with blobs, then we ignore
  // them.
}

bool WriteBatch::Handler::Continue() { return true; }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(WriteBatchInternal::kHeader);

  content_flags_.store(0, std::memory_order_relaxed);

  if (save_points_ != nullptr) {
    while (!save_points_->stack.empty()) {
      save_points_->stack.pop();
    }
  }

  if (prot_info_ != nullptr) {
    prot_info_->entries_.clear();
  }
  wal_term_point_.clear();
  default_cf_ts_sz_ = 0;
}

uint32_t WriteBatch::Count() const { return WriteBatchInternal::Count(this); }

uint32_t WriteBatch::ComputeContentFlags() const {
  auto rv = content_flags_.load(std::memory_order_relaxed);
  if ((rv & ContentFlags::DEFERRED) != 0) {
    BatchContentClassifier classifier;
    // Should we handle status here?
    Iterate(&classifier).PermitUncheckedError();
    rv = classifier.content_flags;

    // this method is conceptually const, because it is performing a lazy
    // computation that doesn't affect the abstract state of the batch.
    // content_flags_ is marked mutable so that we can perform the
    // following assignment
    content_flags_.store(rv, std::memory_order_relaxed);
  }
  return rv;
}

void WriteBatch::MarkWalTerminationPoint() {
  wal_term_point_.size = GetDataSize();
  wal_term_point_.count = Count();
  wal_term_point_.content_flags = content_flags_;
}

size_t WriteBatch::GetProtectionBytesPerKey() const {
  if (prot_info_ != nullptr) {
    return prot_info_->GetBytesPerKey();
  }
  return 0;
}

std::string WriteBatch::Release() {
  std::string ret = std::move(rep_);
  Clear();
  return ret;
}

bool WriteBatch::HasPut() const {
  return (ComputeContentFlags() & ContentFlags::HAS_PUT) != 0;
}

bool WriteBatch::HasPutEntity() const {
  return (ComputeContentFlags() & ContentFlags::HAS_PUT_ENTITY) != 0;
}

bool WriteBatch::HasDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE) != 0;
}

bool WriteBatch::HasSingleDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_SINGLE_DELETE) != 0;
}

bool WriteBatch::HasDeleteRange() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE_RANGE) != 0;
}

bool WriteBatch::HasMerge() const {
  return (ComputeContentFlags() & ContentFlags::HAS_MERGE) != 0;
}

bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record) {
  assert(input != nullptr && key != nullptr);
  // Skip tag byte
  input->remove_prefix(1);

  if (cf_record) {
    // Skip column_family bytes
    uint32_t cf;
    if (!GetVarint32(input, &cf)) {
      return false;
    }
  }

  // Extract key
  return GetLengthPrefixedSlice(input, key);
}

bool WriteBatch::HasBeginPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_BEGIN_PREPARE) != 0;
}

bool WriteBatch::HasEndPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_END_PREPARE) != 0;
}

bool WriteBatch::HasCommit() const {
  return (ComputeContentFlags() & ContentFlags::HAS_COMMIT) != 0;
}

bool WriteBatch::HasRollback() const {
  return (ComputeContentFlags() & ContentFlags::HAS_ROLLBACK) != 0;
}

/**
 * ReadRecordFromWriteBatch - 从 WriteBatch 中读取并解析一条操作记录
 *
 * 功能概述:
 *   - 从序列化的 WriteBatch 数据中解析出单条操作记录
 *   - 支持所有操作类型：Put、Delete、Merge、DeleteRange、BlobIndex 等
 *   - 支持列族（Column Family）和多列宽列实体（WideColumnEntity）
 *   - 支持两阶段提交（2PC）事务操作：Prepare、Commit、Rollback
 *   - 解析结果通过输出参数返回给调用者
 *
 * WriteBatch 记录格式:
 *   每条记录的序列化格式如下（以 Put 为例）：
 *
 *   ┌─────────────────────────────────────────────────────────────────┐
 *   │ 记录结构 (字节流)                                                  │
 *   ├─────────────────────────────────────────────────────────────────┤
 *   │ [tag] [cf?] [key_len] [key] [value_len] [value]                  │
 *   └─────────────────────────────────────────────────────────────────┘
 *
 *   字段说明:
 *   - tag (1字节): 操作类型标识符，如 kTypeValue=0x0A
 *   - cf? (变长): 列族ID（仅当 tag 包含 ColumnFamily 时存在）
 *   - key_len (Varint32): key 的长度
 *   - key (变长): 实际的 key 数据
 *   - value_len (Varint32): value 的长度
 *   - value (变长): 实际的 value 数据
 *
 *   Varint32 编码示例:
 *   数字 123 的编码：0x7B (0111 1011) - 最高位为 0，单字节
 *   数字 300 的编码：0xAC 0x02 (1010 1100 0000 0010) - 多字节
 *
 * 参数说明:
 *   @param input: 输入参数，指向要解析的数据流（Slice 指针）。
 *                注意：此函数会修改 *input，将已解析的数据从流中移除
 *   @param tag: 输出参数，返回操作类型标识符（单字节）
 *                可能的值：kTypeValue, kTypeDeletion, kTypeMerge, 等
 *   @param column_family: 输出参数，返回列族 ID（默认为 0）
 *   @param key: 输出参数，返回 key 的 Slice 视图
 *               注意：key/value/blob/xid 指向 input 的原始数据，零拷贝
 *   @param value: 输出参数，返回 value 的 Slice 视图
 *                 对于 DeleteRange，value 是 end_key
 *   @param blob: 输出参数，仅用于 kTypeLogData 操作
 *                返回日志数据的 Slice 视图
 *   @param xid: 输出参数，仅用于事务操作
 *               返回事务 XID（Transaction ID）的 Slice 视图
 *
 * 返回值:
 *   - Status::OK(): 成功解析记录
 *   - Status::Corruption(): 数据损坏或格式错误
 *
 * 支持的操作类型（tag）:
 *
 *   1. 写入操作（Write Operations）:
 *      - kTypeValue (0x0A): Put [默认列族]
 *      - kTypeColumnFamilyValue (0x1A): Put [指定列族]
 *      - kTypeMerge (0x0E): Merge [默认列族]
 *      - kTypeColumnFamilyMerge (0x2D): Merge [指定列族]
 *      - kTypeBlobIndex: Blob索引操作
 *      - kTypeColumnFamilyBlobIndex: Blob索引操作 [指定列族]
 *
 *   2. 删除操作（Delete Operations）:
 *      - kTypeDeletion (0x0C): Delete [默认列族]
 *      - kTypeColumnFamilyDeletion (0x1F): Delete [指定列族]
 *      - kTypeSingleDeletion (0x0F): SingleDelete [默认列族]
 *      - kTypeColumnFamilySingleDeletion (0x2C): SingleDelete [指定列族]
 *      - kTypeRangeDeletion (0x42): DeleteRange [默认列族]
 *      - kTypeColumnFamilyRangeDeletion (0x56): DeleteRange [指定列族]
 *
 *   3. 宽列实体（Wide Column Entity）:
 *      - kTypeWideColumnEntity (0x2E): 宽列实体 [默认列族]
 *      - kTypeColumnFamilyWideColumnEntity (0x2F): 宽列实体 [指定列族]
 *
 *   4. 事务操作（Transaction Operations - 2PC）:
 *      - kTypeBeginPrepareXID (0xB): Begin Prepare（启动两阶段提交）
 *      - kTypeBeginPersistedPrepareXID (0x20): Begin Persisted Prepare（持久化Prepare）
 *      - kTypeBeginUnprepareXID (0x21): Begin Unprepare（WriteUnprepared事务）
 *      - kTypeEndPrepareXID (0xC): End Prepare（结束Prepare阶段）
 *      - kTypeCommitXID (0xD): Commit（提交事务）
 *      - kTypeCommitXIDAndTimestamp (0x12): Commit with Timestamp（带时间戳提交）
 *      - kTypeRollbackXID (0xE): Rollback（回滚事务）
 *
 *   5. 其他操作（Other Operations）:
 *      - kTypeLogData: Log Data（日志数据）
 *      - kTypeNoop (0x0): No Operation（空操作）
 *
 * 解析示例（Put 操作）:
 *
 *   输入数据流（十六进制）:
 *   1A 01 03 6B6579 05 76616C7565
 *
 *   解析过程:
 *   1. 读取 tag: 0x1A (kTypeColumnFamilyValue) -> *tag = 0x1A
 *   2. 读取列族ID: Varint32(0x01) -> *column_family = 1
 *   3. 读取 key: Varint32(0x03) -> key_len = 3
 *                0x6B6579 -> *key = "key"
 *   4. 读取 value: Varint32(0x05) -> value_len = 5
 *                 0x76616C7565 -> *value = "value"
 *
 *   解析示例（Delete 操作）:
 *
 *   输入数据流（十六进制）:
 *   0C 03 6B6579
 *
 *   解析过程:
 *   1. 读取 tag: 0x0C (kTypeDeletion) -> *tag = 0x0C
 *   2. 列族ID: 无（默认列族） -> *column_family = 0
 *   3. 读取 key: Varint32(0x03) -> key_len = 3
 *                0x6B6579 -> *key = "key"
 *   4. value: 空（Delete 操作无 value）
 *
 * 使用场景:
 *
 *   1. WriteBatch 迭代器（Iterate）内部使用:
 *      - WriteBatch::Iterate() 在遍历每条记录时调用此函数
 *      - 根据解析结果调用 Handler 的对应方法（Put/Delete/Merge 等）
 *
 *   2. WAL 日志重放:
 *      - 从 WAL 中读取 WriteBatch 数据
 *      - 解析每条记录并应用到 MemTable
 *
 *   3. 数据恢复和复制:
 *      - 主从复制时解析 WriteBatch 记录
 *      - 备份恢复时重放操作
 *
 *   4. 调试和监控:
 *      - 解析 WriteBatch 内容用于日志输出
 *      - 性能监控统计各类操作数量
 *
 * 错误处理:
 *
 *   1. 数据损坏（Corruption）:
 *      - Varint32 解析失败（如溢出、不完整）
 *      - 长度前缀解析失败
 *      - 未知 tag 值
 *      - 返回 Status::Corruption 并附带错误信息
 *
 *   2. 参数校验:
 *      - key 和 value 必须非空（assert）
 *      - blob 用于 kTypeLogData 时必须非空
 *      - xid 用于事务操作时必须非空
 *
 *   3. 输入流修改:
 *      - 函数会修改 *input，跳过已解析的数据
 *      - 调用者需在循环中多次调用此函数以遍历所有记录
 *
 * 性能优化:
 *
 *   1. 零拷贝（Zero-Copy）:
 *      - key/value/blob/xid 返回 Slice 视图
 *      - 指向 input 原始数据，无需复制
 *      - 注意：input 被修改后，Slice 可失效
 *
 *   2. 快速路径（Fast Path）:
 *      - 默认列族操作跳过 Varint32 解析（column_family = 0）
 *      - 使用 switch-case 优化分支预测
 *
 *   3. 编码效率:
 *      - Varint32 变长编码减少存储空间
 *      - 小整数用单字节表示，大整数用多字节
 *
 * 线程安全性:
 *   - 此函数是线程安全的（仅读取和修改输入参数）
 *   - 但输入 Slice 指向的数据可能被其他线程修改
 *   - 调用者需确保 input 指向的数据在函数调用期间不被修改
 *
 * 注意事项:
 *   1. input 参数会被修改: 解析后，input 指针会移动到下一条记录
 *   2. Slice 生命周期: 返回的 Slice 指向 input 原始数据，input 修改后可能失效
 *   3. 默认列族: 无列族 ID 的记录，column_family 返回 0
 *   4. 事务操作: XID 相关的 tag（Begin/EndPrepare/Commit/Rollback）需要正确处理
 *   5. 宽列实体: kTypeWideColumnEntity 将多个列编码到 value 字段
 *
 * 相关函数:
 *   - WriteBatch::Iterate(): 遍历 WriteBatch 所有记录
 *   - GetVarint32(): 解析变长整数
 *   - GetLengthPrefixedSlice(): 解析长度前缀的 Slice
 *   - WriteBatchInternal::Put(): 写入 Put 操作（反向操作）
 *
 * 示例代码（遍历 WriteBatch）:
 *
 *   Slice input = WriteBatchInternal::Contents(batch);
 *   while (input.size() > 0) {
 *     char tag;
 *     uint32_t column_family;
 *     Slice key, value, blob, xid;
 *
 *     Status s = ReadRecordFromWriteBatch(&input, &tag, &column_family,
 *                                          &key, &value, &blob, &xid);
 *     if (!s.ok()) {
 *       LOG(ERROR) << "Failed to read record: " << s.ToString();
 *       break;
 *     }
 *
 *     switch (tag) {
 *       case kTypeValue:
 *         printf("Put(key=%s, value=%s)\n",
 *                key.ToString().c_str(), value.ToString().c_str());
 *         break;
 *       case kTypeDeletion:
 *         printf("Delete(key=%s)\n", key.ToString().c_str());
 *         break;
 *       case kTypeMerge:
 *         printf("Merge(key=%s, value=%s)\n",
 *                key.ToString().c_str(), value.ToString().c_str());
 *         break;
 *       // ... 其他操作类型
 *     }
 *   }
 *
 * 历史版本兼容性:
 *   - 新增 tag（如 kTypeWideColumnEntity）需要兼容旧版本
 *   - 未知 tag 返回 Status::Corruption
 *   - Varint32 编码向后兼容（旧版本可解析新版本数据）
 *
 * 调试建议:
 *   1. 使用 LOG(VERBOSE) 输出解析过程的详细信息
 *   2. 在出现 Corruption 时打印 input 的原始数据（十六进制）
 *   3. 统计各类操作的解析次数，帮助性能分析
 *   4. 使用单元测试覆盖所有 tag 类型
 */
Status ReadRecordFromWriteBatch(Slice* input, char* tag,
                                uint32_t* column_family, Slice* key,
                                Slice* value, Slice* blob, Slice* xid) {
  // 参数校验：key 和 value 必须非空
  assert(key != nullptr && value != nullptr);

  // 步骤1: 读取操作类型标识符（tag）
  // tag 是单字节，位于记录的开头
  // 例如: 0x0A = kTypeValue, 0x1A = kTypeColumnFamilyValue
  *tag = (*input)[0];

  // 移除 tag 字节，更新 input 指针
  // 这会使 input 指向记录的下一个字段
  input->remove_prefix(1);

  // 步骤2: 初始化列族 ID 为默认值（0）
  // 对于默认列族操作（如 kTypeValue），column_family 保持为 0
  // 对于指定列族操作（如 kTypeColumnFamilyValue），后续会读取实际的列族 ID
  *column_family = 0;  // default

  // 步骤3: 根据 tag 类型解析记录内容
  // 使用 switch-case 处理不同的操作类型
  // FALLTHROUGH_INTENDED 宏表示故意 fallthrough 到下一个 case
  switch (*tag) {
    // ====== Put 操作 ======
    // kTypeColumnFamilyValue: Put 操作，指定列族
    // 格式: [tag] [column_family_varint] [key_len_varint] [key] [value_len_varint] [value]
    case kTypeColumnFamilyValue:
      // 解析列族 ID（Varint32 编码）
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Put");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeValue，继续解析 key 和 value

    // kTypeValue: Put 操作，默认列族（column_family = 0）
    // 格式: [tag] [key_len_varint] [key] [value_len_varint] [value]
    case kTypeValue:
      // 解析 key: [key_len] [key]
      if (!GetLengthPrefixedSlice(input, key) ||
          // 解析 value: [value_len] [value]
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Put");
      }
      break;

    // ====== Delete 操作 ======
    // kTypeColumnFamilyDeletion: Delete 操作，指定列族
    // kTypeColumnFamilySingleDeletion: SingleDelete 操作，指定列族
    // 格式: [tag] [column_family_varint] [key_len_varint] [key]
    case kTypeColumnFamilyDeletion:
    case kTypeColumnFamilySingleDeletion:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeDeletion，继续解析 key

    // kTypeDeletion: Delete 操作，默认列族
    // kTypeSingleDeletion: SingleDelete 操作，默认列族
    // 格式: [tag] [key_len_varint] [key]
    case kTypeDeletion:
    case kTypeSingleDeletion:
      if (!GetLengthPrefixedSlice(input, key)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
      break;

    // ====== DeleteRange 操作 ======
    // kTypeColumnFamilyRangeDeletion: DeleteRange 操作，指定列族
    // 格式: [tag] [column_family_varint] [begin_key_len] [begin_key] [end_key_len] [end_key]
    case kTypeColumnFamilyRangeDeletion:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch DeleteRange");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeRangeDeletion，继续解析 key 范围

    // kTypeRangeDeletion: DeleteRange 操作，默认列族
    // 格式: [tag] [begin_key_len] [begin_key] [end_key_len] [end_key]
    // 注意：对于 range delete，"key" 参数返回 begin_key，"value" 参数返回 end_key
    case kTypeRangeDeletion:
      // for range delete, "key" is begin_key, "value" is end_key
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch DeleteRange");
      }
      break;

    // ====== Merge 操作 ======
    // kTypeColumnFamilyMerge: Merge 操作，指定列族
    // 格式: [tag] [column_family_varint] [key_len_varint] [key] [value_len_varint] [value]
    case kTypeColumnFamilyMerge:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Merge");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeMerge，继续解析 key 和 value

    // kTypeMerge: Merge 操作，默认列族
    // 格式: [tag] [key_len_varint] [key] [value_len_varint] [value]
    case kTypeMerge:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Merge");
      }
      break;

    // ====== BlobIndex 操作 ======
    // kTypeColumnFamilyBlobIndex: BlobIndex 操作，指定列族
    // 格式: [tag] [column_family_varint] [key_len_varint] [key] [value_len_varint] [value]
    case kTypeColumnFamilyBlobIndex:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch BlobIndex");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeBlobIndex，继续解析 key 和 value

    // kTypeBlobIndex: BlobIndex 操作，默认列族
    // 格式: [tag] [key_len_varint] [key] [value_len_varint] [value]
    case kTypeBlobIndex:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch BlobIndex");
      }
      break;
    // ====== LogData 操作 ======
    // kTypeLogData: 日志数据（用于调试、监控等）
    // 格式: [tag] [data_len_varint] [data]
    case kTypeLogData:
      // 参数校验：blob 参数必须非空（用于返回日志数据）
      assert(blob != nullptr);
      if (!GetLengthPrefixedSlice(input, blob)) {
        return Status::Corruption("bad WriteBatch Blob");
      }
      break;

    // ====== 事务标记操作（无额外数据） ======
    // kTypeNoop: 空操作（用于占位、调试等）
    // 格式: [tag]
    case kTypeNoop:

    // kTypeBeginPrepareXID: 开始 Prepare 阶段（两阶段提交）
    // 这表示 prepared batch 也持久化到数据库中
    // 用于 WritePreparedTxn（写预准备事务）
    // 格式: [tag]
    case kTypeBeginPrepareXID:
      // This indicates that the prepared batch is also persisted in the db.
      // This is used in WritePreparedTxn

    // kTypeBeginPersistedPrepareXID: 开始持久化 Prepare 阶段
    // 用于 WriteUnpreparedTxn（写未准备事务）
    // 格式: [tag]
    case kTypeBeginPersistedPrepareXID:
      // This is used in WriteUnpreparedTxn

    // kTypeBeginUnprepareXID: 开始 Unprepare 阶段
    // 用于 WriteUnpreparedTxn
    // 格式: [tag]
    case kTypeBeginUnprepareXID:
      break;  // 这些操作只有 tag，无额外数据

    // ====== EndPrepare 操作 ======
    // kTypeEndPrepareXID: 结束 Prepare 阶段（两阶段提交）
    // 格式: [tag] [xid_len_varint] [xid]
    case kTypeEndPrepareXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad EndPrepare XID");
      }
      break;

    // ====== Commit 操作 ======
    // kTypeCommitXIDAndTimestamp: 提交操作，带时间戳
    // 格式: [tag] [timestamp_len_varint] [timestamp] [xid_len_varint] [xid]
    case kTypeCommitXIDAndTimestamp:
      // 解析时间戳（存储在 key 参数中）
      if (!GetLengthPrefixedSlice(input, key)) {
        return Status::Corruption("bad commit timestamp");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeCommitXID，继续解析 xid

    // kTypeCommitXID: 提交操作（两阶段提交）
    // 格式: [tag] [xid_len_varint] [xid]
    case kTypeCommitXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Commit XID");
      }
      break;

    // ====== Rollback 操作 ======
    // kTypeRollbackXID: 回滚操作（两阶段提交）
    // 格式: [tag] [xid_len_varint] [xid]
    case kTypeRollbackXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Rollback XID");
      }
      break;

    // ====== WideColumnEntity 操作（宽列实体） ======
    // kTypeColumnFamilyWideColumnEntity: 宽列实体，指定列族
    // 格式: [tag] [column_family_varint] [key_len_varint] [key] [entity_len_varint] [entity]
    // entity 是序列化的宽列数据，包含多个列的 name-value 对
    case kTypeColumnFamilyWideColumnEntity:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch PutEntity");
      }
      FALLTHROUGH_INTENDED;  // fallthrough 到 kTypeWideColumnEntity，继续解析 key 和 entity

    // kTypeWideColumnEntity: 宽列实体，默认列族
    // 格式: [tag] [key_len_varint] [key] [entity_len_varint] [entity]
    // 注意：宽列实体的实体数据存储在 value 参数中
    case kTypeWideColumnEntity:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch PutEntity");
      }
      break;

    // ====== 未知操作类型 ======
    // 当遇到未知的 tag 时，返回错误
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }

  // 成功解析记录，返回 OK
  return Status::OK();
}

Status WriteBatch::Iterate(Handler* handler) const {
  if (rep_.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  return WriteBatchInternal::Iterate(this, handler, WriteBatchInternal::kHeader,
                                     rep_.size());
}

/**
 * WriteBatchInternal::Iterate - 遍历 WriteBatch 并通过 Handler 处理每条记录
 *
 * 功能概述:
 *   - 解析 WriteBatch 的序列化格式，逐条读取操作记录
 *   - 根据操作类型（Put、Delete、Merge 等）调用对应的 Handler 方法
 *   - 支持部分遍历（通过 begin/end 指定范围）
 *   - 支持事务操作（Prepare、Commit、Rollback）
 *   - 处理 TryAgain 机制，允许 Handler 重试特定操作
 *
 * 参数说明:
 *   @param wb: 要遍历的 WriteBatch 对象，包含序列化的操作记录
 *   @param handler: 处理每条记录的回调对象，定义了 Put/Delete/Merge 等操作的处理逻辑
 *   @param begin: 遍历起始位置（字节偏移），通常为 kHeader (12)
 *   @param end: 遍历结束位置（字节偏移），通常为 wb->rep_.size()
 *
 * WriteBatch 格式:
 *   Header (12字节):
 *     - 序列号 (8字节): 批次起始序列号
 *     - 记录数 (4字节): 批次中的操作记录数量
 *   Body: 多条记录，每条格式为:
 *     - tag (1字节): 操作类型枚举值
 *     - column_family_id (变长，可选): 列族ID
 *     - key (变长前缀): 键
 *     - value/blob/xid (变长前缀，可选): 值、blob数据或事务ID
 *
 * 执行流程:
 *   1. 参数校验：验证 begin/end 范围的有效性
 *   2. 初始化遍历状态：设置 whole_batch 标志、空批次标记等
 *   3. 主循环遍历记录：
 *      a) 检查 Handler 是否继续（Continue()）
 *      b) 处理 TryAgain 状态（Handler 返回 TryAgain 时重试）
 *      c) 解析下一条记录（ReadRecordFromWriteBatch）
 *      d) 根据 tag 分发到对应的 Handler 方法
 *   4. 完整性校验：如果遍历完整批次，验证实际记录数是否匹配
 *   5. 返回状态
 *
 * 设计要点:
 *   - TryAgain 机制: 允许 Handler 延迟处理某些记录（如等待锁、等待事务状态）
 *     - Handler 返回 Status::TryAgain 时，下次循环不读取新记录，而是重试当前记录
 *     - 通过 last_was_try_again 标志检测连续 TryAgain，防止无限循环
 *   - empty_batch 标记: 跟踪当前子批次是否包含有效操作
 *     - 用于正确识别批次边界，避免 Noop 被误认为是新批次的开始
 *     - Noop 前如果 empty_batch 为 true，说明这是批次间的分隔符
 *   - content_flags 校验: 确保批次内容与声明的内容标志一致
 *     - 每种操作类型都有对应的 content flag（HAS_PUT, HAS_DELETE 等）
 *   - whole_batch 校验: 遍历完整批次时，验证记录数是否正确
 *
 * 支持的操作类型 (tag):
 *   - kTypeValue/kTypeColumnFamilyValue: Put 操作（插入/更新）
 *   - kTypeDeletion/kTypeColumnFamilyDeletion: Delete 操作（删除）
 *   - kTypeSingleDeletion/kTypeColumnFamilySingleDeletion: SingleDelete 操作
 *   - kTypeMerge/kTypeColumnFamilyMerge: Merge 操作（合并）
 *   - kTypeRangeDeletion/kTypeColumnFamilyRangeDeletion: RangeDelete 操作（范围删除）
 *   - kTypeLogData: LogData 操作（仅记录到WAL）
 *   - kTypeBeginPrepareXID/kTypeBeginPersistedPrepareXID/kTypeBeginUnprepareXID: 事务开始
 *   - kTypeEndPrepareXID: 事务准备结束
 *   - kTypeCommitXID/kTypeCommitXIDAndTimestamp: 事务提交
 *   - kTypeRollbackXID: 事务回滚
 *   - kTypeNoop: 空操作（用于批次分隔）
 *   - kTypeBlobIndex/kTypeColumnFamilyBlobIndex: Blob DB 索引
 *   - kTypeWideColumnEntity/kTypeColumnFamilyWideColumnEntity: 宽列实体
 *
 * 调用时机:
 *   - WriteBatch::Iterate: 公共接口，遍历整个批次
 *   - MemTableInserter: 将 WriteBatch 插入 MemTable
 *   - WriteBatchWithIndex::Iterate: 遍历带索引的 WriteBatch
 *   - WAL 恢复: 从 WAL 重放 WriteBatch 操作
 *   - 事务处理: 处理事务相关的标记记录
 *   - 调试和诊断: 遍历批次内容进行验证
 *
 * 相关数据结构:
 *   - WriteBatch::Handler: 记录处理回调接口
 *   - Slice: 用于读取序列化数据的视图
 *   - content_flags_: 批次内容标志位
 *
 * 异常处理:
 *   - 返回 Status::Corruption: 数据格式错误、记录数不匹配、连续 TryAgain
 *   - 返回 Status::NotSupported: 事务策略不兼容
 *   - 传播 Handler 返回的错误状态
 */
Status WriteBatchInternal::Iterate(const WriteBatch* wb,
                                   WriteBatch::Handler* handler, size_t begin,
                                   size_t end) {
  // 参数校验：检查 begin/end 范围是否有效
  if (begin > wb->rep_.size() || end > wb->rep_.size() || end < begin) {
    return Status::Corruption("Invalid start/end bounds for Iterate");
  }
  assert(begin <= end);

  // 创建输入 Slice，指向指定范围的序列化数据
  Slice input(wb->rep_.data() + begin, static_cast<size_t>(end - begin));

  // 判断是否遍历完整批次（从 header 开始到末尾）
  bool whole_batch =
      (begin == WriteBatchInternal::kHeader) && (end == wb->rep_.size());

  // 初始化各记录的输出变量
  Slice key, value, blob, xid;

  // 有时子批次以 Noop 开头。我们需要排除这些 Noop 作为批次边界符号，
  // 否则会错误计算批次数量。通过检查在遇到下一个 Noop 之前累积的批次是否为空来实现。
  bool empty_batch = true;
  uint32_t found = 0;      // 已找到的记录数
  Status s;                // 当前状态
  char tag = 0;            // 当前记录类型标签
  uint32_t column_family = 0;  // 当前列族ID（0表示默认列族）
  bool last_was_try_again = false;  // 上次循环是否为 TryAgain 状态
  bool handler_continue = true;      // Handler 是否要求继续遍历

  // 主循环：持续遍历直到输入耗尽或出错
  // 条件: (状态OK且输入非空) 或 (状态为TryAgain)
  while (((s.ok() && !input.empty()) || UNLIKELY(s.IsTryAgain()))) {
    // 检查 Handler 是否要求继续遍历
    handler_continue = handler->Continue();
    if (!handler_continue) {
      break;
    }

    // 处理 TryAgain 状态（Handler 返回 TryAgain 时的重试逻辑）
    if (LIKELY(!s.IsTryAgain())) {
      // 正常情况：重置状态，准备读取下一条记录
      last_was_try_again = false;
      tag = 0;
      column_family = 0;  // default

      // 从 WriteBatch 中读取下一条记录
      s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                   &blob, &xid);
      if (!s.ok()) {
        return s;
      }
    } else {
      // TryAgain 情况：不读取新记录，保持当前记录状态以便重试
      assert(s.IsTryAgain());
      // 检测无限循环：不应该连续两次 TryAgain
      assert(!last_was_try_again);  // to detect infinite loop bugs
      if (UNLIKELY(last_was_try_again)) {
        return Status::Corruption(
            "two consecutive TryAgain in WriteBatch handler; this is either a "
            "software bug or data corruption.");
      }
      last_was_try_again = true;
      s = Status::OK();  // 重置状态以便下一次处理
    }

    // 根据 tag 类型分发到对应的 Handler 方法
    switch (tag) {
      case kTypeColumnFamilyValue:   // 带列族ID的 Put 操作
      case kTypeValue:                // 默认列族的 Put 操作
        // 校验批次内容标志是否包含 Put 操作
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_PUT));
        s = handler->PutCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;  // 标记批次非空
          found++;              // 增加记录计数
        }
        break;
      case kTypeColumnFamilyDeletion:  // 带列族ID的 Delete 操作
      case kTypeDeletion:               // 默认列族的 Delete 操作
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE));
        s = handler->DeleteCF(column_family, key);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilySingleDeletion:  // 带列族ID的 SingleDelete 操作
      case kTypeSingleDeletion:               // 默认列族的 SingleDelete 操作
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_SINGLE_DELETE));
        s = handler->SingleDeleteCF(column_family, key);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyRangeDeletion:  // 带列族ID的 RangeDelete 操作
      case kTypeRangeDeletion:               // 默认列族的 RangeDelete 操作
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE_RANGE));
        s = handler->DeleteRangeCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyMerge:  // 带列族ID的 Merge 操作
      case kTypeMerge:               // 默认列族的 Merge 操作
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_MERGE));
        s = handler->MergeCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyBlobIndex:  // 带列族ID的 BlobIndex 操作（Blob DB）
      case kTypeBlobIndex:               // 默认列族的 BlobIndex 操作
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BLOB_INDEX));
        s = handler->PutBlobIndexCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          found++;  // BlobIndex 不影响 empty_batch 标记
        }
        break;
      case kTypeLogData:  // 仅记录到 WAL 的数据（不修改 MemTable）
        handler->LogData(blob);
        // 即使只有 LogData 也是一个有效批次
        empty_batch = false;
        break;
      // ========== 事务相关标记 ==========
      // 以下三种标记用于不同的写入策略（WriteCommitted、WritePrepared、WriteUnprepared）

      // WriteCommitted 模式：事务提交时才写入（write_after_commit=true）
      case kTypeBeginPrepareXID:  // 事务准备阶段开始
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_PREPARE));
        s = handler->MarkBeginPrepare();
        assert(s.ok());
        empty_batch = false;
        // 检查事务策略兼容性
        if (handler->WriteAfterCommit() ==
            WriteBatch::Handler::OptionState::kDisabled) {
          // WriteAfterCommit 禁用时不能有 WriteCommitted 标记
          s = Status::NotSupported(
              "WriteCommitted txn tag when write_after_commit_ is disabled (in "
              "WritePrepared/WriteUnprepared mode). If it is not due to "
              "corruption, the WAL must be emptied before changing the "
              "WritePolicy.");
        }
        if (handler->WriteBeforePrepare() ==
            WriteBatch::Handler::OptionState::kEnabled) {
          // WriteBeforePrepare 启用时不能有 WriteCommitted 标记
          s = Status::NotSupported(
              "WriteCommitted txn tag when write_before_prepare_ is enabled "
              "(in WriteUnprepared mode). If it is not due to corruption, the "
              "WAL must be emptied before changing the WritePolicy.");
        }
        break;
      // WritePrepared 模式：事务准备时持久化，提交时不重写（write_after_commit=false）
      case kTypeBeginPersistedPrepareXID:  // WritePrepared 模式：准备阶段持久化
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_PREPARE));
        s = handler->MarkBeginPrepare();
        assert(s.ok());
        empty_batch = false;
        if (handler->WriteAfterCommit() ==
            WriteBatch::Handler::OptionState::kEnabled) {
          // WritePrepared/WriteUnprepared 标记不能在 WriteCommitted 模式下
          s = Status::NotSupported(
              "WritePrepared/WriteUnprepared txn tag when write_after_commit_ "
              "is enabled (in default WriteCommitted mode). If it is not due "
              "to corruption, the WAL must be emptied before changing the "
              "WritePolicy.");
        }
        break;
      // WriteUnprepared 模式：在 Prepare 之前就写入（write_before_prepare=true）
      case kTypeBeginUnprepareXID:  // WriteUnprepared 模式：未准备阶段的写入
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_UNPREPARE));
        s = handler->MarkBeginPrepare(true /* unprepared */);
        assert(s.ok());
        empty_batch = false;
        if (handler->WriteAfterCommit() ==
            WriteBatch::Handler::OptionState::kEnabled) {
          s = Status::NotSupported(
              "WriteUnprepared txn tag when write_after_commit_ is enabled (in "
              "default WriteCommitted mode). If it is not due to corruption, "
              "the WAL must be emptied before changing the WritePolicy.");
        }
        if (handler->WriteBeforePrepare() ==
            WriteBatch::Handler::OptionState::kDisabled) {
          s = Status::NotSupported(
              "WriteUnprepared txn tag when write_before_prepare_ is disabled "
              "(in WriteCommitted/WritePrepared mode). If it is not due to "
              "corruption, the WAL must be emptied before changing the "
              "WritePolicy.");
        }
        break;
      case kTypeEndPrepareXID:  // 事务准备阶段结束
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_END_PREPARE));
        s = handler->MarkEndPrepare(xid);
        assert(s.ok());
        empty_batch = true;  // 准备阶段结束，标记批次为空（等待新批次或提交/回滚）
        break;
      case kTypeCommitXID:  // 事务提交
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_COMMIT));
        s = handler->MarkCommit(xid);
        assert(s.ok());
        empty_batch = true;
        break;
      case kTypeCommitXIDAndTimestamp:  // 带时间戳的事务提交
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_COMMIT));
        // key 存储提交时间戳
        assert(!key.empty());
        s = handler->MarkCommitWithTimestamp(xid, key);
        if (LIKELY(s.ok())) {
          empty_batch = true;
        }
        break;
      case kTypeRollbackXID:  // 事务回滚
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_ROLLBACK));
        s = handler->MarkRollback(xid);
        assert(s.ok());
        empty_batch = true;
        break;
      case kTypeNoop:  // 空操作（用于分隔批次）
        s = handler->MarkNoop(empty_batch);
        assert(s.ok());
        empty_batch = true;  // Noop 后批次重置为空
        break;
      // ========== 宽列实体（Wide Column Entity）支持 ==========
      // 宽列允许一个键关联多个命名值列，类似于表结构
      case kTypeWideColumnEntity:                // 默认列族的宽列实体
      case kTypeColumnFamilyWideColumnEntity:   // 带列族ID的宽列实体
        assert(wb->content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_PUT_ENTITY));
        s = handler->PutEntityCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          ++found;
        }
        break;
      default:
        // 未知操作类型：数据损坏
        return Status::Corruption("unknown WriteBatch tag");
    }
  }

  // 检查遍历过程中是否有错误
  if (!s.ok()) {
    return s;
  }

  // 完整性校验：如果遍历了完整批次且 Handler 未中断，验证记录数是否匹配
  if (handler_continue && whole_batch &&
      found != WriteBatchInternal::Count(wb)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

bool WriteBatchInternal::IsLatestPersistentState(const WriteBatch* b) {
  return b->is_latest_persistent_state_;
}

void WriteBatchInternal::SetAsLatestPersistentState(WriteBatch* b) {
  b->is_latest_persistent_state_ = true;
}

uint32_t WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, uint32_t n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

size_t WriteBatchInternal::GetFirstOffset(WriteBatch* /*b*/) {
  return WriteBatchInternal::kHeader;
}

std::tuple<Status, uint32_t, size_t>
WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(
    WriteBatch* b, ColumnFamilyHandle* column_family) {
  uint32_t cf_id = GetColumnFamilyID(column_family);
  size_t ts_sz = 0;
  Status s;
  if (column_family) {
    const Comparator* const ucmp = column_family->GetComparator();
    if (ucmp) {
      ts_sz = ucmp->timestamp_size();
      if (0 == cf_id && b->default_cf_ts_sz_ != ts_sz) {
        s = Status::InvalidArgument("Default cf timestamp size mismatch");
      }
    }
  } else if (b->default_cf_ts_sz_ > 0) {
    ts_sz = b->default_cf_ts_sz_;
  }
  return std::make_tuple(s, cf_id, ts_sz);
}

namespace {
Status CheckColumnFamilyTimestampSize(ColumnFamilyHandle* column_family,
                                      const Slice& ts) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be null");
  }
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t cf_ts_sz = ucmp->timestamp_size();
  if (0 == cf_ts_sz) {
    return Status::InvalidArgument("timestamp disabled");
  }
  if (cf_ts_sz != ts.size()) {
    return Status::InvalidArgument("timestamp size mismatch");
  }
  return Status::OK();
}
}  // anonymous namespace

Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                               const Slice& key, const Slice& value) {
  if (key.size() > size_t{std::numeric_limits<uint32_t>::max()}) { //key长度过大
    return Status::InvalidArgument("key is too large");
  }
  if (value.size() > size_t{std::numeric_limits<uint32_t>::max()}) { //value长度过大
    return Status::InvalidArgument("value is too large");
  }

  LocalSavePoint save(b); // 用于回滚的保存点
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1); //更新计数
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue)); //没有列族时使用普通类型
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue)); // 有列族时使用cf类型
    PutVarint32(&b->rep_, column_family_id); // 写入列族id
  }
  PutLengthPrefixedSlice(&b->rep_, key); // 写入键
  PutLengthPrefixedSlice(&b->rep_, value); // 写入值
  // 更新内容标识位
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
  // 如果启用了保护信息 （如校验和）则记录
  if (b->prot_info_ != nullptr) {
    // Technically the optype could've been `kTypeColumnFamilyValue` with the
    // CF ID encoded in the `WriteBatch`. That distinction is unimportant
    // however since we verify CF ID is correct, as well as all other fields
    // (a missing/extra encoded CF ID would corrupt another field). It is
    // convenient to consolidate on `kTypeValue` here as that is what will be
    // inserted into memtable.
    b->prot_info_->entries_.emplace_back(ProtectionInfo64()
                                             .ProtectKVO(key, value, kTypeValue)
                                             .ProtectC(column_family_id));
  }
  return save.commit(); //提交保存点
}

/**
 * @brief 向 WriteBatch 中添加一个 Put 操作（插入或更新键值对）
 *
 * 本函数将一个 Put 操作序列化到 WriteBatch 的内部缓冲区中：
 * 1. 获取列族 ID 和时间戳大小
 * 2. 如果列族未启用时间戳，直接写入普通的键值对
 * 3. 如果列族启用了时间戳，创建一个空时间戳并拼接在键后面
 *
 * @param column_family 列族句柄，如果为 nullptr 则使用默认列族
 * @param key 要插入的键（Slice 引用，不复制数据）
 * @param value 要插入的值（Slice 引用，不复制数据）
 *
 * @return Status 成功返回 Status::OK()，失败返回错误状态
 *
 * @note 关于时间戳的处理：
 *   - 如果列族未启用时间戳（ts_sz == 0）：直接写入键值对
 *   - 如果列族启用了时间戳（ts_sz > 0）：
 *     * 创建一个空的默认时间戳（全 0）
 *     * 将时间戳拼接在键后面，形成 {key, timestamp} 的组合
 *     * 调用 SliceParts 版本的 Put 函数
 *     * 设置 has_key_with_ts_ = true 和 needs_in_place_update_ts_ = true
 *
 * @note 序列化格式：
 *   - 无列族（column_family == nullptr）：kTypeValue | key | value
 *   - 有列族：kTypeColumnFamilyValue | cf_id | key | value
 *   - key 和 value 都使用变长前缀编码（长度 + 数据）
 *
 * @note 内存管理：
 *   - key 和 value 的数据会被复制到 rep_ 缓冲区
 *   - 如果时间戳启用，会创建额外的 dummy_ts 字符串
 *
 * @see WriteBatch::Put(ColumnFamilyHandle*, const Slice&, const Slice&, const Slice&)
 *      带显式时间戳的版本
 * @see WriteBatch::Put(ColumnFamilyHandle*, const SliceParts&, const SliceParts&)
 *      支持 SliceParts 的版本（类似 writev）
 */
Status WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  // 获取列族 ID 和时间戳大小
  // 如果 column_family 为 nullptr，使用默认列族（cf_id = 0）
  // ts_sz 表示该列族是否启用了时间戳功能
  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family); //获取列族ID和时间戳大小

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) { //未开启列族时间戳
    // 普通模式：直接写入键值对
    // 调用 WriteBatchInternal::Put 将操作序列化到 rep_ 缓冲区
    return WriteBatchInternal::Put(this, cf_id, key, value);
  }

  // 时间戳模式：需要特殊处理
  // 标记需要原地更新时间戳
  needs_in_place_update_ts_ = true;
  // 标记批次包含带时间戳的键
  has_key_with_ts_ = true;

  // 创建一个空的时间戳（所有字节为 0）
  // 这是因为用户没有显式提供时间戳，系统使用默认值
  std::string dummy_ts(ts_sz, '\0'); // 创建空时间戳

  // 将键和时间戳拼接为一个 SliceParts 数组
  // SliceParts 类似于 writev，支持多个连续的 Slice
  std::array<Slice, 2> key_with_ts{{key, dummy_ts}}; // 将键和时间戳拼接为键

  // 调用 SliceParts 版本的 Put 函数写入键值对
  // key_with_ts = [key, dummy_ts]，value = [value]
  return WriteBatchInternal::Put(this, cf_id, SliceParts(key_with_ts.data(), 2),
                                 SliceParts(&value, 1)); // 写入键值对
}

/**
 * @brief 向 WriteBatch 中添加一个带显式时间戳的 Put 操作
 *
 * 本函数用于列族启用了时间戳功能时，用户显式指定时间戳：
 * 1. 校验时间戳大小是否与列族配置一致
 * 2. 将时间戳拼接在键后面
 * 3. 写入到 WriteBatch 内部缓冲区
 *
 * @param column_family 列族句柄（不能为 nullptr）
 * @param key 要插入的键（Slice 引用）
 * @param ts 时间戳（Slice 引用，大小必须与列族配置一致）
 * @param value 要插入的值（Slice 引用）
 *
 * @return Status 成功返回 Status::OK()，失败返回错误状态
 *
 * @note 时间戳要求：
 *   - column_family 不能为 nullptr（必须显式指定列族）
 *   - ts 的大小必须与列族的 timestamp_size 配置一致
 *   - 时间戳会被序列化在键后面：{key, timestamp, value}
 *
 * @note 序列化格式：
 *   kTypeColumnFamilyValue | cf_id | key | ts | value
 *   - key 和 ts 合并为一个 SliceParts 传入底层函数
 *   - value 作为独立的 SliceParts
 *
 * @note 错误处理：
 *   - 时间戳大小不匹配：返回 Status::InvalidArgument
 *   - 列族句柄为 nullptr：assert 失败
 *
 * @see WriteBatch::Put(ColumnFamilyHandle*, const Slice&, const Slice&)
 *      不带时间戳的版本
 * @see WriteBatchInternal::CheckColumnFamilyTimestampSize()
 *      时间戳大小校验函数
 */
Status WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& ts, const Slice& value) {
  // 校验时间戳大小是否与列族配置一致
  const Status s = CheckColumnFamilyTimestampSize(column_family, ts);
  if (!s.ok()) {
    return s;
  }

  // 标记批次包含带时间戳的键
  has_key_with_ts_ = true;

  // 断言：列族句柄不能为 nullptr
  // 因为时间戳需要明确的列族配置
  assert(column_family);

  // 获取列族 ID
  uint32_t cf_id = column_family->GetID();

  // 将键和时间戳拼接为一个 SliceParts 数组
  // 这样可以将不连续的内存块作为一个"键"处理
  std::array<Slice, 2> key_with_ts{{key, ts}};

  // 调用底层函数写入键值对
  // key_with_ts = [key, ts]，value = [value]
  return WriteBatchInternal::Put(this, cf_id, SliceParts(key_with_ts.data(), 2),
                                 SliceParts(&value, 1));
}

Status WriteBatchInternal::CheckSlicePartsLength(const SliceParts& key,
                                                 const SliceParts& value) {
  size_t total_key_bytes = 0;
  for (int i = 0; i < key.num_parts; ++i) {
    total_key_bytes += key.parts[i].size();
  }
  if (total_key_bytes >= size_t{std::numeric_limits<uint32_t>::max()}) {
    return Status::InvalidArgument("key is too large");
  }

  size_t total_value_bytes = 0;
  for (int i = 0; i < value.num_parts; ++i) {
    total_value_bytes += value.parts[i].size();
  }
  if (total_value_bytes >= size_t{std::numeric_limits<uint32_t>::max()}) {
    return Status::InvalidArgument("value is too large");
  }
  return Status::OK();
}

// ============================================================================
// WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
//                        const SliceParts& key, const SliceParts& value)
//
// 功能描述:
//   将一个 Put 操作（key-value 对）添加到 WriteBatch 中。
//   这是 WriteBatch 内部使用的底层函数，用于序列化 Put 操作到 WriteBatch 的
//   内部缓冲区（rep_）中。支持指定列族（column family）和非连续的 key/value
//   数据（通过 SliceParts）。
//
// 参数说明:
//   b (WriteBatch*):
//     - 目标 WriteBatch 对象指针，Put 操作将添加到此批次中
//     - 不能为 nullptr，调用方负责确保 b 有效
//     - b 的 rep_（内部缓冲区）将被追加数据
//
//   column_family_id (uint32_t):
//     - 列族 ID，指定要操作的列族
//     - 0 表示默认列族（default column family）
//     - 非 0 表示指定的列族 ID
//     - 列族 ID 由 DB 在创建列族时分配
//
//   key (const SliceParts&):
//     - Key 的数据，支持非连续存储（分片存储）
//     - SliceParts 是一个结构体，包含：
//       * parts: 指向 Slice 数组的指针
//       * num_parts: Slice 数组的元素个数
//     - 每个 Slice 是一个连续的内存块，多个 Slice 组合起来构成完整的 key
//     - 适用场景：key 由多个不连续的内存块组成，避免内存拷贝
//
//   value (const SliceParts&):
//     - Value 的数据，同样支持非连续存储（分片存储）
//     - 结构与 key 相同（SliceParts）
//     - 适用场景：value 由多个不连续的内存块组成，避免内存拷贝
//
// 返回值:
//   Status:
//     - Status::OK(): Put 操作成功添加到批次中
//     - Status::MemoryLimit(): 批次大小超过 max_bytes_ 限制
//     - Status::InvalidArgument(): key 或 value 的长度检查失败
//     - 其他错误: 具体的错误信息
//
// 序列化格式:
//
//   1. 默认列族（column_family_id == 0）:
//      [kTypeValue] [key长度] [key数据] [value长度] [value数据]
//
//      示例: Put("key1", "value1")
//      编码: [0x01] [0x04] ['k','e','y','1'] [0x06] ['v','a','l','u','e','1']
//
//   2. 指定列族（column_family_id != 0）:
//      [kTypeColumnFamilyValue] [列族ID] [key长度] [key数据] [value长度] [value数据]
//
//      示例: Put(cf_id=5, "key2", "value2")
//      编码: [0x05] [0x05] [0x04] ['k','e','y','2'] [0x06] ['v','a','l','u','e','2']
//
//   字段说明:
//   - kTypeValue (0x01): Put 操作类型，默认列族
//   - kTypeColumnFamilyValue (0x05): Put 操作类型，指定列族
//   - 列族ID: 使用 Varint32 编码（变长编码，1-5 字节）
//   - key长度: 使用 Varint32 编码（1-5 字节）
//   - key数据: 实际的 key 数据（长度由 key长度指定）
//   - value长度: 使用 Varint32 编码（1-5 字节）
//   - value数据: 实际的 value 数据（长度由 value长度指定）
//
// SliceParts 工作原理:
//
//   SliceParts 允许将多个不连续的内存块组合成一个逻辑上的 key 或 value：
//
//   结构定义:
//   struct SliceParts {
//     const Slice* parts;  // 指向 Slice 数组的指针
//     int num_parts;      // Slice 数组的元素个数
//   };
//
//   使用示例:
//   假设 key 由两部分组成：
//     char part1[] = "prefix_";
//     char part2[] = "suffix";
//     Slice parts[] = {Slice(part1, 7), Slice(part2, 6)};
//     SliceParts key_parts(parts, 2);
//
//   序列化过程:
//     1. 计算总长度: 7 + 6 = 13 字节
//     2. 编码总长度: PutVarint32(&rep_, 13)
//     3. 追加所有部分: rep_.append(part1, 7); rep_.append(part2, 6);
//
//   优势:
//     - 避免内存拷贝：直接使用原始内存块
//     - 适用于分片数据：如网络缓冲区、文件映射等
//     - 减少内存分配：无需合并成连续内存
//
// 实现细节:
//
//   1. 检查 SliceParts 长度:
//      - CheckSlicePartsLength(key, value): 验证 key 和 value 的总长度
//      - 确保长度不超过 uint32_t 的最大值
//      - 如果长度过大，返回 Status::InvalidArgument()
//
//   2. 创建本地保存点（LocalSavePoint）:
//      - LocalSavePoint save(b): 记录当前批次的状态（大小、计数、标志位）
//      - 用于在错误时回滚（如超过 max_bytes_ 限制）
//      - 作用类似于 RAII（Resource Acquisition Is Initialization）
//
//   3. 更新计数器:
//      - SetCount(b, Count(b) + 1): 将批次的操作计数加 1
//      - 计数器存储在 rep_ 的 [8-11] 字节
//      - 用于迭代和统计批次的操作数量
//
//   4. 写入操作类型:
//      - column_family_id == 0: 写入 kTypeValue (0x01)
//      - column_family_id != 0: 写入 kTypeColumnFamilyValue (0x05)
//      - 操作类型占用 1 字节
//
//   5. 写入列族 ID（如果需要）:
//      - PutVarint32(&b->rep_, column_family_id): 使用 Varint32 编码列族 ID
//      - 仅当 column_family_id != 0 时执行
//      - Varint32 编码节省空间（小列族 ID 只需 1 字节）
//
//   6. 写入 key:
//      - PutLengthPrefixedSliceParts(&b->rep_, key): 写入 key
//      - 格式: [key长度] [key数据]
//      - key长度使用 Varint32 编码
//      - key数据由多个 Slice 组成，依次追加
//
//   7. 写入 value:
//      - PutLengthPrefixedSliceParts(&b->rep_, value): 写入 value
//      - 格式: [value长度] [value数据]
//      - value长度使用 Varint32 编码
//      - value数据由多个 Slice 组成，依次追加
//
//   8. 更新内容标志位:
//      - 设置 HAS_PUT 标志位，表示批次包含 Put 操作
//      - 使用原子操作（store/load），保证线程安全
//      - 使用 relaxed 内存序（标志位不需要强同步）
//
//   9. 更新保护信息（如果启用）:
//      - 如果 prot_info_ != nullptr，则添加保护信息条目
//      - ProtectionInfo64 存储校验和信息（8 字节）
//      - 用于后续的 VerifyChecksum() 验证数据完整性
//
//   10. 提交保存点:
//       - save.commit(): 检查是否超过 max_bytes_ 限制
//       - 如果超过，回滚到保存点的状态
//       - 返回相应的状态（OK 或 MemoryLimit）
//
// 时间复杂度:
//   - 时间复杂度: O(n + m)，其中 n = key 的总字节数，m = value 的总字节数
//   - 主要开销：追加 key 和 value 数据到 rep_
//   - 其他操作（计数、标志位）都是 O(1)
//
// 空间复杂度:
//   - 空间复杂度: O(n + m)，追加到 rep_ 的数据量
//   - 不额外分配内存（直接追加到 rep_）
//
// 线程安全性:
//   - 不线程安全：多个线程同时操作同一个 WriteBatch 需要外部同步
//   - content_flags_ 使用原子操作，但整个操作不是原子的
//   - 调用方负责确保单线程访问 WriteBatch
//
// 错误处理:
//   1. CheckSlicePartsLength 失败:
//      - 返回 Status::InvalidArgument()
//      - key 或 value 的总长度超过 uint32_t 的最大值
//
//   2. 超过 max_bytes_ 限制:
//      - 返回 Status::MemoryLimit()
//      - 自动回滚到保存点的状态
//      - 批次保持添加操作前的状态
//
//   3. 内存分配失败:
//      - std::string::append() 可能抛出 std::bad_alloc
//      - RocksDB 通常不使用异常，此情况较少见
//
// 使用示例:
//
//   示例 1: 使用连续的 key 和 value（最常见的用法）:
//   WriteBatch batch;
//   Slice key("hello");
//   Slice value("world");
//   SliceParts key_parts(&key, 1);  // 单个 Slice
//   SliceParts value_parts(&value, 1);  // 单个 Slice
//   Status s = WriteBatchInternal::Put(&batch, 0, key_parts, value_parts);
//   // batch.rep_ = [0x01, 0x05, 'h','e','l','l','o', 0x05, 'w','o','r','l','d']
//
//   示例 2: 使用分片的 key（避免拷贝）:
//   char prefix[] = "user:";
//   char suffix[] = "12345";
//   Slice parts[] = {Slice(prefix, 5), Slice(suffix, 5)};
//   SliceParts key_parts(parts, 2);
//   WriteBatch batch;
//   Status s = WriteBatchInternal::Put(&batch, 0, key_parts,
//                                     SliceParts(&Slice("value"), 1));
//
//   示例 3: 指定列族:
//   WriteBatch batch;
//   Slice key("mykey");
//   Slice value("myvalue");
//   Status s = WriteBatchInternal::Put(&batch, 5,  // 列族 ID = 5
//                                     SliceParts(&key, 1),
//                                     SliceParts(&value, 1));
//
//   示例 4: 批量添加（使用多个 SliceParts）:
//   WriteBatch batch;
//   for (int i = 0; i < 100; i++) {
//     std::string key = "key" + std::to_string(i);
//     std::string value = "value" + std::to_string(i);
//     Slice key_slice(key);
//     Slice value_slice(value);
//     Status s = WriteBatchInternal::Put(&batch, 0,
//                                          SliceParts(&key_slice, 1),
//                                          SliceParts(&value_slice, 1));
//     if (!s.ok()) {
//       // 处理错误
//       break;
//     }
//   }
//
// 性能考虑:
//   1. SliceParts 的优势:
//      - 避免内存拷贝：直接使用原始内存块
//      - 适用于大对象：减少内存分配和复制
//      - 零拷贝序列化：提高性能
//
//   2. Varint32 编码:
//      - 小数值（<128）只需 1 字节，节省空间
//      - 适用于列族 ID（通常较小）
//      - 适用于 key/value 长度（大多数情况下较小）
//
//   3. 原子操作:
//      - content_flags_ 使用原子操作，避免锁
//      - relaxed 内存序足够（标志位不需要强同步）
//      - 多线程读取标志位是安全的
//
//   4. 保存点机制:
//      - 仅在超过 max_bytes_ 时才回滚
//      - 大多数情况下不需要回滚（无额外开销）
//      - 使用 RAII 保证资源释放
//
// 注意事项:
//   1. SliceParts 的生命周期:
//      - SliceParts 指向的内存必须在序列化期间有效
//      - 不要在序列化前释放或修改 Slice 指向的内存
//      - 建议使用栈内存或确保内存管理正确
//
//   2. 列族 ID 的有效性:
//      - 调用方负责确保 column_family_id 有效
//      - 无效的列族 ID 会在后续操作中失败
//      - 列族 ID 必须由 DB 分配（不能随意指定）
//
//   3. 批次大小限制:
//      - 如果设置 max_bytes_，添加操作可能失败
//      - 返回 Status::MemoryLimit() 时需要分批写入
//      - 建议根据预期大小设置合理的 max_bytes_
//
//   4. 内容标志位的延迟计算:
//      - HAS_PUT 标志位在添加 Put 操作时立即设置
//      - 不需要遍历整个批次来计算标志位
//      - 提高查询性能（HasPut() 可以快速返回）
//
//   5. 保护信息的性能开销:
//      - 启用保护（prot_info_ != nullptr）会增加 CPU 开销
//      - 需要计算和存储校验和（每个 key-value 对 8 字节）
//      - 仅在需要强数据完整性保证时启用
//
// 相关函数:
//   - WriteBatch::Put(): 公共接口（内部调用此函数）
//   - WriteBatchInternal::Put(WriteBatch*, uint32_t, const Slice&, const Slice&):
//     另一个重载（使用简单的 Slice）
//   - PutLengthPrefixedSliceParts(): 序列化 SliceParts
//   - PutVarint32(): Varint32 编码
//   - WriteBatchInternal::SetCount(): 设置操作计数
//   - WriteBatchInternal::Count(): 获取操作计数
//   - LocalSavePoint::commit(): 提交保存点
//
// 内存布局示例:
//
//   示例: WriteBatchInternal::Put(b, 5, key_parts, value_parts)
//   假设:
//     key = "hello" (5 字节)
//     value = "world" (5 字节)
//     column_family_id = 5
//
//   rep_ 的内存布局:
//   +-----------------+-----------------+-----------------+-----------------+
//   | kTypeColumnFam-  | 列族 ID         | key长度         | 'h'             |
//   | ilyValue (0x05) | (0x05)          | (0x05)          |                 |
//   +-----------------+-----------------+-----------------+-----------------+
//   | 'e'             | 'l'             | 'l'             | 'o'             |
//   |                 |                 |                 |                 |
//   +-----------------+-----------------+-----------------+-----------------+
//   | value长度       | 'w'             | 'o'             | 'r'             |
//   | (0x05)          |                 |                 |                 |
//   +-----------------+-----------------+-----------------+-----------------+
//   | 'l'             | 'd'             |
//   |                 |                 |
//   +-----------------+-----------------+
//
//   总共: 1 + 1 + 1 + 5 + 1 + 5 = 14 字节
//   (类型 + 列族ID + key长度 + key数据 + value长度 + value数据)
// ============================================================================
Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                               const SliceParts& key, const SliceParts& value) {
  // 检查 key 和 value 的长度是否合法
  // 确保总长度不超过 uint32_t 的最大值（4294967295）
  // 如果长度过大，返回 Status::InvalidArgument()
  Status s = CheckSlicePartsLength(key, value);
  if (!s.ok()) {
    return s;
  }

  // 创建本地保存点（LocalSavePoint），记录当前批次的状态
  // 保存的信息包括：批次大小（rep_.size()）、操作计数（count）、内容标志位（content_flags_）
  // 用于在错误时回滚（如超过 max_bytes_ 限制）
  // 类似于 RAII（Resource Acquisition Is Initialization）模式
  LocalSavePoint save(b);

  // 更新批次的操作计数（count），加 1
  // 计数器存储在 rep_ 的 [8-11] 字节（4 字节固定整数）
  // 用于统计批次中的操作数量和迭代
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);

  // 根据列族 ID 写入操作类型（ValueType）
  // column_family_id == 0: 默认列族，写入 kTypeValue (0x01)
  // column_family_id != 0: 指定列族，写入 kTypeColumnFamilyValue (0x05)
  // 操作类型占用 1 字节，用于后续解码时识别操作类型
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    // 写入列族 ID，使用 Varint32 编码（变长编码）
    // Varint32 编码节省空间：小列族 ID（<128）只需 1 字节
    // 大列族 ID 可能需要 2-5 字节
    PutVarint32(&b->rep_, column_family_id);
  }

  // 序列化 key 到 rep_
  // 格式: [key长度] [key数据]
  // key长度使用 Varint32 编码（1-5 字节）
  // key数据由多个 Slice 组成，依次追加到 rep_
  // 使用 PutLengthPrefixedSliceParts 可以处理非连续的 key 数据（分片存储）
  PutLengthPrefixedSliceParts(&b->rep_, key);

  // 序列化 value 到 rep_
  // 格式: [value长度] [value数据]
  // value长度使用 Varint32 编码（1-5 字节）
  // value数据由多个 Slice 组成，依次追加到 rep_
  // 使用 PutLengthPrefixedSliceParts 可以处理非连续的 value 数据（分片存储）
  PutLengthPrefixedSliceParts(&b->rep_, value);

  // 更新内容标志位，设置 HAS_PUT 标志
  // 表示批次中包含 Put 操作
  // 使用原子操作（store/load）保证线程安全
  // 使用 relaxed 内存序（标志位不需要强同步，只读操作）
  // 原子操作允许其他线程同时读取 content_flags_
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);

  // 如果启用了保护信息（prot_info_ != nullptr），则添加保护信息条目
  // ProtectionInfo64 存储校验和信息（8 字节）
  // 用于后续的 VerifyChecksum() 验证数据完整性
  // ProtectKVO(): 保护 Key-Value-Operation 三元组
  // ProtectC(): 保护 Column Family ID
  // 注意: ProtectKVO() 的参数是 kTypeValue，无论是否使用列族
  // 这是保护信息层的实现细节，不影响功能
  if (b->prot_info_ != nullptr) {
    // 参考第一个 WriteBatchInternal::Put() 重载中关于传递给 ProtectKVO() 的
    // ValueType 参数的注释说明。
    b->prot_info_->entries_.emplace_back(ProtectionInfo64()
                                             .ProtectKVO(key, value, kTypeValue)
                                             .ProtectC(column_family_id));
  }

  // 提交保存点（LocalSavePoint::commit）
  // 检查是否超过 max_bytes_ 限制
  // 如果超过，回滚到保存点的状态（恢复 rep_、count、content_flags_ 等）
  // 返回相应的状态：
  //   - Status::OK(): 成功，操作已添加到批次
  //   - Status::MemoryLimit(): 失败，超过大小限制，已回滚
  return save.commit();
}

/**
 * @brief 向 WriteBatch 中添加一个 Put 操作（SliceParts 版本）
 *
 * 本函数是 Put 操作的 SliceParts 版本，类似 writev 系统调用：
 * 1. 获取列族 ID 和时间戳大小
 * 2. 如果列族未启用时间戳，调用 SliceParts 版本的底层 Put
 * 3. 如果列族启用了时间戳，返回错误（SliceParts 不支持时间戳）
 *
 * @param column_family 列族句柄，如果为 nullptr 则使用默认列族
 * @param key 键的 SliceParts（可以是多个不连续的内存块）
 * @param value 值的 SliceParts（可以是多个不连续的内存块）
 *
 * @return Status 成功返回 Status::OK()，失败返回错误状态
 *
 * @note SliceParts 的优势：
 *   - 类似 writev，可以一次性写入多个不连续的内存块
 *   - 避免额外的内存拷贝
 *   - 适合从多个 buffer 构建大的 key 或 value
 *
 * @note 时间戳限制：
 *   - 本函数不支持时间戳列族（ts_sz > 0）
 *   - 如果列族启用了时间戳，返回 Status::InvalidArgument
 *   - 需要使用带显式 ts 参数的版本
 *
 * @note 序列化格式：
 *   - 无列族：kTypeValue | key_parts | value_parts
 *   - 有列族：kTypeColumnFamilyValue | cf_id | key_parts | value_parts
 *   - key_parts 和 value_parts 分别序列化为长度+数据块
 *
 * @see WriteBatch::Put(ColumnFamilyHandle*, const Slice&, const Slice&)
 *      普通 Slice 版本
 * @see WriteBatch::Put(ColumnFamilyHandle*, const Slice&, const Slice&, const Slice&)
 *      带显式时间戳的版本
 * @see SliceParts
 *      多个 Slice 组合的结构体
 */
Status WriteBatch::Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                       const SliceParts& value) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  // 获取列族 ID 和时间戳大小
  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (ts_sz == 0) {
    // 未启用时间戳：调用 SliceParts 版本的底层 Put 函数
    return WriteBatchInternal::Put(this, cf_id, key, value);
  }

  // 启用了时间戳：SliceParts 版本不支持
  // 因为时间戳需要明确的位置和大小信息
  return Status::InvalidArgument(
      "Cannot call this method on column family enabling timestamp");
}

Status WriteBatchInternal::PutEntity(WriteBatch* b, uint32_t column_family_id,
                                     const Slice& key,
                                     const WideColumns& columns) {
  assert(b);

  if (key.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
    return Status::InvalidArgument("key is too large");
  }

  WideColumns sorted_columns(columns);
  std::sort(sorted_columns.begin(), sorted_columns.end(),
            [](const WideColumn& lhs, const WideColumn& rhs) {
              return lhs.name().compare(rhs.name()) < 0;
            });

  std::string entity;
  const Status s = WideColumnSerialization::Serialize(sorted_columns, entity);
  if (!s.ok()) {
    return s;
  }

  if (entity.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
    return Status::InvalidArgument("wide column entity is too large");
  }

  LocalSavePoint save(b);

  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);

  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeWideColumnEntity));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyWideColumnEntity));
    PutVarint32(&b->rep_, column_family_id);
  }

  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, entity);

  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_PUT_ENTITY,
                          std::memory_order_relaxed);

  if (b->prot_info_ != nullptr) {
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(key, entity, kTypeWideColumnEntity)
            .ProtectC(column_family_id));
  }

  return save.commit();
}

Status WriteBatch::PutEntity(ColumnFamilyHandle* column_family,
                             const Slice& key, const WideColumns& columns) {
  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call this method without a column family handle");
  }

  Status s;
  uint32_t cf_id = 0;
  size_t ts_sz = 0;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (ts_sz) {
    return Status::InvalidArgument(
        "Cannot call this method on column family enabling timestamp");
  }

  return WriteBatchInternal::PutEntity(this, cf_id, key, columns);
}

Status WriteBatchInternal::InsertNoop(WriteBatch* b) {
  b->rep_.push_back(static_cast<char>(kTypeNoop));
  return Status::OK();
}

Status WriteBatchInternal::MarkEndPrepare(WriteBatch* b, const Slice& xid,
                                          bool write_after_commit,
                                          bool unprepared_batch) {
  // a manually constructed batch can only contain one prepare section
  assert(b->rep_[12] == static_cast<char>(kTypeNoop));

  // all savepoints up to this point are cleared
  if (b->save_points_ != nullptr) {
    while (!b->save_points_->stack.empty()) {
      b->save_points_->stack.pop();
    }
  }

  // rewrite noop as begin marker
  b->rep_[12] = static_cast<char>(
      write_after_commit ? kTypeBeginPrepareXID
                         : (unprepared_batch ? kTypeBeginUnprepareXID
                                             : kTypeBeginPersistedPrepareXID));
  b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_END_PREPARE |
                              ContentFlags::HAS_BEGIN_PREPARE,
                          std::memory_order_relaxed);
  if (unprepared_batch) {
    b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                                ContentFlags::HAS_BEGIN_UNPREPARE,
                            std::memory_order_relaxed);
  }
  return Status::OK();
}

Status WriteBatchInternal::MarkCommit(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeCommitXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_COMMIT,
                          std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::MarkCommitWithTimestamp(WriteBatch* b,
                                                   const Slice& xid,
                                                   const Slice& commit_ts) {
  assert(!commit_ts.empty());
  b->rep_.push_back(static_cast<char>(kTypeCommitXIDAndTimestamp));
  PutLengthPrefixedSlice(&b->rep_, commit_ts);
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_COMMIT,
                          std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::MarkRollback(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeRollbackXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_ROLLBACK,
                          std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                  const Slice& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(key, "" /* value */, kTypeDeletion)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::Delete(this, cf_id, key);
  }

  needs_in_place_update_ts_ = true;
  has_key_with_ts_ = true;
  std::string dummy_ts(ts_sz, '\0');
  std::array<Slice, 2> key_with_ts{{key, dummy_ts}};
  return WriteBatchInternal::Delete(this, cf_id,
                                    SliceParts(key_with_ts.data(), 2));
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key,
                          const Slice& ts) {
  const Status s = CheckColumnFamilyTimestampSize(column_family, ts);
  if (!s.ok()) {
    return s;
  }
  assert(column_family);
  has_key_with_ts_ = true;
  uint32_t cf_id = column_family->GetID();
  std::array<Slice, 2> key_with_ts{{key, ts}};
  return WriteBatchInternal::Delete(this, cf_id,
                                    SliceParts(key_with_ts.data(), 2));
}

Status WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                  const SliceParts& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(key,
                        SliceParts(nullptr /* _parts */, 0 /* _num_parts */),
                        kTypeDeletion)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family,
                          const SliceParts& key) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::Delete(this, cf_id, key);
  }

  return Status::InvalidArgument(
      "Cannot call this method on column family enabling timestamp");
}

Status WriteBatchInternal::SingleDelete(WriteBatch* b,
                                        uint32_t column_family_id,
                                        const Slice& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(key, "" /* value */, kTypeSingleDeletion)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                const Slice& key) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::SingleDelete(this, cf_id, key);
  }

  needs_in_place_update_ts_ = true;
  has_key_with_ts_ = true;
  std::string dummy_ts(ts_sz, '\0');
  std::array<Slice, 2> key_with_ts{{key, dummy_ts}};
  return WriteBatchInternal::SingleDelete(this, cf_id,
                                          SliceParts(key_with_ts.data(), 2));
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& ts) {
  const Status s = CheckColumnFamilyTimestampSize(column_family, ts);
  if (!s.ok()) {
    return s;
  }
  has_key_with_ts_ = true;
  assert(column_family);
  uint32_t cf_id = column_family->GetID();
  std::array<Slice, 2> key_with_ts{{key, ts}};
  return WriteBatchInternal::SingleDelete(this, cf_id,
                                          SliceParts(key_with_ts.data(), 2));
}

Status WriteBatchInternal::SingleDelete(WriteBatch* b,
                                        uint32_t column_family_id,
                                        const SliceParts& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(key,
                        SliceParts(nullptr /* _parts */,
                                   0 /* _num_parts */) /* value */,
                        kTypeSingleDeletion)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                const SliceParts& key) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::SingleDelete(this, cf_id, key);
  }

  return Status::InvalidArgument(
      "Cannot call this method on column family enabling timestamp");
}

Status WriteBatchInternal::DeleteRange(WriteBatch* b, uint32_t column_family_id,
                                       const Slice& begin_key,
                                       const Slice& end_key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeRangeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyRangeDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, begin_key);
  PutLengthPrefixedSlice(&b->rep_, end_key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE_RANGE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    // In `DeleteRange()`, the end key is treated as the value.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(begin_key, end_key, kTypeRangeDeletion)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                               const Slice& begin_key, const Slice& end_key) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::DeleteRange(this, cf_id, begin_key, end_key);
  }

  needs_in_place_update_ts_ = true;
  has_key_with_ts_ = true;
  std::string dummy_ts(ts_sz, '\0');
  std::array<Slice, 2> begin_key_with_ts{{begin_key, dummy_ts}};
  std::array<Slice, 2> end_key_with_ts{{end_key, dummy_ts}};
  return WriteBatchInternal::DeleteRange(
      this, cf_id, SliceParts(begin_key_with_ts.data(), 2),
      SliceParts(end_key_with_ts.data(), 2));
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                               const Slice& begin_key, const Slice& end_key,
                               const Slice& ts) {
  const Status s = CheckColumnFamilyTimestampSize(column_family, ts);
  if (!s.ok()) {
    return s;
  }
  assert(column_family);
  has_key_with_ts_ = true;
  uint32_t cf_id = column_family->GetID();
  std::array<Slice, 2> key_with_ts{{begin_key, ts}};
  std::array<Slice, 2> end_key_with_ts{{end_key, ts}};
  return WriteBatchInternal::DeleteRange(this, cf_id,
                                         SliceParts(key_with_ts.data(), 2),
                                         SliceParts(end_key_with_ts.data(), 2));
}

Status WriteBatchInternal::DeleteRange(WriteBatch* b, uint32_t column_family_id,
                                       const SliceParts& begin_key,
                                       const SliceParts& end_key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeRangeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyRangeDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, begin_key);
  PutLengthPrefixedSliceParts(&b->rep_, end_key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE_RANGE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    // In `DeleteRange()`, the end key is treated as the value.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(begin_key, end_key, kTypeRangeDeletion)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                               const SliceParts& begin_key,
                               const SliceParts& end_key) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::DeleteRange(this, cf_id, begin_key, end_key);
  }

  return Status::InvalidArgument(
      "Cannot call this method on column family enabling timestamp");
}

Status WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                                 const Slice& key, const Slice& value) {
  if (key.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
    return Status::InvalidArgument("key is too large");
  }
  if (value.size() > size_t{std::numeric_limits<uint32_t>::max()}) {
    return Status::InvalidArgument("value is too large");
  }

  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(ProtectionInfo64()
                                             .ProtectKVO(key, value, kTypeMerge)
                                             .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& value) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::Merge(this, cf_id, key, value);
  }

  needs_in_place_update_ts_ = true;
  has_key_with_ts_ = true;
  std::string dummy_ts(ts_sz, '\0');
  std::array<Slice, 2> key_with_ts{{key, dummy_ts}};

  return WriteBatchInternal::Merge(
      this, cf_id, SliceParts(key_with_ts.data(), 2), SliceParts(&value, 1));
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& ts, const Slice& value) {
  const Status s = CheckColumnFamilyTimestampSize(column_family, ts);
  if (!s.ok()) {
    return s;
  }
  has_key_with_ts_ = true;
  assert(column_family);
  uint32_t cf_id = column_family->GetID();
  std::array<Slice, 2> key_with_ts{{key, ts}};
  return WriteBatchInternal::Merge(
      this, cf_id, SliceParts(key_with_ts.data(), 2), SliceParts(&value, 1));
}

Status WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                                 const SliceParts& key,
                                 const SliceParts& value) {
  Status s = CheckSlicePartsLength(key, value);
  if (!s.ok()) {
    return s;
  }

  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(ProtectionInfo64()
                                             .ProtectKVO(key, value, kTypeMerge)
                                             .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family,
                         const SliceParts& key, const SliceParts& value) {
  size_t ts_sz = 0;
  uint32_t cf_id = 0;
  Status s;

  std::tie(s, cf_id, ts_sz) =
      WriteBatchInternal::GetColumnFamilyIdAndTimestampSize(this,
                                                            column_family);

  if (!s.ok()) {
    return s;
  }

  if (0 == ts_sz) {
    return WriteBatchInternal::Merge(this, cf_id, key, value);
  }

  return Status::InvalidArgument(
      "Cannot call this method on column family enabling timestamp");
}

Status WriteBatchInternal::PutBlobIndex(WriteBatch* b,
                                        uint32_t column_family_id,
                                        const Slice& key, const Slice& value) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeBlobIndex));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyBlobIndex));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_BLOB_INDEX,
                          std::memory_order_relaxed);
  if (b->prot_info_ != nullptr) {
    // See comment in first `WriteBatchInternal::Put()` overload concerning the
    // `ValueType` argument passed to `ProtectKVO()`.
    b->prot_info_->entries_.emplace_back(
        ProtectionInfo64()
            .ProtectKVO(key, value, kTypeBlobIndex)
            .ProtectC(column_family_id));
  }
  return save.commit();
}

Status WriteBatch::PutLogData(const Slice& blob) {
  LocalSavePoint save(this);
  rep_.push_back(static_cast<char>(kTypeLogData));
  PutLengthPrefixedSlice(&rep_, blob);
  return save.commit();
}

void WriteBatch::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_.reset(new SavePoints());
  }
  // Record length and count of current batch of writes.
  save_points_->stack.push(SavePoint(
      GetDataSize(), Count(), content_flags_.load(std::memory_order_relaxed)));
}

Status WriteBatch::RollbackToSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  SavePoint savepoint = save_points_->stack.top();
  save_points_->stack.pop();

  assert(savepoint.size <= rep_.size());
  assert(static_cast<uint32_t>(savepoint.count) <= Count());

  if (savepoint.size == rep_.size()) {
    // No changes to rollback
  } else if (savepoint.size == 0) {
    // Rollback everything
    Clear();
  } else {
    rep_.resize(savepoint.size);
    if (prot_info_ != nullptr) {
      prot_info_->entries_.resize(savepoint.count);
    }
    WriteBatchInternal::SetCount(this, savepoint.count);
    content_flags_.store(savepoint.content_flags, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status WriteBatch::PopSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  save_points_->stack.pop();

  return Status::OK();
}

Status WriteBatch::UpdateTimestamps(
    const Slice& ts, std::function<size_t(uint32_t)> ts_sz_func) {
  TimestampUpdater<decltype(ts_sz_func)> ts_updater(prot_info_.get(),
                                                    std::move(ts_sz_func), ts);
  const Status s = Iterate(&ts_updater);
  if (s.ok()) {
    needs_in_place_update_ts_ = false;
  }
  return s;
}

Status WriteBatch::VerifyChecksum() const {
  if (prot_info_ == nullptr) {
    return Status::OK();
  }
  Slice input(rep_.data() + WriteBatchInternal::kHeader,
              rep_.size() - WriteBatchInternal::kHeader);
  Slice key, value, blob, xid;
  char tag = 0;
  uint32_t column_family = 0;  // default
  Status s;
  size_t prot_info_idx = 0;
  bool checksum_protected = true;
  while (!input.empty() && prot_info_idx < prot_info_->entries_.size()) {
    // In case key/value/column_family are not updated by
    // ReadRecordFromWriteBatch
    key.clear();
    value.clear();
    column_family = 0;
    s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                 &blob, &xid);
    if (!s.ok()) {
      return s;
    }
    checksum_protected = true;
    // Write batch checksum uses op_type without ColumnFamily (e.g., if op_type
    // in the write batch is kTypeColumnFamilyValue, kTypeValue is used to
    // compute the checksum), and encodes column family id separately. See
    // comment in first `WriteBatchInternal::Put()` for more detail.
    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        tag = kTypeValue;
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        tag = kTypeDeletion;
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        tag = kTypeSingleDeletion;
        break;
      case kTypeColumnFamilyRangeDeletion:
      case kTypeRangeDeletion:
        tag = kTypeRangeDeletion;
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        tag = kTypeMerge;
        break;
      case kTypeColumnFamilyBlobIndex:
      case kTypeBlobIndex:
        tag = kTypeBlobIndex;
        break;
      case kTypeLogData:
      case kTypeBeginPrepareXID:
      case kTypeEndPrepareXID:
      case kTypeCommitXID:
      case kTypeRollbackXID:
      case kTypeNoop:
      case kTypeBeginPersistedPrepareXID:
      case kTypeBeginUnprepareXID:
      case kTypeDeletionWithTimestamp:
      case kTypeCommitXIDAndTimestamp:
        checksum_protected = false;
        break;
      case kTypeColumnFamilyWideColumnEntity:
      case kTypeWideColumnEntity:
        tag = kTypeWideColumnEntity;
        break;
      default:
        return Status::Corruption(
            "unknown WriteBatch tag",
            std::to_string(static_cast<unsigned int>(tag)));
    }
    if (checksum_protected) {
      s = prot_info_->entries_[prot_info_idx++]
              .StripC(column_family)
              .StripKVO(key, value, static_cast<ValueType>(tag))
              .GetStatus();
      if (!s.ok()) {
        return s;
      }
    }
  }

  if (prot_info_idx != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  }
  assert(WriteBatchInternal::Count(this) == prot_info_->entries_.size());
  return Status::OK();
}

namespace {

/**
 * MemTableInserter - 将 WriteBatch 插入 MemTable 的 Handler 实现
 *
 * 功能概述:
 *   - 继承自 WriteBatch::Handler，用于遍历 WriteBatch 并将操作插入到 MemTable
 *   - 处理所有类型的写操作：Put、Delete、Merge、DeleteRange 等
 *   - 支持事务标记：Prepare、Commit、Rollback
 *   - 处理并发写入场景和 WAL 恢复场景
 *   - 管理 Flush 和 TrimHistory 调度
 *
 * 设计要点:
 *   1. 序列号管理：
 *      - seq_per_batch: 每个 WriteBatch 使用一个序列号（WritePrepared/WriteUnprepared）
 *      - !seq_per_batch: 每个操作使用一个序列号（WriteCommitted）
 *   2. 事务策略支持：
 *      - write_after_commit: 事务提交后才写入 MemTable（WriteCommitted）
 *      - write_before_prepare: 在 Prepare 之前写入（WriteUnprepared）
 *   3. 并发写入支持：
 *      - concurrent_memtable_writes: 启用并发 MemTable 写入
 *      - 使用延迟初始化的 map 来减少内存分配开销
 *   4. WAL 恢复支持：
 *      - recovering_log_number 非 0 时表示恢复模式
 *      - 重建事务对象以跟踪准备阶段的事务
 *   5. 内存优化：
 *      - 使用 aligned_storage 延迟初始化 map，避免不必要的内存分配
 *      - ProtectionInfo 用于并发写入时的数据保护
 *
 * 调用时机:
 *   - WriteThread::Write: 批量插入写操作到 MemTable
 *   - DBImpl::Recover: 从 WAL 恢复数据到 MemTable
 *   - DBImpl::WriteRecoverableState: 恢复可恢复状态
 */
class MemTableInserter : public WriteBatch::Handler {
  // ========== 核心状态成员 ==========
  SequenceNumber sequence_;                    // 当前序列号（随着每次操作递增）
  ColumnFamilyMemTables* const cf_mems_;      // 列族 MemTable 映射（CFID -> MemTable）
  FlushScheduler* const flush_scheduler_;      // Flush 调度器（memtable 满时触发）
  TrimHistoryScheduler* const trim_history_scheduler_;  // MemTable 历史清理调度器
  const bool ignore_missing_column_families_;  // 是否忽略不存在的列族（恢复模式）
  const uint64_t recovering_log_number_;      // 正在恢复的 WAL 日志号（0 表示非恢复模式）
  // 所有插入的 MemTable 应该引用的日志号
  uint64_t log_number_ref_;
  DBImpl* db_;                              // 数据库实例（用于事务恢复等）
  const bool concurrent_memtable_writes_;      // 是否启用并发 MemTable 写入
  bool post_info_created_;                    // 是否已创建 post_process_info map（延迟初始化）
  const WriteBatch::ProtectionInfo* prot_info_;  // 保护信息（并发写入时使用）
  size_t prot_info_idx_;                     // 保护信息索引（当前处理到第几个条目）

  // ========== 重建事务相关（WAL 恢复场景）==========
  bool* has_valid_writes_;                   // 输出参数：是否有有效写入（恢复时使用）
  // 在某些平台上，默认创建 map 在 Write() 路径中开销太大，
  // 因为即使未使用也会导致内存分配。
  // 使创建可选，但避免 std::unique_ptr 的额外分配开销
  using MemPostInfoMap = std::map<MemTable*, MemTablePostProcessInfo>;
  using PostMapType = std::aligned_storage<sizeof(MemPostInfoMap)>::type;
  PostMapType mem_post_info_map_;             // MemTable -> 后处理信息的映射（延迟初始化）

  // 当前正在重建的事务（WAL 恢复时）
  WriteBatch* rebuilding_trx_;                // 重建的事务对象（包含准备阶段的操作）
  SequenceNumber rebuilding_trx_seq_;          // 重建事务的起始序列号

  // ========== 序列号和事务策略 ==========
  // 每个 WriteBatch 增加一次序列号，否则每个键增加一次
  bool seq_per_batch_;
  // MemTable 写入是否只在提交后进行（WriteCommitted 模式）
  bool write_after_commit_;
  // MemTable 写入是否可以在 Prepare 之前进行（WriteUnprepared 模式）
  bool write_before_prepare_;
  // 当前批次是否是未准备批次
  bool unprepared_batch_;

  // ========== 重复检测（WAL 恢复时使用）==========
  using DupDetector = std::aligned_storage<sizeof(DuplicateDetector)>::type;
  DupDetector duplicate_detector_;              // 重复键检测器（延迟初始化）
  bool dup_dectector_on_;                   // 重复检测器是否已启用

  // ========== Hint 优化（并发写入）==========
  bool hint_per_batch_;                      // 是否按批次使用 Hint
  bool hint_created_;                        // Hint map 是否已创建
  // 当前批次的 Hint（用于优化 MemTable 插入性能）
  using HintMap = std::unordered_map<MemTable*, void*>;
  using HintMapType = std::aligned_storage<sizeof(HintMap)>::type;
  HintMapType hint_;

  /**
   * 获取 Hint Map（延迟初始化）
   *
   * Hint 用于优化 MemTable 插入性能，在并发写入模式下尤为有效。
   * Hint 可以跳过某些不必要的检查，提升插入速度。
   *
   * @return HintMap 引用
   */
  HintMap& GetHintMap() {
    assert(hint_per_batch_);
    if (!hint_created_) {
      // 使用 placement new 延迟初始化，避免不必要的内存分配
      new (&hint_) HintMap();
      hint_created_ = true;
    }
    return *reinterpret_cast<HintMap*>(&hint_);
  }

  /**
   * 获取 PostProcess Info Map（延迟初始化）
   *
   * 并发写入模式下，需要对 MemTable 进行后处理（如更新统计信息）。
   * 使用 map 存储每个 MemTable 需要的后处理信息，避免在插入时修改共享状态。
   *
   * @return MemPostInfoMap 引用
   */
  MemPostInfoMap& GetPostMap() {
    assert(concurrent_memtable_writes_);
    if (!post_info_created_) {
      // 使用 placement new 延迟初始化，避免不必要的内存分配
      new (&mem_post_info_map_) MemPostInfoMap();
      post_info_created_ = true;
    }
    return *reinterpret_cast<MemPostInfoMap*>(&mem_post_info_map_);
  }

  /**
   * 检查键和序列号是否重复（WAL 恢复时使用）
   *
   * 在 WAL 恢复过程中，可能遇到相同的键和序列号组合，
   * 需要检测并跳过重复插入以避免数据不一致。
   *
   * 适用场景：
   * - write_after_commit_ 为 false（WritePrepared/WriteUnprepared 模式）
   * - rebuilding_trx_ 非空（正在重建事务）
   *
   * @param column_family_id 列族 ID
   * @param key 键
   * @return true 如果键和序列号组合重复，false 否则
   */
  bool IsDuplicateKeySeq(uint32_t column_family_id, const Slice& key) {
    assert(!write_after_commit_);
    assert(rebuilding_trx_ != nullptr);
    if (!dup_dectector_on_) {
      // 延迟初始化重复检测器
      new (&duplicate_detector_) DuplicateDetector(db_);
      dup_dectector_on_ = true;
    }
    return reinterpret_cast<DuplicateDetector*>(&duplicate_detector_)
        ->IsDuplicateKeySeq(column_family_id, key, sequence_);
  }

  /**
   * 获取下一个保护信息条目
   *
   * 并发写入时，每个键值对都有对应的保护信息（ProtectionInfoKVOC64），
   * 用于数据完整性校验和并发控制。
   *
   * @return 保护信息指针，如果没有保护信息则返回 nullptr
   */
  const ProtectionInfoKVOC64* NextProtectionInfo() {
    const ProtectionInfoKVOC64* res = nullptr;
    if (prot_info_ != nullptr) {
      assert(prot_info_idx_ < prot_info_->entries_.size());
      res = &prot_info_->entries_[prot_info_idx_];
      ++prot_info_idx_;  // 移动到下一个条目
    }
    return res;
  }

  /**
   * 回退保护信息索引（用于 TryAgain 重试）
   *
   * 当 Handler 返回 TryAgain 时，需要回退索引以便下次重试时
   * 使用相同的保护信息。
   *
   * 适用场景：
   * - MemTable 插入时遇到 TryAgain 状态
   * - 需要等待某些条件满足后重试（如等待锁、等待事务状态）
   */
  void DecrementProtectionInfoIdxForTryAgain() {
    if (prot_info_ != nullptr) --prot_info_idx_;
  }

  /**
   * 重置保护信息
   *
   * 在某些场景下（如 WriteCommitted 模式下提交事务时），
   * 需要重置保护信息以便重新遍历 WriteBatch。
   */
  void ResetProtectionInfo() {
    prot_info_idx_ = 0;
    prot_info_ = nullptr;
  }

 protected:
  /**
   * 返回 WriteBeforePrepare 选项状态
   *
   * 用于事务模式兼容性检查：
   * - WriteUnprepared 模式返回 kEnabled
   * - 其他模式返回 kDisabled
   */
  Handler::OptionState WriteBeforePrepare() const override {
    return write_before_prepare_ ? Handler::OptionState::kEnabled
                                 : Handler::OptionState::kDisabled;
  }

  /**
   * 返回 WriteAfterCommit 选项状态
   *
   * 用于事务模式兼容性检查：
   * - WriteCommitted 模式返回 kEnabled
   * - 其他模式返回 kDisabled
   */
  Handler::OptionState WriteAfterCommit() const override {
    return write_after_commit_ ? Handler::OptionState::kEnabled
                               : Handler::OptionState::kDisabled;
  }

 public:
  /**
   * 构造函数
   *
   * @param _sequence 起始序列号
   * @param cf_mems 列族 MemTable 映射（注意：不能与并发插入器共享）
   * @param flush_scheduler Flush 调度器
   * @param trim_history_scheduler TrimHistory 调度器
   * @param ignore_missing_column_families 是否忽略不存在的列族
   * @param recovering_log_number 恢复时的 WAL 日志号（0 表示非恢复模式）
   * @param db 数据库实例
   * @param concurrent_memtable_writes 是否启用并发写入
   * @param prot_info 保护信息（并发写入时使用）
   * @param has_valid_writes 输出参数：是否有有效写入
   * @param seq_per_batch 是否每个 WriteBatch 使用一个序列号
   * @param batch_per_txn 是否每个事务一个 WriteBatch
   * @param hint_per_batch 是否使用 Hint 优化
   *
   * 事务策略推导：
   * - write_after_commit = !seq_per_batch
   *   - seq_per_batch=false (每个操作一个序列号) => write_after_commit=true (WriteCommitted)
   *   - seq_per_batch=true (每个批次一个序列号) => write_after_commit=false (WritePrepared/WriteUnprepared)
   *
   * - write_before_prepare = !batch_per_txn
   *   - batch_per_txn=true (每个事务一个批次) => write_before_prepare=false
   *   - batch_per_txn=false (每个事务多个批次) => write_before_prepare=true (WriteUnprepared)
   */
  // cf_mems should not be shared with concurrent inserters
  MemTableInserter(SequenceNumber _sequence, ColumnFamilyMemTables* cf_mems,
                   FlushScheduler* flush_scheduler,
                   TrimHistoryScheduler* trim_history_scheduler,
                   bool ignore_missing_column_families,
                   uint64_t recovering_log_number, DB* db,
                   bool concurrent_memtable_writes,
                   const WriteBatch::ProtectionInfo* prot_info,
                   bool* has_valid_writes = nullptr, bool seq_per_batch = false,
                   bool batch_per_txn = true, bool hint_per_batch = false)
      : sequence_(_sequence),
        cf_mems_(cf_mems),
        flush_scheduler_(flush_scheduler),
        trim_history_scheduler_(trim_history_scheduler),
        ignore_missing_column_families_(ignore_missing_column_families),
        recovering_log_number_(recovering_log_number),
        log_number_ref_(0),
        db_(static_cast_with_check<DBImpl>(db)),
        concurrent_memtable_writes_(concurrent_memtable_writes),
        post_info_created_(false),
        prot_info_(prot_info),
        prot_info_idx_(0),
        has_valid_writes_(has_valid_writes),
        rebuilding_trx_(nullptr),
        rebuilding_trx_seq_(0),
        seq_per_batch_(seq_per_batch),
        // Write after commit currently uses one seq per key (instead of per
        // batch). So seq_per_batch being false indicates write_after_commit
        // approach.
        write_after_commit_(!seq_per_batch),
        // WriteUnprepared can write WriteBatches per transaction, so
        // batch_per_txn being false indicates write_before_prepare.
        write_before_prepare_(!batch_per_txn),
        unprepared_batch_(false),
        duplicate_detector_(),
        dup_dectector_on_(false),
        hint_per_batch_(hint_per_batch),
        hint_created_(false) {
    assert(cf_mems_);
  }

  /**
   * 析构函数
   *
   * 清理所有延迟初始化的资源：
   * - DuplicateDetector（如果已启用）
   * - MemPostInfoMap（并发写入时）
   * - HintMap（如果使用 Hint）
   * - rebuilding_trx_（恢复时重建的事务）
   */
  ~MemTableInserter() override {
    // 手动调用析构函数（因为使用了 placement new）
    if (dup_dectector_on_) {
      reinterpret_cast<DuplicateDetector*>(&duplicate_detector_)
          ->~DuplicateDetector();
    }
    if (post_info_created_) {
      reinterpret_cast<MemPostInfoMap*>(&mem_post_info_map_)->~MemPostInfoMap();
    }
    if (hint_created_) {
      // 释放 Hint Map 中的所有 Hint 数据
      for (auto iter : GetHintMap()) {
        delete[] reinterpret_cast<char*>(iter.second);
      }
      reinterpret_cast<HintMap*>(&hint_)->~HintMap();
    }
    delete rebuilding_trx_;
  }

  // 禁止拷贝和赋值
  MemTableInserter(const MemTableInserter&) = delete;
  MemTableInserter& operator=(const MemTableInserter&) = delete;

  /**
   * MaybeAdvanceSeq - 根据序列号策略决定是否递增序列号
   *
   * 批次序列号会定期重启：
   * - 正常模式下，在写线程中构造 MemTableInserter 时设置
   * - 恢复模式下，从 WAL 读取带有序列号的批次时设置
   *
   * 在序列化的批次中（可能是多个批次的合并），有两种递增序列号的策略：
   *   i) seq_per_key (默认): 每个键递增一次序列号
   *  ii) seq_per_batch: 每个批次递增一次序列号
   *
   * 实现后者需要标记单个批次之间的边界：
   *   1) 使用终止标记指示边界（kTypeEndPrepareXID, kTypeCommitXID, kTypeRollbackXID）
   *   2) 在没有自然边界标记的情况下，用 kTypeNoop 终止批次
   *
   * @param batch_boundry 是否是批次边界标记
   */
  void MaybeAdvanceSeq(bool batch_boundry = false) {
    if (batch_boundry == seq_per_batch_) {
      sequence_++;
    }
  }

  /**
   * 设置日志引用号
   *
   * 在 WriteCommitted 模式下提交事务时，需要引用包含该事务的 WAL 日志号，
   * 以便后续日志回收时正确处理依赖关系。
   *
   * @param log 日志号
   */
  void set_log_number_ref(uint64_t log) { log_number_ref_ = log; }

  /**
   * 设置保护信息
   *
   * 用于并发写入时的数据完整性校验。
   * 同时重置索引以便从头开始遍历。
   *
   * @param prot_info 保护信息指针
   */
  void set_prot_info(const WriteBatch::ProtectionInfo* prot_info) {
    prot_info_ = prot_info;
    prot_info_idx_ = 0;
  }

  /**
   * 获取当前序列号
   *
   * @return 当前序列号
   */
  SequenceNumber sequence() const { return sequence_; }

  /**
   * PostProcess - 执行 MemTable 后处理（并发写入模式）
   *
   * 在并发写入模式下，插入操作不会立即更新 MemTable 的统计信息，
   * 而是延迟到所有插入完成后批量处理，以减少锁竞争。
   *
   * 后处理包括：
   * - 更新插入计数统计
   * - 更新内存使用统计
   * - 触发相关回调
   *
   * @pre concurrent_memtable_writes_ 必须为 true
   */
  void PostProcess() {
    assert(concurrent_memtable_writes_);
    // 如果 post_info 未创建，则无需处理，也不需要按需创建
    if (post_info_created_) {
      for (auto& pair : GetPostMap()) {
        pair.first->BatchPostProcess(pair.second);
      }
    }
  }

  bool SeekToColumnFamily(uint32_t column_family_id, Status* s) {
    // If we are in a concurrent mode, it is the caller's responsibility
    // to clone the original ColumnFamilyMemTables so that each thread
    // has its own instance.  Otherwise, it must be guaranteed that there
    // is no concurrent access
    bool found = cf_mems_->Seek(column_family_id);
    if (!found) {
      if (ignore_missing_column_families_) {
        *s = Status::OK();
      } else {
        *s = Status::InvalidArgument(
            "Invalid column family specified in write batch");
      }
      return false;
    }
    if (recovering_log_number_ != 0 &&
        recovering_log_number_ < cf_mems_->GetLogNumber()) {
      // This is true only in recovery environment (recovering_log_number_ is
      // always 0 in
      // non-recovery, regular write code-path)
      // * If recovering_log_number_ < cf_mems_->GetLogNumber(), this means that
      // column family already contains updates from this log. We can't apply
      // updates twice because of update-in-place or merge workloads -- ignore
      // the update
      *s = Status::OK();
      return false;
    }

    if (has_valid_writes_ != nullptr) {
      *has_valid_writes_ = true;
    }

    if (log_number_ref_ > 0) {
      cf_mems_->GetMemTable()->RefLogContainingPrepSection(log_number_ref_);
    }

    return true;
  }

  /**
   * PutCFImpl - Put 操作的核心实现
   *
   * 功能概述：
   *   将键值对插入到指定列族的 MemTable 中
   *   支持三种插入模式：普通插入、原地更新（Inplace Update）、原地更新回调
   *
   * 执行流程：
   *
   *   阶段 1：WriteCommitted 模式下的恢复处理
   *   - 如果 write_after_commit_ && rebuilding_trx_ 非空
   *   - 将操作记录到 rebuilding_trx_ 中（不立即插入 MemTable）
   *   - 优化：避免在恢复阶段插入可能被回滚的数据
   *
   *   阶段 2：查找列族
   *   - 调用 SeekToColumnFamily 查找并准备列族
   *   - 处理列族不存在或已恢复的情况
   *   - 如果列族已刷新，且在恢复模式：
   *     * 记录到 rebuilding_trx_ 中
   *     * 检查重复键序列号
   *     * 递增序列号
   *
   *   阶段 3：插入到 MemTable
   *   根据 MemTable 选项选择插入方式：
   *
   *   模式 A：普通插入（inplace_update_support = false）
   *   - 调用 mem->Add() 插入键值对
   *   - 传递并发写入标志和后处理信息
   *   - 传递 Hint 优化插入性能（如果启用）
   *
   *   模式 B：简单原地更新（inplace_callback = nullptr 或 value_type != kTypeValue）
   *   - 调用 mem->Update() 尝试原地更新
   *   - 仅适用于非 Put 操作（Delete、Merge 等）
   *
   *   模式 C：原地更新回调（inplace_callback 非空 且 value_type == kTypeValue）
   *   - 尝试在 MemTable 中原地更新值
   *   - 如果键不存在：
   *     1) 从 SST 文件读取旧值（ReadOptions 指定快照）
   *     2) 调用 inplace_callback 合并新旧值
   *     3) 根据 UpdateStatus 决定如何处理：
   *        - UPDATED_INPLACE：直接在 MemTable 中更新值
   *          * 更新保护信息中的值
   *          * 重新插入更新后的值
   *        - UPDATED：创建新值后插入
   *          * 更新保护信息
   *          * 插入合并后的新值
   *        - UNCHANGED：值未改变，跳过
   *
   *   阶段 4：序列号管理和 Flush 检查
   *   - 如果返回 TryAgain：
   *     * 标记批次边界（seq_per_batch_ 模式）
   *   - 如果返回 OK：
   *     * 递增序列号
   *     * 检查 MemTable 是否需要 Flush
   *   - 如果在恢复模式且操作成功：
   *     * 将操作记录到 rebuilding_trx_ 中
   *
   * 设计要点：
   *   - seq_per_batch 与 inplace_update_support 不兼容（断言检查）
   *   - 原地更新不支持并发写入
   *   - 原地更新回调在恢复时禁用（避免死锁：DB mutex 已持有）
   *   - TryAgain 机制支持延迟插入（等待锁或事务状态）
   *
   * @param column_family_id 列族 ID
   * @param key 键
   * @param value 值
   * @param value_type 值类型（kTypeValue、kTypeWideColumnEntity 等）
   * @param kv_prot_info 保护信息（并发写入时使用）
   * @return Status 操作状态
   */
  Status PutCFImpl(uint32_t column_family_id, const Slice& key,
                   const Slice& value, ValueType value_type,
                   const ProtectionInfoKVOS64* kv_prot_info) {
    // 优化非恢复模式：如果是在提交后写入且正在重建事务，直接写入重建的 WriteBatch
    // 这样可以避免不必要的 MemTable 操作
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      // TODO(ajkr): 需要传递 ProtectionInfoKVOS64
      // 直接将操作添加到重建的 WriteBatch 中，不写入 MemTable
      return WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key,
                                     value);
      // 否则直接将值插入 MemTable
    }

    // 声明返回状态变量
    Status ret_status;

    // 尝试定位到指定的列族
    // 如果无法定位到列族（例如列族已被 flush），进入此分支
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &ret_status))) {
      // 如果操作成功但正在重建事务
      if (ret_status.ok() && rebuilding_trx_ != nullptr) {
        // 断言：重建事务时不允许在提交后写入
        assert(!write_after_commit_);

        // 列族可能已经被 flush，因此不需要插入到 MemTable
        // 但仍然需要跟踪这些 key 以便后续的 rollback/commit 操作
        // TODO(ajkr): 需要传递 ProtectionInfoKVOS64
        ret_status = WriteBatchInternal::Put(rebuilding_trx_, column_family_id,
                                             key, value);
        // 如果写入成功
        if (ret_status.ok()) {
          // 推进序列号，如果是重复 key 需要特殊处理
          MaybeAdvanceSeq(IsDuplicateKeySeq(column_family_id, key));
        }
      } else if (ret_status.ok()) {
        // 如果操作成功但不是在重建事务，正常推进序列号
        // batch_boundary 设为 false，表示这不是一个 batch 边界
        MaybeAdvanceSeq(false /* batch_boundary */);
      }
      // 返回操作状态
      return ret_status;
    }
    // 断言：此时应该成功定位到列族，状态为 OK
    assert(ret_status.ok());

    // 获取当前列族的活动 MemTable 指针
    MemTable* mem = cf_mems_->GetMemTable();

    // 获取 MemTable 的不可变配置选项（包含各种配置参数）
    auto* moptions = mem->GetImmutableMemTableOptions();

    // 断言检查：inplace_update_support（原地更新支持）与快照和事务不兼容
    // 因此 seq_per_batch_ 模式（每个 batch 共享一个序列号）不能与 inplace_update_support 同时开启
    assert(!seq_per_batch_ || !moptions->inplace_update_support);

    // 分支1：如果不支持原地更新（普通模式）
    if (!moptions->inplace_update_support) {
      // 直接调用 MemTable::Add 添加新的键值对
      // 参数说明：序列号、值类型、key、value、并发写入保护信息、
      //           是否并发写入标志、后处理信息、hint 优化指针
      ret_status =
          mem->Add(sequence_, value_type, key, value, kv_prot_info,
                   concurrent_memtable_writes_, get_post_process_info(mem),
                   hint_per_batch_ ? &GetHintMap()[mem] : nullptr);
    } else if (moptions->inplace_callback == nullptr ||
               value_type != kTypeValue) {
      // 分支2：如果支持原地更新，但未设置回调函数或值类型不是 kTypeValue
      // 这种情况下只能进行简单更新（不支持复杂的原地合并逻辑）

      // 断言：简单更新模式不支持并发写入
      assert(!concurrent_memtable_writes_);

      // 调用 MemTable::Update 进行原地更新
      // 该方法会尝试更新已存在的 key-value，如果 key 不存在则插入新值
      ret_status = mem->Update(sequence_, value_type, key, value, kv_prot_info);
    } else {
      // 分支3：支持原地更新、有回调函数、且值类型为 kTypeValue
      // 这是最完整的原地更新模式，支持通过回调函数进行复杂的合并逻辑

      // 断言：回调模式也不支持并发写入
      assert(!concurrent_memtable_writes_);

      // 断言：确保值类型必须是 kTypeValue（普通 Put 操作）
      assert(value_type == kTypeValue);

      // 首先尝试调用 MemTable::UpdateCallback，通过回调函数进行原地更新
      // 该方法会查找 MemTable 中已存在的 key，如果找到则调用回调进行更新
      ret_status = mem->UpdateCallback(sequence_, key, value, kv_prot_info);

      // 如果 MemTable 中找不到该 key（返回 NotFound）
      if (ret_status.IsNotFound()) {
        // 需要从 SST 文件中读取旧值，然后通过回调合并后再插入
        // 下面是实现这个逻辑的代码

        // 创建一个快照对象，用于读取操作
        // 这个快照的序列号设为当前写入序列号，确保读到的是最新的旧值
        SnapshotImpl read_from_snapshot;

        // 设置快照的序列号为当前操作的序列号
        read_from_snapshot.number_ = sequence_;

        // TODO: 需要传递 IOActivity 参数以支持 IO 监控
        // 创建读取选项对象
        ReadOptions ropts;

        // 优化：因为数据肯定会被覆盖，所以不需要缓存包含旧版本的数据块
        // 这样可以避免污染缓存，节省缓存空间
        ropts.fill_cache = false;

        // 设置读取快照，确保读取操作看到的是指定序列号之前的快照
        ropts.snapshot = &read_from_snapshot;

        // 用于存储从 SST 文件中读取的旧值
        std::string prev_value;

        // 用于存储通过回调合并后的新值
        std::string merged_value;

        // 获取列族句柄（用于后续的 Get 操作）
        auto cf_handle = cf_mems_->GetColumnFamilyHandle();

        // 初始化 get_status 为 NotSupported，表示默认不支持从 SST 读取
        Status get_status = Status::NotSupported();

        // 检查 db 指针不为空，并且不是在恢复 WAL 日志的过程中
        // 只有在正常写入模式下（非恢复模式）才能从数据库读取
        if (db_ != nullptr && recovering_log_number_ == 0) {
          // 如果列族句柄为空，使用默认列族
          if (cf_handle == nullptr) {
            cf_handle = db_->DefaultColumnFamily();
          }
          // TODO (yanqin): 需要修复用户定义时间戳的情况
          // 从数据库中读取 key 的旧值，存入 prev_value
          get_status = db_->Get(ropts, cf_handle, key, &prev_value);
        }

        // 如果读取失败且不是因为 key 不存在，则将错误状态传递给 ret_status
        // 如果是 key 不存在（IsNotFound），这实际上是预期的情况，不算错误
        if (!get_status.ok() && !get_status.IsNotFound()) {
          ret_status = get_status;
        } else {
          // 读取成功或 key 不存在都是可以接受的情况，重置为 OK
          ret_status = Status::OK();
        }

        // 如果前一步操作成功，则继续执行回调更新逻辑
        if (ret_status.ok()) {
          // 用于存储回调函数的返回状态（是否原地更新）
          UpdateStatus update_status;

          // 获取 prev_value 的可修改缓冲区指针
          // 因为回调函数可能会原地修改 buffer，所以需要 const_cast
          char* prev_buffer = const_cast<char*>(prev_value.c_str());

          // 获取 prev_value 的长度（转换为 uint32_t）
          uint32_t prev_size = static_cast<uint32_t>(prev_value.size());

          // 如果从数据库中成功读取到旧值（get_status.ok()）
          if (get_status.ok()) {
            // 调用 inplace_callback，传入旧值缓冲区和大小，以及新值
            // 回调函数会尝试在原 buffer 中更新，并将合并结果放在 merged_value
            update_status = moptions->inplace_callback(prev_buffer, &prev_size,
                                                       value, &merged_value);
          } else {
            // 如果旧值不存在（第一次写入该 key）
            // 调用回调函数时传入 nullptr 表示没有旧值
            update_status = moptions->inplace_callback(
                nullptr /* existing_value */, nullptr /* existing_value_size */,
                value, &merged_value);
          }

          // 情况1：回调函数返回 UPDATED_INPLACE，表示成功在原 buffer 中更新
          if (update_status == UpdateStatus::UPDATED_INPLACE) {
            // 断言：原地更新成功意味着一定存在旧值
            assert(get_status.ok());

            // 如果存在并发写入保护信息
            if (kv_prot_info != nullptr) {
              // 复制一份保护信息（因为要更新值部分）
              ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);

              // 更新保护信息中的值，从新值更新为原地更新后的值
              updated_kv_prot_info.UpdateV(value,
                                           Slice(prev_buffer, prev_size));

              // prev_value 已经被原地更新为最终值，将其添加到 MemTable
              ret_status = mem->Add(sequence_, value_type, key,
                                    Slice(prev_buffer, prev_size),
                                    &updated_kv_prot_info);
            } else {
              // 没有保护信息的情况，直接添加原地更新后的值
              ret_status = mem->Add(sequence_, value_type, key,
                                    Slice(prev_buffer, prev_size),
                                    nullptr /* kv_prot_info */);
            }

            // 如果添加成功，记录写入的 key 数量统计
            if (ret_status.ok()) {
              RecordTick(moptions->statistics, NUMBER_KEYS_WRITTEN);
            }
          } else if (update_status == UpdateStatus::UPDATED) {
            // 情况2：回调函数返回 UPDATED，表示成功合并，但产生了新值
            // 新值存储在 merged_value 中

            // 如果存在并发写入保护信息
            if (kv_prot_info != nullptr) {
              // 复制一份保护信息
              ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);

              // 更新保护信息中的值，从新值更新为合并后的值
              updated_kv_prot_info.UpdateV(value, merged_value);

              // merged_value 包含最终值，将其添加到 MemTable
              ret_status = mem->Add(sequence_, value_type, key,
                                    Slice(merged_value), &updated_kv_prot_info);
            } else {
              // 没有保护信息的情况，直接添加合并后的值
              // merged_value 包含最终值
              ret_status =
                  mem->Add(sequence_, value_type, key, Slice(merged_value),
                           nullptr /* kv_prot_info */);
            }

            // 如果添加成功，记录写入的 key 数量统计
            if (ret_status.ok()) {
              RecordTick(moptions->statistics, NUMBER_KEYS_WRITTEN);
            }
          }
        }
      }
    }

    // 处理 TryAgain 状态（UNLIKELY 表示这种情况不太常见）
    if (UNLIKELY(ret_status.IsTryAgain())) {
      // 断言：TryAgain 只在 seq_per_batch_ 模式下发生
      assert(seq_per_batch_);

      // 标记这是一个 batch 边界
      // TryAgain 时需要跳过整个 batch，所以传入 true
      const bool kBatchBoundary = true;

      // 尝试推进序列号到下一个 batch 的起始位置
      MaybeAdvanceSeq(kBatchBoundary);
    } else if (ret_status.ok()) {
      // 如果操作成功，正常处理
      // 推进序列号到下一个 key
      MaybeAdvanceSeq();

      // 检查 MemTable 是否已满，如果满了需要触发 flush
      CheckMemtableFull();
    }

    // 优化：针对非恢复模式
    // 如果 ret_status 是 TryAgain，那么下一次成功的尝试会添加到 rebuilding_trx_ 对象
    // 如果 ret_status 是其他非 OK 状态，rebuilding_trx_ 会被丢弃
    // 因此只有当 ret_status.ok() 时才需要添加到 rebuilding_trx_

    // rebuilding_trx_ 不为空表示正在进行事务恢复（从 WAL 重建事务）
    // 这是一个不常见的情况，使用 UNLIKELY 标记
    if (UNLIKELY(ret_status.ok() && rebuilding_trx_ != nullptr)) {
      // 断言：重建事务时不允许在提交后写入
      assert(!write_after_commit_);

      // TODO(ajkr): 需要传递 ProtectionInfoKVOS64
      // 将当前的操作添加到重建的 WriteBatch 中
      // 这样可以在恢复完成后完整地重建事务
      ret_status = WriteBatchInternal::Put(rebuilding_trx_, column_family_id,
                                           key, value);
    }

    // 返回操作状态
    return ret_status;
  }

  /**
   * PutCF - 处理 Put 操作（写入键值对）
   *
   * 功能概述：
   *   - WriteBatch::Handler 的 PutCF 接口实现
   *   - 处理带列族 ID 的 Put 操作
   *   - 调用 PutCFImpl 执行实际的插入逻辑
   *
   * 执行流程：
   *
   *   步骤 1：获取保护信息
   *   - 调用 NextProtectionInfo() 获取下一个保护信息条目
   *   - 保护信息用于并发写入时的数据完整性校验
   *
   *   步骤 2：准备 MemTable 保护信息
   *   - 如果存在保护信息（kv_prot_info != nullptr）：
   *     1) 调用 StripC(column_family_id) 移除列族 ID
   *        （MemTable 不需要列族 ID，因为已经定位到具体 MemTable）
   *     2) 调用 ProtectS(sequence_) 添加序列号保护
   *   - 如果不存在保护信息，传递 nullptr 给 PutCFImpl
   *
   *   步骤 3：执行 Put 操作
   *   - 调用 PutCFImpl(column_family_id, key, value, kTypeValue, prot_info)
   *   - kTypeValue 表示这是标准的 Put 操作
   *
   *   步骤 4：处理 TryAgain 状态
   *   - 如果返回 TryAgain：
   *     * 回退保护信息索引（DecrementProtectionInfoIdxForTryAgain()）
   *     * 这使得下次重试时能使用相同的保护信息
   *   - TryAgain 的可能原因：
   *     * seq_per_batch_ 模式下，等待批次边界
   *     * MemTable 满或需要等待条件
   *     * 并发冲突需要重试
   *
   * 设计要点：
   *   - MemTable 保护信息转换：
   *     * StripC: 从 ProtectionInfoKVOS64 转为 ProtectionInfoKVOS64
   *       （移除列族 ID，减少存储开销）
   *     * ProtectS: 添加序列号保护
   *       （序列号是 MemTable 的关键信息，需要保护）
   *
   *   - TryAgain 处理：
   *     * TODO 注释指出当前实现假设调用者会实际重试
   *     * 正确的做法是传递 try_again 参数给操作本身
   *     * 但当前简化实现通过回退索引实现
   *
   *   - 性能优化：
   *     * 保护信息为 nullptr 时（非并发模式），避免不必要的转换
   *     * 内联处理常见路径
   *
   * @param column_family_id 列族 ID（0 表示默认列族）
   * @param key 键（可能包含时间戳）
   * @param value 值
   * @return Status 操作状态
   *   - OK: 插入成功
   *   - TryAgain: 需要重试（延迟插入）
   *   - 其他错误: 插入失败
   */
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    // 获取下一个保护信息条目（用于并发写入的数据完整性校验）
    const auto* kv_prot_info = NextProtectionInfo();
    Status ret_status;
    if (kv_prot_info != nullptr) {
      // MemTable 需要序列号，不需要列族 ID
      // StripC: 移除列族 ID（因为已经定位到具体的 MemTable）
      // ProtectS: 添加序列号保护
      auto mem_kv_prot_info =
          kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
      ret_status = PutCFImpl(column_family_id, key, value, kTypeValue,
                             &mem_kv_prot_info);
    } else {
      // 没有保护信息（非并发模式）
      ret_status = PutCFImpl(column_family_id, key, value, kTypeValue,
                             nullptr /* kv_prot_info */);
    }
    // TODO: 这假设如果返回 TryAgain 状态给调用者，操作实际上会重试。
    // 正确的做法是传递一个 `try_again` 参数给操作本身，并根据该
    // 参数递减 prot_info_idx_
    if (UNLIKELY(ret_status.IsTryAgain())) {
      // 回退保护信息索引，使得下次重试时使用相同的保护信息
      DecrementProtectionInfoIdxForTryAgain();
    }
    return ret_status;
  }

  Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                     const Slice& value) override {
    const auto* kv_prot_info = NextProtectionInfo();

    Status s;
    if (kv_prot_info) {
      // Memtable needs seqno, doesn't need CF ID
      auto mem_kv_prot_info =
          kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
      s = PutCFImpl(column_family_id, key, value, kTypeWideColumnEntity,
                    &mem_kv_prot_info);
    } else {
      s = PutCFImpl(column_family_id, key, value, kTypeWideColumnEntity,
                    /* kv_prot_info */ nullptr);
    }

    if (UNLIKELY(s.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }

    return s;
  }

  Status DeleteImpl(uint32_t /*column_family_id*/, const Slice& key,
                    const Slice& value, ValueType delete_type,
                    const ProtectionInfoKVOS64* kv_prot_info) {
    Status ret_status;
    MemTable* mem = cf_mems_->GetMemTable();
    ret_status =
        mem->Add(sequence_, delete_type, key, value, kv_prot_info,
                 concurrent_memtable_writes_, get_post_process_info(mem),
                 hint_per_batch_ ? &GetHintMap()[mem] : nullptr);
    if (UNLIKELY(ret_status.IsTryAgain())) {
      assert(seq_per_batch_);
      const bool kBatchBoundary = true;
      MaybeAdvanceSeq(kBatchBoundary);
    } else if (ret_status.ok()) {
      MaybeAdvanceSeq();
      CheckMemtableFull();
    }
    return ret_status;
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    const auto* kv_prot_info = NextProtectionInfo();
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      return WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
      // else insert the values to the memtable right away
    }

    Status ret_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &ret_status))) {
      if (ret_status.ok() && rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
        ret_status =
            WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
        if (ret_status.ok()) {
          MaybeAdvanceSeq(IsDuplicateKeySeq(column_family_id, key));
        }
      } else if (ret_status.ok()) {
        MaybeAdvanceSeq(false /* batch_boundary */);
      }
      if (UNLIKELY(ret_status.IsTryAgain())) {
        DecrementProtectionInfoIdxForTryAgain();
      }
      return ret_status;
    }

    ColumnFamilyData* cfd = cf_mems_->current();
    assert(!cfd || cfd->user_comparator());
    const size_t ts_sz = (cfd && cfd->user_comparator())
                             ? cfd->user_comparator()->timestamp_size()
                             : 0;
    const ValueType delete_type =
        (0 == ts_sz) ? kTypeDeletion : kTypeDeletionWithTimestamp;
    if (kv_prot_info != nullptr) {
      auto mem_kv_prot_info =
          kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
      mem_kv_prot_info.UpdateO(kTypeDeletion, delete_type);
      ret_status = DeleteImpl(column_family_id, key, Slice(), delete_type,
                              &mem_kv_prot_info);
    } else {
      ret_status = DeleteImpl(column_family_id, key, Slice(), delete_type,
                              nullptr /* kv_prot_info */);
    }
    // optimize for non-recovery mode
    // If `ret_status` is `TryAgain` then the next (successful) try will add
    // the key to the rebuilding transaction object. If `ret_status` is
    // another non-OK `Status`, then the `rebuilding_trx_` will be thrown
    // away. So we only need to add to it when `ret_status.ok()`.
    if (UNLIKELY(ret_status.ok() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      ret_status =
          WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
    }
    if (UNLIKELY(ret_status.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }
    return ret_status;
  }

  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override {
    const auto* kv_prot_info = NextProtectionInfo();
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      return WriteBatchInternal::SingleDelete(rebuilding_trx_, column_family_id,
                                              key);
      // else insert the values to the memtable right away
    }

    Status ret_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &ret_status))) {
      if (ret_status.ok() && rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
        ret_status = WriteBatchInternal::SingleDelete(rebuilding_trx_,
                                                      column_family_id, key);
        if (ret_status.ok()) {
          MaybeAdvanceSeq(IsDuplicateKeySeq(column_family_id, key));
        }
      } else if (ret_status.ok()) {
        MaybeAdvanceSeq(false /* batch_boundary */);
      }
      if (UNLIKELY(ret_status.IsTryAgain())) {
        DecrementProtectionInfoIdxForTryAgain();
      }
      return ret_status;
    }
    assert(ret_status.ok());

    if (kv_prot_info != nullptr) {
      auto mem_kv_prot_info =
          kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
      ret_status = DeleteImpl(column_family_id, key, Slice(),
                              kTypeSingleDeletion, &mem_kv_prot_info);
    } else {
      ret_status = DeleteImpl(column_family_id, key, Slice(),
                              kTypeSingleDeletion, nullptr /* kv_prot_info */);
    }
    // optimize for non-recovery mode
    // If `ret_status` is `TryAgain` then the next (successful) try will add
    // the key to the rebuilding transaction object. If `ret_status` is
    // another non-OK `Status`, then the `rebuilding_trx_` will be thrown
    // away. So we only need to add to it when `ret_status.ok()`.
    if (UNLIKELY(ret_status.ok() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      ret_status = WriteBatchInternal::SingleDelete(rebuilding_trx_,
                                                    column_family_id, key);
    }
    if (UNLIKELY(ret_status.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }
    return ret_status;
  }

  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override {
    const auto* kv_prot_info = NextProtectionInfo();
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      return WriteBatchInternal::DeleteRange(rebuilding_trx_, column_family_id,
                                             begin_key, end_key);
      // else insert the values to the memtable right away
    }

    Status ret_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &ret_status))) {
      if (ret_status.ok() && rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
        ret_status = WriteBatchInternal::DeleteRange(
            rebuilding_trx_, column_family_id, begin_key, end_key);
        if (ret_status.ok()) {
          MaybeAdvanceSeq(IsDuplicateKeySeq(column_family_id, begin_key));
        }
      } else if (ret_status.ok()) {
        MaybeAdvanceSeq(false /* batch_boundary */);
      }
      if (UNLIKELY(ret_status.IsTryAgain())) {
        DecrementProtectionInfoIdxForTryAgain();
      }
      return ret_status;
    }
    assert(ret_status.ok());

    if (db_ != nullptr) {
      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      auto* cfd =
          static_cast_with_check<ColumnFamilyHandleImpl>(cf_handle)->cfd();
      if (!cfd->is_delete_range_supported()) {
        // TODO(ajkr): refactor `SeekToColumnFamily()` so it returns a `Status`.
        ret_status.PermitUncheckedError();
        return Status::NotSupported(
            std::string("DeleteRange not supported for table type ") +
            cfd->ioptions()->table_factory->Name() + " in CF " +
            cfd->GetName());
      }
      int cmp =
          cfd->user_comparator()->CompareWithoutTimestamp(begin_key, end_key);
      if (cmp > 0) {
        // TODO(ajkr): refactor `SeekToColumnFamily()` so it returns a `Status`.
        ret_status.PermitUncheckedError();
        // It's an empty range where endpoints appear mistaken. Don't bother
        // applying it to the DB, and return an error to the user.
        return Status::InvalidArgument("end key comes before start key");
      } else if (cmp == 0) {
        // TODO(ajkr): refactor `SeekToColumnFamily()` so it returns a `Status`.
        ret_status.PermitUncheckedError();
        // It's an empty range. Don't bother applying it to the DB.
        return Status::OK();
      }
    }

    if (kv_prot_info != nullptr) {
      auto mem_kv_prot_info =
          kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
      ret_status = DeleteImpl(column_family_id, begin_key, end_key,
                              kTypeRangeDeletion, &mem_kv_prot_info);
    } else {
      ret_status = DeleteImpl(column_family_id, begin_key, end_key,
                              kTypeRangeDeletion, nullptr /* kv_prot_info */);
    }
    // optimize for non-recovery mode
    // If `ret_status` is `TryAgain` then the next (successful) try will add
    // the key to the rebuilding transaction object. If `ret_status` is
    // another non-OK `Status`, then the `rebuilding_trx_` will be thrown
    // away. So we only need to add to it when `ret_status.ok()`.
    if (UNLIKELY(!ret_status.IsTryAgain() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      ret_status = WriteBatchInternal::DeleteRange(
          rebuilding_trx_, column_family_id, begin_key, end_key);
    }
    if (UNLIKELY(ret_status.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }
    return ret_status;
  }

  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
    const auto* kv_prot_info = NextProtectionInfo();
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      return WriteBatchInternal::Merge(rebuilding_trx_, column_family_id, key,
                                       value);
      // else insert the values to the memtable right away
    }

    Status ret_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &ret_status))) {
      if (ret_status.ok() && rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
        ret_status = WriteBatchInternal::Merge(rebuilding_trx_,
                                               column_family_id, key, value);
        if (ret_status.ok()) {
          MaybeAdvanceSeq(IsDuplicateKeySeq(column_family_id, key));
        }
      } else if (ret_status.ok()) {
        MaybeAdvanceSeq(false /* batch_boundary */);
      }
      if (UNLIKELY(ret_status.IsTryAgain())) {
        DecrementProtectionInfoIdxForTryAgain();
      }
      return ret_status;
    }
    assert(ret_status.ok());

    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetImmutableMemTableOptions();
    if (moptions->merge_operator == nullptr) {
      return Status::InvalidArgument(
          "Merge requires `ColumnFamilyOptions::merge_operator != nullptr`");
    }
    bool perform_merge = false;
    assert(!concurrent_memtable_writes_ ||
           moptions->max_successive_merges == 0);

    // If we pass DB through and options.max_successive_merges is hit
    // during recovery, Get() will be issued which will try to acquire
    // DB mutex and cause deadlock, as DB mutex is already held.
    // So we disable merge in recovery
    if (moptions->max_successive_merges > 0 && db_ != nullptr &&
        recovering_log_number_ == 0) {
      assert(!concurrent_memtable_writes_);
      LookupKey lkey(key, sequence_);

      // Count the number of successive merges at the head
      // of the key in the memtable
      size_t num_merges = mem->CountSuccessiveMergeEntries(lkey);

      if (num_merges >= moptions->max_successive_merges) {
        perform_merge = true;
      }
    }

    if (perform_merge) {
      // 1) Get the existing value
      std::string get_value;

      // Pass in the sequence number so that we also include previous merge
      // operations in the same batch.
      SnapshotImpl read_from_snapshot;
      read_from_snapshot.number_ = sequence_;
      // TODO: plumb Env::IOActivity
      ReadOptions read_options;
      read_options.snapshot = &read_from_snapshot;

      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      Status get_status = db_->Get(read_options, cf_handle, key, &get_value);
      if (!get_status.ok()) {
        // Failed to read a key we know exists. Store the delta in memtable.
        perform_merge = false;
      } else {
        Slice get_value_slice = Slice(get_value);

        // 2) Apply this merge
        auto merge_operator = moptions->merge_operator;
        assert(merge_operator);

        std::string new_value;
        // `op_failure_scope` (an output parameter) is not provided (set to
        // nullptr) since a failure must be propagated regardless of its value.
        Status merge_status = MergeHelper::TimedFullMerge(
            merge_operator, key, &get_value_slice, {value}, &new_value,
            moptions->info_log, moptions->statistics,
            SystemClock::Default().get(), /* result_operand */ nullptr,
            /* update_num_ops_stats */ false,
            /* op_failure_scope */ nullptr);

        if (!merge_status.ok()) {
          // Failed to merge!
          // Store the delta in memtable
          perform_merge = false;
        } else {
          // 3) Add value to memtable
          assert(!concurrent_memtable_writes_);
          if (kv_prot_info != nullptr) {
            auto merged_kv_prot_info =
                kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
            merged_kv_prot_info.UpdateV(value, new_value);
            merged_kv_prot_info.UpdateO(kTypeMerge, kTypeValue);
            ret_status = mem->Add(sequence_, kTypeValue, key, new_value,
                                  &merged_kv_prot_info);
          } else {
            ret_status = mem->Add(sequence_, kTypeValue, key, new_value,
                                  nullptr /* kv_prot_info */);
          }
        }
      }
    }

    if (!perform_merge) {
      assert(ret_status.ok());
      // Add merge operand to memtable
      if (kv_prot_info != nullptr) {
        auto mem_kv_prot_info =
            kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
        ret_status =
            mem->Add(sequence_, kTypeMerge, key, value, &mem_kv_prot_info,
                     concurrent_memtable_writes_, get_post_process_info(mem));
      } else {
        ret_status = mem->Add(
            sequence_, kTypeMerge, key, value, nullptr /* kv_prot_info */,
            concurrent_memtable_writes_, get_post_process_info(mem));
      }
    }

    if (UNLIKELY(ret_status.IsTryAgain())) {
      assert(seq_per_batch_);
      const bool kBatchBoundary = true;
      MaybeAdvanceSeq(kBatchBoundary);
    } else if (ret_status.ok()) {
      MaybeAdvanceSeq();
      CheckMemtableFull();
    }
    // optimize for non-recovery mode
    // If `ret_status` is `TryAgain` then the next (successful) try will add
    // the key to the rebuilding transaction object. If `ret_status` is
    // another non-OK `Status`, then the `rebuilding_trx_` will be thrown
    // away. So we only need to add to it when `ret_status.ok()`.
    if (UNLIKELY(ret_status.ok() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // TODO(ajkr): propagate `ProtectionInfoKVOS64`.
      ret_status = WriteBatchInternal::Merge(rebuilding_trx_, column_family_id,
                                             key, value);
    }
    if (UNLIKELY(ret_status.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }
    return ret_status;
  }

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key,
                        const Slice& value) override {
    const auto* kv_prot_info = NextProtectionInfo();
    Status ret_status;
    if (kv_prot_info != nullptr) {
      // Memtable needs seqno, doesn't need CF ID
      auto mem_kv_prot_info =
          kv_prot_info->StripC(column_family_id).ProtectS(sequence_);
      // Same as PutCF except for value type.
      ret_status = PutCFImpl(column_family_id, key, value, kTypeBlobIndex,
                             &mem_kv_prot_info);
    } else {
      ret_status = PutCFImpl(column_family_id, key, value, kTypeBlobIndex,
                             nullptr /* kv_prot_info */);
    }
    if (UNLIKELY(ret_status.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }
    return ret_status;
  }

  /**
   * CheckMemtableFull - 检查 MemTable 是否需要 Flush 或 TrimHistory
   *
   * 功能概述：
   *   1. Flush 检查：
   *      - 如果 MemTable 达到 Flush 条件（ShouldScheduleFlush()）
   *      - 标记 Flush 已调度（MarkFlushScheduled()），防止重复调度
   *      - 通知 Flush 调度器执行 Flush
   *
   *   2. TrimHistory 检查：
   *      - 检查 MemTable 总大小是否超过 max_write_buffer_size_to_maintain
   *      - 包括活跃 MemTable 和不可变 MemTable（除了最后一个）
   *      - 如果超过限制，标记需要 TrimHistory 并调度清理
   *
   * 设计要点：
   *   - MarkFlushScheduled() 是原子操作，只有一个线程会得到 true
   *   - 避免多个线程重复调度同一个列族的 Flush
   *   - TrimHistory 用于清理旧的历史 MemTable，释放内存
   */
  void CheckMemtableFull() {
    if (flush_scheduler_ != nullptr) {
      auto* cfd = cf_mems_->current();
      assert(cfd != nullptr);
      if (cfd->mem()->ShouldScheduleFlush() &&
          cfd->mem()->MarkFlushScheduled()) {
        // MarkFlushScheduled 只有在我们是应该采取行动的线程时才返回 true，
        // 因此无需进一步去重
        flush_scheduler_->ScheduleWork(cfd);
      }
    }
    // 检查 memtable_list 大小是否超过 max_write_buffer_size_to_maintain
    if (trim_history_scheduler_ != nullptr) {
      auto* cfd = cf_mems_->current();

      assert(cfd);
      assert(cfd->ioptions());

      const size_t size_to_maintain = static_cast<size_t>(
          cfd->ioptions()->max_write_buffer_size_to_maintain);

      if (size_to_maintain > 0) {
        MemTableList* const imm = cfd->imm();
        assert(imm);

        if (imm->HasHistory()) {
          const MemTable* const mem = cfd->mem();
          assert(mem);

          // 如果总内存（活跃 mem + 不可变 mem）超过限制，调度 TrimHistory
          if (mem->MemoryAllocatedBytes() +
                      imm->MemoryAllocatedBytesExcludingLast() >=
                  size_to_maintain &&
              imm->MarkTrimHistoryNeeded()) {
            trim_history_scheduler_->ScheduleWork(cfd);
          }
        }
      }
    }
  }

  /**
   * MarkBeginPrepare - 标记事务准备阶段的开始
   *
   * 功能概述：
   *   - 在 WAL 恢复模式下，开始遇到准备阶段的事务
   *   - 创建一个空的 WriteBatch 用于收集准备阶段的操作
   *   - 记录起始序列号以便后续重建事务
   *
   * 调用时机：
   *   - WriteBatch 遇到 kTypeBeginPrepareXID 标记时
   *   - WriteBatch 遇到 kTypeBeginPersistedPrepareXID 标记时
   *   - WriteBatch 遇到 kTypeBeginUnprepareXID 标记时（unprepare=true）
   *
   * 设计要点：
   *   - 只在恢复模式（recovering_log_number_ != 0）下处理
   *   - 检查数据库是否支持 2PC（双阶段提交）
   *   - 验证 BeginPrepare/EndPrepare 标记匹配
   *   - unprepared_batch_ 标记是否是 WriteUnprepared 模式
   *
   * @param unprepare 是否是未准备批次（WriteUnprepared 模式）
   * @return Status 操作状态
   */
  // The write batch handler calls MarkBeginPrepare with unprepare set to true
  // if it encounters the kTypeBeginUnprepareXID marker.
  Status MarkBeginPrepare(bool unprepare) override {
    assert(rebuilding_trx_ == nullptr);
    assert(db_);

    if (recovering_log_number_ != 0) {
      db_->mutex()->AssertHeld();
      // during recovery we rebuild a hollow transaction
      // from all encountered prepare sections of the wal
      if (db_->allow_2pc() == false) {
        return Status::NotSupported(
            "WAL contains prepared transactions. Open with "
            "TransactionDB::Open().");
      }

      // we are now iterating through a prepared section
      rebuilding_trx_ = new WriteBatch();
      rebuilding_trx_seq_ = sequence_;
      // Verify that we have matching MarkBeginPrepare/MarkEndPrepare markers.
      // unprepared_batch_ should be false because it is false by default, and
      // gets reset to false in MarkEndPrepare.
      assert(!unprepared_batch_);
      unprepared_batch_ = unprepare;

      if (has_valid_writes_ != nullptr) {
        *has_valid_writes_ = true;
      }
    }

    return Status::OK();
  }

  /**
   * MarkEndPrepare - 标记事务准备阶段的结束
   *
   * 功能概述：
   *   - 在 WAL 恢复模式下，完成事务准备阶段的重建
   *   - 将重建的事务插入到恢复事务集合中
   *   - 清理临时状态，重置 rebuilding_trx_
   *
   * 处理逻辑：
   *   - 计算批次数量：如果 write_after_commit_ 为 true，批次数为 0（禁用后续检查）
   *   - 否则批次数 = 当前序列号 - 起始序列号 + 1
   *   - 调用 InsertRecoveredTransaction 插入到恢复事务集合
   *   - 递增序列号（标记批次边界）
   *
   * @param name 事务名称（XID）
   * @return Status 操作状态
   */
  Status MarkEndPrepare(const Slice& name) override {
    assert(db_);
    assert((rebuilding_trx_ != nullptr) == (recovering_log_number_ != 0));

    if (recovering_log_number_ != 0) {
      db_->mutex()->AssertHeld();
      assert(db_->allow_2pc());
      // 计算批次数量
      size_t batch_cnt =
          write_after_commit_
              ? 0  // 0 将禁用后续检查
              : static_cast<size_t>(sequence_ - rebuilding_trx_seq_ + 1);
      db_->InsertRecoveredTransaction(recovering_log_number_, name.ToString(),
                                      rebuilding_trx_, rebuilding_trx_seq_,
                                      batch_cnt, unprepared_batch_);
      unprepared_batch_ = false;
      rebuilding_trx_ = nullptr;
    } else {
      assert(rebuilding_trx_ == nullptr);
    }
    const bool batch_boundry = true;
    MaybeAdvanceSeq(batch_boundry);

    return Status::OK();
  }

  /**
   * MarkNoop - 处理空操作标记
   *
   * 功能概述：
   *   - Noop 用于分隔批次或标记批次的边界
   *   - 在没有 Prepare 标记的情况下，Noop 表示批次的结束
   *
   * 处理逻辑：
   *   - 如果 empty_batch 为 false，说明 Noop 标记了批次边界
   *   - 递增序列号（如果 seq_per_batch_ 为 true）
   *   - 如果 empty_batch 为 true，说明这是批次开头的 Noop，需要忽略
   *
   * @param empty_batch 批次是否为空（如果为 true，忽略此 Noop）
   * @return Status 操作状态
   */
  Status MarkNoop(bool empty_batch) override {
    if (recovering_log_number_ != 0) {
      db_->mutex()->AssertHeld();
    }
    // A hack in pessimistic transaction could result into a noop at the start
    // of the write batch, that should be ignored.
    if (!empty_batch) {
      // In the absence of Prepare markers, a kTypeNoop tag indicates the end of
      // a batch. This happens when write batch commits skipping the prepare
      // phase.
      const bool batch_boundry = true;
      MaybeAdvanceSeq(batch_boundry);
    }
    return Status::OK();
  }

  /**
   * MarkCommit - 标记事务提交
   *
   * 功能概述：
   *   - 在 WAL 恢复模式下，处理已恢复事务的提交
   *   - 对于 WriteCommitted 模式，此时才将事务数据插入 MemTable
   *   - 对于 WritePrepared/WriteUnprepared 模式，数据已在 Prepare 阶段插入，只需清理
   *
   * 处理逻辑：
   *   1. 恢复模式下（recovering_log_number_ != 0）：
   *      - 从恢复事务集合中查找该事务
   *      - 如果找到：
   *        * WriteCommitted 模式：遍历事务批次，插入到 MemTable
   *          - 设置 log_number_ref_ 引用事务日志号
   *          - 重置保护信息
   *          - 遍历批次插入数据
   *          - 清理 log_number_ref_
   *        * 其他模式：数据已插入，只需清理
   *      - 删除已恢复的事务记录
   *
   *   2. 非恢复模式下：
   *      - 断言 write_after_commit_ 和 log_number_ref_ 的关系
   *      - 如果 writes 不延迟到提交，提交无需引用任何日志
   *
   * @param name 事务名称（XID）
   * @return Status 操作状态
   */
  Status MarkCommit(const Slice& name) override {
    assert(db_);

    Status s;

    if (recovering_log_number_ != 0) {
      // We must hold db mutex in recovery.
      db_->mutex()->AssertHeld();
      // in recovery when we encounter a commit marker
      // we lookup this transaction in our set of rebuilt transactions
      // and commit.
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log containing the prepared section may have
      // been released in the last incarnation because the
      // data was flushed to L0
      if (trx != nullptr) {
        // at this point individual CF lognumbers will prevent
        // duplicate re-insertion of values.
        assert(log_number_ref_ == 0);
        if (write_after_commit_) {
          // write_after_commit_ can only have one batch in trx.
          assert(trx->batches_.size() == 1);
          const auto& batch_info = trx->batches_.begin()->second;
          // all inserts must reference this trx log number
          log_number_ref_ = batch_info.log_number_;
          ResetProtectionInfo();
          s = batch_info.batch_->Iterate(this);
          log_number_ref_ = 0;
        }
        // else the values are already inserted before the commit

        if (s.ok()) {
          db_->DeleteRecoveredTransaction(name.ToString());
        }
        if (has_valid_writes_ != nullptr) {
          *has_valid_writes_ = true;
        }
      }
    } else {
      // When writes are not delayed until commit, there is no disconnect
      // between a memtable write and the WAL that supports it. So the commit
      // need not reference any log as the only log to which it depends.
      assert(!write_after_commit_ || log_number_ref_ > 0);
    }
    const bool batch_boundry = true;
    MaybeAdvanceSeq(batch_boundry);

    if (UNLIKELY(s.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }

    return s;
  }

  /**
   * MarkCommitWithTimestamp - 标记带时间戳的事务提交
   *
   * 功能概述：
   *   - 与 MarkCommit 类似，但支持用户定义的时间戳
   *   - 需要更新 WriteBatch 中所有键的时间戳
   *   - 时间戳用于支持用户定义的时间戳功能
   *
   * 处理逻辑：
   *   1. 恢复模式下：
   *      - 从恢复事务集合中查找该事务
   *      - WriteCommitted 模式：
   *        * 调用 UpdateTimestamps 更新所有键的时间戳
   *        * 遍历批次插入数据
   *      * 删除已恢复的事务记录
   *
   *   2. 时间戳更新：
   *      - 遍历 WriteBatch 中的每个键
   *      - 根据列族的时间戳大小更新键的后缀
   *      - 调用 ucmp->timestamp_size() 获取时间戳大小
   *
   * @param name 事务名称（XID）
   * @param commit_ts 提交时间戳
   * @return Status 操作状态
   */
  Status MarkCommitWithTimestamp(const Slice& name,
                                 const Slice& commit_ts) override {
    assert(db_);

    Status s;

    if (recovering_log_number_ != 0) {
      // In recovery, db mutex must be held.
      db_->mutex()->AssertHeld();
      // in recovery when we encounter a commit marker
      // we lookup this transaction in our set of rebuilt transactions
      // and commit.
      auto trx = db_->GetRecoveredTransaction(name.ToString());
      // the log containing the prepared section may have
      // been released in the last incarnation because the
      // data was flushed to L0
      if (trx) {
        // at this point individual CF lognumbers will prevent
        // duplicate re-insertion of values.
        assert(0 == log_number_ref_);
        if (write_after_commit_) {
          // write_after_commit_ can only have one batch in trx.
          assert(trx->batches_.size() == 1);
          const auto& batch_info = trx->batches_.begin()->second;
          // all inserts must reference this trx log number
          log_number_ref_ = batch_info.log_number_;

          s = batch_info.batch_->UpdateTimestamps(
              commit_ts, [this](uint32_t cf) {
                assert(db_);
                VersionSet* const vset = db_->GetVersionSet();
                assert(vset);
                ColumnFamilySet* const cf_set = vset->GetColumnFamilySet();
                assert(cf_set);
                ColumnFamilyData* cfd = cf_set->GetColumnFamily(cf);
                assert(cfd);
                const auto* const ucmp = cfd->user_comparator();
                assert(ucmp);
                return ucmp->timestamp_size();
              });
          if (s.ok()) {
            ResetProtectionInfo();
            s = batch_info.batch_->Iterate(this);
            log_number_ref_ = 0;
          }
        }
        // else the values are already inserted before the commit

        if (s.ok()) {
          db_->DeleteRecoveredTransaction(name.ToString());
        }
        if (has_valid_writes_) {
          *has_valid_writes_ = true;
        }
      }
    } else {
      // When writes are not delayed until commit, there is no connection
      // between a memtable write and the WAL that supports it. So the commit
      // need not reference any log as the only log to which it depends.
      assert(!write_after_commit_ || log_number_ref_ > 0);
    }
    constexpr bool batch_boundary = true;
    MaybeAdvanceSeq(batch_boundary);

    if (UNLIKELY(s.IsTryAgain())) {
      DecrementProtectionInfoIdxForTryAgain();
    }

    return s;
  }

  /**
   * MarkRollback - 标记事务回滚
   *
   * 功能概述：
   *   - 在 WAL 恢复模式下，处理已恢复事务的回滚
   *   - 删除恢复事务集合中的该事务记录
   *   - 不需要插入任何数据到 MemTable
   *
   * 处理逻辑：
   *   1. 恢复模式下：
   *      - 从恢复事务集合中查找该事务
   *      - 如果找到，删除该事务记录
   *      - 注意：如果包含准备部分的日志在之前的生命周期中已释放
   *        （因为已知已回滚），则找不到事务
   *
   *   2. 非恢复模式下：
   *      - 忽略此标记（正常写入时不需要处理回滚）
   *
   * @param name 事务名称（XID）
   * @return Status 操作状态
   */
  Status MarkRollback(const Slice& name) override {
    assert(db_);

    if (recovering_log_number_ != 0) {
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // 包含事务准备部分的日志可能在之前的生命周期中已释放，
      // 因为我们已知其已回滚
      if (trx != nullptr) {
        db_->DeleteRecoveredTransaction(name.ToString());
      }
    } else {
      // 非恢复模式下我们简单地忽略此标记
    }

    const bool batch_boundry = true;
    MaybeAdvanceSeq(batch_boundry);

    return Status::OK();
  }

 private:
  /**
   * get_post_process_info - 获取 MemTable 后处理信息
   *
   * 功能概述：
   *   - 在并发模式下，为每个 MemTable 提供后处理信息存储位置
   *   - 延迟更新统计信息以减少锁竞争
   *
   * 处理逻辑：
   *   - 非并发模式：返回 nullptr（无需批量计数器）
   *   - 并发模式：返回 PostMap 中该 MemTable 的信息
   *
   * @param mem MemTable 指针
   * @return MemTablePostProcessInfo 指针，非并发模式返回 nullptr
   */
  MemTablePostProcessInfo* get_post_process_info(MemTable* mem) {
    if (!concurrent_memtable_writes_) {
      // 如果不使用并发模式，无需本地批量计数器
      return nullptr;
    }
    return &GetPostMap()[mem];
  }
};

}  // anonymous namespace

// This function can only be called in these conditions:
/**
 * @brief 将写入组中的所有 WriteBatch 插入到 memtables（重载版本 1：WriteGroup）
 *
 * 该函数用于处理写入组（WriteGroup），这是 RocksDB 优化写入性能的关键机制。
 * 写入组是多个写操作的集合，可以批量处理以提高吞吐量。
 *
 * @param write_group 写入组，包含多个 Writer 对象
 * @param sequence 起始序列号
 * @param memtables 列族 memtable 映射（CFID -> MemTable）
 * @param flush_scheduler flush 调度器，用于在 memtable 满时触发 flush
 * @param trim_history_scheduler memtable 历史清理调度器
 * @param ignore_missing_column_families 是否忽略不存在的列族
 * @param recovery_log_number 恢复时的 WAL 日志号（0 表示非恢复模式）
 * @param db 数据库实例引用
 * @param concurrent_memtable_writes 是否在并发写入模式下（memtables 已被克隆）
 * @param seq_per_batch 是否每个 WriteBatch 使用一个序列号
 * @param batch_per_txn 是否每个事务一个 WriteBatch（用于序列号分配策略）
 *
 * @return Status 操作状态，OK 表示成功插入
 *
 * 调用时机：
 * 1) Recovery()：数据库恢复期间
 * 2) Write()：单线程写入线程中
 * 3) Write()：并发上下文中，memtables 已被克隆（避免状态缓存问题）
 *
 * 为什么需要克隆 memtables？
 * - memtables->Seek() 有状态缓存，并发访问会导致数据竞争
 * - 在并发模式下，每个线程需要自己的一份 memtable 副本
 * - 这个副本用于 Seek 操作，避免修改原始 memtable 的状态
 *
 * 写入组优化：
 * - 批量处理：多个写操作一起处理，减少锁竞争
 * - 序列号分配优化：可以共享起始序列号，减少原子操作
 */
// 1) During Recovery()
// 2) During Write(), in a single-threaded write thread
// 3) During Write(), in a concurrent context where memtables has been cloned
// The reason is that it calls memtables->Seek(), which has a stateful cache
Status WriteBatchInternal::InsertInto(
    WriteThread::WriteGroup& write_group, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    TrimHistoryScheduler* trim_history_scheduler,
    bool ignore_missing_column_families, uint64_t recovery_log_number, DB* db,
    bool concurrent_memtable_writes, bool seq_per_batch, bool batch_per_txn) {
  // 创建 MemTableInserter 对象，负责实际的 memtable 插入操作
  //
  // MemTableInserter 是 WriteBatch::Handler 的实现，处理每个写操作：
  // - 解析 WriteBatch 中的 Put/Delete/DeleteRange 操作
  // - 将操作插入到对应的 memtable
  // - 处理序列号分配
  // - 管理 WAL 日志号引用
  // - 处理保护信息（ProtectionInfo）用于并发控制
  //
  // 参数说明：
  // - sequence: 起始序列号
  // - nullptr /* prot_info */: 没有保护信息（非并发模式）
  // - nullptr /*has_valid_writes*/: 不需要跟踪有效写入
  MemTableInserter inserter(
      sequence, memtables, flush_scheduler, trim_history_scheduler,
      ignore_missing_column_families, recovery_log_number, db,
      concurrent_memtable_writes, nullptr /* prot_info */,
      nullptr /*has_valid_writes*/, seq_per_batch, batch_per_txn);

  // 遍历写入组中的所有 Writer
  //
  // Writer 对象包含：
  // - batch: WriteBatch，包含实际的写操作
  // - sequence: 该 Writer 的序列号
  // - log_ref: WAL 日志引用
  // - batch_cnt: 该 batch 包含的操作数量
  // - status: 操作状态
  for (auto w : write_group) {
    // 跳过已经失败的 Writer（可能在之前的处理中失败）
    if (w->CallbackFailed()) {
      continue;
    }

    // 将计算得到的序列号赋值给 Writer
    // seq_per_batch 模式：每个 Writer 增加一个序列号
    // 非 seq_per_batch 模式：每个操作都使用独立的序列号
    w->sequence = inserter.sequence();

    // 检查该 Writer 是否应该写入到 memtable
    // 某些 Writer 可能是同步点或元数据操作，不需要写入 memtable
    if (!w->ShouldWriteToMemtable()) {
      // seq_per_batch_ 模式下，序列号前进 1（每个 Writer）
      inserter.MaybeAdvanceSeq(true);
      continue;
    }

    // 设置 WriteBatch 的序列号
    SetSequence(w->batch, inserter.sequence());

    // 设置 WAL 日志号引用
    // 确保插入的操作引用正确的 WAL 文件
    inserter.set_log_number_ref(w->log_ref);

    // 设置保护信息（用于并发控制）
    // ProtectionInfo 包含：
    // - 准备阶段的最大已提交序列号
    // - 用于判断操作是否应该执行（在并发模式下）
    inserter.set_prot_info(w->batch->prot_info_.get());

    // 迭代 WriteBatch 中的所有操作，插入到 memtable
    // InsertInto 方法内部会调用 MemTableInserter 的各种回调方法：
    // - PutCF/PutEntityCF: 插入键值对
    // - DeleteCF/SingleDeleteCF: 删除单个键
    // - DeleteRangeCF: 删除键范围
    w->status = w->batch->Iterate(&inserter);

    // 如果迭代失败，返回错误状态
    if (!w->status.ok()) {
      return w->status;
    }

    // 断言检查序列号分配正确性
    // seq_per_batch 模式：batch_cnt 必须 > 0（至少一个操作）
    // seq_per_batch 模式：序列号增量必须等于 batch_cnt
    assert(!seq_per_batch || w->batch_cnt != 0);
    assert(!seq_per_batch || inserter.sequence() - w->sequence == w->batch_cnt);
  }

  // 所有 Writer 处理完成，返回成功状态
  return Status::OK();
}

/**
 * @brief 将单个 WriteBatch 插入到 memtables（重载版本 2：单个 Writer）
 *
 * 该函数是 InsertInto 的第二个重载版本，专门用于处理单个 Writer 的写入。
 * 与第一个版本的主要区别是直接传入 Writer 对象，而不是 WriteGroup。
 *
 * @param writer 写入器对象，包含 WriteBatch 和序列号
 * @param sequence 起始序列号
 * @param memtables 列族 memtable 映射
 * @param flush_scheduler flush 调度器
 * @param trim_history_scheduler memtable 历史清理调度器
 * @param ignore_missing_column_families 是否忽略不存在的列族
 * @param log_number WAL 日志号
 * @param db 数据库实例
 * @param concurrent_memtable_writes 是否在并发写入模式
 * @param seq_per_batch 是否每个 WriteBatch 使用一个序列号
 * @param batch_cnt WriteBatch 中的操作数量（用于序列号断言检查）
 * @param batch_per_txn 是否每个事务一个 WriteBatch
 * @param hint_per_batch 是否每批处理一个提示（性能优化）
 *
 * @return Status 操作状态
 */
Status WriteBatchInternal::InsertInto(
    WriteThread::Writer* writer, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    TrimHistoryScheduler* trim_history_scheduler,
    bool ignore_missing_column_families, uint64_t log_number, DB* db,
    bool concurrent_memtable_writes, bool seq_per_batch, size_t batch_cnt,
    bool batch_per_txn, bool hint_per_batch) {
#ifdef NDEBUG
  (void)batch_cnt;  // Release 模式下不使用 batch_cnt，避免编译器警告
#endif

  // 断言：Writer 应该写入到 memtable
  // 某些 Writer 可能是同步点或元数据，不实际写入
  assert(writer->ShouldWriteToMemtable());

  // 创建 MemTableInserter 对象
  //
  // 参数区别于第一个版本：
  // - 没有 batch_per_txn（这个版本不处理事务边界）
  // - 有 hint_per_batch（支持每批一个提示）
  // - prot_info 和 has_valid_writes 都为 nullptr（单 Writer 模式）
  MemTableInserter inserter(sequence, memtables, flush_scheduler,
                            trim_history_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes, nullptr /* prot_info */,
                            nullptr /*has_valid_writes*/, seq_per_batch,
                            batch_per_txn, hint_per_batch);

  // 设置 WriteBatch 的起始序列号
  SetSequence(writer->batch, sequence);

  // 设置 WAL 日志号引用
  inserter.set_log_number_ref(writer->log_ref);

  // 设置保护信息
  inserter.set_prot_info(writer->batch->prot_info_.get());

  // 迭代 WriteBatch 中的所有操作
  Status s = writer->batch->Iterate(&inserter);

  // 断言检查序列号分配
  assert(!seq_per_batch || batch_cnt != 0);
  assert(!seq_per_batch || inserter.sequence() - sequence == batch_cnt);

  // 并发模式下的后处理
  // PostProcess() 用于：
  // - 触发 memtable 的后续处理（如通知、同步）
  // - 清理临时状态
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }

  return s;
}

/**
 * @brief 将单个 WriteBatch 插入到 memtables（重载版本 3：直接 WriteBatch）
 *
 * 该函数是 InsertInto 的第三个重载版本，直接传入 WriteBatch 对象。
 * 这是最基本的插入接口，用于大多数写入场景。
 *
 * @param batch 写入批对象，包含所有写操作
 * @param memtables 列族 memtable 映射
 * @param flush_scheduler flush 调度器
 * @param trim_history_scheduler memtable 历史清理调度器
 * @param ignore_missing_column_families 是否忽略不存在的列族
 * @param log_number WAL 日志号
 * @param db 数据库实例
 * @param concurrent_memtable_writes 是否在并发写入模式
 * @param next_seq 输出参数，返回下一个可用的序列号
 * @param has_valid_writes 输出参数，标记是否有有效写入
 * @param seq_per_batch 是否每个 WriteBatch 使用一个序列号
 * @param batch_per_txn 是否每个事务一个 WriteBatch
 *
 * @return Status 操作状态
 */
Status WriteBatchInternal::InsertInto(
    const WriteBatch* batch, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler,
    TrimHistoryScheduler* trim_history_scheduler,
    bool ignore_missing_column_families, uint64_t log_number, DB* db,
    bool concurrent_memtable_writes, SequenceNumber* next_seq,
    bool* has_valid_writes, bool seq_per_batch, bool batch_per_txn) {
  // 创建 MemTableInserter 对象
  //
  // 参数区别：
  // - 直接传入 WriteBatch 的 ProtectionInfo（保护信息）
  // - has_valid_writes: 指向外部标记（用于跟踪）
  // - 不需要 hint_per_batch（这个版本不支持批提示）
  MemTableInserter inserter(Sequence(batch), memtables, flush_scheduler,
                            trim_history_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes, batch->prot_info_.get(),
                            has_valid_writes, seq_per_batch, batch_per_txn);

  // 迭代 WriteBatch 中的所有操作，插入到 memtable
  Status s = batch->Iterate(&inserter);

  // 返回下一个可用的序列号（如果需要）
  // 调用者可以使用这个值来继续后续的序列号分配
  if (next_seq != nullptr) {
    *next_seq = inserter.sequence();
  }

  // 并发模式下的后处理
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }

  return s;
}

namespace {

// This class updates protection info for a WriteBatch.
class ProtectionInfoUpdater : public WriteBatch::Handler {
 public:
  explicit ProtectionInfoUpdater(WriteBatch::ProtectionInfo* prot_info)
      : prot_info_(prot_info) {}

  ~ProtectionInfoUpdater() override {}

  Status PutCF(uint32_t cf, const Slice& key, const Slice& val) override {
    return UpdateProtInfo(cf, key, val, kTypeValue);
  }

  Status PutEntityCF(uint32_t cf, const Slice& key,
                     const Slice& entity) override {
    return UpdateProtInfo(cf, key, entity, kTypeWideColumnEntity);
  }

  Status DeleteCF(uint32_t cf, const Slice& key) override {
    return UpdateProtInfo(cf, key, "", kTypeDeletion);
  }

  Status SingleDeleteCF(uint32_t cf, const Slice& key) override {
    return UpdateProtInfo(cf, key, "", kTypeSingleDeletion);
  }

  Status DeleteRangeCF(uint32_t cf, const Slice& begin_key,
                       const Slice& end_key) override {
    return UpdateProtInfo(cf, begin_key, end_key, kTypeRangeDeletion);
  }

  Status MergeCF(uint32_t cf, const Slice& key, const Slice& val) override {
    return UpdateProtInfo(cf, key, val, kTypeMerge);
  }

  Status PutBlobIndexCF(uint32_t cf, const Slice& key,
                        const Slice& val) override {
    return UpdateProtInfo(cf, key, val, kTypeBlobIndex);
  }

  Status MarkBeginPrepare(bool /* unprepare */) override {
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& /* xid */) override {
    return Status::OK();
  }

  Status MarkCommit(const Slice& /* xid */) override { return Status::OK(); }

  Status MarkCommitWithTimestamp(const Slice& /* xid */,
                                 const Slice& /* ts */) override {
    return Status::OK();
  }

  Status MarkRollback(const Slice& /* xid */) override { return Status::OK(); }

  Status MarkNoop(bool /* empty_batch */) override { return Status::OK(); }

 private:
  Status UpdateProtInfo(uint32_t cf, const Slice& key, const Slice& val,
                        const ValueType op_type) {
    if (prot_info_) {
      prot_info_->entries_.emplace_back(
          ProtectionInfo64().ProtectKVO(key, val, op_type).ProtectC(cf));
    }
    return Status::OK();
  }

  // No copy or move.
  ProtectionInfoUpdater(const ProtectionInfoUpdater&) = delete;
  ProtectionInfoUpdater(ProtectionInfoUpdater&&) = delete;
  ProtectionInfoUpdater& operator=(const ProtectionInfoUpdater&) = delete;
  ProtectionInfoUpdater& operator=(ProtectionInfoUpdater&&) = delete;

  WriteBatch::ProtectionInfo* const prot_info_ = nullptr;
};

}  // anonymous namespace

Status WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= WriteBatchInternal::kHeader);
  assert(b->prot_info_ == nullptr);

  b->rep_.assign(contents.data(), contents.size());
  b->content_flags_.store(ContentFlags::DEFERRED, std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src,
                                  const bool wal_only) {
  assert(dst->Count() == 0 ||
         (dst->prot_info_ == nullptr) == (src->prot_info_ == nullptr));
  if ((src->prot_info_ != nullptr &&
       src->prot_info_->entries_.size() != src->Count()) ||
      (dst->prot_info_ != nullptr &&
       dst->prot_info_->entries_.size() != dst->Count())) {
    return Status::Corruption(
        "Write batch has inconsistent count and number of checksums");
  }

  size_t src_len;
  int src_count;
  uint32_t src_flags;

  const SavePoint& batch_end = src->GetWalTerminationPoint();

  if (wal_only && !batch_end.is_cleared()) {
    src_len = batch_end.size - WriteBatchInternal::kHeader;
    src_count = batch_end.count;
    src_flags = batch_end.content_flags;
  } else {
    src_len = src->rep_.size() - WriteBatchInternal::kHeader;
    src_count = Count(src);
    src_flags = src->content_flags_.load(std::memory_order_relaxed);
  }

  if (src->prot_info_ != nullptr) {
    if (dst->prot_info_ == nullptr) {
      dst->prot_info_.reset(new WriteBatch::ProtectionInfo());
    }
    std::copy(src->prot_info_->entries_.begin(),
              src->prot_info_->entries_.begin() + src_count,
              std::back_inserter(dst->prot_info_->entries_));
  } else if (dst->prot_info_ != nullptr) {
    // dst has empty prot_info->entries
    // In this special case, we allow write batch without prot_info to
    // be appende to write batch with empty prot_info
    dst->prot_info_ = nullptr;
  }
  SetCount(dst, Count(dst) + src_count);
  assert(src->rep_.size() >= WriteBatchInternal::kHeader);
  dst->rep_.append(src->rep_.data() + WriteBatchInternal::kHeader, src_len);
  dst->content_flags_.store(
      dst->content_flags_.load(std::memory_order_relaxed) | src_flags,
      std::memory_order_relaxed);
  return Status::OK();
}

size_t WriteBatchInternal::AppendedByteSize(size_t leftByteSize,
                                            size_t rightByteSize) {
  if (leftByteSize == 0 || rightByteSize == 0) {
    return leftByteSize + rightByteSize;
  } else {
    return leftByteSize + rightByteSize - WriteBatchInternal::kHeader;
  }
}

Status WriteBatchInternal::UpdateProtectionInfo(WriteBatch* wb,
                                                size_t bytes_per_key,
                                                uint64_t* checksum) {
  if (bytes_per_key == 0) {
    if (wb->prot_info_ != nullptr) {
      wb->prot_info_.reset();
      return Status::OK();
    } else {
      // Already not protected.
      return Status::OK();
    }
  } else if (bytes_per_key == 8) {
    if (wb->prot_info_ == nullptr) {
      wb->prot_info_.reset(new WriteBatch::ProtectionInfo());
      ProtectionInfoUpdater prot_info_updater(wb->prot_info_.get());
      Status s = wb->Iterate(&prot_info_updater);
      if (s.ok() && checksum != nullptr) {
        uint64_t expected_hash = XXH3_64bits(wb->rep_.data(), wb->rep_.size());
        if (expected_hash != *checksum) {
          return Status::Corruption("Write batch content corrupted.");
        }
      }
      return s;
    } else {
      // Already protected.
      return Status::OK();
    }
  }
  return Status::NotSupported(
      "WriteBatch protection info must be zero or eight bytes/key");
}

}  // namespace ROCKSDB_NAMESPACE
