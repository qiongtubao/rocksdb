//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"

#include <algorithm>
#include <array>
#include <limits>
#include <memory>

#include "db/dbformat.h"
#include "db/kv_checksum.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/wide/wide_column_serialization.h"
#include "logging/logging.h"
#include "memory/arena.h"
#include "memory/memory_usage.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/types.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

ImmutableMemTableOptions::ImmutableMemTableOptions(
    const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options)
    : arena_block_size(mutable_cf_options.arena_block_size),
      memtable_prefix_bloom_bits(
          static_cast<uint32_t>(
              static_cast<double>(mutable_cf_options.write_buffer_size) *
              mutable_cf_options.memtable_prefix_bloom_size_ratio) *
          8u),
      memtable_huge_page_size(mutable_cf_options.memtable_huge_page_size),
      memtable_whole_key_filtering(
          mutable_cf_options.memtable_whole_key_filtering),
      inplace_update_support(ioptions.inplace_update_support),
      inplace_update_num_locks(mutable_cf_options.inplace_update_num_locks),
      inplace_callback(ioptions.inplace_callback),
      max_successive_merges(mutable_cf_options.max_successive_merges),
      statistics(ioptions.stats),
      merge_operator(ioptions.merge_operator.get()),
      info_log(ioptions.logger),
      allow_data_in_errors(ioptions.allow_data_in_errors),
      protection_bytes_per_key(
          mutable_cf_options.memtable_protection_bytes_per_key) {}

MemTable::MemTable(const InternalKeyComparator& cmp,
                   const ImmutableOptions& ioptions,
                   const MutableCFOptions& mutable_cf_options,
                   WriteBufferManager* write_buffer_manager,
                   SequenceNumber latest_seq, uint32_t column_family_id)
    : comparator_(cmp),
      moptions_(ioptions, mutable_cf_options),
      refs_(0),
      kArenaBlockSize(Arena::OptimizeBlockSize(moptions_.arena_block_size)),
      mem_tracker_(write_buffer_manager),
      arena_(moptions_.arena_block_size,
             (write_buffer_manager != nullptr &&
              (write_buffer_manager->enabled() ||
               write_buffer_manager->cost_to_cache()))
                 ? &mem_tracker_
                 : nullptr,
             mutable_cf_options.memtable_huge_page_size),
      table_(ioptions.memtable_factory->CreateMemTableRep(
          comparator_, &arena_, mutable_cf_options.prefix_extractor.get(),
          ioptions.logger, column_family_id)),
      range_del_table_(SkipListFactory().CreateMemTableRep(
          comparator_, &arena_, nullptr /* transform */, ioptions.logger,
          column_family_id)),
      is_range_del_table_empty_(true),
      data_size_(0),
      num_entries_(0),
      num_deletes_(0),
      write_buffer_size_(mutable_cf_options.write_buffer_size),
      flush_in_progress_(false),
      flush_completed_(false),
      file_number_(0),
      first_seqno_(0),
      earliest_seqno_(latest_seq),
      creation_seq_(latest_seq),
      mem_next_logfile_number_(0),
      min_prep_log_referenced_(0),
      locks_(moptions_.inplace_update_support
                 ? moptions_.inplace_update_num_locks
                 : 0),
      prefix_extractor_(mutable_cf_options.prefix_extractor.get()),
      flush_state_(FLUSH_NOT_REQUESTED),
      clock_(ioptions.clock),
      insert_with_hint_prefix_extractor_(
          ioptions.memtable_insert_with_hint_prefix_extractor.get()),
      oldest_key_time_(std::numeric_limits<uint64_t>::max()),
      atomic_flush_seqno_(kMaxSequenceNumber),
      approximate_memory_usage_(0) {
  UpdateFlushState();
  // something went wrong if we need to flush before inserting anything
  assert(!ShouldScheduleFlush());

  // use bloom_filter_ for both whole key and prefix bloom filter
  if ((prefix_extractor_ || moptions_.memtable_whole_key_filtering) &&
      moptions_.memtable_prefix_bloom_bits > 0) {
    bloom_filter_.reset(
        new DynamicBloom(&arena_, moptions_.memtable_prefix_bloom_bits,
                         6 /* hard coded 6 probes */,
                         moptions_.memtable_huge_page_size, ioptions.logger));
  }
  // Initialize cached_range_tombstone_ here since it could
  // be read before it is constructed in MemTable::Add(), which could also lead
  // to a data race on the global mutex table backing atomic shared_ptr.
  auto new_cache = std::make_shared<FragmentedRangeTombstoneListCache>();
  size_t size = cached_range_tombstone_.Size();
  for (size_t i = 0; i < size; ++i) {
    std::shared_ptr<FragmentedRangeTombstoneListCache>* local_cache_ref_ptr =
        cached_range_tombstone_.AccessAtCore(i);
    auto new_local_cache_ref = std::make_shared<
        const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);
    std::atomic_store_explicit(
        local_cache_ref_ptr,
        std::shared_ptr<FragmentedRangeTombstoneListCache>(new_local_cache_ref,
                                                           new_cache.get()),
        std::memory_order_relaxed);
  }
}

MemTable::~MemTable() {
  mem_tracker_.FreeMem();
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() {
  autovector<size_t> usages = {
      arena_.ApproximateMemoryUsage(), table_->ApproximateMemoryUsage(),
      range_del_table_->ApproximateMemoryUsage(),
      ROCKSDB_NAMESPACE::ApproximateMemoryUsage(insert_hints_)};
  size_t total_usage = 0;
  for (size_t usage : usages) {
    // If usage + total_usage >= kMaxSizet, return kMaxSizet.
    // the following variation is to avoid numeric overflow.
    if (usage >= std::numeric_limits<size_t>::max() - total_usage) {
      return std::numeric_limits<size_t>::max();
    }
    total_usage += usage;
  }
  approximate_memory_usage_.store(total_usage, std::memory_order_relaxed);
  // otherwise, return the actual usage
  return total_usage;
}

bool MemTable::ShouldFlushNow() {
  size_t write_buffer_size = write_buffer_size_.load(std::memory_order_relaxed);
  // In a lot of times, we cannot allocate arena blocks that exactly matches the
  // buffer size. Thus we have to decide if we should over-allocate or
  // under-allocate.
  // This constant variable can be interpreted as: if we still have more than
  // "kAllowOverAllocationRatio * kArenaBlockSize" space left, we'd try to over
  // allocate one more block.
  const double kAllowOverAllocationRatio = 0.6;

  // If arena still have room for new block allocation, we can safely say it
  // shouldn't flush.
  auto allocated_memory = table_->ApproximateMemoryUsage() +
                          range_del_table_->ApproximateMemoryUsage() +
                          arena_.MemoryAllocatedBytes();

  approximate_memory_usage_.store(allocated_memory, std::memory_order_relaxed);

  // if we can still allocate one more block without exceeding the
  // over-allocation ratio, then we should not flush.
  if (allocated_memory + kArenaBlockSize <
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return false;
  }

  // if user keeps adding entries that exceeds write_buffer_size, we need to
  // flush earlier even though we still have much available memory left.
  if (allocated_memory >
      write_buffer_size + kArenaBlockSize * kAllowOverAllocationRatio) {
    return true;
  }

  // In this code path, Arena has already allocated its "last block", which
  // means the total allocatedmemory size is either:
  //  (1) "moderately" over allocated the memory (no more than `0.6 * arena
  // block size`. Or,
  //  (2) the allocated memory is less than write buffer size, but we'll stop
  // here since if we allocate a new arena block, we'll over allocate too much
  // more (half of the arena block size) memory.
  //
  // In either case, to avoid over-allocate, the last block will stop allocation
  // when its usage reaches a certain ratio, which we carefully choose "0.75
  // full" as the stop condition because it addresses the following issue with
  // great simplicity: What if the next inserted entry's size is
  // bigger than AllocatedAndUnused()?
  //
  // The answer is: if the entry size is also bigger than 0.25 *
  // kArenaBlockSize, a dedicated block will be allocated for it; otherwise
  // arena will anyway skip the AllocatedAndUnused() and allocate a new, empty
  // and regular block. In either case, we *overly* over-allocated.
  //
  // Therefore, setting the last block to be at most "0.75 full" avoids both
  // cases.
  //
  // NOTE: the average percentage of waste space of this approach can be counted
  // as: "arena block size * 0.25 / write buffer size". User who specify a small
  // write buffer size and/or big arena block size may suffer.
  return arena_.AllocatedAndUnused() < kArenaBlockSize / 4;
}

void MemTable::UpdateFlushState() {
  auto state = flush_state_.load(std::memory_order_relaxed);
  if (state == FLUSH_NOT_REQUESTED && ShouldFlushNow()) {
    // ignore CAS failure, because that means somebody else requested
    // a flush
    flush_state_.compare_exchange_strong(state, FLUSH_REQUESTED,
                                         std::memory_order_relaxed,
                                         std::memory_order_relaxed);
  }
}

// 更新 MemTable 中最旧键的时间戳
//
// 功能说明：
// 1. 此函数用于记录 MemTable 中最早插入的键的时间戳，用于 TTL（Time To Live）管理
// 2. 时间戳只设置一次（首次插入时），后续插入不会覆盖
// 3. 使用 CAS（Compare-And-Swap）原子操作确保多线程安全
//
// 调用时机：
// - 每次 MemTable::Add() 插入新条目后都会调用
// - 无论是否为并发插入模式
//
// TTL 管理说明：
// - MemTable 可以配置 TTL（通过选项中的 ttl 参数）
// - RocksDB 使用这个时间戳来判断 MemTable 中的数据是否过期
// - 过期数据可以在 flush 时被过滤掉
// - 还可以用于基于时间的 Compaction 策略
//
// 并发安全：
// - 使用 compare_exchange_strong 原子操作
// - 多个线程可能同时调用此函数，但只有第一个会成功设置时间戳
// - 使用 relaxed 内存序，因为时间戳是独立的状态，不需要与其他操作同步
//
// 实现原理：
// 1. 检查 oldest_key_time_ 是否为初始值（max）
// 2. 如果是初始值，尝试获取当前时间戳
// 3. 使用 CAS 原子设置 oldest_key_time_
// 4. 如果 CAS 失败（其他线程已设置），则跳过
//
// 相关配置选项：
// - ColumnFamilyOptions.ttl: 设置 TTL（秒）
// - ImmutableDBOptions.clock: 时钟接口实现（用于获取时间）
//
// 注意事项：
// - 时间戳精度取决于 clock_ 的实现（通常是系统时间，秒级或毫秒级）
// - 即使获取时间失败，也不会影响正常功能，只是 TTL 功能可能无法正常工作
// - 如果 MemTable 为空或所有条目都被删除，oldest_key_time_ 不会被设置
void MemTable::UpdateOldestKeyTime() {
  // 原子加载当前的 oldest_key_time_
  // 使用 relaxed 内存序，因为这是独立的状态检查，不需要与其他内存操作同步
  uint64_t oldest_key_time = oldest_key_time_.load(std::memory_order_relaxed);
  // 检查是否为初始值（std::numeric_limits<uint64_t>::max() 表示未设置）
  // 如果是初始值，说明这是首次插入，需要设置时间戳
  // 如果已经设置过，直接返回，不更新（保持最早插入的时间戳）
  if (oldest_key_time == std::numeric_limits<uint64_t>::max()) {
    int64_t current_time = 0;  // 用于存储获取的当前时间
    auto s = clock_->GetCurrentTime(&current_time);  // 从时钟接口获取当前时间
    // 如果成功获取时间，则尝试设置 oldest_key_time_
    if (s.ok()) {
      assert(current_time >= 0);  // 断言：时间应该是非负的
      // 使用 CAS（Compare-And-Swap）原子操作设置时间戳
      // 参数说明：
      // - oldest_key_time: 期望值（初始值为 max）
      // - static_cast<uint64_t>(current_time): 新值（当前时间）
      // - success memory_order_relaxed: 成功时的内存序（无需与其他操作同步）
      // - failure memory_order_relaxed: 失败时的内存序（读取失败后的值）
      //
      // CAS 语义：
      // - 如果 oldest_key_time_ 当前值等于 oldest_key_time（max），则设置为新值并返回 true
      // - 如果不等，说明其他线程已经修改了 oldest_key_time_，则将 oldest_key_time 更新为当前值并返回 false
      // - If fail, the timestamp is already set.  如果 CAS 失败，说明时间戳已经被其他线程设置了
      oldest_key_time_.compare_exchange_strong(
          oldest_key_time, static_cast<uint64_t>(current_time),
          std::memory_order_relaxed, std::memory_order_relaxed);
    }
    // 如果获取时间失败（s.ok() == false），则不设置时间戳
    // 这可能发生在 clock_ 实现有问题或系统时间不可用的情况
    // 在这种情况下，TTL 功能可能无法正常工作，但不会影响 MemTable 的其他功能
  }
}

Status MemTable::VerifyEntryChecksum(const char* entry,
                                     uint32_t protection_bytes_per_key,
                                     bool allow_data_in_errors) {
  if (protection_bytes_per_key == 0) {
    return Status::OK();
  }
  uint32_t key_length;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  if (key_ptr == nullptr) {
    return Status::Corruption("Unable to parse internal key length");
  }
  if (key_length < 8) {
    return Status::Corruption("Memtable entry internal key length too short.");
  }
  Slice user_key = Slice(key_ptr, key_length - 8);

  const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
  ValueType type;
  SequenceNumber seq;
  UnPackSequenceAndType(tag, &seq, &type);

  uint32_t value_length = 0;
  const char* value_ptr = GetVarint32Ptr(
      key_ptr + key_length, key_ptr + key_length + 5, &value_length);
  if (value_ptr == nullptr) {
    return Status::Corruption("Unable to parse internal key value");
  }
  Slice value = Slice(value_ptr, value_length);

  const char* checksum_ptr = value_ptr + value_length;
  bool match =
      ProtectionInfo64()
          .ProtectKVO(user_key, value, type)
          .ProtectS(seq)
          .Verify(static_cast<uint8_t>(protection_bytes_per_key), checksum_ptr);
  if (!match) {
    std::string msg(
        "Corrupted memtable entry, per key-value checksum verification "
        "failed.");
    if (allow_data_in_errors) {
      msg.append("Unrecognized value type: " +
                 std::to_string(static_cast<int>(type)) + ". ");
      msg.append("User key: " + user_key.ToString(/*hex=*/true) + ". ");
      msg.append("seq: " + std::to_string(seq) + ".");
    }
    return Status::Corruption(msg.c_str());
  }
  return Status::OK();
}

int MemTable::KeyComparator::operator()(const char* prefix_len_key1,
                                        const char* prefix_len_key2) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice k1 = GetLengthPrefixedSlice(prefix_len_key1);
  Slice k2 = GetLengthPrefixedSlice(prefix_len_key2);
  return comparator.CompareKeySeq(k1, k2);
}

int MemTable::KeyComparator::operator()(
    const char* prefix_len_key, const KeyComparator::DecodedType& key) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(prefix_len_key);
  return comparator.CompareKeySeq(a, key);
}

void MemTableRep::InsertConcurrently(KeyHandle /*handle*/) {
  throw std::runtime_error("concurrent insert not supported");
}

Slice MemTableRep::UserKey(const char* key) const {
  Slice slice = GetLengthPrefixedSlice(key);
  return Slice(slice.data(), slice.size() - 8);
}

KeyHandle MemTableRep::Allocate(const size_t len, char** buf) {
  *buf = allocator_->Allocate(len);
  return static_cast<KeyHandle>(*buf);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, static_cast<uint32_t>(target.size()));
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public InternalIterator {
 public:
  MemTableIterator(const MemTable& mem, const ReadOptions& read_options,
                   Arena* arena, bool use_range_del_table = false)
      : bloom_(nullptr),
        prefix_extractor_(mem.prefix_extractor_),
        comparator_(mem.comparator_),
        valid_(false),
        arena_mode_(arena != nullptr),
        value_pinned_(
            !mem.GetImmutableMemTableOptions()->inplace_update_support),
        protection_bytes_per_key_(mem.moptions_.protection_bytes_per_key),
        status_(Status::OK()),
        logger_(mem.moptions_.info_log) {
    if (use_range_del_table) {
      iter_ = mem.range_del_table_->GetIterator(arena);
    } else if (prefix_extractor_ != nullptr && !read_options.total_order_seek &&
               !read_options.auto_prefix_mode) {
      // Auto prefix mode is not implemented in memtable yet.
      bloom_ = mem.bloom_filter_.get();
      iter_ = mem.table_->GetDynamicPrefixIterator(arena);
    } else {
      iter_ = mem.table_->GetIterator(arena);
    }
    status_.PermitUncheckedError();
  }
  // No copying allowed
  MemTableIterator(const MemTableIterator&) = delete;
  void operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override {
#ifndef NDEBUG
    // Assert that the MemTableIterator is never deleted while
    // Pinning is Enabled.
    assert(!pinned_iters_mgr_ || !pinned_iters_mgr_->PinningEnabled());
#endif
    if (arena_mode_) {
      iter_->~Iterator();
    } else {
      delete iter_;
    }
  }

#ifndef NDEBUG
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  bool Valid() const override { return valid_ && status_.ok(); }
  void Seek(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    if (bloom_) {
      // iterator should only use prefix bloom filter
      auto ts_sz = comparator_.comparator.user_comparator()->timestamp_size();
      Slice user_k_without_ts(ExtractUserKeyAndStripTimestamp(k, ts_sz));
      if (prefix_extractor_->InDomain(user_k_without_ts)) {
        if (!bloom_->MayContain(
                prefix_extractor_->Transform(user_k_without_ts))) {
          PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
          valid_ = false;
          return;
        } else {
          PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
        }
      }
    }
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  void SeekForPrev(const Slice& k) override {
    PERF_TIMER_GUARD(seek_on_memtable_time);
    PERF_COUNTER_ADD(seek_on_memtable_count, 1);
    if (bloom_) {
      auto ts_sz = comparator_.comparator.user_comparator()->timestamp_size();
      Slice user_k_without_ts(ExtractUserKeyAndStripTimestamp(k, ts_sz));
      if (prefix_extractor_->InDomain(user_k_without_ts)) {
        if (!bloom_->MayContain(
                prefix_extractor_->Transform(user_k_without_ts))) {
          PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
          valid_ = false;
          return;
        } else {
          PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
        }
      }
    }
    iter_->Seek(k, nullptr);
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
    if (!Valid() && status().ok()) {
      SeekToLast();
    }
    while (Valid() && comparator_.comparator.Compare(k, key()) < 0) {
      Prev();
    }
  }
  void SeekToFirst() override {
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  void SeekToLast() override {
    iter_->SeekToLast();
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  void Next() override {
    PERF_COUNTER_ADD(next_on_memtable_count, 1);
    assert(Valid());
    iter_->Next();
    TEST_SYNC_POINT_CALLBACK("MemTableIterator::Next:0", iter_);
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  bool NextAndGetResult(IterateResult* result) override {
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
    }
    return is_valid;
  }
  void Prev() override {
    PERF_COUNTER_ADD(prev_on_memtable_count, 1);
    assert(Valid());
    iter_->Prev();
    valid_ = iter_->Valid();
    VerifyEntryChecksum();
  }
  Slice key() const override {
    assert(Valid());
    return GetLengthPrefixedSlice(iter_->key());
  }
  Slice value() const override {
    assert(Valid());
    Slice key_slice = GetLengthPrefixedSlice(iter_->key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return status_; }

  bool IsKeyPinned() const override {
    // memtable data is always pinned
    return true;
  }

  bool IsValuePinned() const override {
    // memtable value is always pinned, except if we allow inplace update.
    return value_pinned_;
  }

 private:
  DynamicBloom* bloom_;
  const SliceTransform* const prefix_extractor_;
  const MemTable::KeyComparator comparator_;
  MemTableRep::Iterator* iter_;
  bool valid_;
  bool arena_mode_;
  bool value_pinned_;
  uint32_t protection_bytes_per_key_;
  Status status_;
  Logger* logger_;

  void VerifyEntryChecksum() {
    if (protection_bytes_per_key_ > 0 && Valid()) {
      status_ = MemTable::VerifyEntryChecksum(iter_->key(),
                                              protection_bytes_per_key_);
      if (!status_.ok()) {
        ROCKS_LOG_ERROR(logger_, "In MemtableIterator: %s", status_.getState());
      }
    }
  }
};

InternalIterator* MemTable::NewIterator(const ReadOptions& read_options,
                                        Arena* arena) {
  assert(arena != nullptr);
  auto mem = arena->AllocateAligned(sizeof(MemTableIterator));
  return new (mem) MemTableIterator(*this, read_options, arena);
}

FragmentedRangeTombstoneIterator* MemTable::NewRangeTombstoneIterator(
    const ReadOptions& read_options, SequenceNumber read_seq,
    bool immutable_memtable) {
  if (read_options.ignore_range_deletions ||
      is_range_del_table_empty_.load(std::memory_order_relaxed)) {
    return nullptr;
  }
  return NewRangeTombstoneIteratorInternal(read_options, read_seq,
                                           immutable_memtable);
}

FragmentedRangeTombstoneIterator* MemTable::NewRangeTombstoneIteratorInternal(
    const ReadOptions& read_options, SequenceNumber read_seq,
    bool immutable_memtable) {
  if (immutable_memtable) {
    // Note that caller should already have verified that
    // !is_range_del_table_empty_
    assert(IsFragmentedRangeTombstonesConstructed());
    return new FragmentedRangeTombstoneIterator(
        fragmented_range_tombstone_list_.get(), comparator_.comparator,
        read_seq, read_options.timestamp);
  }

  // takes current cache
  std::shared_ptr<FragmentedRangeTombstoneListCache> cache =
      std::atomic_load_explicit(cached_range_tombstone_.Access(),
                                std::memory_order_relaxed);
  // construct fragmented tombstone list if necessary
  if (!cache->initialized.load(std::memory_order_acquire)) {
    cache->reader_mutex.lock();
    if (!cache->tombstones) {
      auto* unfragmented_iter =
          new MemTableIterator(*this, read_options, nullptr /* arena */,
                               true /* use_range_del_table */);
      cache->tombstones.reset(new FragmentedRangeTombstoneList(
          std::unique_ptr<InternalIterator>(unfragmented_iter),
          comparator_.comparator));
      cache->initialized.store(true, std::memory_order_release);
    }
    cache->reader_mutex.unlock();
  }

  auto* fragmented_iter = new FragmentedRangeTombstoneIterator(
      cache, comparator_.comparator, read_seq, read_options.timestamp);
  return fragmented_iter;
}

void MemTable::ConstructFragmentedRangeTombstones() {
  assert(!IsFragmentedRangeTombstonesConstructed(false));
  // There should be no concurrent Construction
  if (!is_range_del_table_empty_.load(std::memory_order_relaxed)) {
    // TODO: plumb Env::IOActivity
    auto* unfragmented_iter =
        new MemTableIterator(*this, ReadOptions(), nullptr /* arena */,
                             true /* use_range_del_table */);

    fragmented_range_tombstone_list_ =
        std::make_unique<FragmentedRangeTombstoneList>(
            std::unique_ptr<InternalIterator>(unfragmented_iter),
            comparator_.comparator);
  }
}

port::RWMutex* MemTable::GetLock(const Slice& key) {
  return &locks_[GetSliceRangedNPHash(key, locks_.size())];
}

MemTable::MemTableStats MemTable::ApproximateStats(const Slice& start_ikey,
                                                   const Slice& end_ikey) {
  uint64_t entry_count = table_->ApproximateNumEntries(start_ikey, end_ikey);
  entry_count += range_del_table_->ApproximateNumEntries(start_ikey, end_ikey);
  if (entry_count == 0) {
    return {0, 0};
  }
  uint64_t n = num_entries_.load(std::memory_order_relaxed);
  if (n == 0) {
    return {0, 0};
  }
  if (entry_count > n) {
    // (range_del_)table_->ApproximateNumEntries() is just an estimate so it can
    // be larger than actual entries we have. Cap it to entries we have to limit
    // the inaccuracy.
    entry_count = n;
  }
  uint64_t data_size = data_size_.load(std::memory_order_relaxed);
  return {entry_count * (data_size / n), entry_count};
}

Status MemTable::VerifyEncodedEntry(Slice encoded,
                                    const ProtectionInfoKVOS64& kv_prot_info) {
  uint32_t ikey_len = 0;
  if (!GetVarint32(&encoded, &ikey_len)) {
    return Status::Corruption("Unable to parse internal key length");
  }
  size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();
  if (ikey_len < 8 + ts_sz) {
    return Status::Corruption("Internal key length too short");
  }
  if (ikey_len > encoded.size()) {
    return Status::Corruption("Internal key length too long");
  }
  uint32_t value_len = 0;
  const size_t user_key_len = ikey_len - 8;
  Slice key(encoded.data(), user_key_len);
  encoded.remove_prefix(user_key_len);

  uint64_t packed = DecodeFixed64(encoded.data());
  ValueType value_type = kMaxValue;
  SequenceNumber sequence_number = kMaxSequenceNumber;
  UnPackSequenceAndType(packed, &sequence_number, &value_type);
  encoded.remove_prefix(8);

  if (!GetVarint32(&encoded, &value_len)) {
    return Status::Corruption("Unable to parse value length");
  }
  if (value_len < encoded.size()) {
    return Status::Corruption("Value length too short");
  }
  if (value_len > encoded.size()) {
    return Status::Corruption("Value length too long");
  }
  Slice value(encoded.data(), value_len);

  return kv_prot_info.StripS(sequence_number)
      .StripKVO(key, value, value_type)
      .GetStatus();
}

void MemTable::UpdateEntryChecksum(const ProtectionInfoKVOS64* kv_prot_info,
                                   const Slice& key, const Slice& value,
                                   ValueType type, SequenceNumber s,
                                   char* checksum_ptr) {
  if (moptions_.protection_bytes_per_key == 0) {
    return;
  }

  if (kv_prot_info == nullptr) {
    ProtectionInfo64()
        .ProtectKVO(key, value, type)
        .ProtectS(s)
        .Encode(static_cast<uint8_t>(moptions_.protection_bytes_per_key),
                checksum_ptr);
  } else {
    kv_prot_info->Encode(
        static_cast<uint8_t>(moptions_.protection_bytes_per_key), checksum_ptr);
  }
}

// 向MemTable中添加一个键值对条目
//
// 参数说明:
// - s: 序列号,表示此操作的全局顺序,用于实现多版本并发控制(MVCC)。
//      序列号越大,表示版本越新。在读取时会选择符合快照条件的最大序列号版本。
// - type: 值类型,表示此条目的操作类型。
//         常见类型包括:
//         - kTypeValue: 普通的Put操作,存储键值对
//         - kTypeMerge: Merge操作,需要与后续的值合并
//         - kTypeDeletion/kTypeSingleDeletion: 删除操作,标记键被删除
//         - kTypeRangeDeletion: 范围删除,删除一个键范围
//         - kTypeBlobIndex: 大值索引,指向存储在Blob文件中的大值
//         - kTypeWideColumnEntity: 宽列实体,存储多列数据
// - key: 用户键,不包含序列号和类型信息的原始键
// - value: 值数据,根据type的不同可能存储不同的内容
// - kv_prot_info: 键值保护信息指针,用于校验条目完整性,可为nullptr
//                 当protection_bytes_per_key > 0时使用,存储checksum等信息
// - allow_concurrent: 是否允许并发插入
//                     false: 单线程插入,直接更新计数器
//                     true: 多线程并发插入,使用线程安全的插入方法
// - post_process_info: 后处理信息指针,用于并发插入场景下的批量更新
//                      在allow_concurrent=true时必须提供,用于延迟更新统计信息
// - hint: 插入提示指针,用于优化重复键的插入性能
//         当allow_concurrent=true时,如果hint不为nullptr,会使用InsertKeyWithHintConcurrently
//         hint的值由上层调用者维护,通常存储上一次插入的位置信息
//
// 返回值: Status
//         - Status::OK(): 插入成功
//         - Status::TryAgain("key+seq exists"): 插入失败,相同序列号的键已存在
//                                            上层会重试
//         - Status::Corruption(): 数据损坏,checksum校验失败等
//
// 工作原理:
// 1. 条目编码格式:
//    [key_size(varint32)][key数据(8字节序列号+类型+用户键)]
//    [value_size(varint32)][value数据]
//    [checksum(可选,长度由protection_bytes_per_key指定)]
//
// 2. 插入流程:
//    a) 根据type选择table_或range_del_table_
//    b) 从Arena分配内存并编码条目
//    c) 计算并写入checksum(如果启用)
//    d) 插入到SkipList中(单线程或并发)
//    e) 更新统计信息(条目数、数据大小、删除数)
//    f) 更新Bloom Filter(如果启用)
//    g) 更新序列号信息(first_seqno_和earliest_seqno_)
//    h) 如果是RangeDeletion,使缓存的range tombstone失效
//    i) 更新oldest_key_time_和flush状态
//
// 3. 并发控制:
//    - allow_concurrent=false时,使用InsertKey/InsertKeyWithHint,由调用者保证线程安全
//    - allow_concurrent=true时,使用InsertKeyConcurrently/InsertKeyWithHintConcurrently
//    - 统计信息在allow_concurrent=true时通过post_process_info批量更新,减少原子操作开销
//
// 4. Bloom Filter:
//    - 如果prefix_extractor_存在,将前缀添加到prefix bloom filter
//    - 如果memtable_whole_key_filtering=true,将整个键添加到bloom filter
//    - bloom filter用于加速Get操作,避免查找肯定不存在的键
//
// 5. Range Deletion:
//    - RangeDeletion类型的条目插入到range_del_table_中
//    - 插入RangeDeletion会使cached_range_tombstone_失效,触发lazy reconstruction
//    - 多线程插入时需要加range_del_mutex_保护
//
// 性能考虑:
// - 使用Arena内存分配器,减少内存分配开销
// - 使用insert hints优化重复键插入
// - 并发插入时批量更新统计信息,减少原子操作
// - Bloom filter显著减少不必要的查找
//
// 使用场景:
// - 写入路径(WRITE): Put、Merge、Delete等操作都会调用此函数
// - 恢复路径(RECOVERY): 从WAL恢复数据时调用
// - 批量写入: WriteBatch处理时批量调用
Status MemTable::Add(SequenceNumber s, ValueType type,
                     const Slice& key, /* user key */
                     const Slice& value,
                     const ProtectionInfoKVOS64* kv_prot_info,
                     bool allow_concurrent,
                     MemTablePostProcessInfo* post_process_info, void** hint) {
  // 条目编码格式说明:
  //  key_size     : 内部键大小的varint32编码
  //  key bytes    : 内部键字节数组(用户键+8字节序列号和类型)
  //  value_size   : 值大小的varint32编码
  //  value bytes  : 值字节数组
  //  checksum     : 校验和字节数组(长度由protection_bytes_per_key配置决定)
  uint32_t key_size = static_cast<uint32_t>(key.size());  // 获取用户键的大小,转为32位无符号整数
  uint32_t val_size = static_cast<uint32_t>(value.size());  // 获取值的大小,转为32位无符号整数
  uint32_t internal_key_size = key_size + 8;  // 计算内部键大小 = 用户键大小 + 8字节(序列号7字节+类型1字节)
  // 计算编码后的总长度:
  // 1. internal_key_size的varint编码长度
  // 2. 内部键本身的大小(用户键+8字节)
  // 3. val_size的varint编码长度
  // 4. 值本身的大小
  // 5. 校验和的大小
  const uint32_t encoded_len = VarintLength(internal_key_size) +
                               internal_key_size + VarintLength(val_size) +
                               val_size + moptions_.protection_bytes_per_key;
  char* buf = nullptr;  // 声明缓冲区指针,用于存储编码后的条目
  // 根据条目类型选择插入的目标表:
  // - kTypeRangeDeletion类型的条目插入到range_del_table_中
  // - 其他类型的条目插入到常规的table_中
  std::unique_ptr<MemTableRep>& table =
      type == kTypeRangeDeletion ? range_del_table_ : table_;
  // 从Arena内存分配器中分配所需大小的内存,返回KeyHandle和buf指针
  // Arena提供了高效的内存分配策略,减少malloc/free开销
  KeyHandle handle = table->Allocate(encoded_len, &buf);

  char* p = EncodeVarint32(buf, internal_key_size);  // 将internal_key_size编码为varint32格式,写入buf开头,返回写入位置指针
  memcpy(p, key.data(), key_size);  // 将用户键数据拷贝到p指向的位置
  Slice key_slice(p, key_size);  // 创建key_slice引用用户键数据,后续用于前缀提取和Bloom filter
  p += key_size;  // 指针向后移动key_size字节,指向序列号和类型的写入位置
  uint64_t packed = PackSequenceAndType(s, type);  // 将序列号s和类型type打包成一个64位整数(序列号占高56位,类型占低8位)
  EncodeFixed64(p, packed);  // 将打包后的64位整数写入p指向的位置(固定8字节,小端序)
  p += 8;  // 指针向后移动8字节,指向value_size的写入位置
  p = EncodeVarint32(p, val_size);  // 将val_size编码为varint32格式,写入p指向的位置,返回写入位置指针
  memcpy(p, value.data(), val_size);  // 将值数据拷贝到p指向的位置
  // 断言验证:确保实际写入的长度(包括校验和预留空间)等于计算的encoded_len
  // p+val_size指向值数据结束位置,减去buf得到已写入长度,再加上校验和长度应该等于encoded_len
  assert((unsigned)(p + val_size - buf + moptions_.protection_bytes_per_key) ==
         (unsigned)encoded_len);

  UpdateEntryChecksum(kv_prot_info, key, value, type, s,
                      buf + encoded_len - moptions_.protection_bytes_per_key);  // 计算校验和并写入条目末尾,校验和基于key、value、type和sequence计算
  // 创建encoded slice,指向编码后的条目(不包含校验和部分)
  // 这个slice用于后续的校验和验证和测试同步点
  Slice encoded(buf, encoded_len - moptions_.protection_bytes_per_key);
  // 如果提供了kv_prot_info(键值保护信息),则需要验证编码条目的完整性
  if (kv_prot_info != nullptr) {
    TEST_SYNC_POINT_CALLBACK("MemTable::Add:Encoded", &encoded);  // 测试同步点,用于单元测试注入测试逻辑
    Status status = VerifyEncodedEntry(encoded, *kv_prot_info);  // 验证编码条目是否与kv_prot_info匹配
    if (!status.ok()) {  // 如果验证失败,返回错误状态
      return status;
    }
  }

  size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();  // 获取用户比较器配置的timestamp大小(如果不支持timestamp则为0)
  Slice key_without_ts = StripTimestampFromUserKey(key, ts_sz);  // 从用户键中剥离timestamp部分,用于Bloom filter(因为Bloom filter不应该包含timestamp)

  if (!allow_concurrent) {  // 非并发模式:单线程插入,由调用者保证线程安全
    // Extract prefix for insert with hint.  提取前缀用于带提示的插入优化
    // 如果配置了insert_with_hint_prefix_extractor_且key在前缀提取器的定义域内
    if (insert_with_hint_prefix_extractor_ != nullptr &&
        insert_with_hint_prefix_extractor_->InDomain(key_slice)) {
      Slice prefix = insert_with_hint_prefix_extractor_->Transform(key_slice);  // 从key_slice中提取前缀
      bool res = table->InsertKeyWithHint(handle, &insert_hints_[prefix]);  // 使用前缀hint进行插入,insert_hints_[prefix]存储了该前缀的上次插入位置,可以加速重复前缀的插入
      if (UNLIKELY(!res)) {  // UNLIKELY宏告诉编译器这个分支很少发生,优化分支预测
        return Status::TryAgain("key+seq exists");  // 插入失败,说明相同的key+sequence已存在,返回TryAgain状态让上层重试
      }
    } else {  // 不能使用hint,使用普通的插入方法
      bool res = table->InsertKey(handle);  // 将handle插入到table中(SkipList或其他数据结构)
      if (UNLIKELY(!res)) {  // 插入失败(重复的key+seq)
        return Status::TryAgain("key+seq exists");  // 返回TryAgain状态
      }
    }

    // this is a bit ugly, but is the way to avoid locked instructions  这种写法有点丑陋,但这是避免使用锁指令原子递增的方法
    // when incrementing an atomic  当递增原子变量时
    // 直接使用load+store而不是fetch_add,因为fetch_add在x86上会导致总线锁,性能较差
    // 这里是单线程插入,所以load+store是安全的
    num_entries_.store(num_entries_.load(std::memory_order_relaxed) + 1,
                       std::memory_order_relaxed);  // 原子递增条目计数器,使用relaxed内存序(不需要与其他线程同步)
    data_size_.store(data_size_.load(std::memory_order_relaxed) + encoded_len,
                     std::memory_order_relaxed);  // 原子递增数据大小计数器,加上本次插入的编码长度
    // 如果条目类型是删除类型,递增删除计数器
    if (type == kTypeDeletion || type == kTypeSingleDeletion ||
        type == kTypeDeletionWithTimestamp) {
      num_deletes_.store(num_deletes_.load(std::memory_order_relaxed) + 1,
                         std::memory_order_relaxed);  // 原子递增删除计数器
    }

    // 如果配置了bloom_filter_和prefix_extractor_,并且key_without_ts在前缀提取器的定义域内
    if (bloom_filter_ && prefix_extractor_ &&
        prefix_extractor_->InDomain(key_without_ts)) {
      bloom_filter_->Add(prefix_extractor_->Transform(key_without_ts));  // 将key的前缀添加到bloom filter中,用于后续查询优化
    }
    // 如果配置了bloom_filter_且启用了整键过滤
    if (bloom_filter_ && moptions_.memtable_whole_key_filtering) {
      bloom_filter_->Add(key_without_ts);  // 将整个键(不包含timestamp)添加到bloom filter中
    }

    // The first sequence number inserted into the memtable  插入到memtable中的第一个序列号
    assert(first_seqno_ == 0 || s >= first_seqno_);  // 断言:first_seqno_为0(还没有插入)或者当前序列号s >= first_seqno_(保证序列号递增)
    if (first_seqno_ == 0) {  // 如果这是第一次插入
      first_seqno_.store(s, std::memory_order_relaxed);  // 设置first_seqno_为当前序列号s

      if (earliest_seqno_ == kMaxSequenceNumber) {  // 如果earliest_seqno_还未设置
        earliest_seqno_.store(GetFirstSequenceNumber(),  // 获取第一个序列号并设置到earliest_seqno_
                              std::memory_order_relaxed);
      }
      assert(first_seqno_.load() >= earliest_seqno_.load());  // 断言:first_seqno_应该 >= earliest_seqno_
    }
    assert(post_process_info == nullptr);  // 断言:非并发模式下post_process_info应该为nullptr
    UpdateFlushState();  // 更新flush状态,检查是否需要触发flush(当memtable大小超过write_buffer_size时)
  } else {  // 并发模式:多线程并发插入,使用线程安全的插入方法
    bool res = (hint == nullptr)  // 根据hint是否为nullptr选择并发插入方法
                   ? table->InsertKeyConcurrently(handle)  // hint为null,使用普通并发插入
                   : table->InsertKeyWithHintConcurrently(handle, hint);  // hint不为null,使用带hint的并发插入,利用hint加速重复键插入
    if (UNLIKELY(!res)) {  // 插入失败(重复的key+seq)
      return Status::TryAgain("key+seq exists");  // 返回TryAgain状态
    }

    assert(post_process_info != nullptr);  // 断言:并发模式下post_process_info必须不为nullptr
    post_process_info->num_entries++;  // 通过post_process_info批量递增条目计数,避免每次插入都做原子操作
    post_process_info->data_size += encoded_len;  // 通过post_process_info批量累加数据大小
    if (type == kTypeDeletion) {  // 如果是删除类型
      post_process_info->num_deletes++;  // 通过post_process_info批量递增删除计数
    }

    // 并发模式下更新bloom filter,使用线程安全的AddConcurrently方法
    if (bloom_filter_ && prefix_extractor_ &&
        prefix_extractor_->InDomain(key_without_ts)) {
      bloom_filter_->AddConcurrently(  // 并发安全地添加前缀到bloom filter
          prefix_extractor_->Transform(key_without_ts));
    }
    if (bloom_filter_ && moptions_.memtable_whole_key_filtering) {
      bloom_filter_->AddConcurrently(key_without_ts);  // 并发安全地添加整键到bloom filter
    }

    // atomically update first_seqno_ and earliest_seqno_.  原子地更新first_seqno_和earliest_seqno_
    // 使用CAS(Compare-And-Swap)循环来原子更新first_seqno_,确保多线程安全
    uint64_t cur_seq_num = first_seqno_.load(std::memory_order_relaxed);  // 加载当前的first_seqno_
    while ((cur_seq_num == 0 || s < cur_seq_num) &&  // 如果当前值为0或s更小(是第一个或更早的序列号)
           !first_seqno_.compare_exchange_weak(cur_seq_num, s)) {  // 尝试CAS更新:如果cur_seq_num未变则更新为s,否则重试
    }  // CAS循环:compare_exchange_weak失败时会将cur_seq_num更新为最新值
    // 使用CAS循环来原子更新earliest_seqno_
    uint64_t cur_earliest_seqno =
        earliest_seqno_.load(std::memory_order_relaxed);  // 加载当前的earliest_seqno_
    while (
        (cur_earliest_seqno == kMaxSequenceNumber || s < cur_earliest_seqno) &&  // 如果当前值为kMaxSequenceNumber(未初始化)或s更小
        !earliest_seqno_.compare_exchange_weak(cur_earliest_seqno, s)) {  // 尝试CAS更新
    }  // CAS循环更新earliest_seqno_
  }
  if (type == kTypeRangeDeletion) {  // 如果是范围删除类型
    auto new_cache = std::make_shared<FragmentedRangeTombstoneListCache>();  // 创建一个新的空缓存对象,用于使旧缓存失效
    size_t size = cached_range_tombstone_.Size();  // 获取per-core缓存数组的大小
    if (allow_concurrent) {  // 如果是并发插入模式
      range_del_mutex_.lock();  // 加锁保护cached_range_tombstone_的更新,避免并发竞争
    }
    // 遍历每个CPU核心的缓存项,使其失效
    for (size_t i = 0; i < size; ++i) {
      std::shared_ptr<FragmentedRangeTombstoneListCache>* local_cache_ref_ptr =
          cached_range_tombstone_.AccessAtCore(i);  // 获取第i个CPU核心的缓存指针
      auto new_local_cache_ref = std::make_shared<
          const std::shared_ptr<FragmentedRangeTombstoneListCache>>(new_cache);  // 创建一个新的shared_ptr包装new_cache
      // It is okay for some reader to load old cache during invalidation as  在使缓存失效期间,某些reader加载旧缓存是可以接受的
      // the new sequence number is not published yet.  因为新的序列号还未发布
      // Each core will have a shared_ptr to a shared_ptr to the cached  每个CPU核心都有一个指向缓存fragmented range tombstones的shared_ptr的shared_ptr
      // fragmented range tombstones, so that ref count is maintianed locally  因此引用计数在每个CPU核心本地维护
      // per-core using the per-core shared_ptr.  通过per-core的shared_ptr实现
      std::atomic_store_explicit(  // 原子地存储新的缓存指针
          local_cache_ref_ptr,  // 目标:per-core缓存指针
          std::shared_ptr<FragmentedRangeTombstoneListCache>(  // 创建shared_ptr,管理两个引用:
              new_local_cache_ref,  // 1. new_local_cache_ref的引用(内层)
              new_cache.get()),  // 2. new_cache的引用(外层)
          std::memory_order_relaxed);  // 使用relaxed内存序,因为不需要与其他操作同步
    }
    if (allow_concurrent) {  // 如果是并发插入模式
      range_del_mutex_.unlock();  // 解锁
    }
    is_range_del_table_empty_.store(false, std::memory_order_relaxed);  // 设置range_del_table_非空标志
  }
  UpdateOldestKeyTime();  // 更新最旧键的时间戳,用于TTL(过期时间)管理

  TEST_SYNC_POINT_CALLBACK("MemTable::Add:BeforeReturn:Encoded", &encoded);  // 测试同步点,在返回前注入测试逻辑
  return Status::OK();  // 返回成功状态
}

// Callback from MemTable::Get()
namespace {

struct Saver {
  Status* status;
  const LookupKey* key;
  bool* found_final_value;  // Is value set correctly? Used by KeyMayExist
  bool* merge_in_progress;
  std::string* value;
  PinnableWideColumns* columns;
  SequenceNumber seq;
  std::string* timestamp;
  const MergeOperator* merge_operator;
  // the merge operations encountered;
  MergeContext* merge_context;
  SequenceNumber max_covering_tombstone_seq;
  MemTable* mem;
  Logger* logger;
  Statistics* statistics;
  bool inplace_update_support;
  bool do_merge;
  SystemClock* clock;

  ReadCallback* callback_;
  bool* is_blob_index;
  bool allow_data_in_errors;
  uint32_t protection_bytes_per_key;
  bool CheckCallback(SequenceNumber _seq) {
    if (callback_) {
      return callback_->IsVisible(_seq);
    }
    return true;
  }
};
}  // anonymous namespace

static bool SaveValue(void* arg, const char* entry) {
  TEST_SYNC_POINT_CALLBACK("Memtable::SaveValue:Begin:entry", &entry);
  Saver* s = reinterpret_cast<Saver*>(arg);
  assert(s != nullptr);
  assert(!s->value || !s->columns);

  if (s->protection_bytes_per_key > 0) {
    *(s->status) = MemTable::VerifyEntryChecksum(
        entry, s->protection_bytes_per_key, s->allow_data_in_errors);
    if (!s->status->ok()) {
      ROCKS_LOG_ERROR(s->logger, "In SaveValue: %s", s->status->getState());
      // Memtable entry corrupted
      return false;
    }
  }

  MergeContext* merge_context = s->merge_context;
  SequenceNumber max_covering_tombstone_seq = s->max_covering_tombstone_seq;
  const MergeOperator* merge_operator = s->merge_operator;

  assert(merge_context != nullptr);

  // Refer to comments under MemTable::Add() for entry format.
  // Check that it belongs to same user key.
  uint32_t key_length = 0;
  const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
  assert(key_length >= 8);
  Slice user_key_slice = Slice(key_ptr, key_length - 8);
  const Comparator* user_comparator =
      s->mem->GetInternalKeyComparator().user_comparator();
  size_t ts_sz = user_comparator->timestamp_size();
  if (ts_sz && s->timestamp && max_covering_tombstone_seq > 0) {
    // timestamp should already be set to range tombstone timestamp
    assert(s->timestamp->size() == ts_sz);
  }
  if (user_comparator->EqualWithoutTimestamp(user_key_slice,
                                             s->key->user_key())) {
    // Correct user key
    const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
    ValueType type;
    SequenceNumber seq;
    UnPackSequenceAndType(tag, &seq, &type);
    // If the value is not in the snapshot, skip it
    if (!s->CheckCallback(seq)) {
      return true;  // to continue to the next seq
    }

    if (s->seq == kMaxSequenceNumber) {
      s->seq = seq;
      if (s->seq > max_covering_tombstone_seq) {
        if (ts_sz && s->timestamp != nullptr) {
          // `timestamp` was set to range tombstone's timestamp before
          // `SaveValue` is ever called. This key has a higher sequence number
          // than range tombstone, and is the key with the highest seqno across
          // all keys with this user_key, so we update timestamp here.
          Slice ts = ExtractTimestampFromUserKey(user_key_slice, ts_sz);
          s->timestamp->assign(ts.data(), ts_sz);
        }
      } else {
        s->seq = max_covering_tombstone_seq;
      }
    }

    if (ts_sz > 0 && s->timestamp != nullptr) {
      if (!s->timestamp->empty()) {
        assert(ts_sz == s->timestamp->size());
      }
      // TODO optimize for smaller size ts
      const std::string kMaxTs(ts_sz, '\xff');
      if (s->timestamp->empty() ||
          user_comparator->CompareTimestamp(*(s->timestamp), kMaxTs) == 0) {
        Slice ts = ExtractTimestampFromUserKey(user_key_slice, ts_sz);
        s->timestamp->assign(ts.data(), ts_sz);
      }
    }

    if ((type == kTypeValue || type == kTypeMerge || type == kTypeBlobIndex ||
         type == kTypeWideColumnEntity || type == kTypeDeletion ||
         type == kTypeSingleDeletion || type == kTypeDeletionWithTimestamp) &&
        max_covering_tombstone_seq > seq) {
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeBlobIndex: {
        if (!s->do_merge) {
          *(s->status) = Status::NotSupported(
              "GetMergeOperands not supported by stacked BlobDB");
          *(s->found_final_value) = true;
          return false;
        }

        if (*(s->merge_in_progress)) {
          *(s->status) = Status::NotSupported(
              "Merge operator not supported by stacked BlobDB");
          *(s->found_final_value) = true;
          return false;
        }

        if (s->is_blob_index == nullptr) {
          ROCKS_LOG_ERROR(s->logger, "Encountered unexpected blob index.");
          *(s->status) = Status::NotSupported(
              "Encountered unexpected blob index. Please open DB with "
              "ROCKSDB_NAMESPACE::blob_db::BlobDB.");
          *(s->found_final_value) = true;
          return false;
        }

        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }

        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);

        *(s->status) = Status::OK();

        if (s->value) {
          s->value->assign(v.data(), v.size());
        } else if (s->columns) {
          s->columns->SetPlainValue(v);
        }

        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }

        *(s->found_final_value) = true;
        *(s->is_blob_index) = true;

        return false;
      }
      case kTypeValue: {
        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }

        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);

        *(s->status) = Status::OK();

        if (!s->do_merge) {
          // Preserve the value with the goal of returning it as part of
          // raw merge operands to the user
          // TODO(yanqin) update MergeContext so that timestamps information
          // can also be retained.

          merge_context->PushOperand(
              v, s->inplace_update_support == false /* operand_pinned */);
        } else if (*(s->merge_in_progress)) {
          assert(s->do_merge);

          if (s->value || s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), &v,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* result_operand */ nullptr,
                /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              if (s->value) {
                *(s->value) = std::move(result);
              } else {
                assert(s->columns);
                s->columns->SetPlainValue(std::move(result));
              }
            }
          }
        } else if (s->value) {
          s->value->assign(v.data(), v.size());
        } else if (s->columns) {
          s->columns->SetPlainValue(v);
        }

        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }

        *(s->found_final_value) = true;

        if (s->is_blob_index != nullptr) {
          *(s->is_blob_index) = false;
        }

        return false;
      }
      case kTypeWideColumnEntity: {
        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadLock();
        }

        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);

        *(s->status) = Status::OK();

        if (!s->do_merge) {
          // Preserve the value with the goal of returning it as part of
          // raw merge operands to the user

          Slice value_of_default;
          *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
              v, value_of_default);

          if (s->status->ok()) {
            merge_context->PushOperand(
                value_of_default,
                s->inplace_update_support == false /* operand_pinned */);
          }
        } else if (*(s->merge_in_progress)) {
          assert(s->do_merge);

          if (s->value) {
            Slice value_of_default;
            *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
                v, value_of_default);
            if (s->status->ok()) {
              // `op_failure_scope` (an output parameter) is not provided (set
              // to nullptr) since a failure must be propagated regardless of
              // its value.
              *(s->status) = MergeHelper::TimedFullMerge(
                  merge_operator, s->key->user_key(), &value_of_default,
                  merge_context->GetOperands(), s->value, s->logger,
                  s->statistics, s->clock, /* result_operand */ nullptr,
                  /* update_num_ops_stats */ true,
                  /* op_failure_scope */ nullptr);
            }
          } else if (s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMergeWithEntity(
                merge_operator, s->key->user_key(), v,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              *(s->status) = s->columns->SetWideColumnValue(std::move(result));
            }
          }
        } else if (s->value) {
          Slice value_of_default;
          *(s->status) = WideColumnSerialization::GetValueOfDefaultColumn(
              v, value_of_default);
          if (s->status->ok()) {
            s->value->assign(value_of_default.data(), value_of_default.size());
          }
        } else if (s->columns) {
          *(s->status) = s->columns->SetWideColumnValue(v);
        }

        if (s->inplace_update_support) {
          s->mem->GetLock(s->key->user_key())->ReadUnlock();
        }

        *(s->found_final_value) = true;

        if (s->is_blob_index != nullptr) {
          *(s->is_blob_index) = false;
        }

        return false;
      }
      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion: {
        if (*(s->merge_in_progress)) {
          if (s->value || s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), nullptr,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* result_operand */ nullptr,
                /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              if (s->value) {
                *(s->value) = std::move(result);
              } else {
                assert(s->columns);
                s->columns->SetPlainValue(std::move(result));
              }
            }
          } else {
            // We have found a final value (a base deletion) and have newer
            // merge operands that we do not intend to merge. Nothing remains
            // to be done so assign status to OK.
            *(s->status) = Status::OK();
          }
        } else {
          *(s->status) = Status::NotFound();
        }
        *(s->found_final_value) = true;
        return false;
      }
      case kTypeMerge: {
        if (!merge_operator) {
          *(s->status) = Status::InvalidArgument(
              "merge_operator is not properly initialized.");
          // Normally we continue the loop (return true) when we see a merge
          // operand.  But in case of an error, we should stop the loop
          // immediately and pretend we have found the value to stop further
          // seek.  Otherwise, the later call will override this error status.
          *(s->found_final_value) = true;
          return false;
        }
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        *(s->merge_in_progress) = true;
        merge_context->PushOperand(
            v, s->inplace_update_support == false /* operand_pinned */);
        PERF_COUNTER_ADD(internal_merge_point_lookup_count, 1);

        if (s->do_merge && merge_operator->ShouldMerge(
                               merge_context->GetOperandsDirectionBackward())) {
          if (s->value || s->columns) {
            std::string result;
            // `op_failure_scope` (an output parameter) is not provided (set to
            // nullptr) since a failure must be propagated regardless of its
            // value.
            *(s->status) = MergeHelper::TimedFullMerge(
                merge_operator, s->key->user_key(), nullptr,
                merge_context->GetOperands(), &result, s->logger, s->statistics,
                s->clock, /* result_operand */ nullptr,
                /* update_num_ops_stats */ true,
                /* op_failure_scope */ nullptr);

            if (s->status->ok()) {
              if (s->value) {
                *(s->value) = std::move(result);
              } else {
                assert(s->columns);
                s->columns->SetPlainValue(std::move(result));
              }
            }
          }

          *(s->found_final_value) = true;
          return false;
        }
        return true;
      }
      default: {
        std::string msg("Corrupted value not expected.");
        if (s->allow_data_in_errors) {
          msg.append("Unrecognized value type: " +
                     std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " + user_key_slice.ToString(/*hex=*/true) +
                     ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
        }
        *(s->status) = Status::Corruption(msg.c_str());
        return false;
      }
    }
  }

  // s->state could be Corrupt, merge or notfound
  return false;
}

bool MemTable::Get(const LookupKey& key, std::string* value,
                   PinnableWideColumns* columns, std::string* timestamp,
                   Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   bool immutable_memtable, ReadCallback* callback,
                   bool* is_blob_index, bool do_merge) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return false;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
      NewRangeTombstoneIterator(read_opts,
                                GetInternalKeySeqno(key.internal_key()),
                                immutable_memtable));
  if (range_del_iter != nullptr) {
    SequenceNumber covering_seq =
        range_del_iter->MaxCoveringTombstoneSeqnum(key.user_key());
    if (covering_seq > *max_covering_tombstone_seq) {
      *max_covering_tombstone_seq = covering_seq;
      if (timestamp) {
        // Will be overwritten in SaveValue() if there is a point key with
        // a higher seqno.
        timestamp->assign(range_del_iter->timestamp().data(),
                          range_del_iter->timestamp().size());
      }
    }
  }

  bool found_final_value = false;
  bool merge_in_progress = s->IsMergeInProgress();
  bool may_contain = true;
  size_t ts_sz = GetInternalKeyComparator().user_comparator()->timestamp_size();
  Slice user_key_without_ts = StripTimestampFromUserKey(key.user_key(), ts_sz);
  bool bloom_checked = false;
  if (bloom_filter_) {
    // when both memtable_whole_key_filtering and prefix_extractor_ are set,
    // only do whole key filtering for Get() to save CPU
    if (moptions_.memtable_whole_key_filtering) {
      may_contain = bloom_filter_->MayContain(user_key_without_ts);
      bloom_checked = true;
    } else {
      assert(prefix_extractor_);
      if (prefix_extractor_->InDomain(user_key_without_ts)) {
        may_contain = bloom_filter_->MayContain(
            prefix_extractor_->Transform(user_key_without_ts));
        bloom_checked = true;
      }
    }
  }

  if (bloom_filter_ && !may_contain) {
    // iter is null if prefix bloom says the key does not exist
    PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
    *seq = kMaxSequenceNumber;
  } else {
    if (bloom_checked) {
      PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
    }
    GetFromTable(key, *max_covering_tombstone_seq, do_merge, callback,
                 is_blob_index, value, columns, timestamp, s, merge_context,
                 seq, &found_final_value, &merge_in_progress);
  }

  // No change to value, since we have not yet found a Put/Delete
  // Propagate corruption error
  if (!found_final_value && merge_in_progress && !s->IsCorruption()) {
    *s = Status::MergeInProgress();
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
  return found_final_value;
}

void MemTable::GetFromTable(const LookupKey& key,
                            SequenceNumber max_covering_tombstone_seq,
                            bool do_merge, ReadCallback* callback,
                            bool* is_blob_index, std::string* value,
                            PinnableWideColumns* columns,
                            std::string* timestamp, Status* s,
                            MergeContext* merge_context, SequenceNumber* seq,
                            bool* found_final_value, bool* merge_in_progress) {
  Saver saver;
  saver.status = s;
  saver.found_final_value = found_final_value;
  saver.merge_in_progress = merge_in_progress;
  saver.key = &key;
  saver.value = value;
  saver.columns = columns;
  saver.timestamp = timestamp;
  saver.seq = kMaxSequenceNumber;
  saver.mem = this;
  saver.merge_context = merge_context;
  saver.max_covering_tombstone_seq = max_covering_tombstone_seq;
  saver.merge_operator = moptions_.merge_operator;
  saver.logger = moptions_.info_log;
  saver.inplace_update_support = moptions_.inplace_update_support;
  saver.statistics = moptions_.statistics;
  saver.clock = clock_;
  saver.callback_ = callback;
  saver.is_blob_index = is_blob_index;
  saver.do_merge = do_merge;
  saver.allow_data_in_errors = moptions_.allow_data_in_errors;
  saver.protection_bytes_per_key = moptions_.protection_bytes_per_key;
  table_->Get(key, &saver, SaveValue);
  *seq = saver.seq;
}

void MemTable::MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                        ReadCallback* callback, bool immutable_memtable) {
  // The sequence number is updated synchronously in version_set.h
  if (IsEmpty()) {
    // Avoiding recording stats for speed.
    return;
  }
  PERF_TIMER_GUARD(get_from_memtable_time);

  // For now, memtable Bloom filter is effectively disabled if there are any
  // range tombstones. This is the simplest way to ensure range tombstones are
  // handled. TODO: allow Bloom checks where max_covering_tombstone_seq==0
  bool no_range_del = read_options.ignore_range_deletions ||
                      is_range_del_table_empty_.load(std::memory_order_relaxed);
  MultiGetRange temp_range(*range, range->begin(), range->end());
  if (bloom_filter_ && no_range_del) {
    bool whole_key =
        !prefix_extractor_ || moptions_.memtable_whole_key_filtering;
    std::array<Slice, MultiGetContext::MAX_BATCH_SIZE> bloom_keys;
    std::array<bool, MultiGetContext::MAX_BATCH_SIZE> may_match;
    std::array<size_t, MultiGetContext::MAX_BATCH_SIZE> range_indexes;
    int num_keys = 0;
    for (auto iter = temp_range.begin(); iter != temp_range.end(); ++iter) {
      if (whole_key) {
        bloom_keys[num_keys] = iter->ukey_without_ts;
        range_indexes[num_keys++] = iter.index();
      } else if (prefix_extractor_->InDomain(iter->ukey_without_ts)) {
        bloom_keys[num_keys] =
            prefix_extractor_->Transform(iter->ukey_without_ts);
        range_indexes[num_keys++] = iter.index();
      }
    }
    bloom_filter_->MayContain(num_keys, &bloom_keys[0], &may_match[0]);
    for (int i = 0; i < num_keys; ++i) {
      if (!may_match[i]) {
        temp_range.SkipIndex(range_indexes[i]);
        PERF_COUNTER_ADD(bloom_memtable_miss_count, 1);
      } else {
        PERF_COUNTER_ADD(bloom_memtable_hit_count, 1);
      }
    }
  }
  for (auto iter = temp_range.begin(); iter != temp_range.end(); ++iter) {
    bool found_final_value{false};
    bool merge_in_progress = iter->s->IsMergeInProgress();
    if (!no_range_del) {
      std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
          NewRangeTombstoneIteratorInternal(
              read_options, GetInternalKeySeqno(iter->lkey->internal_key()),
              immutable_memtable));
      SequenceNumber covering_seq =
          range_del_iter->MaxCoveringTombstoneSeqnum(iter->lkey->user_key());
      if (covering_seq > iter->max_covering_tombstone_seq) {
        iter->max_covering_tombstone_seq = covering_seq;
        if (iter->timestamp) {
          // Will be overwritten in SaveValue() if there is a point key with
          // a higher seqno.
          iter->timestamp->assign(range_del_iter->timestamp().data(),
                                  range_del_iter->timestamp().size());
        }
      }
    }
    SequenceNumber dummy_seq;
    GetFromTable(*(iter->lkey), iter->max_covering_tombstone_seq, true,
                 callback, &iter->is_blob_index,
                 iter->value ? iter->value->GetSelf() : nullptr, iter->columns,
                 iter->timestamp, iter->s, &(iter->merge_context), &dummy_seq,
                 &found_final_value, &merge_in_progress);

    if (!found_final_value && merge_in_progress) {
      *(iter->s) = Status::MergeInProgress();
    }

    if (found_final_value) {
      if (iter->value) {
        iter->value->PinSelf();
        range->AddValueSize(iter->value->size());
      } else {
        assert(iter->columns);
        range->AddValueSize(iter->columns->serialized_size());
      }

      range->MarkKeyDone(iter);
      RecordTick(moptions_.statistics, MEMTABLE_HIT);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        // Set all remaining keys in range to Abort
        for (auto range_iter = range->begin(); range_iter != range->end();
             ++range_iter) {
          range->MarkKeyDone(range_iter);
          *(range_iter->s) = Status::Aborted();
        }
        break;
      }
    }
  }
  PERF_COUNTER_ADD(get_from_memtable_count, 1);
}

Status MemTable::Update(SequenceNumber seq, ValueType value_type,
                        const Slice& key, const Slice& value,
                        const ProtectionInfoKVOS64* kv_prot_info) {
  LookupKey lkey(key, seq);
  Slice mem_key = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), mem_key.data());

  if (iter->Valid()) {
    // Refer to comments under MemTable::Add() for entry format.
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      SequenceNumber existing_seq;
      UnPackSequenceAndType(tag, &existing_seq, &type);
      assert(existing_seq != seq);
      if (type == value_type) {
        Slice prev_value = GetLengthPrefixedSlice(key_ptr + key_length);
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        uint32_t new_size = static_cast<uint32_t>(value.size());

        // Update value, if new value size  <= previous value size
        if (new_size <= prev_size) {
          char* p =
              EncodeVarint32(const_cast<char*>(key_ptr) + key_length, new_size);
          WriteLock wl(GetLock(lkey.user_key()));
          memcpy(p, value.data(), value.size());
          assert((unsigned)((p + value.size()) - entry) ==
                 (unsigned)(VarintLength(key_length) + key_length +
                            VarintLength(value.size()) + value.size()));
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            // `seq` is swallowed and `existing_seq` prevails.
            updated_kv_prot_info.UpdateS(seq, existing_seq);
            UpdateEntryChecksum(&updated_kv_prot_info, key, value, type,
                                existing_seq, p + value.size());
            Slice encoded(entry, p + value.size() - entry);
            return VerifyEncodedEntry(encoded, updated_kv_prot_info);
          } else {
            UpdateEntryChecksum(nullptr, key, value, type, existing_seq,
                                p + value.size());
          }
          return Status::OK();
        }
      }
    }
  }

  // The latest value is not value_type or key doesn't exist
  return Add(seq, value_type, key, value, kv_prot_info);
}

Status MemTable::UpdateCallback(SequenceNumber seq, const Slice& key,
                                const Slice& delta,
                                const ProtectionInfoKVOS64* kv_prot_info) {
  LookupKey lkey(key, seq);
  Slice memkey = lkey.memtable_key();

  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(lkey.internal_key(), memkey.data());

  if (iter->Valid()) {
    // Refer to comments under MemTable::Add() for entry format.
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Equal(
            Slice(key_ptr, key_length - 8), lkey.user_key())) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      ValueType type;
      uint64_t existing_seq;
      UnPackSequenceAndType(tag, &existing_seq, &type);
      if (type == kTypeValue) {
        Slice prev_value = GetLengthPrefixedSlice(key_ptr + key_length);
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());

        char* prev_buffer = const_cast<char*>(prev_value.data());
        uint32_t new_prev_size = prev_size;

        std::string str_value;
        WriteLock wl(GetLock(lkey.user_key()));
        auto status = moptions_.inplace_callback(prev_buffer, &new_prev_size,
                                                 delta, &str_value);
        if (status == UpdateStatus::UPDATED_INPLACE) {
          // Value already updated by callback.
          assert(new_prev_size <= prev_size);
          if (new_prev_size < prev_size) {
            // overwrite the new prev_size
            char* p = EncodeVarint32(const_cast<char*>(key_ptr) + key_length,
                                     new_prev_size);
            if (VarintLength(new_prev_size) < VarintLength(prev_size)) {
              // shift the value buffer as well.
              memcpy(p, prev_buffer, new_prev_size);
              prev_buffer = p;
            }
          }
          RecordTick(moptions_.statistics, NUMBER_KEYS_UPDATED);
          UpdateFlushState();
          Slice new_value(prev_buffer, new_prev_size);
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            // `seq` is swallowed and `existing_seq` prevails.
            updated_kv_prot_info.UpdateS(seq, existing_seq);
            updated_kv_prot_info.UpdateV(delta, new_value);
            Slice encoded(entry, prev_buffer + new_prev_size - entry);
            UpdateEntryChecksum(&updated_kv_prot_info, key, new_value, type,
                                existing_seq, prev_buffer + new_prev_size);
            return VerifyEncodedEntry(encoded, updated_kv_prot_info);
          } else {
            UpdateEntryChecksum(nullptr, key, new_value, type, existing_seq,
                                prev_buffer + new_prev_size);
          }
          return Status::OK();
        } else if (status == UpdateStatus::UPDATED) {
          Status s;
          if (kv_prot_info != nullptr) {
            ProtectionInfoKVOS64 updated_kv_prot_info(*kv_prot_info);
            updated_kv_prot_info.UpdateV(delta, str_value);
            s = Add(seq, kTypeValue, key, Slice(str_value),
                    &updated_kv_prot_info);
          } else {
            s = Add(seq, kTypeValue, key, Slice(str_value),
                    nullptr /* kv_prot_info */);
          }
          RecordTick(moptions_.statistics, NUMBER_KEYS_WRITTEN);
          UpdateFlushState();
          return s;
        } else if (status == UpdateStatus::UPDATE_FAILED) {
          // `UPDATE_FAILED` is named incorrectly. It indicates no update
          // happened. It does not indicate a failure happened.
          UpdateFlushState();
          return Status::OK();
        }
      }
    }
  }
  // The latest value is not `kTypeValue` or key doesn't exist
  return Status::NotFound();
}

size_t MemTable::CountSuccessiveMergeEntries(const LookupKey& key) {
  Slice memkey = key.memtable_key();

  // A total ordered iterator is costly for some memtablerep (prefix aware
  // reps). By passing in the user key, we allow efficient iterator creation.
  // The iterator only needs to be ordered within the same user key.
  std::unique_ptr<MemTableRep::Iterator> iter(
      table_->GetDynamicPrefixIterator());
  iter->Seek(key.internal_key(), memkey.data());

  size_t num_successive_merges = 0;

  for (; iter->Valid(); iter->Next()) {
    const char* entry = iter->key();
    uint32_t key_length = 0;
    const char* iter_key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (!comparator_.comparator.user_comparator()->Equal(
            Slice(iter_key_ptr, key_length - 8), key.user_key())) {
      break;
    }

    const uint64_t tag = DecodeFixed64(iter_key_ptr + key_length - 8);
    ValueType type;
    uint64_t unused;
    UnPackSequenceAndType(tag, &unused, &type);
    if (type != kTypeMerge) {
      break;
    }

    ++num_successive_merges;
  }

  return num_successive_merges;
}

void MemTableRep::Get(const LookupKey& k, void* callback_args,
                      bool (*callback_func)(void* arg, const char* entry)) {
  auto iter = GetDynamicPrefixIterator();
  for (iter->Seek(k.internal_key(), k.memtable_key().data());
       iter->Valid() && callback_func(callback_args, iter->key());
       iter->Next()) {
  }
}

void MemTable::RefLogContainingPrepSection(uint64_t log) {
  assert(log > 0);
  auto cur = min_prep_log_referenced_.load();
  while ((log < cur || cur == 0) &&
         !min_prep_log_referenced_.compare_exchange_strong(cur, log)) {
    cur = min_prep_log_referenced_.load();
  }
}

uint64_t MemTable::GetMinLogContainingPrepSection() {
  return min_prep_log_referenced_.load();
}

}  // namespace ROCKSDB_NAMESPACE
