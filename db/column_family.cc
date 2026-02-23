//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/column_family.h"

#include <algorithm>
#include <cinttypes>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction_picker.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/db_impl/db_impl.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/range_del_aggregator.h"
#include "db/table_properties_collector.h"
#include "db/version_set.h"
#include "db/write_controller.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/compression.h"

namespace ROCKSDB_NAMESPACE {

ColumnFamilyHandleImpl::ColumnFamilyHandleImpl(
    ColumnFamilyData* column_family_data, DBImpl* db, InstrumentedMutex* mutex)
    : cfd_(column_family_data), db_(db), mutex_(mutex) {
  if (cfd_ != nullptr) {
    cfd_->Ref();
  }
}

ColumnFamilyHandleImpl::~ColumnFamilyHandleImpl() {
  if (cfd_ != nullptr) {
    for (auto& listener : cfd_->ioptions()->listeners) {
      listener->OnColumnFamilyHandleDeletionStarted(this);
    }
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    // Need to hold some shared pointers owned by the initial_cf_options
    // before final cleaning up finishes.
    ColumnFamilyOptions initial_cf_options_copy = cfd_->initial_cf_options();
    JobContext job_context(0);
    mutex_->Lock();
    bool dropped = cfd_->IsDropped();
    if (cfd_->UnrefAndTryDelete()) {
      if (dropped) {
        db_->FindObsoleteFiles(&job_context, false, true);
      }
    }
    mutex_->Unlock();
    if (job_context.HaveSomethingToDelete()) {
      bool defer_purge =
          db_->immutable_db_options().avoid_unnecessary_blocking_io;
      db_->PurgeObsoleteFiles(job_context, defer_purge);
    }
    job_context.Clean();
  }
}

uint32_t ColumnFamilyHandleImpl::GetID() const { return cfd()->GetID(); }

const std::string& ColumnFamilyHandleImpl::GetName() const {
  return cfd()->GetName();
}

Status ColumnFamilyHandleImpl::GetDescriptor(ColumnFamilyDescriptor* desc) {
  // accessing mutable cf-options requires db mutex.
  InstrumentedMutexLock l(mutex_);
  *desc = ColumnFamilyDescriptor(cfd()->GetName(), cfd()->GetLatestCFOptions());
  return Status::OK();
}

const Comparator* ColumnFamilyHandleImpl::GetComparator() const {
  return cfd()->user_comparator();
}

void GetIntTblPropCollectorFactory(
    const ImmutableCFOptions& ioptions,
    IntTblPropCollectorFactories* int_tbl_prop_collector_factories) {
  assert(int_tbl_prop_collector_factories);

  auto& collector_factories = ioptions.table_properties_collector_factories;
  for (size_t i = 0; i < ioptions.table_properties_collector_factories.size();
       ++i) {
    assert(collector_factories[i]);
    int_tbl_prop_collector_factories->emplace_back(
        new UserKeyTablePropertiesCollectorFactory(collector_factories[i]));
  }
}

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options) {
  if (!cf_options.compression_per_level.empty()) {
    for (size_t level = 0; level < cf_options.compression_per_level.size();
         ++level) {
      if (!CompressionTypeSupported(cf_options.compression_per_level[level])) {
        return Status::InvalidArgument(
            "Compression type " +
            CompressionTypeToString(cf_options.compression_per_level[level]) +
            " is not linked with the binary.");
      }
    }
  } else {
    if (!CompressionTypeSupported(cf_options.compression)) {
      return Status::InvalidArgument(
          "Compression type " +
          CompressionTypeToString(cf_options.compression) +
          " is not linked with the binary.");
    }
  }
  if (cf_options.compression_opts.zstd_max_train_bytes > 0) {
    if (cf_options.compression_opts.use_zstd_dict_trainer) {
      if (!ZSTD_TrainDictionarySupported()) {
        return Status::InvalidArgument(
            "zstd dictionary trainer cannot be used because ZSTD 1.1.3+ "
            "is not linked with the binary.");
      }
    } else if (!ZSTD_FinalizeDictionarySupported()) {
      return Status::InvalidArgument(
          "zstd finalizeDictionary cannot be used because ZSTD 1.4.5+ "
          "is not linked with the binary.");
    }
    if (cf_options.compression_opts.max_dict_bytes == 0) {
      return Status::InvalidArgument(
          "The dictionary size limit (`CompressionOptions::max_dict_bytes`) "
          "should be nonzero if we're using zstd's dictionary generator.");
    }
  }

  if (!CompressionTypeSupported(cf_options.blob_compression_type)) {
    std::ostringstream oss;
    oss << "The specified blob compression type "
        << CompressionTypeToString(cf_options.blob_compression_type)
        << " is not available.";

    return Status::InvalidArgument(oss.str());
  }

  return Status::OK();
}

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options) {
  if (cf_options.inplace_update_support) {
    return Status::InvalidArgument(
        "In-place memtable updates (inplace_update_support) is not compatible "
        "with concurrent writes (allow_concurrent_memtable_write)");
  }
  if (!cf_options.memtable_factory->IsInsertConcurrentlySupported()) {
    return Status::InvalidArgument(
        "Memtable doesn't concurrent writes (allow_concurrent_memtable_write)");
  }
  return Status::OK();
}

Status CheckCFPathsSupported(const DBOptions& db_options,
                             const ColumnFamilyOptions& cf_options) {
  // More than one cf_paths are supported only in universal
  // and level compaction styles. This function also checks the case
  // in which cf_paths is not specified, which results in db_paths
  // being used.
  if ((cf_options.compaction_style != kCompactionStyleUniversal) &&
      (cf_options.compaction_style != kCompactionStyleLevel)) {
    if (cf_options.cf_paths.size() > 1) {
      return Status::NotSupported(
          "More than one CF paths are only supported in "
          "universal and level compaction styles. ");
    } else if (cf_options.cf_paths.empty() && db_options.db_paths.size() > 1) {
      return Status::NotSupported(
          "More than one DB paths are only supported in "
          "universal and level compaction styles. ");
    }
  }
  return Status::OK();
}

namespace {
const uint64_t kDefaultTtl = 0xfffffffffffffffe;
const uint64_t kDefaultPeriodicCompSecs = 0xfffffffffffffffe;
}  // anonymous namespace

ColumnFamilyOptions SanitizeOptions(const ImmutableDBOptions& db_options,
                                    const ColumnFamilyOptions& src) {
  ColumnFamilyOptions result = src;
  size_t clamp_max = std::conditional<
      sizeof(size_t) == 4, std::integral_constant<size_t, 0xffffffff>,
      std::integral_constant<uint64_t, 64ull << 30>>::type::value;
  ClipToRange(&result.write_buffer_size, (static_cast<size_t>(64)) << 10,
              clamp_max);
  // if user sets arena_block_size, we trust user to use this value. Otherwise,
  // calculate a proper value from writer_buffer_size;
  if (result.arena_block_size <= 0) {
    result.arena_block_size =
        std::min(size_t{1024 * 1024}, result.write_buffer_size / 8);

    // Align up to 4k
    const size_t align = 4 * 1024;
    result.arena_block_size =
        ((result.arena_block_size + align - 1) / align) * align;
  }
  result.min_write_buffer_number_to_merge =
      std::min(result.min_write_buffer_number_to_merge,
               result.max_write_buffer_number - 1);
  if (result.min_write_buffer_number_to_merge < 1) {
    result.min_write_buffer_number_to_merge = 1;
  }

  if (db_options.atomic_flush && result.min_write_buffer_number_to_merge > 1) {
    ROCKS_LOG_WARN(
        db_options.logger,
        "Currently, if atomic_flush is true, then triggering flush for any "
        "column family internally (non-manual flush) will trigger flushing "
        "all column families even if the number of memtables is smaller "
        "min_write_buffer_number_to_merge. Therefore, configuring "
        "min_write_buffer_number_to_merge > 1 is not compatible and should "
        "be satinized to 1. Not doing so will lead to data loss and "
        "inconsistent state across multiple column families when WAL is "
        "disabled, which is a common setting for atomic flush");

    result.min_write_buffer_number_to_merge = 1;
  }

  if (result.num_levels < 1) {
    result.num_levels = 1;
  }
  if (result.compaction_style == kCompactionStyleLevel &&
      result.num_levels < 2) {
    result.num_levels = 2;
  }

  if (result.compaction_style == kCompactionStyleUniversal &&
      db_options.allow_ingest_behind && result.num_levels < 3) {
    result.num_levels = 3;
  }

  if (result.max_write_buffer_number < 2) {
    result.max_write_buffer_number = 2;
  }
  // fall back max_write_buffer_number_to_maintain if
  // max_write_buffer_size_to_maintain is not set
  if (result.max_write_buffer_size_to_maintain < 0) {
    result.max_write_buffer_size_to_maintain =
        result.max_write_buffer_number *
        static_cast<int64_t>(result.write_buffer_size);
  } else if (result.max_write_buffer_size_to_maintain == 0 &&
             result.max_write_buffer_number_to_maintain < 0) {
    result.max_write_buffer_number_to_maintain = result.max_write_buffer_number;
  }
  // bloom filter size shouldn't exceed 1/4 of memtable size.
  if (result.memtable_prefix_bloom_size_ratio > 0.25) {
    result.memtable_prefix_bloom_size_ratio = 0.25;
  } else if (result.memtable_prefix_bloom_size_ratio < 0) {
    result.memtable_prefix_bloom_size_ratio = 0;
  }

  if (!result.prefix_extractor) {
    assert(result.memtable_factory);
    Slice name = result.memtable_factory->Name();
    if (name.compare("HashSkipListRepFactory") == 0 ||
        name.compare("HashLinkListRepFactory") == 0) {
      result.memtable_factory = std::make_shared<SkipListFactory>();
    }
  }

  if (result.compaction_style == kCompactionStyleFIFO) {
    // since we delete level0 files in FIFO compaction when there are too many
    // of them, these options don't really mean anything
    result.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    result.level0_stop_writes_trigger = std::numeric_limits<int>::max();
  }

  if (result.max_bytes_for_level_multiplier <= 0) {
    result.max_bytes_for_level_multiplier = 1;
  }

  if (result.level0_file_num_compaction_trigger == 0) {
    ROCKS_LOG_WARN(db_options.logger,
                   "level0_file_num_compaction_trigger cannot be 0");
    result.level0_file_num_compaction_trigger = 1;
  }

  if (result.level0_stop_writes_trigger <
          result.level0_slowdown_writes_trigger ||
      result.level0_slowdown_writes_trigger <
          result.level0_file_num_compaction_trigger) {
    ROCKS_LOG_WARN(db_options.logger,
                   "This condition must be satisfied: "
                   "level0_stop_writes_trigger(%d) >= "
                   "level0_slowdown_writes_trigger(%d) >= "
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
    if (result.level0_slowdown_writes_trigger <
        result.level0_file_num_compaction_trigger) {
      result.level0_slowdown_writes_trigger =
          result.level0_file_num_compaction_trigger;
    }
    if (result.level0_stop_writes_trigger <
        result.level0_slowdown_writes_trigger) {
      result.level0_stop_writes_trigger = result.level0_slowdown_writes_trigger;
    }
    ROCKS_LOG_WARN(db_options.logger,
                   "Adjust the value to "
                   "level0_stop_writes_trigger(%d)"
                   "level0_slowdown_writes_trigger(%d)"
                   "level0_file_num_compaction_trigger(%d)",
                   result.level0_stop_writes_trigger,
                   result.level0_slowdown_writes_trigger,
                   result.level0_file_num_compaction_trigger);
  }

  if (result.soft_pending_compaction_bytes_limit == 0) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  } else if (result.hard_pending_compaction_bytes_limit > 0 &&
             result.soft_pending_compaction_bytes_limit >
                 result.hard_pending_compaction_bytes_limit) {
    result.soft_pending_compaction_bytes_limit =
        result.hard_pending_compaction_bytes_limit;
  }

  // When the DB is stopped, it's possible that there are some .trash files that
  // were not deleted yet, when we open the DB we will find these .trash files
  // and schedule them to be deleted (or delete immediately if SstFileManager
  // was not used)
  auto sfm =
      static_cast<SstFileManagerImpl*>(db_options.sst_file_manager.get());
  for (size_t i = 0; i < result.cf_paths.size(); i++) {
    DeleteScheduler::CleanupDirectory(db_options.env, sfm,
                                      result.cf_paths[i].path)
        .PermitUncheckedError();
  }

  if (result.cf_paths.empty()) {
    result.cf_paths = db_options.db_paths;
  }

  if (result.level_compaction_dynamic_level_bytes) {
    if (result.compaction_style != kCompactionStyleLevel) {
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "level_compaction_dynamic_level_bytes only makes sense"
                     "for level-based compaction");
      result.level_compaction_dynamic_level_bytes = false;
    } else if (result.cf_paths.size() > 1U) {
      // we don't yet know how to make both of this feature and multiple
      // DB path work.
      ROCKS_LOG_WARN(db_options.info_log.get(),
                     "multiple cf_paths/db_paths and"
                     "level_compaction_dynamic_level_bytes"
                     "can't be used together");
      result.level_compaction_dynamic_level_bytes = false;
    }
  }

  if (result.max_compaction_bytes == 0) {
    result.max_compaction_bytes = result.target_file_size_base * 25;
  }

  bool is_block_based_table = (result.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));

  const uint64_t kAdjustedTtl = 30 * 24 * 60 * 60;
  if (result.ttl == kDefaultTtl) {
    if (is_block_based_table) {
      // For FIFO, max_open_files is checked in ValidateOptions().
      result.ttl = kAdjustedTtl;
    } else {
      result.ttl = 0;
    }
  }

  const uint64_t kAdjustedPeriodicCompSecs = 30 * 24 * 60 * 60;

  // Turn on periodic compactions and set them to occur once every 30 days if
  // compaction filters are used and periodic_compaction_seconds is set to the
  // default value.
  if (result.compaction_style != kCompactionStyleFIFO) {
    if ((result.compaction_filter != nullptr ||
         result.compaction_filter_factory != nullptr) &&
        result.periodic_compaction_seconds == kDefaultPeriodicCompSecs &&
        is_block_based_table) {
      result.periodic_compaction_seconds = kAdjustedPeriodicCompSecs;
    }
  } else {
    if (result.periodic_compaction_seconds != kDefaultPeriodicCompSecs &&
        result.periodic_compaction_seconds > 0) {
      ROCKS_LOG_WARN(
          db_options.info_log.get(),
          "periodic_compaction_seconds does not support FIFO compaction. You"
          "may want to set option TTL instead.");
    }
  }

  // TTL compactions would work similar to Periodic Compactions in Universal in
  // most of the cases. So, if ttl is set, execute the periodic compaction
  // codepath.
  if (result.compaction_style == kCompactionStyleUniversal && result.ttl != 0) {
    if (result.periodic_compaction_seconds != 0) {
      result.periodic_compaction_seconds =
          std::min(result.ttl, result.periodic_compaction_seconds);
    } else {
      result.periodic_compaction_seconds = result.ttl;
    }
  }

  if (result.periodic_compaction_seconds == kDefaultPeriodicCompSecs) {
    result.periodic_compaction_seconds = 0;
  }

  return result;
}

int SuperVersion::dummy = 0;
void* const SuperVersion::kSVInUse = &SuperVersion::dummy;
void* const SuperVersion::kSVObsolete = nullptr;

SuperVersion::~SuperVersion() {
  for (auto td : to_delete) {
    delete td;
  }
}

SuperVersion* SuperVersion::Ref() {
  refs.fetch_add(1, std::memory_order_relaxed);
  return this;
}

bool SuperVersion::Unref() {
  // fetch_sub returns the previous value of ref
  uint32_t previous_refs = refs.fetch_sub(1);
  assert(previous_refs > 0);
  return previous_refs == 1;
}

void SuperVersion::Cleanup() {
  assert(refs.load(std::memory_order_relaxed) == 0);
  // Since this SuperVersion object is being deleted,
  // decrement reference to the immutable MemtableList
  // this SV object was pointing to.
  imm->Unref(&to_delete);
  MemTable* m = mem->Unref();
  if (m != nullptr) {
    auto* memory_usage = current->cfd()->imm()->current_memory_usage();
    assert(*memory_usage >= m->ApproximateMemoryUsage());
    *memory_usage -= m->ApproximateMemoryUsage();
    to_delete.push_back(m);
  }
  current->Unref();
  cfd->UnrefAndTryDelete();
}

void SuperVersion::Init(ColumnFamilyData* new_cfd, MemTable* new_mem,
                        MemTableListVersion* new_imm, Version* new_current) {
  cfd = new_cfd;
  mem = new_mem;
  imm = new_imm;
  current = new_current;
  cfd->Ref();
  mem->Ref();
  imm->Ref();
  current->Ref();
  refs.store(1, std::memory_order_relaxed);
}

namespace {
void SuperVersionUnrefHandle(void* ptr) {
  // UnrefHandle is called when a thread exits or a ThreadLocalPtr gets
  // destroyed. When the former happens, the thread shouldn't see kSVInUse.
  // When the latter happens, only super_version_ holds a reference
  // to ColumnFamilyData, so no further queries are possible.
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  bool was_last_ref __attribute__((__unused__));
  was_last_ref = sv->Unref();
  // Thread-local SuperVersions can't outlive ColumnFamilyData::super_version_.
  // This is important because we can't do SuperVersion cleanup here.
  // That would require locking DB mutex, which would deadlock because
  // SuperVersionUnrefHandle is called with locked ThreadLocalPtr mutex.
  assert(!was_last_ref);
}
}  // anonymous namespace

std::vector<std::string> ColumnFamilyData::GetDbPaths() const {
  std::vector<std::string> paths;
  paths.reserve(ioptions_.cf_paths.size());
  for (const DbPath& db_path : ioptions_.cf_paths) {
    paths.emplace_back(db_path.path);
  }
  return paths;
}

const uint32_t ColumnFamilyData::kDummyColumnFamilyDataId =
    std::numeric_limits<uint32_t>::max();

ColumnFamilyData::ColumnFamilyData(
    uint32_t id, const std::string& name, Version* _dummy_versions,
    Cache* _table_cache, WriteBufferManager* write_buffer_manager,
    const ColumnFamilyOptions& cf_options, const ImmutableDBOptions& db_options,
    const FileOptions* file_options, ColumnFamilySet* column_family_set,
    BlockCacheTracer* const block_cache_tracer,
    const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
    const std::string& db_session_id)
    : id_(id),
      name_(name),
      dummy_versions_(_dummy_versions),
      current_(nullptr),
      refs_(0),
      initialized_(false),
      dropped_(false),
      internal_comparator_(cf_options.comparator),
      initial_cf_options_(SanitizeOptions(db_options, cf_options)),
      ioptions_(db_options, initial_cf_options_),
      mutable_cf_options_(initial_cf_options_),
      is_delete_range_supported_(
          cf_options.table_factory->IsDeleteRangeSupported()),
      write_buffer_manager_(write_buffer_manager),
      mem_(nullptr),
      imm_(ioptions_.min_write_buffer_number_to_merge,
           ioptions_.max_write_buffer_number_to_maintain,
           ioptions_.max_write_buffer_size_to_maintain),
      super_version_(nullptr),
      super_version_number_(0),
      local_sv_(new ThreadLocalPtr(&SuperVersionUnrefHandle)),
      next_(nullptr),
      prev_(nullptr),
      log_number_(0),
      column_family_set_(column_family_set),
      queued_for_flush_(false),
      queued_for_compaction_(false),
      prev_compaction_needed_bytes_(0),
      allow_2pc_(db_options.allow_2pc),
      last_memtable_id_(0),
      db_paths_registered_(false),
      mempurge_used_(false),
      next_epoch_number_(1) {
  if (id_ != kDummyColumnFamilyDataId) {
    // TODO(cc): RegisterDbPaths can be expensive, considering moving it
    // outside of this constructor which might be called with db mutex held.
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->RegisterDbPaths(GetDbPaths());
    if (s.ok()) {
      db_paths_registered_ = true;
    } else {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to register data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
  Ref();

  // Convert user defined table properties collector factories to internal ones.
  GetIntTblPropCollectorFactory(ioptions_, &int_tbl_prop_collector_factories_);

  // if _dummy_versions is nullptr, then this is a dummy column family.
  if (_dummy_versions != nullptr) {
    internal_stats_.reset(
        new InternalStats(ioptions_.num_levels, ioptions_.clock, this));
    table_cache_.reset(new TableCache(ioptions_, file_options, _table_cache,
                                      block_cache_tracer, io_tracer,
                                      db_session_id));
    blob_file_cache_.reset(
        new BlobFileCache(_table_cache, ioptions(), soptions(), id_,
                          internal_stats_->GetBlobFileReadHist(), io_tracer));
    blob_source_.reset(new BlobSource(ioptions(), db_id, db_session_id,
                                      blob_file_cache_.get()));

    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleUniversal) {
      compaction_picker_.reset(
          new UniversalCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleFIFO) {
      compaction_picker_.reset(
          new FIFOCompactionPicker(ioptions_, &internal_comparator_));
    } else if (ioptions_.compaction_style == kCompactionStyleNone) {
      compaction_picker_.reset(
          new NullCompactionPicker(ioptions_, &internal_comparator_));
      ROCKS_LOG_WARN(ioptions_.logger,
                     "Column family %s does not use any background compaction. "
                     "Compactions can only be done via CompactFiles\n",
                     GetName().c_str());
    } else {
      ROCKS_LOG_ERROR(ioptions_.logger,
                      "Unable to recognize the specified compaction style %d. "
                      "Column family %s will use kCompactionStyleLevel.\n",
                      ioptions_.compaction_style, GetName().c_str());
      compaction_picker_.reset(
          new LevelCompactionPicker(ioptions_, &internal_comparator_));
    }

    if (column_family_set_->NumberOfColumnFamilies() < 10) {
      ROCKS_LOG_INFO(ioptions_.logger,
                     "--------------- Options for column family [%s]:\n",
                     name.c_str());
      initial_cf_options_.Dump(ioptions_.logger);
    } else {
      ROCKS_LOG_INFO(ioptions_.logger, "\t(skipping printing options)\n");
    }
  }

  RecalculateWriteStallConditions(mutable_cf_options_);

  if (cf_options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName()) &&
      cf_options.table_factory->GetOptions<BlockBasedTableOptions>()) {
    const BlockBasedTableOptions* bbto =
        cf_options.table_factory->GetOptions<BlockBasedTableOptions>();
    const auto& options_overrides = bbto->cache_usage_options.options_overrides;
    const auto file_metadata_charged =
        options_overrides.at(CacheEntryRole::kFileMetadata).charged;
    if (bbto->block_cache &&
        file_metadata_charged == CacheEntryRoleOptions::Decision::kEnabled) {
      // TODO(hx235): Add a `ConcurrentCacheReservationManager` at DB scope
      // responsible for reservation of `ObsoleteFileInfo` so that we can keep
      // this `file_metadata_cache_res_mgr_` nonconcurrent
      file_metadata_cache_res_mgr_.reset(new ConcurrentCacheReservationManager(
          std::make_shared<
              CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>>(
              bbto->block_cache)));
    }
  }
}

// DB mutex held
ColumnFamilyData::~ColumnFamilyData() {
  assert(refs_.load(std::memory_order_relaxed) == 0);
  // remove from linked list
  auto prev = prev_;
  auto next = next_;
  prev->next_ = next;
  next->prev_ = prev;

  if (!dropped_ && column_family_set_ != nullptr) {
    // If it's dropped, it's already removed from column family set
    // If column_family_set_ == nullptr, this is dummy CFD and not in
    // ColumnFamilySet
    column_family_set_->RemoveColumnFamily(this);
  }

  if (current_ != nullptr) {
    current_->Unref();
  }

  // It would be wrong if this ColumnFamilyData is in flush_queue_ or
  // compaction_queue_ and we destroyed it
  assert(!queued_for_flush_);
  assert(!queued_for_compaction_);
  assert(super_version_ == nullptr);

  if (dummy_versions_ != nullptr) {
    // List must be empty
    assert(dummy_versions_->Next() == dummy_versions_);
    bool deleted __attribute__((__unused__));
    deleted = dummy_versions_->Unref();
    assert(deleted);
  }

  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  autovector<MemTable*> to_delete;
  imm_.current()->Unref(&to_delete);
  for (MemTable* m : to_delete) {
    delete m;
  }

  if (db_paths_registered_) {
    // TODO(cc): considering using ioptions_.fs, currently some tests rely on
    // EnvWrapper, that's the main reason why we use env here.
    Status s = ioptions_.env->UnregisterDbPaths(GetDbPaths());
    if (!s.ok()) {
      ROCKS_LOG_ERROR(
          ioptions_.logger,
          "Failed to unregister data paths of column family (id: %d, name: %s)",
          id_, name_.c_str());
    }
  }
}

bool ColumnFamilyData::UnrefAndTryDelete() {
  int old_refs = refs_.fetch_sub(1);
  assert(old_refs > 0);

  if (old_refs == 1) {
    assert(super_version_ == nullptr);
    delete this;
    return true;
  }

  if (old_refs == 2 && super_version_ != nullptr) {
    // Only the super_version_ holds me
    SuperVersion* sv = super_version_;
    super_version_ = nullptr;

    // Release SuperVersion references kept in ThreadLocalPtr.
    local_sv_.reset();

    if (sv->Unref()) {
      // Note: sv will delete this ColumnFamilyData during Cleanup()
      assert(sv->cfd == this);
      sv->Cleanup();
      delete sv;
      return true;
    }
  }
  return false;
}

void ColumnFamilyData::SetDropped() {
  // can't drop default CF
  assert(id_ != 0);
  dropped_ = true;
  write_controller_token_.reset();

  // remove from column_family_set
  column_family_set_->RemoveColumnFamily(this);
}

ColumnFamilyOptions ColumnFamilyData::GetLatestCFOptions() const {
  return BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
}

uint64_t ColumnFamilyData::OldestLogToKeep() {
  auto current_log = GetLogNumber();

  if (allow_2pc_) {
    auto imm_prep_log = imm()->PrecomputeMinLogContainingPrepSection();
    auto mem_prep_log = mem()->GetMinLogContainingPrepSection();

    if (imm_prep_log > 0 && imm_prep_log < current_log) {
      current_log = imm_prep_log;
    }

    if (mem_prep_log > 0 && mem_prep_log < current_log) {
      current_log = mem_prep_log;
    }
  }

  return current_log;
}

const double kIncSlowdownRatio = 0.8;
const double kDecSlowdownRatio = 1 / kIncSlowdownRatio;
const double kNearStopSlowdownRatio = 0.6;
const double kDelayRecoverSlowdownRatio = 1.4;

namespace {
// SetupDelay：设置写入延迟参数和速率限制
//
// 功能说明：
// 1. 根据压缩债务（compaction debt）的变化动态调整写入速率
// 2. 实现自适应写入节流：当压缩落后时降低写入速率，压缩跟上时提高速率
// 3. 支持惩罚机制：当接近停止条件时，更激进地降低速率
//
// 参数说明：
// - write_controller: 写入控制器，管理全局写入节流状态
// - compaction_needed_bytes: 当前需要的压缩字节数（压缩债务）
// - prev_compaction_need_bytes: 上一次的压缩债务字节数
// - penalize_stop: 是否惩罚停止条件，true 表示需要更激进地降低速率
// - auto_compactions_disabled: 是否禁用了自动压缩
//
// 返回值：
// - WriteControllerToken: 写入控制令牌，用于应用新的写入速率限制
//
// 注：如果 penalize_stop 为 true，会进一步降低减速速率
// If penalize_stop is true, we further reduce slowdown rate.
std::unique_ptr<WriteControllerToken> SetupDelay(
    WriteController* write_controller, uint64_t compaction_needed_bytes,
    uint64_t prev_compaction_need_bytes, bool penalize_stop,
    bool auto_compactions_disabled) {
  // 最小写入速率：16KB/s
  // 这是为了保证即使需要严重节流，写入也不会完全停止（除非达到 kStopped 状态）
  const uint64_t kMinWriteRate = 16 * 1024u;  // Minimum write rate 16KB/s.

  // 获取用户配置的最大延迟写入速率（上限）
  // 这是用户通过 max_write_rate_per_second 设置的最大值
  uint64_t max_write_rate = write_controller->max_delayed_write_rate();

  // 获取当前实际的延迟写入速率
  // 这是动态调整的值，可能在 [kMinWriteRate, max_write_rate] 之间
  uint64_t write_rate = write_controller->delayed_write_rate();

  // 如果自动压缩被禁用
  if (auto_compactions_disabled) {
    // 当自动压缩禁用时，始终使用用户给定的值（不进行自适应调整）
    // 因为没有自动压缩来处理写入积累的数据，节流效果有限
    // When auto compaction is disabled, always use the value user gave.
    write_rate = max_write_rate;

  } else if (write_controller->NeedsDelay() && max_write_rate > kMinWriteRate) {
    // 以下情况需要调整写入速率：
    // 1. 写入控制器检测到需要延迟（NeedsDelay() 返回 true）
    // 2. 用户配置的最大速率大于最小速率（否则不调整）
    //
    // 如果用户给出的速率小于 kMinWriteRate，则不进行调整
    // If user gives rate less than kMinWriteRate, don't adjust it.
    //
    // 如果已经处于延迟状态，需要根据之前的压缩债务进行调整
    // 当有两个或更多列族需要延迟时，我们总是基于单个列族的信息
    // 增加或减少写入速率。这通常是可以接受的，但如果出现问题可以改进。
    //
    // If already delayed, need to adjust based on previous compaction debt.
    // When there are two or more column families require delay, we always
    // increase or reduce write rate based on information for one single
    // column family. It is likely to be OK but we can improve if there is a
    // problem.
    //
    // 忽略 compaction_needed_bytes = 0 的情况，因为 compaction_needed_bytes
    // 仅在基于级别的压缩（level-based compaction）中可用
    // Ignore compaction_needed_bytes = 0 case because compaction_needed_bytes
    // is only available in level-based compaction

    // 如果压缩债务保持不变或增加，我们需要进一步降低速率
    // 这通常意味着 memtable 已满。这主要用于以下情况：
    // flush 和压缩的速度都远慢于我们向 memtable 插入数据的速度，
    // 因此我们需要在从压缩和 flush 获得反馈信号之前主动降低速率，
    // 以避免因达到最大写缓冲区数量而完全停止。
    //
    // If the compaction debt stays the same as previously, we also further slow
    // down. It usually means a mem table is full. It's mainly for the case
    // where both of flush and compaction are much slower than the speed we
    // insert to mem tables, so we need to actively slow down before we get
    // feedback signal from compaction and flushes to avoid the full stop
    // because of hitting the max write buffer number.

    // 如果数据库刚刚陷入停止条件，我们需要进一步降低写入速率
    // 以避免停止条件。
    //
    // If DB just falled into the stop condition, we need to further reduce
    // the write rate to avoid the stop condition.

    if (penalize_stop) {
      // 通过更激进的减速来惩罚接近停止或停止的条件
      // 这是为了提供长期的减速增加信号
      // 惩罚幅度大于恢复到正常条件的奖励幅度
      // kNearStopSlowdownRatio = 0.6，意味着速率降低到原来的 60%
      //
      // Penalize the near stop or stop condition by more aggressive slowdown.
      // This is to provide the long term slowdown increase signal.
      // The penalty is more than the reward of recovering to the normal
      // condition.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kNearStopSlowdownRatio);
      // 确保写入速率不低于最小值
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }

    } else if (prev_compaction_need_bytes > 0 &&
               prev_compaction_need_bytes <= compaction_needed_bytes) {
      // 压缩债务保持不变或增加：需要进一步降低速率
      // kIncSlowdownRatio = 0.8，意味着速率降低到原来的 80%
      // 每次调整最多降低 20%
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kIncSlowdownRatio);
      // 确保写入速率不低于最小值
      if (write_rate < kMinWriteRate) {
        write_rate = kMinWriteRate;
      }

    } else if (prev_compaction_need_bytes > compaction_needed_bytes) {
      // 压缩债务减少：说明正在追赶进度，可以提高写入速率
      // kDecSlowdownRatio = 1 / 0.8 = 1.25，意味着速率提高到原来的 125%
      // 每次调整最多提高 25%
      //
      // 当我们已经偿还了压缩债务时，我们以 kSlowdownRatio 的比率加速。
      // 但我们永远不会加速到超过用户给定的写入速率。
      //
      // We are speeding up by ratio of kSlowdownRatio when we have paid
      // compaction debt. But we'll never speed up to faster than the write rate
      // given by users.
      write_rate = static_cast<uint64_t>(static_cast<double>(write_rate) *
                                         kDecSlowdownRatio);
      // 确保写入速率不超过用户配置的最大值
      if (write_rate > max_write_rate) {
        write_rate = max_write_rate;
      }
    }
  }

  // 返回一个延迟令牌，将计算出的写入速率应用到写入控制器
  // 这个令牌会在其生命周期内维持速率限制，销毁时自动释放
  return write_controller->GetDelayToken(write_rate);
}

int GetL0ThresholdSpeedupCompaction(int level0_file_num_compaction_trigger,
                                    int level0_slowdown_writes_trigger) {
  // SanitizeOptions() ensures it.
  assert(level0_file_num_compaction_trigger <= level0_slowdown_writes_trigger);

  if (level0_file_num_compaction_trigger < 0) {
    return std::numeric_limits<int>::max();
  }

  const int64_t twice_level0_trigger =
      static_cast<int64_t>(level0_file_num_compaction_trigger) * 2;

  const int64_t one_fourth_trigger_slowdown =
      static_cast<int64_t>(level0_file_num_compaction_trigger) +
      ((level0_slowdown_writes_trigger - level0_file_num_compaction_trigger) /
       4);

  assert(twice_level0_trigger >= 0);
  assert(one_fourth_trigger_slowdown >= 0);

  // 1/4 of the way between L0 compaction trigger threshold and slowdown
  // condition.
  // Or twice as compaction trigger, if it is smaller.
  int64_t res = std::min(twice_level0_trigger, one_fourth_trigger_slowdown);
  if (res >= std::numeric_limits<int32_t>::max()) {
    return std::numeric_limits<int32_t>::max();
  } else {
    // res fits in int
    return static_cast<int>(res);
  }
}
}  // anonymous namespace

std::pair<WriteStallCondition, WriteStallCause>
ColumnFamilyData::GetWriteStallConditionAndCause(
    int num_unflushed_memtables, int num_l0_files,
    uint64_t num_compaction_needed_bytes,
    const MutableCFOptions& mutable_cf_options,
    const ImmutableCFOptions& immutable_cf_options) {
  if (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number) {
    return {WriteStallCondition::kStopped, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             num_l0_files >= mutable_cf_options.level0_stop_writes_trigger) {
    return {WriteStallCondition::kStopped, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.hard_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kStopped,
            WriteStallCause::kPendingCompactionBytes};
  } else if (mutable_cf_options.max_write_buffer_number > 3 &&
             num_unflushed_memtables >=
                 mutable_cf_options.max_write_buffer_number - 1 &&
             num_unflushed_memtables - 1 >=
                 immutable_cf_options.min_write_buffer_number_to_merge) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kMemtableLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.level0_slowdown_writes_trigger >= 0 &&
             num_l0_files >=
                 mutable_cf_options.level0_slowdown_writes_trigger) {
    return {WriteStallCondition::kDelayed, WriteStallCause::kL0FileCountLimit};
  } else if (!mutable_cf_options.disable_auto_compactions &&
             mutable_cf_options.soft_pending_compaction_bytes_limit > 0 &&
             num_compaction_needed_bytes >=
                 mutable_cf_options.soft_pending_compaction_bytes_limit) {
    return {WriteStallCondition::kDelayed,
            WriteStallCause::kPendingCompactionBytes};
  }
  return {WriteStallCondition::kNormal, WriteStallCause::kNone};
}

WriteStallCondition ColumnFamilyData::RecalculateWriteStallConditions(
    const MutableCFOptions& mutable_cf_options) {
  // 初始化写入停顿条件为正常状态
  auto write_stall_condition = WriteStallCondition::kNormal;
  if (current_ != nullptr) {
    // 获取当前版本的存储信息，包含各层级文件统计和压缩相关信息
    auto* vstorage = current_->storage_info();
    // 获取写入控制器，用于管理写入限流
    auto write_controller = column_family_set_->write_controller_;
    // 获取预估的需要压缩的字节数，这是判断是否需要限流的关键指标之一
    uint64_t compaction_needed_bytes =
        vstorage->estimated_compaction_needed_bytes();

    // 根据当前状态判断是否需要限流以及限流的原因
    // 传入参数：未刷新的memtable数量、L0文件数量、需要压缩的字节数等
    auto write_stall_condition_and_cause = GetWriteStallConditionAndCause(
        imm()->NumNotFlushed(), vstorage->l0_delay_trigger_count(),
        vstorage->estimated_compaction_needed_bytes(), mutable_cf_options,
        *ioptions());
    write_stall_condition = write_stall_condition_and_cause.first;
    auto write_stall_cause = write_stall_condition_and_cause.second;

    // 记录之前的停顿状态，用于判断状态变化
    bool was_stopped = write_controller->IsStopped();
    bool needed_delay = write_controller->NeedsDelay();

    // === 停止写入条件处理 ===
    // 当满足硬限制时，完全停止写入
    if (write_stall_condition == WriteStallCondition::kStopped &&
        write_stall_cause == WriteStallCause::kMemtableLimit) {
      // Memtable数量限制导致停止写入
      // GetStopToken：获取停止令牌，会阻塞新的写入操作
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d",
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number);
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      // L0文件数量超过限制导致停止写入
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_STOPS, 1);
      // 统计在有进行中压缩任务的情况下触发的停止次数
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_STOPS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stopping writes because we have %d level-0 files",
                     name_.c_str(), vstorage->l0_delay_trigger_count());
    } else if (write_stall_condition == WriteStallCondition::kStopped &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      // 待压缩字节数超过硬限制导致停止写入
      write_controller_token_ = write_controller->GetStopToken();
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_STOPS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stopping writes because of estimated pending compaction "
          "bytes %" PRIu64,
          name_.c_str(), compaction_needed_bytes);
    } 
    // === 延迟写入条件处理 ===
    // 当满足软限制时，降低写入速率但不是完全停止
    else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kMemtableLimit) {
      // Memtable数量接近限制，需要延迟写入
      // SetupDelay：设置延迟参数，包括写入速率等
      // 参数：写入控制器、当前需要压缩字节数、之前需要压缩字节数、之前是否停止、是否禁用自动压缩
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::MEMTABLE_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because we have %d immutable memtables "
          "(waiting for flush), max_write_buffer_number is set to %d "
          "rate %" PRIu64,
          name_.c_str(), imm()->NumNotFlushed(),
          mutable_cf_options.max_write_buffer_number,
          write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kL0FileCountLimit) {
      // L0文件数量接近停止阈值，需要延迟写入
      // L0 is the last two files from stopping.
      // 判断是否接近停止（距离停止阈值只有2个文件）
      bool near_stop = vstorage->l0_delay_trigger_count() >=
                       mutable_cf_options.level0_stop_writes_trigger - 2;
      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(InternalStats::L0_FILE_COUNT_LIMIT_DELAYS, 1);
      if (compaction_picker_->IsLevel0CompactionInProgress()) {
        internal_stats_->AddCFStats(
            InternalStats::L0_FILE_COUNT_LIMIT_DELAYS_WITH_ONGOING_COMPACTION,
            1);
      }
      ROCKS_LOG_WARN(ioptions_.logger,
                     "[%s] Stalling writes because we have %d level-0 files "
                     "rate %" PRIu64,
                     name_.c_str(), vstorage->l0_delay_trigger_count(),
                     write_controller->delayed_write_rate());
    } else if (write_stall_condition == WriteStallCondition::kDelayed &&
               write_stall_cause == WriteStallCause::kPendingCompactionBytes) {
      // 待压缩字节数超过软限制，需要延迟写入
      // If the distance to hard limit is less than 1/4 of the gap between soft
      // and hard bytes limit, we think it is near stop and speed up the slowdown.
      // 计算是否接近硬限制：如果距离硬限制小于软硬限制差距的1/4，则认为接近停止
      bool near_stop =
          mutable_cf_options.hard_pending_compaction_bytes_limit > 0 &&
          (compaction_needed_bytes -
           mutable_cf_options.soft_pending_compaction_bytes_limit) >
              3 *
                  (mutable_cf_options.hard_pending_compaction_bytes_limit -
                   mutable_cf_options.soft_pending_compaction_bytes_limit) /
                  4;

      write_controller_token_ =
          SetupDelay(write_controller, compaction_needed_bytes,
                     prev_compaction_needed_bytes_, was_stopped || near_stop,
                     mutable_cf_options.disable_auto_compactions);
      internal_stats_->AddCFStats(
          InternalStats::PENDING_COMPACTION_BYTES_LIMIT_DELAYS, 1);
      ROCKS_LOG_WARN(
          ioptions_.logger,
          "[%s] Stalling writes because of estimated pending compaction "
          "bytes %" PRIu64 " rate %" PRIu64,
          name_.c_str(), vstorage->estimated_compaction_needed_bytes(),
          write_controller->delayed_write_rate());
    } 
    // === 正常写入条件处理 ===
    // 当没有触发限流时，根据压力情况增加压缩线程或调整速率
    else {
      assert(write_stall_condition == WriteStallCondition::kNormal);
      // 如果L0文件数达到加速压缩的阈值，增加压缩线程以缓解压力
      if (vstorage->l0_delay_trigger_count() >=
          GetL0ThresholdSpeedupCompaction(
              mutable_cf_options.level0_file_num_compaction_trigger,
              mutable_cf_options.level0_slowdown_writes_trigger)) {
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        ROCKS_LOG_INFO(
            ioptions_.logger,
            "[%s] Increasing compaction threads because we have %d level-0 "
            "files ",
            name_.c_str(), vstorage->l0_delay_trigger_count());
      } else if (vstorage->estimated_compaction_needed_bytes() >=
                 mutable_cf_options.soft_pending_compaction_bytes_limit / 4) {
        // Increase compaction threads if bytes needed for compaction exceeds
        // 1/4 of threshold for slowing down.
        // If soft pending compaction byte limit is not set, always speed up
        // compaction.
        // 如果压缩所需字节数超过软限制的1/4，增加压缩线程
        // 如果没有设置软限制，则始终加速压缩
        write_controller_token_ =
            write_controller->GetCompactionPressureToken();
        if (mutable_cf_options.soft_pending_compaction_bytes_limit > 0) {
          ROCKS_LOG_INFO(
              ioptions_.logger,
              "[%s] Increasing compaction threads because of estimated pending "
              "compaction "
              "bytes %" PRIu64,
              name_.c_str(), vstorage->estimated_compaction_needed_bytes());
        }
      } else {
        // 没有压力，清空写入控制器令牌
        write_controller_token_.reset();
      }
      // If the DB recovers from delay conditions, we reward with reducing
      // double the slowdown ratio. This is to balance the long term slowdown
      // increase signal.
      // 如果DB从延迟条件恢复，通过提高写入速率（乘以kDelayRecoverSlowdownRatio）进行奖励
      // 这是为了平衡长期的降速增长信号
      if (needed_delay) {
        uint64_t write_rate = write_controller->delayed_write_rate();
        write_controller->set_delayed_write_rate(static_cast<uint64_t>(
            static_cast<double>(write_rate) * kDelayRecoverSlowdownRatio));
        // Set the low pri limit to be 1/4 the delayed write rate.
        // Note we don't reset this value even after delay condition is relased.
        // Low-pri rate will continue to apply if there is a compaction
        // pressure.
        // 设置低优先级速率为延迟写入速率的1/4
        // 注意：即使延迟条件释放后也不重置该值
        // 如果存在压缩压力，低优先级速率将继续应用
        write_controller->low_pri_rate_limiter()->SetBytesPerSecond(write_rate /
                                                                    4);
      }
    }
    // 保存当前的压缩所需字节数，用于下次比较，判断压缩债务是在增加还是减少
    prev_compaction_needed_bytes_ = compaction_needed_bytes;
  }
  return write_stall_condition;
}

const FileOptions* ColumnFamilyData::soptions() const {
  return &(column_family_set_->file_options_);
}

void ColumnFamilyData::SetCurrent(Version* current_version) {
  current_ = current_version;
}

uint64_t ColumnFamilyData::GetNumLiveVersions() const {
  return VersionSet::GetNumLiveVersions(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalSstFilesSize() const {
  return VersionSet::GetTotalSstFilesSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetTotalBlobFileSize() const {
  return VersionSet::GetTotalBlobFileSize(dummy_versions_);
}

uint64_t ColumnFamilyData::GetLiveSstFilesSize() const {
  return current_->GetSstFilesSize();
}

MemTable* ColumnFamilyData::ConstructNewMemtable(
    const MutableCFOptions& mutable_cf_options, SequenceNumber earliest_seq) {
  return new MemTable(internal_comparator_, ioptions_, mutable_cf_options,
                      write_buffer_manager_, earliest_seq, id_);
}

void ColumnFamilyData::CreateNewMemtable(
    const MutableCFOptions& mutable_cf_options, SequenceNumber earliest_seq) {
  if (mem_ != nullptr) {
    delete mem_->Unref();
  }
  SetMemtable(ConstructNewMemtable(mutable_cf_options, earliest_seq));
  mem_->Ref();
}

/**
 * @brief 判断列族是否需要Compaction
 *
 * 该函数是触发Compaction调用的入口点：
 * 1. 检查是否禁用了自动Compaction
 * 2. 调用CompactionPicker的NeedsCompaction检查具体触发条件
 * 3. 对于Level风格，核心是检查是否有层的Compaction分数>=1
 *
 * 调用位置：MaybeScheduleFlushOrCompaction 中检查是否需要调度Compaction
 *
 * @return true表示需要Compaction，false表示不需要
 */
bool ColumnFamilyData::NeedsCompaction() const {
  return !mutable_cf_options_.disable_auto_compactions &&
         compaction_picker_->NeedsCompaction(current_->storage_info());
}

Compaction* ColumnFamilyData::PickCompaction(
    const MutableCFOptions& mutable_options,
    const MutableDBOptions& mutable_db_options, LogBuffer* log_buffer) {
  auto* result = compaction_picker_->PickCompaction(
      GetName(), mutable_options, mutable_db_options, current_->storage_info(),
      log_buffer);
  if (result != nullptr) {
    result->SetInputVersion(current_);
  }
  return result;
}

bool ColumnFamilyData::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int level) const {
  return compaction_picker_->RangeOverlapWithCompaction(
      smallest_user_key, largest_user_key, level);
}

Status ColumnFamilyData::RangesOverlapWithMemtables(
    const autovector<Range>& ranges, SuperVersion* super_version,
    bool allow_data_in_errors, bool* overlap) {
  assert(overlap != nullptr);
  *overlap = false;
  // Create an InternalIterator over all unflushed memtables
  Arena arena;
  // TODO: plumb Env::IOActivity
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  MergeIteratorBuilder merge_iter_builder(&internal_comparator_, &arena);
  merge_iter_builder.AddIterator(
      super_version->mem->NewIterator(read_opts, &arena));
  super_version->imm->AddIterators(read_opts, &merge_iter_builder,
                                   false /* add_range_tombstone_iter */);
  ScopedArenaIterator memtable_iter(merge_iter_builder.Finish());

  auto read_seq = super_version->current->version_set()->LastSequence();
  ReadRangeDelAggregator range_del_agg(&internal_comparator_, read_seq);
  auto* active_range_del_iter = super_version->mem->NewRangeTombstoneIterator(
      read_opts, read_seq, false /* immutable_memtable */);
  range_del_agg.AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator>(active_range_del_iter));
  Status status;
  status = super_version->imm->AddRangeTombstoneIterators(
      read_opts, nullptr /* arena */, &range_del_agg);
  // AddRangeTombstoneIterators always return Status::OK.
  assert(status.ok());

  for (size_t i = 0; i < ranges.size() && status.ok() && !*overlap; ++i) {
    auto* vstorage = super_version->current->storage_info();
    auto* ucmp = vstorage->InternalComparator()->user_comparator();
    InternalKey range_start(ranges[i].start, kMaxSequenceNumber,
                            kValueTypeForSeek);
    memtable_iter->Seek(range_start.Encode());
    status = memtable_iter->status();
    ParsedInternalKey seek_result;

    if (status.ok() && memtable_iter->Valid()) {
      status = ParseInternalKey(memtable_iter->key(), &seek_result,
                                allow_data_in_errors);
    }

    if (status.ok()) {
      if (memtable_iter->Valid() &&
          ucmp->Compare(seek_result.user_key, ranges[i].limit) <= 0) {
        *overlap = true;
      } else if (range_del_agg.IsRangeOverlapped(ranges[i].start,
                                                 ranges[i].limit)) {
        *overlap = true;
      }
    }
  }
  return status;
}

const int ColumnFamilyData::kCompactAllLevels = -1;
const int ColumnFamilyData::kCompactToBaseLevel = -2;

Compaction* ColumnFamilyData::CompactRange(
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, int input_level,
    int output_level, const CompactRangeOptions& compact_range_options,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* conflict,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts) {
  auto* result = compaction_picker_->CompactRange(
      GetName(), mutable_cf_options, mutable_db_options,
      current_->storage_info(), input_level, output_level,
      compact_range_options, begin, end, compaction_end, conflict,
      max_file_num_to_ignore, trim_ts);
  if (result != nullptr) {
    result->SetInputVersion(current_);
  }
  TEST_SYNC_POINT("ColumnFamilyData::CompactRange:Return");
  return result;
}

SuperVersion* ColumnFamilyData::GetReferencedSuperVersion(DBImpl* db) {
  SuperVersion* sv = GetThreadLocalSuperVersion(db);
  sv->Ref();
  if (!ReturnThreadLocalSuperVersion(sv)) {
    // This Unref() corresponds to the Ref() in GetThreadLocalSuperVersion()
    // when the thread-local pointer was populated. So, the Ref() earlier in
    // this function still prevents the returned SuperVersion* from being
    // deleted out from under the caller.
    sv->Unref();
  }
  return sv;
}

SuperVersion* ColumnFamilyData::GetThreadLocalSuperVersion(DBImpl* db) {
  // The SuperVersion is cached in thread local storage to avoid acquiring
  // mutex when SuperVersion does not change since the last use. When a new
  // SuperVersion is installed, the compaction or flush thread cleans up
  // cached SuperVersion in all existing thread local storage. To avoid
  // acquiring mutex for this operation, we use atomic Swap() on the thread
  // local pointer to guarantee exclusive access. If the thread local pointer
  // is being used while a new SuperVersion is installed, the cached
  // SuperVersion can become stale. In that case, the background thread would
  // have swapped in kSVObsolete. We re-check the value at when returning
  // SuperVersion back to thread local, with an atomic compare and swap.
  // The superversion will need to be released if detected to be stale.
  void* ptr = local_sv_->Swap(SuperVersion::kSVInUse);
  // Invariant:
  // (1) Scrape (always) installs kSVObsolete in ThreadLocal storage
  // (2) the Swap above (always) installs kSVInUse, ThreadLocal storage
  // should only keep kSVInUse before ReturnThreadLocalSuperVersion call
  // (if no Scrape happens).
  assert(ptr != SuperVersion::kSVInUse);
  SuperVersion* sv = static_cast<SuperVersion*>(ptr);
  if (sv == SuperVersion::kSVObsolete) {
    RecordTick(ioptions_.stats, NUMBER_SUPERVERSION_ACQUIRES);
    db->mutex()->Lock();
    sv = super_version_->Ref();
    db->mutex()->Unlock();
  }
  assert(sv != nullptr);
  return sv;
}

bool ColumnFamilyData::ReturnThreadLocalSuperVersion(SuperVersion* sv) {
  assert(sv != nullptr);
  // Put the SuperVersion back
  void* expected = SuperVersion::kSVInUse;
  if (local_sv_->CompareAndSwap(static_cast<void*>(sv), expected)) {
    // When we see kSVInUse in the ThreadLocal, we are sure ThreadLocal
    // storage has not been altered and no Scrape has happened. The
    // SuperVersion is still current.
    return true;
  } else {
    // ThreadLocal scrape happened in the process of this GetImpl call (after
    // thread local Swap() at the beginning and before CompareAndSwap()).
    // This means the SuperVersion it holds is obsolete.
    assert(expected == SuperVersion::kSVObsolete);
  }
  return false;
}

// ColumnFamilyData::InstallSuperVersion (带互斥量版本)
//
// 功能概述：
// 安装新的 SuperVersion 到 ColumnFamilyData。这是包装函数，
// 确保在调用真正的 InstallSuperVersion 之前已持有互斥量。
//
// 参数说明：
// - sv_context: SuperVersionContext 上下文，包含新创建的 SuperVersion
// - db_mutex: 数据库互斥量，需要在调用时持有
//
// 调用场景：
// - Flush 后：安装包含新 memtable 和 immutable memtable 的 SuperVersion
// - Compaction 后：安装包含新 Version 的 SuperVersion
// - 初始化时：安装初始的 SuperVersion
// - MemTable 切换后：安装包含新 memtable 的 SuperVersion
void ColumnFamilyData::InstallSuperVersion(SuperVersionContext* sv_context,
                                           InstrumentedMutex* db_mutex) {
  // 断言：必须持有数据库互斥量
  // 这是一个重要的不变量，确保操作的线程安全
  db_mutex->AssertHeld();

  // 调用真正的 InstallSuperVersion 函数
  // 使用当前列族的 mutable_cf_options
  return InstallSuperVersion(sv_context, mutable_cf_options_);
}

// ColumnFamilyData::InstallSuperVersion - 安装新的 SuperVersion
//
// 功能概述：
// 安装新的 SuperVersion 到 ColumnFamilyData，替换旧的 SuperVersion。
// SuperVersion 是 RocksDB 版本控制系统的核心，它聚合了所有读取操作所需的信息：
// - mem: 当前活跃的 MemTable
// - imm: 不可变的 MemTable 列表（等待 flush）
// - current: 当前的 Version（包含所有 SST 文件的信息）
//
// SuperVersion 的作用：
// - 提供读取操作的统一视图，避免在读取时需要分别访问 mem/imm/current
// - 缓存 memtable iterator，加速迭代
// - 缓存 Bloom Filter 和其他索引，减少查找开销
// - 记录写入停顿条件，用于写入节流
//
// 参数说明：
// - sv_context: SuperVersionContext 上下文
//   - new_superversion: 新创建的 SuperVersion（所有权转移）
//   - superversions_to_free: 用于存储待释放的 SuperVersion
//   - 其他上下文信息（如 write_stall_notification）
// - mutable_cf_options: 可变的列族选项
//   - 用于写入停顿条件的计算
//   - 用于更新 memtable 的写缓冲区大小
//
// 安装流程：
// 1. 从 sv_context 获取新的 SuperVersion
// 2. 初始化新 SuperVersion（设置 mem/imm/current）
// 3. 替换当前的 SuperVersion
// 4. 重新计算写入停顿条件（如果必要）
// 5. 重置线程本地 SuperVersion 缓存
// 6. 更新 memtable 写缓冲区大小（如果改变）
// 7. 通知写入停顿状态变化（如果改变）
// 8. 释放旧的 SuperVersion（如果引用计数归零）
//
// 设计考虑：
// 1. 为什么需要重置线程本地 SuperVersion？
//    - SuperVersion 可能被缓存在线程本地存储中
//    - 如果不重置，线程可能继续使用旧的、过时的 SuperVersion
//    - 重置确保线程下次访问时获取新的 SuperVersion
//
// 2. 什么时候需要重新计算写入停顿条件？
//    - 旧的 SuperVersion 为空（首次安装）
//    - mem/imm/current 任何一个发生变化
//    - 这些变化影响写入速率限制的计算
//
// 3. 为什么需要比较 old 和 new 的 write_stall_condition？
//    - 只有当条件真正改变时才发送通知
//    - 避免不必要的通知和状态变化
//    - 减少系统开销
//
// 4. 为什么在 Unref() 之前重置线程本地存储？
//    - 确保 local_sv_ 永不持有 SuperVersion 的最后一个引用
//    - 线程本地存储没有安全清理 SuperVersion 的机制
//    - 避免引用计数归零后，线程本地存储仍持有旧引用
//
// 线程安全：
// - 调用者必须持有互斥量（通过 AssertHeld 检查）
// - 原子地替换 super_version_ 指针
// - 引用计数确保 SuperVersion 的安全释放
// - 线程本地存储的清除在互斥量保护下进行
//
// 性能影响：
// - 安装新 SuperVersion 会影响读取操作的性能
// - 新的 SuperVersion 可能包含不同的 memtable 和文件布局
// - 线程本地缓存的重置会导致短暂的性能下降（缓存失效）
// - 但这是必要的，确保所有线程使用一致的数据视图
//
// 内存管理：
// - 新的 SuperVersion 从 sv_context 转移所有权
// - 旧的 SuperVersion 的引用计数减 1
// - 如果引用计数归零，旧的 SuperVersion 被添加到待释放列表
// - sv_context 的 superversions_to_free 列表稍后用于释放
void ColumnFamilyData::InstallSuperVersion(
    SuperVersionContext* sv_context,
    const MutableCFOptions& mutable_cf_options) {
  // ============================================================================
  // 步骤 1：获取新的 SuperVersion
  // ============================================================================
  // 从 SuperVersionContext 中释放并获取新创建的 SuperVersion
  // release() 转移所有权，new_superversion 现在由 CFD 管理
  SuperVersion* new_superversion = sv_context->new_superversion.release();

  // 设置新 SuperVersion 的可变选项
  // 这些选项用于写入停顿条件的计算
  new_superversion->mutable_cf_options = mutable_cf_options;

  // ============================================================================
  // 步骤 2：初始化新 SuperVersion
  // ============================================================================
  // Init() 设置 SuperVersion 的三个核心组件：
  // 1. mem: 当前活跃的 MemTable
  // 2. imm: 不可变的 MemTable 列表（imm_.current()）
  // 3. current: 当前的 Version（包含所有 SST 文件）
  //
  // 这些组件是读取操作所需的完整信息
  new_superversion->Init(this, mem_, imm_.current(), current_);

  // ============================================================================
  // 步骤 3：替换 SuperVersion
  // ============================================================================
  // 保存旧的 SuperVersion 指针
  SuperVersion* old_superversion = super_version_;

  // 原子地替换当前的 SuperVersion
  // 所有后续的读取操作将使用新的 SuperVersion
  super_version_ = new_superversion;

  // 增加 SuperVersion 序号
  // 用于跟踪 SuperVersion 的变化，用于调试和统计
  ++super_version_number_;

  // 设置新 SuperVersion 的版本号
  // 与 ColumnFamilyData 的版本号保持一致
  super_version_->version_number = super_version_number_;

  // ============================================================================
  // 步骤 4：重新计算写入停顿条件（如果必要）
  // ============================================================================
  // 判断是否需要重新计算写入停顿条件
  // 只有当以下任一条件满足时才重新计算：
  // 1. old_superversion == nullptr: 首次安装
  // 2. old_superversion->current != current(): Version 发生变化（compaction）
  // 3. old_superversion->mem != mem_: MemTable 发生变化（flush）
  // 4. old_superversion->imm != imm_.current(): Imm 列表发生变化
  if (old_superversion == nullptr || old_superversion->current != current() ||
      old_superversion->mem != mem_ ||
      old_superversion->imm != imm_.current()) {
    // Should not recalculate slow down condition if nothing has changed, since
    // should not recalculate slow down condition if nothing has changed, since
    // currently RecalculateWriteStallConditions() treats it as further slowing
    // down is needed.
    // 如果没有任何变化，不应该重新计算慢速条件
    // 因为 RecalculateWriteStallConditions() 会将其视为需要进一步减速
    //
    // 重新计算写入停顿条件
    // 基于新的 mem/imm/current 状态计算
    super_version_->write_stall_condition =
        RecalculateWriteStallConditions(mutable_cf_options);
  } else {
    // 如果没有变化，复用旧的写入停顿条件
    // 避免不必要的计算
    super_version_->write_stall_condition =
        old_superversion->write_stall_condition;
  }

  // ============================================================================
  // 步骤 5：处理旧的 SuperVersion
  // ============================================================================
  if (old_superversion != nullptr) {
    // ----------------------------------------------------------------------
    // 子步骤 5.1：重置线程本地 SuperVersion 缓存
    // ----------------------------------------------------------------------
    // Reset SuperVersions cached in thread local storage.
    // 重置缓存在线程本地存储中的 SuperVersions
    //
    // This should be done before old_superversion->Unref(). That's to ensure
    // 这应该在 old_superversion->Unref() 之前完成。这是为了确保
    // that local_sv_ never holds the last reference to SuperVersion, since
    // local_sv_ 永不持有 SuperVersion 的最后一个引用，因为
    // it has no means to safely do SuperVersion cleanup.
    // 它没有安全清理 SuperVersion 的方法
    //
    // 重置线程本地存储的原因：
    // 1. 线程可能缓存了旧的 SuperVersion
    // 2. 如果不重置，线程会继续使用过时的数据
    // 3. 导致读取不一致或错误的数据
    // 4. 重置后，线程下次访问时会获取新的 SuperVersion
    ResetThreadLocalSuperVersions();

    // ----------------------------------------------------------------------
    // 子步骤 5.2：更新 memtable 写缓冲区大小（如果改变）
    // ----------------------------------------------------------------------
    // 检查写缓冲区大小是否改变
    if (old_superversion->mutable_cf_options.write_buffer_size !=
        mutable_cf_options.write_buffer_size) {
      // 更新 memtable 的写缓冲区大小
      // memtable 需要知道新的限制，以正确控制内存使用
      mem_->UpdateWriteBufferSize(mutable_cf_options.write_buffer_size);
    }

    // ----------------------------------------------------------------------
    // 子步骤 5.3：通知写入停顿状态变化（如果改变）
    // ----------------------------------------------------------------------
    // 检查写入停顿条件是否改变
    if (old_superversion->write_stall_condition !=
        new_superversion->write_stall_condition) {
      // 推送写入停顿通知
      // 通知上层（如 DBImpl）写入停顿状态的变化
      // 例如：从正常状态变为需要减速，或反之
      sv_context->PushWriteStallNotification(
          old_superversion->write_stall_condition,
          new_superversion->write_stall_condition, GetName(), ioptions());
    }

    // ----------------------------------------------------------------------
    // 子步骤 5.4：释放旧的 SuperVersion
    // ----------------------------------------------------------------------
    // 减少旧 SuperVersion 的引用计数
    // 如果引用计数归零，清理旧 SuperVersion
    if (old_superversion->Unref()) {
      // 清理旧 SuperVersion 的资源
      // 释放 memtable iterator、Bloom Filter 缓存等
      old_superversion->Cleanup();

      // 将旧 SuperVersion 添加到待释放列表
      // sv_context 会稍后统一释放这些 SuperVersion
      // 延迟释放避免在互斥量内部进行复杂的清理操作
      sv_context->superversions_to_free.push_back(old_superversion);
    }
  }
}

void ColumnFamilyData::ResetThreadLocalSuperVersions() {
  autovector<void*> sv_ptrs;
  local_sv_->Scrape(&sv_ptrs, SuperVersion::kSVObsolete);
  for (auto ptr : sv_ptrs) {
    assert(ptr);
    if (ptr == SuperVersion::kSVInUse) {
      continue;
    }
    auto sv = static_cast<SuperVersion*>(ptr);
    bool was_last_ref __attribute__((__unused__));
    was_last_ref = sv->Unref();
    // sv couldn't have been the last reference because
    // ResetThreadLocalSuperVersions() is called before
    // unref'ing super_version_.
    assert(!was_last_ref);
  }
}

// ColumnFamilyData::ValidateOptions - 验证列族选项的合法性和兼容性
//
// 功能概述：
// 此函数用于验证列族选项的合法性和与数据库选项的兼容性。
// 在打开数据库、动态修改选项等关键操作之前调用，确保配置是有效的。
//
// 参数说明：
// - db_options: 数据库级别的选项（DBOptions）
// - cf_options: 列族级别的选项（ColumnFamilyOptions）
//
// 返回值：
// - Status::OK(): 选项验证通过
// - Status::InvalidArgument(): 参数无效（如配置冲突）
// - Status::NotSupported(): 功能不支持（如某些组合不被允许）
//
// 调用时机：
// 1. DBImpl::Open: 打开数据库时验证所有列族选项
// 2. DBImpl::CreateColumnFamily: 创建新列族时验证选项
// 3. ColumnFamilyData::SetOptions: 动态修改选项时验证新选项
// 4. DBImpl::AlterOptions: 修改列族选项时验证
//
// 验证项：
// 1. 压缩算法支持检查
// 2. 并发写入支持检查
// 3. 无序写入与 merge 操作兼容性检查
// 4. 列族路径支持检查
// 5. TTL 与 TableFactory 兼容性检查
// 6. 周期性压缩与 TableFactory 兼容性检查
// 7. Blob 垃圾回收参数检查
// 8. FIFO 压缩与 TTL 兼容性检查
// 9. MemTable 和 Block 保护字节长度检查
// 10. 文件温度阈值参数检查
//
// 设计原则：
// - 提前验证：在执行任何实际操作前验证选项，避免运行时错误
// - 明确错误：返回清晰的错误消息，帮助用户快速定位问题
// - 严格检查：拒绝任何可能导致数据损坏或性能问题的配置
Status ColumnFamilyData::ValidateOptions(
    const DBOptions& db_options, const ColumnFamilyOptions& cf_options) {
  // 初始化状态对象，用于跟踪验证过程中的错误
  Status s;

  // ============================================================================
  // 验证项 1：检查压缩算法是否支持
  // ============================================================================
  // CheckCompressionSupported 会检查：
  // 1. 压缩算法是否被编译到 RocksDB 中（如 Snappy、Zlib、LZ4 等）
  // 2. 压缩算法是否与当前的配置兼容
  // 3. 压缩算法是否适用于当前的存储引擎
  //
  // 如果使用了不支持的压缩算法（如编译时未包含 Zstd 但配置了 Zstd），
  // 会返回 Status::NotSupported 或 Status::InvalidArgument
  s = CheckCompressionSupported(cf_options);

  // 如果之前的检查通过，检查并发写入支持
  if (s.ok() && db_options.allow_concurrent_memtable_write) {
    // ============================================================================
    // 验证项 2：检查并发写入是否支持
    // ============================================================================
    // 当 enable_concurrent_memtable_write = true 时，需要检查：
    // 1. MemTable Rep 是否支持并发插入
    // 2. 是否有某些选项与并发写入不兼容
    //
    // CheckConcurrentWritesSupported 会检查：
    // - SkipListFactory: 支持并发写入
    // - HashSkipListRepFactory: 支持并发写入
    // - 其他自定义 Factory 需要实现并发插入接口
    //
    // 如果使用不支持并发写入的 MemTable Factory 但启用了 allow_concurrent_memtable_write，
    // 会返回 Status::NotSupported
    s = CheckConcurrentWritesSupported(cf_options);
  }

  // ============================================================================
  // 验证项 3：检查无序写入与 merge 操作的兼容性
  // ============================================================================
  // 无序写入（unordered_write）的特性：
  // - 不保证写入的顺序
  // - 可能提高某些场景下的性能
  //
  // max_successive_merges 的作用：
  // - 控制 max_successive_merges 个连续的 merge 操作后被合并为一个 put
  // - 减少读操作时的 merge 次数，提高读取性能
  //
  // 为什么不兼容：
  // - max_successive_merges > 0 需要保持写入的顺序
  // - unordered_write 不保证写入顺序
  // - 两者结合可能导致数据不一致或语义错误
  //
  // 示例场景：
  //   写入顺序：Merge(k, v1) -> Merge(k, v2) -> Merge(k, v3)
  //   max_successive_merges = 2: 会在 2 个 merge 后执行合并，需要知道前 2 个操作的顺序
  //   unordered_write: 不保证顺序，可能乱序执行，导致合并结果错误
  if (s.ok() && db_options.unordered_write &&
      cf_options.max_successive_merges != 0) {
    // 返回无效参数错误，说明这两个选项不能同时使用
    s = Status::InvalidArgument(
        "max_successive_merges > 0 is incompatible with unordered_write");
  }

  // ============================================================================
  // 验证项 4：检查列族路径支持
  // ============================================================================
  // CheckCFPathsSupported 会检查：
  // 1. cf_paths 配置是否有效（路径是否存在、可写等）
  // 2. 多路径存储是否与当前配置兼容
  // 3. 文件系统是否支持路径操作
  //
  // cf_paths 的作用：
  // - 允许将 SST 文件存储在多个目录中
  // - 可以利用多个磁盘提高 I/O 性能
  // - 可以将热数据和冷数据分离存储
  //
  // 如果路径配置无效或不可访问，会返回 Status::InvalidArgument
  if (s.ok()) {
    s = CheckCFPathsSupported(db_options, cf_options);
  }

  // 如果之前的检查有错误，立即返回错误
  if (!s.ok()) {
    return s;
  }

  // ============================================================================
  // 验证项 5：检查 TTL 与 TableFactory 的兼容性
  // ============================================================================
  // TTL (Time To Live) 的作用：
  // - 自动删除超过指定时间的键值对
  // - 适用于需要自动清理过期数据的场景（如缓存、会话数据等）
  //
  // kDefaultTtl 的定义：
  // - 表示未设置 TTL 的特殊值
  // - 通常为 0 或某个特殊值（如 std::numeric_limits<uint64_t>::max()）
  // - cf_options.ttl == kDefaultTtl 表示没有启用 TTL
  //
  // 为什么 TTL 只支持 BlockBasedTable：
  // 1. TTL 需要在 SST 文件中存储时间戳信息
  // 2. BlockBasedTable 的设计支持存储额外的元数据（如时间戳）
  // 3. PlainTable 的设计更简单，不支持复杂的元数据管理
  // 4. 其他 TableFactory（如 CuckooTable）也可能不支持 TTL
  //
  // TTL 实现机制：
  // - 在写入时记录时间戳
  // - 在读取时检查时间戳，如果超过 TTL 则返回 NotFound
  // - 在压缩时自动清理过期的键值对
  //
  // 错误示例：
  //   Options options;
  //   options.table_factory.reset(new PlainTableFactory());  // 不支持 TTL
  //   options.ttl = 3600;  // 启用 1 小时的 TTL
  //   DB::Open(...) 会返回 Status::NotSupported
  if (cf_options.ttl > 0 && cf_options.ttl != kDefaultTtl) {
    // 检查 TableFactory 是否是 BlockBasedTable 的实例
    // IsInstanceOf 通过检查 Factory 的类型名称或类型 ID 判断
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      // 返回不支持错误，说明 TTL 只支持 BlockBasedTable
      return Status::NotSupported(
          "TTL is only supported in Block-Based Table format. ");
    }
  }

  // ============================================================================
  // 验证项 6：检查周期性压缩与 TableFactory 的兼容性
  // ============================================================================
  // 周期性压缩（Periodic Compaction）的作用：
  // - 即使没有其他压缩触发条件，也会定期执行压缩
  // - 用于确保数据不会在某个层级停留过久
  // - 有助于保持数据的健康状态（如及时应用删除标记）
  //
  // periodic_compaction_seconds 的作用：
  // - 指定周期性压缩的间隔时间（秒）
  // - 0 表示禁用周期性压缩
  // - 正数表示每隔 N 秒执行一次压缩
  //
  // kDefaultPeriodicCompSecs 的定义：
  // - 默认值，通常为 0（禁用）或某个特殊值
  // - cf_options.periodic_compaction_seconds == kDefaultPeriodicCompSecs
  //   表示使用默认配置（通常禁用）
  //
  // 为什么只支持 BlockBasedTable：
  // - 原因与 TTL 类似，需要支持时间戳相关的元数据管理
  // - 周期性压缩需要知道 SST 文件的创建时间或最后压缩时间
  // - BlockBasedTable 设计支持这类元数据的存储和查询
  //
  // 使用场景：
  // - 数据需要定期更新或清理（如删除标记应用）
  // - 数据有生命周期，需要定期迁移到更高层级
  // - 保持数据的可读性和性能
  if (cf_options.periodic_compaction_seconds > 0 &&
      cf_options.periodic_compaction_seconds != kDefaultPeriodicCompSecs) {
    // 检查 TableFactory 是否是 BlockBasedTable 的实例
    if (!cf_options.table_factory->IsInstanceOf(
            TableFactory::kBlockBasedTableName())) {
      // 返回不支持错误，说明周期性压缩只支持 BlockBasedTable
      return Status::NotSupported(
          "Periodic Compaction is only supported in "
          "Block-Based Table format. ");
    }
  }

  // ============================================================================
  // 验证项 7：检查 Blob 垃圾回收参数的有效性
  // ============================================================================
  // Blob (Binary Large Object) 的作用：
  // - 将较大的值（> blob_cache_size）存储在独立的 blob 文件中
  // - 减少 SST 文件的大小，提高缓存效率
  // - 适用于存储图片、文档等大型二进制数据
  //
  // Blob 垃圾回收（Blob Garbage Collection）的作用：
  // - 清理不再使用的 blob 数据（被覆盖或删除的值）
  // - 释放磁盘空间
  // - 减少 blob 文件的碎片
  //
  // blob_garbage_collection_age_cutoff 的作用：
  // - 范围：[0.0, 1.0]
  // - 表示只回收年龄超过此比例的 blob 数据
  // - 0.0: 回收所有可回收的 blob
  // - 1.0: 不回收任何 blob（禁用）
  // - 0.5: 只回收年龄超过中位数的 blob
  //
  // blob_garbage_collection_force_threshold 的作用：
  // - 范围：[0.0, 1.0]
  // - 表示当垃圾比例超过此值时强制回收
  // - 0.0: 不强制回收
  // - 1.0: 任何垃圾都强制回收
  // - 0.3: 当垃圾比例超过 30% 时强制回收
  //
  // 错误示例：
  //   cf_options.blob_garbage_collection_age_cutoff = 1.5;  // 超出范围
  //   cf_options.blob_garbage_collection_force_threshold = -0.1;  // 超出范围
  //   会导致返回 Status::InvalidArgument
  if (cf_options.enable_blob_garbage_collection) {
    // 检查 blob_garbage_collection_age_cutoff 是否在有效范围 [0.0, 1.0] 内
    // < 0.0: 无意义（负数）
    // > 1.0: 无意义（超过 100%）
    if (cf_options.blob_garbage_collection_age_cutoff < 0.0 ||
        cf_options.blob_garbage_collection_age_cutoff > 1.0) {
      // 返回无效参数错误，说明 age_cutoff 必须在 [0.0, 1.0] 范围内
      return Status::InvalidArgument(
          "The age cutoff for blob garbage collection should be in the range "
          "[0.0, 1.0].");
    }

    // 检查 blob_garbage_collection_force_threshold 是否在有效范围 [0.0, 1.0] 内
    // < 0.0: 无意义（负数）
    // > 1.0: 无意义（超过 100%）
    if (cf_options.blob_garbage_collection_force_threshold < 0.0 ||
        cf_options.blob_garbage_collection_force_threshold > 1.0) {
      // 返回无效参数错误，说明 force_threshold 必须在 [0.0, 1.0] 范围内
      return Status::InvalidArgument(
          "The garbage ratio threshold for forcing blob garbage collection "
          "should be in the range [0.0, 1.0].");
    }
  }

  // ============================================================================
  // 验证项 8：检查 FIFO 压缩与 TTL 的兼容性
  // ============================================================================
  // FIFO (First-In-First-Out) 压缩策略：
  // - 最简单的压缩策略
  // - 当文件数量超过 max_table_files 时，删除最老的文件
  // - 不考虑数据大小、压缩比等因素
  // - 适用于日志数据、时序数据等场景
  //
  // FIFO 与 TTL 的冲突：
  // 1. FIFO 压缩删除最老的文件，不考虑 TTL
  // 2. TTL 根据时间戳删除过期数据
  // 3. 两者同时使用可能导致：
  //    - FIFO 删除的文件可能包含未过期的数据（TTL 未到）
  //    - TTL 过期的数据可能保留在较新的文件中
  //    - 语义冲突，数据不可预测
  //
  // 为什么需要 max_open_files = -1：
  // - max_open_files = -1 表示不限制打开的文件数量
  // - FIFO 压缩需要频繁打开和关闭文件
  // - 如果限制了打开的文件数量，可能导致性能问题
  // - RocksDB 需要能够访问所有 SST 文件来实现 FIFO 逻辑
  //
  // kCompactionStyleFIFO 的定义：
  // - 压缩风格的枚举值之一
  // - 其他值：kCompactionStyleLevel, kCompactionStyleUniversal
  // - FIFO 只支持单层级（num_levels = 1）
  if (cf_options.compaction_style == kCompactionStyleFIFO &&
      db_options.max_open_files != -1 && cf_options.ttl > 0) {
    // 返回不支持错误，说明 FIFO 压缩需要 max_open_files = -1
    // 并且不能与 TTL 同时使用
    return Status::NotSupported(
        "FIFO compaction only supported with max_open_files = -1.");
  }

  // ============================================================================
  // 验证项 9：检查 MemTable 保护字节长度的有效性
  // ============================================================================
  // memtable_protection_bytes_per_key 的作用：
  // - 为 MemTable 中的每个键值对添加校验和保护信息
  // - 范围：0, 1, 2, 4, 8
  // - 0: 不添加保护
  // - 1-8: 添加相应字节的保护信息（如 CRC32、XXH64 等）
  //
  // 为什么只支持这些值：
  // - 0: 禁用保护（高性能）
  // - 1: 最小保护（8 位校验）
  // - 2: 小保护（16 位校验）
  // - 4: 标准保护（32 位校验，如 CRC32）
  // - 8: 强保护（64 位校验，如 XXH64）
  // - 其他值不被支持，可能导致内存对齐问题或性能下降
  //
  // 保护机制：
  // - 在写入时计算校验和
  // - 在读取时验证校验和
  // - 如果校验失败，返回错误（数据损坏）
  //
  // 性能权衡：
  // - 保护越多，CPU 开销越大（计算校验和）
  // - 保护越多，内存占用越大（存储校验和）
  // - 保护越多，数据可靠性越高（检测数据损坏）
  std::vector<uint32_t> supported{0, 1, 2, 4, 8};
  // 检查 memtable_protection_bytes_per_key 是否在支持的值列表中
  // std::find 返回迭代器，如果找不到则返回 end()
  if (std::find(supported.begin(), supported.end(),
                cf_options.memtable_protection_bytes_per_key) ==
      supported.end()) {
    // 返回不支持错误，说明只支持 0, 1, 2, 4, 8
    return Status::NotSupported(
        "Memtable per key-value checksum protection only supports 0, 1, 2, 4 "
        "or 8 bytes per key.");
  }

  // ============================================================================
  // 验证项 10：检查 Block 保护字节长度的有效性
  // ============================================================================
  // block_protection_bytes_per_key 的作用：
  // - 为 SST Block 中的每个键值对添加校验和保护信息
  // - 范围：0, 1, 2, 4, 8
  // - 0: 不添加保护
  // - 1-8: 添加相应字节的保护信息
  //
  // 与 memtable_protection_bytes_per_key 的区别：
  // - MemTable 保护：内存中的键值对（写入时）
  // - Block 保护：SST 文件中的键值对（持久化后）
  // - Block 保护通常比 MemTable 保护更重要（磁盘数据更易损坏）
  //
  // 为什么只支持这些值：
  // - 与 MemTable 保护相同的原因
  // - 内存对齐、性能、兼容性考虑
  //
  // 块级别的保护：
  // - Block 有完整的校验和（XxHash64）
  // - block_protection_bytes_per_key 是额外的键值对级别保护
  // - 可以检测 Block 内部的损坏（即使 Block 校验和正确）
  if (std::find(supported.begin(), supported.end(),
                cf_options.block_protection_bytes_per_key) == supported.end()) {
    // 返回不支持错误，说明只支持 0, 1, 2, 4, 8
    return Status::NotSupported(
        "Block per key-value checksum protection only supports 0, 1, 2, 4 "
        "or 8 bytes per key.");
  }

  // ============================================================================
  // 验证项 11：检查文件温度阈值参数
  // ============================================================================
  // 文件温度（File Temperature）的作用：
  // - 根据文件的访问频率分类（冷、温、热）
  // - 冷数据：很少访问
  // - 温数据：偶尔访问
  // - 热数据：频繁访问
  //
  // file_temperature_age_thresholds 的作用：
  // - 定义不同温度的年龄阈值
  // - 用于 FIFO 压缩策略
  // - 年龄阈值表示文件被创建后经过的时间
  //
  // 结构说明：
  // - std::vector<TemperatureAgeThreshold>
  // - 每个元素包含年龄和对应的温度
  // - 必须按年龄升序排列
  //
  // 使用场景：
  // - 将热数据保持在更快的存储介质（如 NVMe SSD）
  // - 将冷数据迁移到更慢的存储介质（如 HDD）
  // - 优化存储成本和性能
  //
  // 为什么只支持 FIFO 压缩：
  // - FIFO 压缩策略是按文件年龄删除的
  // - 其他压缩策略（Level、Universal）不直接基于年龄
  //
  // 为什么只支持单层级：
  // - FIFO 压缩只使用单层级（Level 0）
  // - 多层级（num_levels > 1）与文件温度机制不兼容
  if (!cf_options.compaction_options_fifo.file_temperature_age_thresholds
           .empty()) {
    // 检查压缩风格是否为 FIFO
    if (cf_options.compaction_style != kCompactionStyleFIFO) {
      // 返回不支持错误，说明文件温度阈值只支持 FIFO 压缩
      return Status::NotSupported(
          "Option file_temperature_age_thresholds only supports FIFO "
          "compaction.");
    } else if (cf_options.num_levels > 1) {
      // 检查是否为单层级
      // 返回不支持错误，说明文件温度阈值只支持单层级
      return Status::NotSupported(
          "Option file_temperature_age_thresholds is only supported when "
          "num_levels = 1.");
    } else {
      // 获取年龄阈值数组
      const auto& ages =
          cf_options.compaction_options_fifo.file_temperature_age_thresholds;
      // 断言确保至少有一个阈值
      assert(ages.size() >= 1);

      // 检查年龄阈值是否按升序排列
      // ages[i].age >= ages[i + 1].age 表示非升序（即乱序）
      // 升序排列是必要的，因为年龄阈值需要从低到高判断
      for (size_t i = 0; i < ages.size() - 1; ++i) {
        if (ages[i].age >= ages[i + 1].age) {
          // 返回不支持错误，说明年龄阈值必须按升序排列
          return Status::NotSupported(
              "Option file_temperature_age_thresholds requires elements to be "
              "sorted in increasing order with respect to `age` field.");
        }
      }
    }
  }

  // 所有验证通过，返回 OK 状态
  return s;
}

Status ColumnFamilyData::SetOptions(
    const DBOptions& db_opts,
    const std::unordered_map<std::string, std::string>& options_map) {
  ColumnFamilyOptions cf_opts =
      BuildColumnFamilyOptions(initial_cf_options_, mutable_cf_options_);
  ConfigOptions config_opts;
  config_opts.mutable_options_only = true;
  Status s = GetColumnFamilyOptionsFromMap(config_opts, cf_opts, options_map,
                                           &cf_opts);
  if (s.ok()) {
    s = ValidateOptions(db_opts, cf_opts);
  }
  if (s.ok()) {
    mutable_cf_options_ = MutableCFOptions(cf_opts);
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  }
  return s;
}

// REQUIRES: DB mutex held
Env::WriteLifeTimeHint ColumnFamilyData::CalculateSSTWriteHint(int level) {
  if (initial_cf_options_.compaction_style != kCompactionStyleLevel) {
    return Env::WLTH_NOT_SET;
  }
  if (level == 0) {
    return Env::WLTH_MEDIUM;
  }
  int base_level = current_->storage_info()->base_level();

  // L1: medium, L2: long, ...
  if (level - base_level >= 2) {
    return Env::WLTH_EXTREME;
  } else if (level < base_level) {
    // There is no restriction which prevents level passed in to be smaller
    // than base_level.
    return Env::WLTH_MEDIUM;
  }
  return static_cast<Env::WriteLifeTimeHint>(
      level - base_level + static_cast<int>(Env::WLTH_MEDIUM));
}

Status ColumnFamilyData::AddDirectories(
    std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs) {
  Status s;
  assert(created_dirs != nullptr);
  assert(data_dirs_.empty());
  for (auto& p : ioptions_.cf_paths) {
    auto existing_dir = created_dirs->find(p.path);

    if (existing_dir == created_dirs->end()) {
      std::unique_ptr<FSDirectory> path_directory;
      s = DBImpl::CreateAndNewDirectory(ioptions_.fs.get(), p.path,
                                        &path_directory);
      if (!s.ok()) {
        return s;
      }
      assert(path_directory != nullptr);
      data_dirs_.emplace_back(path_directory.release());
      (*created_dirs)[p.path] = data_dirs_.back();
    } else {
      data_dirs_.emplace_back(existing_dir->second);
    }
  }
  assert(data_dirs_.size() == ioptions_.cf_paths.size());
  return s;
}

FSDirectory* ColumnFamilyData::GetDataDir(size_t path_id) const {
  if (data_dirs_.empty()) {
    return nullptr;
  }

  assert(path_id < data_dirs_.size());
  return data_dirs_[path_id].get();
}

void ColumnFamilyData::RecoverEpochNumbers() {
  assert(current_);
  auto* vstorage = current_->storage_info();
  assert(vstorage);
  vstorage->RecoverEpochNumbers(this);
}

/**
 * ColumnFamilySet::ColumnFamilySet - 构造函数
 *
 * 功能概述:
 *   - 初始化 ColumnFamilySet 实例
 *   - 创建 dummy_cfd_ 作为循环链表的哨兵节点
 *   - 初始化链表结构（dummy_cfd_ 指向自己）
 *   - 保存全局资源和配置的引用
 *
 * 参数说明:
 *   @param dbname: 数据库名称（路径）
 *   @param db_options: 不可变数据库选项（指针）
 *   @param file_options: 文件操作选项
 *   @param table_cache: SSTable 缓存（所有列族共享）
 *   @param _write_buffer_manager: 写入缓冲区管理器
 *   @param _write_controller: 写入控制器（流量控制）
 *   @param block_cache_tracer: Block Cache 追踪器
 *   @param io_tracer: IO 追踪器
 *   @param db_id: 数据库 ID
 *   @param db_session_id: 数据库会话 ID
 *
 * 初始化列表:
 *   - max_column_family_: 0
 *   - file_options_: 传入的文件选项
 *   - dummy_cfd_: 创建新的 ColumnFamilyData 作为哨兵节点
 *   - default_cfd_cache_: nullptr（待初始化）
 *   - 其他成员：保存传入的参数引用
 *
 * 哨兵节点初始化:
 *   - ID: kDummyColumnFamilyDataId（特殊 ID，不表示真实列族）
 *   - 名称: ""（空字符串）
 *   - 链表: dummy_cfd_->prev_ = dummy_cfd_
 *           - dummy_cfd_->next_ = dummy_cfd_
 *   - 形成循环链表，简化边界条件处理
 *
 * 初始化流程:
 *   1. 初始化成员变量
 *   2. 创建 dummy_cfd_ 哨兵节点
 *   3. 初始化链表结构（dummy_cfd_ 指向自己）
 *   4. 列族集合为空（column_families_ 和 column_family_data_ 都是空）
 *
 * 链表结构:
 *   初始状态: dummy_cfd_ ↔ dummy_cfd_
 *   添加列族后: dummy_cfd_ ↔ cfd1 ↔ cfd2 ↔ ... ↔ dummy_cfd_
 *
 * 注意事项:
 *   - db_options 必须在实例生命周期内有效
 *   - table_cache、write_buffer_manager 等由 DBImpl 拥有
 *   - ColumnFamilySet 仅保存引用，不负责释放
 *   - default_cfd_cache_ 初始化为 nullptr，后续创建默认列族时更新
 */
ColumnFamilySet::ColumnFamilySet(const std::string& dbname,
                                 const ImmutableDBOptions* db_options,
                                 const FileOptions& file_options,
                                 Cache* table_cache,
                                 WriteBufferManager* _write_buffer_manager,
                                 WriteController* _write_controller,
                                 BlockCacheTracer* const block_cache_tracer,
                                 const std::shared_ptr<IOTracer>& io_tracer,
                                 const std::string& db_id,
                                 const std::string& db_session_id)
    : max_column_family_(0),
      file_options_(file_options),
      dummy_cfd_(new ColumnFamilyData(
          ColumnFamilyData::kDummyColumnFamilyDataId, "", nullptr, nullptr,
          nullptr, ColumnFamilyOptions(), *db_options, &file_options_, nullptr,
          block_cache_tracer, io_tracer, db_id, db_session_id)),
      default_cfd_cache_(nullptr),
      db_name_(dbname),
      db_options_(db_options),
      table_cache_(table_cache),
      write_buffer_manager_(_write_buffer_manager),
      write_controller_(_write_controller),
      block_cache_tracer_(block_cache_tracer),
      io_tracer_(io_tracer),
      db_id_(db_id),
      db_session_id_(db_session_id) {
  // initialize linked list
  // 初始化链表结构，形成循环链表
  // dummy_cfd_ 是哨兵节点，prev_ 和 next_ 都指向自己
  // 这样可以简化遍历和边界条件处理
  dummy_cfd_->prev_ = dummy_cfd_;
  dummy_cfd_->next_ = dummy_cfd_;
}

/**
 * ColumnFamilySet::~ColumnFamilySet - 析构函数
 *
 * 功能概述:
 *   - 清理所有列族的数据结构
 *   - 释放所有 ColumnFamilyData 对象
 *   - 释放 dummy_cfd_ 哨兵节点
 *
 * 清理流程:
 *   1. 遍历所有列族（column_family_data_）
 *   2. 对每个列族调用 UnrefAndTryDelete()
 *   3. 列族的析构函数会自动从 column_family_data_ 中移除自己
 *   4. 最后释放 dummy_cfd_
 *
 * 注意事项:
 *   - 清理过程中，列族数量会动态变化
 *   - 使用 while 循环直到集合为空
 *   - 断言所有引用计数为 0
 *   - dummy_cfd_ 也要释放（哨兵节点也需要清理）
 */
ColumnFamilySet::~ColumnFamilySet() {
  // 遍历所有列族，直到集合为空
  while (column_family_data_.size() > 0) {
    // cfd destructor will delete itself from column_family_data_
    // 列族的析构函数会自动从 column_family_data_ 中移除自己
    // 因此可以安全地遍历删除
    auto cfd = column_family_data_.begin()->second;

    // 减少引用计数，如果为 0 则删除列族
    // __attribute__((__unused__)) 避免未使用变量警告
    bool last_ref __attribute__((__unused__));
    last_ref = cfd->UnrefAndTryDelete();

    // 断言：这是最后一个引用
    // 如果不是，说明还有其他地方持有引用，析构会出错
    assert(last_ref);
  }

  // 释放 dummy_cfd_ 哨兵节点
  // dummy_cfd_ 也是 ColumnFamilyData，需要正确释放
  bool dummy_last_ref __attribute__((__unused__));
  dummy_last_ref = dummy_cfd_->UnrefAndTryDelete();
  assert(dummy_last_ref);
}

/**
 * ColumnFamilySet::GetDefault - 获取默认列族
 *
 * 功能概述:
 *   - 返回默认列族的 ColumnFamilyData 指针
 *   - 默认列族的 ID 固定为 0
 *   - 使用 default_cfd_cache_ 缓存优化性能
 *
 * 返回值:
 *   - 默认列族的 ColumnFamilyData 指针
 *
 * 断言:
 *   - 确保 default_cfd_cache_ 不为 nullptr
 *   - 需要先创建默认列族（通常是打开数据库时）
 *
 * 性能优化:
 *   - 使用 default_cfd_cache_ 缓存指针
 *   - 避免在 column_family_data_ 中查找
 *   - 这是一个常见的优化路径（大多数操作针对默认列族）
 *
 * 使用场景:
 *   - WriteBatch 写入到默认列族
 *   - 读取默认列族的数据
 *   - 获取默认列族的配置和统计信息
 */
ColumnFamilyData* ColumnFamilySet::GetDefault() const {
  // 断言：确保默认列族缓存已初始化
  // 如果为 nullptr，说明还未创建默认列族
  assert(default_cfd_cache_ != nullptr);

  // 直接返回缓存的指针
  // 默认列族始终存在，不需要检查有效性
  return default_cfd_cache_;
}

/**
 * ColumnFamilySet::GetColumnFamily - 根据列族 ID 获取列族
 *
 * 功能概述:
 *   - 通过列族 ID 查找对应的列族数据
 *   - 在哈希映射中查找，时间复杂度 O(1)
 *   - 返回 nullptr 表示列族不存在
 *
 * 参数说明:
 *   @param id: 列族 ID（uint32_t）
 *             0 表示默认列族
 *             非 0 表示指定的列族 ID
 *
 * 返回值:
 *   - 成功：列族的 ColumnFamilyData 指针
 *   - 失败：nullptr（列族不存在或已被删除）
 *
 * 查找流程:
 *   1. 在 column_family_data_ 中查找 ID
 *   2. 如果找到，返回对应的 ColumnFamilyData 指针
 *   3. 如果未找到，返回 nullptr
 *
 * 性能特性:
 *   - 使用 UnorderedMap（哈希表）
 *   - 平均查找时间：O(1)
 *   - 最坏查找时间：O(n)（哈希冲突）
 *
 * 使用场景:
 *   - WriteBatch 写入时根据 ID 查找目标列族
 *   - 内部操作中通过 ID 访问列族
 *   - WAL 恢复时根据 ID 找到对应的列族
 */
ColumnFamilyData* ColumnFamilySet::GetColumnFamily(uint32_t id) const {
  // 在哈希表中查找列族 ID
  // column_family_data_ 是 UnorderedMap<uint32_t, ColumnFamilyData*>
  // find() 返回迭代器，时间复杂度平均 O(1)
  auto cfd_iter = column_family_data_.find(id);

  // 检查是否找到
  if (cfd_iter != column_family_data_.end()) {
    // 找到列族，返回对应的 ColumnFamilyData 指针
    return cfd_iter->second;
  } else {
    // 未找到列族，返回 nullptr
    return nullptr;
  }
}

/**
 * ColumnFamilySet::GetColumnFamily - 根据列族名称获取列族
 *
 * 功能概述:
 *   - 通过列族名称查找对应的列族数据
 *   - 先在名称 → ID 映射中查找 ID
 *   - 再在 ID → ColumnFamilyData 映射中查找数据
 *   - 返回 nullptr 表示列族不存在
 *
 * 参数说明:
 *   @param name: 列族名称（字符串）
 *
 * 返回值:
 *   - 成功：列族的 ColumnFamilyData 指针
 *   - 失败：nullptr（列族不存在或已被删除）
 *
 * 查找流程:
 *   1. 在 column_families_ 中查找 name → ID
 *   2. 如果找到 ID，调用 GetColumnFamily(id)
 *   3. 断言找到的 ColumnFamilyData 不为 nullptr
 *   4. 返回找到的 ColumnFamilyData 或 nullptr
 *
 * 性能特性:
 *   - 两次哈希表查找
 *   - 平均查找时间：O(1)
 *   - 名称 → ID 查找 + ID → ColumnFamilyData 查找
 *
 * 使用场景:
 *   - 用户通过名称访问列族
 *   - DB::GetColumnFamily() 等用户 API
 *   - 日志和调试输出
 *
 * 注意事项:
 *   - 列族名称区分大小写
 *   - 列族名称在数据库中必须唯一
 *   - 返回的指针生命周期由 ColumnFamilySet 管理
 */
ColumnFamilyData* ColumnFamilySet::GetColumnFamily(
    const std::string& name) const {
  // 在名称映射中查找列族名称
  // column_families_ 是 UnorderedMap<std::string, uint32_t>
  // find() 返回迭代器，时间复杂度平均 O(1)
  auto cfd_iter = column_families_.find(name);

  if (cfd_iter != column_families_.end()) {
    // 找到名称对应的 ID
    // 调用 GetColumnFamily(id) 根据 ID 查找列族数据
    auto cfd = GetColumnFamily(cfd_iter->second);

    // 断言：确保找到的列族数据不为 nullptr
    // 正常情况下，如果名称存在于 column_families_，
    // 则对应的 ID 也应该存在于 column_family_data_
    assert(cfd != nullptr);

    return cfd;
  } else {
    // 未找到列族名称，返回 nullptr
    return nullptr;
  }
}

/**
 * ColumnFamilySet::GetNextColumnFamilyID - 获取下一个可用的列族 ID
 *
 * 功能概述:
 *   - 分配一个新的列族 ID
 *   - ID 单调递增，确保唯一性
 *   - 即使删除列族，ID 也不会重用
 *
 * 返回值:
 *   - 新的列族 ID（uint32_t）
 *   - 返回 ++max_column_family_（先递增后返回）
 *
 * ID 分配策略:
 *   - 使用 max_column_family_ 计数器
 *   - 每次调用返回 ++max_column_family_
 *   - 保证 ID 在数据库生命周期内不重复
 *
 * 唯一性保证:
 *   - 返回的 ID 大于任何已存在的列族 ID
 *   - 返回的 ID 在整个 RocksDB 实例历史中也是唯一的
 *   - 这确保了持久化到磁盘的 ID 不会冲突
 *
 * 使用场景:
 *   - 创建新列族时分配 ID
 *   - DB::CreateColumnFamily() 内部调用
 *   - 确保新列族的 ID 唯一
 *
 * 示例代码:
 *   // 创建新列族
 *   uint32_t new_id = cf_set.GetNextColumnFamilyID();
 *   ColumnFamilyData* cfd = cf_set.CreateColumnFamily(
 *       "new_cf", new_id, dummy_version, cf_options);
 *
 *   printf("Created column family with ID: %u\n", new_id);
 *
 * 注意事项:
 *   - ID 单调递增，可能溢出（uint32_t 最大值约 42 亿）
 *   - 溢出后会从 0 开始，但 0 已被默认列族使用
 *   - 实际使用中不太可能溢出（需要 40 亿个列族）
 *   - 调用前不需要持有锁（调用者负责同步）
 */
uint32_t ColumnFamilySet::GetNextColumnFamilyID() {
  // 返回递增后的 max_column_family_
  // 后置递增（++max_column_family_）表示先递增后返回
  // 例如：max_column_family_ = 5
  //       第一次调用返回 6，max_column_family_ 变为 7
  //       第二次调用返回 7，max_column_family_ 变为 8
  return ++max_column_family_;
}

/**
 * ColumnFamilySet::GetMaxColumnFamily - 获取当前最大的列族 ID
 *
 * 功能概述:
 *   - 返回当前已分配的最大列族 ID
 *   - 与 GetNextColumnFamilyID() 配合使用
 *   - 用于跟踪 ID 分配进度
 *
 * 返回值:
 *   - 当前最大列族 ID（uint32_t）
 *   - 如果没有列族，返回 0
 *
 * 使用场景:
 *   - 检查 ID 分配进度
 *   - 调试和诊断
 *   - 统计信息
 *   - 从 MANIFEST 恢复时验证 ID 状态
 *
 * 示例代码:
 *   uint32_t max_id = cf_set.GetMaxColumnFamily();
 *   printf("Current max CF ID: %u\n", max_id);
 *
 * 注意事项:
 *   - 删除列族不会减小 max_column_family_
 *   - 只单调递增，不递减
 *   - 这确保了 ID 的历史唯一性
 */
uint32_t ColumnFamilySet::GetMaxColumnFamily() { return max_column_family_; }

/**
 * ColumnFamilySet::UpdateMaxColumnFamily - 更新最大列族 ID
 *
 * 功能概述:
 *   - 更新 max_column_family_ 为指定值
 *   - 只能增大，不能减小
 *   - 用于从 MANIFEST 恢复时同步 ID 状态
 *
 * 参数说明:
 *   @param new_max_column_family: 新的最大列族 ID
 *                               函数会取此值与当前最大值的较大者
 *
 * 更新策略:
 *   - max_column_family_ = max(new_max_column_family, max_column_family_)
 *   - 确保只增大不减小
 *   - 使用 std::max 比较两个值
 *
 * 使用场景:
 *   - 从 MANIFEST 恢复列族 ID
 *   - 确保恢复后的 ID 分配器状态正确
 *   - DBImpl 初始化时从 MANIFEST 读取最大 ID
 *
 * 更新示例:
 *   // 假设当前 max_column_family_ = 5
 *   UpdateMaxColumnFamily(10);  // max_column_family_ 变为 10
 *   UpdateMaxColumnFamily(3);   // max_column_family_ 保持为 10
 *   UpdateMaxColumnFamily(15);  // max_column_family_ 变为 15
 *
 * 调用要求:
 *   - 需要在 DB 互斥锁保护下调用
 *   - 调用者需要确保传入的值是有效的
 *   - 不应频繁调用（只在恢复时调用）
 *
 * 注意事项:
 *   - 只能增大 max_column_family_
 *   - 调用者需要确保传入的值是有效的
 *   - 不应随意调用（只用于恢复同步）
 */
void ColumnFamilySet::UpdateMaxColumnFamily(uint32_t new_max_column_family) {
  // 更新 max_column_family_ 为当前值和新值的较大者
  // 使用 std::max 比较两个值
  // 确保只增大不减小，保持单调递增
  // 例如：
  // - max_column_family_ = 5, new_max = 10
  //   结果：max_column_family_ = 10（增大）
  // - max_column_family_ = 10, new_max = 3
  //   结果：max_column_family_ = 10（保持不变）
  max_column_family_ = std::max(new_max_column_family, max_column_family_);
}

/**
 * ColumnFamilySet::NumberOfColumnFamilies - 获取列族数量
 *
 * 功能概述:
 *   - 返回当前活跃的列族数量
 *   - 包括默认列族和所有用户创建的列族
 *   - 不包括已标记为 dropped 但未删除的列族
 *
 * 返回值:
 *   - 列族数量（size_t）
 *   - 至少为 1（包含默认列族）
 *
 * 计算方式:
 *   - 返回 column_families_.size()
 *   - column_families_ 是名称 → ID 的映射
 *   - 大小等于活跃列族的数量
 *
 * 使用场景:
 *   - 统计和监控
 *   - 调试和诊断
 *   - 性能分析
 *   - 日志输出
 *
 * 示例代码:
 *   size_t num_cfs = cf_set.NumberOfColumnFamilies();
 *   printf("Total column families: %zu\n", num_cfs);
 *
 * 注意事项:
 *   - 返回 column_families_ 的大小
 *   - 不包括 dummy_cfd_（哨兵节点）
 *   - 不包括已删除的列族（即使数据还存在）
 *   - 调用前不需要持有锁（调用者负责同步）
 */
size_t ColumnFamilySet::NumberOfColumnFamilies() const {
  // 返回名称映射的大小
  // column_families_ 是 UnorderedMap<std::string, uint32_t>
  // size() 返回当前映射中的元素数量
  // 这就是活跃列族的数量
  return column_families_.size();
}

/**
 * ColumnFamilySet::CreateColumnFamily - 创建新的列族
 *
 * 功能概述:
 *   - 创建新的列族并添加到集合
 *   - 初始化列族的元数据和资源
 *   - 更新名称 → ID 映射和 ID → ColumnFamilyData 映射
 *   - 将列族添加到双向链表
 *   - 如果是默认列族（ID=0），更新缓存
 *
 * 参数说明:
 *   @param name: 列族名称（必须唯一）
 *   @param id: 列族 ID（通过 GetNextColumnFamilyID() 分配）
 *   @param dummy_versions: 哨兵版本（用于版本管理）
 *   @param options: 列族选项（配置参数）
 *
 * 返回值:
 *   - 新创建的 ColumnFamilyData 指针
 *   - 调用者不应释放此指针
 *
 * 创建流程:
 *   1. 断言列族名称不重复
 *   2. 创建 ColumnFamilyData 对象
 *   3. 添加到 column_families_（名称 → ID）
 *   4. 添加到 column_family_data_（ID → ColumnFamilyData）
 *   5. 记录时间戳大小（如果启用了用户定义时间戳）
 *   6. 更新 max_column_family_
 *   7. 添加到双向链表
 *   8. 如果是默认列族（ID=0），更新 default_cfd_cache_
 *
 * 链表插入逻辑:
 *   - 插入到 dummy_cfd_ 之前
 *   - 保持插入顺序
 *   - 链表结构：... → dummy_cfd_ → new_cfd ↔ dummy_cfd_
 *
 * 调用要求:
 *   - 必须在 DB 互斥锁保护下调用
 *   - 必须从单线程写线程调用
 *   - 列族名称不能已存在
 *
 * 使用示例:
 *   uint32_t new_id = cf_set.GetNextColumnFamilyID();
 *   ColumnFamilyData* cfd = cf_set.CreateColumnFamily(
 *       "my_cf", new_id, dummy_version, cf_options);
 *
 *   if (cfd != nullptr) {
 *     printf("Created column family: %s (ID: %u)\n",
 *            cfd->GetName().c_str(), cfd->GetID());
 *   }
 *
 * 注意事项:
 *   - 列族名称必须唯一
 *   - 默认列族（ID=0）只能创建一次
 *   - ColumnFamilyData 的生命周期由引用计数管理
 *   - 新列族初始引用计数为 1
 */
// under a DB mutex AND write thread
ColumnFamilyData* ColumnFamilySet::CreateColumnFamily(
    const std::string& name, uint32_t id, Version* dummy_versions,
    const ColumnFamilyOptions& options) {
  // 断言：列族名称不能已存在
  // column_families_ 是名称 → ID 的映射
  // 如果名称已存在，find() 会返回非 end() 的迭代器
  // 这里断言名称不存在，确保唯一性
  assert(column_families_.find(name) == column_families_.end());

  // 创建新的 ColumnFamilyData 对象
  // 参数说明：
  // - id: 列族 ID
  // - name: 列族名称
  // - dummy_versions: 哨兵版本（用于版本管理）
  // - table_cache_: SSTable 缓存（共享）
  // - write_buffer_manager_: 写入缓冲区管理器（共享）
  // - options: 列族选项
  // - *db_options_: 数据库选项（引用）
  // - &file_options_: 文件选项（引用）
  // - this: ColumnFamilySet 指针（用于 RemoveColumnFamily）
  // - block_cache_tracer_, io_tracer_: 追踪器
  // - db_id_, db_session_id_: 数据库标识
  ColumnFamilyData* new_cfd = new ColumnFamilyData(
      id, name, dummy_versions, table_cache_, write_buffer_manager_, options,
      *db_options_, &file_options_, this, block_cache_tracer_, io_tracer_,
      db_id_, db_session_id_);

  // 添加到名称 → ID 映射
  // column_families_ 是 UnorderedMap<std::string, uint32_t>
  // insert() 添加新的键值对
  column_families_.insert({name, id});

  // 添加到 ID → ColumnFamilyData 映射
  // column_family_data_ 是 UnorderedMap<uint32_t, ColumnFamilyData*>
  // insert() 添加新的键值对
  column_family_data_.insert({id, new_cfd});

  // 获取用户比较器，检查时间戳大小
  // user_comparator() 返回列族使用的比较器
  // 比较器包含时间戳大小信息
  auto ucmp = new_cfd->user_comparator();
  assert(ucmp);

  // 获取时间戳大小
  // timestamp_size() 返回用户定义时间戳的字节数
  // 0 表示未启用时间戳
  // 非 0 表示时间戳的字节数（如 8 字节）
  size_t ts_sz = ucmp->timestamp_size();

  // 记录时间戳大小到运行时映射
  // running_ts_sz_ 包含所有列族的时间戳大小
  running_ts_sz_.insert({id, ts_sz});

  // 如果时间戳大小非 0，记录到持久化映射
  // ts_sz_for_record_ 只包含启用了时间戳的列族
  // 用于持久化和版本管理
  if (ts_sz > 0) {
    ts_sz_for_record_.insert({id, ts_sz});
  }

  // 更新最大列族 ID
  // 使用 std::max 确保只增大不减小
  max_column_family_ = std::max(max_column_family_, id);

  // 添加到双向链表
  // 插入到 dummy_cfd_ 之前，保持插入顺序
  // 链表结构：... → prev → new_cfd → dummy_cfd_ ↔ ...
  //
  // 步骤：
  // 1. new_cfd->next_ = dummy_cfd_（新节点下一个是 dummy）
  // 2. prev = dummy_cfd_->prev_（获取当前最后一个节点）
  // 3. new_cfd->prev_ = prev（新节点前驱是当前最后一个节点）
  // 4. prev->next_ = new_cfd（当前最后一个节点的下一个是新节点）
  // 5. dummy_cfd_->prev_ = new_cfd（dummy 的前驱是新节点）
  new_cfd->next_ = dummy_cfd_;
  auto prev = dummy_cfd_->prev_;
  new_cfd->prev_ = prev;
  prev->next_ = new_cfd;
  dummy_cfd_->prev_ = new_cfd;

  // 如果是默认列族（ID=0），更新缓存
  // 默认列族的 ID 固定为 0
  // 缓存指针以优化访问性能
  if (id == 0) {
    default_cfd_cache_ = new_cfd;
  }

  // 返回新创建的列族数据
  // 调用者可以使用此指针进行后续操作
  return new_cfd;
}

/**
 * ColumnFamilySet::RemoveColumnFamily - 从集合中移除列族
 *
 * 功能概述:
 *   - 从列族集合中移除指定的列族
 *   - 清理名称 → ID 映射和 ID → ColumnFamilyData 映射
 *   - 清理时间戳大小映射
 *   - 由 ColumnFamilyData 的析构函数自动调用
 *
 * 参数说明:
 *   @param cfd: 要移除的列族数据指针
 *
 * 移除流程:
 *   1. 获取列族 ID
 *   2. 从 column_family_data_ 中移除 ID → ColumnFamilyData 映射
 *   3. 从 column_families_ 中移除名称 → ID 映射
 *   4. 从 running_ts_sz_ 中移除时间戳大小映射
 *   5. 从 ts_sz_for_record_ 中移除时间戳大小映射（如果存在）
 *
 * 调用时机:
 *   - ColumnFamilyData 的引用计数为 0 时
 *   - 在 UnrefAndTryDelete() 中调用
 *   - 由 ColumnFamilyData 的析构函数自动调用
 *
 * 调用要求:
 *   - 必须在 DB 互斥锁保护下调用
 *   - 断言列族存在于集合中
 *   - 由 ColumnFamilyData 析构函数调用，不需要手动调用
 *
 * 清理说明:
 *   - 此函数只从映射中移除引用
 *   - 不释放 ColumnFamilyData 对象（由调用者负责）
 *   - 不从链表中移除（由 ColumnFamilyData 析构函数处理）
 *
 * 注意事项:
 *   - 此函数由 ColumnFamilyData 析构函数调用
 *   - 不需要手动调用
 *   - 不释放 ColumnFamilyData 对象（由调用者负责）
 */
// under a DB mutex AND from a write thread
void ColumnFamilySet::RemoveColumnFamily(ColumnFamilyData* cfd) {
  // 获取列族 ID
  uint32_t cf_id = cfd->GetID();

  // 在 ID → ColumnFamilyData 映射中查找列族
  // column_family_data_ 是 UnorderedMap<uint32_t, ColumnFamilyData*>
  auto cfd_iter = column_family_data_.find(cf_id);

  // 断言：列族必须存在于映射中
  // 如果不存在，说明状态不一致，是程序错误
  assert(cfd_iter != column_family_data_.end());

  // 从 ID → ColumnFamilyData 映射中移除
  // erase() 删除指定的键值对
  column_family_data_.erase(cfd_iter);

  // 从名称 → ID 映射中移除
  // 列族名称必须在 column_families_ 中存在
  column_families_.erase(cfd->GetName());

  // 从运行时时间戳大小映射中移除
  // running_ts_sz_ 是 UnorderedMap<uint32_t, size_t>
  running_ts_sz_.erase(cf_id);

  // 从持久化时间戳大小映射中移除
  // ts_sz_for_record_ 是 UnorderedMap<uint32_t, size_t>
  // 只包含时间戳大小非 0 的列族
  // erase() 会检查键是否存在，不存在时不报错
  ts_sz_for_record_.erase(cf_id);
}

// under a DB mutex OR from a write thread
/**
 * ColumnFamilyMemTablesImpl::Seek - 查找并选中指定的列族
 *
 * 功能概述:
 *   - 根据列族 ID 在列族集合中查找对应的列族
 *   - 更新内部状态 current_ 指向找到的列族数据
 *   - 更新内部句柄 handle_ 指向当前列族
 *   - 支持默认列族的快速路径优化
 *
 * 查找流程:
 *   1. 检查列族 ID:
 *      - 如果 column_family_id == 0:
 *        → 快速路径：直接获取默认列族
 *        → 调用 column_family_set_->GetDefault()
 *      - 如果 column_family_id != 0:
 *        → 常规路径：在列族集合中查找
 *        → 调用 column_family_set_->GetColumnFamily(column_family_id)
 *
 *   2. 更新内部状态:
 *      - current_ = 找到的列族数据（或 nullptr）
 *      - handle_.SetCFD(current_)  // 更新句柄的列族引用
 *
 *   3. 返回查找结果:
 *      - 返回 true: 列族存在，current_ 已更新
 *      - 返回 false: 列族不存在，current_ 为 nullptr
 *
 * 参数说明:
 *   @param column_family_id: 要查找的列族 ID
 *                           0 表示默认列族（快速路径）
 *                           非 0 表示指定的列族 ID
 *
 * 返回值:
 *   - true: 成功找到列族
 *   - false: 列族不存在
 *
 * 实现细节:
 *   - 默认列族优化（column_family_id == 0）:
 *     - 大多数操作针对默认列族（ID=0）
 *     - GetDefault() 直接返回默认列族指针，避免查找
 *     - 这是一个常见的优化路径
 *
 *   - 非默认列族（column_family_id != 0）:
 *     - GetColumnFamily() 在列族集合中查找
 *     - 使用哈希表或映射结构，查找时间复杂度 O(1)
 *     - 如果列族不存在，返回 nullptr
 *
 *   - 内部句柄更新:
 *     - handle_.SetCFD(current_) 更新句柄的列族引用
 *     - 后续调用 GetColumnFamilyHandle() 时返回此句柄
 *     - 避免重复创建句柄对象
 *
 * 性能优化:
 *   - 默认列族快速路径：直接获取，避免查找
 *   - 避免重复查找：批量处理同一列族的多个操作时，只调用一次 Seek()
 *   - ColumnFamilySet 通常使用 std::unordered_map，查找速度 O(1)
 *
 * 使用示例:
 *   // 示例1: 查找默认列族
 *   ColumnFamilyMemTablesImpl cf_mems(cf_set);
 *   if (cf_mems.Seek(0)) {
 *     MemTable* mem = cf_mems.GetMemTable();
 *     // 使用 mem 插入数据...
 *   }
 *
 *   // 示例2: 查找指定列族
 *   uint32_t cf_id = GetColumnFamilyID(handle);
 *   if (cf_mems.Seek(cf_id)) {
 *     MemTable* mem = cf_mems.GetMemTable();
 *     uint64_t log_num = cf_mems.GetLogNumber();
 *     // 使用 mem 和 log_num...
 *   } else {
 *     LOG(ERROR) << "Column family " << cf_id << " not found";
 *   }
 *
 *   // 示例3: 批量处理 WriteBatch
 *   WriteBatch batch;
 *   // ... 添加多个操作到 batch ...
 *
 *   uint32_t last_cf_id = UINT32_MAX;
 *   for (auto& entry : batch) {
 *     uint32_t cf_id = entry.column_family;
 *
 *     // 只在列族 ID 变化时调用 Seek()
 *     if (cf_id != last_cf_id) {
 *       if (!cf_mems.Seek(cf_id)) {
 *         continue;  // 列族不存在，跳过
 *       }
 *       last_cf_id = cf_id;
 *     }
 *
 *     MemTable* mem = cf_mems.GetMemTable();
 *     mem->Add(entry.sequence, entry.type, entry.key, entry.value);
 *   }
 *
 * 错误处理:
 *   - 列族不存在时返回 false
 *   - current_ 被设置为 nullptr
 *   - 后续方法（GetMemTable、GetLogNumber）会 assert(current_ != nullptr)
 *   - 调用者必须检查返回值
 *
 * 并发控制:
 *   - 必须在 DB 互斥锁保护下调用
 *   - 不能与其他线程的 Seek() 并发调用（会修改 current_）
 *   - 线程间的其他方法调用（GetMemTable 等）也需要互斥锁保护
 *
 * 相关函数:
 *   - GetMemTable(): 需要先调用 Seek()
 *   - GetLogNumber(): 需要先调用 Seek()
 *   - GetColumnFamilyHandle(): 需要先调用 Seek()
 *   - current(): 返回 current_ 指针
 *
 * 注意事项:
 *   1. 调用后必须检查返回值
 *   2. Seek() 会修改内部状态，不能并发调用
 *   3. 返回的句柄生命周期与实例绑定
 *   4. 列族可能在调用后被删除（需要外部同步）
 */
bool ColumnFamilyMemTablesImpl::Seek(uint32_t column_family_id) {
  if (column_family_id == 0) {
    // optimization for common case
    // 默认列族的快速路径优化
    // 大多数操作针对默认列族（ID=0），直接获取避免查找
    current_ = column_family_set_->GetDefault();
  } else {
    // 在列族集合中查找指定 ID 的列族
    // ColumnFamilySet 通常使用 std::unordered_map，查找时间复杂度 O(1)
    // 如果列族不存在，返回 nullptr
    current_ = column_family_set_->GetColumnFamily(column_family_id);
  }

  // 更新内部句柄的列族引用
  // handle_ 是 ColumnFamilyHandleInternal 类型
  // SetCFD() 设置 handle_.internal_cfd_ = current_
  // 后续调用 GetColumnFamilyHandle() 时返回此句柄
  handle_.SetCFD(current_);

  // 返回查找结果
  // true: 列族存在，current_ 已更新
  // false: 列族不存在，current_ 为 nullptr
  return current_ != nullptr;
}

/**
 * ColumnFamilyMemTablesImpl::GetLogNumber - 获取当前选中列族的日志号
 *
 * 功能概述:
 *   - 返回当前列族的 WAL 日志号
 *   - 日志号表示该列族数据关联的 WAL 文件编号
 *   - 用于判断写入是否已经持久化到磁盘
 *
 * 日志号的作用:
 *   1. WAL 恢复时判断操作是否已经处理:
 *      - 每个列族维护一个日志号，表示该列族数据关联的 WAL 文件
 *      - 重放 WAL 时，如果当前日志号 >= WAL 日志号，说明数据已处理
 *      - 可以跳过重复的操作，避免重复写入
 *
 *   2. 跟踪写入进度:
 *      - 每次写入时更新列族的日志号
 *      - 用于标识哪些操作已经持久化到 WAL
 *      - 在 Flush 和 Compaction 时用于版本管理
 *
 *   3. 列族恢复:
 *      - 打开数据库时，从 MANIFEST 文件读取列族的日志号
 *      - 用于确定哪些 WAL 文件需要重放
 *
 * 返回值:
 *   - 当前列族的日志号（uint64_t）
 *   - 表示该列族数据关联的 WAL 文件编号
 *
 * 实现细节:
 *   - 直接调用 current_->GetLogNumber()
 *   - current_ 是 ColumnFamilyData 指针
 *   - ColumnFamilyData 内部维护 log_number_ 成员
 *
 * 日志号的更新时机:
 *   1. 写入到 WAL 后:
 *      - 每次写入成功后，更新列族的日志号
 *      - log_number_ = 当前 WAL 文件编号
 *
 *   2. Flush 到磁盘后:
 *      - Flush 完成后，更新列族的日志号
 *      - 表示 Flush 后的数据已经持久化
 *
 *   3. 恢复数据库时:
 *      - 从 MANIFEST 文件读取列族的日志号
 *      - 用于确定需要重放的 WAL 文件范围
 *
 * 调用要求:
 *   - REQUIRES: 必须先调用 Seek() 并返回 true
 *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
 *   - 如果 current_ 为 nullptr，会触发 assert
 *
 * 使用示例:
 *   // 示例1: WAL 恢复时判断操作是否已处理
 *   uint64_t wal_log_number = wal_reader->GetLogNumber();
 *
 *   if (cf_mems.Seek(cf_id)) {
 *     uint64_t cf_log_number = cf_mems.GetLogNumber();
 *
 *     if (cf_log_number >= wal_log_number) {
 *       // 操作已经处理过，跳过
 *       LOG(INFO) << "Skipping already processed operation";
 *       continue;
 *     }
 *
 *     // 将操作插入到 MemTable
 *     MemTable* mem = cf_mems.GetMemTable();
 *     mem->Add(entry.sequence, entry.type, entry.key, entry.value);
 *   }
 *
 *   // 示例2: 跟踪写入进度
 *   if (cf_mems.Seek(cf_id)) {
 *     uint64_t log_num = cf_mems.GetLogNumber();
 *     printf("Column family %u log number: %lu\n", cf_id, log_num);
 *   }
 *
 *   // 示例3: Flush 时检查日志号
 *   if (cf_mems.GetLogNumber() > flush_threshold) {
 *     // 需要触发 Flush
 *     ScheduleFlush(cf_id);
 *   }
 *
 * 错误处理:
 *   - 如果 current_ 为 nullptr（Seek 未成功），会触发 assert
 *   - 调用者必须确保 Seek() 返回 true
 *   - 返回的日志号可能为 0（初始状态或异常情况）
 *
 * 并发控制:
 *   - 必须在 DB 互斥锁保护下调用
 *   - 与其他方法（Seek、GetMemTable 等）共享互斥锁
 *   - ColumnFamilyData 内部确保 log_number_ 的线程安全
 *
 * 性能考虑:
 *   - 直接返回成员变量，开销极小
 *   - 可以在循环中频繁调用（无需缓存）
 *
 * 相关概念:
 *   - WAL (Write-Ahead Log): 预写日志，用于崩溃恢复
 *   - Log Number: WAL 文件的编号，单调递增
 *   - ColumnFamilyData: 列族数据结构，包含 log_number_ 成员
 *   - Recovering: 从 WAL 恢复数据时使用日志号判断重复
 *
 * 注意事项:
 *   1. 必须先调用 Seek() 并成功
 *   2. 日志号是列族级别的，不同列族可能有不同的日志号
 *   3. 日志号在写入和 Flush 时更新，需要注意并发访问
 *   4. 恢复时需要正确处理日志号，避免重复或丢失数据
 */
uint64_t ColumnFamilyMemTablesImpl::GetLogNumber() const {
  // 断言：确保当前列族已选中
  // 如果 current_ 为 nullptr，说明 Seek() 未成功或未调用
  // 这属于程序错误，会触发 assert
  assert(current_ != nullptr);

  // 返回当前列族的日志号
  // current_->GetLogNumber() 返回 ColumnFamilyData 内部的 log_number_ 成员
  // log_number_ 表示该列族数据关联的 WAL 文件编号
  return current_->GetLogNumber();
}

/**
 * ColumnFamilyMemTablesImpl::GetMemTable - 获取当前选中列族的 MemTable
 *
 * 功能概述:
 *   - 返回当前列族的活跃 MemTable 指针
 *   - MemTable 用于存储最近的写入操作（未持久化到磁盘）
 *   - 调用者可以使用此指针进行数据的插入、查询等操作
 *
 * MemTable 的作用:
 *   1. 存储最近的写入:
 *      - MemTable 是内存中的数据结构
 *      - 存储最近的 Put、Delete、Merge 等操作
 *      - 提供快速的内存读写访问
 *
 *   2. 写入路径:
 *      - 写入流程：Write → MemTable → Immutable MemTable → SSTable
 *      - 所有写入首先写入 MemTable（快速路径）
 *      - MemTable 满后切换到不可变 MemTable
 *      - 不可变 MemTable 被后台线程 Flush 到磁盘
 *
 *   3. 读操作:
 *      - 读取数据时先查询 MemTable
 *      - 如果 MemTable 中没有，查询不可变 MemTable
 *      - 最后查询磁盘上的 SSTable
 *   - MemTable 中的数据是最新的（序列号最大）
 *
 *   4. 持久化:
 *      - MemTable 数据在 Flush 时写入 SSTable
 *      - Flush 后 MemTable 被重置或释放
 *      - 确保数据最终持久化到磁盘
 *
 * 返回值:
 *   - 当前列族的 MemTable 指针（MemTable*）
 *   - 指针由 ColumnFamilyData 拥有，调用者不应释放
 *   - 如果列族正在 Flush，可能返回新的 MemTable
 *
 * MemTable 的结构:
 *   - 底层存储：跳表（SkipList）
 *   - 支持：Put、Delete、Merge、DeleteRange 等操作
 *   - 并发写入：使用 mutex 或无锁数据结构
 *   - 内存管理：使用 Arena 分配器，减少内存碎片
 *
 * MemTable 的生命周期:
 *   1. 创建:
 *      - 列族创建时初始化 MemTable
 *      - Flush 后创建新的 MemTable
 *
 *   2. 活跃状态:
 *      - 接受新的写入操作
 *      - 可以读取和修改
 *
 *   3. 切换:
 *      - MemTable 达到大小限制后切换
 *      - 切换为不可变 MemTable（Immutable MemTable）
 *      - 创建新的活跃 MemTable
 *
 *   4. Flush:
 *      - 不可变 MemTable 被 Flush 到磁盘
 *      - Flush 完成后释放内存
 *
 * 调用要求:
 *   - REQUIRES: 必须先调用 Seek() 并返回 true
 *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
 *   - 如果 current_ 为 nullptr，会触发 assert
 *
 * 使用示例:
 *   // 示例1: WriteBatch 写入到 MemTable
 *   WriteBatch batch;
 *   // ... 添加 Put/Delete 操作到 batch ...
 *
 *   for (auto& entry : batch) {
 *     uint32_t cf_id = entry.column_family;
 *
 *     if (cf_mems.Seek(cf_id)) {
 *       MemTable* mem = cf_mems.GetMemTable();
 *
 *       // 插入到 MemTable
 *       // entry.sequence: 序列号
 *       // entry.type: 操作类型（Put/Delete/Merge 等）
 *       // entry.key: 键
 *       // entry.value: 值
 *       mem->Add(entry.sequence, entry.type, entry.key, entry.value);
 *     }
 *   }
 *
 *   // 示例2: 查询 MemTable 中的数据
 *   if (cf_mems.Seek(cf_id)) {
 *     MemTable* mem = cf_mems.GetMemTable();
 *
 *     // 创建迭代器查询数据
 *     Arena arena;
 *     auto iter = mem->NewIterator(ReadOptions(), &arena);
 *     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
 *       printf("Key: %s, Value: %s\n",
 *              iter->key().ToString().c_str(),
 *              iter->value().ToString().c_str());
 *     }
 *   }
 *
 *   // 示例3: 获取 MemTable 统计信息
 *   if (cf_mems.Seek(cf_id)) {
 *     MemTable* mem = cf_mems.GetMemTable();
 *
 *     size_t num_entries = mem->num_entries();
 *     size_t approximate_size = mem->ApproximateMemoryUsage();
 *
 *     printf("MemTable entries: %zu, size: %zu bytes\n",
 *            num_entries, approximate_size);
 *   }
 *
 * 实现细节:
 *   - 直接调用 current_->mem()
 *   - ColumnFamilyData 内部维护活跃 MemTable 的引用
 *   - MemTable 的生命周期由 ColumnFamilyData 管理
 *   - 返回的指针在持有 DB 互斥锁时有效
 *
 * MemTable 的配置:
 *   - 大小限制：由 write_buffer_size 控制（默认 64MB）
 *   - 并发写入：由 allow_concurrent_memtable_write 控制
 *   - 写入速率限制：由 memtable_insert_hint_bytes_per_block 控制
 *
 * 错误处理:
 *   - 如果 current_ 为 nullptr（Seek 未成功），会触发 assert
 *   - 调用者必须确保 Seek() 返回 true
 *   - 返回的 MemTable 指针可能为 nullptr（异常情况）
 *   - MemTable 可能处于 Flush 状态（需要外部同步）
 *
 * 并发控制:
 *   - 必须在 DB 互斥锁保护下调用
 *   - MemTable 本身支持并发写入（有自己的锁机制）
 *   - 调用者不应在持有 DB 互斥锁的情况下长时间操作 MemTable
 *   - MemTable 插入时内部使用 mutex（或无锁数据结构）
 *
 * 性能考虑:
 *   - 直接返回成员变量，开销极小
 *   - 可以在循环中频繁调用（无需缓存）
 *   - MemTable 插入是内存操作，速度快
 *   - 批量操作同一列族时，可以多次调用 GetMemTable()
 *
 * 相关概念:
 *   - MemTable: 内存中的数据结构，存储最近的写入
 *   - Immutable MemTable: 不可变的 MemTable，等待 Flush
 *   - Flush: 将不可变 MemTable 写入 SSTable
 *   - SSTable: 磁盘上的有序数据文件
 *   - SkipList: 跳表数据结构，MemTable 的底层实现
 *
 * 优化建议:
 *   1. 批量处理同一列族的多个操作:
 *      - 只调用一次 Seek()
 *      - 多次调用 GetMemTable()（开销小）
 *
 *   2. 避免长时间持有 DB 互斥锁:
 *      - 获取 MemTable 指针后释放锁
 *      - 在 MemTable 内部使用自己的锁
 *
 *   3. 监控 MemTable 大小:
 *      - 及时 Flush 避免 OOM
 *      - 调整 write_buffer_size 参数
 *
 * 注意事项:
 *   1. 必须先调用 Seek() 并成功
 *   2. 返回的指针仅在持有 DB 互斥锁时有效（或确保列族不被删除）
 *   3. 不要释放此指针（由 ColumnFamilyData 管理）
 *   4. MemTable 可能在后台被 Flush，需要注意并发访问
 *   5. 不同列族的 MemTable 是独立的
 */
MemTable* ColumnFamilyMemTablesImpl::GetMemTable() const {
  // 断言：确保当前列族已选中
  // 如果 current_ 为 nullptr，说明 Seek() 未成功或未调用
  // 这属于程序错误，会触发 assert
  assert(current_ != nullptr);

  // 返回当前列族的活跃 MemTable 指针
  // current_->mem() 返回 ColumnFamilyData 内部的 mem_ 成员
  // mem_ 是 MemTable* 类型，指向当前列族的活跃 MemTable
  // MemTable 用于存储最近的写入操作（未持久化到磁盘）
  //
  // MemTable 的特性:
  // - 底层存储：跳表（SkipList）
  // - 支持：Put、Delete、Merge、DeleteRange 等操作
  // - 并发写入：使用 mutex 或无锁数据结构
  // - 内存管理：使用 Arena 分配器，减少内存碎片
  //
  // 调用者可以使用此指针进行:
  // - 插入数据：mem->Add(sequence, type, key, value)
  // - 查询数据：mem->Get(key, value)
  // - 创建迭代器：mem->NewIterator(options, arena)
  //
  // 注意事项:
  // - 指针由 ColumnFamilyData 拥有，调用者不应释放
  // - 指针的生命周期与 ColumnFamilyData 绑定
  // - 在持有 DB 互斥锁时有效（或确保列族不被删除）
  return current_->mem();
}

/**
 * ColumnFamilyMemTablesImpl::GetColumnFamilyHandle - 获取当前选中列族的句柄
 *
 * 功能概述:
 *   - 返回当前列族的内部句柄（ColumnFamilyHandleInternal*）
 *   - 句柄可以用于后续的列族操作（如读取、写入）
 *   - 句柄的生命周期与 ColumnFamilyMemTablesImpl 实例绑定
 *
 * ColumnFamilyHandle 的作用:
 *   1. 列族标识:
 *      - 唯一标识一个列族
 *      - 包含列族 ID、比较器等信息
 *   2. API 参数:
 *      - 许多 RocksDB API 需要 ColumnFamilyHandle 作为参数
 *      - 用于指定操作的目标列族
 *   3. 用户接口:
 *      - 用户通过句柄访问列族
 *      - 句柄提供了列族的抽象接口
 *
 * ColumnFamilyHandleInternal 的特性:
 *   1. 动态切换:
 *      - 支持动态设置关联的 ColumnFamilyData（SetCFD）
 *      - 可以在运行时切换到不同的列族
 *      - 适合内部操作场景
 *
 *   2. 轻量级:
 *      - 比标准的 ColumnFamilyHandleImpl 更轻量级
 *      - 避免频繁创建新的句柄对象
 *      - 减少内存分配和复制开销
 *
 *   3. 内部使用:
 *      - 主要用于 DB 内部操作
 *      - 不暴露给用户
 *      - 用于优化性能
 *
 * 返回值:
 *   - 当前列族的句柄（ColumnFamilyHandle*）
 *   - 指向内部的 handle_ 成员
 *   - 调用者不应释放此指针
 *
 * ColumnFamilyHandle 的接口:
 *   - GetID(): 获取列族 ID
 *   - GetName(): 获取列族名称
 *   - GetComparator(): 获取比较器
 *   - GetDefault(): 判断是否是默认列族
 *   - cfd(): 获取内部 ColumnFamilyData 指针
 *
 * 调用要求:
 *   - REQUIRES: 必须先调用 Seek() 并返回 true
 *   - REQUIRES: 必须在 DB 互斥锁保护下调用，或从写线程调用
 *   - 如果 current_ 为 nullptr，会触发 assert
 *
 * 使用示例:
 *   // 示例1: 获取列族 ID
 *   if (cf_mems.Seek(cf_id)) {
 *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
 *     uint32_t id = handle->GetID();
 *     printf("Column family ID: %u\n", id);
 *   }
 *
 *   // 示例2: 获取列族名称
 *   if (cf_mems.Seek(cf_id)) {
 *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
 *     std::string name = handle->GetName();
 *     printf("Column family name: %s\n", name.c_str());
 *   }
 *
 *   // 示例3: 用于 ReadOptions
 *   if (cf_mems.Seek(cf_id)) {
 *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
 *     ReadOptions opts;
 *
 *     // 创建迭代器，指定列族
 *     auto iter = db->NewIterator(opts, handle);
 *
 *     // 使用迭代器查询数据
 *     for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
 *       printf("Key: %s, Value: %s\n",
 *              iter->key().ToString().c_str(),
 *              iter->value().ToString().c_str());
 *     }
 *   }
 *
 *   // 示例4: 用于写入操作
 *   if (cf_mems.Seek(cf_id)) {
 *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
 *
 *     // 使用句柄进行写入
 *     Status s = db->Put(WriteOptions(), handle, "key", "value");
 *     if (!s.ok()) {
 *       LOG(ERROR) << "Write failed: " << s.ToString();
 *     }
 *   }
 *
 *   // 示例5: 用于事务操作
 *   if (cf_mems.Seek(cf_id)) {
 *     ColumnFamilyHandle* handle = cf_mems.GetColumnFamilyHandle();
 *
 *     // 在事务中使用句柄
 *     Transaction* txn = db->BeginTransaction(WriteOptions());
 *     txn->Put(handle, "key", "value");
 *     Status s = txn->Commit();
 *     if (!s.ok()) {
 *       LOG(ERROR) << "Transaction commit failed: " << s.ToString();
 *     }
 *     delete txn;
 *   }
 *
 * 实现细节:
 *   - 返回内部成员 handle_ 的指针
 *   - handle_ 是 ColumnFamilyHandleInternal 类型
 *   - handle_.internal_cfd_ 在 Seek() 时被设置为 current_
 *   - handle_ 的生命周期与 ColumnFamilyMemTablesImpl 实例绑定
 *
 * ColumnFamilyHandleInternal 的结构:
 *   class ColumnFamilyHandleInternal : public ColumnFamilyHandleImpl {
 *    public:
 *     ColumnFamilyHandleInternal()
 *         : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
 *           internal_cfd_(nullptr) {}
 *
 *     void SetCFD(ColumnFamilyData* _cfd) { internal_cfd_ = _cfd; }
 *     virtual ColumnFamilyData* cfd() const override { return internal_cfd_; }
 *
 *    private:
 *     ColumnFamilyData* internal_cfd_;  // 动态关联的列族数据
 *   };
 *
 * 与标准句柄的区别:
 *   1. 标准句柄（ColumnFamilyHandleImpl）:
 *      - 在创建时固定列族
 *      - 不支持动态切换列族
 *      - 用于用户接口
 *
 *   2. 内部句柄（ColumnFamilyHandleInternal）:
 *      - 可以动态切换列族（通过 SetCFD）
 *      - 轻量级，避免重复创建
 *      - 用于 DB 内部操作
 *
 * 错误处理:
 *   - 如果 current_ 为 nullptr（Seek 未成功），会触发 assert
 *   - 调用者必须确保 Seek() 返回 true
 *   - 返回的句柄可能指向无效数据（异常情况）
 *   - 句柄关联的列族可能被删除（需要外部同步）
 *
 * 并发控制:
 *   - 必须在 DB 互斥锁保护下调用
 *   - 句柄本身不支持并发使用
 *   - 调用者不应在多线程间共享此句柄
 *   - 句柄关联的列族可能被其他线程修改（需要外部同步）
 *
 * 性能考虑:
 *   - 直接返回成员指针，开销极小
 *   - 避免频繁创建新的句柄对象
 *   - 适合在循环中频繁调用
 *
 * 使用场景:
 *   1. WriteBatch 内部操作:
 *      - 遍历 WriteBatch 时获取列族句柄
 *      - 用于后续的列族操作
 *
 *   2. WAL 恢复:
 *      - 恢复 WAL 时获取列族句柄
 *      - 用于将数据恢复到正确的列族
 *
 *   3. 内部 API 调用:
 *      - 某些内部 API 需要 ColumnFamilyHandle 参数
 *      - 使用此方法获取句柄
 *
 * 注意事项:
 *   1. 句柄的生命周期与 ColumnFamilyMemTablesImpl 实例绑定:
 *      - 不要在实例销毁后使用返回的句柄
 *      - 句柄在实例销毁时失效
 *
 *   2. 调用 Seek() 后，句柄会更新:
 *      - 每次调用 Seek() 都会更新句柄的列族引用
 *      - 调用者需要注意句柄的当前状态
 *
 *   3. 不要释放返回的句柄:
 *      - 句柄由 ColumnFamilyMemTablesImpl 拥有
 *      - 释放句柄会导致未定义行为
 *
 *   4. 多线程安全:
 *      - 句柄不支持多线程并发使用
 *      - 调用者需要外部同步
 *
 *   5. 列族删除:
 *      - 句柄关联的列族可能被删除
 *      - 调用者需要检查列族是否有效
 *
 * 相关类:
 *   - ColumnFamilyHandle: 句柄基类
 *   - ColumnFamilyHandleImpl: 标准句柄实现
 *   - ColumnFamilyHandleInternal: 内部句柄实现（此类使用的）
 *   - ColumnFamilyData: 列族数据结构
 *
 * 优化建议:
 *   1. 批量操作:
 *      - 批量处理同一列族的多个操作时，只调用一次 GetColumnFamilyHandle()
 *      - 避免重复调用
 *
 *   2. 缓存句柄:
 *      - 如果需要在多次操作中使用同一列族的句柄，可以缓存
 *      - 注意：缓存时需要保证句柄的有效性
 *
 *   3. 错误检查:
 *      - 调用后检查句柄的有效性
 *      - 检查列族是否被删除
 */
ColumnFamilyHandle* ColumnFamilyMemTablesImpl::GetColumnFamilyHandle() {
  // 断言：确保当前列族已选中
  // 如果 current_ 为 nullptr，说明 Seek() 未成功或未调用
  // 这属于程序错误，会触发 assert
  assert(current_ != nullptr);

  // 返回当前列族的内部句柄
  // handle_ 是 ColumnFamilyHandleInternal 类型
  // 在 Seek() 时，handle_.internal_cfd_ 被设置为 current_
  //
  // ColumnFamilyHandleInternal 的特性:
  // - 继承自 ColumnFamilyHandleImpl
  // - 支持动态设置关联的 ColumnFamilyData（SetCFD）
  // - 用于内部操作，避免频繁创建新的句柄对象
  // - 比标准的 ColumnFamilyHandleImpl 更轻量级
  //
  // 返回的句柄可以用于:
  // - GetID(): 获取列族 ID
  // - GetName(): 获取列族名称
  // - GetComparator(): 获取比较器
  // - cfd(): 获取内部 ColumnFamilyData 指针
  // - 作为 API 参数传递（如 NewIterator、Put 等）
  //
  // 注意事项:
  // - 句柄的生命周期与 ColumnFamilyMemTablesImpl 实例绑定
  // - 调用 Seek() 后，句柄会更新为新的列族
  // - 不要释放此句柄（由 ColumnFamilyMemTablesImpl 拥有）
  // - 不要在实例销毁后使用返回的句柄
  return &handle_;
}

uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family) {
  uint32_t column_family_id = 0;
  if (column_family != nullptr) {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    column_family_id = cfh->GetID();
  }
  return column_family_id;
}

const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family) {
  if (column_family != nullptr) {
    return column_family->GetComparator();
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
