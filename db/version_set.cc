//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstdio>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_source.h"
#include "db/compaction/compaction.h"
#include "db/compaction/file_pri.h"
#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/version_edit_handler.h"
#include "file/file_util.h"
#include "table/compaction_merging_iterator.h"

#if USE_COROUTINES
#include "folly/experimental/coro/BlockingWait.h"
#include "folly/experimental/coro/Collect.h"
#endif
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/persistent_stats_history.h"
#include "options/options_helper.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/meta_blocks.h"
#include "table/multiget_context.h"
#include "table/plain/plain_table_factory.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/coro_utils.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/user_comparator_wrapper.h"

// Generate the regular and coroutine versions of some methods by
// including version_set_sync_and_async.h twice
// Macros in the header will expand differently based on whether
// WITH_COROUTINES or WITHOUT_COROUTINES is defined
// clang-format off
#define WITHOUT_COROUTINES
#include "db/version_set_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "db/version_set_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

namespace {

// Find File in LevelFilesBrief data structure
// Within an index range defined by left and right
int FindFileInRange(const InternalKeyComparator& icmp,
                    const LevelFilesBrief& file_level, const Slice& key,
                    uint32_t left, uint32_t right) {
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return icmp.InternalKeyComparator::Compare(f.largest_key, k) < 0;
  };
  const auto& b = file_level.files;
  return static_cast<int>(std::lower_bound(b + left, b + right, key, cmp) - b);
}

Status OverlapWithIterator(const Comparator* ucmp,
                           const Slice& smallest_user_key,
                           const Slice& largest_user_key,
                           InternalIterator* iter, bool* overlap) {
  InternalKey range_start(smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
  iter->Seek(range_start.Encode());
  if (!iter->status().ok()) {
    return iter->status();
  }

  *overlap = false;
  if (iter->Valid()) {
    ParsedInternalKey seek_result;
    Status s = ParseInternalKey(iter->key(), &seek_result,
                                false /* log_err_key */);  // TODO
    if (!s.ok()) return s;

    if (ucmp->CompareWithoutTimestamp(seek_result.user_key, largest_user_key) <=
        0) {
      *overlap = true;
    }
  }

  return iter->status();
}

// Class to help choose the next file to search for the particular key.
// Searches and returns files level by level.
// We can search level-by-level since entries never hop across
// levels. Therefore we are guaranteed that if we find data
// in a smaller level, later levels are irrelevant (unless we
// are MergeInProgress).
class FilePicker {
 public:
  FilePicker(const Slice& user_key, const Slice& ikey,
             autovector<LevelFilesBrief>* file_levels, unsigned int num_levels,
             FileIndexer* file_indexer, const Comparator* user_comparator,
             const InternalKeyComparator* internal_comparator)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        returned_file_level_(static_cast<unsigned int>(-1)),
        hit_file_level_(static_cast<unsigned int>(-1)),
        search_left_bound_(0),
        search_right_bound_(FileIndexer::kLevelMaxIndex),
        level_files_brief_(file_levels),
        is_hit_file_last_in_level_(false),
        curr_file_level_(nullptr),
        user_key_(user_key),
        ikey_(ikey),
        file_indexer_(file_indexer),
        user_comparator_(user_comparator),
        internal_comparator_(internal_comparator) {
    // Setup member variables to search first level.
    search_ended_ = !PrepareNextLevel();
    if (!search_ended_) {
      // Prefetch Level 0 table data to avoid cache miss if possible.
      for (unsigned int i = 0; i < (*level_files_brief_)[0].num_files; ++i) {
        auto* r = (*level_files_brief_)[0].files[i].fd.table_reader;
        if (r) {
          r->Prepare(ikey);
        }
      }
    }
  }

  int GetCurrentLevel() const { return curr_level_; }

  FdWithKeyRange* GetNextFile() {
    while (!search_ended_) {  // Loops over different levels.
      while (curr_index_in_curr_level_ < curr_file_level_->num_files) {
        // Loops over all files in current level.
        FdWithKeyRange* f = &curr_file_level_->files[curr_index_in_curr_level_];
        hit_file_level_ = curr_level_;
        is_hit_file_last_in_level_ =
            curr_index_in_curr_level_ == curr_file_level_->num_files - 1;
        int cmp_largest = -1;

        // Do key range filtering of files or/and fractional cascading if:
        // (1) not all the files are in level 0, or
        // (2) there are more than 3 current level files
        // If there are only 3 or less current level files in the system, we
        // skip the key range filtering. In this case, more likely, the system
        // is highly tuned to minimize number of tables queried by each query,
        // so it is unlikely that key range filtering is more efficient than
        // querying the files.
        if (num_levels_ > 1 || curr_file_level_->num_files > 3) {
          // Check if key is within a file's range. If search left bound and
          // right bound point to the same find, we are sure key falls in
          // range.
          assert(curr_level_ == 0 ||
                 curr_index_in_curr_level_ == start_index_in_curr_level_ ||
                 user_comparator_->CompareWithoutTimestamp(
                     user_key_, ExtractUserKey(f->smallest_key)) <= 0);

          int cmp_smallest = user_comparator_->CompareWithoutTimestamp(
              user_key_, ExtractUserKey(f->smallest_key));
          if (cmp_smallest >= 0) {
            cmp_largest = user_comparator_->CompareWithoutTimestamp(
                user_key_, ExtractUserKey(f->largest_key));
          }

          // Setup file search bound for the next level based on the
          // comparison results
          if (curr_level_ > 0) {
            file_indexer_->GetNextLevelIndex(
                curr_level_, curr_index_in_curr_level_, cmp_smallest,
                cmp_largest, &search_left_bound_, &search_right_bound_);
          }
          // Key falls out of current file's range
          if (cmp_smallest < 0 || cmp_largest > 0) {
            if (curr_level_ == 0) {
              ++curr_index_in_curr_level_;
              continue;
            } else {
              // Search next level.
              break;
            }
          }
        }

        returned_file_level_ = curr_level_;
        if (curr_level_ > 0 && cmp_largest < 0) {
          // No more files to search in this level.
          search_ended_ = !PrepareNextLevel();
        } else {
          ++curr_index_in_curr_level_;
        }
        return f;
      }
      // Start searching next level.
      search_ended_ = !PrepareNextLevel();
    }
    // Search ended.
    return nullptr;
  }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return hit_file_level_; }

  // Returns true if the most recent "hit file" (i.e., one returned by
  // GetNextFile()) is at the last index in its level.
  bool IsHitFileLastInLevel() { return is_hit_file_last_in_level_; }

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  unsigned int returned_file_level_;
  unsigned int hit_file_level_;
  int32_t search_left_bound_;
  int32_t search_right_bound_;
  autovector<LevelFilesBrief>* level_files_brief_;
  bool search_ended_;
  bool is_hit_file_last_in_level_;
  LevelFilesBrief* curr_file_level_;
  unsigned int curr_index_in_curr_level_;
  unsigned int start_index_in_curr_level_;
  Slice user_key_;
  Slice ikey_;
  FileIndexer* file_indexer_;
  const Comparator* user_comparator_;
  const InternalKeyComparator* internal_comparator_;

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    curr_level_++;
    while (curr_level_ < num_levels_) {
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        // When current level is empty, the search bound generated from upper
        // level must be [0, -1] or [0, FileIndexer::kLevelMaxIndex] if it is
        // also empty.
        assert(search_left_bound_ == 0);
        assert(search_right_bound_ == -1 ||
               search_right_bound_ == FileIndexer::kLevelMaxIndex);
        // Since current level is empty, it will need to search all files in
        // the next level
        search_left_bound_ = 0;
        search_right_bound_ = FileIndexer::kLevelMaxIndex;
        curr_level_++;
        continue;
      }

      // Some files may overlap each other. We find
      // all files that overlap user_key and process them in order from
      // newest to oldest. In the context of merge-operator, this can occur at
      // any level. Otherwise, it only occurs at Level-0 (since Put/Deletes
      // are always compacted into a single entry).
      int32_t start_index;
      if (curr_level_ == 0) {
        // On Level-0, we read through all files to check for overlap.
        start_index = 0;
      } else {
        // On Level-n (n>=1), files are sorted. Binary search to find the
        // earliest file whose largest key >= ikey. Search left bound and
        // right bound are used to narrow the range.
        if (search_left_bound_ <= search_right_bound_) {
          if (search_right_bound_ == FileIndexer::kLevelMaxIndex) {
            search_right_bound_ =
                static_cast<int32_t>(curr_file_level_->num_files) - 1;
          }
          // `search_right_bound_` is an inclusive upper-bound, but since it was
          // determined based on user key, it is still possible the lookup key
          // falls to the right of `search_right_bound_`'s corresponding file.
          // So, pass a limit one higher, which allows us to detect this case.
          start_index =
              FindFileInRange(*internal_comparator_, *curr_file_level_, ikey_,
                              static_cast<uint32_t>(search_left_bound_),
                              static_cast<uint32_t>(search_right_bound_) + 1);
          if (start_index == search_right_bound_ + 1) {
            // `ikey_` comes after `search_right_bound_`. The lookup key does
            // not exist on this level, so let's skip this level and do a full
            // binary search on the next level.
            search_left_bound_ = 0;
            search_right_bound_ = FileIndexer::kLevelMaxIndex;
            curr_level_++;
            continue;
          }
        } else {
          // search_left_bound > search_right_bound, key does not exist in
          // this level. Since no comparison is done in this level, it will
          // need to search all files in the next level.
          search_left_bound_ = 0;
          search_right_bound_ = FileIndexer::kLevelMaxIndex;
          curr_level_++;
          continue;
        }
      }
      start_index_in_curr_level_ = start_index;
      curr_index_in_curr_level_ = start_index;

      return true;
    }
    // curr_level_ = num_levels_. So, no more levels to search.
    return false;
  }
};
}  // anonymous namespace

class FilePickerMultiGet {
 private:
  struct FilePickerContext;

 public:
  FilePickerMultiGet(MultiGetRange* range,
                     autovector<LevelFilesBrief>* file_levels,
                     unsigned int num_levels, FileIndexer* file_indexer,
                     const Comparator* user_comparator,
                     const InternalKeyComparator* internal_comparator)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        returned_file_level_(static_cast<unsigned int>(-1)),
        hit_file_level_(static_cast<unsigned int>(-1)),
        range_(*range, range->begin(), range->end()),
        maybe_repeat_key_(false),
        current_level_range_(*range, range->begin(), range->end()),
        current_file_range_(*range, range->begin(), range->end()),
        batch_iter_(range->begin()),
        batch_iter_prev_(range->begin()),
        upper_key_(range->begin()),
        level_files_brief_(file_levels),
        is_hit_file_last_in_level_(false),
        curr_file_level_(nullptr),
        file_indexer_(file_indexer),
        user_comparator_(user_comparator),
        internal_comparator_(internal_comparator),
        hit_file_(nullptr) {
    for (auto iter = range_.begin(); iter != range_.end(); ++iter) {
      fp_ctx_array_[iter.index()] =
          FilePickerContext(0, FileIndexer::kLevelMaxIndex);
    }

    // Setup member variables to search first level.
    search_ended_ = !PrepareNextLevel();
    if (!search_ended_) {
      // REVISIT
      // Prefetch Level 0 table data to avoid cache miss if possible.
      // As of now, only PlainTableReader and CuckooTableReader do any
      // prefetching. This may not be necessary anymore once we implement
      // batching in those table readers
      for (unsigned int i = 0; i < (*level_files_brief_)[0].num_files; ++i) {
        auto* r = (*level_files_brief_)[0].files[i].fd.table_reader;
        if (r) {
          for (auto iter = range_.begin(); iter != range_.end(); ++iter) {
            r->Prepare(iter->ikey);
          }
        }
      }
    }
  }

  FilePickerMultiGet(MultiGetRange* range, const FilePickerMultiGet& other)
      : num_levels_(other.num_levels_),
        curr_level_(other.curr_level_),
        returned_file_level_(other.returned_file_level_),
        hit_file_level_(other.hit_file_level_),
        fp_ctx_array_(other.fp_ctx_array_),
        range_(*range, range->begin(), range->end()),
        maybe_repeat_key_(false),
        current_level_range_(*range, range->begin(), range->end()),
        current_file_range_(*range, range->begin(), range->end()),
        batch_iter_(range->begin()),
        batch_iter_prev_(range->begin()),
        upper_key_(range->begin()),
        level_files_brief_(other.level_files_brief_),
        is_hit_file_last_in_level_(false),
        curr_file_level_(other.curr_file_level_),
        file_indexer_(other.file_indexer_),
        user_comparator_(other.user_comparator_),
        internal_comparator_(other.internal_comparator_),
        hit_file_(nullptr) {
    PrepareNextLevelForSearch();
  }

  int GetCurrentLevel() const { return curr_level_; }

  void PrepareNextLevelForSearch() { search_ended_ = !PrepareNextLevel(); }

  FdWithKeyRange* GetNextFileInLevel() {
    if (batch_iter_ == current_level_range_.end() || search_ended_) {
      hit_file_ = nullptr;
      return nullptr;
    } else {
      if (maybe_repeat_key_) {
        maybe_repeat_key_ = false;
        // Check if we found the final value for the last key in the
        // previous lookup range. If we did, then there's no need to look
        // any further for that key, so advance batch_iter_. Else, keep
        // batch_iter_ positioned on that key so we look it up again in
        // the next file
        // For L0, always advance the key because we will look in the next
        // file regardless for all keys not found yet
        if (current_level_range_.CheckKeyDone(batch_iter_) ||
            curr_level_ == 0) {
          batch_iter_ = upper_key_;
        }
      }
      // batch_iter_prev_ will become the start key for the next file
      // lookup
      batch_iter_prev_ = batch_iter_;
    }

    MultiGetRange next_file_range(current_level_range_, batch_iter_prev_,
                                  current_level_range_.end());
    size_t curr_file_index =
        (batch_iter_ != current_level_range_.end())
            ? fp_ctx_array_[batch_iter_.index()].curr_index_in_curr_level
            : curr_file_level_->num_files;
    FdWithKeyRange* f;
    bool is_last_key_in_file;
    if (!GetNextFileInLevelWithKeys(&next_file_range, &curr_file_index, &f,
                                    &is_last_key_in_file)) {
      hit_file_ = nullptr;
      return nullptr;
    } else {
      if (is_last_key_in_file) {
        // Since cmp_largest is 0, batch_iter_ still points to the last key
        // that falls in this file, instead of the next one. Increment
        // the file index for all keys between batch_iter_ and upper_key_
        auto tmp_iter = batch_iter_;
        while (tmp_iter != upper_key_) {
          ++(fp_ctx_array_[tmp_iter.index()].curr_index_in_curr_level);
          ++tmp_iter;
        }
        maybe_repeat_key_ = true;
      }
      // Set the range for this file
      current_file_range_ =
          MultiGetRange(next_file_range, batch_iter_prev_, upper_key_);
      returned_file_level_ = curr_level_;
      hit_file_level_ = curr_level_;
      is_hit_file_last_in_level_ =
          curr_file_index == curr_file_level_->num_files - 1;
      hit_file_ = f;
      return f;
    }
  }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return hit_file_level_; }

  FdWithKeyRange* GetHitFile() { return hit_file_; }

  // Returns true if the most recent "hit file" (i.e., one returned by
  // GetNextFile()) is at the last index in its level.
  bool IsHitFileLastInLevel() { return is_hit_file_last_in_level_; }

  bool KeyMaySpanNextFile() { return maybe_repeat_key_; }

  bool IsSearchEnded() { return search_ended_; }

  const MultiGetRange& CurrentFileRange() { return current_file_range_; }

  bool RemainingOverlapInLevel() {
    return !current_level_range_.Suffix(current_file_range_).empty();
  }

  MultiGetRange& GetRange() { return range_; }

  void ReplaceRange(const MultiGetRange& other) {
    assert(hit_file_ == nullptr);
    range_ = other;
    current_level_range_ = other;
  }

  FilePickerMultiGet(FilePickerMultiGet&& other)
      : num_levels_(other.num_levels_),
        curr_level_(other.curr_level_),
        returned_file_level_(other.returned_file_level_),
        hit_file_level_(other.hit_file_level_),
        fp_ctx_array_(std::move(other.fp_ctx_array_)),
        range_(std::move(other.range_)),
        maybe_repeat_key_(other.maybe_repeat_key_),
        current_level_range_(std::move(other.current_level_range_)),
        current_file_range_(std::move(other.current_file_range_)),
        batch_iter_(other.batch_iter_, &current_level_range_),
        batch_iter_prev_(other.batch_iter_prev_, &current_level_range_),
        upper_key_(other.upper_key_, &current_level_range_),
        level_files_brief_(other.level_files_brief_),
        search_ended_(other.search_ended_),
        is_hit_file_last_in_level_(other.is_hit_file_last_in_level_),
        curr_file_level_(other.curr_file_level_),
        file_indexer_(other.file_indexer_),
        user_comparator_(other.user_comparator_),
        internal_comparator_(other.internal_comparator_),
        hit_file_(other.hit_file_) {}

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  unsigned int returned_file_level_;
  unsigned int hit_file_level_;

  struct FilePickerContext {
    int32_t search_left_bound;
    int32_t search_right_bound;
    unsigned int curr_index_in_curr_level;
    unsigned int start_index_in_curr_level;

    FilePickerContext(int32_t left, int32_t right)
        : search_left_bound(left),
          search_right_bound(right),
          curr_index_in_curr_level(0),
          start_index_in_curr_level(0) {}

    FilePickerContext() = default;
  };
  std::array<FilePickerContext, MultiGetContext::MAX_BATCH_SIZE> fp_ctx_array_;
  MultiGetRange range_;
  bool maybe_repeat_key_;
  MultiGetRange current_level_range_;
  MultiGetRange current_file_range_;
  // Iterator to iterate through the keys in a MultiGet batch, that gets reset
  // at the beginning of each level. Each call to GetNextFile() will position
  // batch_iter_ at or right after the last key that was found in the returned
  // SST file
  MultiGetRange::Iterator batch_iter_;
  // An iterator that records the previous position of batch_iter_, i.e last
  // key found in the previous SST file, in order to serve as the start of
  // the batch key range for the next SST file
  MultiGetRange::Iterator batch_iter_prev_;
  MultiGetRange::Iterator upper_key_;
  autovector<LevelFilesBrief>* level_files_brief_;
  bool search_ended_;
  bool is_hit_file_last_in_level_;
  LevelFilesBrief* curr_file_level_;
  FileIndexer* file_indexer_;
  const Comparator* user_comparator_;
  const InternalKeyComparator* internal_comparator_;
  FdWithKeyRange* hit_file_;

  // Iterates through files in the current level until it finds a file that
  // contains at least one key from the MultiGet batch
  bool GetNextFileInLevelWithKeys(MultiGetRange* next_file_range,
                                  size_t* file_index, FdWithKeyRange** fd,
                                  bool* is_last_key_in_file) {
    size_t curr_file_index = *file_index;
    FdWithKeyRange* f = nullptr;
    bool file_hit = false;
    int cmp_largest = -1;
    if (curr_file_index >= curr_file_level_->num_files) {
      // In the unlikely case the next key is a duplicate of the current key,
      // and the current key is the last in the level and the internal key
      // was not found, we need to skip lookup for the remaining keys and
      // reset the search bounds
      if (batch_iter_ != current_level_range_.end()) {
        ++batch_iter_;
        for (; batch_iter_ != current_level_range_.end(); ++batch_iter_) {
          struct FilePickerContext& fp_ctx = fp_ctx_array_[batch_iter_.index()];
          fp_ctx.search_left_bound = 0;
          fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
        }
      }
      return false;
    }
    // Loops over keys in the MultiGet batch until it finds a file with
    // atleast one of the keys. Then it keeps moving forward until the
    // last key in the batch that falls in that file
    while (batch_iter_ != current_level_range_.end() &&
           (fp_ctx_array_[batch_iter_.index()].curr_index_in_curr_level ==
                curr_file_index ||
            !file_hit)) {
      struct FilePickerContext& fp_ctx = fp_ctx_array_[batch_iter_.index()];
      f = &curr_file_level_->files[fp_ctx.curr_index_in_curr_level];
      Slice& user_key = batch_iter_->ukey_without_ts;

      // Do key range filtering of files or/and fractional cascading if:
      // (1) not all the files are in level 0, or
      // (2) there are more than 3 current level files
      // If there are only 3 or less current level files in the system, we
      // skip the key range filtering. In this case, more likely, the system
      // is highly tuned to minimize number of tables queried by each query,
      // so it is unlikely that key range filtering is more efficient than
      // querying the files.
      if (num_levels_ > 1 || curr_file_level_->num_files > 3) {
        // Check if key is within a file's range. If search left bound and
        // right bound point to the same find, we are sure key falls in
        // range.
        int cmp_smallest = user_comparator_->CompareWithoutTimestamp(
            user_key, false, ExtractUserKey(f->smallest_key), true);

        assert(curr_level_ == 0 ||
               fp_ctx.curr_index_in_curr_level ==
                   fp_ctx.start_index_in_curr_level ||
               cmp_smallest <= 0);

        if (cmp_smallest >= 0) {
          cmp_largest = user_comparator_->CompareWithoutTimestamp(
              user_key, false, ExtractUserKey(f->largest_key), true);
        } else {
          cmp_largest = -1;
        }

        // Setup file search bound for the next level based on the
        // comparison results
        if (curr_level_ > 0) {
          file_indexer_->GetNextLevelIndex(
              curr_level_, fp_ctx.curr_index_in_curr_level, cmp_smallest,
              cmp_largest, &fp_ctx.search_left_bound,
              &fp_ctx.search_right_bound);
        }
        // Key falls out of current file's range
        if (cmp_smallest < 0 || cmp_largest > 0) {
          next_file_range->SkipKey(batch_iter_);
        } else {
          file_hit = true;
        }
      } else {
        file_hit = true;
      }
      if (cmp_largest == 0) {
        // cmp_largest is 0, which means the next key will not be in this
        // file, so stop looking further. However, its possible there are
        // duplicates in the batch, so find the upper bound for the batch
        // in this file (upper_key_) by skipping past the duplicates. We
        // leave batch_iter_ as is since we may have to pick up from there
        // for the next file, if this file has a merge value rather than
        // final value
        upper_key_ = batch_iter_;
        ++upper_key_;
        while (upper_key_ != current_level_range_.end() &&
               user_comparator_->CompareWithoutTimestamp(
                   batch_iter_->ukey_without_ts, false,
                   upper_key_->ukey_without_ts, false) == 0) {
          ++upper_key_;
        }
        break;
      } else {
        if (curr_level_ == 0) {
          // We need to look through all files in level 0
          ++fp_ctx.curr_index_in_curr_level;
        }
        ++batch_iter_;
      }
      if (!file_hit) {
        curr_file_index =
            (batch_iter_ != current_level_range_.end())
                ? fp_ctx_array_[batch_iter_.index()].curr_index_in_curr_level
                : curr_file_level_->num_files;
      }
    }

    *fd = f;
    *file_index = curr_file_index;
    *is_last_key_in_file = cmp_largest == 0;
    if (!*is_last_key_in_file) {
      // If the largest key in the batch overlapping the file is not the
      // largest key in the file, upper_ley_ would not have been updated so
      // update it here
      upper_key_ = batch_iter_;
    }
    return file_hit;
  }

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    if (curr_level_ == 0) {
      MultiGetRange::Iterator mget_iter = current_level_range_.begin();
      if (fp_ctx_array_[mget_iter.index()].curr_index_in_curr_level <
          curr_file_level_->num_files) {
        batch_iter_prev_ = current_level_range_.begin();
        upper_key_ = batch_iter_ = current_level_range_.begin();
        return true;
      }
    }

    curr_level_++;
    // Reset key range to saved value
    while (curr_level_ < num_levels_) {
      bool level_contains_keys = false;
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        // When current level is empty, the search bound generated from upper
        // level must be [0, -1] or [0, FileIndexer::kLevelMaxIndex] if it is
        // also empty.

        for (auto mget_iter = current_level_range_.begin();
             mget_iter != current_level_range_.end(); ++mget_iter) {
          struct FilePickerContext& fp_ctx = fp_ctx_array_[mget_iter.index()];

          assert(fp_ctx.search_left_bound == 0);
          assert(fp_ctx.search_right_bound == -1 ||
                 fp_ctx.search_right_bound == FileIndexer::kLevelMaxIndex);
          // Since current level is empty, it will need to search all files in
          // the next level
          fp_ctx.search_left_bound = 0;
          fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
        }
        // Skip all subsequent empty levels
        do {
          ++curr_level_;
        } while ((curr_level_ < num_levels_) &&
                 (*level_files_brief_)[curr_level_].num_files == 0);
        continue;
      }

      // Some files may overlap each other. We find
      // all files that overlap user_key and process them in order from
      // newest to oldest. In the context of merge-operator, this can occur at
      // any level. Otherwise, it only occurs at Level-0 (since Put/Deletes
      // are always compacted into a single entry).
      int32_t start_index = -1;
      current_level_range_ =
          MultiGetRange(range_, range_.begin(), range_.end());
      for (auto mget_iter = current_level_range_.begin();
           mget_iter != current_level_range_.end(); ++mget_iter) {
        struct FilePickerContext& fp_ctx = fp_ctx_array_[mget_iter.index()];
        if (curr_level_ == 0) {
          // On Level-0, we read through all files to check for overlap.
          start_index = 0;
          level_contains_keys = true;
        } else {
          // On Level-n (n>=1), files are sorted. Binary search to find the
          // earliest file whose largest key >= ikey. Search left bound and
          // right bound are used to narrow the range.
          if (fp_ctx.search_left_bound <= fp_ctx.search_right_bound) {
            if (fp_ctx.search_right_bound == FileIndexer::kLevelMaxIndex) {
              fp_ctx.search_right_bound =
                  static_cast<int32_t>(curr_file_level_->num_files) - 1;
            }
            // `search_right_bound_` is an inclusive upper-bound, but since it
            // was determined based on user key, it is still possible the lookup
            // key falls to the right of `search_right_bound_`'s corresponding
            // file. So, pass a limit one higher, which allows us to detect this
            // case.
            Slice& ikey = mget_iter->ikey;
            start_index = FindFileInRange(
                *internal_comparator_, *curr_file_level_, ikey,
                static_cast<uint32_t>(fp_ctx.search_left_bound),
                static_cast<uint32_t>(fp_ctx.search_right_bound) + 1);
            if (start_index == fp_ctx.search_right_bound + 1) {
              // `ikey_` comes after `search_right_bound_`. The lookup key does
              // not exist on this level, so let's skip this level and do a full
              // binary search on the next level.
              fp_ctx.search_left_bound = 0;
              fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
              current_level_range_.SkipKey(mget_iter);
              continue;
            } else {
              level_contains_keys = true;
            }
          } else {
            // search_left_bound > search_right_bound, key does not exist in
            // this level. Since no comparison is done in this level, it will
            // need to search all files in the next level.
            fp_ctx.search_left_bound = 0;
            fp_ctx.search_right_bound = FileIndexer::kLevelMaxIndex;
            current_level_range_.SkipKey(mget_iter);
            continue;
          }
        }
        fp_ctx.start_index_in_curr_level = start_index;
        fp_ctx.curr_index_in_curr_level = start_index;
      }
      if (level_contains_keys) {
        batch_iter_prev_ = current_level_range_.begin();
        upper_key_ = batch_iter_ = current_level_range_.begin();
        return true;
      }
      curr_level_++;
    }
    // curr_level_ = num_levels_. So, no more levels to search.
    return false;
  }
};

VersionStorageInfo::~VersionStorageInfo() { delete[] files_; }

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (size_t i = 0; i < storage_info_.files_[level].size(); i++) {
      FileMetaData* f = storage_info_.files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        assert(cfd_ != nullptr);
        uint32_t path_id = f->fd.GetPathId();
        assert(path_id < cfd_->ioptions()->cf_paths.size());
        vset_->obsolete_files_.push_back(
            ObsoleteFileInfo(f, cfd_->ioptions()->cf_paths[path_id].path,
                             cfd_->GetFileMetadataCacheReservationManager()));
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const LevelFilesBrief& file_level, const Slice& key) {
  return FindFileInRange(icmp, file_level, key, 0,
                         static_cast<uint32_t>(file_level.num_files));
}

void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                               const std::vector<FileMetaData*>& files,
                               Arena* arena) {
  assert(file_level);
  assert(arena);

  size_t num = files.size();
  file_level->num_files = num;
  char* mem = arena->AllocateAligned(num * sizeof(FdWithKeyRange));
  file_level->files = new (mem) FdWithKeyRange[num];

  for (size_t i = 0; i < num; i++) {
    Slice smallest_key = files[i]->smallest.Encode();
    Slice largest_key = files[i]->largest.Encode();

    // Copy key slice to sequential memory
    size_t smallest_size = smallest_key.size();
    size_t largest_size = largest_key.size();
    mem = arena->AllocateAligned(smallest_size + largest_size);
    memcpy(mem, smallest_key.data(), smallest_size);
    memcpy(mem + smallest_size, largest_key.data(), largest_size);

    FdWithKeyRange& f = file_level->files[i];
    f.fd = files[i]->fd;
    f.file_metadata = files[i];
    f.smallest_key = Slice(mem, smallest_size);
    f.largest_key = Slice(mem + smallest_size, largest_size);
  }
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FdWithKeyRange* f) {
  // nullptr user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->CompareWithoutTimestamp(*user_key,
                                        ExtractUserKey(f->largest_key)) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FdWithKeyRange* f) {
  // nullptr user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->CompareWithoutTimestamp(*user_key,
                                        ExtractUserKey(f->smallest_key)) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const LevelFilesBrief& file_level,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  // ==================== 参数说明 ====================
  // @param icmp                 内部键比较器，包含 user_comparator 用于比较用户键
  // @param disjoint_sorted_files 文件是否不重叠且有序
  //                              - false: L0 层，文件可能重叠，需要线性扫描
  //                              - true:  L1+ 层，文件不重叠且有序，可用二分查找
  // @param file_level           文件元数据简表，包含该层所有文件的键范围信息
  // @param smallest_user_key     要检查的起始键（可以为 nullptr，表示没有下界）
  // @param largest_user_key      要检查的结束键（可以为 nullptr，表示没有上界）
  //
  // ==================== 函数功能 ====================
  // 判断指定层中是否存在文件与给定的用户键范围 [smallest_user_key, largest_user_key] 重叠
  //
  // ==================== 返回值 ====================
  // @return true  - 存在文件与范围重叠
  // @return false - 不存在文件与范围重叠
  //
  // ==================== 使用场景 ====================
  // 1. VersionStorageInfo::OverlapInLevel: 判断某层是否有重叠
  // 2. CompactionPicker::SetupOtherInputs: 查找输出层中与输入范围重叠的文件
  // 3. VersionStorageInfo::RangeMightExistAfterSortedRun: 检查更深层的重叠
  //
  // ==================== 两种算法的选择 ====================
  // 根据文件组织特性选择不同算法：
  //
  // 【算法1：L0 层线性扫描（disjoint_sorted_files = false）】
  // 适用场景：L0 层
  //   - L0 文件来自 memtable flush，文件之间可能有 key 重叠
  //   - 无法使用二分查找，因为文件范围不是严格有序的
  //   - 必须检查所有文件
  //
  // 时间复杂度：O(n)，n = file_level.num_files
  //
  // 【算法2：L1+ 层二分查找（disjoint_sorted_files = true）】
  // 适用场景：L1, L2, ..., L6 层
  //   - L1+ 文件通过 compaction 生成，文件之间严格不重叠
  //   - 文件按 smallest key 排序，可以用二分查找快速定位
  //
  // 时间复杂度：O(log n)，n = file_level.num_files
  //
  // ==================== 算法1：L0 层线性扫描 ====================
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // L0 层：文件可能重叠，需要线性扫描检查所有文件
    //
    // 检查逻辑：
    // 对于每个文件 f = [smallest_f, largest_f] 和范围 [smallest_user_key, largest_user_key]：
    //   如果 largest_user_key < smallest_f  或  smallest_user_key > largest_f
    //   则文件 f 不与范围重叠
    //
    // 数学表达：
    //   不重叠条件: (largest_user_key < smallest_f) || (smallest_user_key > largest_f)
    //   重叠条件:   !不重叠条件
    //
    // 示例：
    //   文件: [a, e]
    //   范围: [c, g]  → 重叠（c 在 [a, e] 内）
    //   范围: [f, g]  → 重叠（e 在 [f, g] 内）
    //   范围: [g, h]  → 不重叠（范围在文件右侧）
    //   范围: [b, c]  → 重叠（范围在文件内）
    //   范围: [x, y]  → 不重叠（范围在文件左侧）
    for (size_t i = 0; i < file_level.num_files; i++) {
      const FdWithKeyRange* f = &(file_level.files[i]);
      // AfterFile: 判断范围是否在文件右侧（largest_user_key < smallest_f）
      // BeforeFile: 判断范围是否在文件左侧（smallest_user_key > largest_f）
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // 文件 f 与给定范围不重叠，继续检查下一个文件
        // No overlap
      } else {
        // 文件 f 与给定范围重叠，立即返回 true
        // 由于只需要判断是否存在重叠，找到第一个就足够了
        return true;  // Overlap
      }
    }
    // 所有文件都检查完毕，没有发现重叠
    return false;
  }

  // ==================== 算法2：L1+ 层二分查找 ====================
  // L1+ 层：文件不重叠且有序，使用二分查找快速定位
  //
  // 二分查找策略：
  // 1. 找到 smallest_user_key 可能存在的最左位置
  // 2. 检查该位置的文件是否与范围重叠
  //
  // 为什么只需检查一个文件？
  // 因为 L1+ 层文件不重叠：
  //   - 如果找到位置的文件不与范围重叠，则该范围不可能与任何文件重叠
  //   - 如果找到位置的文件与范围重叠，则必然存在重叠
  //
  // 示例：
  // L1 文件: [a,b], [c,d], [e,f], [g,h], [i,j]
  // 范围: [e,g]
  // 二分查找 smallest_key = e → 找到文件 [e,f]
  // 检查 largest_key = g 是否与 [e,f] 重叠 → 重叠，返回 true
  //
  // 范围: [k,l]
  // 二分查找 smallest_key = k → 找到文件 [i,j] 的右侧（index >= num_files）
  // 返回 false
  //
  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // 步骤1：找到 smallest_user_key 可能存在的最左文件位置
    //
    // 为什么需要 SetMinPossibleForUserKey？
    // 因为 smallest_user_key 只是用户键，而文件中存储的是内部键（包含 sequence number）
    // 内部键比较规则：user_key 相同时，sequence number 越小，内部键越小
    //
    // SetMinPossibleForUserKey(smallest_user_key) 会生成：
    //   InternalKey(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek)
    // 这样可以找到可能包含 smallest_user_key 的最左文件
    //
    // 示例：
    // 用户键: "apple"
    // 内部键: ("apple", 100), ("apple", 99), ("apple", 1), ("banana", 50)
    // SetMinPossibleForUserKey("apple") → ("apple", kMaxSequenceNumber)
    // FindFile 找到第一个 >= ("apple", kMaxSequenceNumber) 的文件
    // Find the leftmost possible internal key for smallest_user_key
    InternalKey small;
    small.SetMinPossibleForUserKey(*smallest_user_key);
    // 调用 FindFile 进行二分查找
    // FindFile 返回第一个 >= small 的文件索引
    index = FindFile(icmp, file_level, small.Encode());
  }

  // 步骤2：检查查找结果是否超出文件列表
  // 如果 index >= num_files，说明 smallest_user_key 比所有文件的 largest_key 都大
  // 因此范围在所有文件右侧，不可能有重叠
  if (index >= file_level.num_files) {
    // 范围起始位置在所有文件之后，所以没有重叠
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // 步骤3：检查找到的文件是否与范围重叠
  // 此时 file_level.files[index] 是第一个可能包含 smallest_user_key 的文件
  //
  // 检查逻辑：
  // BeforeFile(largest_user_key, file) = (largest_user_key < file->smallest)
  // 如果 largest_user_key < file->smallest，说明范围在文件左侧，不重叠
  // 反之，范围与文件重叠（因为文件之间不重叠，这个文件是第一个可能的）
  //
  // 返回 !BeforeFile 的结果：
  //   - !BeforeFile = false → largest_user_key < file->smallest → 不重叠 → 返回 false
  //   - !BeforeFile = true  → largest_user_key >= file->smallest → 重叠 → 返回 true
  //
  // 示例：
  // L1 文件: [a,b], [c,d], [e,f], [g,h], [i,j]
  // 范围: [e,g]
  // index = 2 (文件 [e,f])
  // BeforeFile(g, [e,f]) = (g < e) = false
  // !BeforeFile = true → 返回 true（有重叠）
  //
  // 范围: [b,b]
  // index = 0 (文件 [a,b])
  // BeforeFile(b, [a,b]) = (b < a) = false
  // !BeforeFile = true → 返回 true（有重叠）
  //
  // 范围: [a,a]
  // index = 0 (文件 [a,b])
  // BeforeFile(a, [a,b]) = (a < a) = false
  // !BeforeFile = true → 返回 true（有重叠）
  //
  // 范围: [Z,Z]
  // index = 0 (文件 [a,b])
  // BeforeFile(Z, [a,b]) = true
  // !BeforeFile = false → 返回 false（无重叠）
  return !BeforeFile(ucmp, largest_user_key, &file_level.files[index]);
}

namespace {

class LevelIterator final : public InternalIterator {
 public:
  // @param read_options Must outlive this iterator.
  LevelIterator(
      TableCache* table_cache, const ReadOptions& read_options,
      const FileOptions& file_options, const InternalKeyComparator& icomparator,
      const LevelFilesBrief* flevel,
      const std::shared_ptr<const SliceTransform>& prefix_extractor,
      bool should_sample, HistogramImpl* file_read_hist,
      TableReaderCaller caller, bool skip_filters, int level,
      uint8_t block_protection_bytes_per_key, RangeDelAggregator* range_del_agg,
      const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries =
          nullptr,
      bool allow_unprepared_value = false,
      TruncatedRangeDelIterator**** range_tombstone_iter_ptr_ = nullptr)
      : table_cache_(table_cache),
        read_options_(read_options),
        file_options_(file_options),
        icomparator_(icomparator),
        user_comparator_(icomparator.user_comparator()),
        flevel_(flevel),
        prefix_extractor_(prefix_extractor),
        file_read_hist_(file_read_hist),
        should_sample_(should_sample),
        caller_(caller),
        skip_filters_(skip_filters),
        allow_unprepared_value_(allow_unprepared_value),
        file_index_(flevel_->num_files),
        level_(level),
        range_del_agg_(range_del_agg),
        pinned_iters_mgr_(nullptr),
        compaction_boundaries_(compaction_boundaries),
        is_next_read_sequential_(false),
        block_protection_bytes_per_key_(block_protection_bytes_per_key),
        range_tombstone_iter_(nullptr),
        to_return_sentinel_(false) {
    // Empty level is not supported.
    assert(flevel_ != nullptr && flevel_->num_files > 0);
    if (range_tombstone_iter_ptr_) {
      *range_tombstone_iter_ptr_ = &range_tombstone_iter_;
    }
  }

  ~LevelIterator() override { delete file_iter_.Set(nullptr); }

  // Seek to the first file with a key >= target.
  // If range_tombstone_iter_ is not nullptr, then we pretend that file
  // boundaries are fake keys (sentinel keys). These keys are used to keep range
  // tombstones alive even when all point keys in an SST file are exhausted.
  // These sentinel keys will be skipped in merging iterator.
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() final override;
  bool NextAndGetResult(IterateResult* result) override;
  void Prev() override;

  // In addition to valid and invalid state (!file_iter.Valid() and
  // status.ok()), a third state of the iterator is when !file_iter_.Valid() and
  // to_return_sentinel_. This means we are at the end of a file, and a sentinel
  // key (the file boundary that we pretend as a key) is to be returned next.
  // file_iter_.Valid() and to_return_sentinel_ should not both be true.
  bool Valid() const override {
    assert(!(file_iter_.Valid() && to_return_sentinel_));
    return file_iter_.Valid() || to_return_sentinel_;
  }
  Slice key() const override {
    assert(Valid());
    if (to_return_sentinel_) {
      // Sentinel should be returned after file_iter_ reaches the end of the
      // file
      assert(!file_iter_.Valid());
      return sentinel_;
    }
    return file_iter_.key();
  }

  Slice value() const override {
    assert(Valid());
    assert(!to_return_sentinel_);
    return file_iter_.value();
  }

  Status status() const override {
    return file_iter_.iter() ? file_iter_.status() : Status::OK();
  }

  bool PrepareValue() override { return file_iter_.PrepareValue(); }

  inline bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return may_be_out_of_lower_bound_ && file_iter_.MayBeOutOfLowerBound();
  }

  inline IterBoundCheck UpperBoundCheckResult() override {
    if (Valid()) {
      return file_iter_.UpperBoundCheckResult();
    } else {
      return IterBoundCheck::kUnknown;
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_.iter()) {
      file_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsKeyPinned();
  }

  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsValuePinned();
  }

  bool IsDeleteRangeSentinelKey() const override { return to_return_sentinel_; }

 private:
  // Return true if at least one invalid file is seen and skipped.
  bool SkipEmptyFileForward();
  void SkipEmptyFileBackward();
  void SetFileIterator(InternalIterator* iter);
  void InitFileIterator(size_t new_file_index);

  const Slice& file_smallest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].smallest_key;
  }

  const Slice& file_largest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].largest_key;
  }

  bool KeyReachedUpperBound(const Slice& internal_key) {
    return read_options_.iterate_upper_bound != nullptr &&
           user_comparator_.CompareWithoutTimestamp(
               ExtractUserKey(internal_key), /*a_has_ts=*/true,
               *read_options_.iterate_upper_bound, /*b_has_ts=*/false) >= 0;
  }

  void ClearRangeTombstoneIter() {
    if (range_tombstone_iter_ && *range_tombstone_iter_) {
      delete *range_tombstone_iter_;
      *range_tombstone_iter_ = nullptr;
    }
  }

  // Move file_iter_ to the file at file_index_.
  // range_tombstone_iter_ is updated with a range tombstone iterator
  // into the new file. Old range tombstone iterator is cleared.
  InternalIterator* NewFileIterator() {
    assert(file_index_ < flevel_->num_files);
    auto file_meta = flevel_->files[file_index_];
    if (should_sample_) {
      sample_file_read_inc(file_meta.file_metadata);
    }

    const InternalKey* smallest_compaction_key = nullptr;
    const InternalKey* largest_compaction_key = nullptr;
    if (compaction_boundaries_ != nullptr) {
      smallest_compaction_key = (*compaction_boundaries_)[file_index_].smallest;
      largest_compaction_key = (*compaction_boundaries_)[file_index_].largest;
    }
    CheckMayBeOutOfLowerBound();
    ClearRangeTombstoneIter();
    return table_cache_->NewIterator(
        read_options_, file_options_, icomparator_, *file_meta.file_metadata,
        range_del_agg_, prefix_extractor_,
        nullptr /* don't need reference to table */, file_read_hist_, caller_,
        /*arena=*/nullptr, skip_filters_, level_,
        /*max_file_size_for_l0_meta_pin=*/0, smallest_compaction_key,
        largest_compaction_key, allow_unprepared_value_,
        block_protection_bytes_per_key_, range_tombstone_iter_);
  }

  // Check if current file being fully within iterate_lower_bound.
  //
  // Note MyRocks may update iterate bounds between seek. To workaround it,
  // we need to check and update may_be_out_of_lower_bound_ accordingly.
  void CheckMayBeOutOfLowerBound() {
    if (read_options_.iterate_lower_bound != nullptr &&
        file_index_ < flevel_->num_files) {
      may_be_out_of_lower_bound_ =
          user_comparator_.CompareWithoutTimestamp(
              ExtractUserKey(file_smallest_key(file_index_)), /*a_has_ts=*/true,
              *read_options_.iterate_lower_bound, /*b_has_ts=*/false) < 0;
    }
  }

  TableCache* table_cache_;
  const ReadOptions& read_options_;
  const FileOptions& file_options_;
  const InternalKeyComparator& icomparator_;
  const UserComparatorWrapper user_comparator_;
  const LevelFilesBrief* flevel_;
  mutable FileDescriptor current_value_;
  // `prefix_extractor_` may be non-null even for total order seek. Checking
  // this variable is not the right way to identify whether prefix iterator
  // is used.
  const std::shared_ptr<const SliceTransform>& prefix_extractor_;

  HistogramImpl* file_read_hist_;
  bool should_sample_;
  TableReaderCaller caller_;
  bool skip_filters_;
  bool allow_unprepared_value_;
  bool may_be_out_of_lower_bound_ = true;
  size_t file_index_;
  int level_;
  RangeDelAggregator* range_del_agg_;
  IteratorWrapper file_iter_;  // May be nullptr
  PinnedIteratorsManager* pinned_iters_mgr_;

  // To be propagated to RangeDelAggregator in order to safely truncate range
  // tombstones.
  const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries_;

  bool is_next_read_sequential_;

  uint8_t block_protection_bytes_per_key_;

  // This is set when this level iterator is used under a merging iterator
  // that processes range tombstones. range_tombstone_iter_ points to where the
  // merging iterator stores the range tombstones iterator for this level. When
  // this level iterator moves to a new SST file, it updates the range
  // tombstones accordingly through this pointer. So the merging iterator always
  // has access to the current SST file's range tombstones.
  //
  // The level iterator treats file boundary as fake keys (sentinel keys) to
  // keep range tombstones alive if needed and make upper level, i.e. merging
  // iterator, aware of file changes (when level iterator moves to a new SST
  // file, there is some bookkeeping work that needs to be done at merging
  // iterator end).
  //
  // *range_tombstone_iter_ points to range tombstones of the current SST file
  TruncatedRangeDelIterator** range_tombstone_iter_;

  // Whether next/prev key is a sentinel key.
  bool to_return_sentinel_ = false;
  // The sentinel key to be returned
  Slice sentinel_;
  // Sets flags for if we should return the sentinel key next.
  // The condition for returning sentinel is reaching the end of current
  // file_iter_: !Valid() && status.().ok().
  void TrySetDeleteRangeSentinel(const Slice& boundary_key);
  void ClearSentinel() { to_return_sentinel_ = false; }

  // Set in Seek() when a prefix seek reaches end of the current file,
  // and the next file has a different prefix. SkipEmptyFileForward()
  // will not move to next file when this flag is set.
  bool prefix_exhausted_ = false;
};

void LevelIterator::TrySetDeleteRangeSentinel(const Slice& boundary_key) {
  assert(range_tombstone_iter_);
  if (file_iter_.iter() != nullptr && !file_iter_.Valid() &&
      file_iter_.status().ok()) {
    to_return_sentinel_ = true;
    sentinel_ = boundary_key;
  }
}

void LevelIterator::Seek(const Slice& target) {
  prefix_exhausted_ = false;
  ClearSentinel();
  // Check whether the seek key fall under the same file
  bool need_to_reseek = true;
  if (file_iter_.iter() != nullptr && file_index_ < flevel_->num_files) {
    const FdWithKeyRange& cur_file = flevel_->files[file_index_];
    if (icomparator_.InternalKeyComparator::Compare(
            target, cur_file.largest_key) <= 0 &&
        icomparator_.InternalKeyComparator::Compare(
            target, cur_file.smallest_key) >= 0) {
      need_to_reseek = false;
      assert(static_cast<size_t>(FindFile(icomparator_, *flevel_, target)) ==
             file_index_);
    }
  }
  if (need_to_reseek) {
    TEST_SYNC_POINT("LevelIterator::Seek:BeforeFindFile");
    size_t new_file_index = FindFile(icomparator_, *flevel_, target);
    InitFileIterator(new_file_index);
  }

  if (file_iter_.iter() != nullptr) {
    file_iter_.Seek(target);
    // Status::TryAgain indicates asynchronous request for retrieval of data
    // blocks has been submitted. So it should return at this point and Seek
    // should be called again to retrieve the requested block and execute the
    // remaining code.
    if (file_iter_.status() == Status::TryAgain()) {
      return;
    }
    if (!file_iter_.Valid() && file_iter_.status().ok() &&
        prefix_extractor_ != nullptr && !read_options_.total_order_seek &&
        !read_options_.auto_prefix_mode &&
        file_index_ < flevel_->num_files - 1) {
      size_t ts_sz = user_comparator_.user_comparator()->timestamp_size();
      Slice target_user_key_without_ts =
          ExtractUserKeyAndStripTimestamp(target, ts_sz);
      Slice next_file_first_user_key_without_ts =
          ExtractUserKeyAndStripTimestamp(file_smallest_key(file_index_ + 1),
                                          ts_sz);
      if (prefix_extractor_->InDomain(target_user_key_without_ts) &&
          (!prefix_extractor_->InDomain(next_file_first_user_key_without_ts) ||
           user_comparator_.CompareWithoutTimestamp(
               prefix_extractor_->Transform(target_user_key_without_ts), false,
               prefix_extractor_->Transform(
                   next_file_first_user_key_without_ts),
               false) != 0)) {
        // SkipEmptyFileForward() will not advance to next file when this flag
        // is set for reason detailed below.
        //
        // The file we initially positioned to has no keys under the target
        // prefix, and the next file's smallest key has a different prefix than
        // target. When doing prefix iterator seek, when keys for one prefix
        // have been exhausted, it can jump to any key that is larger. Here we
        // are enforcing a stricter contract than that, in order to make it
        // easier for higher layers (merging and DB iterator) to reason the
        // correctness:
        // 1. Within the prefix, the result should be accurate.
        // 2. If keys for the prefix is exhausted, it is either positioned to
        // the next key after the prefix, or make the iterator invalid.
        // A side benefit will be that it invalidates the iterator earlier so
        // that the upper level merging iterator can merge fewer child
        // iterators.
        //
        // The flag is cleared in Seek*() calls. There is no need to clear the
        // flag in Prev() since Prev() will not be called when the flag is set
        // for reasons explained below. If range_tombstone_iter_ is nullptr,
        // then there is no file boundary sentinel key. Since
        // !file_iter_.Valid() from the if condition above, this level iterator
        // is !Valid(), so Prev() will not be called. If range_tombstone_iter_
        // is not nullptr, there are two cases depending on if this level
        // iterator reaches top of the heap in merging iterator (the upper
        // layer).
        //  If so, merging iterator will see the sentinel key, call
        //  NextAndGetResult() and the call to NextAndGetResult() will skip the
        //  sentinel key and makes this level iterator invalid. If not, then it
        //  could be because the upper layer is done before any method of this
        //  level iterator is called or another Seek*() call is invoked. Either
        //  way, Prev() is never called before Seek*().
        // The flag should not be cleared at the beginning of
        // Next/NextAndGetResult() since it is used in SkipEmptyFileForward()
        // called in Next/NextAndGetResult().
        prefix_exhausted_ = true;
      }
    }

    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
  }
  SkipEmptyFileForward();
  CheckMayBeOutOfLowerBound();
}

void LevelIterator::SeekForPrev(const Slice& target) {
  prefix_exhausted_ = false;
  ClearSentinel();
  size_t new_file_index = FindFile(icomparator_, *flevel_, target);
  // Seek beyond this level's smallest key
  if (new_file_index == 0 &&
      icomparator_.Compare(target, file_smallest_key(0)) < 0) {
    SetFileIterator(nullptr);
    ClearRangeTombstoneIter();
    CheckMayBeOutOfLowerBound();
    return;
  }
  if (new_file_index >= flevel_->num_files) {
    new_file_index = flevel_->num_files - 1;
  }

  InitFileIterator(new_file_index);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekForPrev(target);
    if (range_tombstone_iter_ &&
        icomparator_.Compare(target, file_smallest_key(file_index_)) >= 0) {
      // In SeekForPrev() case, it is possible that the target is less than
      // file's lower boundary since largest key is used to determine file index
      // (FindFile()). When target is less than file's lower boundary, sentinel
      // key should not be set so that SeekForPrev() does not result in a key
      // larger than target. This is correct in that there is no need to keep
      // the range tombstones in this file alive as they only cover keys
      // starting from the file's lower boundary, which is after `target`.
      TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
    }
    SkipEmptyFileBackward();
  }
  CheckMayBeOutOfLowerBound();
}

void LevelIterator::SeekToFirst() {
  prefix_exhausted_ = false;
  ClearSentinel();
  InitFileIterator(0);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekToFirst();
    if (range_tombstone_iter_) {
      // We do this in SeekToFirst() and SeekToLast() since
      // we could have an empty file with only range tombstones.
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
  }
  SkipEmptyFileForward();
  CheckMayBeOutOfLowerBound();
}

void LevelIterator::SeekToLast() {
  prefix_exhausted_ = false;
  ClearSentinel();
  InitFileIterator(flevel_->num_files - 1);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekToLast();
    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
    }
  }
  SkipEmptyFileBackward();
  CheckMayBeOutOfLowerBound();
}

void LevelIterator::Next() {
  assert(Valid());
  if (to_return_sentinel_) {
    // file_iter_ is at EOF already when to_return_sentinel_
    ClearSentinel();
  } else {
    file_iter_.Next();
    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
  }
  SkipEmptyFileForward();
}

bool LevelIterator::NextAndGetResult(IterateResult* result) {
  assert(Valid());
  // file_iter_ is at EOF already when to_return_sentinel_
  bool is_valid = !to_return_sentinel_ && file_iter_.NextAndGetResult(result);
  if (!is_valid) {
    if (to_return_sentinel_) {
      ClearSentinel();
    } else if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_largest_key(file_index_));
    }
    is_next_read_sequential_ = true;
    SkipEmptyFileForward();
    is_next_read_sequential_ = false;
    is_valid = Valid();
    if (is_valid) {
      // This could be set in TrySetDeleteRangeSentinel() or
      // SkipEmptyFileForward() above.
      if (to_return_sentinel_) {
        result->key = sentinel_;
        result->bound_check_result = IterBoundCheck::kUnknown;
        result->value_prepared = true;
      } else {
        result->key = key();
        result->bound_check_result = file_iter_.UpperBoundCheckResult();
        // Ideally, we should return the real file_iter_.value_prepared but the
        // information is not here. It would casue an extra PrepareValue()
        // for the first key of a file.
        result->value_prepared = !allow_unprepared_value_;
      }
    }
  }
  return is_valid;
}

void LevelIterator::Prev() {
  assert(Valid());
  if (to_return_sentinel_) {
    ClearSentinel();
  } else {
    file_iter_.Prev();
    if (range_tombstone_iter_) {
      TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
    }
  }
  SkipEmptyFileBackward();
}

bool LevelIterator::SkipEmptyFileForward() {
  bool seen_empty_file = false;
  // Pause at sentinel key
  while (!to_return_sentinel_ &&
         (file_iter_.iter() == nullptr ||
          (!file_iter_.Valid() && file_iter_.status().ok() &&
           file_iter_.iter()->UpperBoundCheckResult() !=
               IterBoundCheck::kOutOfBound))) {
    seen_empty_file = true;
    // Move to next file
    if (file_index_ >= flevel_->num_files - 1 ||
        KeyReachedUpperBound(file_smallest_key(file_index_ + 1)) ||
        prefix_exhausted_) {
      SetFileIterator(nullptr);
      ClearRangeTombstoneIter();
      break;
    }
    // may init a new *range_tombstone_iter
    InitFileIterator(file_index_ + 1);
    // We moved to a new SST file
    // Seek range_tombstone_iter_ to reset its !Valid() default state.
    // We do not need to call range_tombstone_iter_.Seek* in
    // LevelIterator::Seek* since when the merging iterator calls
    // LevelIterator::Seek*, it should also call Seek* into the corresponding
    // range tombstone iterator.
    if (file_iter_.iter() != nullptr) {
      file_iter_.SeekToFirst();
      if (range_tombstone_iter_) {
        if (*range_tombstone_iter_) {
          (*range_tombstone_iter_)->SeekToFirst();
        }
        TrySetDeleteRangeSentinel(file_largest_key(file_index_));
      }
    }
  }
  return seen_empty_file;
}

void LevelIterator::SkipEmptyFileBackward() {
  // Pause at sentinel key
  while (!to_return_sentinel_ &&
         (file_iter_.iter() == nullptr ||
          (!file_iter_.Valid() && file_iter_.status().ok()))) {
    // Move to previous file
    if (file_index_ == 0) {
      // Already the first file
      SetFileIterator(nullptr);
      ClearRangeTombstoneIter();
      return;
    }
    InitFileIterator(file_index_ - 1);
    // We moved to a new SST file
    // Seek range_tombstone_iter_ to reset its !Valid() default state.
    if (file_iter_.iter() != nullptr) {
      file_iter_.SeekToLast();
      if (range_tombstone_iter_) {
        if (*range_tombstone_iter_) {
          (*range_tombstone_iter_)->SeekToLast();
        }
        TrySetDeleteRangeSentinel(file_smallest_key(file_index_));
        if (to_return_sentinel_) {
          break;
        }
      }
    }
  }
}

void LevelIterator::SetFileIterator(InternalIterator* iter) {
  if (pinned_iters_mgr_ && iter) {
    iter->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  InternalIterator* old_iter = file_iter_.Set(iter);

  // Update the read pattern for PrefetchBuffer.
  if (is_next_read_sequential_) {
    file_iter_.UpdateReadaheadState(old_iter);
  }

  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(old_iter);
  } else {
    delete old_iter;
  }
}

void LevelIterator::InitFileIterator(size_t new_file_index) {
  if (new_file_index >= flevel_->num_files) {
    file_index_ = new_file_index;
    SetFileIterator(nullptr);
    ClearRangeTombstoneIter();
    return;
  } else {
    // If the file iterator shows incomplete, we try it again if users seek
    // to the same file, as this time we may go to a different data block
    // which is cached in block cache.
    //
    if (file_iter_.iter() != nullptr && !file_iter_.status().IsIncomplete() &&
        new_file_index == file_index_) {
      // file_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      file_index_ = new_file_index;
      InternalIterator* iter = NewFileIterator();
      SetFileIterator(iter);
    }
  }
}
}  // anonymous namespace

Status Version::GetTableProperties(const ReadOptions& read_options,
                                   std::shared_ptr<const TableProperties>* tp,
                                   const FileMetaData* file_meta,
                                   const std::string* fname) const {
  auto table_cache = cfd_->table_cache();
  auto ioptions = cfd_->ioptions();
  Status s = table_cache->GetTableProperties(
      file_options_, read_options, cfd_->internal_comparator(), *file_meta, tp,
      mutable_cf_options_.block_protection_bytes_per_key,
      mutable_cf_options_.prefix_extractor, true /* no io */);
  if (s.ok()) {
    return s;
  }

  // We only ignore error type `Incomplete` since it's by design that we
  // disallow table when it's not in table cache.
  if (!s.IsIncomplete()) {
    return s;
  }

  // 2. Table is not present in table cache, we'll read the table properties
  // directly from the properties block in the file.
  std::unique_ptr<FSRandomAccessFile> file;
  std::string file_name;
  if (fname != nullptr) {
    file_name = *fname;
  } else {
    file_name = TableFileName(ioptions->cf_paths, file_meta->fd.GetNumber(),
                              file_meta->fd.GetPathId());
  }
  s = ioptions->fs->NewRandomAccessFile(file_name, file_options_, &file,
                                        nullptr);
  if (!s.ok()) {
    return s;
  }

  // By setting the magic number to kNullTableMagicNumber, we can bypass
  // the magic number check in the footer.
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(
          std::move(file), file_name, ioptions->clock /* clock */, io_tracer_,
          ioptions->stats /* stats */,
          Histograms::SST_READ_MICROS /* hist_type */,
          nullptr /* file_read_hist */, nullptr /* rate_limiter */,
          ioptions->listeners));
  std::unique_ptr<TableProperties> props;
  s = ReadTableProperties(
      file_reader.get(), file_meta->fd.GetFileSize(),
      Footer::kNullTableMagicNumber /* table's magic number */, *ioptions,
      read_options, &props);
  if (!s.ok()) {
    return s;
  }
  *tp = std::move(props);
  RecordTick(ioptions->stats, NUMBER_DIRECT_LOAD_TABLE_PROPERTIES);
  return s;
}

Status Version::GetPropertiesOfAllTables(const ReadOptions& read_options,
                                         TablePropertiesCollection* props) {
  Status s;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    s = GetPropertiesOfAllTables(read_options, props, level);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status Version::TablesRangeTombstoneSummary(int max_entries_to_print,
                                            std::string* out_str) {
  if (max_entries_to_print <= 0) {
    return Status::OK();
  }
  int num_entries_left = max_entries_to_print;

  std::stringstream ss;

  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (const auto& file_meta : storage_info_.files_[level]) {
      auto fname =
          TableFileName(cfd_->ioptions()->cf_paths, file_meta->fd.GetNumber(),
                        file_meta->fd.GetPathId());

      ss << "=== file : " << fname << " ===\n";

      TableCache* table_cache = cfd_->table_cache();
      std::unique_ptr<FragmentedRangeTombstoneIterator> tombstone_iter;

      Status s = table_cache->GetRangeTombstoneIterator(
          read_options, cfd_->internal_comparator(), *file_meta,
          cfd_->GetLatestMutableCFOptions()->block_protection_bytes_per_key,
          &tombstone_iter);
      if (!s.ok()) {
        return s;
      }
      if (tombstone_iter) {
        tombstone_iter->SeekToFirst();

        // TODO: print timestamp
        while (tombstone_iter->Valid() && num_entries_left > 0) {
          ss << "start: " << tombstone_iter->start_key().ToString(true)
             << " end: " << tombstone_iter->end_key().ToString(true)
             << " seq: " << tombstone_iter->seq() << '\n';
          tombstone_iter->Next();
          num_entries_left--;
        }
        if (num_entries_left <= 0) {
          break;
        }
      }
    }
    if (num_entries_left <= 0) {
      break;
    }
  }
  assert(num_entries_left >= 0);
  if (num_entries_left <= 0) {
    ss << "(results may not be complete)\n";
  }

  *out_str = ss.str();
  return Status::OK();
}

Status Version::GetPropertiesOfAllTables(const ReadOptions& read_options,
                                         TablePropertiesCollection* props,
                                         int level) {
  for (const auto& file_meta : storage_info_.files_[level]) {
    auto fname =
        TableFileName(cfd_->ioptions()->cf_paths, file_meta->fd.GetNumber(),
                      file_meta->fd.GetPathId());
    // 1. If the table is already present in table cache, load table
    // properties from there.
    std::shared_ptr<const TableProperties> table_properties;
    Status s =
        GetTableProperties(read_options, &table_properties, file_meta, &fname);
    if (s.ok()) {
      props->insert({fname, table_properties});
    } else {
      return s;
    }
  }

  return Status::OK();
}

Status Version::GetPropertiesOfTablesInRange(
    const ReadOptions& read_options, const Range* range, std::size_t n,
    TablePropertiesCollection* props) const {
  for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
    for (decltype(n) i = 0; i < n; i++) {
      // Convert user_key into a corresponding internal key.
      InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
      InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
      std::vector<FileMetaData*> files;
      storage_info_.GetOverlappingInputs(level, &k1, &k2, &files, -1, nullptr,
                                         false);
      for (const auto& file_meta : files) {
        auto fname =
            TableFileName(cfd_->ioptions()->cf_paths, file_meta->fd.GetNumber(),
                          file_meta->fd.GetPathId());
        if (props->count(fname) == 0) {
          // 1. If the table is already present in table cache, load table
          // properties from there.
          std::shared_ptr<const TableProperties> table_properties;
          Status s = GetTableProperties(read_options, &table_properties,
                                        file_meta, &fname);
          if (s.ok()) {
            props->insert({fname, table_properties});
          } else {
            return s;
          }
        }
      }
    }
  }

  return Status::OK();
}

Status Version::GetAggregatedTableProperties(
    const ReadOptions& read_options, std::shared_ptr<const TableProperties>* tp,
    int level) {
  TablePropertiesCollection props;
  Status s;
  if (level < 0) {
    s = GetPropertiesOfAllTables(read_options, &props);
  } else {
    s = GetPropertiesOfAllTables(read_options, &props, level);
  }
  if (!s.ok()) {
    return s;
  }

  auto* new_tp = new TableProperties();
  for (const auto& item : props) {
    new_tp->Add(*item.second);
  }
  tp->reset(new_tp);
  return Status::OK();
}

size_t Version::GetMemoryUsageByTableReaders(const ReadOptions& read_options) {
  size_t total_usage = 0;
  for (auto& file_level : storage_info_.level_files_brief_) {
    for (size_t i = 0; i < file_level.num_files; i++) {
      total_usage += cfd_->table_cache()->GetMemoryUsageByTableReader(
          file_options_, read_options, cfd_->internal_comparator(),
          *file_level.files[i].file_metadata,
          mutable_cf_options_.block_protection_bytes_per_key,
          mutable_cf_options_.prefix_extractor);
    }
  }
  return total_usage;
}

void Version::GetColumnFamilyMetaData(ColumnFamilyMetaData* cf_meta) {
  assert(cf_meta);
  assert(cfd_);

  cf_meta->name = cfd_->GetName();
  cf_meta->size = 0;
  cf_meta->file_count = 0;
  cf_meta->levels.clear();

  cf_meta->blob_file_size = 0;
  cf_meta->blob_file_count = 0;
  cf_meta->blob_files.clear();

  auto* ioptions = cfd_->ioptions();
  auto* vstorage = storage_info();

  for (int level = 0; level < cfd_->NumberLevels(); level++) {
    uint64_t level_size = 0;
    cf_meta->file_count += vstorage->LevelFiles(level).size();
    std::vector<SstFileMetaData> files;
    for (const auto& file : vstorage->LevelFiles(level)) {
      uint32_t path_id = file->fd.GetPathId();
      std::string file_path;
      if (path_id < ioptions->cf_paths.size()) {
        file_path = ioptions->cf_paths[path_id].path;
      } else {
        assert(!ioptions->cf_paths.empty());
        file_path = ioptions->cf_paths.back().path;
      }
      const uint64_t file_number = file->fd.GetNumber();
      files.emplace_back(
          MakeTableFileName("", file_number), file_number, file_path,
          file->fd.GetFileSize(), file->fd.smallest_seqno,
          file->fd.largest_seqno, file->smallest.user_key().ToString(),
          file->largest.user_key().ToString(),
          file->stats.num_reads_sampled.load(std::memory_order_relaxed),
          file->being_compacted, file->temperature,
          file->oldest_blob_file_number, file->TryGetOldestAncesterTime(),
          file->TryGetFileCreationTime(), file->epoch_number,
          file->file_checksum, file->file_checksum_func_name);
      files.back().num_entries = file->num_entries;
      files.back().num_deletions = file->num_deletions;
      files.back().smallest = file->smallest.Encode().ToString();
      files.back().largest = file->largest.Encode().ToString();
      level_size += file->fd.GetFileSize();
    }
    cf_meta->levels.emplace_back(level, level_size, std::move(files));
    cf_meta->size += level_size;
  }
  for (const auto& meta : vstorage->GetBlobFiles()) {
    assert(meta);

    cf_meta->blob_files.emplace_back(
        meta->GetBlobFileNumber(), BlobFileName("", meta->GetBlobFileNumber()),
        ioptions->cf_paths.front().path, meta->GetBlobFileSize(),
        meta->GetTotalBlobCount(), meta->GetTotalBlobBytes(),
        meta->GetGarbageBlobCount(), meta->GetGarbageBlobBytes(),
        meta->GetChecksumMethod(), meta->GetChecksumValue());
    ++cf_meta->blob_file_count;
    cf_meta->blob_file_size += meta->GetBlobFileSize();
  }
}

uint64_t Version::GetSstFilesSize() {
  uint64_t sst_files_size = 0;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (const auto& file_meta : storage_info_.LevelFiles(level)) {
      sst_files_size += file_meta->fd.GetFileSize();
    }
  }
  return sst_files_size;
}

void Version::GetSstFilesBoundaryKeys(Slice* smallest_user_key,
                                      Slice* largest_user_key) {
  smallest_user_key->clear();
  largest_user_key->clear();
  bool initialized = false;
  const Comparator* ucmp = storage_info_.user_comparator_;
  for (int level = 0; level < cfd_->NumberLevels(); level++) {
    if (storage_info_.LevelFiles(level).size() == 0) {
      continue;
    }
    if (level == 0) {
      // we need to consider all files on level 0
      for (const auto& file : storage_info_.LevelFiles(level)) {
        const Slice& start_user_key = file->smallest.user_key();
        if (!initialized ||
            ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
          *smallest_user_key = start_user_key;
        }
        const Slice& end_user_key = file->largest.user_key();
        if (!initialized ||
            ucmp->Compare(end_user_key, *largest_user_key) > 0) {
          *largest_user_key = end_user_key;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      const Slice& start_user_key =
          storage_info_.LevelFiles(level)[0]->smallest.user_key();
      if (!initialized ||
          ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
        *smallest_user_key = start_user_key;
      }
      const Slice& end_user_key =
          storage_info_.LevelFiles(level).back()->largest.user_key();
      if (!initialized || ucmp->Compare(end_user_key, *largest_user_key) > 0) {
        *largest_user_key = end_user_key;
      }
      initialized = true;
    }
  }
}

void Version::GetCreationTimeOfOldestFile(uint64_t* creation_time) {
  uint64_t oldest_time = std::numeric_limits<uint64_t>::max();
  for (int level = 0; level < storage_info_.num_non_empty_levels_; level++) {
    for (FileMetaData* meta : storage_info_.LevelFiles(level)) {
      assert(meta->fd.table_reader != nullptr);
      uint64_t file_creation_time = meta->TryGetFileCreationTime();
      if (file_creation_time == kUnknownFileCreationTime) {
        *creation_time = 0;
        return;
      }
      if (file_creation_time < oldest_time) {
        oldest_time = file_creation_time;
      }
    }
  }
  *creation_time = oldest_time;
}

InternalIterator* Version::TEST_GetLevelIterator(
    const ReadOptions& read_options, MergeIteratorBuilder* merge_iter_builder,
    int level, bool allow_unprepared_value) {
  auto* arena = merge_iter_builder->GetArena();
  auto* mem = arena->AllocateAligned(sizeof(LevelIterator));
  TruncatedRangeDelIterator*** tombstone_iter_ptr = nullptr;
  auto level_iter = new (mem) LevelIterator(
      cfd_->table_cache(), read_options, file_options_,
      cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
      mutable_cf_options_.prefix_extractor, should_sample_file_read(),
      cfd_->internal_stats()->GetFileReadHist(level),
      TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
      mutable_cf_options_.block_protection_bytes_per_key,
      nullptr /* range_del_agg */, nullptr /* compaction_boundaries */,
      allow_unprepared_value, &tombstone_iter_ptr);
  if (read_options.ignore_range_deletions) {
    merge_iter_builder->AddIterator(level_iter);
  } else {
    merge_iter_builder->AddPointAndTombstoneIterator(
        level_iter, nullptr /* tombstone_iter */, tombstone_iter_ptr);
  }
  return level_iter;
}

uint64_t VersionStorageInfo::GetEstimatedActiveKeys() const {
  // Estimation will be inaccurate when:
  // (1) there exist merge keys
  // (2) keys are directly overwritten
  // (3) deletion on non-existing keys
  // (4) low number of samples
  if (current_num_samples_ == 0) {
    return 0;
  }

  if (current_num_non_deletions_ <= current_num_deletions_) {
    return 0;
  }

  uint64_t est = current_num_non_deletions_ - current_num_deletions_;

  uint64_t file_count = 0;
  for (int level = 0; level < num_levels_; ++level) {
    file_count += files_[level].size();
  }

  if (current_num_samples_ < file_count) {
    // casting to avoid overflowing
    return static_cast<uint64_t>(
        (est * static_cast<double>(file_count) / current_num_samples_));
  } else {
    return est;
  }
}

double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
    int level) const {
  assert(level < num_levels_);
  uint64_t sum_file_size_bytes = 0;
  uint64_t sum_data_size_bytes = 0;
  for (auto* file_meta : files_[level]) {
    sum_file_size_bytes += file_meta->fd.GetFileSize();
    sum_data_size_bytes += file_meta->raw_key_size + file_meta->raw_value_size;
  }
  if (sum_file_size_bytes == 0) {
    return -1.0;
  }
  return static_cast<double>(sum_data_size_bytes) / sum_file_size_bytes;
}

void Version::AddIterators(const ReadOptions& read_options,
                           const FileOptions& soptions,
                           MergeIteratorBuilder* merge_iter_builder,
                           bool allow_unprepared_value) {
  assert(storage_info_.finalized_);

  for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
    AddIteratorsForLevel(read_options, soptions, merge_iter_builder, level,
                         allow_unprepared_value);
  }
}

void Version::AddIteratorsForLevel(const ReadOptions& read_options,
                                   const FileOptions& soptions,
                                   MergeIteratorBuilder* merge_iter_builder,
                                   int level, bool allow_unprepared_value) {
  assert(storage_info_.finalized_);
  if (level >= storage_info_.num_non_empty_levels()) {
    // This is an empty level
    return;
  } else if (storage_info_.LevelFilesBrief(level).num_files == 0) {
    // No files in this level
    return;
  }

  bool should_sample = should_sample_file_read();

  auto* arena = merge_iter_builder->GetArena();
  if (level == 0) {
    // Merge all level zero files together since they may overlap
    TruncatedRangeDelIterator* tombstone_iter = nullptr;
    for (size_t i = 0; i < storage_info_.LevelFilesBrief(0).num_files; i++) {
      const auto& file = storage_info_.LevelFilesBrief(0).files[i];
      auto table_iter = cfd_->table_cache()->NewIterator(
          read_options, soptions, cfd_->internal_comparator(),
          *file.file_metadata, /*range_del_agg=*/nullptr,
          mutable_cf_options_.prefix_extractor, nullptr,
          cfd_->internal_stats()->GetFileReadHist(0),
          TableReaderCaller::kUserIterator, arena,
          /*skip_filters=*/false, /*level=*/0, max_file_size_for_l0_meta_pin_,
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr, allow_unprepared_value,
          mutable_cf_options_.block_protection_bytes_per_key, &tombstone_iter);
      if (read_options.ignore_range_deletions) {
        merge_iter_builder->AddIterator(table_iter);
      } else {
        merge_iter_builder->AddPointAndTombstoneIterator(table_iter,
                                                         tombstone_iter);
      }
    }
    if (should_sample) {
      // Count ones for every L0 files. This is done per iterator creation
      // rather than Seek(), while files in other levels are recored per seek.
      // If users execute one range query per iterator, there may be some
      // discrepancy here.
      for (FileMetaData* meta : storage_info_.LevelFiles(0)) {
        sample_file_read_inc(meta);
      }
    }
  } else if (storage_info_.LevelFilesBrief(level).num_files > 0) {
    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    auto* mem = arena->AllocateAligned(sizeof(LevelIterator));
    TruncatedRangeDelIterator*** tombstone_iter_ptr = nullptr;
    auto level_iter = new (mem) LevelIterator(
        cfd_->table_cache(), read_options, soptions,
        cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
        mutable_cf_options_.prefix_extractor, should_sample_file_read(),
        cfd_->internal_stats()->GetFileReadHist(level),
        TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
        mutable_cf_options_.block_protection_bytes_per_key,
        /*range_del_agg=*/nullptr,
        /*compaction_boundaries=*/nullptr, allow_unprepared_value,
        &tombstone_iter_ptr);
    if (read_options.ignore_range_deletions) {
      merge_iter_builder->AddIterator(level_iter);
    } else {
      merge_iter_builder->AddPointAndTombstoneIterator(
          level_iter, nullptr /* tombstone_iter */, tombstone_iter_ptr);
    }
  }
}

Status Version::OverlapWithLevelIterator(const ReadOptions& read_options,
                                         const FileOptions& file_options,
                                         const Slice& smallest_user_key,
                                         const Slice& largest_user_key,
                                         int level, bool* overlap) {
  assert(storage_info_.finalized_);

  auto icmp = cfd_->internal_comparator();
  auto ucmp = icmp.user_comparator();

  Arena arena;
  Status status;
  ReadRangeDelAggregator range_del_agg(&icmp,
                                       kMaxSequenceNumber /* upper_bound */);

  *overlap = false;

  if (level == 0) {
    for (size_t i = 0; i < storage_info_.LevelFilesBrief(0).num_files; i++) {
      const auto file = &storage_info_.LevelFilesBrief(0).files[i];
      if (AfterFile(ucmp, &smallest_user_key, file) ||
          BeforeFile(ucmp, &largest_user_key, file)) {
        continue;
      }
      ScopedArenaIterator iter(cfd_->table_cache()->NewIterator(
          read_options, file_options, cfd_->internal_comparator(),
          *file->file_metadata, &range_del_agg,
          mutable_cf_options_.prefix_extractor, nullptr,
          cfd_->internal_stats()->GetFileReadHist(0),
          TableReaderCaller::kUserIterator, &arena,
          /*skip_filters=*/false, /*level=*/0, max_file_size_for_l0_meta_pin_,
          /*smallest_compaction_key=*/nullptr,
          /*largest_compaction_key=*/nullptr,
          /*allow_unprepared_value=*/false,
          mutable_cf_options_.block_protection_bytes_per_key));
      status = OverlapWithIterator(ucmp, smallest_user_key, largest_user_key,
                                   iter.get(), overlap);
      if (!status.ok() || *overlap) {
        break;
      }
    }
  } else if (storage_info_.LevelFilesBrief(level).num_files > 0) {
    auto mem = arena.AllocateAligned(sizeof(LevelIterator));
    ScopedArenaIterator iter(new (mem) LevelIterator(
        cfd_->table_cache(), read_options, file_options,
        cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
        mutable_cf_options_.prefix_extractor, should_sample_file_read(),
        cfd_->internal_stats()->GetFileReadHist(level),
        TableReaderCaller::kUserIterator, IsFilterSkipped(level), level,
        mutable_cf_options_.block_protection_bytes_per_key, &range_del_agg,
        nullptr, false));
    status = OverlapWithIterator(ucmp, smallest_user_key, largest_user_key,
                                 iter.get(), overlap);
  }

  if (status.ok() && *overlap == false &&
      range_del_agg.IsRangeOverlapped(smallest_user_key, largest_user_key)) {
    *overlap = true;
  }
  return status;
}

VersionStorageInfo::VersionStorageInfo(
    const InternalKeyComparator* internal_comparator,
    const Comparator* user_comparator, int levels,
    CompactionStyle compaction_style, VersionStorageInfo* ref_vstorage,
    bool _force_consistency_checks,
    EpochNumberRequirement epoch_number_requirement)
    : internal_comparator_(internal_comparator),
      user_comparator_(user_comparator),
      // cfd is nullptr if Version is dummy
      num_levels_(levels),
      num_non_empty_levels_(0),
      file_indexer_(user_comparator),
      compaction_style_(compaction_style),
      files_(new std::vector<FileMetaData*>[num_levels_]),
      base_level_(num_levels_ == 1 ? -1 : 1),
      lowest_unnecessary_level_(-1),
      level_multiplier_(0.0),
      files_by_compaction_pri_(num_levels_),
      level0_non_overlapping_(false),
      next_file_to_compact_by_size_(num_levels_),
      compaction_score_(num_levels_),
      compaction_level_(num_levels_),
      l0_delay_trigger_count_(0),
      compact_cursor_(num_levels_),
      accumulated_file_size_(0),
      accumulated_raw_key_size_(0),
      accumulated_raw_value_size_(0),
      accumulated_num_non_deletions_(0),
      accumulated_num_deletions_(0),
      current_num_non_deletions_(0),
      current_num_deletions_(0),
      current_num_samples_(0),
      estimated_compaction_needed_bytes_(0),
      finalized_(false),
      force_consistency_checks_(_force_consistency_checks),
      epoch_number_requirement_(epoch_number_requirement) {
  if (ref_vstorage != nullptr) {
    accumulated_file_size_ = ref_vstorage->accumulated_file_size_;
    accumulated_raw_key_size_ = ref_vstorage->accumulated_raw_key_size_;
    accumulated_raw_value_size_ = ref_vstorage->accumulated_raw_value_size_;
    accumulated_num_non_deletions_ =
        ref_vstorage->accumulated_num_non_deletions_;
    accumulated_num_deletions_ = ref_vstorage->accumulated_num_deletions_;
    current_num_non_deletions_ = ref_vstorage->current_num_non_deletions_;
    current_num_deletions_ = ref_vstorage->current_num_deletions_;
    current_num_samples_ = ref_vstorage->current_num_samples_;
    oldest_snapshot_seqnum_ = ref_vstorage->oldest_snapshot_seqnum_;
    compact_cursor_ = ref_vstorage->compact_cursor_;
    compact_cursor_.resize(num_levels_);
  }
}

Version::Version(ColumnFamilyData* column_family_data, VersionSet* vset,
                 const FileOptions& file_opt,
                 const MutableCFOptions mutable_cf_options,
                 const std::shared_ptr<IOTracer>& io_tracer,
                 uint64_t version_number,
                 EpochNumberRequirement epoch_number_requirement)
    : env_(vset->env_),
      clock_(vset->clock_),
      cfd_(column_family_data),
      info_log_((cfd_ == nullptr) ? nullptr : cfd_->ioptions()->logger),
      db_statistics_((cfd_ == nullptr) ? nullptr : cfd_->ioptions()->stats),
      table_cache_((cfd_ == nullptr) ? nullptr : cfd_->table_cache()),
      blob_source_(cfd_ ? cfd_->blob_source() : nullptr),
      merge_operator_(
          (cfd_ == nullptr) ? nullptr : cfd_->ioptions()->merge_operator.get()),
      storage_info_(
          (cfd_ == nullptr) ? nullptr : &cfd_->internal_comparator(),
          (cfd_ == nullptr) ? nullptr : cfd_->user_comparator(),
          cfd_ == nullptr ? 0 : cfd_->NumberLevels(),
          cfd_ == nullptr ? kCompactionStyleLevel
                          : cfd_->ioptions()->compaction_style,
          (cfd_ == nullptr || cfd_->current() == nullptr)
              ? nullptr
              : cfd_->current()->storage_info(),
          cfd_ == nullptr ? false : cfd_->ioptions()->force_consistency_checks,
          epoch_number_requirement),
      vset_(vset),
      next_(this),
      prev_(this),
      refs_(0),
      file_options_(file_opt),
      mutable_cf_options_(mutable_cf_options),
      max_file_size_for_l0_meta_pin_(
          MaxFileSizeForL0MetaPin(mutable_cf_options_)),
      version_number_(version_number),
      io_tracer_(io_tracer),
      use_async_io_(false) {
  if (CheckFSFeatureSupport(env_->GetFileSystem().get(),
                            FSSupportedOps::kAsyncIO)) {
    use_async_io_ = true;
  }
}

Status Version::GetBlob(const ReadOptions& read_options, const Slice& user_key,
                        const Slice& blob_index_slice,
                        FilePrefetchBuffer* prefetch_buffer,
                        PinnableSlice* value, uint64_t* bytes_read) const {
  BlobIndex blob_index;

  {
    Status s = blob_index.DecodeFrom(blob_index_slice);
    if (!s.ok()) {
      return s;
    }
  }

  return GetBlob(read_options, user_key, blob_index, prefetch_buffer, value,
                 bytes_read);
}

Status Version::GetBlob(const ReadOptions& read_options, const Slice& user_key,
                        const BlobIndex& blob_index,
                        FilePrefetchBuffer* prefetch_buffer,
                        PinnableSlice* value, uint64_t* bytes_read) const {
  assert(value);

  if (blob_index.HasTTL() || blob_index.IsInlined()) {
    return Status::Corruption("Unexpected TTL/inlined blob index");
  }

  const uint64_t blob_file_number = blob_index.file_number();

  auto blob_file_meta = storage_info_.GetBlobFileMetaData(blob_file_number);
  if (!blob_file_meta) {
    return Status::Corruption("Invalid blob file number");
  }

  assert(blob_source_);
  value->Reset();
  const Status s = blob_source_->GetBlob(
      read_options, user_key, blob_file_number, blob_index.offset(),
      blob_file_meta->GetBlobFileSize(), blob_index.size(),
      blob_index.compression(), prefetch_buffer, value, bytes_read);

  return s;
}

void Version::MultiGetBlob(
    const ReadOptions& read_options, MultiGetRange& range,
    std::unordered_map<uint64_t, BlobReadContexts>& blob_ctxs) {
  assert(!blob_ctxs.empty());

  autovector<BlobFileReadRequests> blob_reqs;

  for (auto& ctx : blob_ctxs) {
    const auto file_number = ctx.first;
    const auto blob_file_meta = storage_info_.GetBlobFileMetaData(file_number);

    autovector<BlobReadRequest> blob_reqs_in_file;
    BlobReadContexts& blobs_in_file = ctx.second;
    for (auto& blob : blobs_in_file) {
      const BlobIndex& blob_index = blob.blob_index;
      const KeyContext* const key_context = blob.key_context;
      assert(key_context);
      assert(key_context->get_context);
      assert(key_context->s);

      if (key_context->value) {
        key_context->value->Reset();
      } else {
        assert(key_context->columns);
        key_context->columns->Reset();
      }

      if (!blob_file_meta) {
        *key_context->s = Status::Corruption("Invalid blob file number");
        continue;
      }

      if (blob_index.HasTTL() || blob_index.IsInlined()) {
        *key_context->s =
            Status::Corruption("Unexpected TTL/inlined blob index");
        continue;
      }

      blob_reqs_in_file.emplace_back(
          key_context->get_context->ukey_to_get_blob_value(),
          blob_index.offset(), blob_index.size(), blob_index.compression(),
          &blob.result, key_context->s);
    }
    if (blob_reqs_in_file.size() > 0) {
      const auto file_size = blob_file_meta->GetBlobFileSize();
      blob_reqs.emplace_back(file_number, file_size, blob_reqs_in_file);
    }
  }

  if (blob_reqs.size() > 0) {
    blob_source_->MultiGetBlob(read_options, blob_reqs,
                               /*bytes_read=*/nullptr);
  }

  for (auto& ctx : blob_ctxs) {
    BlobReadContexts& blobs_in_file = ctx.second;
    for (auto& blob : blobs_in_file) {
      const KeyContext* const key_context = blob.key_context;
      assert(key_context);
      assert(key_context->get_context);
      assert(key_context->s);

      if (key_context->s->ok()) {
        if (key_context->value) {
          *key_context->value = std::move(blob.result);
          range.AddValueSize(key_context->value->size());
        } else {
          assert(key_context->columns);
          key_context->columns->SetPlainValue(std::move(blob.result));
          range.AddValueSize(key_context->columns->serialized_size());
        }

        if (range.GetValueSize() > read_options.value_size_soft_limit) {
          *key_context->s = Status::Aborted();
        }
      } else if (key_context->s->IsIncomplete()) {
        // read_options.read_tier == kBlockCacheTier
        // Cannot read blob(s): no disk I/O allowed
        auto& get_context = *(key_context->get_context);
        get_context.MarkKeyMayExist();
      }
    }
  }
}

void Version::Get(const ReadOptions& read_options, const LookupKey& k,
                  PinnableSlice* value, PinnableWideColumns* columns,
                  std::string* timestamp, Status* status,
                  MergeContext* merge_context,
                  SequenceNumber* max_covering_tombstone_seq,
                  PinnedIteratorsManager* pinned_iters_mgr, bool* value_found,
                  bool* key_exists, SequenceNumber* seq, ReadCallback* callback,
                  bool* is_blob, bool do_merge) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();

  assert(status->ok() || status->IsMergeInProgress());

  if (key_exists != nullptr) {
    // will falsify below if not found
    *key_exists = true;
  }

  uint64_t tracing_get_id = BlockCacheTraceHelper::kReservedGetId;
  if (vset_ && vset_->block_cache_tracer_ &&
      vset_->block_cache_tracer_->is_tracing_enabled()) {
    tracing_get_id = vset_->block_cache_tracer_->NextGetId();
  }

  // Note: the old StackableDB-based BlobDB passes in
  // GetImplOptions::is_blob_index; for the integrated BlobDB implementation, we
  // need to provide it here.
  bool is_blob_index = false;
  bool* const is_blob_to_use = is_blob ? is_blob : &is_blob_index;
  BlobFetcher blob_fetcher(this, read_options);

  assert(pinned_iters_mgr);
  GetContext get_context(
      user_comparator(), merge_operator_, info_log_, db_statistics_,
      status->ok() ? GetContext::kNotFound : GetContext::kMerge, user_key,
      do_merge ? value : nullptr, do_merge ? columns : nullptr,
      do_merge ? timestamp : nullptr, value_found, merge_context, do_merge,
      max_covering_tombstone_seq, clock_, seq,
      merge_operator_ ? pinned_iters_mgr : nullptr, callback, is_blob_to_use,
      tracing_get_id, &blob_fetcher);

  // Pin blocks that we read to hold merge operands
  if (merge_operator_) {
    pinned_iters_mgr->StartPinning();
  }

  FilePicker fp(user_key, ikey, &storage_info_.level_files_brief_,
                storage_info_.num_non_empty_levels_,
                &storage_info_.file_indexer_, user_comparator(),
                internal_comparator());
  FdWithKeyRange* f = fp.GetNextFile();

  while (f != nullptr) {
    if (*max_covering_tombstone_seq > 0) {
      // The remaining files we look at will only contain covered keys, so we
      // stop here.
      break;
    }
    if (get_context.sample()) {
      sample_file_read_inc(f->file_metadata);
    }

    bool timer_enabled =
        GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
        get_perf_context()->per_level_perf_context_enabled;
    StopWatchNano timer(clock_, timer_enabled /* auto_start */);
    *status = table_cache_->Get(
        read_options, *internal_comparator(), *f->file_metadata, ikey,
        &get_context, mutable_cf_options_.block_protection_bytes_per_key,
        mutable_cf_options_.prefix_extractor,
        cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
        IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                        fp.IsHitFileLastInLevel()),
        fp.GetHitFileLevel(), max_file_size_for_l0_meta_pin_);
    // TODO: examine the behavior for corrupted key
    if (timer_enabled) {
      PERF_COUNTER_BY_LEVEL_ADD(get_from_table_nanos, timer.ElapsedNanos(),
                                fp.GetHitFileLevel());
    }
    if (!status->ok()) {
      if (db_statistics_ != nullptr) {
        get_context.ReportCounters();
      }
      return;
    }

    // report the counters before returning
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        break;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        break;
      case GetContext::kFound:
        if (fp.GetHitFileLevel() == 0) {
          RecordTick(db_statistics_, GET_HIT_L0);
        } else if (fp.GetHitFileLevel() == 1) {
          RecordTick(db_statistics_, GET_HIT_L1);
        } else if (fp.GetHitFileLevel() >= 2) {
          RecordTick(db_statistics_, GET_HIT_L2_AND_UP);
        }

        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1,
                                  fp.GetHitFileLevel());

        if (is_blob_index && do_merge && (value || columns)) {
          assert(!columns ||
                 (!columns->columns().empty() &&
                  columns->columns().front().name() == kDefaultWideColumnName));

          Slice blob_index =
              value ? *value : columns->columns().front().value();

          TEST_SYNC_POINT_CALLBACK("Version::Get::TamperWithBlobIndex",
                                   &blob_index);

          constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

          PinnableSlice result;

          constexpr uint64_t* bytes_read = nullptr;

          *status = GetBlob(read_options, get_context.ukey_to_get_blob_value(),
                            blob_index, prefetch_buffer, &result, bytes_read);
          if (!status->ok()) {
            if (status->IsIncomplete()) {
              get_context.MarkKeyMayExist();
            }
            return;
          }

          if (value) {
            *value = std::move(result);
          } else {
            assert(columns);
            columns->SetPlainValue(std::move(result));
          }
        }

        return;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        return;
      case GetContext::kCorrupt:
        *status = Status::Corruption("corrupted key for ", user_key);
        return;
      case GetContext::kUnexpectedBlobIndex:
        ROCKS_LOG_ERROR(info_log_, "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "ROCKSDB_NAMESPACE::blob_db::BlobDB instead.");
        return;
      case GetContext::kMergeOperatorFailed:
        *status = Status::Corruption(Status::SubCode::kMergeOperatorFailed);
        return;
    }
    f = fp.GetNextFile();
  }
  if (db_statistics_ != nullptr) {
    get_context.ReportCounters();
  }
  if (GetContext::kMerge == get_context.State()) {
    if (!do_merge) {
      *status = Status::OK();
      return;
    }
    if (!merge_operator_) {
      *status = Status::InvalidArgument(
          "merge_operator is not properly initialized.");
      return;
    }
    // merge_operands are in saver and we hit the beginning of the key history
    // do a final merge of nullptr and operands;
    if (value || columns) {
      std::string result;
      // `op_failure_scope` (an output parameter) is not provided (set to
      // nullptr) since a failure must be propagated regardless of its value.
      *status = MergeHelper::TimedFullMerge(
          merge_operator_, user_key, nullptr, merge_context->GetOperands(),
          &result, info_log_, db_statistics_, clock_,
          /* result_operand */ nullptr, /* update_num_ops_stats */ true,
          /* op_failure_scope */ nullptr);
      if (status->ok()) {
        if (LIKELY(value != nullptr)) {
          *(value->GetSelf()) = std::move(result);
          value->PinSelf();
        } else {
          assert(columns != nullptr);
          columns->SetPlainValue(std::move(result));
        }
      }
    }
  } else {
    if (key_exists != nullptr) {
      *key_exists = false;
    }
    *status = Status::NotFound();  // Use an empty error message for speed
  }
}

void Version::MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                       ReadCallback* callback) {
  PinnedIteratorsManager pinned_iters_mgr;

  // Pin blocks that we read to hold merge operands
  if (merge_operator_) {
    pinned_iters_mgr.StartPinning();
  }
  uint64_t tracing_mget_id = BlockCacheTraceHelper::kReservedGetId;

  if (vset_ && vset_->block_cache_tracer_ &&
      vset_->block_cache_tracer_->is_tracing_enabled()) {
    tracing_mget_id = vset_->block_cache_tracer_->NextGetId();
  }
  // Even though we know the batch size won't be > MAX_BATCH_SIZE,
  // use autovector in order to avoid unnecessary construction of GetContext
  // objects, which is expensive
  autovector<GetContext, 16> get_ctx;
  BlobFetcher blob_fetcher(this, read_options);
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    assert(iter->s->ok() || iter->s->IsMergeInProgress());
    get_ctx.emplace_back(
        user_comparator(), merge_operator_, info_log_, db_statistics_,
        iter->s->ok() ? GetContext::kNotFound : GetContext::kMerge,
        iter->ukey_with_ts, iter->value, iter->columns, iter->timestamp,
        nullptr, &(iter->merge_context), true,
        &iter->max_covering_tombstone_seq, clock_, nullptr,
        merge_operator_ ? &pinned_iters_mgr : nullptr, callback,
        &iter->is_blob_index, tracing_mget_id, &blob_fetcher);
    // MergeInProgress status, if set, has been transferred to the get_context
    // state, so we set status to ok here. From now on, the iter status will
    // be used for IO errors, and get_context state will be used for any
    // key level errors
    *(iter->s) = Status::OK();
  }
  int get_ctx_index = 0;
  for (auto iter = range->begin(); iter != range->end();
       ++iter, get_ctx_index++) {
    iter->get_context = &(get_ctx[get_ctx_index]);
  }

  Status s;
  // blob_file => [[blob_idx, it], ...]
  std::unordered_map<uint64_t, BlobReadContexts> blob_ctxs;
  MultiGetRange keys_with_blobs_range(*range, range->begin(), range->end());
#if USE_COROUTINES
  if (read_options.async_io && read_options.optimize_multiget_for_io &&
      using_coroutines() && use_async_io_) {
    s = MultiGetAsync(read_options, range, &blob_ctxs);
  } else
#endif  // USE_COROUTINES
  {
    MultiGetRange file_picker_range(*range, range->begin(), range->end());
    FilePickerMultiGet fp(&file_picker_range, &storage_info_.level_files_brief_,
                          storage_info_.num_non_empty_levels_,
                          &storage_info_.file_indexer_, user_comparator(),
                          internal_comparator());
    FdWithKeyRange* f = fp.GetNextFileInLevel();
    uint64_t num_index_read = 0;
    uint64_t num_filter_read = 0;
    uint64_t num_sst_read = 0;
    uint64_t num_level_read = 0;

    int prev_level = -1;

    while (!fp.IsSearchEnded()) {
      // This will be set to true later if we actually look up in a file in L0.
      // For per level stats purposes, an L0 file is treated as a level
      bool dump_stats_for_l0_file = false;

      // Avoid using the coroutine version if we're looking in a L0 file, since
      // L0 files won't be parallelized anyway. The regular synchronous version
      // is faster.
      if (!read_options.async_io || !using_coroutines() || !use_async_io_ ||
          fp.GetHitFileLevel() == 0 || !fp.RemainingOverlapInLevel()) {
        if (f) {
          bool skip_filters =
              IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                              fp.IsHitFileLastInLevel());
          // Call MultiGetFromSST for looking up a single file
          s = MultiGetFromSST(read_options, fp.CurrentFileRange(),
                              fp.GetHitFileLevel(), skip_filters,
                              /*skip_range_deletions=*/false, f, blob_ctxs,
                              /*table_handle=*/nullptr, num_filter_read,
                              num_index_read, num_sst_read);
          if (fp.GetHitFileLevel() == 0) {
            dump_stats_for_l0_file = true;
          }
        }
        if (s.ok()) {
          f = fp.GetNextFileInLevel();
        }
#if USE_COROUTINES
      } else {
        std::vector<folly::coro::Task<Status>> mget_tasks;
        while (f != nullptr) {
          MultiGetRange file_range = fp.CurrentFileRange();
          TableCache::TypedHandle* table_handle = nullptr;
          bool skip_filters =
              IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                              fp.IsHitFileLastInLevel());
          bool skip_range_deletions = false;
          if (!skip_filters) {
            Status status = table_cache_->MultiGetFilter(
                read_options, *internal_comparator(), *f->file_metadata,
                mutable_cf_options_.prefix_extractor,
                cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
                fp.GetHitFileLevel(), &file_range, &table_handle,
                mutable_cf_options_.block_protection_bytes_per_key);
            skip_range_deletions = true;
            if (status.ok()) {
              skip_filters = true;
            } else if (!status.IsNotSupported()) {
              s = status;
            }
          }

          if (!s.ok()) {
            break;
          }

          if (!file_range.empty()) {
            mget_tasks.emplace_back(MultiGetFromSSTCoroutine(
                read_options, file_range, fp.GetHitFileLevel(), skip_filters,
                skip_range_deletions, f, blob_ctxs, table_handle,
                num_filter_read, num_index_read, num_sst_read));
          }
          if (fp.KeyMaySpanNextFile()) {
            break;
          }
          f = fp.GetNextFileInLevel();
        }
        if (mget_tasks.size() > 0) {
          RecordTick(db_statistics_, MULTIGET_COROUTINE_COUNT,
                     mget_tasks.size());
          // Collect all results so far
          std::vector<Status> statuses = folly::coro::blockingWait(
              folly::coro::collectAllRange(std::move(mget_tasks))
                  .scheduleOn(&range->context()->executor()));
          if (s.ok()) {
            for (Status stat : statuses) {
              if (!stat.ok()) {
                s = std::move(stat);
                break;
              }
            }
          }

          if (s.ok() && fp.KeyMaySpanNextFile()) {
            f = fp.GetNextFileInLevel();
          }
        }
#endif  // USE_COROUTINES
      }
      // If bad status or we found final result for all the keys
      if (!s.ok() || file_picker_range.empty()) {
        break;
      }
      if (!f) {
        // Reached the end of this level. Prepare the next level
        fp.PrepareNextLevelForSearch();
        if (!fp.IsSearchEnded()) {
          // Its possible there is no overlap on this level and f is nullptr
          f = fp.GetNextFileInLevel();
        }
        if (dump_stats_for_l0_file ||
            (prev_level != 0 && prev_level != (int)fp.GetHitFileLevel())) {
          // Dump the stats if the search has moved to the next level and
          // reset for next level.
          if (num_filter_read + num_index_read) {
            RecordInHistogram(db_statistics_,
                              NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                              num_index_read + num_filter_read);
          }
          if (num_sst_read) {
            RecordInHistogram(db_statistics_, NUM_SST_READ_PER_LEVEL,
                              num_sst_read);
            num_level_read++;
          }
          num_filter_read = 0;
          num_index_read = 0;
          num_sst_read = 0;
        }
        prev_level = fp.GetHitFileLevel();
      }
    }

    // Dump stats for most recent level
    if (num_filter_read + num_index_read) {
      RecordInHistogram(db_statistics_,
                        NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                        num_index_read + num_filter_read);
    }
    if (num_sst_read) {
      RecordInHistogram(db_statistics_, NUM_SST_READ_PER_LEVEL, num_sst_read);
      num_level_read++;
    }
    if (num_level_read) {
      RecordInHistogram(db_statistics_, NUM_LEVEL_READ_PER_MULTIGET,
                        num_level_read);
    }
  }

  if (s.ok() && !blob_ctxs.empty()) {
    MultiGetBlob(read_options, keys_with_blobs_range, blob_ctxs);
  }

  // Process any left over keys
  for (auto iter = range->begin(); s.ok() && iter != range->end(); ++iter) {
    GetContext& get_context = *iter->get_context;
    Status* status = iter->s;
    Slice user_key = iter->lkey->user_key();

    if (db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    if (GetContext::kMerge == get_context.State()) {
      if (!merge_operator_) {
        *status = Status::InvalidArgument(
            "merge_operator is not properly initialized.");
        range->MarkKeyDone(iter);
        continue;
      }
      // merge_operands are in saver and we hit the beginning of the key history
      // do a final merge of nullptr and operands;
      std::string result;

      // `op_failure_scope` (an output parameter) is not provided (set to
      // nullptr) since a failure must be propagated regardless of its value.
      *status = MergeHelper::TimedFullMerge(
          merge_operator_, user_key, nullptr, iter->merge_context.GetOperands(),
          &result, info_log_, db_statistics_, clock_,
          /* result_operand */ nullptr, /* update_num_ops_stats */ true,
          /* op_failure_scope */ nullptr);
      if (LIKELY(iter->value != nullptr)) {
        *iter->value->GetSelf() = std::move(result);
        iter->value->PinSelf();
        range->AddValueSize(iter->value->size());
      } else {
        assert(iter->columns);
        iter->columns->SetPlainValue(std::move(result));
        range->AddValueSize(iter->columns->serialized_size());
      }

      range->MarkKeyDone(iter);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        s = Status::Aborted();
        break;
      }
    } else {
      range->MarkKeyDone(iter);
      *status = Status::NotFound();  // Use an empty error message for speed
    }
  }

  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    range->MarkKeyDone(iter);
    *(iter->s) = s;
  }
}

#ifdef USE_COROUTINES
Status Version::ProcessBatch(
    const ReadOptions& read_options, FilePickerMultiGet* batch,
    std::vector<folly::coro::Task<Status>>& mget_tasks,
    std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs,
    autovector<FilePickerMultiGet, 4>& batches, std::deque<size_t>& waiting,
    std::deque<size_t>& to_process, unsigned int& num_tasks_queued,
    std::unordered_map<int, std::tuple<uint64_t, uint64_t, uint64_t>>&
        mget_stats) {
  FilePickerMultiGet& fp = *batch;
  MultiGetRange range = fp.GetRange();
  // Initialize a new empty range. Any keys that are not in this level will
  // eventually become part of the new range.
  MultiGetRange leftover(range, range.begin(), range.begin());
  FdWithKeyRange* f = nullptr;
  Status s;

  f = fp.GetNextFileInLevel();
  while (!f) {
    fp.PrepareNextLevelForSearch();
    if (!fp.IsSearchEnded()) {
      f = fp.GetNextFileInLevel();
    } else {
      break;
    }
  }
  while (f) {
    MultiGetRange file_range = fp.CurrentFileRange();
    TableCache::TypedHandle* table_handle = nullptr;
    bool skip_filters = IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                                        fp.IsHitFileLastInLevel());
    bool skip_range_deletions = false;
    if (!skip_filters) {
      Status status = table_cache_->MultiGetFilter(
          read_options, *internal_comparator(), *f->file_metadata,
          mutable_cf_options_.prefix_extractor,
          cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
          fp.GetHitFileLevel(), &file_range, &table_handle,
          mutable_cf_options_.block_protection_bytes_per_key);
      if (status.ok()) {
        skip_filters = true;
        skip_range_deletions = true;
      } else if (!status.IsNotSupported()) {
        s = status;
      }
    }
    if (!s.ok()) {
      break;
    }
    // At this point, file_range contains any keys that are likely in this
    // file. It may have false positives, but that's ok since higher level
    // lookups for the key are dependent on this lookup anyway.
    // Add the complement of file_range to leftover. That's the set of keys
    // definitely not in this level.
    // Subtract the complement of file_range from range, since they will be
    // processed in a separate batch in parallel.
    leftover += ~file_range;
    range -= ~file_range;
    if (!file_range.empty()) {
      int level = fp.GetHitFileLevel();
      auto stat = mget_stats.find(level);
      if (stat == mget_stats.end()) {
        auto entry = mget_stats.insert({level, {0, 0, 0}});
        assert(entry.second);
        stat = entry.first;
      }

      if (waiting.empty() && to_process.empty() &&
          !fp.RemainingOverlapInLevel() && leftover.empty() &&
          mget_tasks.empty()) {
        // All keys are in one SST file, so take the fast path
        s = MultiGetFromSST(read_options, file_range, fp.GetHitFileLevel(),
                            skip_filters, skip_range_deletions, f, *blob_ctxs,
                            table_handle, std::get<0>(stat->second),
                            std::get<1>(stat->second),
                            std::get<2>(stat->second));
      } else {
        mget_tasks.emplace_back(MultiGetFromSSTCoroutine(
            read_options, file_range, fp.GetHitFileLevel(), skip_filters,
            skip_range_deletions, f, *blob_ctxs, table_handle,
            std::get<0>(stat->second), std::get<1>(stat->second),
            std::get<2>(stat->second)));
        ++num_tasks_queued;
      }
    }
    if (fp.KeyMaySpanNextFile() && !file_range.empty()) {
      break;
    }
    f = fp.GetNextFileInLevel();
  }
  // Split the current batch only if some keys are likely in this level and
  // some are not. Only split if we're done with this level, i.e f is null.
  // Otherwise, it means there are more files in this level to look at.
  if (s.ok() && !f && !leftover.empty() && !range.empty()) {
    fp.ReplaceRange(range);
    batches.emplace_back(&leftover, fp);
    to_process.emplace_back(batches.size() - 1);
  }
  // 1. If f is non-null, that means we might not be done with this level.
  //    This can happen if one of the keys is the last key in the file, i.e
  //    fp.KeyMaySpanNextFile() is true.
  // 2. If range is empty, then we're done with this range and no need to
  //    prepare the next level
  // 3. If some tasks were queued for this range, then the next level will be
  //    prepared after executing those tasks
  if (!f && !range.empty() && !num_tasks_queued) {
    fp.PrepareNextLevelForSearch();
  }
  return s;
}

Status Version::MultiGetAsync(
    const ReadOptions& options, MultiGetRange* range,
    std::unordered_map<uint64_t, BlobReadContexts>* blob_ctxs) {
  autovector<FilePickerMultiGet, 4> batches;
  std::deque<size_t> waiting;
  std::deque<size_t> to_process;
  Status s;
  std::vector<folly::coro::Task<Status>> mget_tasks;
  std::unordered_map<int, std::tuple<uint64_t, uint64_t, uint64_t>> mget_stats;

  // Create the initial batch with the input range
  batches.emplace_back(range, &storage_info_.level_files_brief_,
                       storage_info_.num_non_empty_levels_,
                       &storage_info_.file_indexer_, user_comparator(),
                       internal_comparator());
  to_process.emplace_back(0);

  while (!to_process.empty()) {
    // As we process a batch, it may get split into two. So reserve space for
    // an additional batch in the autovector in order to prevent later moves
    // of elements in ProcessBatch().
    batches.reserve(batches.size() + 1);

    size_t idx = to_process.front();
    FilePickerMultiGet* batch = &batches.at(idx);
    unsigned int num_tasks_queued = 0;
    to_process.pop_front();
    if (batch->IsSearchEnded() || batch->GetRange().empty()) {
      // If to_process is empty, i.e no more batches to look at, then we need
      // schedule the enqueued coroutines and wait for them. Otherwise, we
      // skip this batch and move to the next one in to_process.
      if (!to_process.empty()) {
        continue;
      }
    } else {
      // Look through one level. This may split the batch and enqueue it to
      // to_process
      s = ProcessBatch(options, batch, mget_tasks, blob_ctxs, batches, waiting,
                       to_process, num_tasks_queued, mget_stats);
      // If ProcessBatch didn't enqueue any coroutine tasks, it means all
      // keys were filtered out. So put the batch back in to_process to
      // lookup in the next level
      if (!num_tasks_queued && !batch->IsSearchEnded()) {
        // Put this back in the processing queue
        to_process.emplace_back(idx);
      } else if (num_tasks_queued) {
        waiting.emplace_back(idx);
      }
    }
    // If ProcessBatch() returned an error, then schedule the enqueued
    // coroutines and wait for them, then abort the MultiGet.
    if (to_process.empty() || !s.ok()) {
      if (mget_tasks.size() > 0) {
        assert(waiting.size());
        RecordTick(db_statistics_, MULTIGET_COROUTINE_COUNT, mget_tasks.size());
        // Collect all results so far
        std::vector<Status> statuses = folly::coro::blockingWait(
            folly::coro::collectAllRange(std::move(mget_tasks))
                .scheduleOn(&range->context()->executor()));
        mget_tasks.clear();
        if (s.ok()) {
          for (Status stat : statuses) {
            if (!stat.ok()) {
              s = std::move(stat);
              break;
            }
          }
        }

        if (!s.ok()) {
          break;
        }

        for (size_t wait_idx : waiting) {
          FilePickerMultiGet& fp = batches.at(wait_idx);
          // 1. If fp.GetHitFile() is non-null, then there could be more
          // overlap in this level. So skip preparing next level.
          // 2. If fp.GetRange() is empty, then this batch is completed
          // and no need to prepare the next level.
          if (!fp.GetHitFile() && !fp.GetRange().empty()) {
            fp.PrepareNextLevelForSearch();
          }
        }
        to_process.swap(waiting);
      } else {
        assert(!s.ok() || waiting.size() == 0);
      }
    }
    if (!s.ok()) {
      break;
    }
  }

  uint64_t num_levels = 0;
  for (auto& stat : mget_stats) {
    if (stat.first == 0) {
      num_levels += std::get<2>(stat.second);
    } else {
      num_levels++;
    }

    uint64_t num_meta_reads =
        std::get<0>(stat.second) + std::get<1>(stat.second);
    uint64_t num_sst_reads = std::get<2>(stat.second);
    if (num_meta_reads > 0) {
      RecordInHistogram(db_statistics_,
                        NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                        num_meta_reads);
    }
    if (num_sst_reads > 0) {
      RecordInHistogram(db_statistics_, NUM_SST_READ_PER_LEVEL, num_sst_reads);
    }
  }
  if (num_levels > 0) {
    RecordInHistogram(db_statistics_, NUM_LEVEL_READ_PER_MULTIGET, num_levels);
  }

  return s;
}
#endif

bool Version::IsFilterSkipped(int level, bool is_file_last_in_level) {
  // Reaching the bottom level implies misses at all upper levels, so we'll
  // skip checking the filters when we predict a hit.
  return cfd_->ioptions()->optimize_filters_for_hits &&
         (level > 0 || is_file_last_in_level) &&
         level == storage_info_.num_non_empty_levels() - 1;
}

void VersionStorageInfo::GenerateLevelFilesBrief() {
  level_files_brief_.resize(num_non_empty_levels_);
  for (int level = 0; level < num_non_empty_levels_; level++) {
    DoGenerateLevelFilesBrief(&level_files_brief_[level], files_[level],
                              &arena_);
  }
}

void VersionStorageInfo::PrepareForVersionAppend(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options) {
  ComputeCompensatedSizes();
  UpdateNumNonEmptyLevels();
  CalculateBaseBytes(immutable_options, mutable_cf_options);
  UpdateFilesByCompactionPri(immutable_options, mutable_cf_options);
  GenerateFileIndexer();
  GenerateLevelFilesBrief();
  GenerateLevel0NonOverlapping();
  if (!immutable_options.allow_ingest_behind) {
    GenerateBottommostFiles();
  }
  GenerateFileLocationIndex();
}

void Version::PrepareAppend(const MutableCFOptions& mutable_cf_options,
                            const ReadOptions& read_options,
                            bool update_stats) {
  TEST_SYNC_POINT_CALLBACK(
      "Version::PrepareAppend:forced_check",
      reinterpret_cast<void*>(&storage_info_.force_consistency_checks_));

  if (update_stats) {
    UpdateAccumulatedStats(read_options);
  }

  storage_info_.PrepareForVersionAppend(*cfd_->ioptions(), mutable_cf_options);
}

bool Version::MaybeInitializeFileMetaData(const ReadOptions& read_options,
                                          FileMetaData* file_meta) {
  if (file_meta->init_stats_from_file || file_meta->compensated_file_size > 0) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  Status s = GetTableProperties(read_options, &tp, file_meta);
  file_meta->init_stats_from_file = true;
  if (!s.ok()) {
    ROCKS_LOG_ERROR(vset_->db_options_->info_log,
                    "Unable to load table properties for file %" PRIu64
                    " --- %s\n",
                    file_meta->fd.GetNumber(), s.ToString().c_str());
    return false;
  }
  if (tp.get() == nullptr) return false;
  file_meta->num_entries = tp->num_entries;
  file_meta->num_deletions = tp->num_deletions;
  file_meta->raw_value_size = tp->raw_value_size;
  file_meta->raw_key_size = tp->raw_key_size;
  file_meta->num_range_deletions = tp->num_range_deletions;
  return true;
}

void VersionStorageInfo::UpdateAccumulatedStats(FileMetaData* file_meta) {
  TEST_SYNC_POINT_CALLBACK("VersionStorageInfo::UpdateAccumulatedStats",
                           nullptr);

  assert(file_meta->init_stats_from_file);
  accumulated_file_size_ += file_meta->fd.GetFileSize();
  accumulated_raw_key_size_ += file_meta->raw_key_size;
  accumulated_raw_value_size_ += file_meta->raw_value_size;
  accumulated_num_non_deletions_ +=
      file_meta->num_entries - file_meta->num_deletions;
  accumulated_num_deletions_ += file_meta->num_deletions;

  current_num_non_deletions_ +=
      file_meta->num_entries - file_meta->num_deletions;
  current_num_deletions_ += file_meta->num_deletions;
  current_num_samples_++;
}

void VersionStorageInfo::RemoveCurrentStats(FileMetaData* file_meta) {
  if (file_meta->init_stats_from_file) {
    current_num_non_deletions_ -=
        file_meta->num_entries - file_meta->num_deletions;
    current_num_deletions_ -= file_meta->num_deletions;
    current_num_samples_--;
  }
}

void Version::UpdateAccumulatedStats(const ReadOptions& read_options) {
  // maximum number of table properties loaded from files.
  const int kMaxInitCount = 20;
  int init_count = 0;
  // here only the first kMaxInitCount files which haven't been
  // initialized from file will be updated with num_deletions.
  // The motivation here is to cap the maximum I/O per Version creation.
  // The reason for choosing files from lower-level instead of higher-level
  // is that such design is able to propagate the initialization from
  // lower-level to higher-level:  When the num_deletions of lower-level
  // files are updated, it will make the lower-level files have accurate
  // compensated_file_size, making lower-level to higher-level compaction
  // will be triggered, which creates higher-level files whose num_deletions
  // will be updated here.
  for (int level = 0;
       level < storage_info_.num_levels_ && init_count < kMaxInitCount;
       ++level) {
    for (auto* file_meta : storage_info_.files_[level]) {
      if (MaybeInitializeFileMetaData(read_options, file_meta)) {
        // each FileMeta will be initialized only once.
        storage_info_.UpdateAccumulatedStats(file_meta);
        // when option "max_open_files" is -1, all the file metadata has
        // already been read, so MaybeInitializeFileMetaData() won't incur
        // any I/O cost. "max_open_files=-1" means that the table cache passed
        // to the VersionSet and then to the ColumnFamilySet has a size of
        // TableCache::kInfiniteCapacity
        if (vset_->GetColumnFamilySet()->get_table_cache()->GetCapacity() ==
            TableCache::kInfiniteCapacity) {
          continue;
        }
        if (++init_count >= kMaxInitCount) {
          break;
        }
      }
    }
  }
  // In case all sampled-files contain only deletion entries, then we
  // load the table-property of a file in higher-level to initialize
  // that value.
  for (int level = storage_info_.num_levels_ - 1;
       storage_info_.accumulated_raw_value_size_ == 0 && level >= 0; --level) {
    for (int i = static_cast<int>(storage_info_.files_[level].size()) - 1;
         storage_info_.accumulated_raw_value_size_ == 0 && i >= 0; --i) {
      if (MaybeInitializeFileMetaData(read_options,
                                      storage_info_.files_[level][i])) {
        storage_info_.UpdateAccumulatedStats(storage_info_.files_[level][i]);
      }
    }
  }
}

/**
 * @brief 计算每个文件的补偿大小（Compensated Size）
 *
 * 本函数用于计算每个文件的"补偿大小"，这是 RocksDB 选择 compaction 文件时的关键指标。
 * 补偿大小 = 文件实际大小 + 删除条目的额外补偿大小。
 *
 * 为什么需要补偿大小？
 * 1. 文件中的删除操作（Deletion）会增加 compaction 的工作量
 * 2. 删除操作在合并时需要特殊处理（丢弃旧版本的键）
 * 3. 只看文件大小会低估包含大量删除的文件的 compaction 成本
 * 4. 补偿大小更能反映真实的 compaction 工作量
 *
 * 补偿大小的计算逻辑：
 * 1. 基础值：文件的实际大小（fd.GetFileSize()）
 * 2. 单点删除补偿：当单点删除数 > 非删除条目数/2 时，增加额外大小
 * 3. 范围删除补偿：直接加上范围删除的补偿大小
 *
 * 为什么要只在删除数 > 非删除数/2 时补偿？
 * - 在稳定工作负载中，删除数应该与非删除数大致相等
 * - 如果对所有删除都补偿，可能改变 LSM tree 的形状
 * - 只补偿"异常"文件（删除占比过高的文件），避免副作用
 *
 * 单点删除补偿公式：
 * compensated_size += (num_point_deletions * 2 - num_entries) *
 *                      average_value_size * kDeletionWeightOnCompaction
 *
 * 其中：
 * - num_point_deletions = num_deletions - num_range_deletions
 * - num_entries = 文件中所有条目数
 * - average_value_size = 所有文件的平均 value 大小
 * - kDeletionWeightOnCompaction = 2（删除权重）
 *
 * 范围删除补偿：
 * compensated_size += compensated_range_deletion_size
 * （这个值已经在创建文件时预先计算好）
 *
 * 调用时机：
 * - 创建新文件后（Flush 或 Compaction 完成后）
 * - 在 VersionSet::AppendVersion 中调用
 * - 只对新创建的文件计算（compensated_file_size == 0）
 *
 * 为什么只对新文件计算？
 * - 老文件的 compensated_file_size 已经在之前的 Version 中计算过
 * - 新创建的文件没有其他线程访问，可以安全修改
 * - 避免重复计算和并发问题
 *
 * @note kDeletionWeightOnCompaction = 2：
 *   - 删除操作的权重是普通操作的 2 倍
 *   - 反映删除操作在 compaction 时的额外成本
 *   - 可以调整此值来改变删除文件的选择倾向
 *
 * @note average_value_size 的作用：
 *   - 无法精确知道每个删除条目节省的空间
 *   - 使用平均值作为估计
 *   - 计算方式：总数据大小 / 总条目数（排除删除）
 *
 * @note 条件 (num_point_deletions * 2 >= num_entries)：
 *   - num_point_deletions * 2 >= num_entries
 *   - 即：点删除数 >= 总条目数 / 2
 *   - 意味着：删除条目占比 >= 50%（假设点删除和非删除各占一半）
 *   - 只对"异常"文件进行补偿
 *
 * @note 范围删除为什么单独处理？
 *   - 范围删除影响的 key 数量不确定
 *   - 难以精确估计其节省的空间
 *   - 在文件创建时预先计算 compensated_range_deletion_size
 *   - 这里直接累加即可
 *
 * @note 补偿大小的用途：
 *   - compaction_picker 按补偿大小选择文件（kByCompensatedSize 策略）
 *   - 大补偿大小的文件优先被压缩
 *   - 可以更准确地估计 compaction 工作量
 *
 * @see UpdateFilesByCompactionPri 按补偿大小排序文件
 * @see PickFileToCompact 按优先级选择文件
 */
void VersionStorageInfo::ComputeCompensatedSizes() {
  // 删除操作的权重系数（2倍）
  // 表示删除操作在 compaction 时的额外成本是普通操作的 2 倍
  static const int kDeletionWeightOnCompaction = 2;
  // 计算所有文件的平均 value 大小
  // 用于估计每个删除条目节省的空间
  uint64_t average_value_size = GetAverageValueSize();

  // compute the compensated size
  // 遍历所有层级，计算每个文件的补偿大小
  for (int level = 0; level < num_levels_; level++) {
    // 遍历该层的所有文件
    for (auto* file_meta : files_[level]) {
      // Here we only compute compensated_file_size for those file_meta
      // which compensated_file_size is uninitialized (== 0). This is true only
      // for files that have been created right now and no other thread has
      // access to them. That's why we can safely mutate compensated_file_size.
      // 只对补偿大小未初始化的文件（== 0）进行计算。
      // 这仅适用于刚刚创建且没有其他线程访问的文件。
      // 因此可以安全地修改 compensated_file_size。
      if (file_meta->compensated_file_size == 0) {
        // 基础值：文件的实际大小
        file_meta->compensated_file_size = file_meta->fd.GetFileSize();

        // Here we only boost the size of deletion entries of a file only
        // when the number of deletion entries is greater than the number of
        // non-deletion entries in the file.  The motivation here is that in
        // a stable workload, the number of deletion entries should be roughly
        // equal to the number of non-deletion entries.  If we compensate the
        // size of deletion entries in a stable workload, the deletion
        // compensation logic might introduce unwanted effet which changes the
        // shape of LSM tree.
        // 只在文件中的删除条目数大于非删除条目数时，才增加文件大小。
        // 这里的动机是，在稳定的工作负载中，删除条目数应该与非删除条目数大致相等。
        // 如果我们对稳定工作负载中的删除条目大小进行补偿，
        // 删除补偿逻辑可能会引入不希望的副作用，改变 LSM tree 的形状。
        //
        // 条件：(num_deletions - num_range_deletions) * 2 >= num_entries
        // 即：单点删除数 * 2 >= 总条目数
        // 等价于：单点删除数 >= 总条目数 / 2（假设点删除和非删除各占一半）
        if ((file_meta->num_deletions - file_meta->num_range_deletions) * 2 >=
            file_meta->num_entries) {
          // 计算需要补偿的额外大小：
          // (单点删除数 * 2 - 总条目数) * 平均值大小 * 删除权重
          // 解释：
          // - 单点删除数 * 2：假设点删除和非删除各占一半，那么点删除数 * 2 ≈ 总条目数
          // - 如果点删除数 * 2 > 总条目数，说明删除占比过高
          // - 差值就是"额外"的删除数，需要补偿
          // - 乘以平均值大小和权重，得到补偿的字节数
          file_meta->compensated_file_size +=
              ((file_meta->num_deletions - file_meta->num_range_deletions) * 2 -
               file_meta->num_entries) *
              average_value_size * kDeletionWeightOnCompaction;
        }
        // 加上范围删除的补偿大小
        // 这个值在文件创建时已经预先计算好
        // 范围删除补偿的具体逻辑在写入文件时计算
        file_meta->compensated_file_size +=
            file_meta->compensated_range_deletion_size;
      }
    }
  }
}

int VersionStorageInfo::MaxInputLevel() const {
  if (compaction_style_ == kCompactionStyleLevel) {
    return num_levels() - 2;
  }
  return 0;
}

int VersionStorageInfo::MaxOutputLevel(bool allow_ingest_behind) const {
  if (allow_ingest_behind) {
    assert(num_levels() > 1);
    return num_levels() - 2;
  }
  return num_levels() - 1;
}

void VersionStorageInfo::EstimateCompactionBytesNeeded(
    const MutableCFOptions& mutable_cf_options) {
  // Only implemented for level-based compaction
  if (compaction_style_ != kCompactionStyleLevel) {
    estimated_compaction_needed_bytes_ = 0;
    return;
  }

  // Start from Level 0, if level 0 qualifies compaction to level 1,
  // we estimate the size of compaction.
  // Then we move on to the next level and see whether it qualifies compaction
  // to the next level. The size of the level is estimated as the actual size
  // on the level plus the input bytes from the previous level if there is any.
  // If it exceeds, take the exceeded bytes as compaction input and add the size
  // of the compaction size to tatal size.
  // We keep doing it to Level 2, 3, etc, until the last level and return the
  // accumulated bytes.

  uint64_t bytes_compact_to_next_level = 0;
  uint64_t level_size = 0;
  for (auto* f : files_[0]) {
    level_size += f->fd.GetFileSize();
  }
  // Level 0
  bool level0_compact_triggered = false;
  if (static_cast<int>(files_[0].size()) >=
          mutable_cf_options.level0_file_num_compaction_trigger ||
      level_size >= mutable_cf_options.max_bytes_for_level_base) {
    level0_compact_triggered = true;
    estimated_compaction_needed_bytes_ = level_size;
    bytes_compact_to_next_level = level_size;
  } else {
    estimated_compaction_needed_bytes_ = 0;
  }

  // Level 1 and up.
  uint64_t bytes_next_level = 0;
  for (int level = base_level(); level <= MaxInputLevel(); level++) {
    level_size = 0;
    if (bytes_next_level > 0) {
#ifndef NDEBUG
      uint64_t level_size2 = 0;
      for (auto* f : files_[level]) {
        level_size2 += f->fd.GetFileSize();
      }
      assert(level_size2 == bytes_next_level);
#endif
      level_size = bytes_next_level;
      bytes_next_level = 0;
    } else {
      for (auto* f : files_[level]) {
        level_size += f->fd.GetFileSize();
      }
    }
    if (level == base_level() && level0_compact_triggered) {
      // Add base level size to compaction if level0 compaction triggered.
      estimated_compaction_needed_bytes_ += level_size;
    }
    // Add size added by previous compaction
    level_size += bytes_compact_to_next_level;
    bytes_compact_to_next_level = 0;
    uint64_t level_target = MaxBytesForLevel(level);
    if (level_size > level_target) {
      bytes_compact_to_next_level = level_size - level_target;
      // Estimate the actual compaction fan-out ratio as size ratio between
      // the two levels.

      assert(bytes_next_level == 0);
      if (level + 1 < num_levels_) {
        for (auto* f : files_[level + 1]) {
          bytes_next_level += f->fd.GetFileSize();
        }
      }
      if (bytes_next_level > 0) {
        assert(level_size > 0);
        estimated_compaction_needed_bytes_ += static_cast<uint64_t>(
            static_cast<double>(bytes_compact_to_next_level) *
            (static_cast<double>(bytes_next_level) /
                 static_cast<double>(level_size) +
             1));
      }
    }
  }
}

namespace {
uint32_t GetExpiredTtlFilesCount(const ImmutableOptions& ioptions,
                                 const MutableCFOptions& mutable_cf_options,
                                 const std::vector<FileMetaData*>& files) {
  uint32_t ttl_expired_files_count = 0;

  int64_t _current_time;
  auto status = ioptions.clock->GetCurrentTime(&_current_time);
  if (status.ok()) {
    const uint64_t current_time = static_cast<uint64_t>(_current_time);
    for (FileMetaData* f : files) {
      if (!f->being_compacted) {
        uint64_t oldest_ancester_time = f->TryGetOldestAncesterTime();
        if (oldest_ancester_time != 0 &&
            oldest_ancester_time < (current_time - mutable_cf_options.ttl)) {
          ttl_expired_files_count++;
        }
      }
    }
  }
  return ttl_expired_files_count;
}

bool ShouldChangeFileTemperature(const ImmutableOptions& ioptions,
                                 const MutableCFOptions& mutable_cf_options,
                                 const std::vector<FileMetaData*>& files) {
  const std::vector<FileTemperatureAge>& ages =
      mutable_cf_options.compaction_options_fifo
          .file_temperature_age_thresholds;
  if (ages.empty()) {
    return false;
  }
  if (files.empty()) {
    return false;
  }
  int64_t _current_time;
  auto status = ioptions.clock->GetCurrentTime(&_current_time);
  const uint64_t current_time = static_cast<uint64_t>(_current_time);
  // We use oldest_ancestor_time of a file to be the estimate age of
  // the file just older than it. This is the same logic used in
  // FIFOCompactionPicker::PickTemperatureChangeCompaction().
  if (status.ok() && current_time >= ages[0].age) {
    uint64_t create_time_threshold = current_time - ages[0].age;
    Temperature target_temp;
    assert(files.size() >= 1);
    for (size_t index = files.size() - 1; index >= 1; --index) {
      FileMetaData* cur_file = files[index];
      FileMetaData* prev_file = files[index - 1];
      if (!cur_file->being_compacted) {
        uint64_t oldest_ancestor_time = prev_file->TryGetOldestAncesterTime();
        if (oldest_ancestor_time == kUnknownOldestAncesterTime) {
          return false;
        }
        if (oldest_ancestor_time > create_time_threshold) {
          return false;
        }
        target_temp = ages[0].temperature;
        for (size_t i = 1; i < ages.size(); ++i) {
          if (current_time >= ages[i].age &&
              oldest_ancestor_time <= current_time - ages[i].age) {
            target_temp = ages[i].temperature;
          }
        }
        if (cur_file->temperature != target_temp) {
          return true;
        }
      }
    }
  }
  return false;
}
}  // anonymous namespace

/**
 * @brief 获取指定Level的最大目标字节数
 *
 * 该值在ComputeCompactionScore中用于计算Compaction分数：
 * - score = level实际大小 / MaxBytesForLevel(level)
 * - 当 score >= 1.0 时触发Compaction
 *
 * level_max_bytes_ 的计算在 CalculateBaseBytes 中完成：
 * - 静态模式：L0/L1 = max_bytes_for_level_base, L2+按倍数递增
 * - 动态模式：根据实际数据大小动态计算
 *
 * @param level 层级编号（0为L0）
 * @return 该层的最大目标字节数
 *
 * @note L0层的阈值主要基于文件数量而非字节数
 */
uint64_t VersionStorageInfo::MaxBytesForLevel(int level) const {
  // Note: the result for level zero is not really used since we set
  // level-0 compaction threshold based on number of files.
  // 注意：level 0的结果并未真正使用，因为我们基于文件数量设置L0压缩阈值。
  assert(level >= 0);
  assert(level < static_cast<int>(level_max_bytes_.size()));
  return level_max_bytes_[level];
}

/**
 * @brief 计算各层的Compaction分数
 *
 * 该函数是Level层满触发Compaction的核心逻辑：
 * 1. 遍历所有层级（L0到MaxInputLevel）
 * 2. 对每层计算Compaction分数
 * 3. 分数 >= 1.0 表示该层需要Compaction
 *
 * 分数计算规则：
 * - L0层：基于文件数量或大小（取决于Compaction风格）
 * - L1+层：score = 实际大小 / MaxBytesForLevel(level)
 *
 * 触发阈值：score >= 1.0
 */
/**
 * @brief 计算所有层级的 compaction 分数并排序
 *
 * 本函数是 Level Compaction 触发的核心逻辑，负责：
 * 1. 遍历所有层级，计算每个层的 compaction 分数
 * 2. 区分 L0 和其他层的计算方式
 * 3. 根据分数对层级进行排序，分数高的优先 compact
 * 4. 处理 TTL compaction、周期性 compaction、Blob GC 等特殊情况
 *
 * @param immutable_options 不可变配置（如 compaction_style、allow_ingest_behind）
 * @param mutable_cf_options 可变配置（如 max_bytes_for_level_base、ttl）
 *
 * @note Compaction Score 的含义：
 *   - score >= 1.0：该层需要 compaction
 *   - score < 1.0：该层暂不需要 compaction
 *   - 分数越高，优先级越高
 *   - 如果 score > 1.0，可能乘以 10 倍（kScoreScale）以便排序
 *
 * @note 计算方式：
 *   - L0 层：score = 文件数 / level0_file_num_compaction_trigger
 *   - L1+ 层（静态模式）：score = 层大小 / MaxBytesForLevel(level)
 *   - L1+ 层（动态模式）：score = 层大小 / (MaxBytesForLevel + 传入数据)
 *
 * @note 动态模式的影响：
 *   - 当有大量数据正在 compact 到某层时，会降低该层的 score
 *   - 避免在大量数据传入时进行不必要的 compaction
 *   - 传入数据由 total_downcompact_bytes 累计
 *
 * @note L0 层的特殊处理：
 *   - 考虑文件数而非字节数（避免过多小文件）
 *   - 动态模式时考虑 L0 与 base level 的大小关系
 *   - 避免 L0 积累过多数据导致写 stall
 *
 * @note 排序规则：
 *   - 使用冒泡排序对层级的 score 进行降序排列
 *   - 排序后：compaction_score_[0] 最高，compaction_level_[0] 对应的层
 *   - LevelCompactionPicker 按照 score 从高到低的顺序选择层
 *
 * @note 特殊情况：
 *   - TTL 过期文件：单独计算 score
 *   - 周期性 compaction：强制标记需要 compact
 *   - Blob 垃圾回收：标记需要回收的文件
 *   - FIFO compaction：特殊的 score 计算方式
 *   - Universal compaction：特殊的 L0 score 计算方式
 *
 * @note 相关成员变量：
 *   - compaction_score_[]：各层的 compaction 分数（降序排列）
 *   - compaction_level_[]：各层的层级号（降序排列）
 *   - lowest_unnecessary_level_：最低的不必要层级（用于降优先）
 *   - files_marked_for_compaction_：标记为需要 compact 的文件
 */
void VersionStorageInfo::ComputeCompactionScore(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options) {
  // 累计所有层级待压缩的字节数，用于动态模式下的分数调整
  double total_downcompact_bytes = 0.0;
  // Historically, score is defined as actual bytes in a level divided by
  // the level's target size, and 1.0 is the threshold for triggering
  // compaction. Higher score means higher prioritization.
  // Now we keep the compaction triggering condition, but consider more
  // factors for prioritization, while still keeping the 1.0 threshold.
  // In order to provide flexibility for reducing score while still
  // maintaining it to be over 1.0, we scale the original score by 10x
  // if it is larger than 1.0.
  // 分数缩放系数：当分数 > 1.0 时，乘以 10 以便在排序时区分优先级
  const double kScoreScale = 10.0;
  // 计算最大输出层级（如果允许 ingest_behind，则最大层级减一）
  int max_output_level = MaxOutputLevel(immutable_options.allow_ingest_behind);
  // 遍历所有层级（从 L0 到最大输入层），为每层计算 compaction 分数
  for (int level = 0; level <= MaxInputLevel(); level++) {
    double score;
    if (level == 0) {
      // L0 层的特殊处理
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      int num_sorted_runs = 0;  // L0 层未在压缩中的有序运行数量（即文件数）
      uint64_t total_size = 0;  // L0 层未在压缩中的总大小
      // 遍历 L0 层的所有文件
      for (auto* f : files_[level]) {
        // 累计文件大小到 total_downcompact_bytes（用于动态模式调整）
        total_downcompact_bytes += static_cast<double>(f->fd.GetFileSize());
        // 只计算未被压缩的文件（避免重复计算）
        if (!f->being_compacted) {
          total_size += f->compensated_file_size;
          num_sorted_runs++;
        }
      }
      // Universal Compaction 的特殊处理
      if (compaction_style_ == kCompactionStyleUniversal) {
        // For universal compaction, we use level0 score to indicate
        // compaction score for the whole DB. Adding other levels as if
        // they are L0 files.
        // 将其他层级也视为 L0 文件，计算整个 DB 的 compaction 分数
        for (int i = 1; i <= max_output_level; i++) {
          // It's possible that a subset of the files in a level may be in a
          // compaction, due to delete triggered compaction or trivial move.
          // In that case, the below check may not catch a level being
          // compacted as it only checks the first file. The worst that can
          // happen is a scheduled compaction thread will find nothing to do.
          // 只检查第一个文件是否在压缩中（简化处理）
          if (!files_[i].empty() && !files_[i][0]->being_compacted) {
            num_sorted_runs++;  // 将整个层视为一个有序运行
          }
        }
      }

      // FIFO Compaction 的特殊处理
      if (compaction_style_ == kCompactionStyleFIFO) {
        // FIFO 的分数 = L0 总大小 / 最大文件大小限制
        score = static_cast<double>(total_size) /
                mutable_cf_options.compaction_options_fifo.max_table_files_size;
        // 如果分数 < 1 但允许 compaction，则考虑文件数触发
        if (score < 1 &&
            mutable_cf_options.compaction_options_fifo.allow_compaction) {
          // 取文件数分数和大小分数的最大值
          score = std::max(
              static_cast<double>(num_sorted_runs) /
                  mutable_cf_options.level0_file_num_compaction_trigger,
              score);
        }
        // 如果分数 < 1 但配置了 TTL，则考虑 TTL 过期文件
        if (score < 1 && mutable_cf_options.ttl > 0) {
          // 取 TTL 过期分数和当前分数的最大值
          score =
              std::max(static_cast<double>(GetExpiredTtlFilesCount(
                           immutable_options, mutable_cf_options, files_[0])),
                       score);
        }
        // 如果分数 < 1 但需要改变文件温度，则设置一个足够高的分数
        if (score < 1 &&
            ShouldChangeFileTemperature(immutable_options, mutable_cf_options,
                                        files_[0])) {
          // For FIFO, just need a large enough score to trigger compaction.
          const double kScoreForNeedCompaction = 1.1;
          score = kScoreForNeedCompaction;  // 设置为 1.1 以触发 compaction
        }
      } else {
        // 非 FIFO 模式（Level 和 Universal），基于文件数计算分数
        score = static_cast<double>(num_sorted_runs) /
                mutable_cf_options.level0_file_num_compaction_trigger;
        // Level Compaction 的额外处理
        if (compaction_style_ == kCompactionStyleLevel && num_levels() > 1) {
          // Level-based involves L0->L0 compactions that can lead to oversized
          // L0 files. Take into account size as well to avoid later giant
          // compactions to the base level.
          // If score in L0 is always too high, L0->LBase will always be
          // prioritized over LBase->LBase+1 compaction and LBase will
          // accumulate to too large. But if L0 score isn't high enough, L0 will
          // accumulate and data is not moved to LBase fast enough. The score
          // calculation below takes into account L0 size vs LBase size.
          // 动态字节模式处理
          if (immutable_options.level_compaction_dynamic_level_bytes) {
            // 如果 L0 大小 >= base level 的目标大小，强制分数 > 1.0
            if (total_size >= mutable_cf_options.max_bytes_for_level_base) {
              // When calculating estimated_compaction_needed_bytes, we assume
              // L0 is qualified as pending compactions. We will need to make
              // sure that it qualifies for compaction.
              // It might be guaranteed by logic below anyway, but we are
              // explicit here to make sure we don't stop writes with no
              // compaction scheduled.
              score = std::max(score, 1.01);  // 确保分数 > 1.0
            }
            // 如果 L0 大小 > base level 的实际最大字节数，提高 L0 分数
            if (total_size > level_max_bytes_[base_level_]) {
              // In this case, we compare L0 size with actual LBase size and
              // make sure score is more than 1.0 (10.0 after scaled) if L0 is
              // larger than LBase. Since LBase score = LBase size /
              // (target size + total_downcompact_bytes) where
              // total_downcompact_bytes = total_size > LBase size,
              // LBase score is lower than 10.0. So L0->LBase is prioritized
              // over LBase -> LBase+1.
              // 计算 base level 的实际大小
              uint64_t base_level_size = 0;
              for (auto f : files_[base_level_]) {
                base_level_size += f->compensated_file_size;
              }
              // L0 分数 = max(当前分数, L0大小 / max(base level大小, base level目标大小))
              score = std::max(score, static_cast<double>(total_size) /
                                          static_cast<double>(std::max(
                                              base_level_size,
                                              level_max_bytes_[base_level_])));
            }
            // 如果分数 > 1.0，乘以 kScoreScale 以提高优先级
            if (score > 1.0) {
              score *= kScoreScale;
            }
          } else {
            // 静态模式：L0 分数 = max(文件数分数, L0大小 / base level目标大小)
            score = std::max(score,
                             static_cast<double>(total_size) /
                                 mutable_cf_options.max_bytes_for_level_base);
          }
        }
      }
    } else {  // level > 0，处理 L1 及以上层级
      // Compute the ratio of current size to size limit.
      // 计算当前大小与大小限制的比率（核心的Level层满逻辑）
      uint64_t level_bytes_no_compacting = 0; // 该层未在压缩中的文件总大小
      uint64_t level_total_bytes = 0; // 该层文件总大小
      // 遍历该层所有文件
      for (auto f : files_[level]) {
        // 累计文件总大小（包括正在压缩的文件）
        level_total_bytes += f->fd.GetFileSize();
        // 累计未在压缩中的文件大小（用于计算分数）
        if (!f->being_compacted) {
          level_bytes_no_compacting += f->compensated_file_size;
        }
      }
      // 静态字节模式
      if (!immutable_options.level_compaction_dynamic_level_bytes) {
        // 非动态模式：分数 = 实际大小 / 目标大小，当分数>=1时触发Compaction
        score = static_cast<double>(level_bytes_no_compacting) /
                MaxBytesForLevel(level);
      } else {
        // 动态字节模式：考虑正在从上层传来的数据
        if (level_bytes_no_compacting < MaxBytesForLevel(level)) {
          // 如果未达到目标大小，正常计算分数
          score = static_cast<double>(level_bytes_no_compacting) /
                  MaxBytesForLevel(level);
        } else {
          // If there are a large mount of data being compacted down to the
          // current level soon, we would de-prioritize compaction from
          // a level where the incoming data would be a large ratio. We do
          // it by dividing level size not by target level size, but
          // the target size and the incoming compaction bytes.
          // 如果有大量数据即将压缩到该层，降低该层的优先级
          // 分数 = 层大小 / (目标大小 + 传入数据) * kScoreScale
          score = static_cast<double>(level_bytes_no_compacting) /
                  (MaxBytesForLevel(level) + total_downcompact_bytes) *
                  kScoreScale;
        }
        // Drain unnecessary levels, but with lower priority compared to
        // when L0 is eligible. Only non-empty levels can be unnecessary.
        // If there is no unnecessary levels, lowest_unnecessary_level_ = -1.
        // 逐步降低不必要的层级（L0 有资格时优先级更高）
        if (level_bytes_no_compacting > 0 &&
            level <= lowest_unnecessary_level_) {
          // 为不必要的层级设置一个高于 1.0 的分数，但越低层分数越高
          score = std::max(
              score, kScoreScale *
                         (1.001 + 0.001 * (lowest_unnecessary_level_ - level)));
        }
      }
      // 累计 total_downcompact_bytes，用于动态模式调整后续层的分数
      if (level <= lowest_unnecessary_level_) {
        // 如果是不必要的层级，累计所有数据
        total_downcompact_bytes += level_total_bytes;
      } else if (level_total_bytes > MaxBytesForLevel(level)) {
        // 如果是必要层级但超过目标大小，累计超出的部分
        total_downcompact_bytes +=
            static_cast<double>(level_total_bytes - MaxBytesForLevel(level));
      }
      }
    }
    // 保存该层的层级号到 compaction_level_ 数组
    compaction_level_[level] = level;
    // 保存该层的 compaction 分数到 compaction_score_ 数组
    compaction_score_[level] = score;
  }

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  // 使用冒泡排序对层级按分数降序排列（层级数少，冒泡排序足够高效）
  for (int i = 0; i < num_levels() - 2; i++) {
    for (int j = i + 1; j < num_levels() - 1; j++) {
      // 如果前一个分数 < 后一个分数，则交换（降序排序）
      if (compaction_score_[i] < compaction_score_[j]) {
        double score = compaction_score_[i];  // 临时保存分数
        int level = compaction_level_[i];    // 临时保存层级号
        compaction_score_[i] = compaction_score_[j];  // 交换分数
        compaction_level_[i] = compaction_level_[j];  // 交换层级号
        compaction_score_[j] = score;  // 恢复分数
        compaction_level_[j] = level;  // 恢复层级号
      }
    }
  }
  // 计算被标记为需要 compaction 的文件
  ComputeFilesMarkedForCompaction(max_output_level);
  // 如果不允许 ingest_behind，则计算底层需要 compaction 的文件
  if (!immutable_options.allow_ingest_behind) {
    ComputeBottommostFilesMarkedForCompaction();
  }
  // 如果配置了 TTL 且使用 Level compaction，计算过期的 TTL 文件
  if (mutable_cf_options.ttl > 0 &&
      compaction_style_ == kCompactionStyleLevel) {
    ComputeExpiredTtlFiles(immutable_options, mutable_cf_options.ttl);
  }
  // 如果配置了周期性 compaction，计算需要周期性 compact 的文件
  if (mutable_cf_options.periodic_compaction_seconds > 0) {
    ComputeFilesMarkedForPeriodicCompaction(
        immutable_options, mutable_cf_options.periodic_compaction_seconds,
        max_output_level);
  }

  // 如果启用了 Blob 垃圾回收且满足条件，计算需要强制 Blob GC 的文件
  if (mutable_cf_options.enable_blob_garbage_collection &&
      mutable_cf_options.blob_garbage_collection_age_cutoff > 0.0 &&
      mutable_cf_options.blob_garbage_collection_force_threshold < 1.0) {
    ComputeFilesMarkedForForcedBlobGC(
        mutable_cf_options.blob_garbage_collection_age_cutoff,
        mutable_cf_options.blob_garbage_collection_force_threshold);
  }

  // 估计需要 compaction 的字节数（用于写停止控制）
  EstimateCompactionBytesNeeded(mutable_cf_options);
}

void VersionStorageInfo::ComputeFilesMarkedForCompaction(int last_level) {
  files_marked_for_compaction_.clear();
  int last_qualify_level = 0;

  // Do not include files from the last level with data
  // If table properties collector suggests a file on the last level,
  // we should not move it to a new level.
  for (int level = last_level; level >= 1; level--) {
    if (!files_[level].empty()) {
      last_qualify_level = level - 1;
      break;
    }
  }

  for (int level = 0; level <= last_qualify_level; level++) {
    for (auto* f : files_[level]) {
      if (!f->being_compacted && f->marked_for_compaction) {
        files_marked_for_compaction_.emplace_back(level, f);
      }
    }
  }
}

/**
 * @brief 计算哪些文件因 TTL（Time To Live）过期而需要压缩
 *
 * 本函数的作用：
 * 1. 根据 TTL 配置，标记包含过期数据的文件
 * 2. TTL 是一种基于数据生存时间的自动数据过期机制
 * 3. 过期数据会通过压缩被删除或重写
 *
 * 核心概念：
 *
 * 1. TTL（Time To Live）：
 *    - 定义：数据的生存时间，超过此时间的数据被视为过期
 *    - 单位：秒（seconds）
 *    - 作用：
 *      a. 自动清理过期数据
 *      b. 减少存储空间占用
 *      c. 提高查询性能（减少需要扫描的数据量）
 *      d. 实现数据生命周期管理
 *    - 配置：通过 ColumnFamilyOptions.ttl 设置
 *    - 默认值：0（禁用）
 *
 * 2. TTL vs Periodic Compaction：
 *    - TTL：基于数据插入时间的生存期，数据真正过期后才删除
 *      a. 关注点：数据的有效性
 *      b. 目的：清理不再有效的数据
 *      c. 适用场景：有时间敏感性的数据（如日志、临时数据）
 *    - Periodic Compaction：基于文件年龄的定期压缩，无论数据是否过期
 *      a. 关注点：文件的年龄
 *      b. 目的：回收空间、优化布局
 *      c. 适用场景：通用场景，确保文件定期重写
 *    - 关键区别：
 *      a. TTL：数据真的过期了（插入时间 + TTL < 当前时间）
 *      b. Periodic：文件老了（创建时间 + 间隔 < 当前时间），但数据可能仍有效
 *
 * 3. oldest_ancester_time（最祖先文件时间）：
 *    - 定义：文件来源的最原始文件的创建时间
 *    - 作用：追踪文件的历史和数据链
 *    - 特点：
 *      a. 如果文件是通过 compaction 产生的，这个时间保持不变
 *      b. 可以追溯到数据的最初插入时间
 *      c. 即使文件经过多次 compaction，时间戳不变
 *    - 与 TTL 的关系：
 *      a. TTL 是基于数据插入时间的
 *      b. oldest_ancester_time 保留了最初的插入时间
 *      c. 可以准确判断数据是否过期
 *
 * 4. 为什么使用 oldest_ancester_time：
 *    a. 准确性：
 *       - 保留数据的最初插入时间
 *       - 即使经过多次 compaction，也能准确判断过期
 *    b. 一致性：
 *       - 同一批次的数据有相同的 oldest_ancester_time
 *       - 确保过期判断的一致性
 *    c. 效率：
 *       - 存储在 table properties 中，读取快速
 *       - 不需要额外的 I/O 操作
 *
 * 5. 为什么只处理非最底层（num_levels() - 1）：
 *    a. 最底层（bottom level）的特性：
 *       - 数据不会被更底层的文件覆盖
 *       - 是数据的最终存储位置
 *       - 通常不会被再次 compaction（除非满足其他条件）
 *    b. 非最底层：
 *       - 数据会被移动到更底层
 *       - 过期数据会在后续的 compaction 中自然删除
 *       - 不需要专门的 TTL 压缩
 *    c. 性能考虑：
 *       - 减少需要标记的文件数量
 *       - 避免不必要的压缩
 *       - 让正常的 compaction 流程处理过期数据
 *
 * 6. TTL 压缩与 Leveled Compaction 的关系：
 *    - Leveled compaction 的特性：
 *      a. 数据从上层移动到下层
 *      b. 每一层都有大小限制
 *      c. 压缩会自然地移动过期数据
 *    - TTL 压缩的作用：
 *      a. 加速过期数据的清理
 *      b. 在非最底层触发压缩，将过期数据向下移动
 *      c. 最终在最底层被删除
 *    - 级联效果：
 *      a. 上层文件被压缩
 *      b. 过期数据移动到下层
 *      c. 下层也可能被触发 TTL 压缩
 *      d. 最终过期数据到达最底层并被删除
 *
 * 7. being_compacted 的检查：
 *    - 如果文件正在被压缩，不能再次标记
 *    - 避免重复压缩同一个文件
 *    - 等待当前压缩完成后再次检查
 *
 * 算法流程：
 * 1. 清空已标记文件列表
 * 2. 断言 TTL > 0（确保 TTL 有效）
 * 3. 获取当前时间（从系统时钟）
 * 4. 如果获取时间失败，直接返回
 * 5. 计算过期阈值：current_time - ttl
 *    - 如果 oldest_ancester_time < 过期阈值，说明数据过期
 * 6. 遍历所有层级（除了最底层）：
 *    a. 遍历当前层级的所有文件
 *    b. 跳过正在压缩的文件
 *    c. 获取文件的 oldest_ancester_time
 *    d. 如果 oldest_ancester_time > 0 且 < 过期阈值，标记为过期
 * 7. 完成后，expired_ttl_files_ 包含所有包含过期数据的文件
 *
 * 应用场景：
 * 1. 时间敏感数据：
 *    - 日志数据：只保留最近 N 天
 *    - 临时数据：过期后自动删除
 *    - 缓存数据：有固定的生存时间
 *
 * 2. 存储空间管理：
 *    - 自动清理不再需要的数据
 *    - 减少磁盘占用
 *    - 降低存储成本
 *
 * 3. 数据生命周期管理：
 *    - 确保数据不会永久存储
 *    - 符合数据保留策略
 *    - 满足合规性要求（如 GDPR）
 *
 * 4. 性能优化：
 *    - 减少需要扫描的数据量
 *    - 提高查询速度
 *    - 降低 I/O 开销
 *
 * 性能考虑：
 * 1. 最底层不处理：
 *    - 减少需要标记的文件数量
 *    - 让正常的 compaction 流程处理
 *    - 避免不必要的压缩
 *
 * 2. 批量计算：
 *    - 在 VersionStorageInfo 初始化时一次性计算
 *    - 避免频繁计算
 *    - 提高效率
 *
 * 3. 快速时间获取：
 *    - 使用 table properties 中的 oldest_ancester_time
 *    - 避免系统调用
 *    - 提高性能
 *
 * 注意事项：
 * 1. 时钟同步：
 *    - 依赖系统时钟的准确性
 *    - NTP 同步可能导致时间跳变
 *    - 需要处理时间跳变的情况
 *
 * 2. TTL 的范围：
 *    - 只影响非最底层
 *    - 最底层的过期数据需要其他方式处理
 *    - 可以通过 periodic compaction 或 manual compaction
 *
 * 3. 数据一致性：
 *    - oldest_ancester_time 保持不变，即使经过多次 compaction
 *    - 确保过期判断的准确性
 *    - 避免数据意外删除
 *
 * 4. 配置建议：
 *    - TTL 应该根据业务需求设置
 *    - 过短：频繁压缩，影响性能
 *    - 过长：数据堆积，占用空间
 *    - 建议值：7-30 天（取决于业务场景）
 *
 * 5. 与其他压缩策略的配合：
 *    - TTL 压缩可以与 size-based compaction 配合
 *    - 优先级：size-based > TTL > periodic
 *    - 确保多种压缩策略协同工作
 *
 * @param ioptions 不可变选项（包含时钟、环境、日志等）
 * @param ttl 生存时间（秒），表示数据的最大生存期
 *
 * @note TTL 是可选的，通过配置启用
 * @note 只处理非最底层（num_levels() - 1）的文件
 * @note 过期数据会在 compaction 中被删除或重写
 * @note 此函数在 VersionStorageInfo 初始化时调用
 * @see expired_ttl_files_ 过期文件列表
 * @see ttl TTL 配置参数
 * @see oldest_ancester_time 最祖先文件时间
 */
void VersionStorageInfo::ComputeExpiredTtlFiles(
    const ImmutableOptions& ioptions, const uint64_t ttl) {
  // 断言 TTL 必须大于 0
  // 如果 TTL = 0，表示禁用 TTL，不应该调用此函数
  assert(ttl > 0);

  // 清空之前计算的过期文件列表
  // 每次重新计算时，先清空结果
  expired_ttl_files_.clear();

  // 获取当前时间（从系统时钟）
  // ioptions.clock 是一个时钟接口，可以获取当前时间
  // int64_t 类型，用于存储 Unix 时间戳（秒）
  int64_t _current_time;
  // 调用时钟接口获取当前时间
  auto status = ioptions.clock->GetCurrentTime(&_current_time);
  // 如果获取时间失败，直接返回
  // 可能的原因：时钟接口错误、系统问题等
  if (!status.ok()) {
    return;
  }
  // 将时间转换为 uint64_t 类型，用于后续计算
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  // 遍历所有层级（除了最底层）
  // num_levels() - 1 表示不处理最底层
  // 原因：
  // 1. 最底层的数据不会被更底层的文件覆盖
  // 2. 过期数据会在正常的 compaction 流程中自然删除
  // 3. 减少需要标记的文件数量，提高效率
  for (int level = 0; level < num_levels() - 1; level++) {
    // 遍历当前层级的所有文件
    for (FileMetaData* f : files_[level]) {
      // 检查文件是否正在被压缩
      // 如果正在被压缩，跳过该文件，避免重复压缩
      if (!f->being_compacted) {
        // 获取文件的最祖先时间（oldest_ancester_time）
        // oldest_ancester_time 表示数据最初插入的时间
        // 即使文件经过多次 compaction，这个时间保持不变
        // 用于准确判断数据是否过期
        uint64_t oldest_ancester_time = f->TryGetOldestAncesterTime();
        // 检查数据是否过期
        // 条件 1：oldest_ancester_time > 0
        //   - 确保时间戳有效（不为 0）
        //   - 0 表示未知时间，不应该被标记
        // 条件 2：oldest_ancester_time < (current_time - ttl)
        //   - 数据的插入时间 < 当前时间 - TTL
        //   - 即：插入时间 + TTL < 当前时间
        //   - 说明数据已经过期
        if (oldest_ancester_time > 0 &&
            oldest_ancester_time < (current_time - ttl)) {
          // 将文件添加到过期列表中
          // 记录层级和文件元数据指针
          // 这个列表将被 CompactionPicker 使用，选择需要 TTL 压缩的文件
          expired_ttl_files_.emplace_back(level, f);
        }
      }
    }
  }
}

/**
 * @brief 计算哪些文件需要周期性压缩（Periodic Compaction）
 *
 * 本函数的作用：
 * 1. 根据文件的创建时间或修改时间，标记需要压缩的文件
 * 2. 周期性压缩是一种基于时间的压缩策略，确保文件定期被重写
 * 3. 即使文件没有被覆盖或删除，也会被压缩以回收空间
 *
 * 核心概念：
 *
 * 1. 周期性压缩（Periodic Compaction）：
 *    - 定义：按照固定的时间间隔对文件进行压缩
 *    - 目的：
 *      a. 确保文件定期被重写，避免文件过大
 *      b. 回收磁盘空间（即使文件没有被覆盖）
 *      c. 优化文件布局和压缩比
 *      d. 清除过期的 tombstone（标记删除）
 *    - 配置：通过 periodic_compaction_seconds 参数设置
 *
 * 2. 文件修改时间的确定顺序（优先级从高到低）：
 *    a. file_creation_time（文件创建时间）：
 *       - 存储在 table properties 中
 *       - RocksDB 6.18+ 开始支持
 *       - 记录文件实际创建的 Unix 时间戳
 *       - 精度：秒级
 *
 *    b. oldest_ancester_time（最祖先文件时间）：
 *       - 记录文件来源的最原始文件的创建时间
 *       - 用于追踪文件的历史
 *       - 如果文件是通过 compaction 产生的，这个时间保持不变
 *
 *    c. 文件系统修改时间（mtime）：
 *       - 从文件系统获取（通过 env->GetFileModificationTime）
 *       - 可能耗时（系统调用）
 *       - 可能不准确（文件移动、复制等操作）
 *
 * 3. 为什么要优先使用 table properties 中的时间：
 *    a. 性能：避免系统调用
 *    b. 准确性：不受文件操作（移动、复制）影响
 *    c. 一致性：即使文件被压缩，时间戳保持不变
 *
 * 4. being_compacted 的检查：
 *    - 如果文件正在被压缩，不能再次标记
 *    - 避免重复压缩同一个文件
 *    - 防止资源浪费和冲突
 *
 * 5. 为什么需要检查 periodic_compaction_seconds > current_time：
 *    - 避免 uint64_t 下溢（current_time - periodic_compaction_seconds）
 *    - 系统时钟可能被调整（如 NTP 同步）
 *    - 这种情况下，暂时跳过周期性压缩
 *
 * 算法流程：
 * 1. 清空已标记文件列表
 * 2. 获取当前时间（从系统时钟）
 * 3. 计算时间阈值：current_time - periodic_compaction_seconds
 * 4. 遍历指定层级（0 到 last_level）的所有文件：
 *    a. 跳过正在压缩的文件
 *    b. 按优先级获取文件修改时间：
 *       - 首先尝试 file_creation_time
 *       - 然后尝试 oldest_ancester_time
 *       - 最后从文件系统获取 mtime
 *    c. 如果获取失败，跳过该文件
 *    d. 如果文件修改时间 > 0 且 < 时间阈值，标记为需要压缩
 * 5. 完成后，files_marked_for_periodic_compaction_ 包含所有需要压缩的文件
 *
 * 应用场景：
 * 1. 回收过期数据：
 *    - 即使文件没有被覆盖，周期性压缩也会重写文件
 *    - 可以删除过期的 tombstone（标记删除）
 *    - 可以应用新的压缩算法或配置
 *
 * 2. 防止文件无限增长：
 *    - 即使写入量很小，文件也会定期被重写
 *    - 避免文件变得过大
 *    - 保持文件大小合理
 *
 * 3. 优化压缩比：
 *    - 随着时间推移，数据分布可能变化
 *    - 重新压缩可能获得更好的压缩比
 *    - 特别是使用新的压缩算法时
 *
 * 4. 磁盘空间回收：
 *    - SSTable 文件可能有空洞（被删除的数据）
 *    - 周期性压缩可以回收这些空间
 *    - 即使没有新数据覆盖旧数据
 *
 * 性能考虑：
 * 1. 优先使用 table properties 中的时间：
 *    - 避免系统调用
 *    - 提高查找速度
 *    - 减少 I/O 开销
 *
 * 2. 只在需要时获取文件系统时间：
 *    - 前两种方式失败时才使用
 *    - 减少系统调用次数
 *    - 提高性能
 *
 * 3. 批量计算：
 *    - 在 VersionStorageInfo 初始化时一次性计算
 *    - 避免频繁计算
 *    - 提高效率
 *
 * 注意事项：
 * 1. 时钟同步：
 *    - 依赖系统时钟的准确性
 *    - NTP 同步可能导致时间跳变
 *    - 需要处理时间跳变的情况
 *
 * 2. 文件系统时间：
 *    - 可能不准确（文件移动、复制等操作）
 *    - 系统调用可能耗时
 *    - 不同文件系统可能表现不同
 *
 * 3. 周期性压缩的代价：
 *    - 会消耗 CPU 和 I/O 资源
 *    - 即使文件没有被覆盖，也会被压缩
 *    - 需要合理设置 periodic_compaction_seconds
 *
 * @param ioptions 不可变选项（包含时钟、环境、日志等）
 * @param periodic_compaction_seconds 周期性压缩的时间间隔（秒）
 * @param last_level 需要检查的最高层级
 *
 * @note 周期性压缩是可选的，通过配置启用
 * @note 即使文件没有达到 size-based 压缩条件，也会被压缩
 * @note 此函数在 VersionStorageInfo 初始化时调用
 * @see files_marked_for_periodic_compaction_ 标记文件列表
 * @see periodic_compaction_seconds 配置参数
 */
void VersionStorageInfo::ComputeFilesMarkedForPeriodicCompaction(
    const ImmutableOptions& ioptions,
    const uint64_t periodic_compaction_seconds, int last_level) {
  // 断言周期性压缩间隔必须大于 0
  // 如果为 0，表示不启用周期性压缩，不应该调用此函数
  assert(periodic_compaction_seconds > 0);

  // 清空之前计算的标记文件列表
  // 每次重新计算时，先清空结果
  files_marked_for_periodic_compaction_.clear();

  // 获取当前时间（从系统时钟）
  // ioptions.clock 是一个时钟接口，可以获取当前时间
  // int64_t 类型，用于存储 Unix 时间戳（秒）
  int64_t temp_current_time;
  // 调用时钟接口获取当前时间
  auto status = ioptions.clock->GetCurrentTime(&temp_current_time);
  // 如果获取时间失败，直接返回
  // 可能的原因：时钟接口错误、系统问题等
  if (!status.ok()) {
    return;
  }
  // 将时间转换为 uint64_t 类型，用于后续计算
  const uint64_t current_time = static_cast<uint64_t>(temp_current_time);

  // If periodic_compaction_seconds is larger than current time, periodic
  // compaction can't possibly be triggered.
  // 检查周期性压缩间隔是否大于当前时间
  // 如果 periodic_compaction_seconds > current_time，说明：
  // a. current_time - periodic_compaction_seconds 会下溢（变成很大的数）
  // b. 这种情况下无法正确判断哪些文件需要压缩
  // c. 可能的原因：系统时钟被调整（如 NTP 同步）
  // 直接返回，不进行标记
  if (periodic_compaction_seconds > current_time) {
    return;
  }

  // 计算允许的时间阈值
  // 文件修改时间 < allowed_time_limit 的文件需要被压缩
  // 即：文件的年龄 > periodic_compaction_seconds
  // 示例：current_time = 1000, periodic_compaction_seconds = 300
  //       allowed_time_limit = 1000 - 300 = 700
  //       修改时间为 500 的文件需要被压缩（500 < 700）
  //       修改时间为 800 的文件不需要被压缩（800 >= 700）
  const uint64_t allowed_time_limit =
      current_time - periodic_compaction_seconds;

  // 遍历所有层级（从 L0 到 last_level）
  for (int level = 0; level <= last_level; level++) {
    // 遍历当前层级的所有文件
    for (auto f : files_[level]) {
      // 检查文件是否正在被压缩
      // 如果正在被压缩，跳过该文件，避免重复压缩
      if (!f->being_compacted) {
        // Compute a file's modification time in the following order:
        // 1. Use file_creation_time table property if it is > 0.
        // 2. Use creation_time table property if it is > 0.
        // 3. Use file's mtime metadata if the above two table properties are 0.
        // Don't consider the file at all if the modification time cannot be
        // correctly determined based on the above conditions.
        // 获取文件修改时间（优先级 1：file_creation_time）
        // file_creation_time 存储在 table properties 中
        // 这是文件实际创建的 Unix 时间戳
        uint64_t file_modification_time = f->TryGetFileCreationTime();

        // 如果 file_creation_time 未知（kUnknownFileCreationTime = 0）
        // 尝试获取 oldest_ancester_time（优先级 2）
        // oldest_ancester_time 记录文件来源的最原始文件的创建时间
        // 用于追踪文件的历史，即使在 compaction 后也保持不变
        if (file_modification_time == kUnknownFileCreationTime) {
          file_modification_time = f->TryGetOldestAncesterTime();
        }

        // 如果 oldest_ancester_time 也未知（kUnknownOldestAncesterTime = 0）
        // 尝试从文件系统获取修改时间（优先级 3）
        // 这是最不推荐的方式，因为：
        // a. 需要系统调用，性能开销大
        // b. 可能不准确（文件移动、复制等操作会改变 mtime）
        // c. 不同文件系统可能表现不同
        if (file_modification_time == kUnknownOldestAncesterTime) {
          // 构造文件路径
          // TableFileName() 根据 cf_paths、文件号、路径 ID 生成完整路径
          auto file_path = TableFileName(ioptions.cf_paths, f->fd.GetNumber(),
                                         f->fd.GetPathId());
          // 调用环境接口获取文件修改时间
          // 这是一个系统调用，可能耗时
          status = ioptions.env->GetFileModificationTime(
              file_path, &file_modification_time);
          // 如果获取失败，记录警告并跳过该文件
          if (!status.ok()) {
            // 记录警告日志：无法获取文件修改时间
            // 包括文件路径和错误信息
            ROCKS_LOG_WARN(ioptions.logger,
                           "Can't get file modification time: %s: %s",
                           file_path.c_str(), status.ToString().c_str());
            // 跳过该文件，继续检查下一个文件
            continue;
          }
        }

        // 检查文件是否需要周期性压缩
        // 条件 1：file_modification_time > 0
        //   - 确保时间戳有效（不为 0）
        //   - 0 表示未知时间，不应该被压缩
        // 条件 2：file_modification_time < allowed_time_limit
        //   - 文件的年龄大于 periodic_compaction_seconds
        //   - 即文件创建时间早于阈值时间
        if (file_modification_time > 0 &&
            file_modification_time < allowed_time_limit) {
          // 将文件添加到标记列表中
          // 记录层级和文件元数据指针
          // 这个列表将被 CompactionPicker 使用，选择需要压缩的文件
          files_marked_for_periodic_compaction_.emplace_back(level, f);
        }
      }
    }
  }
}

/**
 * @brief 计算哪些 SSTable 文件需要强制 Blob 垃圾回收（GC）
 *
 * 本函数的作用：
 * 1. 根据 Blob 文件的垃圾比例和年龄，标记需要强制 GC 的 SSTable 文件
 * 2. Blob GC 可以回收 Blob 文件中被删除的 Blob 数据
 * 3. 通过压缩引用 Blob 文件的 SSTable，触发 Blob GC
 *
 * 核心概念：
 *
 * 1. Blob 文件（Blob File）：
 *    - 定义：存储大值（large value）的独立文件
 *    - 目的：减少 SSTable 文件的大小，提高压缩效率
 *    - 使用：当 value 大小超过 blob_garbage_collection_age_cutoff 时，存储到 Blob 文件
 *    - 引用：SSTable 文件包含指向 Blob 文件的引用
 *
 * 2. Blob 垃圾回收（Blob GC）：
 *    - 定义：回收 Blob 文件中被删除的 Blob 数据
 *    - 原因：
 *      a. 当 key 被删除或覆盖时，对应的 Blob 数据变成垃圾
 *      b. 这些垃圾数据占用磁盘空间，但不会被读取
 *      c. 需要通过 GC 回收这些空间
 *    - 触发方式：
 *      a. 自动 GC：根据垃圾比例自动触发
 *      b. 强制 GC：通过压缩 SSTable 强制触发
 *
 * 3. Linked SST（关联的 SSTable）：
 *    - 定义：引用某个 Blob 文件的 SSTable 文件集合
 *    - 作用：追踪哪些 SSTable 引用了哪些 Blob 文件
 *    - 数据结构：LinkedSsts = std::unordered_set<uint64_t>
 *    - 示例：Blob 文件 10 被 SST 文件 1 和 2 引用，LinkedSsts = {1, 2}
 *
 * 4. Blob Batch（Blob 批次）：
 *    - 定义：被同一组 SSTable 引用的 Blob 文件集合
 *    - 特点：
 *      a. 同一批次的 Blob 文件可以被一起 GC
 *      b. 通过压缩引用这批 Blob 文件的所有 SSTable 触发
 *      c. 确保所有相关的 Blob 数据都被处理
 *    - 示例：
 *      - Blob 文件 10 和 11 都被 SST 1 和 2 引用
 *      - 它们属于同一个批次
 *      - 压缩 SST 1 和 2 可以同时 GC Blob 文件 10 和 11
 *
 * 5. 垃圾比例（Garbage Ratio）：
 *    - 定义：垃圾字节数 / 总字节数
 *    - 计算公式：garbage_blob_bytes / total_blob_bytes
 *    - 阈值：blob_garbage_collection_force_threshold（如 0.5 表示 50%）
 *    - 作用：决定是否需要强制 GC
 *
 * 6. 年龄截止（Age Cutoff）：
 *    - 定义：Blob 文件的最老部分比例
 *    - 参数：blob_garbage_collection_age_cutoff（如 0.25 表示最老的 25%）
 *    - 计算：cutoff_count = age_cutoff * blob_files_.size()
 *    - 作用：限制 GC 只影响最老的 Blob 文件
 *
 * 为什么使用 Batch 概念：
 * 1. 一致性：
 *    - 确保 Blob 数据和 SSTable 数据的一致性
 *    - 避免部分 GC 导致的数据不一致
 *
 * 2. 效率：
 *    - 一次性压缩多个 SSTable，比逐个压缩更高效
 *    - 减少 I/O 和 CPU 开销
 *
 * 3. 完整性：
 *    - 确保所有相关的 Blob 数据都被处理
 *    - 避免遗漏某些 Blob 文件
 *
 * 算法流程：
 * 1. 清空已标记文件列表
 * 2. 检查是否有 Blob 文件，如果没有，直接返回
 * 3. 计算年龄截止：cutoff_count = age_cutoff * blob_files_count
 * 4. 如果 cutoff_count = 0，说明没有文件满足年龄条件，直接返回
 * 5. 找到最老的 Blob 批次：
 *    a. 从最老的 Blob 文件开始
 *    b. 如果下一个 Blob 文件的 LinkedSsts 不为空，说明是新批次的开始
 *    c. 统计当前批次的 total_blob_bytes 和 garbage_blob_bytes
 *    d. 如果当前批次的文件数 >= cutoff_count，停止
 * 6. 检查是否所有文件都满足年龄条件：
 *    a. 如果当前批次的文件数 < cutoff_count
 *    b. 且下一个 Blob 文件的 LinkedSsts 不为空
 *    c. 说明新批次的文件也满足年龄条件，无法确定边界
 *    d. 直接返回，不标记任何文件
 * 7. 检查垃圾比例：
 *    a. 计算 ratio = garbage_blob_bytes / total_blob_bytes
 *    b. 如果 ratio < blob_garbage_collection_force_threshold
 *    c. 说明垃圾比例不够高，不需要强制 GC
 *    d. 直接返回，不标记任何文件
 * 8. 标记需要压缩的 SSTable：
 *    a. 遍历最老批次的 LinkedSsts
 *    b. 查找每个 SSTable 的位置（层级和位置）
 *    c. 跳过正在压缩的 SSTable
 *    d. 将满足条件的 SSTable 添加到标记列表
 * 9. 完成后，files_marked_for_forced_blob_gc_ 包含所有需要压缩的 SSTable
 *
 * Batch 的判断逻辑：
 * - Blob 文件按创建时间排序（blob_files_）
 * - LinkedSsts 为空表示该 Blob 文件没有被 SSTable 引用（已被 GC）
 * - LinkedSsts 不为空表示该 Blob 文件被某些 SSTable 引用
 * - 同一批次的 Blob 文件有相同的 LinkedSsts
 * - 新批次的开始：LinkedSsts 从空变为非空，或者内容发生变化
 *
 * 示例说明：
 * 假设有 4 个 Blob 文件（10, 11, 12, 13）和 3 个 SST 文件（1, 2, 3）
 *
 * SST → Blob 映射：
 *   SST 1 引用 Blob 文件 10, 11
 *   SST 2 引用 Blob 文件 10, 11
 *   SST 3 引用 Blob 文件 12, 13
 *
 * Blob → SST 映射（LinkedSsts）：
 *   Blob 10: {1, 2}
 *   Blob 11: {1, 2}  （与 Blob 10 相同，属于同一批次）
 *   Blob 12: {3}       （不同的 LinkedSsts，新批次的开始）
 *   Blob 13: {3}       （与 Blob 12 相同，属于同一批次）
 *
 * 情况 1：blob_garbage_collection_age_cutoff = 0.5（50%）
 *   cutoff_count = 0.5 * 4 = 2
 *   最老批次包含 Blob 10 和 11（count = 2）
 *   Blob 12 的 LinkedSsts = {3} 不为空，是新批次的开始
 *   停止统计，count = 2
 *   检查垃圾比例，如果满足阈值，标记 SST 1 和 2
 *
 * 情况 2：blob_garbage_collection_age_cutoff = 0.75（75%）
 *   cutoff_count = 0.75 * 4 = 3
 *   统计 Blob 10, 11, 12（count = 3）
 *   Blob 13 的 LinkedSsts = {3} 不为空，是新批次的开始
 *   停止统计，count = 3
 *   但是 Blob 12 和 13 属于同一个批次（LinkedSsts 相同）
 *   意味着新批次的开始（Blob 12）也满足年龄条件
 *   无法确定批次边界，直接返回，不标记任何文件
 *
 * 应用场景：
 * 1. 回收大量 Blob 垃圾：
 *    - 当 Blob 文件中有大量被删除的数据时
 *    - 垃圾比例超过阈值
 *    - 强制 GC 可以显著减少磁盘占用
 *
 * 2. 优化存储空间：
 *    - Blob 文件可能包含大量无效数据
 *    - 通过 GC 回收这些空间
 *    - 提高磁盘利用率
 *
 * 3. 避免自动 GC 的延迟：
 *    - 自动 GC 可能等待垃圾比例更高
 *    - 强制 GC 可以提前触发
 *    - 及时回收空间
 *
 * 性能考虑：
 * 1. 批量 GC：
 *    - 一次性处理整个批次的 Blob 文件
 *    - 减少压缩次数
 *    - 提高效率
 *
 * 2. 年龄限制：
 *    - 只处理最老的 Blob 文件
 *    - 避免频繁 GC
 *    - 减少对性能的影响
 *
 * 3. 垃圾比例阈值：
 *    - 确保只在垃圾比例足够高时才 GC
 *    - 避免 GC 的开销超过收益
 *
 * 注意事项：
 * 1. Batch 的完整性：
 *    - 确保同一批次的 Blob 文件被一起 GC
 *    - 避免部分 GC 导致的数据不一致
 *
 * 2. 年龄边界的确定：
 *    - 如果新批次的开始也满足年龄条件
 *    - 无法确定批次边界
 *    - 暂时不标记任何文件，等待下次计算
 *
 * 3. 垃圾比例的计算：
 *    - 使用整个批次的平均垃圾比例
 *    - 而不是单个 Blob 文件的垃圾比例
 *    - 确保整体的收益
 *
 * 4. 正在压缩的 SSTable：
 *    - 如果 SSTable 正在被压缩，不能再次标记
 *    - 避免重复压缩
 *    - 等待当前压缩完成后再次检查
 *
 * @param blob_garbage_collection_age_cutoff 年龄截止比例（0.0 - 1.0）
 * @param blob_garbage_collection_force_threshold 垃圾比例阈值（0.0 - 1.0）
 *
 * @note Blob GC 是可选的，通过配置启用
 * @note 强制 GC 通过压缩 SSTable 触发，而不是直接操作 Blob 文件
 * @note 此函数在 VersionStorageInfo 初始化时调用
 * @see files_marked_for_forced_blob_gc_ 标记文件列表
 * @see blob_files_ Blob 文件列表（按创建时间排序）
 * @see BlobFileMetaData Blob 文件元数据
 */
void VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC(
    double blob_garbage_collection_age_cutoff,
    double blob_garbage_collection_force_threshold) {
  // 清空之前计算的标记文件列表
  // 每次重新计算时，先清空结果
  files_marked_for_forced_blob_gc_.clear();

  // 检查是否有 Blob 文件
  // blob_files_ 是一个包含所有 Blob 文件元数据的向量
  // 如果为空，说明没有 Blob 文件，直接返回
  if (blob_files_.empty()) {
    return;
  }

  // Number of blob files eligible for GC based on age
  // 计算基于年龄符合 GC 条件的 Blob 文件数量
  // cutoff_count = age_cutoff * total_blob_files_count
  // 示例：如果有 100 个 Blob 文件，age_cutoff = 0.25
  //       cutoff_count = 0.25 * 100 = 25
  //       表示最老的 25 个 Blob 文件符合年龄条件
  const size_t cutoff_count = static_cast<size_t>(
      blob_garbage_collection_age_cutoff * blob_files_.size());
  // 如果 cutoff_count = 0，说明没有文件满足年龄条件
  // 可能的原因：age_cutoff = 0 或者 blob_files_.size() = 0
  // 直接返回，不标记任何文件
  if (!cutoff_count) {
    return;
  }

  // Compute the sum of total and garbage bytes over the oldest batch of blob
  // files. The oldest batch is defined as the set of blob files which are
  // kept alive by the same SSTs as the very oldest one. Here is a toy example.
  // Let's assume we have three SSTs 1, 2, and 3, and four blob files 10, 11,
  // 12, and 13. Also, let's say SSTs 1 and 2 both rely on blob file 10 and
  // potentially some higher-numbered ones, while SST 3 relies on blob file 12
  // and potentially some higher-numbered ones. Then, the SST to oldest blob
  // file mapping is as follows:
  //
  // SST file number               Oldest blob file number
  // 1                             10
  // 2                             10
  // 3                             12
  //
  // This is what the same thing looks like from the blob files' POV. (Note that
  // the linked SSTs simply denote the inverse mapping of the above.)
  //
  // Blob file number              Linked SST set
  // 10                            {1, 2}
  // 11                            {}
  // 12                            {3}
  // 13                            {}
  //
  // Then, the oldest batch of blob files consists of blob files 10 and 11,
  // and we can get rid of them by forcing the compaction of SSTs 1 and 2.
  //
  // Note that the overall ratio of garbage computed for the batch has to exceed
  // blob_garbage_collection_force_threshold and the entire batch has to be
  // eligible for GC according to blob_garbage_collection_age_cutoff in order
  // for us to schedule any compactions.
  // 获取最老的 Blob 文件元数据
  // blob_files_ 按创建时间排序，front() 是最老的
  const auto& oldest_meta = blob_files_.front();
  // 断言 oldest_meta 不为空
  assert(oldest_meta);

  // 获取最老 Blob 文件的关联 SSTable 集合
  // LinkedSsts 是一个 unordered_set<uint64_t>，包含所有引用该 Blob 文件的 SSTable 编号
  // 示例：{1, 2} 表示 SSTable 文件 1 和 2 引用这个 Blob 文件
  const auto& linked_ssts = oldest_meta->GetLinkedSsts();
  // 断言 LinkedSsts 不为空
  // 如果为空，说明该 Blob 文件没有被任何 SSTable 引用
  // 不应该标记任何文件
  assert(!linked_ssts.empty());

  // 初始化计数器（从 1 开始，已经包含了最老的 Blob 文件）
  size_t count = 1;
  // 累计最老批次的所有 Blob 文件的总字节数
  // 初始值：最老 Blob 文件的总字节数
  uint64_t sum_total_blob_bytes = oldest_meta->GetTotalBlobBytes();
  // 累计最老批次的所有 Blob 文件的垃圾字节数
  // 初始值：最老 Blob 文件的垃圾字节数
  uint64_t sum_garbage_blob_bytes = oldest_meta->GetGarbageBlobBytes();

  // 断言 cutoff_count 不超过 Blob 文件总数
  // 这是一个基本的安全检查
  assert(cutoff_count <= blob_files_.size());

  // 从第二个 Blob 文件开始遍历，寻找最老批次
  // 停止条件：
  // 1. count >= cutoff_count：已经找到足够的文件
  // 2. 找到新批次的开始（LinkedSsts 不为空）
  for (; count < cutoff_count; ++count) {
    // 获取当前 Blob 文件的元数据
    const auto& meta = blob_files_[count];
    // 断言 meta 不为空
    assert(meta);

    // 检查当前 Blob 文件是否有关联的 SSTable
    // 如果 LinkedSsts 不为空，说明：
    // a. 该 Blob 文件被某些 SSTable 引用
    // b. 这些 SSTable 与最老的 Blob 文件的 SSTable 不同
    // c. 这是新批次的开始
    // 停止遍历
    if (!meta->GetLinkedSsts().empty()) {
      // Found the beginning of the next batch of blob files
      // 找到下一个 Blob 批次的开始，停止统计
      break;
    }

    // 如果 LinkedSsts 为空，说明该 Blob 文件与最老的 Blob 文件属于同一批次
    // 累加总字节数和垃圾字节数
    sum_total_blob_bytes += meta->GetTotalBlobBytes();
    sum_garbage_blob_bytes += meta->GetGarbageBlobBytes();
  }

  // 检查是否所有文件都满足年龄条件
  // 如果 count < blob_files_.size()，说明还有更多的 Blob 文件
  if (count < blob_files_.size()) {
    // 获取下一个 Blob 文件的元数据
    const auto& meta = blob_files_[count];
    // 断言 meta 不为空
    assert(meta);

    // 检查下一个 Blob 文件的 LinkedSsts 是否为空
    // 如果不为空，说明：
    // a. 下一个 Blob 文件属于新批次
    // b. 新批次的开始（count 位置）也满足年龄条件（count < cutoff_count）
    // c. 无法确定批次边界，不知道应该包含哪些文件
    // 这种情况下，不能安全地进行 GC，直接返回
    if (meta->GetLinkedSsts().empty()) {
      // Some files in the oldest batch are not eligible for GC
      // 最老批次中的一些文件不符合 GC 条件
      // 无法确定批次边界，直接返回，不标记任何文件
      return;
    }
  }

  // 检查垃圾比例是否超过阈值
  // 计算公式：garbage_blob_bytes / total_blob_bytes
  // 条件：garbage_blob_bytes >= force_threshold * total_blob_bytes
  // 示例：
  //   total_blob_bytes = 1000
  //   garbage_blob_bytes = 600
  //   force_threshold = 0.5
  //   600 >= 0.5 * 1000 = 500，满足条件
  if (sum_garbage_blob_bytes <
      blob_garbage_collection_force_threshold * sum_total_blob_bytes) {
    // 垃圾比例不够高，GC 的收益不足以覆盖成本
    // 直接返回，不标记任何文件
    return;
  }

  // 遍历最老批次的所有关联 SSTable
  // linked_ssts 是最老 Blob 文件的 LinkedSsts
  // 包含所有引用该批次 Blob 文件的 SSTable 编号
  for (uint64_t sst_file_number : linked_ssts) {
    // 根据 SSTable 文件号查找文件位置
    // FileLocation 包含层级（level）和位置（position）
    const FileLocation location = GetFileLocation(sst_file_number);
    // 断言位置有效
    assert(location.IsValid());

    // 获取 SSTable 所在的层级
    const int level = location.GetLevel();
    // 断言层级 >= 0
    assert(level >= 0);

    // 获取 SSTable 在该层级中的位置索引
    const size_t pos = location.GetPosition();

    // 获取 SSTable 的元数据指针
    // files_[level] 是该层级的所有文件列表
    // files_[level][pos] 是指定位置的文件
    FileMetaData* const sst_meta = files_[level][pos];
    // 断言元数据不为空
    assert(sst_meta);

    // 检查 SSTable 是否正在被压缩
    // 如果正在被压缩，跳过该文件，避免重复压缩
    if (sst_meta->being_compacted) {
      continue;
    }

    // 将 SSTable 添加到标记列表中
    // 记录层级和文件元数据指针
    // 这个列表将被 CompactionPicker 使用，选择需要强制 Blob GC 的 SSTable
    files_marked_for_forced_blob_gc_.emplace_back(level, sst_meta);
  }
}

namespace {

// used to sort files by size
struct Fsize {
  size_t index;
  FileMetaData* file;
};

// Comparator that is used to sort files based on their size
// In normal mode: descending size
bool CompareCompensatedSizeDescending(const Fsize& first, const Fsize& second) {
  return (first.file->compensated_file_size >
          second.file->compensated_file_size);
}
}  // anonymous namespace

void VersionStorageInfo::AddFile(int level, FileMetaData* f) {
  auto& level_files = files_[level];
  level_files.push_back(f);

  f->refs++;
}

void VersionStorageInfo::AddBlobFile(
    std::shared_ptr<BlobFileMetaData> blob_file_meta) {
  assert(blob_file_meta);

  assert(blob_files_.empty() ||
         (blob_files_.back() && blob_files_.back()->GetBlobFileNumber() <
                                    blob_file_meta->GetBlobFileNumber()));

  blob_files_.emplace_back(std::move(blob_file_meta));
}

VersionStorageInfo::BlobFiles::const_iterator
VersionStorageInfo::GetBlobFileMetaDataLB(uint64_t blob_file_number) const {
  return std::lower_bound(
      blob_files_.begin(), blob_files_.end(), blob_file_number,
      [](const std::shared_ptr<BlobFileMetaData>& lhs, uint64_t rhs) {
        assert(lhs);
        return lhs->GetBlobFileNumber() < rhs;
      });
}

void VersionStorageInfo::SetFinalized() {
  finalized_ = true;

#ifndef NDEBUG
  if (compaction_style_ != kCompactionStyleLevel) {
    // Not level based compaction.
    return;
  }
  assert(base_level_ < 0 || num_levels() == 1 ||
         (base_level_ >= 1 && base_level_ < num_levels()));
  // Verify all levels newer than base_level are empty except L0
  for (int level = 1; level < base_level(); level++) {
    assert(NumLevelBytes(level) == 0);
  }
  uint64_t max_bytes_prev_level = 0;
  for (int level = base_level(); level < num_levels() - 1; level++) {
    if (LevelFiles(level).size() == 0) {
      continue;
    }
    assert(MaxBytesForLevel(level) >= max_bytes_prev_level);
    max_bytes_prev_level = MaxBytesForLevel(level);
  }
  for (int level = 0; level < num_levels(); level++) {
    assert(LevelFiles(level).size() == 0 ||
           LevelFiles(level).size() == LevelFilesBrief(level).num_files);
    if (LevelFiles(level).size() > 0) {
      assert(level < num_non_empty_levels());
    }
  }
  assert(compaction_level_.size() > 0);
  assert(compaction_level_.size() == compaction_score_.size());
#endif
}

void VersionStorageInfo::UpdateNumNonEmptyLevels() {
  num_non_empty_levels_ = num_levels_;
  for (int i = num_levels_ - 1; i >= 0; i--) {
    if (files_[i].size() != 0) {
      return;
    } else {
      num_non_empty_levels_ = i;
    }
  }
}

namespace {
// Sort `temp` based on ratio of overlapping size over file size
void SortFileByOverlappingRatio(
    const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files,
    const std::vector<FileMetaData*>& next_level_files, SystemClock* clock,
    int level, int num_non_empty_levels, uint64_t ttl,
    std::vector<Fsize>* temp) {
  std::unordered_map<uint64_t, uint64_t> file_to_order;
  auto next_level_it = next_level_files.begin();

  int64_t curr_time;
  Status status = clock->GetCurrentTime(&curr_time);
  if (!status.ok()) {
    // If we can't get time, disable TTL.
    ttl = 0;
  }

  FileTtlBooster ttl_booster(static_cast<uint64_t>(curr_time), ttl,
                             num_non_empty_levels, level);

  for (auto& file : files) {
    uint64_t overlapping_bytes = 0;
    // Skip files in next level that is smaller than current file
    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->largest, file->smallest) < 0) {
      next_level_it++;
    }

    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->smallest, file->largest) < 0) {
      overlapping_bytes += (*next_level_it)->fd.file_size;

      if (icmp.Compare((*next_level_it)->largest, file->largest) > 0) {
        // next level file cross large boundary of current file.
        break;
      }
      next_level_it++;
    }

    uint64_t ttl_boost_score = (ttl > 0) ? ttl_booster.GetBoostScore(file) : 1;
    assert(ttl_boost_score > 0);
    assert(file->compensated_file_size != 0);
    file_to_order[file->fd.GetNumber()] = overlapping_bytes * 1024U /
                                          file->compensated_file_size /
                                          ttl_boost_score;
  }

  size_t num_to_sort = temp->size() > VersionStorageInfo::kNumberFilesToSort
                           ? VersionStorageInfo::kNumberFilesToSort
                           : temp->size();

  std::partial_sort(temp->begin(), temp->begin() + num_to_sort, temp->end(),
                    [&](const Fsize& f1, const Fsize& f2) -> bool {
                      // If score is the same, pick file with smaller keys.
                      // This makes the algorithm more deterministic, and also
                      // help the trivial move case to have more files to
                      // extend.
                      if (file_to_order[f1.file->fd.GetNumber()] ==
                          file_to_order[f2.file->fd.GetNumber()]) {
                        return icmp.Compare(f1.file->smallest,
                                            f2.file->smallest) < 0;
                      }
                      return file_to_order[f1.file->fd.GetNumber()] <
                             file_to_order[f2.file->fd.GetNumber()];
                    });
}

void SortFileByRoundRobin(const InternalKeyComparator& icmp,
                          std::vector<InternalKey>* compact_cursor,
                          bool level0_non_overlapping, int level,
                          std::vector<Fsize>* temp) {
  if (level == 0 && !level0_non_overlapping) {
    // Using kOldestSmallestSeqFirst when level === 0, since the
    // files may overlap (not fully sorted)
    std::sort(temp->begin(), temp->end(),
              [](const Fsize& f1, const Fsize& f2) -> bool {
                return f1.file->fd.smallest_seqno < f2.file->fd.smallest_seqno;
              });
    return;
  }

  bool should_move_files =
      compact_cursor->at(level).size() > 0 && temp->size() > 1;

  // The iterator points to the Fsize with smallest key larger than or equal to
  // the given cursor
  std::vector<Fsize>::iterator current_file_iter;
  if (should_move_files) {
    // Find the file of which the smallest key is larger than or equal to
    // the cursor (the smallest key in the successor file of the last
    // chosen file), skip this if the cursor is invalid or there is only
    // one file in this level
    current_file_iter = std::lower_bound(
        temp->begin(), temp->end(), compact_cursor->at(level),
        [&](const Fsize& f, const InternalKey& cursor) -> bool {
          return icmp.Compare(cursor, f.file->smallest) > 0;
        });

    should_move_files =
        current_file_iter != temp->end() && current_file_iter != temp->begin();
  }
  if (should_move_files) {
    // Construct a local temporary vector
    std::vector<Fsize> local_temp;
    local_temp.reserve(temp->size());
    // Move the selected File into the first position and its successors
    // into the second, third, ..., positions
    for (auto iter = current_file_iter; iter != temp->end(); iter++) {
      local_temp.push_back(*iter);
    }
    // Move the origin predecessors of the selected file in a round-robin
    // manner
    for (auto iter = temp->begin(); iter != current_file_iter; iter++) {
      local_temp.push_back(*iter);
    }
    // Replace all the items in temp
    for (size_t i = 0; i < local_temp.size(); i++) {
      temp->at(i) = local_temp[i];
    }
  }
}
}  // anonymous namespace

/**
 * @brief 根据 compaction 优先级策略更新各层文件的排序顺序
 *
 * 本函数是 Level Compaction 中文件选择机制的核心，负责：
 * 1. 按照 compaction_pri 配置的优先级策略，对每层的文件进行排序
 * 2. 将排序结果保存到 files_by_compaction_pri_ 数组中
 * 3. 重置每层的 compact 游标为 0
 *
 * 调用时机：
 * - 每次创建新的 Version 时（VersionSet::AppendVersion）
 * - 在 Compaction 阶段使用文件选择时，会读取此排序结果
 *
 * 为什么需要排序？
 * - 不同层级的文件选择策略不同（大小、序号、重叠率等）
 * - 排序后，PickCompaction 可以从最优的文件开始选择
 * - 避免每次 compaction 都要重新排序，提高性能
 *
 * Compaction 优先级策略（compaction_pri）：
 * 1. kByCompensatedSize（默认）：按补偿大小降序（最大文件优先）
 * 2. kOldestLargestSeqFirst：按最大序号升序（最旧数据优先）
 * 3. kOldestSmallestSeqFirst：按最小序号升序（最早数据优先）
 * 4. kMinOverlappingRatio：按与下一层的重叠率升序（重叠率低优先）
 * 5. kRoundRobin：轮询选择，避免重复选择同一文件
 *
 * 排序范围：
 * - kByCompensatedSize：只对前 kNumberFilesToSort（50）个文件排序
 * - 其他策略：对所有文件排序
 * - 原因：部分排序足够找到最大文件，避免全排序开销
 *
 * 数据结构：
 * - Fsize：临时结构体，保存文件在原列表中的索引和文件指针
 * - files_by_compaction_pri_[level]：保存排序后的索引数组
 * - next_file_to_compact_by_size_[level]：下次选择的起始索引
 *
 * @param ioptions 不可变配置（包含 compaction_pri 策略）
 * @param options 可变配置（包含 ttl 等）
 *
 * @note 只对 Level Compaction 有效：
 *   - FIFO：只基于文件大小和 TTL，不需要排序
 *   - Universal：只基于文件序号，不需要排序
 *   - None：不进行 compaction
 *
 * @note 不排序最高层：
 *   - 最高层永远不会被压缩（没有输出层）
 *   - 排序无意义且浪费 CPU
 *
 * @note kNumberFilesToSort = 50：
 *   - 只需要找到最大的几个文件即可
 *   - partial_sort 的时间复杂度：O(n log k)，k=50
 *   - 全排序的时间复杂度：O(n log n)
 *   - 当 n 很大时，部分排序显著更快
 *
 * @note files_by_compaction_pri_ 的含义：
 *   - 这是一个索引数组，不是文件指针数组
 *   - 值：files_[level][index]，即原文件列表中的索引
 *   - 顺序：按 compaction 优先级降序排列（优先级高的在前）
 *
 * @see kByCompensatedSize 按文件大小降序，大文件优先
 * @see kOldestLargestSeqFirst 按最大序号升序，最旧数据优先
 * @see kOldestSmallestSeqFirst 按最小序号升序，最早数据优先
 * @see kMinOverlappingRatio 按重叠率升序，低重叠率优先
 * @see kRoundRobin 轮询选择，避免重复
 */
void VersionStorageInfo::UpdateFilesByCompactionPri(
    const ImmutableOptions& ioptions, const MutableCFOptions& options) {
  // 检查 compaction 风格，只处理 Level Compaction
  if (compaction_style_ == kCompactionStyleNone ||
      compaction_style_ == kCompactionStyleFIFO ||
      compaction_style_ == kCompactionStyleUniversal) {
    // don't need this
    // FIFO、Universal、None 不需要排序，直接返回
    return;
  }
  // No need to sort the highest level because it is never compacted.
  // 遍历所有层级，除了最高层（最高层不会被压缩）
  for (int level = 0; level < num_levels() - 1; level++) {
    // 获取该层所有文件的原始列表
    const std::vector<FileMetaData*>& files = files_[level];
    // 获取该层的排序结果索引数组（输出）
    auto& files_by_compaction_pri = files_by_compaction_pri_[level];
    // 确保排序结果数组是空的（未初始化状态）
    assert(files_by_compaction_pri.size() == 0);

    // populate a temp vector for sorting based on size
    // 创建临时向量，用于基于 compaction 优先级排序
    // Fsize 结构体保存文件在原列表中的索引和文件指针
    std::vector<Fsize> temp(files.size());
    // 填充临时向量，保存每个文件的原索引
    for (size_t i = 0; i < files.size(); i++) {
      temp[i].index = i;  // 保存原索引
      temp[i].file = files[i];  // 保存文件指针
    }

    // sort the top number_of_files_to_sort_ based on file size
    // 确定需要排序的文件数量（只对前 kNumberFilesToSort 个文件排序）
    // 对于 kByCompensatedSize 策略，部分排序即可
    size_t num = VersionStorageInfo::kNumberFilesToSort; //最多前50个文件排序
    if (num > temp.size()) {
      num = temp.size();  // 如果文件总数少于 kNumberFilesToSort，则全部排序
    }
    // 根据 compaction_pri 策略，选择不同的排序方法
    switch (ioptions.compaction_pri) {
      case kByCompensatedSize:
        // 按补偿大小降序排序（最大的文件优先）
        // 使用 partial_sort，只对前 num 个文件排序，提高性能
        std::partial_sort(temp.begin(), temp.begin() + num, temp.end(),
                          CompareCompensatedSizeDescending);
        break;
      case kOldestLargestSeqFirst:
        // 按最大序号升序排序（序号越小，数据越旧）
        // 这样最旧的数据会优先被压缩
        std::sort(temp.begin(), temp.end(),
                  [](const Fsize& f1, const Fsize& f2) -> bool {
                    return f1.file->fd.largest_seqno <
                           f2.file->fd.largest_seqno;
                  });
        break;
      case kOldestSmallestSeqFirst:
        // 按最小序号升序排序（序号越小，数据越旧）
        // 这样最早的数据会优先被压缩
        std::sort(temp.begin(), temp.end(),
                  [](const Fsize& f1, const Fsize& f2) -> bool {
                    return f1.file->fd.smallest_seqno <
                           f2.file->fd.smallest_seqno;
                  });
        break;
      case kMinOverlappingRatio:
        // 按与下一层的重叠率升序排序（重叠率低的优先）
        // 这样可以减少 compaction 的工作量（重叠少，读取输出层数据少）
        SortFileByOverlappingRatio(*internal_comparator_, files_[level],
                                   files_[level + 1], ioptions.clock, level,
                                   num_non_empty_levels_, options.ttl, &temp);
        break;
      case kRoundRobin:
        // 轮询排序，避免重复选择同一文件
        // 使用 compact_cursor_ 来记录上次选择的文件位置
        SortFileByRoundRobin(*internal_comparator_, &compact_cursor_,
                             level0_non_overlapping_, level, &temp);
        break;
      default:
        // 不应该到达这里
        assert(false);
    }
    // 确保临时向量的大小与原文件列表一致
    assert(temp.size() == files.size());

    // initialize files_by_compaction_pri_
    // 将排序结果转换为索引数组，保存到 files_by_compaction_pri_
    for (size_t i = 0; i < temp.size(); i++) {
      // 保存排序后的原索引
      files_by_compaction_pri.push_back(static_cast<int>(temp[i].index));
    }
    // 重置该层的 compact 游标为 0，表示下次从第一个文件开始选择
    next_file_to_compact_by_size_[level] = 0;
    // 确保索引数组的大小与文件列表的大小一致
    assert(files_[level].size() == files_by_compaction_pri_[level].size());
  }
}

void VersionStorageInfo::GenerateLevel0NonOverlapping() {
  assert(!finalized_);
  level0_non_overlapping_ = true;
  if (level_files_brief_.size() == 0) {
    return;
  }

  // A copy of L0 files sorted by smallest key
  std::vector<FdWithKeyRange> level0_sorted_file(
      level_files_brief_[0].files,
      level_files_brief_[0].files + level_files_brief_[0].num_files);
  std::sort(level0_sorted_file.begin(), level0_sorted_file.end(),
            [this](const FdWithKeyRange& f1, const FdWithKeyRange& f2) -> bool {
              return (internal_comparator_->Compare(f1.smallest_key,
                                                    f2.smallest_key) < 0);
            });

  for (size_t i = 1; i < level0_sorted_file.size(); ++i) {
    FdWithKeyRange& f = level0_sorted_file[i];
    FdWithKeyRange& prev = level0_sorted_file[i - 1];
    if (internal_comparator_->Compare(prev.largest_key, f.smallest_key) >= 0) {
      level0_non_overlapping_ = false;
      break;
    }
  }
}

void VersionStorageInfo::GenerateBottommostFiles() {
  assert(!finalized_);
  assert(bottommost_files_.empty());
  for (size_t level = 0; level < level_files_brief_.size(); ++level) {
    for (size_t file_idx = 0; file_idx < level_files_brief_[level].num_files;
         ++file_idx) {
      const FdWithKeyRange& f = level_files_brief_[level].files[file_idx];
      int l0_file_idx;
      if (level == 0) {
        l0_file_idx = static_cast<int>(file_idx);
      } else {
        l0_file_idx = -1;
      }
      Slice smallest_user_key = ExtractUserKey(f.smallest_key);
      Slice largest_user_key = ExtractUserKey(f.largest_key);
      if (!RangeMightExistAfterSortedRun(smallest_user_key, largest_user_key,
                                         static_cast<int>(level),
                                         l0_file_idx)) {
        bottommost_files_.emplace_back(static_cast<int>(level),
                                       f.file_metadata);
      }
    }
  }
}

void VersionStorageInfo::GenerateFileLocationIndex() {
  size_t num_files = 0;

  for (int level = 0; level < num_levels_; ++level) {
    num_files += files_[level].size();
  }

  file_locations_.reserve(num_files);

  for (int level = 0; level < num_levels_; ++level) {
    for (size_t pos = 0; pos < files_[level].size(); ++pos) {
      const FileMetaData* const meta = files_[level][pos];
      assert(meta);

      const uint64_t file_number = meta->fd.GetNumber();

      assert(file_locations_.find(file_number) == file_locations_.end());
      file_locations_.emplace(file_number, FileLocation(level, pos));
    }
  }
}

void VersionStorageInfo::UpdateOldestSnapshot(SequenceNumber seqnum) {
  assert(seqnum >= oldest_snapshot_seqnum_);
  oldest_snapshot_seqnum_ = seqnum;
  if (oldest_snapshot_seqnum_ > bottommost_files_mark_threshold_) {
    ComputeBottommostFilesMarkedForCompaction();
  }
}

/**
 * @brief 计算哪些最底层文件应该被标记为需要压缩
 *
 * 本函数的作用：
 * 1. 从最底层文件（bottommost_files_）中筛选出需要压缩的文件
 * 2. 基于快照和序列号判断文件是否包含可回收空间
 * 3. 更新 bottommost_files_mark_threshold_ 以便后续快速判断
 *
 * 核心概念：
 *
 * 1. 最底层文件（Bottommost Files）：
 *    - 定义：在 LSM 树的最底层，且没有更低层文件的文件
 *    - 特点：这些文件的数据不会被更底层的文件覆盖
 *    - 重要性：是回收空间的最后机会
 *
 * 2. largest_seqno（文件中的最大序列号）：
 *    - 表示文件中所有 key 的最大序列号
 *    - 序列号越小，表示数据越旧
 *    - 如果 largest_seqno < oldest_snapshot_seqnum_，说明：
 *      a. 文件中的所有数据都早于最旧的快照
 *      b. 不会被任何快照引用
 *      c. 可以安全地删除覆盖的数据（回收空间）
 *
 * 3. being_compacted（是否正在被压缩）：
 *    - 如果文件正在被压缩，不能再次标记
 *    - 避免重复压缩同一个文件
 *
 * 4. largest_seqno == 0 的特殊情况：
 *    - largest_seqno 为 0 表示文件可能是空的或没有有效的序列号
 *    - 这种情况通常出现在特殊场景（如某些边界情况）
 *    - 这种文件不能简单地判断是否可以回收空间，因此跳过
 *
 * 5. 为什么使用 largest_seqno < oldest_snapshot_seqnum_：
 *    - oldest_snapshot_seqnum_ 是所有快照中的最小序列号
 *    - 如果文件的最大序列号小于这个值，说明：
 *      a. 文件中的所有数据都早于最旧的快照
 *      b. 这些数据不会被任何快照读取
 *      c. 可以安全地删除这些数据（如果被覆盖）
 *    - 反之，如果 largest_seqno >= oldest_snapshot_seqnum_，说明：
 *      a. 文件中可能还有数据被快照引用
 *      b. 不能安全地回收空间
 *      c. 需要等待快照释放
 *
 * 6. bottommost_files_mark_threshold_ 的作用：
 *    - 记录未标记文件中最小的 largest_seqno
 *    - 当释放快照时，检查 oldest_snapshot_seqnum_ 是否超过这个阈值
 *    - 如果超过，需要重新计算哪些文件可以被标记
 *    - 这样可以避免每次释放快照都遍历所有文件
 *
 * 算法流程：
 * 1. 清空已标记文件列表
 * 2. 初始化阈值为最大序列号（表示所有文件都需要检查）
 * 3. 遍历所有最底层文件：
 *    a. 跳过正在压缩的文件
 *    b. 跳过 largest_seqno 为 0 的文件
 *    c. 如果 largest_seqno < oldest_snapshot_seqnum_，标记为需要压缩
 *    d. 否则，更新阈值（取最小值）
 * 4. 完成后，bottommost_files_marked_for_compaction_ 包含所有需要压缩的文件
 *
 * 应用场景：
 * 1. 释放快照后，判断是否有新的文件可以被压缩
 *    - UpdateOldestSnapshot() 会检查阈值，决定是否需要重新计算
 * 2. 压缩选择器（CompactionPicker）使用这个列表选择压缩任务
 *    - PickFilesMarkedForCompaction() 会优先选择这些文件
 *
 * 为什么需要这个函数：
 * 1. 最底层文件的压缩可以回收大量空间
 *    - 包含大量被覆盖或删除的数据
 *    - 压缩后可以显著减少存储空间
 * 2. 基于快照的生命周期管理
 *    - 确保不会删除快照需要的数据
 *    - 在快照释放后及时回收空间
 * 3. 提高压缩效率
 *    - 只选择真正可以回收空间的文件
 *    - 避免无效的压缩（文件中所有数据都被引用）
 *
 * 性能优化：
 * 1. 使用 bottommost_files_mark_threshold_ 避免频繁重新计算
 * 2. 只在必要时调用（释放快照时检查阈值）
 * 3. 使用 autovector 存储结果，避免频繁内存分配
 *
 * @note 此函数在 VersionStorageInfo 初始化时调用，也在释放快照时调用
 * @note bottommost_files_ 由 GenerateBottommostFiles() 生成
 * @see GenerateBottommostFiles() 生成最底层文件列表
 * @see UpdateOldestSnapshot() 更新最旧快照序列号并触发重新计算
 */
void VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction() {
  // 清空之前计算的标记文件列表
  // 每次重新计算时，先清空结果
  bottommost_files_marked_for_compaction_.clear();
  // 初始化阈值为最大序列号（64 位无符号整数的最大值）
  // kMaxSequenceNumber = (0x1ull << 56) - 1，即 2^56 - 1
  // 初始值表示没有文件需要阈值检查（所有文件都可能被标记）
  bottommost_files_mark_threshold_ = kMaxSequenceNumber;

  // 遍历所有最底层文件
  // level_and_file 是一个 pair<int, FileMetaData*>
  // first：文件所在的层级
  // second：文件的元数据指针
  for (auto& level_and_file : bottommost_files_) {
    // 检查文件是否正在被压缩（being_compacted）
    // 如果正在被压缩，跳过该文件，避免重复压缩
    // 检查文件的最大序列号是否为 0
    // largest_seqno == 0 表示文件可能有特殊情况（如空文件）
    // 这种文件不能简单地判断是否可以回收空间，因此跳过
    if (!level_and_file.second->being_compacted &&
        level_and_file.second->fd.largest_seqno != 0) {
      // largest_seqno might be nonzero due to containing the final key in an
      // earlier compaction, whose seqnum we didn't zero out. Multiple deletions
      // ensures the file really contains deleted or overwritten keys.
      // 检查文件的最大序列号是否小于最旧快照的序列号
      // 如果 largest_seqno < oldest_snapshot_seqnum_，说明：
      // 1. 文件中的所有数据都早于最旧的快照
      // 2. 这些数据不会被任何快照引用
      // 3. 压缩这个文件可以安全地回收空间
      if (level_and_file.second->fd.largest_seqno < oldest_snapshot_seqnum_) {
        // 将文件添加到标记列表中
        // 这个列表将被 CompactionPicker 使用，选择需要压缩的文件
        bottommost_files_marked_for_compaction_.push_back(level_and_file);
      // 否则（文件的最大序列号 >= 最旧快照的序列号）
      // 说明文件中可能还有数据被快照引用，暂时不能压缩
      } else {
        // 更新阈值为当前文件的最大序列号和已有阈值的较小值
        // bottommost_files_mark_threshold_ 记录未标记文件中最小的 largest_seqno
        // 作用：
        // 1. 当释放快照时，检查 oldest_snapshot_seqnum_ 是否超过这个阈值
        // 2. 如果超过，说明可能有新的文件满足压缩条件
        // 3. 此时需要重新调用本函数，更新标记列表
        // 使用 std::min 确保阈值是最小的 largest_seqno
        bottommost_files_mark_threshold_ =
            std::min(bottommost_files_mark_threshold_,
                     level_and_file.second->fd.largest_seqno);
      }
    }
  }
}

void Version::Ref() { ++refs_; }

bool Version::Unref() {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
    return true;
  }
  return false;
}

bool VersionStorageInfo::OverlapInLevel(int level,
                                        const Slice* smallest_user_key,
                                        const Slice* largest_user_key) {
  // ==================== 参数说明 ====================
  // @param level           要检查的层号（0 = L0, 1 = L1, ..., 6 = L6）
  // @param smallest_user_key 要检查范围的起始键（可以为 nullptr 表示没有下界）
  // @param largest_user_key  要检查范围的结束键（可以为 nullptr 表示没有上界）
  //
  // ==================== 函数功能 ====================
  // 判断指定层中是否存在文件与给定的用户键范围 [smallest_user_key, largest_user_key] 重叠
  //
  // ==================== 返回值 ====================
  // @return true  - 该层存在文件与给定范围重叠
  // @return false - 该层没有文件与给定范围重叠（包括该层为空的情况）
  //
  // ==================== 使用场景 ====================
  // 1. CompactionPicker::CompactRange: 查找与压缩范围重叠的文件
  // 2. RangeMightExistAfterSortedRun: 判断输出范围之后是否有重叠文件
  // 3. GetOverlappingInputs: 查找所有与范围重叠的输入文件
  //
  // ==================== 实现逻辑 ====================
  // L0 层（level == 0）：
  //   - 文件之间可能重叠（disjoint_sorted_files = false）
  //   - 使用线性扫描 O(n) 检查所有文件
  //
  // L1+ 层（level > 0）：
  //   - 文件之间不重叠且有序（disjoint_sorted_files = true）
  //   - 使用二分查找 O(log n) 快速定位
  //
  // ==================== 性能优化 ====================
  // 早期检查：如果层号 >= num_non_empty_levels_，直接返回 false
  // 这避免了不必要的函数调用，因为该层必然为空
  if (level >= num_non_empty_levels_) {
    // 该层为空，不可能有重叠
    return false;
  }
  // 调用底层函数进行重叠检查
  // 参数说明：
  //   *internal_comparator_ - 内部键比较器（考虑了 sequence number）
  //   (level > 0)           - 层号 > 0 表示 L1+ 层，文件不重叠，可用二分查找
  //   level_files_brief_[level] - 该层的文件元数据简表
  //   smallest_user_key     - 范围下界
  //   largest_user_key      - 范围上界
  return SomeFileOverlapsRange(*internal_comparator_, (level > 0),
                               level_files_brief_[level], smallest_user_key,
                               largest_user_key);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// If hint_index is specified, then it points to a file in the
// overlapping range.
// The file_index returns a pointer to any file in an overlapping range.
/**
 * @brief 查找指定层级中与给定范围 [begin, end] 重叠的文件
 *
 * 本函数是 RocksDB compaction 的核心工具函数，用于：
 * 1. 查找与指定 key 范围重叠的所有文件
 * 2. 支持 L0 和 L1+ 层的不同处理逻辑
 * 3. 支持范围扩展（expand_range）用于 Clean Cut
 * 4. 提供 next_smallest 输出，用于后续判断 trivial move
 *
 * L0 层与 L1+ 层的区别：
 * L0 层：
 * - 文件之间可能有 key 重叠
 * - 使用线性扫描 + 范围扩展算法
 * - 需要迭代扩展范围，直到稳定
 * - next_smallest 设为 nullptr（因为文件重叠，无法确定下一个）
 *
 * L1+ 层：
 * - 文件之间无 key 重叠（按范围排序）
 * - 使用二分查找算法快速定位
 * - 直接返回重叠范围，无需迭代
 * - next_smallest 返回范围之后的第一个 key（用于 trivial move）
 *
 * 为什么 L0 和 L1+ 使用不同算法？
 * L0 层：
 * - 文件重叠，无法简单的二分查找
 * - 线性扫描可以找到所有重叠文件
 * - 范围扩展算法确保找到传递的重叠文件
 *
 * L1+ 层：
 * - 文件不重叠，可以高效二分查找
 * - 时间复杂度：O(log n) vs O(n)
 * - 性能显著提升
 *
 * 范围扩展（expand_range）的作用：
 * - 当 expand_range = true 时，每找到一个重叠文件就扩展范围
 * - 初始范围：[user_begin, user_end]
 * - 每次找到重叠文件后，扩展为：[min(user_begin, file_start), max(user_end, file_limit)]
 * - 用于 ExpandInputsToCleanCut，确保找到所有传递的重叠文件
 *
 * hint_index 的作用：
 * - 优化查找性能的提示索引
 * - L1+：用于二分查找的起始位置
 * - L0：未使用（线性扫描不需要）
 * - 避免每次从头遍历，提高性能
 *
 * file_index 的作用：
 * - 输出参数，返回找到的任何重叠文件的索引
 * - 只记录第一个找到的文件索引
 * - 用于后续优化查找（循环调用时传递）
 *
 * next_smallest 的作用：
 * - 输出参数，范围之后的第一个 key
 * - L0：总是 nullptr（文件重叠，无法确定）
 * - L1+：返回范围之后的文件的最小 key
 * - 用于判断是否可以 trivial move（无重叠则可以移动）
 *
 * 算法详解：
 *
 * L0 层算法（线性扫描 + 范围扩展）：
 * 1. 将所有文件索引放入待检查列表（index）
 * 2. 初始化查找范围：[user_begin, user_end]
 * 3. 遍历待检查列表：
 *    a. 跳过完全在范围左边的文件
 *    b. 跳过完全在范围右边的文件
 *    c. 如果重叠：
 *       - 添加到 inputs
 *       - 记录第一个文件的索引到 file_index
 *       - 从待检查列表中移除
 *       - 如果 expand_range，扩展查找范围
 * 4. 重复步骤 3，直到找不到重叠文件
 *
 * 判断重叠的条件：
 * - 文件在范围左边：file_limit < user_begin
 * - 文件在范围右边：file_start > user_end
 * - 重叠：否则（部分或完全包含）
 *
 * L1+ 层算法（二分查找）：
 * - 调用 GetOverlappingInputsRangeBinarySearch
 * - 使用 std::lower_bound 和 std::upper_bound
 * - 时间复杂度：O(log n)
 * - 详见该函数的注释
 *
 * 为什么 L0 使用 std::list 而不是 std::vector？
 * - L0 需要频繁删除元素（找到重叠文件后移除）
 * - std::list::erase 是 O(1)，std::vector::erase 是 O(n)
 * - 虽然文件数量少，但使用 list 更高效
 *
 * 为什么 CompareWithoutTimestamp 而不是 Compare？
 * - L0 层的文件排序基于 InternalKey（包含 seqno 和 type）
 * - user key 比较忽略 seqno 和 type，只比较 user_key
 * - 这样可以正确找到所有 user key 重叠的文件
 * - 避免因为 seqno 或 type 导致的误判
 *
 * @param level 查找的层级（0 = L0, 1 = L1, ...）
 * @param begin 查找范围的起始 key（可为 null，表示无下限）
 * @param end 查找范围的结束 key（可为 null，表示无上限）
 * @param inputs 输出参数：找到的重叠文件列表（会被清空）
 * @param hint_index 优化查找的提示索引（L1+ 使用）
 * @param file_index 输出参数：找到的第一个重叠文件的索引（可为 null）
 * @param expand_range 是否扩展查找范围（true：每找到文件就扩展范围）
 * @param next_smallest 输出参数：范围之后的第一个 key（用于 trivial move）
 *
 * @note 输入参数 begin 和 end 的类型是 InternalKey：
 *   - InternalKey 包含：user_key, sequence_number, value_type
 *   - 比较时使用 ExtractUserKey 获取纯 user_key 部分
 *   - 这确保比较基于 user key，忽略 seqno 和 type
 *
 * @note L0 和 L1+ 的处理差异：
 *   - L0：线性扫描，文件可能重叠，next_smallest = nullptr
 *   - L1+：二分查找，文件不重叠，next_smallest 有效
 *
 * @note expand_range 的作用：
 *   - 扩展查找范围，确保找到所有传递的重叠文件
 *   - 用于 ExpandInputsToCleanCut 实现 Clean Cut
 *   - 示例：范围 [a, c]，文件 [b, d] 也重叠
 *           （因为文件 [b, d] 与范围 [a, c] 重叠）
 *           扩展后范围变为 [a, d]，找到文件 [b, d]
 *
 * @note 为什么使用 CompareWithoutTimestamp：
 *   - 只比较 user_key，忽略 timestamp（如果启用）
 *   - timestamp 是 user_key 的一部分，但在比较时忽略
 *   - 确保找到所有相同 user key 的文件，无论 timestamp
 *
 * @see GetOverlappingInputsRangeBinarySearch L1+ 的二分查找实现
 * @see ExpandInputsToCleanCut 使用此函数实现 Clean Cut
 * @see level_files_brief_ 紧凑的文件元数据结构
 */
void VersionStorageInfo::GetOverlappingInputs(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index,
    bool expand_range, InternalKey** next_smallest) const {
  // 检查指定层级是否存在（是否为空层）
  // 如果层级大于等于非空层数，说明该层是空的，直接返回
  if (level >= num_non_empty_levels_) {
    // this level is empty, no overlapping inputs
    return;
  }

  // 清空输出参数 inputs，确保之前的内容不影响结果
  inputs->clear();
  // 如果 file_index 不为空，初始化为 -1（表示未找到文件）
  // file_index 用于记录找到的第一个重叠文件的索引
  if (file_index) {
    *file_index = -1;
  }
  // 获取用户比较器（用于比较 user key）
  const Comparator* user_cmp = user_comparator_;
  // 如果层级大于 0（L1+ 层），使用二分查找算法
  // L1+ 层的文件之间没有 key 重叠，可以使用二分查找快速定位
  if (level > 0) {
    // 调用二分查找函数查找重叠文件
    // L1+ 层使用二分查找，时间复杂度 O(log n)
    GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hint_index,
                                          file_index, false, next_smallest);
    // 查找完成，直接返回
    return;
  }

  // L0 层的处理
  // 如果 next_smallest 不为空，设为 nullptr
  // next_smallest 只对非 L0 层有意义（因为 L0 文件重叠，无法确定下一个最小 key）
  if (next_smallest) {
    // next_smallest key only makes sense for non-level 0, where files are
    // non-overlapping
    *next_smallest = nullptr;
  }

// L0 层的处理：文件可能重叠，使用线性扫描算法
  // 定义查找范围的起止 user key
  Slice user_begin, user_end;
  // 如果 begin 不为空，再次提取 user_key（上面的重复代码保留原样）
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  // 如果 end 不为空，再次提取 user_key（重复代码，保持原样）
  if (end != nullptr) {
    user_end = end->user_key();
  }

  // index stores the file index need to check.
  // 将 L0 层所有文件的索引添加到待检查列表中
  std::list<size_t> index;
  // 遍历 L0 层的所有文件，将其索引添加到 index 列表中
  for (size_t i = 0; i < level_files_brief_[level].num_files; i++) {
    // 将文件索引添加到待检查列表末尾
    index.emplace_back(i);
  }

  // 主循环：不断检查待检查列表中的文件，直到找不到重叠文件
  // 这个循环是范围扩展算法的核心
  // 每次找到重叠文件后，如果 expand_range=true，会扩展查找范围
  // 然后重新扫描待检查列表，直到没有新的重叠文件
  while (!index.empty()) {
    // 标记本次循环是否找到重叠文件
    bool found_overlapping_file = false;
    // 从待检查列表的开头开始遍历
    auto iter = index.begin();
    // 遍历待检查列表中的每个文件索引
    while (iter != index.end()) {
      // 获取文件元数据指针（FdWithKeyRange 是紧凑的文件元数据结构）
      FdWithKeyRange* f = &(level_files_brief_[level].files[*iter]);
      // 提取文件的最小 key 的 user key 部分
      const Slice file_start = ExtractUserKey(f->smallest_key);
      // 提取文件的最大 key 的 user key 部分
      const Slice file_limit = ExtractUserKey(f->largest_key);
      // 如果查找范围有下限，且文件的最大 key 小于查找范围的下限
      // 说明文件完全在查找范围的左边，不需要合并
      if (begin != nullptr &&
          user_cmp->CompareWithoutTimestamp(file_limit, user_begin) < 0) {
        // "f" is completely before specified range; skip it
        // 文件在查找范围左边，跳过，检查下一个文件
        iter++;
      // 如果查找范围有上限，且文件的最小 key 大于查找范围的上限
      // 说明文件完全在查找范围的右边，不需要合并
      } else if (end != nullptr &&
                 user_cmp->CompareWithoutTimestamp(file_start, user_end) > 0) {
        // "f" is completely after specified range; skip it
        // 文件在查找范围右边，跳过，检查下一个文件
        iter++;
      // 否则，文件与查找范围有重叠（或包含关系）
      } else {
        // if overlap
        // 将文件添加到输出列表 inputs
        inputs->emplace_back(files_[level][*iter]);
        // 标记找到了重叠文件
        found_overlapping_file = true;
        // record the first file index.
        // 如果是第一个找到的文件，且 file_index 不为空，记录文件索引
        if (file_index && *file_index == -1) {
          *file_index = static_cast<int>(*iter);
        }
        // the related file is overlap, erase to avoid checking again.
        // 将找到的文件从待检查列表中删除，避免重复检查
        iter = index.erase(iter);
        // 如果启用了范围扩展
        // 扩展查找范围，确保找到所有传递的重叠文件
        if (expand_range) {
          // 如果文件的最小 key 小于当前查找范围的下限
          // 扩展查找范围的下限到文件的最小 key
          if (begin != nullptr &&
              user_cmp->CompareWithoutTimestamp(file_start, user_begin) < 0) {
            user_begin = file_start;
          }
          // 如果文件的最大 key 大于当前查找范围的上限
          // 扩展查找范围的上限到文件的最大 key
          if (end != nullptr &&
              user_cmp->CompareWithoutTimestamp(file_limit, user_end) > 0) {
            user_end = file_limit;
          }
        }
      }
    }
    // if all the files left are not overlap, break
    // 如果本次循环没有找到任何重叠文件，说明已经找到所有传递的重叠文件
    // 退出外层循环，避免无效的迭代
    if (!found_overlapping_file) {
      break;
    }
  }
}

// Store in "*inputs" files in "level" that within range [begin,end]
// Guarantee a "clean cut" boundary between the files in inputs
// and the surrounding files and the maxinum number of files.
// This will ensure that no parts of a key are lost during compaction.
// If hint_index is specified, then it points to a file in the range.
// The file_index returns a pointer to any file in an overlapping range.
void VersionStorageInfo::GetCleanInputsWithinInterval(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index) const {
  inputs->clear();
  if (file_index) {
    *file_index = -1;
  }
  if (level >= num_non_empty_levels_ || level == 0 ||
      level_files_brief_[level].num_files == 0) {
    // this level is empty, no inputs within range
    // also don't support clean input interval within L0
    return;
  }

  GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hint_index,
                                        file_index, true /* within_interval */);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// Employ binary search to find at least one file that overlaps the
// specified range. From that file, iterate backwards and
// forwards to find all overlapping files.
// if within_range is set, then only store the maximum clean inputs
// within range [begin, end]. "clean" means there is a boundary
// between the files in "*inputs" and the surrounding files
void VersionStorageInfo::GetOverlappingInputsRangeBinarySearch(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index,
    bool within_interval, InternalKey** next_smallest) const {
  // 断言层级必须大于 0
  // 这个函数只用于 L1+ 层，L0 层使用线性扫描算法
  assert(level > 0);

  // 获取用户比较器（用于比较 InternalKey）
  auto user_cmp = user_comparator_;
  // 获取指定层级的文件数组（紧凑的文件元数据结构）
  const FdWithKeyRange* files = level_files_brief_[level].files;
  // 获取该层级的文件数量（转换为 int 类型）
  const int num_files = static_cast<int>(level_files_brief_[level].num_files);

  // begin to use binary search to find lower bound
  // and upper bound.
  // 初始化查找范围的起始索引（默认为 0）
  int start_index = 0;
  // 初始化查找范围的结束索引（默认为文件总数）
  int end_index = num_files;

  // 如果查找范围有下限（begin 不为空）
  // 使用二分查找确定 start_index
  if (begin != nullptr) {
    // if within_interval is true, with file_key would find
    // not overlapping ranges in std::lower_bound.
    // 定义比较函数，用于 std::lower_bound
    // 根据是否使用 within_interval 模式，选择比较文件的最大 key 或最小 key
    auto cmp = [&user_cmp, &within_interval](const FdWithKeyRange& f,
                                             const InternalKey* k) {
      // 如果启用 within_interval，使用文件的最小 key 进行比较
      // 否则使用文件的最大 key 进行比较
      auto& file_key = within_interval ? f.file_metadata->smallest
                                       : f.file_metadata->largest;
      // 使用 sstableKeyCompare 比较 file_key 和 k
      // 返回 true 表示 file_key < k（升序排序）
      return sstableKeyCompare(user_cmp, file_key, *k) < 0;
    };

    // 使用 std::lower_bound 二分查找第一个 >= begin 的文件
    // 搜索范围：从 files 开始，到 hint_index 或 num_files 结束
    // hint_index 是提示索引，用于优化查找性能
    start_index = static_cast<int>(
        std::lower_bound(files,
                         files + (hint_index == -1 ? num_files : hint_index),
                         begin, cmp) -
        files);

    // 如果 start_index > 0 且启用了 within_interval 模式
    // 需要检查 start_index 之前的文件是否与 start_index 处的文件重叠
    // 这是为了确保 clean cut（不分割 user key）
    if (start_index > 0 && within_interval) {
      // 初始化重叠标志为 true
      bool is_overlapping = true;
      // 向前检查，直到找到不重叠的文件
      while (is_overlapping && start_index < num_files) {
        // 获取前一个文件的最大 key
        auto& pre_limit = files[start_index - 1].file_metadata->largest;
        // 获取当前文件的最小 key
        auto& cur_start = files[start_index].file_metadata->smallest;
        // 检查两个文件是否重叠（最大 key 是否等于最小 key）
        // sstableKeyCompare 返回 0 表示相等
        is_overlapping = sstableKeyCompare(user_cmp, pre_limit, cur_start) == 0;
        // 如果重叠，start_index 向前移动（包含前一个文件）
        start_index += is_overlapping;
      }
    }
  }

  // 如果查找范围有上限（end 不为空）
  // 使用二分查找确定 end_index
  if (end != nullptr) {
    // if within_interval is true, with file_key would find
    // not overlapping ranges in std::upper_bound.
    // 定义比较函数，用于 std::upper_bound
    // 根据是否使用 within_interval 模式，选择比较文件的最大 key 或最小 key
    auto cmp = [&user_cmp, &within_interval](const InternalKey* k,
                                             const FdWithKeyRange& f) {
      // 如果启用 within_interval，使用文件的最大 key 进行比较
      // 否则使用文件的最小 key 进行比较
      auto& file_key = within_interval ? f.file_metadata->largest
                                       : f.file_metadata->smallest;
      // 使用 sstableKeyCompare 比较 k 和 file_key
      // 返回 true 表示 k < file_key（升序排序）
      return sstableKeyCompare(user_cmp, *k, file_key) < 0;
    };

    // 使用 std::upper_bound 二分查找第一个 > end 的文件
    // 搜索范围：从 start_index 开始，到 num_files 结束
    end_index = static_cast<int>(
        std::upper_bound(files + start_index, files + num_files, end, cmp) -
        files);

    // 如果 end_index < num_files 且启用了 within_interval 模式
    // 需要检查 end_index 处的文件是否与 end_index - 1 处的文件重叠
    // 这是为了确保 clean cut（不分割 user key）
    if (end_index < num_files && within_interval) {
      // 初始化重叠标志为 true
      bool is_overlapping = true;
      // 向后检查，直到找到不重叠的文件
      while (is_overlapping && end_index > start_index) {
        // 获取下一个文件的最小 key
        auto& next_start = files[end_index].file_metadata->smallest;
        // 获取当前文件的最大 key
        auto& cur_limit = files[end_index - 1].file_metadata->largest;
        // 检查两个文件是否重叠（最大 key 是否等于最小 key）
        is_overlapping =
            sstableKeyCompare(user_cmp, cur_limit, next_start) == 0;
        // 如果重叠，end_index 向后移动（包含下一个文件）
        end_index -= is_overlapping;
      }
    }
  }

  // 断言 start_index <= end_index
  // 这是一个基本的逻辑检查，确保查找范围有效
  assert(start_index <= end_index);

  // If there were no overlapping files, return immediately.
  // 如果没有找到重叠文件（start_index == end_index），直接返回
  if (start_index == end_index) {
    // 如果 next_smallest 不为空，设为 nullptr（没有文件，也没有下一个最小 key）
    if (next_smallest) {
      *next_smallest = nullptr;
    }
    return;
  }

  // 断言 start_index < end_index
  // 如果执行到这里，说明找到了重叠文件
  assert(start_index < end_index);

  // returns the index where an overlap is found
  // 如果 file_index 不为空，记录找到的第一个重叠文件的索引
  if (file_index) {
    *file_index = start_index;
  }

  // insert overlapping files into vector
  // 将 [start_index, end_index) 范围内的文件添加到输出列表 inputs
  for (int i = start_index; i < end_index; i++) {
    // 将文件指针添加到 inputs 向量末尾
    inputs->push_back(files_[level][i]);
  }

  // 如果 next_smallest 不为空（需要返回范围之外的下一个最小 key）
  if (next_smallest != nullptr) {
    // Provide the next key outside the range covered by inputs
    // 检查 end_index 是否小于该层的文件总数
    // 即是否还有文件在查找范围之外
    if (end_index < static_cast<int>(files_[level].size())) {
      // 返回 end_index 处文件的最小 key
      // 这是在查找范围之外的第一个文件的最小 key
      // 用于判断是否可以 trivial move（如果无重叠则可以）
      **next_smallest = files_[level][end_index]->smallest;
    // 如果 end_index 已经是最后一个文件的索引
    // 说明没有更多文件了
    } else {
      // 设为 nullptr（范围之后没有文件）
      *next_smallest = nullptr;
    }
  }
}

uint64_t VersionStorageInfo::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < num_levels());
  return TotalFileSize(files_[level]);
}

const char* VersionStorageInfo::LevelSummary(
    LevelSummaryStorage* scratch) const {
  int len = 0;
  if (compaction_style_ == kCompactionStyleLevel && num_levels() > 1) {
    assert(base_level_ < static_cast<int>(level_max_bytes_.size()));
    if (level_multiplier_ != 0.0) {
      len = snprintf(
          scratch->buffer, sizeof(scratch->buffer),
          "base level %d level multiplier %.2f max bytes base %" PRIu64 " ",
          base_level_, level_multiplier_, level_max_bytes_[base_level_]);
    }
  }
  len +=
      snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "files[");
  for (int i = 0; i < num_levels(); i++) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "%d ", int(files_[i].size()));
    if (ret < 0 || ret >= sz) break;
    len += ret;
  }
  if (len > 0) {
    // overwrite the last space
    --len;
  }
  len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                  "] max score %.2f", compaction_score_[0]);

  if (!files_marked_for_compaction_.empty()) {
    snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
             " (%" ROCKSDB_PRIszt " files need compaction)",
             files_marked_for_compaction_.size());
  }

  return scratch->buffer;
}

const char* VersionStorageInfo::LevelFileSummary(FileSummaryStorage* scratch,
                                                 int level) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files_size[");
  for (const auto& f : files_[level]) {
    int sz = sizeof(scratch->buffer) - len;
    char sztxt[16];
    AppendHumanBytes(f->fd.GetFileSize(), sztxt, sizeof(sztxt));
    int ret = snprintf(scratch->buffer + len, sz,
                       "#%" PRIu64 "(seq=%" PRIu64 ",sz=%s,%d) ",
                       f->fd.GetNumber(), f->fd.smallest_seqno, sztxt,
                       static_cast<int>(f->being_compacted));
    if (ret < 0 || ret >= sz) break;
    len += ret;
  }
  // overwrite the last space (only if files_[level].size() is non-zero)
  if (files_[level].size() && len > 0) {
    --len;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

bool VersionStorageInfo::HasMissingEpochNumber() const {
  for (int level = 0; level < num_levels_; ++level) {
    for (const FileMetaData* f : files_[level]) {
      if (f->epoch_number == kUnknownEpochNumber) {
        return true;
      }
    }
  }
  return false;
}

uint64_t VersionStorageInfo::GetMaxEpochNumberOfFiles() const {
  uint64_t max_epoch_number = kUnknownEpochNumber;
  for (int level = 0; level < num_levels_; ++level) {
    for (const FileMetaData* f : files_[level]) {
      max_epoch_number = std::max(max_epoch_number, f->epoch_number);
    }
  }
  return max_epoch_number;
}

void VersionStorageInfo::RecoverEpochNumbers(ColumnFamilyData* cfd) {
  cfd->ResetNextEpochNumber();

  bool reserve_epoch_num_for_file_ingested_behind =
      cfd->ioptions()->allow_ingest_behind;
  if (reserve_epoch_num_for_file_ingested_behind) {
    uint64_t reserved_epoch_number = cfd->NewEpochNumber();
    assert(reserved_epoch_number == kReservedEpochNumberForFileIngestedBehind);
    ROCKS_LOG_INFO(cfd->ioptions()->info_log.get(),
                   "[%s]CF has reserved epoch number %" PRIu64
                   " for files ingested "
                   "behind since `Options::allow_ingest_behind` is true",
                   cfd->GetName().c_str(), reserved_epoch_number);
  }

  if (HasMissingEpochNumber()) {
    assert(epoch_number_requirement_ == EpochNumberRequirement::kMightMissing);
    assert(num_levels_ >= 1);

    for (int level = num_levels_ - 1; level >= 1; --level) {
      auto& files_at_level = files_[level];
      if (files_at_level.empty()) {
        continue;
      }
      uint64_t next_epoch_number = cfd->NewEpochNumber();
      for (FileMetaData* f : files_at_level) {
        f->epoch_number = next_epoch_number;
      }
    }

    for (auto file_meta_iter = files_[0].rbegin();
         file_meta_iter != files_[0].rend(); file_meta_iter++) {
      FileMetaData* f = *file_meta_iter;
      f->epoch_number = cfd->NewEpochNumber();
    }

    ROCKS_LOG_WARN(cfd->ioptions()->info_log.get(),
                   "[%s]CF's epoch numbers are inferred based on seqno",
                   cfd->GetName().c_str());
    epoch_number_requirement_ = EpochNumberRequirement::kMustPresent;
  } else {
    assert(epoch_number_requirement_ == EpochNumberRequirement::kMustPresent);
    cfd->SetNextEpochNumber(
        std::max(GetMaxEpochNumberOfFiles() + 1, cfd->GetNextEpochNumber()));
  }
}

uint64_t VersionStorageInfo::MaxNextLevelOverlappingBytes() {
  uint64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < num_levels() - 1; level++) {
    for (const auto& f : files_[level]) {
      GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

uint64_t VersionStorageInfo::MaxBytesForLevel(int level) const {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  assert(level >= 0);
  assert(level < static_cast<int>(level_max_bytes_.size()));
  return level_max_bytes_[level];
}

void VersionStorageInfo::CalculateBaseBytes(const ImmutableOptions& ioptions,
                                            const MutableCFOptions& options) {
  // Special logic to set number of sorted runs.
  // It is to match the previous behavior when all files are in L0.
  int num_l0_count = static_cast<int>(files_[0].size());
  if (compaction_style_ == kCompactionStyleUniversal) {
    // For universal compaction, we use level0 score to indicate
    // compaction score for the whole DB. Adding other levels as if
    // they are L0 files.
    for (int i = 1; i < num_levels(); i++) {
      if (!files_[i].empty()) {
        num_l0_count++;
      }
    }
  }
  set_l0_delay_trigger_count(num_l0_count);

  level_max_bytes_.resize(ioptions.num_levels);
  if (!ioptions.level_compaction_dynamic_level_bytes) {
    base_level_ = (ioptions.compaction_style == kCompactionStyleLevel) ? 1 : -1;

    // Calculate for static bytes base case
    for (int i = 0; i < ioptions.num_levels; ++i) {
      if (i == 0 && ioptions.compaction_style == kCompactionStyleUniversal) {
        level_max_bytes_[i] = options.max_bytes_for_level_base;
      } else if (i > 1) {
        level_max_bytes_[i] = MultiplyCheckOverflow(
            MultiplyCheckOverflow(level_max_bytes_[i - 1],
                                  options.max_bytes_for_level_multiplier),
            options.MaxBytesMultiplerAdditional(i - 1));
      } else {
        level_max_bytes_[i] = options.max_bytes_for_level_base;
      }
    }
  } else {
    assert(ioptions.compaction_style == kCompactionStyleLevel);
    uint64_t max_level_size = 0;

    int first_non_empty_level = -1;
    // Find size of non-L0 level of most data.
    // Cannot use the size of the last level because it can be empty or less
    // than previous levels after compaction.
    for (int i = 1; i < num_levels_; i++) {
      uint64_t total_size = 0;
      for (const auto& f : files_[i]) {
        total_size += f->fd.GetFileSize();
      }
      if (total_size > 0 && first_non_empty_level == -1) {
        first_non_empty_level = i;
      }
      if (total_size > max_level_size) {
        max_level_size = total_size;
      }
    }

    // Prefill every level's max bytes to disallow compaction from there.
    for (int i = 0; i < num_levels_; i++) {
      level_max_bytes_[i] = std::numeric_limits<uint64_t>::max();
    }

    lowest_unnecessary_level_ = -1;
    if (max_level_size == 0) {
      // No data for L1 and up. L0 compacts to last level directly.
      // No compaction from L1+ needs to be scheduled.
      base_level_ = num_levels_ - 1;
    } else {
      assert(first_non_empty_level >= 1);
      uint64_t base_bytes_max = options.max_bytes_for_level_base;
      uint64_t base_bytes_min = static_cast<uint64_t>(
          base_bytes_max / options.max_bytes_for_level_multiplier);

      // Try whether we can make last level's target size to be max_level_size
      uint64_t cur_level_size = max_level_size;
      for (int i = num_levels_ - 2; i >= first_non_empty_level; i--) {
        // Round up after dividing
        cur_level_size = static_cast<uint64_t>(
            cur_level_size / options.max_bytes_for_level_multiplier);
        if (lowest_unnecessary_level_ == -1 &&
            cur_level_size <= base_bytes_min &&
            (ioptions.preclude_last_level_data_seconds == 0 ||
             i < num_levels_ - 2)) {
          // When per_key_placement is enabled, the penultimate level is
          // necessary.
          lowest_unnecessary_level_ = i;
        }
      }

      // Calculate base level and its size.
      uint64_t base_level_size;
      if (cur_level_size <= base_bytes_min) {
        // If per_key_placement is not enabled,
        // either there is only one non-empty level after level 0,
        // which can less than base_bytes_min AND necessary,
        // or there is some unnecessary level.
        assert(first_non_empty_level == num_levels_ - 1 ||
               ioptions.preclude_last_level_data_seconds > 0 ||
               lowest_unnecessary_level_ != -1);
        // Case 1. If we make target size of last level to be max_level_size,
        // target size of the first non-empty level would be smaller than
        // base_bytes_min. We set it be base_bytes_min.
        base_level_size = base_bytes_min + 1U;
        base_level_ = first_non_empty_level;
        if (base_level_ < num_levels_ - 1) {
          ROCKS_LOG_INFO(
              ioptions.logger,
              "More existing levels in DB than needed: all non-zero "
              "levels <= level %d are unnecessary.  "
              "max_bytes_for_level_multiplier may not be guaranteed.",
              lowest_unnecessary_level_);
        }
      } else {
        assert(lowest_unnecessary_level_ == -1);
        // Find base level (where L0 data is compacted to).
        base_level_ = first_non_empty_level;
        while (base_level_ > 1 && cur_level_size > base_bytes_max) {
          --base_level_;
          cur_level_size = static_cast<uint64_t>(
              cur_level_size / options.max_bytes_for_level_multiplier);
        }
        if (cur_level_size > base_bytes_max) {
          // Even L1 will be too large
          assert(base_level_ == 1);
          base_level_size = base_bytes_max;
        } else {
          base_level_size = std::max(static_cast<uint64_t>(1), cur_level_size);
        }
      }

      level_multiplier_ = options.max_bytes_for_level_multiplier;
      assert(base_level_size > 0);

      uint64_t level_size = base_level_size;
      for (int i = base_level_; i < num_levels_; i++) {
        if (i > base_level_) {
          level_size = MultiplyCheckOverflow(level_size, level_multiplier_);
        }
        // Don't set any level below base_bytes_max. Otherwise, the LSM can
        // assume an hourglass shape where L1+ sizes are smaller than L0. This
        // causes compaction scoring, which depends on level sizes, to favor L1+
        // at the expense of L0, which may fill up and stall.
        level_max_bytes_[i] = std::max(level_size, base_bytes_max);
      }
    }
  }
}

uint64_t VersionStorageInfo::EstimateLiveDataSize() const {
  // Estimate the live data size by adding up the size of a maximal set of
  // sst files with no range overlap in same or higher level. The less
  // compacted, the more optimistic (smaller) this estimate is. Also,
  // for multiple sorted runs within a level, file order will matter.
  uint64_t size = 0;

  auto ikey_lt = [this](InternalKey* x, InternalKey* y) {
    return internal_comparator_->Compare(*x, *y) < 0;
  };
  // (Ordered) map of largest keys in files being included in size estimate
  std::map<InternalKey*, FileMetaData*, decltype(ikey_lt)> ranges(ikey_lt);

  for (int l = num_levels_ - 1; l >= 0; l--) {
    bool found_end = false;
    for (auto file : files_[l]) {
      // Find the first file already included with largest key is larger than
      // the smallest key of `file`. If that file does not overlap with the
      // current file, none of the files in the map does. If there is
      // no potential overlap, we can safely insert the rest of this level
      // (if the level is not 0) into the map without checking again because
      // the elements in the level are sorted and non-overlapping.
      auto lb = (found_end && l != 0) ? ranges.end()
                                      : ranges.lower_bound(&file->smallest);
      found_end = (lb == ranges.end());
      if (found_end || internal_comparator_->Compare(
                           file->largest, (*lb).second->smallest) < 0) {
        ranges.emplace_hint(lb, &file->largest, file);
        size += file->fd.file_size;
      }
    }
  }

  // For BlobDB, the result also includes the exact value of live bytes in the
  // blob files of the version.
  for (const auto& meta : blob_files_) {
    assert(meta);

    size += meta->GetTotalBlobBytes();
    size -= meta->GetGarbageBlobBytes();
  }

  return size;
}

bool VersionStorageInfo::RangeMightExistAfterSortedRun(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int last_level, int last_l0_idx) {
  // 断言：last_l0_idx为-1当且仅当last_level不为0
  // 这确保了L0层的特殊参数last_l0_idx只在last_level=0时使用
  assert((last_l0_idx != -1) == (last_level == 0));
  
  // ==================== L0层特殊处理 ====================
  // L0层的文件是无序的，可能互相重叠，无法简单地按范围判断
  // 当前的保守策略：只有当L0文件是最老的文件（last_l0_idx == LevelFiles(0).size() - 1）
  // 且更深层（L1, L2, ...）没有文件时，才认为是bottommost
  //
  // TODO(ajkr): 这个判断过于保守。理想情况下，应该检查L0输出范围是否与
  // 更深层的文件有重叠。如果有重叠，就不是bottommost；如果没有重叠，可以是bottommost。
  // 当前的实现要求必须是L0层最后一个文件才能是bottommost，限制过于严格。
  //
  // 例如场景：
  //   L0: [file_a, file_b, file_c]  (假设file_c是最新文件)
  //   如果file_b的输出范围与L1-L6所有文件都不重叠，理论上可以是bottommost
  //   但当前实现会返回false（因为file_b不是最后一个文件）
  if (last_level == 0 &&
      last_l0_idx != static_cast<int>(LevelFiles(0).size() - 1)) {
    // L0层且不是最后一个文件，说明之后还有L0文件
    // 这些文件可能包含更新的数据或与当前范围重叠
    // 因此，当前范围之后可能存在数据，返回true
    return true;
  }

  // ==================== 检查更深层次的文件 ====================
  // 从 last_level + 1 开始，逐层检查是否有文件与给定范围重叠
  //
  // 对于Level压缩风格（Leveled Compaction）：
  //   - last_level = 0: 检查L1, L2, L3, ..., L6是否有文件
  //     * 只要L1-L6任何一层有文件，就返回true（因为L0文件总是可能被覆盖）
  //   - last_level = 5: 检查L6是否有文件与[smallest_user_key, largest_user_key]重叠
  //     * L6有文件且不重叠 -> 返回false（是bottommost）
  //     * L6有文件且重叠 -> 返回true（不是bottommost）
  //   - last_level = 6: L6是最后一层，没有更深的层，直接返回false（是bottommost）
  //
  // 对于Universal压缩风格（Universal Compaction）：
  //   - sorted run按年龄排序，编号0是最老的，编号n是最新的
  //   - last_level表示当前sorted run的编号
  //   - 检查编号 > last_level 的所有sorted run是否与给定范围重叠
  //   - 如果有任何后续sorted run重叠，说明数据可能被覆盖，返回true
  //   - 如果所有后续sorted run都不重叠，说明这是最底层的run，返回false
  //
  // Bottommost优化的前提：
  //   1. 更深层（last_level + 1 及之后）没有文件，或
  //   2. 更深层的文件与[smallest_user_key, largest_user_key]没有重叠
  //   此时可以安全地进行bottommost优化（如删除旧数据、移除tombstone等）
  for (int level = last_level + 1; level < num_levels(); level++) {
    // 判断是否可能存在后续数据的逻辑：
    // 条件1：该层有文件（files_[level].size() > 0）
    // 条件2（满足其一即可）：
    //   A) last_level == 0: L0层的任何情况下，只要更深层有文件就返回true
    //      因为L0文件是无序的，无法保证与深层文件的重叠关系
    //   B) OverlapInLevel返回true: 该层存在文件与给定范围重叠
    //
    // 示例场景（Level压缩）：
    //   last_level = 5, 范围 = [key_a, key_b]
    //   L6文件: file_1: [key_x, key_y] (不重叠) -> 继续检查
    //         file_2: [key_a, key_c] (重叠!) -> 返回true（不是bottommost）
    //
    //   last_level = 5, 范围 = [key_a, key_b]
    //   L6文件: file_1: [key_x, key_y] (不重叠)
    //         file_2: [key_z, key_w] (不重叠)
    //   循环结束，返回false（是bottommost）
    if (files_[level].size() > 0 &&
        (last_level == 0 ||
         OverlapInLevel(level, &smallest_user_key, &largest_user_key))) {
      return true;
    }
  }
  // 所有更深层次的检查都通过，没有发现与给定范围重叠的文件
  // 说明该范围之后不存在数据，是bottommost level
  return false;
}

void Version::AddLiveFiles(std::vector<uint64_t>* live_table_files,
                           std::vector<uint64_t>* live_blob_files) const {
  assert(live_table_files);
  assert(live_blob_files);

  for (int level = 0; level < storage_info_.num_levels(); ++level) {
    const auto& level_files = storage_info_.LevelFiles(level);
    for (const auto& meta : level_files) {
      assert(meta);

      live_table_files->emplace_back(meta->fd.GetNumber());
    }
  }

  const auto& blob_files = storage_info_.GetBlobFiles();
  for (const auto& meta : blob_files) {
    assert(meta);

    live_blob_files->emplace_back(meta->GetBlobFileNumber());
  }
}

void Version::RemoveLiveFiles(
    std::vector<ObsoleteFileInfo>& sst_delete_candidates,
    std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const {
  for (ObsoleteFileInfo& fi : sst_delete_candidates) {
    if (!fi.only_delete_metadata &&
        storage_info()->GetFileLocation(fi.metadata->fd.GetNumber()) !=
            VersionStorageInfo::FileLocation::Invalid()) {
      fi.only_delete_metadata = true;
    }
  }

  blob_delete_candidates.erase(
      std::remove_if(
          blob_delete_candidates.begin(), blob_delete_candidates.end(),
          [this](ObsoleteBlobFileInfo& x) {
            return storage_info()->GetBlobFileMetaData(x.GetBlobFileNumber());
          }),
      blob_delete_candidates.end());
}

std::string Version::DebugString(bool hex, bool print_stats) const {
  std::string r;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123[1 .. 124]['a' .. 'd']
    //   20:43[124 .. 128]['e' .. 'g']
    //
    // if print_stats=true:
    //   17:123[1 .. 124]['a' .. 'd'](4096)
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" --- version# ");
    AppendNumberTo(&r, version_number_);
    if (storage_info_.compact_cursor_[level].Valid()) {
      r.append(" --- compact_cursor: ");
      r.append(storage_info_.compact_cursor_[level].DebugString(hex));
    }
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = storage_info_.files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->fd.GetNumber());
      r.push_back(':');
      AppendNumberTo(&r, files[i]->fd.GetFileSize());
      r.append("[");
      AppendNumberTo(&r, files[i]->fd.smallest_seqno);
      r.append(" .. ");
      AppendNumberTo(&r, files[i]->fd.largest_seqno);
      r.append("]");
      r.append("[");
      r.append(files[i]->smallest.DebugString(hex));
      r.append(" .. ");
      r.append(files[i]->largest.DebugString(hex));
      r.append("]");
      if (files[i]->oldest_blob_file_number != kInvalidBlobFileNumber) {
        r.append(" blob_file:");
        AppendNumberTo(&r, files[i]->oldest_blob_file_number);
      }
      if (print_stats) {
        r.append("(");
        r.append(std::to_string(
            files[i]->stats.num_reads_sampled.load(std::memory_order_relaxed)));
        r.append(")");
      }
      r.append("\n");
    }
  }

  const auto& blob_files = storage_info_.GetBlobFiles();
  if (!blob_files.empty()) {
    r.append("--- blob files --- version# ");
    AppendNumberTo(&r, version_number_);
    r.append(" ---\n");
    for (const auto& blob_file_meta : blob_files) {
      assert(blob_file_meta);

      r.append(blob_file_meta->DebugString());
      r.push_back('\n');
    }
  }

  return r;
}

// this is used to batch writes to the manifest file
struct VersionSet::ManifestWriter {
  Status status;
  bool done;
  InstrumentedCondVar cv;
  ColumnFamilyData* cfd;
  const MutableCFOptions mutable_cf_options;
  const autovector<VersionEdit*>& edit_list;
  const std::function<void(const Status&)> manifest_write_callback;

  explicit ManifestWriter(
      InstrumentedMutex* mu, ColumnFamilyData* _cfd,
      const MutableCFOptions& cf_options, const autovector<VersionEdit*>& e,
      const std::function<void(const Status&)>& manifest_wcb)
      : done(false),
        cv(mu),
        cfd(_cfd),
        mutable_cf_options(cf_options),
        edit_list(e),
        manifest_write_callback(manifest_wcb) {}
  ~ManifestWriter() { status.PermitUncheckedError(); }

  bool IsAllWalEdits() const {
    bool all_wal_edits = true;
    for (const auto& e : edit_list) {
      if (!e->IsWalManipulation()) {
        all_wal_edits = false;
        break;
      }
    }
    return all_wal_edits;
  }
};

Status AtomicGroupReadBuffer::AddEdit(VersionEdit* edit) {
  assert(edit);
  if (edit->is_in_atomic_group_) {
    TEST_SYNC_POINT("AtomicGroupReadBuffer::AddEdit:AtomicGroup");
    if (replay_buffer_.empty()) {
      replay_buffer_.resize(edit->remaining_entries_ + 1);
      TEST_SYNC_POINT_CALLBACK(
          "AtomicGroupReadBuffer::AddEdit:FirstInAtomicGroup", edit);
    }
    read_edits_in_atomic_group_++;
    if (read_edits_in_atomic_group_ + edit->remaining_entries_ !=
        static_cast<uint32_t>(replay_buffer_.size())) {
      TEST_SYNC_POINT_CALLBACK(
          "AtomicGroupReadBuffer::AddEdit:IncorrectAtomicGroupSize", edit);
      return Status::Corruption("corrupted atomic group");
    }
    replay_buffer_[read_edits_in_atomic_group_ - 1] = *edit;
    if (read_edits_in_atomic_group_ == replay_buffer_.size()) {
      TEST_SYNC_POINT_CALLBACK(
          "AtomicGroupReadBuffer::AddEdit:LastInAtomicGroup", edit);
      return Status::OK();
    }
    return Status::OK();
  }

  // A normal edit.
  if (!replay_buffer().empty()) {
    TEST_SYNC_POINT_CALLBACK(
        "AtomicGroupReadBuffer::AddEdit:AtomicGroupMixedWithNormalEdits", edit);
    return Status::Corruption("corrupted atomic group");
  }
  return Status::OK();
}

bool AtomicGroupReadBuffer::IsFull() const {
  return read_edits_in_atomic_group_ == replay_buffer_.size();
}

bool AtomicGroupReadBuffer::IsEmpty() const { return replay_buffer_.empty(); }

void AtomicGroupReadBuffer::Clear() {
  read_edits_in_atomic_group_ = 0;
  replay_buffer_.clear();
}

VersionSet::VersionSet(const std::string& dbname,
                       const ImmutableDBOptions* _db_options,
                       const FileOptions& storage_options, Cache* table_cache,
                       WriteBufferManager* write_buffer_manager,
                       WriteController* write_controller,
                       BlockCacheTracer* const block_cache_tracer,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       const std::string& db_id,
                       const std::string& db_session_id)
    : column_family_set_(new ColumnFamilySet(
          dbname, _db_options, storage_options, table_cache,
          write_buffer_manager, write_controller, block_cache_tracer, io_tracer,
          db_id, db_session_id)),
      table_cache_(table_cache),
      env_(_db_options->env),
      fs_(_db_options->fs, io_tracer),
      clock_(_db_options->clock),
      dbname_(dbname),
      db_options_(_db_options),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      options_file_number_(0),
      options_file_size_(0),
      pending_manifest_file_number_(0),
      last_sequence_(0),
      last_allocated_sequence_(0),
      last_published_sequence_(0),
      prev_log_number_(0),
      current_version_number_(0),
      manifest_file_size_(0),
      file_options_(storage_options),
      block_cache_tracer_(block_cache_tracer),
      io_tracer_(io_tracer),
      db_session_id_(db_session_id) {}

VersionSet::~VersionSet() {
  // we need to delete column_family_set_ because its destructor depends on
  // VersionSet
  column_family_set_.reset();
  for (auto& file : obsolete_files_) {
    if (file.metadata->table_reader_handle) {
      table_cache_->Release(file.metadata->table_reader_handle);
      TableCache::Evict(table_cache_, file.metadata->fd.GetNumber());
    }
    file.DeleteMetadata();
  }
  obsolete_files_.clear();
  io_status_.PermitUncheckedError();
}

void VersionSet::Reset() {
  if (column_family_set_) {
    WriteBufferManager* wbm = column_family_set_->write_buffer_manager();
    WriteController* wc = column_family_set_->write_controller();
    // db_id becomes the source of truth after DBImpl::Recover():
    // https://github.com/facebook/rocksdb/blob/v7.3.1/db/db_impl/db_impl_open.cc#L527
    // Note: we may not be able to recover db_id from MANIFEST if
    // options.write_dbid_to_manifest is false (default).
    column_family_set_.reset(new ColumnFamilySet(
        dbname_, db_options_, file_options_, table_cache_, wbm, wc,
        block_cache_tracer_, io_tracer_, db_id_, db_session_id_));
  }
  db_id_.clear();
  next_file_number_.store(2);
  min_log_number_to_keep_.store(0);
  manifest_file_number_ = 0;
  options_file_number_ = 0;
  pending_manifest_file_number_ = 0;
  last_sequence_.store(0);
  last_allocated_sequence_.store(0);
  last_published_sequence_.store(0);
  prev_log_number_ = 0;
  descriptor_log_.reset();
  current_version_number_ = 0;
  manifest_writers_.clear();
  manifest_file_size_ = 0;
  obsolete_files_.clear();
  obsolete_manifests_.clear();
  wals_.Reset();
}

void VersionSet::AppendVersion(ColumnFamilyData* column_family_data,
                               Version* v) {
  // compute new compaction score
  v->storage_info()->ComputeCompactionScore(
      *column_family_data->ioptions(),
      *column_family_data->GetLatestMutableCFOptions());

  // Mark v finalized
  v->storage_info_.SetFinalized();

  // Make "v" current
  assert(v->refs_ == 0);
  Version* current = column_family_data->current();
  assert(v != current);
  if (current != nullptr) {
    assert(current->refs_ > 0);
    current->Unref();
  }
  column_family_data->SetCurrent(v);
  v->Ref();

  // Append to linked list
  v->prev_ = column_family_data->dummy_versions()->prev_;
  v->next_ = column_family_data->dummy_versions();
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::ProcessManifestWrites(
    std::deque<ManifestWriter>& writers, InstrumentedMutex* mu,
    FSDirectory* dir_contains_current_file, bool new_descriptor_log,
    const ColumnFamilyOptions* new_cf_options,
    const ReadOptions& read_options) {
  mu->AssertHeld();
  assert(!writers.empty());
  ManifestWriter& first_writer = writers.front();
  ManifestWriter* last_writer = &first_writer;

  assert(!manifest_writers_.empty());
  assert(manifest_writers_.front() == &first_writer);

  autovector<VersionEdit*> batch_edits;
  // This vector keeps track of the corresponding user-defined timestamp size
  // for `batch_edits` side by side, which is only needed for encoding a
  // `VersionEdit` that adds new SST files.
  // Note that anytime `batch_edits` has new element added or get existing
  // element removed, `batch_edits_ts_sz` should be updated too.
  autovector<std::optional<size_t>> batch_edits_ts_sz;
  autovector<Version*> versions;
  autovector<const MutableCFOptions*> mutable_cf_options_ptrs;
  std::vector<std::unique_ptr<BaseReferencedVersionBuilder>> builder_guards;

  // Tracking `max_last_sequence` is needed to ensure we write
  // `VersionEdit::last_sequence_`s in non-decreasing order according to the
  // recovery code's requirement. It also allows us to defer updating
  // `descriptor_last_sequence_` until the apply phase, after the log phase
  // succeeds.
  SequenceNumber max_last_sequence = descriptor_last_sequence_;

  if (first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
    // No group commits for column family add or drop
    LogAndApplyCFHelper(first_writer.edit_list.front(), &max_last_sequence);
    batch_edits.push_back(first_writer.edit_list.front());
    batch_edits_ts_sz.push_back(std::nullopt);
  } else {
    auto it = manifest_writers_.cbegin();
    size_t group_start = std::numeric_limits<size_t>::max();
    while (it != manifest_writers_.cend()) {
      if ((*it)->edit_list.front()->IsColumnFamilyManipulation()) {
        // no group commits for column family add or drop
        break;
      }
      last_writer = *(it++);
      assert(last_writer != nullptr);
      assert(last_writer->cfd != nullptr);
      if (last_writer->cfd->IsDropped()) {
        // If we detect a dropped CF at this point, and the corresponding
        // version edits belong to an atomic group, then we need to find out
        // the preceding version edits in the same atomic group, and update
        // their `remaining_entries_` member variable because we are NOT going
        // to write the version edits' of dropped CF to the MANIFEST. If we
        // don't update, then Recover can report corrupted atomic group because
        // the `remaining_entries_` do not match.
        if (!batch_edits.empty()) {
          if (batch_edits.back()->is_in_atomic_group_ &&
              batch_edits.back()->remaining_entries_ > 0) {
            assert(group_start < batch_edits.size());
            const auto& edit_list = last_writer->edit_list;
            size_t k = 0;
            while (k < edit_list.size()) {
              if (!edit_list[k]->is_in_atomic_group_) {
                break;
              } else if (edit_list[k]->remaining_entries_ == 0) {
                ++k;
                break;
              }
              ++k;
            }
            for (auto i = group_start; i < batch_edits.size(); ++i) {
              assert(static_cast<uint32_t>(k) <=
                     batch_edits.back()->remaining_entries_);
              batch_edits[i]->remaining_entries_ -= static_cast<uint32_t>(k);
            }
          }
        }
        continue;
      }
      // We do a linear search on versions because versions is small.
      // TODO(yanqin) maybe consider unordered_map
      Version* version = nullptr;
      VersionBuilder* builder = nullptr;
      for (int i = 0; i != static_cast<int>(versions.size()); ++i) {
        uint32_t cf_id = last_writer->cfd->GetID();
        if (versions[i]->cfd()->GetID() == cf_id) {
          version = versions[i];
          assert(!builder_guards.empty() &&
                 builder_guards.size() == versions.size());
          builder = builder_guards[i]->version_builder();
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::ProcessManifestWrites:SameColumnFamily", &cf_id);
          break;
        }
      }
      if (version == nullptr) {
        // WAL manipulations do not need to be applied to versions.
        if (!last_writer->IsAllWalEdits()) {
          version = new Version(last_writer->cfd, this, file_options_,
                                last_writer->mutable_cf_options, io_tracer_,
                                current_version_number_++);
          versions.push_back(version);
          mutable_cf_options_ptrs.push_back(&last_writer->mutable_cf_options);
          builder_guards.emplace_back(
              new BaseReferencedVersionBuilder(last_writer->cfd));
          builder = builder_guards.back()->version_builder();
        }
        assert(last_writer->IsAllWalEdits() || builder);
        assert(last_writer->IsAllWalEdits() || version);
        TEST_SYNC_POINT_CALLBACK("VersionSet::ProcessManifestWrites:NewVersion",
                                 version);
      }
      const Comparator* ucmp = last_writer->cfd->user_comparator();
      assert(ucmp);
      std::optional<size_t> edit_ts_sz = ucmp->timestamp_size();
      for (const auto& e : last_writer->edit_list) {
        if (e->is_in_atomic_group_) {
          if (batch_edits.empty() || !batch_edits.back()->is_in_atomic_group_ ||
              (batch_edits.back()->is_in_atomic_group_ &&
               batch_edits.back()->remaining_entries_ == 0)) {
            group_start = batch_edits.size();
          }
        } else if (group_start != std::numeric_limits<size_t>::max()) {
          group_start = std::numeric_limits<size_t>::max();
        }
        Status s = LogAndApplyHelper(last_writer->cfd, builder, e,
                                     &max_last_sequence, mu);
        if (!s.ok()) {
          // free up the allocated memory
          for (auto v : versions) {
            delete v;
          }
          return s;
        }
        batch_edits.push_back(e);
        batch_edits_ts_sz.push_back(edit_ts_sz);
      }
    }
    for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
      assert(!builder_guards.empty() &&
             builder_guards.size() == versions.size());
      auto* builder = builder_guards[i]->version_builder();
      Status s = builder->SaveTo(versions[i]->storage_info());
      if (!s.ok()) {
        // free up the allocated memory
        for (auto v : versions) {
          delete v;
        }
        return s;
      }
    }
  }

#ifndef NDEBUG
  // Verify that version edits of atomic groups have correct
  // remaining_entries_.
  size_t k = 0;
  while (k < batch_edits.size()) {
    while (k < batch_edits.size() && !batch_edits[k]->is_in_atomic_group_) {
      ++k;
    }
    if (k == batch_edits.size()) {
      break;
    }
    size_t i = k;
    while (i < batch_edits.size()) {
      if (!batch_edits[i]->is_in_atomic_group_) {
        break;
      }
      assert(i - k + batch_edits[i]->remaining_entries_ ==
             batch_edits[k]->remaining_entries_);
      if (batch_edits[i]->remaining_entries_ == 0) {
        ++i;
        break;
      }
      ++i;
    }
    assert(batch_edits[i - 1]->is_in_atomic_group_);
    assert(0 == batch_edits[i - 1]->remaining_entries_);
    std::vector<VersionEdit*> tmp;
    for (size_t j = k; j != i; ++j) {
      tmp.emplace_back(batch_edits[j]);
    }
    TEST_SYNC_POINT_CALLBACK(
        "VersionSet::ProcessManifestWrites:CheckOneAtomicGroup", &tmp);
    k = i;
  }
#endif  // NDEBUG

  assert(pending_manifest_file_number_ == 0);
  if (!descriptor_log_ ||
      manifest_file_size_ > db_options_->max_manifest_file_size) {
    TEST_SYNC_POINT("VersionSet::ProcessManifestWrites:BeforeNewManifest");
    new_descriptor_log = true;
  } else {
    pending_manifest_file_number_ = manifest_file_number_;
  }

  // Local cached copy of state variable(s). WriteCurrentStateToManifest()
  // reads its content after releasing db mutex to avoid race with
  // SwitchMemtable().
  std::unordered_map<uint32_t, MutableCFState> curr_state;
  VersionEdit wal_additions;
  if (new_descriptor_log) {
    pending_manifest_file_number_ = NewFileNumber();
    batch_edits.back()->SetNextFile(next_file_number_.load());

    // if we are writing out new snapshot make sure to persist max column
    // family.
    if (column_family_set_->GetMaxColumnFamily() > 0) {
      first_writer.edit_list.front()->SetMaxColumnFamily(
          column_family_set_->GetMaxColumnFamily());
    }
    for (const auto* cfd : *column_family_set_) {
      assert(curr_state.find(cfd->GetID()) == curr_state.end());
      curr_state.emplace(std::make_pair(
          cfd->GetID(),
          MutableCFState(cfd->GetLogNumber(), cfd->GetFullHistoryTsLow())));
    }

    for (const auto& wal : wals_.GetWals()) {
      wal_additions.AddWal(wal.first, wal.second);
    }
  }

  uint64_t new_manifest_file_size = 0;
  Status s;
  IOStatus io_s;
  IOStatus manifest_io_status;
  {
    FileOptions opt_file_opts = fs_->OptimizeForManifestWrite(file_options_);
    mu->Unlock();
    TEST_SYNC_POINT("VersionSet::LogAndApply:WriteManifestStart");
    TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:WriteManifest", nullptr);
    if (!first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        assert(!builder_guards.empty() &&
               builder_guards.size() == versions.size());
        assert(!mutable_cf_options_ptrs.empty() &&
               builder_guards.size() == versions.size());
        ColumnFamilyData* cfd = versions[i]->cfd_;
        s = builder_guards[i]->version_builder()->LoadTableHandlers(
            cfd->internal_stats(), 1 /* max_threads */,
            true /* prefetch_index_and_filter_in_cache */,
            false /* is_initial_load */,
            mutable_cf_options_ptrs[i]->prefix_extractor,
            MaxFileSizeForL0MetaPin(*mutable_cf_options_ptrs[i]), read_options,
            mutable_cf_options_ptrs[i]->block_protection_bytes_per_key);
        if (!s.ok()) {
          if (db_options_->paranoid_checks) {
            break;
          }
          s = Status::OK();
        }
      }
    }

    if (s.ok() && new_descriptor_log) {
      // This is fine because everything inside of this block is serialized --
      // only one thread can be here at the same time
      // create new manifest file
      ROCKS_LOG_INFO(db_options_->info_log, "Creating manifest %" PRIu64 "\n",
                     pending_manifest_file_number_);
      std::string descriptor_fname =
          DescriptorFileName(dbname_, pending_manifest_file_number_);
      std::unique_ptr<FSWritableFile> descriptor_file;
      io_s = NewWritableFile(fs_.get(), descriptor_fname, &descriptor_file,
                             opt_file_opts);
      if (io_s.ok()) {
        descriptor_file->SetPreallocationBlockSize(
            db_options_->manifest_preallocation_size);
        FileTypeSet tmp_set = db_options_->checksum_handoff_file_types;
        std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
            std::move(descriptor_file), descriptor_fname, opt_file_opts, clock_,
            io_tracer_, nullptr, db_options_->listeners, nullptr,
            tmp_set.Contains(FileType::kDescriptorFile),
            tmp_set.Contains(FileType::kDescriptorFile)));
        descriptor_log_.reset(
            new log::Writer(std::move(file_writer), 0, false));
        s = WriteCurrentStateToManifest(curr_state, wal_additions,
                                        descriptor_log_.get(), io_s);
      } else {
        manifest_io_status = io_s;
        s = io_s;
      }
    }

    if (s.ok()) {
      if (!first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
        constexpr bool update_stats = true;

        for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
          versions[i]->PrepareAppend(*mutable_cf_options_ptrs[i], read_options,
                                     update_stats);
        }
      }

      // Write new records to MANIFEST log
#ifndef NDEBUG
      size_t idx = 0;
#endif
      assert(batch_edits.size() == batch_edits_ts_sz.size());
      for (size_t bidx = 0; bidx < batch_edits.size(); bidx++) {
        auto& e = batch_edits[bidx];
        std::string record;
        if (!e->EncodeTo(&record, batch_edits_ts_sz[bidx])) {
          s = Status::Corruption("Unable to encode VersionEdit:" +
                                 e->DebugString(true));
          break;
        }
        TEST_KILL_RANDOM_WITH_WEIGHT("VersionSet::LogAndApply:BeforeAddRecord",
                                     REDUCE_ODDS2);
#ifndef NDEBUG
        if (batch_edits.size() > 1 && batch_edits.size() - 1 == idx) {
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
              nullptr);
          TEST_SYNC_POINT(
              "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:1");
        }
        ++idx;
#endif /* !NDEBUG */
        io_s = descriptor_log_->AddRecord(record);
        if (!io_s.ok()) {
          s = io_s;
          manifest_io_status = io_s;
          break;
        }
      }

      if (s.ok()) {
        io_s = SyncManifest(db_options_, descriptor_log_->file());
        manifest_io_status = io_s;
        TEST_SYNC_POINT_CALLBACK(
            "VersionSet::ProcessManifestWrites:AfterSyncManifest", &io_s);
      }
      if (!io_s.ok()) {
        s = io_s;
        ROCKS_LOG_ERROR(db_options_->info_log, "MANIFEST write %s\n",
                        s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok()) {
      assert(manifest_io_status.ok());
    }
    if (s.ok() && new_descriptor_log) {
      io_s = SetCurrentFile(fs_.get(), dbname_, pending_manifest_file_number_,
                            dir_contains_current_file);
      if (!io_s.ok()) {
        s = io_s;
      }
    }

    if (s.ok()) {
      // find offset in manifest file where this version is stored.
      new_manifest_file_size = descriptor_log_->file()->GetFileSize();
    }

    if (first_writer.edit_list.front()->is_column_family_drop_) {
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:0");
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:1");
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:2");
    }

    LogFlush(db_options_->info_log);
    TEST_SYNC_POINT("VersionSet::LogAndApply:WriteManifestDone");
    mu->Lock();
  }

  if (s.ok()) {
    // Apply WAL edits, DB mutex must be held.
    for (auto& e : batch_edits) {
      if (e->IsWalAddition()) {
        s = wals_.AddWals(e->GetWalAdditions());
      } else if (e->IsWalDeletion()) {
        s = wals_.DeleteWalsBefore(e->GetWalDeletion().GetLogNumber());
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  if (!io_s.ok()) {
    if (io_status_.ok()) {
      io_status_ = io_s;
    }
  } else if (!io_status_.ok()) {
    io_status_ = io_s;
  }

  // Append the old manifest file to the obsolete_manifest_ list to be deleted
  // by PurgeObsoleteFiles later.
  if (s.ok() && new_descriptor_log) {
    obsolete_manifests_.emplace_back(
        DescriptorFileName("", manifest_file_number_));
  }

  // Install the new versions
  if (s.ok()) {
    if (first_writer.edit_list.front()->is_column_family_add_) {
      assert(batch_edits.size() == 1);
      assert(new_cf_options != nullptr);
      assert(max_last_sequence == descriptor_last_sequence_);
      CreateColumnFamily(*new_cf_options, read_options,
                         first_writer.edit_list.front());
    } else if (first_writer.edit_list.front()->is_column_family_drop_) {
      assert(batch_edits.size() == 1);
      assert(max_last_sequence == descriptor_last_sequence_);
      first_writer.cfd->SetDropped();
      first_writer.cfd->UnrefAndTryDelete();
    } else {
      // Each version in versions corresponds to a column family.
      // For each column family, update its log number indicating that logs
      // with number smaller than this should be ignored.
      uint64_t last_min_log_number_to_keep = 0;
      for (const auto& e : batch_edits) {
        ColumnFamilyData* cfd = nullptr;
        if (!e->IsColumnFamilyManipulation()) {
          cfd = column_family_set_->GetColumnFamily(e->column_family_);
          // e would not have been added to batch_edits if its corresponding
          // column family is dropped.
          assert(cfd);
        }
        if (cfd) {
          if (e->has_log_number_ && e->log_number_ > cfd->GetLogNumber()) {
            cfd->SetLogNumber(e->log_number_);
          }
          if (e->HasFullHistoryTsLow()) {
            cfd->SetFullHistoryTsLow(e->GetFullHistoryTsLow());
          }
        }
        if (e->has_min_log_number_to_keep_) {
          last_min_log_number_to_keep =
              std::max(last_min_log_number_to_keep, e->min_log_number_to_keep_);
        }
      }

      if (last_min_log_number_to_keep != 0) {
        MarkMinLogNumberToKeep(last_min_log_number_to_keep);
      }

      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        ColumnFamilyData* cfd = versions[i]->cfd_;
        AppendVersion(cfd, versions[i]);
      }
    }
    assert(max_last_sequence >= descriptor_last_sequence_);
    descriptor_last_sequence_ = max_last_sequence;
    manifest_file_number_ = pending_manifest_file_number_;
    manifest_file_size_ = new_manifest_file_size;
    prev_log_number_ = first_writer.edit_list.front()->prev_log_number_;
  } else {
    std::string version_edits;
    for (auto& e : batch_edits) {
      version_edits += ("\n" + e->DebugString(true));
    }
    ROCKS_LOG_ERROR(db_options_->info_log,
                    "Error in committing version edit to MANIFEST: %s",
                    version_edits.c_str());
    for (auto v : versions) {
      delete v;
    }
    if (manifest_io_status.ok()) {
      manifest_file_number_ = pending_manifest_file_number_;
      manifest_file_size_ = new_manifest_file_size;
    }
    // If manifest append failed for whatever reason, the file could be
    // corrupted. So we need to force the next version update to start a
    // new manifest file.
    descriptor_log_.reset();
    // If manifest operations failed, then we know the CURRENT file still
    // points to the original MANIFEST. Therefore, we can safely delete the
    // new MANIFEST.
    // If manifest operations succeeded, and we are here, then it is possible
    // that renaming tmp file to CURRENT failed.
    //
    // On local POSIX-compliant FS, the CURRENT must point to the original
    // MANIFEST. We can delete the new MANIFEST for simplicity, but we can also
    // keep it. Future recovery will ignore this MANIFEST. It's also ok for the
    // process not to crash and continue using the db. Any future LogAndApply()
    // call will switch to a new MANIFEST and update CURRENT, still ignoring
    // this one.
    //
    // On non-local FS, it is
    // possible that the rename operation succeeded on the server (remote)
    // side, but the client somehow returns a non-ok status to RocksDB. Note
    // that this does not violate atomicity. Should we delete the new MANIFEST
    // successfully, a subsequent recovery attempt will likely see the CURRENT
    // pointing to the new MANIFEST, thus fail. We will not be able to open the
    // DB again. Therefore, if manifest operations succeed, we should keep the
    // the new MANIFEST. If the process proceeds, any future LogAndApply() call
    // will switch to a new MANIFEST and update CURRENT. If user tries to
    // re-open the DB,
    // a) CURRENT points to the new MANIFEST, and the new MANIFEST is present.
    // b) CURRENT points to the original MANIFEST, and the original MANIFEST
    //    also exists.
    if (new_descriptor_log && !manifest_io_status.ok()) {
      ROCKS_LOG_INFO(db_options_->info_log,
                     "Deleting manifest %" PRIu64 " current manifest %" PRIu64
                     "\n",
                     pending_manifest_file_number_, manifest_file_number_);
      Status manifest_del_status = env_->DeleteFile(
          DescriptorFileName(dbname_, pending_manifest_file_number_));
      if (!manifest_del_status.ok()) {
        ROCKS_LOG_WARN(db_options_->info_log,
                       "Failed to delete manifest %" PRIu64 ": %s",
                       pending_manifest_file_number_,
                       manifest_del_status.ToString().c_str());
      }
    }
  }

  pending_manifest_file_number_ = 0;

#ifndef NDEBUG
  // This is here kind of awkwardly because there's no other consistency
  // checks on `VersionSet`'s updates for the new `Version`s. We might want
  // to move it to a dedicated function, or remove it if we gain enough
  // confidence in `descriptor_last_sequence_`.
  if (s.ok()) {
    for (const auto* v : versions) {
      const auto* vstorage = v->storage_info();
      for (int level = 0; level < vstorage->num_levels(); ++level) {
        for (const auto& file : vstorage->LevelFiles(level)) {
          assert(file->fd.largest_seqno <= descriptor_last_sequence_);
        }
      }
    }
  }
#endif  // NDEBUG

  // wake up all the waiting writers
  while (true) {
    ManifestWriter* ready = manifest_writers_.front();
    manifest_writers_.pop_front();
    bool need_signal = true;
    for (const auto& w : writers) {
      if (&w == ready) {
        need_signal = false;
        break;
      }
    }
    ready->status = s;
    ready->done = true;
    if (ready->manifest_write_callback) {
      (ready->manifest_write_callback)(s);
    }
    if (need_signal) {
      ready->cv.Signal();
    }
    if (ready == last_writer) {
      break;
    }
  }
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
  return s;
}

void VersionSet::WakeUpWaitingManifestWriters() {
  // wake up all the waiting writers
  // Notify new head of manifest write queue.
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
}

// 'datas' is grammatically incorrect. We still use this notation to indicate
// that this variable represents a collection of column_family_data.
Status VersionSet::LogAndApply(
    const autovector<ColumnFamilyData*>& column_family_datas,
    const autovector<const MutableCFOptions*>& mutable_cf_options_list,
    const ReadOptions& read_options,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    InstrumentedMutex* mu, FSDirectory* dir_contains_current_file,
    bool new_descriptor_log, const ColumnFamilyOptions* new_cf_options,
    const std::vector<std::function<void(const Status&)>>& manifest_wcbs) {
  mu->AssertHeld();
  int num_edits = 0;
  for (const auto& elist : edit_lists) {
    num_edits += static_cast<int>(elist.size());
  }
  if (num_edits == 0) {
    return Status::OK();
  } else if (num_edits > 1) {
#ifndef NDEBUG
    for (const auto& edit_list : edit_lists) {
      for (const auto& edit : edit_list) {
        assert(!edit->IsColumnFamilyManipulation());
      }
    }
#endif /* ! NDEBUG */
  }

  int num_cfds = static_cast<int>(column_family_datas.size());
  if (num_cfds == 1 && column_family_datas[0] == nullptr) {
    assert(edit_lists.size() == 1 && edit_lists[0].size() == 1);
    assert(edit_lists[0][0]->is_column_family_add_);
    assert(new_cf_options != nullptr);
  }
  std::deque<ManifestWriter> writers;
  if (num_cfds > 0) {
    assert(static_cast<size_t>(num_cfds) == mutable_cf_options_list.size());
    assert(static_cast<size_t>(num_cfds) == edit_lists.size());
  }
  for (int i = 0; i < num_cfds; ++i) {
    const auto wcb =
        manifest_wcbs.empty() ? [](const Status&) {} : manifest_wcbs[i];
    writers.emplace_back(mu, column_family_datas[i],
                         *mutable_cf_options_list[i], edit_lists[i], wcb);
    manifest_writers_.push_back(&writers[i]);
  }
  assert(!writers.empty());
  ManifestWriter& first_writer = writers.front();
  TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:BeforeWriterWaiting",
                           nullptr);
  while (!first_writer.done && &first_writer != manifest_writers_.front()) {
    first_writer.cv.Wait();
  }
  if (first_writer.done) {
    // All non-CF-manipulation operations can be grouped together and committed
    // to MANIFEST. They should all have finished. The status code is stored in
    // the first manifest writer.
#ifndef NDEBUG
    for (const auto& writer : writers) {
      assert(writer.done);
    }
    TEST_SYNC_POINT_CALLBACK("VersionSet::LogAndApply:WakeUpAndDone", mu);
#endif /* !NDEBUG */
    return first_writer.status;
  }

  int num_undropped_cfds = 0;
  for (auto cfd : column_family_datas) {
    // if cfd == nullptr, it is a column family add.
    if (cfd == nullptr || !cfd->IsDropped()) {
      ++num_undropped_cfds;
    }
  }
  if (0 == num_undropped_cfds) {
    for (int i = 0; i != num_cfds; ++i) {
      manifest_writers_.pop_front();
    }
    // Notify new head of manifest write queue.
    if (!manifest_writers_.empty()) {
      manifest_writers_.front()->cv.Signal();
    }
    return Status::ColumnFamilyDropped();
  }
  return ProcessManifestWrites(writers, mu, dir_contains_current_file,
                               new_descriptor_log, new_cf_options,
                               read_options);
}

void VersionSet::LogAndApplyCFHelper(VersionEdit* edit,
                                     SequenceNumber* max_last_sequence) {
  assert(max_last_sequence != nullptr);
  assert(edit->IsColumnFamilyManipulation());
  edit->SetNextFile(next_file_number_.load());
  assert(!edit->HasLastSequence());
  edit->SetLastSequence(*max_last_sequence);
  if (edit->is_column_family_drop_) {
    // if we drop column family, we have to make sure to save max column family,
    // so that we don't reuse existing ID
    edit->SetMaxColumnFamily(column_family_set_->GetMaxColumnFamily());
  }
}

Status VersionSet::LogAndApplyHelper(ColumnFamilyData* cfd,
                                     VersionBuilder* builder, VersionEdit* edit,
                                     SequenceNumber* max_last_sequence,
                                     InstrumentedMutex* mu) {
#ifdef NDEBUG
  (void)cfd;
#endif
  mu->AssertHeld();
  assert(!edit->IsColumnFamilyManipulation());
  assert(max_last_sequence != nullptr);

  if (edit->has_log_number_) {
    assert(edit->log_number_ >= cfd->GetLogNumber());
    assert(edit->log_number_ < next_file_number_.load());
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_.load());
  if (edit->HasLastSequence() && edit->GetLastSequence() > *max_last_sequence) {
    *max_last_sequence = edit->GetLastSequence();
  } else {
    edit->SetLastSequence(*max_last_sequence);
  }

  // The builder can be nullptr only if edit is WAL manipulation,
  // because WAL edits do not need to be applied to versions,
  // we return Status::OK() in this case.
  assert(builder || edit->IsWalManipulation());
  return builder ? builder->Apply(edit) : Status::OK();
}

Status VersionSet::GetCurrentManifestPath(const std::string& dbname,
                                          FileSystem* fs,
                                          std::string* manifest_path,
                                          uint64_t* manifest_file_number) {
  assert(fs != nullptr);
  assert(manifest_path != nullptr);
  assert(manifest_file_number != nullptr);

  std::string fname;
  Status s = ReadFileToString(fs, CurrentFileName(dbname), &fname);
  if (!s.ok()) {
    return s;
  }
  if (fname.empty() || fname.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  // remove the trailing '\n'
  fname.resize(fname.size() - 1);
  FileType type;
  bool parse_ok = ParseFileName(fname, manifest_file_number, &type);
  if (!parse_ok || type != kDescriptorFile) {
    return Status::Corruption("CURRENT file corrupted");
  }
  *manifest_path = dbname;
  if (dbname.back() != '/') {
    manifest_path->push_back('/');
  }
  manifest_path->append(fname);
  return Status::OK();
}

Status VersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    std::string* db_id, bool no_error_if_files_missing) {
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  // Read "CURRENT" file, which contains a pointer to the current manifest
  // file
  std::string manifest_path;
  Status s = GetCurrentManifestPath(dbname_, fs_.get(), &manifest_path,
                                    &manifest_file_number_);
  if (!s.ok()) {
    return s;
  }

  ROCKS_LOG_INFO(db_options_->info_log, "Recovering from manifest file: %s\n",
                 manifest_path.c_str());

  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  {
    std::unique_ptr<FSSequentialFile> manifest_file;
    s = fs_->NewSequentialFile(manifest_path,
                               fs_->OptimizeForManifestRead(file_options_),
                               &manifest_file, nullptr);
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(new SequentialFileReader(
        std::move(manifest_file), manifest_path,
        db_options_->log_readahead_size, io_tracer_, db_options_->listeners));
  }
  uint64_t current_manifest_file_size = 0;
  uint64_t log_number = 0;
  {
    VersionSet::LogReporter reporter;
    Status log_read_status;
    reporter.status = &log_read_status;
    log::Reader reader(nullptr, std::move(manifest_file_reader), &reporter,
                       true /* checksum */, 0 /* log_number */);
    VersionEditHandler handler(
        read_only, column_families, const_cast<VersionSet*>(this),
        /*track_missing_files=*/false, no_error_if_files_missing, io_tracer_,
        read_options, EpochNumberRequirement::kMightMissing);
    handler.Iterate(reader, &log_read_status);
    s = handler.status();
    if (s.ok()) {
      log_number = handler.GetVersionEditParams().log_number_;
      current_manifest_file_size = reader.GetReadOffset();
      assert(current_manifest_file_size != 0);
      handler.GetDbId(db_id);
    }
    if (s.ok()) {
      RecoverEpochNumbers();
    }
  }

  if (s.ok()) {
    manifest_file_size_ = current_manifest_file_size;
    ROCKS_LOG_INFO(
        db_options_->info_log,
        "Recovered from manifest file:%s succeeded,"
        "manifest_file_number is %" PRIu64 ", next_file_number is %" PRIu64
        ", last_sequence is %" PRIu64 ", log_number is %" PRIu64
        ",prev_log_number is %" PRIu64 ",max_column_family is %" PRIu32
        ",min_log_number_to_keep is %" PRIu64 "\n",
        manifest_path.c_str(), manifest_file_number_, next_file_number_.load(),
        last_sequence_.load(), log_number, prev_log_number_,
        column_family_set_->GetMaxColumnFamily(), min_log_number_to_keep());

    for (auto cfd : *column_family_set_) {
      if (cfd->IsDropped()) {
        continue;
      }
      ROCKS_LOG_INFO(db_options_->info_log,
                     "Column family [%s] (ID %" PRIu32
                     "), log number is %" PRIu64 "\n",
                     cfd->GetName().c_str(), cfd->GetID(), cfd->GetLogNumber());
    }
  }

  return s;
}

namespace {
class ManifestPicker {
 public:
  explicit ManifestPicker(const std::string& dbname,
                          const std::vector<std::string>& files_in_dbname);
  // REQUIRES Valid() == true
  std::string GetNextManifest(uint64_t* file_number, std::string* file_name);
  bool Valid() const { return manifest_file_iter_ != manifest_files_.end(); }

 private:
  const std::string& dbname_;
  // MANIFEST file names(s)
  std::vector<std::string> manifest_files_;
  std::vector<std::string>::const_iterator manifest_file_iter_;
};

ManifestPicker::ManifestPicker(const std::string& dbname,
                               const std::vector<std::string>& files_in_dbname)
    : dbname_(dbname) {
  // populate manifest files
  assert(!files_in_dbname.empty());
  for (const auto& fname : files_in_dbname) {
    uint64_t file_num = 0;
    FileType file_type;
    bool parse_ok = ParseFileName(fname, &file_num, &file_type);
    if (parse_ok && file_type == kDescriptorFile) {
      manifest_files_.push_back(fname);
    }
  }
  // seek to first manifest
  std::sort(manifest_files_.begin(), manifest_files_.end(),
            [](const std::string& lhs, const std::string& rhs) {
              uint64_t num1 = 0;
              uint64_t num2 = 0;
              FileType type1;
              FileType type2;
              bool parse_ok1 = ParseFileName(lhs, &num1, &type1);
              bool parse_ok2 = ParseFileName(rhs, &num2, &type2);
#ifndef NDEBUG
              assert(parse_ok1);
              assert(parse_ok2);
#else
              (void)parse_ok1;
              (void)parse_ok2;
#endif
              return num1 > num2;
            });
  manifest_file_iter_ = manifest_files_.begin();
}

std::string ManifestPicker::GetNextManifest(uint64_t* number,
                                            std::string* file_name) {
  assert(Valid());
  std::string ret;
  if (manifest_file_iter_ != manifest_files_.end()) {
    ret.assign(dbname_);
    if (ret.back() != kFilePathSeparator) {
      ret.push_back(kFilePathSeparator);
    }
    ret.append(*manifest_file_iter_);
    if (number) {
      FileType type;
      bool parse = ParseFileName(*manifest_file_iter_, number, &type);
      assert(type == kDescriptorFile);
#ifndef NDEBUG
      assert(parse);
#else
      (void)parse;
#endif
    }
    if (file_name) {
      *file_name = *manifest_file_iter_;
    }
    ++manifest_file_iter_;
  }
  return ret;
}
}  // anonymous namespace

Status VersionSet::TryRecover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    const std::vector<std::string>& files_in_dbname, std::string* db_id,
    bool* has_missing_table_file) {
  ManifestPicker manifest_picker(dbname_, files_in_dbname);
  if (!manifest_picker.Valid()) {
    return Status::Corruption("Cannot locate MANIFEST file in " + dbname_);
  }
  Status s;
  std::string manifest_path =
      manifest_picker.GetNextManifest(&manifest_file_number_, nullptr);
  while (!manifest_path.empty()) {
    s = TryRecoverFromOneManifest(manifest_path, column_families, read_only,
                                  db_id, has_missing_table_file);
    if (s.ok() || !manifest_picker.Valid()) {
      break;
    }
    Reset();
    manifest_path =
        manifest_picker.GetNextManifest(&manifest_file_number_, nullptr);
  }
  return s;
}

Status VersionSet::TryRecoverFromOneManifest(
    const std::string& manifest_path,
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    std::string* db_id, bool* has_missing_table_file) {
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  ROCKS_LOG_INFO(db_options_->info_log, "Trying to recover from manifest: %s\n",
                 manifest_path.c_str());
  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  Status s;
  {
    std::unique_ptr<FSSequentialFile> manifest_file;
    s = fs_->NewSequentialFile(manifest_path,
                               fs_->OptimizeForManifestRead(file_options_),
                               &manifest_file, nullptr);
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(new SequentialFileReader(
        std::move(manifest_file), manifest_path,
        db_options_->log_readahead_size, io_tracer_, db_options_->listeners));
  }

  assert(s.ok());
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(manifest_file_reader), &reporter,
                     /*checksum=*/true, /*log_num=*/0);
  VersionEditHandlerPointInTime handler_pit(
      read_only, column_families, const_cast<VersionSet*>(this), io_tracer_,
      read_options, EpochNumberRequirement::kMightMissing);

  handler_pit.Iterate(reader, &s);

  handler_pit.GetDbId(db_id);

  assert(nullptr != has_missing_table_file);
  *has_missing_table_file = handler_pit.HasMissingFiles();

  s = handler_pit.status();
  if (s.ok()) {
    RecoverEpochNumbers();
  }
  return s;
}

void VersionSet::RecoverEpochNumbers() {
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    assert(cfd->initialized());
    cfd->RecoverEpochNumbers();
  }
}

Status VersionSet::ListColumnFamilies(std::vector<std::string>* column_families,
                                      const std::string& dbname,
                                      FileSystem* fs) {
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string manifest_path;
  uint64_t manifest_file_number;
  Status s =
      GetCurrentManifestPath(dbname, fs, &manifest_path, &manifest_file_number);
  if (!s.ok()) {
    return s;
  }
  return ListColumnFamiliesFromManifest(manifest_path, fs, column_families);
}

Status VersionSet::ListColumnFamiliesFromManifest(
    const std::string& manifest_path, FileSystem* fs,
    std::vector<std::string>* column_families) {
  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;
  std::unique_ptr<SequentialFileReader> file_reader;
  Status s;
  {
    std::unique_ptr<FSSequentialFile> file;
    // these are just for performance reasons, not correctness,
    // so we're fine using the defaults
    s = fs->NewSequentialFile(manifest_path, FileOptions(), &file, nullptr);
    if (!s.ok()) {
      return s;
    }
    file_reader = std::make_unique<SequentialFileReader>(
        std::move(file), manifest_path, /*io_tracer=*/nullptr);
  }

  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /* checksum */, 0 /* log_number */);

  ListColumnFamiliesHandler handler(read_options);
  handler.Iterate(reader, &s);

  assert(column_families);
  column_families->clear();
  if (handler.status().ok()) {
    for (const auto& iter : handler.GetColumnFamilyNames()) {
      column_families->push_back(iter.second);
    }
  }

  return handler.status();
}

Status VersionSet::ReduceNumberOfLevels(const std::string& dbname,
                                        const Options* options,
                                        const FileOptions& file_options,
                                        int new_levels) {
  if (new_levels <= 1) {
    return Status::InvalidArgument(
        "Number of levels needs to be bigger than 1");
  }

  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;

  ImmutableDBOptions db_options(*options);
  ColumnFamilyOptions cf_options(*options);
  std::shared_ptr<Cache> tc(NewLRUCache(options->max_open_files - 10,
                                        options->table_cache_numshardbits));
  WriteController wc(options->delayed_write_rate);
  WriteBufferManager wb(options->db_write_buffer_size);
  VersionSet versions(dbname, &db_options, file_options, tc.get(), &wb, &wc,
                      nullptr /*BlockCacheTracer*/, nullptr /*IOTracer*/,
                      /*db_id*/ "",
                      /*db_session_id*/ "");
  Status status;

  std::vector<ColumnFamilyDescriptor> dummy;
  ColumnFamilyDescriptor dummy_descriptor(kDefaultColumnFamilyName,
                                          ColumnFamilyOptions(*options));
  dummy.push_back(dummy_descriptor);
  status = versions.Recover(dummy);
  if (!status.ok()) {
    return status;
  }

  Version* current_version =
      versions.GetColumnFamilySet()->GetDefault()->current();
  auto* vstorage = current_version->storage_info();
  int current_levels = vstorage->num_levels();

  if (current_levels <= new_levels) {
    return Status::OK();
  }

  // Make sure there are file only on one level from
  // (new_levels-1) to (current_levels-1)
  int first_nonempty_level = -1;
  int first_nonempty_level_filenum = 0;
  for (int i = new_levels - 1; i < current_levels; i++) {
    int file_num = vstorage->NumLevelFiles(i);
    if (file_num != 0) {
      if (first_nonempty_level < 0) {
        first_nonempty_level = i;
        first_nonempty_level_filenum = file_num;
      } else {
        char msg[255];
        snprintf(msg, sizeof(msg),
                 "Found at least two levels containing files: "
                 "[%d:%d],[%d:%d].\n",
                 first_nonempty_level, first_nonempty_level_filenum, i,
                 file_num);
        return Status::InvalidArgument(msg);
      }
    }
  }

  // we need to allocate an array with the old number of levels size to
  // avoid SIGSEGV in WriteCurrentStatetoManifest()
  // however, all levels bigger or equal to new_levels will be empty
  std::vector<FileMetaData*>* new_files_list =
      new std::vector<FileMetaData*>[current_levels];
  for (int i = 0; i < new_levels - 1; i++) {
    new_files_list[i] = vstorage->LevelFiles(i);
  }

  if (first_nonempty_level > 0) {
    auto& new_last_level = new_files_list[new_levels - 1];

    new_last_level = vstorage->LevelFiles(first_nonempty_level);

    for (size_t i = 0; i < new_last_level.size(); ++i) {
      const FileMetaData* const meta = new_last_level[i];
      assert(meta);

      const uint64_t file_number = meta->fd.GetNumber();

      vstorage->file_locations_[file_number] =
          VersionStorageInfo::FileLocation(new_levels - 1, i);
    }
  }

  delete[] vstorage->files_;
  vstorage->files_ = new_files_list;
  vstorage->num_levels_ = new_levels;
  vstorage->ResizeCompactCursors(new_levels);

  MutableCFOptions mutable_cf_options(*options);
  VersionEdit ve;
  InstrumentedMutex dummy_mutex;
  InstrumentedMutexLock l(&dummy_mutex);
  return versions.LogAndApply(versions.GetColumnFamilySet()->GetDefault(),
                              mutable_cf_options, read_options, &ve,
                              &dummy_mutex, nullptr, true);
}

// Get the checksum information including the checksum and checksum function
// name of all SST and blob files in VersionSet. Store the information in
// FileChecksumList which contains a map from file number to its checksum info.
// If DB is not running, make sure call VersionSet::Recover() to load the file
// metadata from Manifest to VersionSet before calling this function.
Status VersionSet::GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) {
  // Clean the previously stored checksum information if any.
  Status s;
  if (checksum_list == nullptr) {
    s = Status::InvalidArgument("checksum_list is nullptr");
    return s;
  }
  checksum_list->reset();

  for (auto cfd : *column_family_set_) {
    assert(cfd);

    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }

    const auto* current = cfd->current();
    assert(current);

    const auto* vstorage = current->storage_info();
    assert(vstorage);

    /* SST files */
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      const auto& level_files = vstorage->LevelFiles(level);

      for (const auto& file : level_files) {
        assert(file);

        s = checksum_list->InsertOneFileChecksum(file->fd.GetNumber(),
                                                 file->file_checksum,
                                                 file->file_checksum_func_name);
        if (!s.ok()) {
          return s;
        }
      }
    }

    /* Blob files */
    const auto& blob_files = vstorage->GetBlobFiles();
    for (const auto& meta : blob_files) {
      assert(meta);

      std::string checksum_value = meta->GetChecksumValue();
      std::string checksum_method = meta->GetChecksumMethod();
      assert(checksum_value.empty() == checksum_method.empty());
      if (meta->GetChecksumMethod().empty()) {
        checksum_value = kUnknownFileChecksum;
        checksum_method = kUnknownFileChecksumFuncName;
      }

      s = checksum_list->InsertOneFileChecksum(meta->GetBlobFileNumber(),
                                               checksum_value, checksum_method);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return s;
}

Status VersionSet::DumpManifest(
    Options& options, std::string& dscname, bool verbose, bool hex, bool json,
    const std::vector<ColumnFamilyDescriptor>& cf_descs) {
  assert(options.env);
  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;

  std::vector<std::string> column_families;
  Status s = ListColumnFamiliesFromManifest(
      dscname, options.env->GetFileSystem().get(), &column_families);
  if (!s.ok()) {
    return s;
  }

  // Open the specified manifest file.
  std::unique_ptr<SequentialFileReader> file_reader;
  {
    std::unique_ptr<FSSequentialFile> file;
    const std::shared_ptr<FileSystem>& fs = options.env->GetFileSystem();
    s = fs->NewSequentialFile(
        dscname, fs->OptimizeForManifestRead(file_options_), &file, nullptr);
    if (!s.ok()) {
      return s;
    }
    file_reader = std::make_unique<SequentialFileReader>(
        std::move(file), dscname, db_options_->log_readahead_size, io_tracer_);
  }

  std::map<std::string, const ColumnFamilyDescriptor*> cf_name_to_desc;
  for (const auto& cf_desc : cf_descs) {
    cf_name_to_desc[cf_desc.name] = &cf_desc;
  }
  std::vector<ColumnFamilyDescriptor> final_cf_descs;
  for (const auto& cf : column_families) {
    const auto iter = cf_name_to_desc.find(cf);
    if (iter != cf_name_to_desc.cend()) {
      final_cf_descs.push_back(*iter->second);
    } else {
      final_cf_descs.emplace_back(cf, options);
    }
  }

  DumpManifestHandler handler(final_cf_descs, this, io_tracer_, read_options,
                              verbose, hex, json);
  {
    VersionSet::LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file_reader), &reporter,
                       true /* checksum */, 0 /* log_number */);
    handler.Iterate(reader, &s);
  }

  return handler.status();
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  // only called during recovery and repair which are single threaded, so this
  // works because there can't be concurrent calls
  if (next_file_number_.load(std::memory_order_relaxed) <= number) {
    next_file_number_.store(number + 1, std::memory_order_relaxed);
  }
}
// Called only either from ::LogAndApply which is protected by mutex or during
// recovery which is single-threaded.
void VersionSet::MarkMinLogNumberToKeep(uint64_t number) {
  if (min_log_number_to_keep_.load(std::memory_order_relaxed) < number) {
    min_log_number_to_keep_.store(number, std::memory_order_relaxed);
  }
}

Status VersionSet::WriteCurrentStateToManifest(
    const std::unordered_map<uint32_t, MutableCFState>& curr_state,
    const VersionEdit& wal_additions, log::Writer* log, IOStatus& io_s) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // WARNING: This method doesn't hold a mutex!!

  // This is done without DB mutex lock held, but only within single-threaded
  // LogAndApply. Column family manipulations can only happen within LogAndApply
  // (the same single thread), so we're safe to iterate.

  assert(io_s.ok());
  if (db_options_->write_dbid_to_manifest) {
    VersionEdit edit_for_db_id;
    assert(!db_id_.empty());
    edit_for_db_id.SetDBId(db_id_);
    std::string db_id_record;
    if (!edit_for_db_id.EncodeTo(&db_id_record)) {
      return Status::Corruption("Unable to Encode VersionEdit:" +
                                edit_for_db_id.DebugString(true));
    }
    io_s = log->AddRecord(db_id_record);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // Save WALs.
  if (!wal_additions.GetWalAdditions().empty()) {
    TEST_SYNC_POINT_CALLBACK("VersionSet::WriteCurrentStateToManifest:SaveWal",
                             const_cast<VersionEdit*>(&wal_additions));
    std::string record;
    if (!wal_additions.EncodeTo(&record)) {
      return Status::Corruption("Unable to Encode VersionEdit: " +
                                wal_additions.DebugString(true));
    }
    io_s = log->AddRecord(record);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  // New manifest should rollover the WAL deletion record from previous
  // manifest. Otherwise, when an addition record of a deleted WAL gets added to
  // this new manifest later (which can happens in e.g, SyncWAL()), this new
  // manifest creates an illusion that such WAL hasn't been deleted.
  VersionEdit wal_deletions;
  wal_deletions.DeleteWalsBefore(min_log_number_to_keep());
  std::string wal_deletions_record;
  if (!wal_deletions.EncodeTo(&wal_deletions_record)) {
    return Status::Corruption("Unable to Encode VersionEdit: " +
                              wal_deletions.DebugString(true));
  }
  io_s = log->AddRecord(wal_deletions_record);
  if (!io_s.ok()) {
    return io_s;
  }

  for (auto cfd : *column_family_set_) {
    assert(cfd);

    if (cfd->IsDropped()) {
      continue;
    }
    assert(cfd->initialized());
    {
      // Store column family info
      VersionEdit edit;
      if (cfd->GetID() != 0) {
        // default column family is always there,
        // no need to explicitly write it
        edit.AddColumnFamily(cfd->GetName());
        edit.SetColumnFamily(cfd->GetID());
      }
      edit.SetComparatorName(
          cfd->internal_comparator().user_comparator()->Name());
      std::string record;
      if (!edit.EncodeTo(&record)) {
        return Status::Corruption("Unable to Encode VersionEdit:" +
                                  edit.DebugString(true));
      }
      io_s = log->AddRecord(record);
      if (!io_s.ok()) {
        return io_s;
      }
    }

    {
      // Save files
      VersionEdit edit;
      edit.SetColumnFamily(cfd->GetID());

      const auto* current = cfd->current();
      assert(current);

      const auto* vstorage = current->storage_info();
      assert(vstorage);

      for (int level = 0; level < cfd->NumberLevels(); level++) {
        const auto& level_files = vstorage->LevelFiles(level);

        for (const auto& f : level_files) {
          assert(f);

          edit.AddFile(level, f->fd.GetNumber(), f->fd.GetPathId(),
                       f->fd.GetFileSize(), f->smallest, f->largest,
                       f->fd.smallest_seqno, f->fd.largest_seqno,
                       f->marked_for_compaction, f->temperature,
                       f->oldest_blob_file_number, f->oldest_ancester_time,
                       f->file_creation_time, f->epoch_number, f->file_checksum,
                       f->file_checksum_func_name, f->unique_id,
                       f->compensated_range_deletion_size, f->tail_size,
                       f->user_defined_timestamps_persisted);
        }
      }

      edit.SetCompactCursors(vstorage->GetCompactCursors());

      const auto& blob_files = vstorage->GetBlobFiles();
      for (const auto& meta : blob_files) {
        assert(meta);

        const uint64_t blob_file_number = meta->GetBlobFileNumber();

        edit.AddBlobFile(blob_file_number, meta->GetTotalBlobCount(),
                         meta->GetTotalBlobBytes(), meta->GetChecksumMethod(),
                         meta->GetChecksumValue());
        if (meta->GetGarbageBlobCount() > 0) {
          edit.AddBlobFileGarbage(blob_file_number, meta->GetGarbageBlobCount(),
                                  meta->GetGarbageBlobBytes());
        }
      }

      const auto iter = curr_state.find(cfd->GetID());
      assert(iter != curr_state.end());
      uint64_t log_number = iter->second.log_number;
      edit.SetLogNumber(log_number);

      if (cfd->GetID() == 0) {
        // min_log_number_to_keep is for the whole db, not for specific column
        // family. So it does not need to be set for every column family, just
        // need to be set once. Since default CF can never be dropped, we set
        // the min_log to the default CF here.
        uint64_t min_log = min_log_number_to_keep();
        if (min_log != 0) {
          edit.SetMinLogNumberToKeep(min_log);
        }
      }

      const std::string& full_history_ts_low = iter->second.full_history_ts_low;
      if (!full_history_ts_low.empty()) {
        edit.SetFullHistoryTsLow(full_history_ts_low);
      }

      edit.SetLastSequence(descriptor_last_sequence_);

      const Comparator* ucmp = cfd->user_comparator();
      assert(ucmp);
      std::string record;
      if (!edit.EncodeTo(&record, ucmp->timestamp_size())) {
        return Status::Corruption("Unable to Encode VersionEdit:" +
                                  edit.DebugString(true));
      }
      io_s = log->AddRecord(record);
      if (!io_s.ok()) {
        return io_s;
      }
    }
  }
  return Status::OK();
}

// TODO(aekmekji): in CompactionJob::GenSubcompactionBoundaries(), this
// function is called repeatedly with consecutive pairs of slices. For example
// if the slice list is [a, b, c, d] this function is called with arguments
// (a,b) then (b,c) then (c,d). Knowing this, an optimization is possible where
// we avoid doing binary search for the keys b and c twice and instead somehow
// maintain state of where they first appear in the files.
uint64_t VersionSet::ApproximateSize(const SizeApproximationOptions& options,
                                     const ReadOptions& read_options,
                                     Version* v, const Slice& start,
                                     const Slice& end, int start_level,
                                     int end_level, TableReaderCaller caller) {
  const auto& icmp = v->cfd_->internal_comparator();

  // pre-condition
  assert(icmp.Compare(start, end) <= 0);

  uint64_t total_full_size = 0;
  const auto* vstorage = v->storage_info();
  const int num_non_empty_levels = vstorage->num_non_empty_levels();
  end_level = (end_level == -1) ? num_non_empty_levels
                                : std::min(end_level, num_non_empty_levels);
  if (end_level <= start_level) {
    return 0;
  }

  // Outline of the optimization that uses options.files_size_error_margin.
  // When approximating the files total size that is used to store a keys range,
  // we first sum up the sizes of the files that fully fall into the range.
  // Then we sum up the sizes of all the files that may intersect with the range
  // (this includes all files in L0 as well). Then, if total_intersecting_size
  // is smaller than total_full_size * options.files_size_error_margin - we can
  // infer that the intersecting files have a sufficiently negligible
  // contribution to the total size, and we can approximate the storage required
  // for the keys in range as just half of the intersecting_files_size.
  // E.g., if the value of files_size_error_margin is 0.1, then the error of the
  // approximation is limited to only ~10% of the total size of files that fully
  // fall into the keys range. In such case, this helps to avoid a costly
  // process of binary searching the intersecting files that is required only
  // for a more precise calculation of the total size.

  autovector<FdWithKeyRange*, 32> first_files;
  autovector<FdWithKeyRange*, 16> last_files;

  // scan all the levels
  for (int level = start_level; level < end_level; ++level) {
    const LevelFilesBrief& files_brief = vstorage->LevelFilesBrief(level);
    if (files_brief.num_files == 0) {
      // empty level, skip exploration
      continue;
    }

    if (level == 0) {
      // level 0 files are not in sorted order, we need to iterate through
      // the list to compute the total bytes that require scanning,
      // so handle the case explicitly (similarly to first_files case)
      for (size_t i = 0; i < files_brief.num_files; i++) {
        first_files.push_back(&files_brief.files[i]);
      }
      continue;
    }

    assert(level > 0);
    assert(files_brief.num_files > 0);

    // identify the file position for start key
    const int idx_start =
        FindFileInRange(icmp, files_brief, start, 0,
                        static_cast<uint32_t>(files_brief.num_files - 1));
    assert(static_cast<size_t>(idx_start) < files_brief.num_files);

    // identify the file position for end key
    int idx_end = idx_start;
    if (icmp.Compare(files_brief.files[idx_end].largest_key, end) < 0) {
      idx_end =
          FindFileInRange(icmp, files_brief, end, idx_start,
                          static_cast<uint32_t>(files_brief.num_files - 1));
    }
    assert(idx_end >= idx_start &&
           static_cast<size_t>(idx_end) < files_brief.num_files);

    // scan all files from the starting index to the ending index
    // (inferred from the sorted order)

    // first scan all the intermediate full files (excluding first and last)
    for (int i = idx_start + 1; i < idx_end; ++i) {
      uint64_t file_size = files_brief.files[i].fd.GetFileSize();
      // The entire file falls into the range, so we can just take its size.
      assert(file_size == ApproximateSize(read_options, v, files_brief.files[i],
                                          start, end, caller));
      total_full_size += file_size;
    }

    // save the first and the last files (which may be the same file), so we
    // can scan them later.
    first_files.push_back(&files_brief.files[idx_start]);
    if (idx_start != idx_end) {
      // we need to estimate size for both files, only if they are different
      last_files.push_back(&files_brief.files[idx_end]);
    }
  }

  // The sum of all file sizes that intersect the [start, end] keys range.
  uint64_t total_intersecting_size = 0;
  for (const auto* file_ptr : first_files) {
    total_intersecting_size += file_ptr->fd.GetFileSize();
  }
  for (const auto* file_ptr : last_files) {
    total_intersecting_size += file_ptr->fd.GetFileSize();
  }

  // Now scan all the first & last files at each level, and estimate their size.
  // If the total_intersecting_size is less than X% of the total_full_size - we
  // want to approximate the result in order to avoid the costly binary search
  // inside ApproximateSize. We use half of file size as an approximation below.

  const double margin = options.files_size_error_margin;
  if (margin > 0 && total_intersecting_size <
                        static_cast<uint64_t>(total_full_size * margin)) {
    total_full_size += total_intersecting_size / 2;
  } else {
    // Estimate for all the first files (might also be last files), at each
    // level
    for (const auto file_ptr : first_files) {
      total_full_size +=
          ApproximateSize(read_options, v, *file_ptr, start, end, caller);
    }

    // Estimate for all the last files, at each level
    for (const auto file_ptr : last_files) {
      // We could use ApproximateSize here, but calling ApproximateOffsetOf
      // directly is just more efficient.
      total_full_size +=
          ApproximateOffsetOf(read_options, v, *file_ptr, end, caller);
    }
  }

  return total_full_size;
}

uint64_t VersionSet::ApproximateOffsetOf(const ReadOptions& read_options,
                                         Version* v, const FdWithKeyRange& f,
                                         const Slice& key,
                                         TableReaderCaller caller) {
  // pre-condition
  assert(v);
  const auto& icmp = v->cfd_->internal_comparator();

  uint64_t result = 0;
  if (icmp.Compare(f.largest_key, key) <= 0) {
    // Entire file is before "key", so just add the file size
    result = f.fd.GetFileSize();
  } else if (icmp.Compare(f.smallest_key, key) > 0) {
    // Entire file is after "key", so ignore
    result = 0;
  } else {
    // "key" falls in the range for this table.  Add the
    // approximate offset of "key" within the table.
    TableCache* table_cache = v->cfd_->table_cache();
    const MutableCFOptions& cf_opts = v->GetMutableCFOptions();
    if (table_cache != nullptr) {
      result = table_cache->ApproximateOffsetOf(
          read_options, key, *f.file_metadata, caller, icmp,
          cf_opts.block_protection_bytes_per_key, cf_opts.prefix_extractor);
    }
  }
  return result;
}

uint64_t VersionSet::ApproximateSize(const ReadOptions& read_options,
                                     Version* v, const FdWithKeyRange& f,
                                     const Slice& start, const Slice& end,
                                     TableReaderCaller caller) {
  // pre-condition
  assert(v);
  const auto& icmp = v->cfd_->internal_comparator();
  assert(icmp.Compare(start, end) <= 0);

  if (icmp.Compare(f.largest_key, start) <= 0 ||
      icmp.Compare(f.smallest_key, end) > 0) {
    // Entire file is before or after the start/end keys range
    return 0;
  }

  if (icmp.Compare(f.smallest_key, start) >= 0) {
    // Start of the range is before the file start - approximate by end offset
    return ApproximateOffsetOf(read_options, v, f, end, caller);
  }

  if (icmp.Compare(f.largest_key, end) < 0) {
    // End of the range is after the file end - approximate by subtracting
    // start offset from the file size
    uint64_t start_offset =
        ApproximateOffsetOf(read_options, v, f, start, caller);
    assert(f.fd.GetFileSize() >= start_offset);
    return f.fd.GetFileSize() - start_offset;
  }

  // The interval falls entirely in the range for this file.
  TableCache* table_cache = v->cfd_->table_cache();
  if (table_cache == nullptr) {
    return 0;
  }
  const MutableCFOptions& cf_opts = v->GetMutableCFOptions();
  return table_cache->ApproximateSize(
      read_options, start, end, *f.file_metadata, caller, icmp,
      cf_opts.block_protection_bytes_per_key, cf_opts.prefix_extractor);
}

void VersionSet::RemoveLiveFiles(
    std::vector<ObsoleteFileInfo>& sst_delete_candidates,
    std::vector<ObsoleteBlobFileInfo>& blob_delete_candidates) const {
  assert(column_family_set_);
  for (auto cfd : *column_family_set_) {
    assert(cfd);
    if (!cfd->initialized()) {
      continue;
    }

    auto* current = cfd->current();
    bool found_current = false;

    Version* const dummy_versions = cfd->dummy_versions();
    assert(dummy_versions);

    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      v->RemoveLiveFiles(sst_delete_candidates, blob_delete_candidates);
      if (v == current) {
        found_current = true;
      }
    }

    if (!found_current && current != nullptr) {
      // Should never happen unless it is a bug.
      assert(false);
      current->RemoveLiveFiles(sst_delete_candidates, blob_delete_candidates);
    }
  }
}

void VersionSet::AddLiveFiles(std::vector<uint64_t>* live_table_files,
                              std::vector<uint64_t>* live_blob_files) const {
  assert(live_table_files);
  assert(live_blob_files);

  // pre-calculate space requirement
  size_t total_table_files = 0;
  size_t total_blob_files = 0;

  assert(column_family_set_);
  for (auto cfd : *column_family_set_) {
    assert(cfd);

    if (!cfd->initialized()) {
      continue;
    }

    Version* const dummy_versions = cfd->dummy_versions();
    assert(dummy_versions);

    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      assert(v);

      const auto* vstorage = v->storage_info();
      assert(vstorage);

      for (int level = 0; level < vstorage->num_levels(); ++level) {
        total_table_files += vstorage->LevelFiles(level).size();
      }

      total_blob_files += vstorage->GetBlobFiles().size();
    }
  }

  // just one time extension to the right size
  live_table_files->reserve(live_table_files->size() + total_table_files);
  live_blob_files->reserve(live_blob_files->size() + total_blob_files);

  assert(column_family_set_);
  for (auto cfd : *column_family_set_) {
    assert(cfd);
    if (!cfd->initialized()) {
      continue;
    }

    auto* current = cfd->current();
    bool found_current = false;

    Version* const dummy_versions = cfd->dummy_versions();
    assert(dummy_versions);

    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      v->AddLiveFiles(live_table_files, live_blob_files);
      if (v == current) {
        found_current = true;
      }
    }

    if (!found_current && current != nullptr) {
      // Should never happen unless it is a bug.
      assert(false);
      current->AddLiveFiles(live_table_files, live_blob_files);
    }
  }
}

InternalIterator* VersionSet::MakeInputIterator(
    const ReadOptions& read_options, const Compaction* c,
    RangeDelAggregator* range_del_agg,
    const FileOptions& file_options_compactions,
    const std::optional<const Slice>& start,
    const std::optional<const Slice>& end) {
  auto cfd = c->column_family_data();
  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const size_t space = (c->level() == 0 ? c->input_levels(0)->num_files +
                                              c->num_input_levels() - 1
                                        : c->num_input_levels());
  InternalIterator** list = new InternalIterator*[space];
  // First item in the pair is a pointer to range tombstones.
  // Second item is a pointer to a member of a LevelIterator,
  // that will be initialized to where CompactionMergingIterator stores
  // pointer to its range tombstones. This is used by LevelIterator
  // to update pointer to range tombstones as it traverse different SST files.
  std::vector<
      std::pair<TruncatedRangeDelIterator*, TruncatedRangeDelIterator***>>
      range_tombstones;
  size_t num = 0;
  for (size_t which = 0; which < c->num_input_levels(); which++) {
    if (c->input_levels(which)->num_files != 0) {
      if (c->level(which) == 0) {
        const LevelFilesBrief* flevel = c->input_levels(which);
        for (size_t i = 0; i < flevel->num_files; i++) {
          const FileMetaData& fmd = *flevel->files[i].file_metadata;
          if (start.has_value() &&
              cfd->user_comparator()->CompareWithoutTimestamp(
                  *start, fmd.largest.user_key()) > 0) {
            continue;
          }
          // We should be able to filter out the case where the end key
          // equals to the end boundary, since the end key is exclusive.
          // We try to be extra safe here.
          if (end.has_value() &&
              cfd->user_comparator()->CompareWithoutTimestamp(
                  *end, fmd.smallest.user_key()) < 0) {
            continue;
          }
          TruncatedRangeDelIterator* range_tombstone_iter = nullptr;
          list[num++] = cfd->table_cache()->NewIterator(
              read_options, file_options_compactions,
              cfd->internal_comparator(), fmd, range_del_agg,
              c->mutable_cf_options()->prefix_extractor,
              /*table_reader_ptr=*/nullptr,
              /*file_read_hist=*/nullptr, TableReaderCaller::kCompaction,
              /*arena=*/nullptr,
              /*skip_filters=*/false,
              /*level=*/static_cast<int>(c->level(which)),
              MaxFileSizeForL0MetaPin(*c->mutable_cf_options()),
              /*smallest_compaction_key=*/nullptr,
              /*largest_compaction_key=*/nullptr,
              /*allow_unprepared_value=*/false,
              c->mutable_cf_options()->block_protection_bytes_per_key,
              /*range_del_iter=*/&range_tombstone_iter);
          range_tombstones.emplace_back(range_tombstone_iter, nullptr);
        }
      } else {
        // Create concatenating iterator for the files from this level
        TruncatedRangeDelIterator*** tombstone_iter_ptr = nullptr;
        list[num++] = new LevelIterator(
            cfd->table_cache(), read_options, file_options_compactions,
            cfd->internal_comparator(), c->input_levels(which),
            c->mutable_cf_options()->prefix_extractor,
            /*should_sample=*/false,
            /*no per level latency histogram=*/nullptr,
            TableReaderCaller::kCompaction, /*skip_filters=*/false,
            /*level=*/static_cast<int>(c->level(which)),
            c->mutable_cf_options()->block_protection_bytes_per_key,
            range_del_agg, c->boundaries(which), false, &tombstone_iter_ptr);
        range_tombstones.emplace_back(nullptr, tombstone_iter_ptr);
      }
    }
  }
  assert(num <= space);
  InternalIterator* result = NewCompactionMergingIterator(
      &c->column_family_data()->internal_comparator(), list,
      static_cast<int>(num), range_tombstones);
  delete[] list;
  return result;
}

Status VersionSet::GetMetadataForFile(uint64_t number, int* filelevel,
                                      FileMetaData** meta,
                                      ColumnFamilyData** cfd) {
  for (auto cfd_iter : *column_family_set_) {
    if (!cfd_iter->initialized()) {
      continue;
    }
    Version* version = cfd_iter->current();
    const auto* vstorage = version->storage_info();
    for (int level = 0; level < vstorage->num_levels(); level++) {
      for (const auto& file : vstorage->LevelFiles(level)) {
        if (file->fd.GetNumber() == number) {
          *meta = file;
          *filelevel = level;
          *cfd = cfd_iter;
          return Status::OK();
        }
      }
    }
  }
  return Status::NotFound("File not present in any level");
}

void VersionSet::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      for (const auto& file :
           cfd->current()->storage_info()->LevelFiles(level)) {
        LiveFileMetaData filemetadata;
        filemetadata.column_family_name = cfd->GetName();
        uint32_t path_id = file->fd.GetPathId();
        if (path_id < cfd->ioptions()->cf_paths.size()) {
          filemetadata.db_path = cfd->ioptions()->cf_paths[path_id].path;
        } else {
          assert(!cfd->ioptions()->cf_paths.empty());
          filemetadata.db_path = cfd->ioptions()->cf_paths.back().path;
        }
        filemetadata.directory = filemetadata.db_path;
        const uint64_t file_number = file->fd.GetNumber();
        filemetadata.name = MakeTableFileName("", file_number);
        filemetadata.relative_filename = filemetadata.name.substr(1);
        filemetadata.file_number = file_number;
        filemetadata.level = level;
        filemetadata.size = file->fd.GetFileSize();
        filemetadata.smallestkey = file->smallest.user_key().ToString();
        filemetadata.largestkey = file->largest.user_key().ToString();
        filemetadata.smallest_seqno = file->fd.smallest_seqno;
        filemetadata.largest_seqno = file->fd.largest_seqno;
        filemetadata.num_reads_sampled =
            file->stats.num_reads_sampled.load(std::memory_order_relaxed);
        filemetadata.being_compacted = file->being_compacted;
        filemetadata.num_entries = file->num_entries;
        filemetadata.num_deletions = file->num_deletions;
        filemetadata.oldest_blob_file_number = file->oldest_blob_file_number;
        filemetadata.file_checksum = file->file_checksum;
        filemetadata.file_checksum_func_name = file->file_checksum_func_name;
        filemetadata.temperature = file->temperature;
        filemetadata.oldest_ancester_time = file->TryGetOldestAncesterTime();
        filemetadata.file_creation_time = file->TryGetFileCreationTime();
        filemetadata.epoch_number = file->epoch_number;
        metadata->push_back(filemetadata);
      }
    }
  }
}

void VersionSet::GetObsoleteFiles(std::vector<ObsoleteFileInfo>* files,
                                  std::vector<ObsoleteBlobFileInfo>* blob_files,
                                  std::vector<std::string>* manifest_filenames,
                                  uint64_t min_pending_output) {
  assert(files);
  assert(blob_files);
  assert(manifest_filenames);
  assert(files->empty());
  assert(blob_files->empty());
  assert(manifest_filenames->empty());

  std::vector<ObsoleteFileInfo> pending_files;
  for (auto& f : obsolete_files_) {
    if (f.metadata->fd.GetNumber() < min_pending_output) {
      files->emplace_back(std::move(f));
    } else {
      pending_files.emplace_back(std::move(f));
    }
  }
  obsolete_files_.swap(pending_files);

  std::vector<ObsoleteBlobFileInfo> pending_blob_files;
  for (auto& blob_file : obsolete_blob_files_) {
    if (blob_file.GetBlobFileNumber() < min_pending_output) {
      blob_files->emplace_back(std::move(blob_file));
    } else {
      pending_blob_files.emplace_back(std::move(blob_file));
    }
  }
  obsolete_blob_files_.swap(pending_blob_files);

  obsolete_manifests_.swap(*manifest_filenames);
}

uint64_t VersionSet::GetObsoleteSstFilesSize() const {
  uint64_t ret = 0;
  for (auto& f : obsolete_files_) {
    if (f.metadata != nullptr) {
      ret += f.metadata->fd.GetFileSize();
    }
  }
  return ret;
}

ColumnFamilyData* VersionSet::CreateColumnFamily(
    const ColumnFamilyOptions& cf_options, const ReadOptions& read_options,
    const VersionEdit* edit) {
  assert(edit->is_column_family_add_);

  MutableCFOptions dummy_cf_options;
  Version* dummy_versions =
      new Version(nullptr, this, file_options_, dummy_cf_options, io_tracer_);
  // Ref() dummy version once so that later we can call Unref() to delete it
  // by avoiding calling "delete" explicitly (~Version is private)
  dummy_versions->Ref();
  auto new_cfd = column_family_set_->CreateColumnFamily(
      edit->column_family_name_, edit->column_family_, dummy_versions,
      cf_options);

  Version* v = new Version(new_cfd, this, file_options_,
                           *new_cfd->GetLatestMutableCFOptions(), io_tracer_,
                           current_version_number_++);

  constexpr bool update_stats = false;

  v->PrepareAppend(*new_cfd->GetLatestMutableCFOptions(), read_options,
                   update_stats);

  AppendVersion(new_cfd, v);
  // GetLatestMutableCFOptions() is safe here without mutex since the
  // cfd is not available to client
  new_cfd->CreateNewMemtable(*new_cfd->GetLatestMutableCFOptions(),
                             LastSequence());
  new_cfd->SetLogNumber(edit->log_number_);
  return new_cfd;
}

uint64_t VersionSet::GetNumLiveVersions(Version* dummy_versions) {
  uint64_t count = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    count++;
  }
  return count;
}

uint64_t VersionSet::GetTotalSstFilesSize(Version* dummy_versions) {
  std::unordered_set<uint64_t> unique_files;
  uint64_t total_files_size = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    VersionStorageInfo* storage_info = v->storage_info();
    for (int level = 0; level < storage_info->num_levels_; level++) {
      for (const auto& file_meta : storage_info->LevelFiles(level)) {
        if (unique_files.find(file_meta->fd.packed_number_and_path_id) ==
            unique_files.end()) {
          unique_files.insert(file_meta->fd.packed_number_and_path_id);
          total_files_size += file_meta->fd.GetFileSize();
        }
      }
    }
  }
  return total_files_size;
}

uint64_t VersionSet::GetTotalBlobFileSize(Version* dummy_versions) {
  std::unordered_set<uint64_t> unique_blob_files;

  uint64_t all_versions_blob_file_size = 0;

  for (auto* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    // iterate all the versions
    const auto* vstorage = v->storage_info();
    assert(vstorage);

    const auto& blob_files = vstorage->GetBlobFiles();

    for (const auto& meta : blob_files) {
      assert(meta);

      const uint64_t blob_file_number = meta->GetBlobFileNumber();

      if (unique_blob_files.find(blob_file_number) == unique_blob_files.end()) {
        // find Blob file that has not been counted
        unique_blob_files.insert(blob_file_number);
        all_versions_blob_file_size += meta->GetBlobFileSize();
      }
    }
  }

  return all_versions_blob_file_size;
}

Status VersionSet::VerifyFileMetadata(const ReadOptions& read_options,
                                      ColumnFamilyData* cfd,
                                      const std::string& fpath, int level,
                                      const FileMetaData& meta) {
  uint64_t fsize = 0;
  Status status = fs_->GetFileSize(fpath, IOOptions(), &fsize, nullptr);
  if (status.ok()) {
    if (fsize != meta.fd.GetFileSize()) {
      status = Status::Corruption("File size mismatch: " + fpath);
    }
  }
  if (status.ok() && db_options_->verify_sst_unique_id_in_manifest) {
    assert(cfd);
    TableCache* table_cache = cfd->table_cache();
    assert(table_cache);

    const MutableCFOptions* const cf_opts = cfd->GetLatestMutableCFOptions();
    assert(cf_opts);
    std::shared_ptr<const SliceTransform> pe = cf_opts->prefix_extractor;
    size_t max_sz_for_l0_meta_pin = MaxFileSizeForL0MetaPin(*cf_opts);

    const FileOptions& file_opts = file_options();

    Version* version = cfd->current();
    assert(version);
    VersionStorageInfo& storage_info = version->storage_info_;
    const InternalKeyComparator* icmp = storage_info.InternalComparator();
    assert(icmp);

    InternalStats* internal_stats = cfd->internal_stats();

    TableCache::TypedHandle* handle = nullptr;
    FileMetaData meta_copy = meta;
    status = table_cache->FindTable(
        read_options, file_opts, *icmp, meta_copy, &handle,
        cf_opts->block_protection_bytes_per_key, pe,
        /*no_io=*/false, internal_stats->GetFileReadHist(level), false, level,
        /*prefetch_index_and_filter_in_cache*/ false, max_sz_for_l0_meta_pin,
        meta_copy.temperature);
    if (handle) {
      table_cache->get_cache().Release(handle);
    }
  }
  return status;
}

ReactiveVersionSet::ReactiveVersionSet(
    const std::string& dbname, const ImmutableDBOptions* _db_options,
    const FileOptions& _file_options, Cache* table_cache,
    WriteBufferManager* write_buffer_manager, WriteController* write_controller,
    const std::shared_ptr<IOTracer>& io_tracer)
    : VersionSet(dbname, _db_options, _file_options, table_cache,
                 write_buffer_manager, write_controller,
                 /*block_cache_tracer=*/nullptr, io_tracer, /*db_id*/ "",
                 /*db_session_id*/ "") {}

ReactiveVersionSet::~ReactiveVersionSet() {}

Status ReactiveVersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
    std::unique_ptr<log::Reader::Reporter>* manifest_reporter,
    std::unique_ptr<Status>* manifest_reader_status) {
  assert(manifest_reader != nullptr);
  assert(manifest_reporter != nullptr);
  assert(manifest_reader_status != nullptr);

  manifest_reader_status->reset(new Status());
  manifest_reporter->reset(new LogReporter());
  static_cast_with_check<LogReporter>(manifest_reporter->get())->status =
      manifest_reader_status->get();
  Status s = MaybeSwitchManifest(manifest_reporter->get(), manifest_reader);
  if (!s.ok()) {
    return s;
  }
  log::Reader* reader = manifest_reader->get();
  assert(reader);

  manifest_tailer_.reset(new ManifestTailer(
      column_families, const_cast<ReactiveVersionSet*>(this), io_tracer_,
      read_options_, EpochNumberRequirement::kMightMissing));

  manifest_tailer_->Iterate(*reader, manifest_reader_status->get());

  s = manifest_tailer_->status();
  if (s.ok()) {
    RecoverEpochNumbers();
  }
  return s;
}

Status ReactiveVersionSet::ReadAndApply(
    InstrumentedMutex* mu,
    std::unique_ptr<log::FragmentBufferedReader>* manifest_reader,
    Status* manifest_read_status,
    std::unordered_set<ColumnFamilyData*>* cfds_changed) {
  assert(manifest_reader != nullptr);
  assert(cfds_changed != nullptr);
  mu->AssertHeld();

  Status s;
  log::Reader* reader = manifest_reader->get();
  assert(reader);
  s = MaybeSwitchManifest(reader->GetReporter(), manifest_reader);
  if (!s.ok()) {
    return s;
  }
  manifest_tailer_->Iterate(*(manifest_reader->get()), manifest_read_status);
  s = manifest_tailer_->status();
  if (s.ok()) {
    *cfds_changed = std::move(manifest_tailer_->GetUpdatedColumnFamilies());
  }

  return s;
}

Status ReactiveVersionSet::MaybeSwitchManifest(
    log::Reader::Reporter* reporter,
    std::unique_ptr<log::FragmentBufferedReader>* manifest_reader) {
  assert(manifest_reader != nullptr);
  Status s;
  std::string manifest_path;
  s = GetCurrentManifestPath(dbname_, fs_.get(), &manifest_path,
                             &manifest_file_number_);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<FSSequentialFile> manifest_file;
  if (manifest_reader->get() != nullptr &&
      manifest_reader->get()->file()->file_name() == manifest_path) {
    // CURRENT points to the same MANIFEST as before, no need to switch
    // MANIFEST.
    return s;
  }
  assert(nullptr == manifest_reader->get() ||
         manifest_reader->get()->file()->file_name() != manifest_path);
  s = fs_->FileExists(manifest_path, IOOptions(), nullptr);
  if (s.IsNotFound()) {
    return Status::TryAgain(
        "The primary may have switched to a new MANIFEST and deleted the old "
        "one.");
  } else if (!s.ok()) {
    return s;
  }
  TEST_SYNC_POINT(
      "ReactiveVersionSet::MaybeSwitchManifest:"
      "AfterGetCurrentManifestPath:0");
  TEST_SYNC_POINT(
      "ReactiveVersionSet::MaybeSwitchManifest:"
      "AfterGetCurrentManifestPath:1");
  // The primary can also delete the MANIFEST while the secondary is reading
  // it. This is OK on POSIX. For other file systems, maybe create a hard link
  // to MANIFEST. The hard link should be cleaned up later by the secondary.
  s = fs_->NewSequentialFile(manifest_path,
                             fs_->OptimizeForManifestRead(file_options_),
                             &manifest_file, nullptr);
  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  if (s.ok()) {
    manifest_file_reader.reset(new SequentialFileReader(
        std::move(manifest_file), manifest_path,
        db_options_->log_readahead_size, io_tracer_, db_options_->listeners));
    manifest_reader->reset(new log::FragmentBufferedReader(
        nullptr, std::move(manifest_file_reader), reporter, true /* checksum */,
        0 /* log_number */));
    ROCKS_LOG_INFO(db_options_->info_log, "Switched to new manifest: %s\n",
                   manifest_path.c_str());
    if (manifest_tailer_) {
      manifest_tailer_->PrepareToReadNewManifest();
    }
  } else if (s.IsPathNotFound()) {
    // This can happen if the primary switches to a new MANIFEST after the
    // secondary reads the CURRENT file but before the secondary actually tries
    // to open the MANIFEST.
    s = Status::TryAgain(
        "The primary may have switched to a new MANIFEST and deleted the old "
        "one.");
  }
  return s;
}

#ifndef NDEBUG
uint64_t ReactiveVersionSet::TEST_read_edits_in_atomic_group() const {
  assert(manifest_tailer_);
  return manifest_tailer_->GetReadBuffer().TEST_read_edits_in_atomic_group();
}
#endif  // !NDEBUG

std::vector<VersionEdit>& ReactiveVersionSet::replay_buffer() {
  assert(manifest_tailer_);
  return manifest_tailer_->GetReadBuffer().replay_buffer();
}

}  // namespace ROCKSDB_NAMESPACE
