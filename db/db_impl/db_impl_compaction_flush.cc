//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cinttypes>
#include <deque>

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/concurrent_task_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {

bool DBImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    bool* sfm_reserved_compact_space, LogBuffer* log_buffer) {
  // Check if we have enough room to do the compaction
  bool enough_room = true;
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm) {
    // Pass the current bg_error_ to SFM so it can decide what checks to
    // perform. If this DB instance hasn't seen any error yet, the SFM can be
    // optimistic and not do disk space checks
    Status bg_error = error_handler_.GetBGError();
    enough_room = sfm->EnoughRoomForCompaction(cfd, inputs, bg_error);
    bg_error.PermitUncheckedError();  // bg_error is just a copy of the Status
                                      // from the error_handler_
    if (enough_room) {
      *sfm_reserved_compact_space = true;
    }
  }
  if (!enough_room) {
    // Just in case tests want to change the value of enough_room
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction():CancelledCompaction", &enough_room);
    ROCKS_LOG_BUFFER(log_buffer,
                     "Cancelled compaction because not enough room");
    RecordTick(stats_, COMPACTION_CANCELLED, 1);
  }
  return enough_room;
}

/**
 * @brief 请求压缩任务限制令牌
 *
 * 该函数用于在调度压缩任务前请求任务限制令牌，用于控制并发压缩任务的数量，
 * 防止过多的压缩任务同时执行导致系统资源耗尽或性能下降。
 *
 * @param cfd 目标列族数据指针，指定要执行压缩的列族
 * @param force 是否强制获取令牌（忽略限制）
 *            - true：强制获取令牌，即使超出并发限制
 *            - false：遵守限制，只有当未超过限制时才获取令牌
 *            - 手动压缩通常使用 false，避免强制调度
 * @param token 输出参数，任务限制令牌的唯一指针
 *             - 输入时必须为 nullptr（通过断言检查）
 *             - 输出时指向获取到的令牌，nullptr 表示获取失败
 *             - 令牌会在任务完成后自动释放（通过析构函数）
 * @param log_buffer 日志缓冲区，用于记录令牌请求过程
 *
 * @return bool 操作结果
 *         - true：成功获取到压缩令牌，可以调度压缩任务
 *         - false：未能获取到令牌，不应调度压缩任务
 *
 * @note 调用此函数时必须持有 mutex_（db_mutex）
 *
 * @brief 任务限制器（Task Limiter）的作用：
 * - 控制并发压缩任务的最大数量
 * - 防止系统资源（CPU、内存、磁盘 I/O）耗尽
 * - 避免过多的并发压缩导致性能下降（锁竞争、I/O 竞争）
 * - 可以通过 DBOptions::max_background_compactions 和 DBOptions::max_background_jobs 配置
 *
 * @brief ConcurrentTaskLimiterImpl 工作原理：
 * - 维护一个当前运行的任务计数器
 * - GetToken(force) 试图获取令牌：
 *   - 如果 force = true：总是返回有效令牌，不检查限制
 *   - 如果 force = false：只有当前任务数 < 限制时才返回令牌
 * - 令牌的析构函数会自动释放，减少任务计数
 * - 使用 RAII（Resource Acquisition Is Initialization）模式管理资源
 *
 * @brief 使用场景：
 * 1. 自动压缩：force = false，遵守并发限制
 * 2. 手动压缩：force = false，遵守并发限制（通常）
 * 3. 紧急压缩：force = true，强制执行（可能超出限制）
 *
 * @brief 返回值说明：
 * - 返回 true：
 *   - 成功获取到令牌
 *   - 可以安全地调度压缩任务
 *   - 令牌会在任务完成时自动释放
 *   - 记录日志说明任务数变化
 *
 * - 返回 false：
 *   - 未获取到令牌（超出并发限制）
 *   - 不应调度压缩任务
 *   - 任务会等待后续重试
 *   - 可能是因为达到了 max_background_compactions 限制
 */
bool DBImpl::RequestCompactionToken(ColumnFamilyData* cfd, bool force,
                                    std::unique_ptr<TaskLimiterToken>* token,
                                    LogBuffer* log_buffer) {
  // 断言：确保 token 输入时为空
  // 这是一个安全检查，防止资源泄漏
  assert(*token == nullptr);

  // 获取列族的压缩线程限制器
  // compaction_thread_limiter 是在 ColumnFamilyOptions 中配置的任务限制器
  // 如果未配置（nullptr），则不限制并发压缩任务
  auto limiter = static_cast<ConcurrentTaskLimiterImpl*>(
      cfd->ioptions()->compaction_thread_limiter.get());

  // 如果没有配置限制器，直接返回成功
  // 这种情况下，不限制并发压缩任务的数量
  if (limiter == nullptr) {
    return true;
  }

  // 尝试获取任务限制令牌
  // GetToken(force) 的行为：
  // - force = true：总是返回有效令牌（可能超出限制）
  // - force = false：只有当前任务数 < 限制时才返回令牌
  *token = limiter->GetToken(force);

  // 如果成功获取到令牌
  if (*token != nullptr) {
    // 记录日志：任务数增加
    ROCKS_LOG_BUFFER(log_buffer,
                     "Thread limiter [%s] increase [%s] compaction task, "
                     "force: %s, tasks after: %d",
                     limiter->GetName().c_str(), cfd->GetName().c_str(),
                     force ? "true" : "false", limiter->GetOutstandingTask());

    // 返回成功：可以调度压缩任务
    return true;
  }

  // 未获取到令牌：超出并发限制
  // 返回 false：不应调度压缩任务
  return false;
}

IOStatus DBImpl::SyncClosedLogs(JobContext* job_context,
                                VersionEdit* synced_wals) {
  TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Start");
  InstrumentedMutexLock l(&log_write_mutex_);
  autovector<log::Writer*, 1> logs_to_sync;
  uint64_t current_log_number = logfile_number_;
  while (logs_.front().number < current_log_number &&
         logs_.front().IsSyncing()) {
    log_sync_cv_.Wait();
  }
  for (auto it = logs_.begin();
       it != logs_.end() && it->number < current_log_number; ++it) {
    auto& log = *it;
    log.PrepareForSync();
    logs_to_sync.push_back(log.writer);
  }

  IOStatus io_s;
  if (!logs_to_sync.empty()) {
    log_write_mutex_.Unlock();

    assert(job_context);

    for (log::Writer* log : logs_to_sync) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "[JOB %d] Syncing log #%" PRIu64, job_context->job_id,
                     log->get_log_number());
      if (error_handler_.IsRecoveryInProgress()) {
        log->file()->reset_seen_error();
      }
      io_s = log->file()->Sync(immutable_db_options_.use_fsync);
      if (!io_s.ok()) {
        break;
      }

      if (immutable_db_options_.recycle_log_file_num > 0) {
        if (error_handler_.IsRecoveryInProgress()) {
          log->file()->reset_seen_error();
        }
        io_s = log->Close();
        if (!io_s.ok()) {
          break;
        }
      }
    }
    if (io_s.ok()) {
      io_s = directories_.GetWalDir()->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
    }

    TEST_SYNC_POINT_CALLBACK("DBImpl::SyncClosedLogs:BeforeReLock",
                             /*arg=*/nullptr);
    log_write_mutex_.Lock();

    // "number <= current_log_number - 1" is equivalent to
    // "number < current_log_number".
    if (io_s.ok()) {
      MarkLogsSynced(current_log_number - 1, true, synced_wals);
    } else {
      MarkLogsNotSynced(current_log_number - 1);
    }
    if (!io_s.ok()) {
      TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Failed");
      return io_s;
    }
  }
  TEST_SYNC_POINT("DBImpl::SyncClosedLogs:end");
  return io_s;
}

/**
 * @brief 将不可变 memtables 刷写成 SST 文件
 *
 * 该函数是 flush 流程的核心：创建 FlushJob，挑选 memtable，同步 WAL（多 CF 时），
 * 然后 PickMemTable、NotifyOnFlushBegin、flush_job.Run() 写 L0，最后安装 SuperVersion 并通知监听器。
 *
 * @param cfd 目标列族数据指针，指定要执行 flush 的列族
 * @param mutable_cf_options 可变列族选项，包含 flush 相关配置（如压缩算法、文件大小等）
 * @param made_progress 输出参数，指示 flush 是否实际完成（true 表示生成了 SST 文件）
 * @param job_context 后台任务上下文，包含 flush 相关的资源管理信息
 * @param flush_reason flush 触发原因（如 memtable 满了、手动触发、WAL 大小限制等）
 * @param superversion_context SuperVersion 上下文，用于 flush 完成后更新版本
 * @param snapshot_seqs 需要保留的快照序列号列表，用于正确处理快照读取
 * @param earliest_write_conflict_snapshot 最早的写冲突快照序列号
 * @param snapshot_checker 快照检查器，用于确定哪些 key 需要保留
 * @param log_buffer 日志缓冲区，用于记录 flush 过程的日志信息
 * @param thread_pri 线程优先级，指定 flush 线程的优先级级别
 *
 * @return Status 操作状态，OK 表示 flush 成功完成
 *
 * @note 调用此函数时必须持有 mutex_（db_mutex）
 *
 * 函数执行流程：
 * 1. 前置检查和 WAL 同步决策（多列族场景需要同步已关闭的 WAL）
 * 2. 设置 max_memtable_id 以防止 flush 期间新增的 memtable 被错误包含
 * 3. 创建 FlushJob 并选择要 flush 的 memtables
 * 4. 通知监听器 flush 开始
 * 5. 执行实际的 flush 操作（写入 SST 文件）
 * 6. 失败时取消已选择的 memtables
 * 7. 成功时更新 SuperVersion 并调度后续任务
 * 8. 错误处理和资源清理
 * 9. 通知监听器 flush 完成并更新文件管理器
 *
 * 关键设计要点：
 * - WAL 同步：多列族场景下必须同步已关闭的 WAL，避免数据不一致
 * - Memtable ID 过滤：使用 max_memtable_id 确保只 flush 已经存在且有 WAL 支持的 memtables
 * - 快照处理：通过 snapshot_seqs 确保快照数据的可见性
 * - 锁释放策略：flush_job.Run 期间释放 db_mutex，允许并发写操作
 * - 错误恢复：失败时清理已修改的状态，避免部分更新
 * - 空间管理：flush 成功后通知 SST 文件管理器
 *
 * 调用时机：
 * - memtable 大小达到 write_buffer_size
 * - WAL 大小超过 max_total_wal_size
 * - 用户调用 Flush() 手动触发
 * - 后台线程调度 flush 任务
 */
Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context, FlushReason flush_reason,
    SuperVersionContext* superversion_context,
    std::vector<SequenceNumber>& snapshot_seqs,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
    Env::Priority thread_pri) {
  // 断言：调用者必须持有数据库互斥锁
  mutex_.AssertHeld();
  // 前置断言：确保列族和不可变 memtables 存在且需要 flush
  assert(cfd);
  assert(cfd->imm());
  assert(cfd->imm()->NumNotFlushed() != 0);
  assert(cfd->imm()->IsFlushPending());
  assert(versions_);
  assert(versions_->GetColumnFamilySet());

  // 决定是否需要同步已关闭的 WAL 文件
  // 场景说明：
  // - 如果存在多个列族，一个写入操作可能同时更新多个列族的 memtables
  // - 这些写入记录在同一个 WAL 文件中
  // - 如果只 flush 某个列族后主机崩溃，但其他列族的 memtables 还在内存中且 WAL 未持久化
  // - 那么恢复后，SST 文件中包含的数据对应的 WAL 更新丢失，导致数据不一致
  // 因此需要确保除了最新的 WAL 外，所有旧 WAL 都已同步到磁盘
  const bool needs_to_sync_closed_wals =
      logfile_number_ > 0 &&
      versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 1;

  // 设置最大 memtable ID 用于过滤
  //
  // 设计原因 1 - 防止 WAL 未同步的 memtable 被 flush：
  // - SyncClosedLogs() 会释放和重新获取 db mutex
  // - 在此期间，该列族可能发生 memtable 切换（生成新的 memtable）
  // - 新创建的 memtable 的数据由未同步的 WAL 支持
  // - 如果将这些 memtable 包含在本次 flush 中，一旦主机崩溃，SST 文件数据将无法从 WAL 恢复
  //
  // 设计原因 2 - 防止未知的快照数据被错误丢弃：
  // - SyncClosedLogs() 释放 db mutex 后，应用可以继续写入，增加数据库的序列号
  // - 应用可能在此时创建快照，希望快照可见的数据被保留
  // - snapshot_seqs 参数是在此函数调用前计算的，不包含新创建的快照
  // - 如果 flush 了包含新快照数据的 memtable 但不知道该快照存在，可能会错误地删除某些 key
  // - 导致使用该快照读取时返回错误的数据
  //
  // 解决方案：
  // - 记录当前的 max_memtable_id，后续 PickMemTable() 只选择 ID <= max_memtable_id 的 memtables
  // - 这样确保了：
  //   1. 不会 flush 由未同步 WAL 支持的新 memtables
  //   2. 不会 flush 包含未知快照的新 memtables（因为它们有更高的 ID）
  uint64_t max_memtable_id = needs_to_sync_closed_wals
                                 ? cfd->imm()->GetLatestMemTableID()
                                 : std::numeric_limits<uint64_t>::max();

  // 关于 memtable 选择和快照的时序设计
  //
  // 当 needs_to_sync_closed_wals = false 时（通常是单列族场景）：
  // - flush job 将选择该列族的所有现有 memtables
  // - 虽然不调用 SyncClosedLogs()，但会调用 NotifyOnFlushBegin()
  // - NotifyOnFlushBegin() 也会释放和重新获取 db mutex
  // - 在释放期间，应用可以继续写入并创建新的快照
  // - 新创建的快照不在 snapshot_seqs 中，flush job 不知道它的存在
  // - 如果 flush 删除了该快照可见的某些 key，会导致快照读取返回错误数据
  //
  // 解决方案：确保 NotifyOnFlushBegin() 在 memtable 选择之后执行
  // - 先调用 PickMemTable() 选择要 flush 的 memtables
  // - 后调用 NotifyOnFlushBegin() 通知监听器
  // - 这样即使期间创建了新快照，也不会影响已选择的 memtables

  // 创建 FlushJob 对象，封装 flush 操作的所有必要信息
  FlushJob flush_job(
      dbname_, cfd, immutable_db_options_, mutable_cf_options, max_memtable_id,
      file_options_for_compaction_, versions_.get(), &mutex_, &shutting_down_,
      snapshot_seqs, earliest_write_conflict_snapshot, snapshot_checker,
      job_context, flush_reason, log_buffer, directories_.GetDbDir(),
      GetDataDir(cfd, 0U),
      GetCompressionFlush(*cfd->ioptions(), mutable_cf_options), stats_,
      &event_logger_, mutable_cf_options.report_bg_io_stats,
      true /* sync_output_directory */, true /* write_manifest */, thread_pri,
      io_tracer_, seqno_time_mapping_, db_id_, db_session_id_,
      cfd->GetFullHistoryTsLow(), &blob_callback_);
  FileMetaData file_meta;

  Status s;
  bool need_cancel = false;  // 标记是否需要在失败时取消已选择的 memtables
  IOStatus log_io_s = IOStatus::OK();

  // 步骤 1: 同步已关闭的 WAL 文件（多列族场景）
  if (needs_to_sync_closed_wals) {
    // SyncClosedLogs() 可能会多次释放和重新获取 log_write_mutex
    VersionEdit synced_wals;
    mutex_.Unlock();
    log_io_s = SyncClosedLogs(job_context, &synced_wals);
    mutex_.Lock();

    // 如果 WAL 同步成功且有 WAL 添加记录，将其应用到 MANIFEST
    if (log_io_s.ok() && synced_wals.IsWalAddition()) {
      const ReadOptions read_options(Env::IOActivity::kFlush);
      log_io_s =
          status_to_io_status(ApplyWALToManifest(read_options, &synced_wals));
      TEST_SYNC_POINT_CALLBACK("DBImpl::FlushMemTableToOutputFile:CommitWal:1",
                               nullptr);
    }

    // 如果 WAL 同步失败且不是关机或列族删除，设置后台错误
    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
    }
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedLogs:Skip");
  }
  s = log_io_s;

  // 步骤 2: 选择要 flush 的 memtables
  // 如果 WAL 同步失败，不需要选择 memtable
  // 否则，num_flush_not_started_ 需要回滚
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:BeforePickMemtables");
  if (s.ok()) {
    flush_job.PickMemTable();
    need_cancel = true;  // 已选择 memtables，如果后续步骤失败需要取消
  }
  TEST_SYNC_POINT_CALLBACK(
      "DBImpl::FlushMemTableToOutputFile:AfterPickMemtables", &flush_job);

  // 步骤 3: 通知监听器 flush 开始
  // 注意：此函数可能会临时释放和重新获取 mutex
  // 这就是为什么需要在 PickMemTable() 之后调用，避免期间新增 memtables 影响已选择的集合
  NotifyOnFlushBegin(cfd, &file_meta, mutable_cf_options, job_context->job_id,
                     flush_reason);

  // 步骤 4: 执行实际的 flush 操作
  bool switched_to_mempurge = false;
  // flush_job.Run 内部可能会调用事件监听器通知文件创建和删除
  // 注意：flush_job.Run 会释放和重新获取 db_mutex
  // 事件监听器的回调会在 db_mutex 被释放时调用
  if (s.ok()) {
    s = flush_job.Run(&logs_with_prep_tracker_, &file_meta,
                      &switched_to_mempurge);
    need_cancel = false;  // flush 成功，不需要取消
  }

  // 步骤 5: 失败时取消已选择的 memtables
  if (!s.ok() && need_cancel) {
    flush_job.Cancel();
  }

  // 步骤 6: flush 成功后的处理
  if (s.ok()) {
    // 安装新的 SuperVersion 并调度后台任务（flush/compaction）
    InstallSuperVersionAndScheduleWork(cfd, superversion_context,
                                       mutable_cf_options);
    if (made_progress) {
      *made_progress = true;  // 标记 flush 实际完成了
    }

    // 记录日志信息：层级摘要和 blob 文件摘要
    const std::string& column_family_name = cfd->GetName();

    Version* const current = cfd->current();
    assert(current);

    const VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);

    // 记录各层级文件数量和大小信息
    VersionStorageInfo::LevelSummaryStorage tmp;
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                     column_family_name.c_str(),
                     storage_info->LevelSummary(&tmp));

    // 记录 blob 文件信息（如果启用了 blob 功能）
    const auto& blob_files = storage_info->GetBlobFiles();
    if (!blob_files.empty()) {
      assert(blob_files.front());
      assert(blob_files.back());

      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
          column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
          blob_files.back()->GetBlobFileNumber());
    }
  }

  // 步骤 7: 错误处理
  if (!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped()) {
    if (log_io_s.ok()) {
      // MANIFEST 写入失败
      // 注意：versions_->io_status() 可能也是 CURRENT 文件重命名失败的结果
      // 当前代码难以区分，所以采取悲观策略，尝试写入新的 MANIFEST
      // TODO: 区分 MANIFEST 写入和 CURRENT 文件重命名
      if (!versions_->io_status().ok()) {
        // 如果 WAL 同步成功（WAL 大小为 0 或无 IO 错误），
        // 所有 MANIFEST 写入错误都映射为软错误
        // TODO: kManifestWriteNoWAL 和 kFlushNoWAL 名称有误导性，需要重构
        error_handler_.SetBGError(s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // 如果 WAL 同步成功（WAL 大小为 0 或无 IO 错误），
        // 其他所有 SST 文件写入错误都设置为 kFlushNoWAL
        error_handler_.SetBGError(s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      // WAL 同步失败
      assert(s == log_io_s);
      Status new_bg_error = s;
      error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
    }
  }

  // 步骤 8: 通知监听器 flush 完成并更新文件管理器
  // 条件：flush 成功完成且没有切换到 mempurge（内存清理模式）
  if (s.ok() && (!switched_to_mempurge)) {
    // 通知监听器 flush 完成（可能会临时释放和重新获取 mutex）
    NotifyOnFlushCompleted(cfd, mutable_cf_options,
                           flush_job.GetCommittedFlushJobsInfo());

    // 更新 SST 文件管理器
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm) {
      // 通知文件管理器有新文件添加
      std::string file_path = MakeTableFileName(
          cfd->ioptions()->cf_paths[0].path, file_meta.fd.GetNumber());
      // TODO (PR7798): 应该只在文件存在时添加到 FileManager
      // 否则某些测试可能会失败。暂时忽略错误
      sfm->OnAddFile(file_path).PermitUncheckedError();

      // 检查是否达到最大允许空间限制
      if (sfm->IsMaxAllowedSpaceReached()) {
        Status new_bg_error =
            Status::SpaceLimit("Max allowed space was reached");
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
            &new_bg_error);
        error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
      }
    }
  }

  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:Finish");
  return s;
}

// 将一批后台 flush 参数对应的列族 memtable 刷成 SST 文件。
// 若开启原子 flush，则多列族一起刷并统一提交 MANIFEST；否则仅支持单列族，直接调 FlushMemTableToOutputFile。
Status DBImpl::FlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
  // 原子 flush 模式：多列族必须全部刷完再一次性提交 MANIFEST，保证多 CF 间一致性
  if (immutable_db_options_.atomic_flush) {
    return AtomicFlushMemTablesToOutputFiles(
        bg_flush_args, made_progress, job_context, log_buffer, thread_pri);
  }
  // 非原子模式下，BackgroundFlush 保证每次只传一个列族，这里断言以捕获错误
  assert(bg_flush_args.size() == 1);
  // 获取当前所有快照的 sequence 列表及写冲突相关快照，供 FlushMemTableToOutputFile 中可见性判断用
  std::vector<SequenceNumber> snapshot_seqs;
  SequenceNumber earliest_write_conflict_snapshot;
  SnapshotChecker* snapshot_checker;
  GetSnapshotContext(job_context, &snapshot_seqs,
                     &earliest_write_conflict_snapshot, &snapshot_checker);
  // 取出唯一的列族及其参数
  const auto& bg_flush_arg = bg_flush_args[0];
  ColumnFamilyData* cfd = bg_flush_arg.cfd_;
  // 拷贝一份当前可变列族选项，避免 flush 过程中选项被其它线程修改
  MutableCFOptions mutable_cf_options_copy = *cfd->GetLatestMutableCFOptions();
  SuperVersionContext* superversion_context =
      bg_flush_arg.superversion_context_;
  FlushReason flush_reason = bg_flush_arg.flush_reason_;
  // 对该列族执行单次 flush：写 L0 SST、安装 SuperVersion 等
  Status s = FlushMemTableToOutputFile(
      cfd, mutable_cf_options_copy, made_progress, job_context, flush_reason,
      superversion_context, snapshot_seqs, earliest_write_conflict_snapshot,
      snapshot_checker, log_buffer, thread_pri);
  return s;
}

/*
 * Atomically flushes multiple column families.
 *
 * For each column family, all memtables with ID smaller than or equal to the
 * ID specified in bg_flush_args will be flushed. Only after all column
 * families finish flush will this function commit to MANIFEST. If any of the
 * column families are not flushed successfully, this function does not have
 * any side-effect on the state of the database.
 */
Status DBImpl::AtomicFlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
  mutex_.AssertHeld();

  autovector<ColumnFamilyData*> cfds;
  for (const auto& arg : bg_flush_args) {
    cfds.emplace_back(arg.cfd_);
  }

#ifndef NDEBUG
  for (const auto cfd : cfds) {
    assert(cfd->imm()->NumNotFlushed() != 0);
    assert(cfd->imm()->IsFlushPending());
  }
  for (const auto& bg_flush_arg : bg_flush_args) {
    assert(bg_flush_arg.flush_reason_ == bg_flush_args[0].flush_reason_);
  }
#endif /* !NDEBUG */

  std::vector<SequenceNumber> snapshot_seqs;
  SequenceNumber earliest_write_conflict_snapshot;
  SnapshotChecker* snapshot_checker;
  GetSnapshotContext(job_context, &snapshot_seqs,
                     &earliest_write_conflict_snapshot, &snapshot_checker);

  autovector<FSDirectory*> distinct_output_dirs;
  autovector<std::string> distinct_output_dir_paths;
  std::vector<std::unique_ptr<FlushJob>> jobs;
  std::vector<MutableCFOptions> all_mutable_cf_options;
  int num_cfs = static_cast<int>(cfds.size());
  all_mutable_cf_options.reserve(num_cfs);
  for (int i = 0; i < num_cfs; ++i) {
    auto cfd = cfds[i];
    FSDirectory* data_dir = GetDataDir(cfd, 0U);
    const std::string& curr_path = cfd->ioptions()->cf_paths[0].path;

    // Add to distinct output directories if eligible. Use linear search. Since
    // the number of elements in the vector is not large, performance should be
    // tolerable.
    bool found = false;
    for (const auto& path : distinct_output_dir_paths) {
      if (path == curr_path) {
        found = true;
        break;
      }
    }
    if (!found) {
      distinct_output_dir_paths.emplace_back(curr_path);
      distinct_output_dirs.emplace_back(data_dir);
    }

    all_mutable_cf_options.emplace_back(*cfd->GetLatestMutableCFOptions());
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.back();
    uint64_t max_memtable_id = bg_flush_args[i].max_memtable_id_;
    FlushReason flush_reason = bg_flush_args[i].flush_reason_;
    jobs.emplace_back(new FlushJob(
        dbname_, cfd, immutable_db_options_, mutable_cf_options,
        max_memtable_id, file_options_for_compaction_, versions_.get(), &mutex_,
        &shutting_down_, snapshot_seqs, earliest_write_conflict_snapshot,
        snapshot_checker, job_context, flush_reason, log_buffer,
        directories_.GetDbDir(), data_dir,
        GetCompressionFlush(*cfd->ioptions(), mutable_cf_options), stats_,
        &event_logger_, mutable_cf_options.report_bg_io_stats,
        false /* sync_output_directory */, false /* write_manifest */,
        thread_pri, io_tracer_, seqno_time_mapping_, db_id_, db_session_id_,
        cfd->GetFullHistoryTsLow(), &blob_callback_));
  }

  std::vector<FileMetaData> file_meta(num_cfs);
  // Use of deque<bool> because vector<bool>
  // is specific and doesn't allow &v[i].
  std::deque<bool> switched_to_mempurge(num_cfs, false);
  Status s;
  IOStatus log_io_s = IOStatus::OK();
  assert(num_cfs == static_cast<int>(jobs.size()));

  for (int i = 0; i != num_cfs; ++i) {
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.at(i);
    // may temporarily unlock and lock the mutex.
    FlushReason flush_reason = bg_flush_args[i].flush_reason_;
    NotifyOnFlushBegin(cfds[i], &file_meta[i], mutable_cf_options,
                       job_context->job_id, flush_reason);
  }

  if (logfile_number_ > 0) {
    // TODO (yanqin) investigate whether we should sync the closed logs for
    // single column family case.
    VersionEdit synced_wals;
    mutex_.Unlock();
    log_io_s = SyncClosedLogs(job_context, &synced_wals);
    mutex_.Lock();
    if (log_io_s.ok() && synced_wals.IsWalAddition()) {
      const ReadOptions read_options(Env::IOActivity::kFlush);
      log_io_s =
          status_to_io_status(ApplyWALToManifest(read_options, &synced_wals));
    }

    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      if (total_log_size_ > 0) {
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
      } else {
        // If the WAL is empty, we use different error reason
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlushNoWAL);
      }
    }
  }
  s = log_io_s;

  // exec_status stores the execution status of flush_jobs as
  // <bool /* executed */, Status /* status code */>
  autovector<std::pair<bool, Status>> exec_status;
  std::vector<bool> pick_status;
  for (int i = 0; i != num_cfs; ++i) {
    // Initially all jobs are not executed, with status OK.
    exec_status.emplace_back(false, Status::OK());
    pick_status.push_back(false);
  }

  if (s.ok()) {
    for (int i = 0; i != num_cfs; ++i) {
      jobs[i]->PickMemTable();
      pick_status[i] = true;
    }
  }

  if (s.ok()) {
    assert(switched_to_mempurge.size() ==
           static_cast<long unsigned int>(num_cfs));
    // TODO (yanqin): parallelize jobs with threads.
    for (int i = 1; i != num_cfs; ++i) {
      exec_status[i].second =
          jobs[i]->Run(&logs_with_prep_tracker_, &file_meta[i],
                       &(switched_to_mempurge.at(i)));
      exec_status[i].first = true;
    }
    if (num_cfs > 1) {
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1");
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2");
    }
    assert(exec_status.size() > 0);
    assert(!file_meta.empty());
    exec_status[0].second = jobs[0]->Run(
        &logs_with_prep_tracker_, file_meta.data() /* &file_meta[0] */,
        switched_to_mempurge.empty() ? nullptr : &(switched_to_mempurge.at(0)));
    exec_status[0].first = true;

    Status error_status;
    for (const auto& e : exec_status) {
      if (!e.second.ok()) {
        s = e.second;
        if (!e.second.IsShutdownInProgress() &&
            !e.second.IsColumnFamilyDropped()) {
          // If a flush job did not return OK, and the CF is not dropped, and
          // the DB is not shutting down, then we have to return this result to
          // caller later.
          error_status = e.second;
        }
      }
    }

    s = error_status.ok() ? s : error_status;
  }

  if (s.IsColumnFamilyDropped()) {
    s = Status::OK();
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    // Sync on all distinct output directories.
    for (auto dir : distinct_output_dirs) {
      if (dir != nullptr) {
        Status error_status = dir->FsyncWithDirOptions(
            IOOptions(), nullptr,
            DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
        if (!error_status.ok()) {
          s = error_status;
          break;
        }
      }
    }
  } else {
    // Need to undo atomic flush if something went wrong, i.e. s is not OK and
    // it is not because of CF drop.
    // Have to cancel the flush jobs that have NOT executed because we need to
    // unref the versions.
    for (int i = 0; i != num_cfs; ++i) {
      if (pick_status[i] && !exec_status[i].first) {
        jobs[i]->Cancel();
      }
    }
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].second.ok() && exec_status[i].first) {
        auto& mems = jobs[i]->GetMemTables();
        cfds[i]->imm()->RollbackMemtableFlush(mems,
                                              file_meta[i].fd.GetNumber());
      }
    }
  }

  if (s.ok()) {
    const auto wait_to_install_func =
        [&]() -> std::pair<Status, bool /*continue to wait*/> {
      if (!versions_->io_status().ok()) {
        // Something went wrong elsewhere, we cannot count on waiting for our
        // turn to write/sync to MANIFEST or CURRENT. Just return.
        return std::make_pair(versions_->io_status(), false);
      } else if (shutting_down_.load(std::memory_order_acquire)) {
        return std::make_pair(Status::ShutdownInProgress(), false);
      }
      bool ready = true;
      for (size_t i = 0; i != cfds.size(); ++i) {
        const auto& mems = jobs[i]->GetMemTables();
        if (cfds[i]->IsDropped()) {
          // If the column family is dropped, then do not wait.
          continue;
        } else if (!mems.empty() &&
                   cfds[i]->imm()->GetEarliestMemTableID() < mems[0]->GetID()) {
          // If a flush job needs to install the flush result for mems and
          // mems[0] is not the earliest memtable, it means another thread must
          // be installing flush results for the same column family, then the
          // current thread needs to wait.
          ready = false;
          break;
        } else if (mems.empty() && cfds[i]->imm()->GetEarliestMemTableID() <=
                                       bg_flush_args[i].max_memtable_id_) {
          // If a flush job does not need to install flush results, then it has
          // to wait until all memtables up to max_memtable_id_ (inclusive) are
          // installed.
          ready = false;
          break;
        }
      }
      return std::make_pair(Status::OK(), !ready);
    };

    bool resuming_from_bg_err =
        error_handler_.IsDBStopped() ||
        (bg_flush_args[0].flush_reason_ == FlushReason::kErrorRecovery ||
         bg_flush_args[0].flush_reason_ ==
             FlushReason::kErrorRecoveryRetryFlush);
    while ((!resuming_from_bg_err || error_handler_.GetRecoveryError().ok())) {
      std::pair<Status, bool> res = wait_to_install_func();

      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitToCommit", &res);

      if (!res.first.ok()) {
        s = res.first;
        break;
      } else if (!res.second) {
        break;
      }
      atomic_flush_install_cv_.Wait();

      resuming_from_bg_err =
          error_handler_.IsDBStopped() ||
          (bg_flush_args[0].flush_reason_ == FlushReason::kErrorRecovery ||
           bg_flush_args[0].flush_reason_ ==
               FlushReason::kErrorRecoveryRetryFlush);
    }

    if (!resuming_from_bg_err) {
      // If not resuming from bg err, then we determine future action based on
      // whether we hit background error.
      if (s.ok()) {
        s = error_handler_.GetBGError();
      }
    } else if (s.ok()) {
      // If resuming from bg err, we still rely on wait_to_install_func()'s
      // result to determine future action. If wait_to_install_func() returns
      // non-ok already, then we should not proceed to flush result
      // installation.
      s = error_handler_.GetRecoveryError();
    }
  }

  if (s.ok()) {
    autovector<ColumnFamilyData*> tmp_cfds;
    autovector<const autovector<MemTable*>*> mems_list;
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    autovector<FileMetaData*> tmp_file_meta;
    autovector<std::list<std::unique_ptr<FlushJobInfo>>*>
        committed_flush_jobs_info;
    for (int i = 0; i != num_cfs; ++i) {
      const auto& mems = jobs[i]->GetMemTables();
      if (!cfds[i]->IsDropped() && !mems.empty()) {
        tmp_cfds.emplace_back(cfds[i]);
        mems_list.emplace_back(&mems);
        mutable_cf_options_list.emplace_back(&all_mutable_cf_options[i]);
        tmp_file_meta.emplace_back(&file_meta[i]);
        committed_flush_jobs_info.emplace_back(
            jobs[i]->GetCommittedFlushJobsInfo());
      }
    }

    s = InstallMemtableAtomicFlushResults(
        nullptr /* imm_lists */, tmp_cfds, mutable_cf_options_list, mems_list,
        versions_.get(), &logs_with_prep_tracker_, &mutex_, tmp_file_meta,
        committed_flush_jobs_info, &job_context->memtables_to_free,
        directories_.GetDbDir(), log_buffer);
  }

  if (s.ok()) {
    assert(num_cfs ==
           static_cast<int>(job_context->superversion_contexts.size()));
    for (int i = 0; i != num_cfs; ++i) {
      assert(cfds[i]);

      if (cfds[i]->IsDropped()) {
        continue;
      }
      InstallSuperVersionAndScheduleWork(cfds[i],
                                         &job_context->superversion_contexts[i],
                                         all_mutable_cf_options[i]);

      const std::string& column_family_name = cfds[i]->GetName();

      Version* const current = cfds[i]->current();
      assert(current);

      const VersionStorageInfo* const storage_info = current->storage_info();
      assert(storage_info);

      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                       column_family_name.c_str(),
                       storage_info->LevelSummary(&tmp));

      const auto& blob_files = storage_info->GetBlobFiles();
      if (!blob_files.empty()) {
        assert(blob_files.front());
        assert(blob_files.back());

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
            column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
            blob_files.back()->GetBlobFileNumber());
      }
    }
    if (made_progress) {
      *made_progress = true;
    }
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    assert(all_mutable_cf_options.size() == static_cast<size_t>(num_cfs));
    for (int i = 0; s.ok() && i != num_cfs; ++i) {
      // If mempurge happened instead of Flush,
      // no NotifyOnFlushCompleted call (no SST file created).
      if (switched_to_mempurge[i]) {
        continue;
      }
      if (cfds[i]->IsDropped()) {
        continue;
      }
      NotifyOnFlushCompleted(cfds[i], all_mutable_cf_options[i],
                             jobs[i]->GetCommittedFlushJobsInfo());
      if (sfm) {
        std::string file_path = MakeTableFileName(
            cfds[i]->ioptions()->cf_paths[0].path, file_meta[i].fd.GetNumber());
        // TODO (PR7798).  We should only add the file to the FileManager if it
        // exists. Otherwise, some tests may fail.  Ignore the error in the
        // interim.
        sfm->OnAddFile(file_path).PermitUncheckedError();
        if (sfm->IsMaxAllowedSpaceReached() &&
            error_handler_.GetBGError().ok()) {
          Status new_bg_error =
              Status::SpaceLimit("Max allowed space was reached");
          error_handler_.SetBGError(new_bg_error,
                                    BackgroundErrorReason::kFlush);
        }
      }
    }
  }

  // Need to undo atomic flush if something went wrong, i.e. s is not OK and
  // it is not because of CF drop.
  if (!s.ok() && !s.IsColumnFamilyDropped()) {
    if (log_io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor
        // is needed.
        error_handler_.SetBGError(s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      assert(s == log_io_s);
      Status new_bg_error = s;
      error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
    }
  }

  return s;
}

void DBImpl::NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                                const MutableCFOptions& mutable_cf_options,
                                int job_id, FlushReason flush_reason) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    FlushJobInfo info{};
    info.cf_id = cfd->GetID();
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    const uint64_t file_number = file_meta->fd.GetNumber();
    info.file_path =
        MakeTableFileName(cfd->ioptions()->cf_paths[0].path, file_number);
    info.file_number = file_number;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = file_meta->fd.smallest_seqno;
    info.largest_seqno = file_meta->fd.largest_seqno;
    info.flush_reason = flush_reason;
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnFlushBegin(this, info);
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

void DBImpl::NotifyOnFlushCompleted(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    std::list<std::unique_ptr<FlushJobInfo>>* flush_jobs_info) {
  assert(flush_jobs_info != nullptr);
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumLevelFiles(0) >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    for (auto& info : *flush_jobs_info) {
      info->triggered_writes_slowdown = triggered_writes_slowdown;
      info->triggered_writes_stop = triggered_writes_stop;
      for (auto listener : immutable_db_options_.listeners) {
        listener->OnFlushCompleted(this, *info);
      }
      TEST_SYNC_POINT(
          "DBImpl::NotifyOnFlushCompleted::PostAllOnFlushCompleted");
    }
    flush_jobs_info->clear();
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin_without_ts,
                            const Slice* end_without_ts) {
  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0) {
    return CompactRangeInternal(options, column_family, begin_without_ts,
                                end_without_ts, "" /*trim_ts*/);
  }

  std::string begin_str;
  std::string end_str;

  // CompactRange compact all keys: [begin, end] inclusively. Add maximum
  // timestamp to include all `begin` keys, and add minimal timestamp to include
  // all `end` keys.
  if (begin_without_ts != nullptr) {
    AppendKeyWithMaxTimestamp(&begin_str, *begin_without_ts, ts_sz);
  }
  if (end_without_ts != nullptr) {
    AppendKeyWithMinTimestamp(&end_str, *end_without_ts, ts_sz);
  }
  Slice begin(begin_str);
  Slice end(end_str);

  Slice* begin_with_ts = begin_without_ts ? &begin : nullptr;
  Slice* end_with_ts = end_without_ts ? &end : nullptr;

  return CompactRangeInternal(options, column_family, begin_with_ts,
                              end_with_ts, "" /*trim_ts*/);
}

Status DBImpl::IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                        std::string ts_low) {
  ColumnFamilyData* cfd = nullptr;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    assert(cfh != nullptr);
    cfd = cfh->cfd();
  }
  assert(cfd != nullptr && cfd->user_comparator() != nullptr);
  if (cfd->user_comparator()->timestamp_size() == 0) {
    return Status::InvalidArgument(
        "Timestamp is not enabled in this column family");
  }
  if (cfd->user_comparator()->timestamp_size() != ts_low.size()) {
    return Status::InvalidArgument("ts_low size mismatch");
  }
  return IncreaseFullHistoryTsLowImpl(cfd, ts_low);
}

Status DBImpl::IncreaseFullHistoryTsLowImpl(ColumnFamilyData* cfd,
                                            std::string ts_low) {
  VersionEdit edit;
  edit.SetColumnFamily(cfd->GetID());
  edit.SetFullHistoryTsLow(ts_low);

  // TODO: plumb Env::IOActivity
  const ReadOptions read_options;
  TEST_SYNC_POINT_CALLBACK("DBImpl::IncreaseFullHistoryTsLowImpl:BeforeEdit",
                           &edit);

  InstrumentedMutexLock l(&mutex_);
  std::string current_ts_low = cfd->GetFullHistoryTsLow();
  const Comparator* ucmp = cfd->user_comparator();
  assert(ucmp->timestamp_size() == ts_low.size() && !ts_low.empty());
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(ts_low, current_ts_low) < 0) {
    return Status::InvalidArgument("Cannot decrease full_history_ts_low");
  }

  Status s = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(),
                                    read_options, &edit, &mutex_,
                                    directories_.GetDbDir());
  if (!s.ok()) {
    return s;
  }
  current_ts_low = cfd->GetFullHistoryTsLow();
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(current_ts_low, ts_low) > 0) {
    std::stringstream oss;
    oss << "full_history_ts_low: " << Slice(current_ts_low).ToString(true)
        << " is set to be higher than the requested "
           "timestamp: "
        << Slice(ts_low).ToString(true) << std::endl;
    return Status::TryAgain(oss.str());
  }
  return Status::OK();
}

Status DBImpl::CompactRangeInternal(const CompactRangeOptions& options,
                                    ColumnFamilyHandle* column_family,
                                    const Slice* begin, const Slice* end,
                                    const std::string& trim_ts) {
  // 将列族句柄转换为 ColumnFamilyHandleImpl 类型
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);

  // 获取列族数据指针
  auto cfd = cfh->cfd();

  // 验证目标路径 ID 是否有效
  if (options.target_path_id >= cfd->ioptions()->cf_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }

  // 标记是否需要 flush，默认为 true
  bool flush_needed = true;

  // 如果设置了 full_history_ts_low，则更新它
  if (options.full_history_ts_low != nullptr &&
      !options.full_history_ts_low->empty()) {
    // 将时间戳转换为字符串
    std::string ts_low = options.full_history_ts_low->ToString();

    // 检查：如果指定了压缩范围，则不允许同时设置 full_history_ts_low
    if (begin != nullptr || end != nullptr) {
      return Status::InvalidArgument(
          "Cannot specify compaction range with full_history_ts_low");
    }

    // 调用实现更新 full_history_ts_low
    Status s = IncreaseFullHistoryTsLowImpl(cfd, ts_low);
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  // 声明状态变量
  Status s;

  // 如果指定了压缩范围的开始和结束
  if (begin != nullptr && end != nullptr) {
    // TODO(ajkr): 我们可以在某些情况下优化掉 flush，例如区间的一边或两边是无界的
    // 但这需要对 RangesOverlapWithMemtables 做更多修改

    // 创建范围对象
    Range range(*begin, *end);

    // 获取当前 SuperVersion 的引用
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);

    // 检查压缩范围是否与 MemTable 重叠
    s = cfd->RangesOverlapWithMemtables(
        {range}, super_version, immutable_db_options_.allow_data_in_errors,
        &flush_needed);

    // 清理 SuperVersion 引用
    CleanupSuperVersion(super_version);
  }

  // 如果前面操作成功且需要 flush
  if (s.ok() && flush_needed) {
    // 创建 Flush 选项
    FlushOptions fo;

    // 允许写入停顿
    fo.allow_write_stall = options.allow_write_stall;

    // 如果启用了原子 flush
    if (immutable_db_options_.atomic_flush) {
      // 原子 flush 所有列族的 MemTable
      s = AtomicFlushMemTables(fo, FlushReason::kManualCompaction);
    } else {  // default false
      // 只 flush 当前列族的 MemTable
      s = FlushMemTable(cfd, fo, FlushReason::kManualCompaction);
    }

    // 如果 flush 失败
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  // 定义无效 level 的常量
  constexpr int kInvalidLevel = -1;

  // 最终输出 level，初始化为无效值
  int final_output_level = kInvalidLevel;

  // 是否是独占的手动压缩
  bool exclusive = options.exclusive_manual_compaction; //默认false

  // 如果使用 Universal 压缩风格且层级数大于 1
  if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal &&
      cfd->NumberLevels() > 1) {
    // Universal 压缩总是将所有文件一起压缩

    // 最终输出 level 是最底层
    final_output_level = cfd->NumberLevels() - 1;

    // 如果最底层被保留（用于 ingest_behind）
    if (immutable_db_options_.allow_ingest_behind) {
      // 则输出到倒数第二层
      final_output_level--;
    }

    // 运行手动压缩，压缩所有层
    s = RunManualCompaction(cfd, ColumnFamilyData::kCompactAllLevels,
                            final_output_level, options, begin, end, exclusive,
                            false /* disable_trivial_move */,
                            std::numeric_limits<uint64_t>::max(), trim_ts);
  } else {
    // 非Universal压缩风格的情况
    // 记录第一个与压缩范围重叠的 level
    int first_overlapped_level = kInvalidLevel;

    {
      // 获取当前 SuperVersion 的引用
      SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);

      // 获取当前版本指针
      Version* current_version = super_version->current;

      // 可能需要查询分区器
      SstPartitionerFactory* partitioner_factory =
          current_version->cfd()->ioptions()->sst_partitioner_factory.get();

      // 分区器对象
      std::unique_ptr<SstPartitioner> partitioner;

      // 如果存在分区器工厂且指定了压缩范围
      if (partitioner_factory && begin != nullptr && end != nullptr) {
        // 创建分区器上下文
        SstPartitioner::Context context;

        // 不是全压缩
        context.is_full_compaction = false;

        // 是手动压缩
        context.is_manual_compaction = true;

        // 输出 level 未知
        context.output_level = /*unknown*/ -1;

        // 关于压缩范围的小谎言（用于分区器判断）
        context.smallest_user_key = *begin;
        context.largest_user_key = *end;

        // 创建分区器实例
        partitioner = partitioner_factory->CreatePartitioner(context);
      }

      // 创建读取选项
      ReadOptions ro;

      // 启用全局顺序查找
      ro.total_order_seek = true;

      // 设置 IO 活动类型为压缩
      ro.io_activity = Env::IOActivity::kCompaction;

      // 重叠标志
      bool overlap;

      // 遍历所有非空层
      for (int level = 0;
           level < current_version->storage_info()->num_non_empty_levels();
           level++) {
        // 默认为重叠
        overlap = true;

        // 是否需要在文件内部检查具体 key 与压缩范围的重叠
        // 而不仅仅是检查文件元数据中的最大和最小 key
        bool check_overlap_within_file = false;

        // 如果指定了压缩范围的开始和结束
        if (begin != nullptr && end != nullptr) {
          // 通常在这种情况下会检查文件内部的重叠
          check_overlap_within_file = true;

          // WART: 不知道为什么在单侧边界的情况下不检查文件内部
          if (partitioner) {
            // 特别是如果分区器是新的，手动压缩可能被用来强制执行分区
            // 检查文件内部的重叠可能会错过需要压缩文件进行分区的情况，例如：
            // * 文件有两个 key "001" 和 "111"
            // * 压缩范围是 ["011", "101")
            // * 分区边界在 "100"
            // 在这种情况下，文件级别与压缩范围的重叠就足以强制压缩范围内所需的任何分区
            //
            // 但是如果压缩范围内没有分区边界，我们可以确定不需要在该范围内修复分区
            // 因此可以安全地检查文件内部的重叠
            //
            // 使用假设的 trivial move 查询来检查范围内是否有分区边界
            // （注意：违反所有惯例，这里的 `begin` 和 `end` 都是包含边界，
            // 这使得即使 `end` 是分区中的第一个 key，这种与 CanDoTrivialMove() 的类比也是准确的）
            if (!partitioner->CanDoTrivialMove(*begin, *end)) {
              // 如果不能执行 trivial move，则不检查文件内部
              check_overlap_within_file = false;
            }
          }
        }

        // 如果需要检查文件内部的重叠
        if (check_overlap_within_file) {
          // 使用级别迭代器检查重叠
          Status status = current_version->OverlapWithLevelIterator(
              ro, file_options_, *begin, *end, level, &overlap);
          if (!status.ok()) {
            // 如果失败，则回退到文件级别检查
            check_overlap_within_file = false;
          }
        }

        // 如果不检查文件内部的重叠，则检查文件级别的重叠
        if (!check_overlap_within_file) {
          overlap = current_version->storage_info()->OverlapInLevel(level,
                                                                    begin, end);
        }

        // 如果找到重叠
        if (overlap) {
          // 记录第一个重叠的 level
          first_overlapped_level = level;
          break;
        }
      }

      // 清理 SuperVersion 引用
      CleanupSuperVersion(super_version);
    }

    // 如果前面操作成功且找到了第一个重叠的 level
    if (s.ok() && first_overlapped_level != kInvalidLevel) {
      // 如果是 Universal 或 FIFO 压缩风格
      if (cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
          cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
        // 断言：这些风格的重叠 level 必须是 0
        assert(first_overlapped_level == 0);

        // 运行手动压缩，从 L0 压缩到 L0
        s = RunManualCompaction(
            cfd, first_overlapped_level, first_overlapped_level, options, begin,
            end, exclusive, true /* disallow_trivial_move */,
            std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
            trim_ts);

        // 最终输出 level 就是第一个重叠的 level
        final_output_level = first_overlapped_level;
      } else {
        // Level 压缩风格
        assert(cfd->ioptions()->compaction_style == kCompactionStyleLevel);

        // 获取当前下一个文件编号（用于避免重写新压缩的文件）
        uint64_t next_file_number = versions_->current_next_file_number();

        // 从 `first_overlapped_level` 开始压缩，一次压缩一层
        // 直到输出 level >= max_overlapped_level
        // 当 max_overlapped_level == 0 时，我们仍然会从 L0 -> L1（或 LBase）压缩
        // 然后可能在 L1（或 LBase）进行底层级内压缩（如果适用）
        int level = first_overlapped_level;
        final_output_level = level;
        int output_level = 0, base_level = 0;

        // 无限循环，逐层压缩
        for (;;) {
          // 始终允许 L0 -> L1 压缩
          if (level > 0) {
            // 如果使用动态 level 字节大小
            if (cfd->ioptions()->level_compaction_dynamic_level_bytes) {
              // 断言：最终输出 level 必须小于总 level 数
              assert(final_output_level < cfd->ioptions()->num_levels);

              // 如果已经到达最后一层，停止
              if (final_output_level + 1 == cfd->ioptions()->num_levels) {
                break;
              }
            } else {
              // TODO(cbi): 这里仍然存在竞态条件
              // 如果后台压缩在检查后立即压缩了某些文件到
              // current()->storage_info()->num_non_empty_levels() 之外
              // 这应该非常罕见，并且一旦用户填充了 LSM 的最后一层就不会发生

              // 获取互斥锁
              InstrumentedMutexLock l(&mutex_);

              // 压缩后 num_non_empty_levels 可能会降低，所以这里检查 >=
              if (final_output_level + 1 >=
                  cfd->current()->storage_info()->num_non_empty_levels()) {
                break;
              }
            }
          }

          // 输出 level 是当前 level + 1
          output_level = level + 1;

          // 如果使用动态 level 字节大小且当前 level 是 0
          if (cfd->ioptions()->level_compaction_dynamic_level_bytes &&
              level == 0) {
            // L0 直接压缩到 base level
            output_level = ColumnFamilyData::kCompactToBaseLevel;
          }

          // 使用最大值作为 `max_file_num_to_ignore` 以始终向下压缩文件
          s = RunManualCompaction(
              cfd, level, output_level, options, begin, end, exclusive,
              !trim_ts.empty() /* disallow_trivial_move */,
              std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
              trim_ts,
              output_level == ColumnFamilyData::kCompactToBaseLevel
                  ? &base_level
                  : nullptr);

          // 如果压缩失败，停止循环
          if (!s.ok()) {
            break;
          }

          // 如果压缩到了 base level
          if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
            // 断言：base level 必须大于 0
            assert(base_level > 0);

            // 设置 level 为 base level
            level = base_level;
          } else {
            // 否则 level 加 1
            ++level;
          }

          // 更新最终输出 level
          final_output_level = level;

          // 测试同步点
          TEST_SYNC_POINT("DBImpl::RunManualCompaction()::1");
          TEST_SYNC_POINT("DBImpl::RunManualCompaction()::2");
        }

        // 如果前面操作成功
        if (s.ok()) {
          // 断言：最终输出 level 必须大于 0
          assert(final_output_level > 0);

          // 底层级内压缩
          // 条件：设置了压缩过滤器 且 选项为 kIfHaveCompactionFilter
          //       或者 选项为 kForceOptimized
          //       或者 选项为 kForce
          if ((options.bottommost_level_compaction ==
                   BottommostLevelCompaction::kIfHaveCompactionFilter &&
               (cfd->ioptions()->compaction_filter != nullptr ||
                cfd->ioptions()->compaction_filter_factory != nullptr)) ||
              options.bottommost_level_compaction ==
                  BottommostLevelCompaction::kForceOptimized ||
              options.bottommost_level_compaction ==
                  BottommostLevelCompaction::kForce) {
            // 使用 `next_file_number` 作为 `max_file_num_to_ignore`
            // 以避免在 kForceOptimized 或 kIfHaveCompactionFilter 且设置了压缩过滤器时
            // 重写新压缩的文件
            s = RunManualCompaction(
                cfd, final_output_level, final_output_level, options, begin,
                end, exclusive, true /* disallow_trivial_move */,
                next_file_number /* max_file_num_to_ignore */, trim_ts);
          }
        }
      }
    }
  }

  // 如果操作失败或没有有效的最终输出 level
  if (!s.ok() || final_output_level == kInvalidLevel) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  // 如果需要改变 level（ReFitLevel）
  if (options.change_level) {
    // 测试同步点
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:1");
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:2");

    // 记录日志：等待后台线程停止
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[RefitLevel] waiting for background threads to stop");

    // TODO(hx235): 一旦我们确保注册 RefitLevel() 的范围足以避免范围冲突
    // 就移除 `Enable/DisableManualCompaction` 和 `Continue/PauseBackgroundWork`
    // （如果不够，还需要什么）与当前通过 `Enable/DisableManualCompaction` 和
    // `Continue/PauseBackgroundWork` 避免的其他活动（例如压缩、flush）的冲突

    // 禁用手动压缩
    DisableManualCompaction();

    // 暂停后台工作
    s = PauseBackgroundWork();

    // 如果暂停成功
    if (s.ok()) {
      // 测试同步点
      TEST_SYNC_POINT("DBImpl::CompactRange:PreRefitLevel");

      // 执行 ReFitLevel 调整 level 布局
      s = ReFitLevel(cfd, final_output_level, options.target_level);

      // 测试同步点
      TEST_SYNC_POINT("DBImpl::CompactRange:PostRefitLevel");

      // 继续后台工作（总是返回 Status::OK()）
      Status temp_s = ContinueBackgroundWork();
      assert(temp_s.ok());
    }

    // 启用手动压缩
    EnableManualCompaction();

    // 测试同步点
    TEST_SYNC_POINT(
        "DBImpl::CompactRange:PostRefitLevel:ManualCompactionEnabled");
  }

  // 刷新日志
  LogFlush(immutable_db_options_.info_log);

  {
    // 获取互斥锁
    InstrumentedMutexLock l(&mutex_);

    // 已经调度的自动压缩可能被手动压缩抢占
    // 需要重新调度它
    MaybeScheduleFlushOrCompaction();
  }

  // 返回状态
  return s;
}

Status DBImpl::CompactFiles(const CompactionOptions& compact_options,
                            ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& input_file_names,
                            const int output_level, const int output_path_id,
                            std::vector<std::string>* const output_file_names,
                            CompactionJobInfo* compaction_job_info) {
  if (column_family == nullptr) {
    return Status::InvalidArgument("ColumnFamilyHandle must be non-null.");
  }

  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  assert(cfd);

  Status s;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  // Perform CompactFiles
  TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile2");
  TEST_SYNC_POINT_CALLBACK(
      "TestCompactFiles:PausingManualCompaction:3",
      reinterpret_cast<void*>(
          const_cast<std::atomic<int>*>(&manual_compaction_paused_)));
  {
    InstrumentedMutexLock l(&mutex_);
    auto* current = cfd->current();
    current->Ref();

    s = CompactFilesImpl(compact_options, cfd, current, input_file_names,
                         output_file_names, output_level, output_path_id,
                         &job_context, &log_buffer, compaction_job_info);

    current->Unref();
  }

  // Find and delete obsolete files
  {
    InstrumentedMutexLock l(&mutex_);
    // If !s.ok(), this means that Compaction failed. In that case, we want
    // to delete all obsolete files we might have created and we force
    // FindObsoleteFiles(). This is because job_context does not
    // catch all created files if compaction failed.
    FindObsoleteFiles(&job_context, !s.ok());
  }  // release the mutex

  // delete unnecessary files if any, this is done outside the mutex
  if (job_context.HaveSomethingToClean() ||
      job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
    // Have to flush the info logs before bg_compaction_scheduled_--
    // because if bg_flush_scheduled_ becomes 0 and the lock is
    // released, the deconstructor of DB can kick in and destroy all the
    // states of DB so info_log might not be available after that point.
    // It also applies to access other states that DB owns.
    log_buffer.FlushBufferToLog();
    if (job_context.HaveSomethingToDelete()) {
      // no mutex is locked here.  No need to Unlock() and Lock() here.
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
  }

  return s;
}

Status DBImpl::CompactFilesImpl(
    const CompactionOptions& compact_options, ColumnFamilyData* cfd,
    Version* version, const std::vector<std::string>& input_file_names,
    std::vector<std::string>* const output_file_names, const int output_level,
    int output_path_id, JobContext* job_context, LogBuffer* log_buffer,
    CompactionJobInfo* compaction_job_info) {
  mutex_.AssertHeld();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }
  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input_file_names) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  ColumnFamilyMetaData cf_meta;
  // TODO(yhchiang): can directly use version here if none of the
  // following functions call is pluggable to external developers.
  version->GetColumnFamilyMetaData(&cf_meta);

  if (output_path_id < 0) {
    if (cfd->ioptions()->cf_paths.size() == 1U) {
      output_path_id = 0;
    } else {
      return Status::NotSupported(
          "Automatic output path selection is not "
          "yet supported in CompactFiles()");
    }
  }

  if (cfd->ioptions()->allow_ingest_behind &&
      output_level >= cfd->ioptions()->num_levels - 1) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm with ingest_behind --- " +
        std::to_string(cfd->ioptions()->num_levels - 1));
  }

  Status s = cfd->compaction_picker()->SanitizeCompactionInputFiles(
      &input_set, cf_meta, output_level);
  TEST_SYNC_POINT("DBImpl::CompactFilesImpl::PostSanitizeCompactionInputFiles");
  if (!s.ok()) {
    return s;
  }

  std::vector<CompactionInputFiles> input_files;
  s = cfd->compaction_picker()->GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, version->storage_info(), compact_options);
  if (!s.ok()) {
    return s;
  }

  for (const auto& inputs : input_files) {
    if (cfd->compaction_picker()->AreFilesInCompaction(inputs.files)) {
      return Status::Aborted(
          "Some of the necessary compaction input "
          "files are already being compacted");
    }
  }
  bool sfm_reserved_compact_space = false;
  // First check if we have enough room to do the compaction
  bool enough_room = EnoughRoomForCompaction(
      cfd, input_files, &sfm_reserved_compact_space, log_buffer);

  if (!enough_room) {
    // m's vars will get set properly at the end of this function,
    // as long as status == CompactionTooLarge
    return Status::CompactionTooLarge();
  }

  // At this point, CompactFiles will be run.
  bg_compaction_scheduled_++;

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->CompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      *cfd->GetLatestMutableCFOptions(), mutable_db_options_, output_path_id));
  // we already sanitized the set of input files and checked for conflicts
  // without releasing the lock, so we're guaranteed a compaction can be formed.
  assert(c != nullptr);

  c->SetInputVersion(version);
  // deletion compaction currently not allowed in CompactFiles.
  assert(!c->deletion_compaction());

  std::vector<SequenceNumber> snapshot_seqs;
  SequenceNumber earliest_write_conflict_snapshot;
  SnapshotChecker* snapshot_checker;
  GetSnapshotContext(job_context, &snapshot_seqs,
                     &earliest_write_conflict_snapshot, &snapshot_checker);

  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));

  assert(is_snapshot_supported_ || snapshots_.empty());
  CompactionJobStats compaction_job_stats;
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_, mutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      log_buffer, directories_.GetDbDir(),
      GetDataDir(c->column_family_data(), c->output_path_id()),
      GetDataDir(c->column_family_data(), 0), stats_, &mutex_, &error_handler_,
      snapshot_seqs, earliest_write_conflict_snapshot, snapshot_checker,
      job_context, table_cache_, &event_logger_,
      c->mutable_cf_options()->paranoid_file_checks,
      c->mutable_cf_options()->report_bg_io_stats, dbname_,
      &compaction_job_stats, Env::Priority::USER, io_tracer_,
      kManualCompactionCanceledFalse_, db_id_, db_session_id_,
      c->column_family_data()->GetFullHistoryTsLow(), c->trim_ts(),
      &blob_callback_, &bg_compaction_scheduled_,
      &bg_bottom_compaction_scheduled_);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here.
  version->storage_info()->ComputeCompactionScore(*cfd->ioptions(),
                                                  *c->mutable_cf_options());

  compaction_job.Prepare();

  mutex_.Unlock();
  TEST_SYNC_POINT("CompactFilesImpl:0");
  TEST_SYNC_POINT("CompactFilesImpl:1");
  // Ignore the status here, as it will be checked in the Install down below...
  compaction_job.Run().PermitUncheckedError();
  TEST_SYNC_POINT("CompactFilesImpl:2");
  TEST_SYNC_POINT("CompactFilesImpl:3");
  mutex_.Lock();

  Status status = compaction_job.Install(*c->mutable_cf_options());
  if (status.ok()) {
    assert(compaction_job.io_status().ok());
    InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                       &job_context->superversion_contexts[0],
                                       *c->mutable_cf_options());
  }
  // status above captures any error during compaction_job.Install, so its ok
  // not check compaction_job.io_status() explicitly if we're not calling
  // SetBGError
  compaction_job.io_status().PermitUncheckedError();
  c->ReleaseCompactionFiles(s);
  // Need to make sure SstFileManager does its bookkeeping
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm && sfm_reserved_compact_space) {
    sfm->OnCompactionCompletion(c.get());
  }

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  if (compaction_job_info != nullptr) {
    BuildCompactionJobInfo(cfd, c.get(), s, compaction_job_stats,
                           job_context->job_id, version, compaction_job_info);
  }

  if (status.ok()) {
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else if (status.IsManualCompactionPaused()) {
    // Don't report stopping manual compaction as error
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Stopping manual compaction",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id);
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Compaction error: %s",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id, status.ToString().c_str());
    IOStatus io_s = compaction_job.io_status();
    if (!io_s.ok()) {
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kCompaction);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
  }

  if (output_file_names != nullptr) {
    for (const auto& newf : c->edit()->GetNewFiles()) {
      output_file_names->push_back(TableFileName(
          c->immutable_options()->cf_paths, newf.second.fd.GetNumber(),
          newf.second.fd.GetPathId()));
    }

    for (const auto& blob_file : c->edit()->GetBlobFileAdditions()) {
      output_file_names->push_back(
          BlobFileName(c->immutable_options()->cf_paths.front().path,
                       blob_file.GetBlobFileNumber()));
    }
  }

  c.reset();

  bg_compaction_scheduled_--;
  if (bg_compaction_scheduled_ == 0) {
    bg_cv_.SignalAll();
  }
  MaybeScheduleFlushOrCompaction();
  TEST_SYNC_POINT("CompactFilesImpl:End");

  return status;
}

Status DBImpl::PauseBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  bg_compaction_paused_++;
  while (bg_bottom_compaction_scheduled_ > 0 || bg_compaction_scheduled_ > 0 ||
         bg_flush_scheduled_ > 0) {
    bg_cv_.Wait();
  }
  bg_work_paused_++;
  return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  if (bg_work_paused_ == 0) {
    return Status::InvalidArgument();
  }
  assert(bg_work_paused_ > 0);
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  bg_work_paused_--;
  // It's sufficient to check just bg_work_paused_ here since
  // bg_work_paused_ is always no greater than bg_compaction_paused_
  if (bg_work_paused_ == 0) {
    MaybeScheduleFlushOrCompaction();
  }
  return Status::OK();
}

void DBImpl::NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                                     const Status& st,
                                     const CompactionJobStats& job_stats,
                                     int job_id) {
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return;
  }

  c->SetNotifyOnCompactionCompleted();
  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionBegin::UnlockMutex");
  {
    CompactionJobInfo info{};
    BuildCompactionJobInfo(cfd, c, st, job_stats, job_id, current, &info);
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnCompactionBegin(this, info);
    }
    info.status.PermitUncheckedError();
  }
  mutex_.Lock();
  current->Unref();
}

void DBImpl::NotifyOnCompactionCompleted(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  if (c->ShouldNotifyOnCompactionCompleted() == false) {
    return;
  }

  Version* current = cfd->current();
  current->Ref();
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionCompleted::UnlockMutex");
  {
    CompactionJobInfo info{};
    BuildCompactionJobInfo(cfd, c, st, compaction_job_stats, job_id, current,
                           &info);
    for (auto listener : immutable_db_options_.listeners) {
      listener->OnCompactionCompleted(this, info);
    }
  }
  mutex_.Lock();
  current->Unref();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

// REQUIREMENT: block all background work by calling PauseBackgroundWork()
// before calling this function
Status DBImpl::ReFitLevel(ColumnFamilyData* cfd, int level, int target_level) {
  assert(level < cfd->NumberLevels());
  if (target_level >= cfd->NumberLevels()) {
    return Status::InvalidArgument("Target level exceeds number of levels");
  }

  const ReadOptions read_options(Env::IOActivity::kCompaction);

  SuperVersionContext sv_context(/* create_superversion */ true);

  InstrumentedMutexLock guard_lock(&mutex_);

  auto* vstorage = cfd->current()->storage_info();
  if (vstorage->LevelFiles(level).empty()) {
    return Status::OK();
  }
  // only allow one thread refitting
  if (refitting_level_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[ReFitLevel] another thread is refitting");
    return Status::NotSupported("another thread is refitting");
  }
  refitting_level_ = true;

  const MutableCFOptions mutable_cf_options = *cfd->GetLatestMutableCFOptions();
  // move to a smaller level
  int to_level = target_level;
  if (target_level < 0) {
    to_level = FindMinimumEmptyLevelFitting(cfd, mutable_cf_options, level);
  }

  if (to_level != level) {
    std::vector<CompactionInputFiles> input(1);
    input[0].level = level;
    for (auto& f : vstorage->LevelFiles(level)) {
      input[0].files.push_back(f);
    }
    InternalKey refit_level_smallest;
    InternalKey refit_level_largest;
    cfd->compaction_picker()->GetRange(input[0], &refit_level_smallest,
                                       &refit_level_largest);
    if (to_level > level) {
      if (level == 0) {
        refitting_level_ = false;
        return Status::NotSupported(
            "Cannot change from level 0 to other levels.");
      }
      // Check levels are empty for a trivial move
      for (int l = level + 1; l <= to_level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
        if (cfd->RangeOverlapWithCompaction(refit_level_smallest.user_key(),
                                            refit_level_largest.user_key(),
                                            l)) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target "
              "will have some ongoing compaction's output.");
        }
      }
    } else {
      // to_level < level
      // Check levels are empty for a trivial move
      for (int l = to_level; l < level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
        if (cfd->RangeOverlapWithCompaction(refit_level_smallest.user_key(),
                                            refit_level_largest.user_key(),
                                            l)) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target "
              "will have some ongoing compaction's output.");
        }
      }
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
                    cfd->current()->DebugString().data());

    std::unique_ptr<Compaction> c(new Compaction(
        vstorage, *cfd->ioptions(), mutable_cf_options, mutable_db_options_,
        {input}, to_level,
        MaxFileSizeForLevel(
            mutable_cf_options, to_level,
            cfd->ioptions()
                ->compaction_style) /* output file size limit, not applicable */
        ,
        LLONG_MAX /* max compaction bytes, not applicable */,
        0 /* output path ID, not applicable */, mutable_cf_options.compression,
        mutable_cf_options.compression_opts, Temperature::kUnknown,
        0 /* max_subcompactions, not applicable */,
        {} /* grandparents, not applicable */, false /* is manual */,
        "" /* trim_ts */, -1 /* score, not applicable */,
        false /* is deletion compaction, not applicable */,
        false /* l0_files_might_overlap, not applicable */,
        CompactionReason::kRefitLevel));
    cfd->compaction_picker()->RegisterCompaction(c.get());
    TEST_SYNC_POINT("DBImpl::ReFitLevel:PostRegisterCompaction");
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());

    for (const auto& f : vstorage->LevelFiles(level)) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddFile(
          to_level, f->fd.GetNumber(), f->fd.GetPathId(), f->fd.GetFileSize(),
          f->smallest, f->largest, f->fd.smallest_seqno, f->fd.largest_seqno,
          f->marked_for_compaction, f->temperature, f->oldest_blob_file_number,
          f->oldest_ancester_time, f->file_creation_time, f->epoch_number,
          f->file_checksum, f->file_checksum_func_name, f->unique_id,
          f->compensated_range_deletion_size, f->tail_size,
          f->user_defined_timestamps_persisted);
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Apply version edit:\n%s", cfd->GetName().c_str(),
                    edit.DebugString().data());

    Status status =
        versions_->LogAndApply(cfd, mutable_cf_options, read_options, &edit,
                               &mutex_, directories_.GetDbDir());

    cfd->compaction_picker()->UnregisterCompaction(c.get());
    c.reset();

    InstallSuperVersionAndScheduleWork(cfd, &sv_context, mutable_cf_options);

    ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] LogAndApply: %s\n",
                    cfd->GetName().c_str(), status.ToString().data());

    if (status.ok()) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] After refitting:\n%s", cfd->GetName().c_str(),
                      cfd->current()->DebugString().data());
    }
    sv_context.Clean();
    refitting_level_ = false;

    return status;
  }

  refitting_level_ = false;
  return Status::OK();
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::MaxMemCompactionLevel(ColumnFamilyHandle* /*column_family*/) {
  return 0;
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  InstrumentedMutexLock l(&mutex_);
  return cfh->cfd()
      ->GetSuperVersion()
      ->mutable_cf_options.level0_stop_writes_trigger;
}

Status DBImpl::FlushAllColumnFamilies(const FlushOptions& flush_options,
                                      FlushReason flush_reason) {
  mutex_.AssertHeld();
  Status status;
  if (immutable_db_options_.atomic_flush) {
    mutex_.Unlock();
    status = AtomicFlushMemTables(flush_options, flush_reason);
    if (status.IsColumnFamilyDropped()) {
      status = Status::OK();
    }
    mutex_.Lock();
  } else {
    for (auto cfd : versions_->GetRefedColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      mutex_.Unlock();
      status = FlushMemTable(cfd, flush_options, flush_reason);
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:1");
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:2");
      mutex_.Lock();
      if (!status.ok() && !status.IsColumnFamilyDropped()) {
        break;
      } else if (status.IsColumnFamilyDropped()) {
        status = Status::OK();
      }
    }
  }
  return status;
}

// 对单个列族执行手动 flush。
Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  // 将句柄转成内部实现类型，以便取列族数据
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  // 记录开始日志
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "[%s] Manual flush start.",
                 cfh->GetName().c_str());
  Status s;
  // 若开启原子 flush，则对该 CF 做原子 flush（多 CF 一起刷）
  if (immutable_db_options_.atomic_flush) {
    s = AtomicFlushMemTables(flush_options, FlushReason::kManualFlush,
                             {cfh->cfd()});
  } else {
    // 否则只对该列族做普通 flush
    s = FlushMemTable(cfh->cfd(), flush_options, FlushReason::kManualFlush);
  }
  // 记录结束日志并返回状态
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual flush finished, status: %s\n",
                 cfh->GetName().c_str(), s.ToString().c_str());
  return s;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  if (!immutable_db_options_.atomic_flush) {
    for (auto cfh : column_families) {
      s = Flush(flush_options, cfh);
      if (!s.ok()) {
        break;
      }
    }
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush start.\n"
                   "=====Column families:=====");
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
    autovector<ColumnFamilyData*> cfds;
    std::for_each(column_families.begin(), column_families.end(),
                  [&cfds](ColumnFamilyHandle* elem) {
                    auto cfh = static_cast<ColumnFamilyHandleImpl*>(elem);
                    cfds.emplace_back(cfh->cfd());
                  });
    s = AtomicFlushMemTables(flush_options, FlushReason::kManualFlush, cfds);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush finished, status: %s\n"
                   "=====Column families:=====",
                   s.ToString().c_str());
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
  }
  return s;
}

Status DBImpl::RunManualCompaction(
    ColumnFamilyData* cfd, int input_level, int output_level,
    const CompactRangeOptions& compact_range_options, const Slice* begin,
    const Slice* end, bool exclusive, bool disallow_trivial_move,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts,
    int* final_output_level) {
  // 断言：输入层必须有效（要么是所有层，要么是非负数）
  assert(input_level == ColumnFamilyData::kCompactAllLevels ||
         input_level >= 0);

  // 存储范围键的内部表示（InternalKey 会包含 sequence number）
  InternalKey begin_storage, end_storage;
  CompactionArg* ca = nullptr; // 压缩参数，用于传递给后台工作线程

  // 调度状态标志
  bool scheduled = false;    // 是否已经调度了压缩任务
  bool unscheduled = false;  // 是否已经取消调度了压缩任务
  Env::Priority thread_pool_priority = Env::Priority::TOTAL; // 线程池优先级
  bool manual_conflict = false; // 是否存在冲突的压缩

  // 初始化手动压缩状态对象
  ManualCompactionState manual(
      cfd, input_level, output_level, compact_range_options.target_path_id,
      exclusive, disallow_trivial_move, compact_range_options.canceled);

  // 对于 Universal 和 FIFO 压缩风格，强制压缩所有文件（忽略范围参数）
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
    manual.begin = nullptr; // 压缩整个数据范围（从最小键开始）
  } else {
    // 设置为 user key 的最小可能 sequence number（包含所有版本）
    begin_storage.SetMinPossibleForUserKey(*begin);
    manual.begin = &begin_storage;
  }

  if (end == nullptr ||
      cfd->ioptions()->compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
    manual.end = nullptr; // 压缩整个数据范围（到最大键结束）
  } else {
    // 设置为 user key 的最大可能 sequence number（包含所有版本）
    end_storage.SetMaxPossibleForUserKey(*end);
    manual.end = &end_storage;
  }

  // 测试同步点，用于单元测试
  TEST_SYNC_POINT("DBImpl::RunManualCompaction:0");
  TEST_SYNC_POINT("DBImpl::RunManualCompaction:1");

  // 获取数据库锁，保护共享状态
  InstrumentedMutexLock l(&mutex_);

  // 检查是否暂停了手动压缩
  // DisableManualCompaction() 会等待所有手动压缩完成后才设置该标志
  // 此时不应再添加新的手动压缩任务，直接返回
  if (manual_compaction_paused_ > 0) {
    // Does not make sense to `AddManualCompaction()` in this scenario since
    // `DisableManualCompaction()` just waited for the manual compaction queue
    // to drain. So return immediately.
    TEST_SYNC_POINT("DBImpl::RunManualCompaction:PausedAtStart");
    manual.status =
        Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    manual.done = true;
    return manual.status;
  }

  // 当手动压缩请求到达时，需要处理排他性调度：
  // 1. 临时禁用非手动压缩的调度（通过 HasPendingManualCompaction 标志）
  // 2. 如果 exclusive=true，等待所有后台压缩任务完成
  //
  // 这样做是为了确保手动压缩能够访问任意范围的键/文件。
  // 现在这是可选项（通过 CompactRangeOptions::exclusive_manual_compaction 控制）
  //
  // HasPendingManualCompaction() 在至少有一个线程在 RunManualCompaction() 中时返回 true
  // 此时不会有其他压缩被调度（见 MaybeScheduleFlushOrCompaction）
  //
  // 注意：下面的循环不会阻止多个线程同时调用 RunManualCompaction()
  // 多个线程都可以到达下面的第二个 while 循环
  // 但只有一个线程会实际调度压缩，其他线程会在条件变量上等待直到完成

  // 将手动压缩任务添加到挂起的压缩队列
  AddManualCompaction(&manual);
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunManualCompaction:NotScheduled", &mutex_);

  // 如果要求排他性执行（exclusive=true）
  if (exclusive) {
    // 限制：当用户设置 *manual.canceled 时，没有方式唤醒下面的循环
    // 所以 exclusive_manual_compaction 和 canceled 可能无法很好地协同工作
    while (bg_bottom_compaction_scheduled_ > 0 ||
           bg_compaction_scheduled_ > 0) {
      // 检查是否被取消或暂停
      if (manual_compaction_paused_ > 0 || manual.canceled == true) {
        // 停止等待，假装错误来自压缩，以便下面的清理/错误处理代码能够处理它
        manual.done = true;
        manual.status =
            Status::Incomplete(Status::SubCode::kManualCompactionPaused);
        break;
      }
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:WaitScheduled");
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "[%s] Manual compaction waiting for all other scheduled background "
          "compactions to finish",
          cfd->GetName().c_str());
      // 等待条件变量，直到所有后台压缩完成
      bg_cv_.Wait();
    }
  }

  // 创建日志缓冲区，用于批量写入日志
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  ROCKS_LOG_BUFFER(&log_buffer, "[%s] Manual compaction starting",
                   cfd->GetName().c_str());

  // 主循环：持续执行直到手动压缩完成
  // We don't check bg_error_ here, because if we get the error in compaction,
  // the compaction will set manual.status to bg_error_ and set manual.done to
  // true.
  while (!manual.done) {
    assert(HasPendingManualCompaction());
    manual_conflict = false;
    Compaction* compaction = nullptr;

    // 检查是否应该运行手动压缩：
    // 1. ShouldntRunManualCompaction：检查是否有其他条件阻止运行（如暂停、取消）
    // 2. manual.in_progress：检查是否已经在运行中
    // 3. scheduled：检查是否已经调度了
    // 4. 尝试创建压缩计划：
    //    - manual.manual_end = &manual.tmp_storage1：设置输出参数接收实际压缩范围
    //    - 调用 CompactRange() 创建压缩计划
    //    - 如果返回 nullptr 且 manual_conflict=true，说明存在冲突的压缩
    if (ShouldntRunManualCompaction(&manual) || (manual.in_progress == true) ||
        scheduled ||
        (((manual.manual_end = &manual.tmp_storage1) != nullptr) &&
         ((compaction = manual.cfd->CompactRange(
               *manual.cfd->GetLatestMutableCFOptions(), mutable_db_options_,
               manual.input_level, manual.output_level, compact_range_options,
               manual.begin, manual.end, &manual.manual_end, &manual_conflict,
               max_file_num_to_ignore, trim_ts)) == nullptr &&
          manual_conflict))) {

      // 进入这个分支说明：
      // - 还没有调度压缩，或者
      // - 存在冲突的压缩，需要等待
      if (!scheduled) {
        // There is a conflicting compaction
        if (manual_compaction_paused_ > 0 || manual.canceled == true) {
          // Stop waiting since it was canceled. Pretend the error came from
          // compaction so the below cleanup/error handling code can process it.
          manual.done = true;
          manual.status =
              Status::Incomplete(Status::SubCode::kManualCompactionPaused);
        }
      }
      if (!manual.done) {
        // 等待条件变量，直到状态变化（如冲突的压缩完成）
        bg_cv_.Wait();
      }

      // 处理暂停时的取消调度逻辑
      if (manual_compaction_paused_ > 0 && scheduled && !unscheduled) {
        assert(thread_pool_priority != Env::Priority::TOTAL);
        // unschedule all manual compactions
        // 取消所有手动压缩任务（从线程池队列中移除）
        auto unscheduled_task_num = env_->UnSchedule(
            GetTaskTag(TaskType::kManualCompaction), thread_pool_priority);
        if (unscheduled_task_num > 0) {
          ROCKS_LOG_INFO(
              immutable_db_options_.info_log,
              "[%s] Unscheduled %d number of manual compactions from the "
              "thread-pool",
              cfd->GetName().c_str(), unscheduled_task_num);
          // 可能会取消其他手动压缩任务，通知所有等待的线程
          bg_cv_.SignalAll();
        }
        unscheduled = true;
        TEST_SYNC_POINT("DBImpl::RunManualCompaction:Unscheduled");
      }

      // 如果压缩不完整（incomplete=true），重置调度状态
      // 这允许在压缩完成后重新调度
      if (scheduled && manual.incomplete == true) {
        assert(!manual.in_progress);
        scheduled = false;
        manual.incomplete = false;
      }
    } else if (!scheduled) {
      // 成功创建了压缩计划，且还没有调度
      if (compaction == nullptr) {
        // 不需要压缩（范围为空）或存在冲突的压缩（但在上面的逻辑中已处理）
        manual.done = true;
        if (final_output_level) {
          // No compaction needed or there is a conflicting compaction.
          // Still set `final_output_level` to the level where we would
          // have compacted to.
          *final_output_level = output_level;
          if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
            // 如果输出层是 base_level，则获取实际的 base level
            *final_output_level = cfd->current()->storage_info()->base_level();
          }
        }
        bg_cv_.SignalAll();
        continue;
      }

      // 创建压缩参数对象，传递给后台工作线程
      ca = new CompactionArg;
      ca->db = this;
      ca->prepicked_compaction = new PrepickedCompaction;
      ca->prepicked_compaction->manual_compaction_state = &manual;
      ca->prepicked_compaction->compaction = compaction;

      // 请求压缩 token（用于控制并发压缩数量）
      // 手动压缩不会被限流，只统计任务数
      if (!RequestCompactionToken(
              cfd, true, &ca->prepicked_compaction->task_token, &log_buffer)) {
        // Don't throttle manual compaction, only count outstanding tasks.
        assert(false);
      }

      manual.incomplete = false;

      // 根据压缩类型选择线程池：
      // - bottommost_level 压缩：使用 BOTTOM 优先级线程池（如果配置了）
      // - 普通压缩：使用 LOW 优先级线程池
      if (compaction->bottommost_level() &&
          env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
        bg_bottom_compaction_scheduled_++; // 增加 bottom 压缩计数
        ca->compaction_pri_ = Env::Priority::BOTTOM;
        env_->Schedule(&DBImpl::BGWorkBottomCompaction, ca,
                       Env::Priority::BOTTOM,
                       GetTaskTag(TaskType::kManualCompaction),
                       &DBImpl::UnscheduleCompactionCallback);
        thread_pool_priority = Env::Priority::BOTTOM;
      } else {
        bg_compaction_scheduled_++; // 增加普通压缩计数
        ca->compaction_pri_ = Env::Priority::LOW;
        env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW,
                       GetTaskTag(TaskType::kManualCompaction),
                       &DBImpl::UnscheduleCompactionCallback);
        thread_pool_priority = Env::Priority::LOW;
      }

      scheduled = true; // 标记为已调度
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:Scheduled");

      // 记录最终输出层
      if (final_output_level) {
        *final_output_level = compaction->output_level();
      }
    }
    // 注意：在循环中，当 scheduled=true 时，我们会在 bg_cv_.Wait() 上等待
    // 压缩完成后，BGWorkCompaction/BGWorkBottomCompaction 会：
    // 1. 设置 manual.done = true（如果完成）
    // 2. 设置 manual.incomplete = true（如果需要继续下一轮压缩）
    // 3. 调用 bg_cv_.SignalAll() 唤醒等待线程
  }

  // 将缓冲的日志刷新到日志文件
  log_buffer.FlushBufferToLog();

  // 清理：从挂起的压缩队列中移除
  assert(!manual.in_progress);
  assert(HasPendingManualCompaction());
  RemoveManualCompaction(&manual);

  // 如果手动压缩被取消调度（因为暂停），尝试调度其他压缩任务
  // 可能有其他压缩任务被排他性的手动压缩阻塞了
  if (manual.status.IsIncomplete() &&
      manual.status.subcode() == Status::SubCode::kManualCompactionPaused) {
    MaybeScheduleFlushOrCompaction();
  }

  // 唤醒所有等待的线程
  bg_cv_.SignalAll();
  return manual.status;
}

void DBImpl::GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                                  FlushReason flush_reason, FlushRequest* req) {
  assert(req != nullptr);
  req->flush_reason = flush_reason;
  req->cfd_to_max_mem_id_to_persist.reserve(cfds.size());
  for (const auto cfd : cfds) {
    if (nullptr == cfd) {
      // cfd may be null, see DBImpl::ScheduleFlushes
      continue;
    }
    uint64_t max_memtable_id = cfd->imm()->GetLatestMemTableID();
    req->cfd_to_max_mem_id_to_persist.emplace(cfd, max_memtable_id);
  }
}

// 将指定列族的 memtable 刷到 L0 SST（单列族、非原子 flush 时使用）。
Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& flush_options,
                             FlushReason flush_reason,
                             bool entered_write_thread) {
  assert(!immutable_db_options_.atomic_flush);
  // 若不等待且当前写入已停止，则无法执行手动 flush，直接返回 TryAgain
  if (!flush_options.wait && write_controller_.IsStopped()) {
    std::ostringstream oss;
    oss << "Writes have been stopped, thus unable to perform manual flush. "
           "Please try again later after writes are resumed";
    return Status::TryAgain(oss.str());
  }
  Status s;
  // 若不允许写停顿，则先等待直到本次 flush 不会引发写停顿
  if (!flush_options.allow_write_stall) {
    bool flush_needed = true;
    s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
    TEST_SYNC_POINT("DBImpl::FlushMemTable:StallWaitDone");
    if (!s.ok() || !flush_needed) {
      return s;
    }
  }

  // 若当前不在写线程里，则需要先加入写线程，以便后续能安全切 memtable
  const bool needs_to_join_write_thread = !entered_write_thread; //true
  autovector<FlushRequest> flush_reqs;
  autovector<uint64_t> memtable_ids_to_wait;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (needs_to_join_write_thread) {
      // 以非批处理方式加入写线程，避免与其它写请求交错（自己是STATE_GROUP_LEADER）
      write_thread_.EnterUnbatched(&w, &mutex_);
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }
    }
    // 等待所有尚未完成的写请求结束，再切 memtable
    WaitForPendingWrites();

    // 若非错误恢复重试且（当前 mem 非空或仍有可恢复状态），则切换 memtable
    if (flush_reason != FlushReason::kErrorRecoveryRetryFlush &&
        (!cfd->mem()->IsEmpty() || !cached_recoverable_state_empty_.load())) {
      s = SwitchMemtable(cfd, &context);
    }
    const uint64_t flush_memtable_id = std::numeric_limits<uint64_t>::max();
    if (s.ok()) {
      // 若有待刷的 imm 或 mem 非空或可恢复状态非空，则构造本 CF 的 flush 请求
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load()) {
        FlushRequest req{flush_reason, {{cfd, flush_memtable_id}}};
        flush_reqs.emplace_back(std::move(req));
        memtable_ids_to_wait.emplace_back(cfd->imm()->GetLatestMemTableID());
      }
      // 若开启持久化统计且非错误恢复重试，检查是否需要顺带 flush 统计列族
      if (immutable_db_options_.persist_stats_to_disk &&
          flush_reason != FlushReason::kErrorRecoveryRetryFlush) {
        ColumnFamilyData* cfd_stats =
            versions_->GetColumnFamilySet()->GetColumnFamily(
                kPersistentStatsColumnFamilyName);
        if (cfd_stats != nullptr && cfd_stats != cfd &&
            !cfd_stats->mem()->IsEmpty()) {
          // 仅当统计 CF 会是当前 flush 后唯一落后的 CF 时才强制刷统计 CF
          bool stats_cf_flush_needed = true;
          for (auto* loop_cfd : *versions_->GetColumnFamilySet()) {
            if (loop_cfd == cfd_stats || loop_cfd == cfd) {
              continue;
            }
            if (loop_cfd->GetLogNumber() <= cfd_stats->GetLogNumber()) {
              stats_cf_flush_needed = false;
            }
          }
          if (stats_cf_flush_needed) {
            ROCKS_LOG_INFO(immutable_db_options_.info_log,
                           "Force flushing stats CF with manual flush of %s "
                           "to avoid holding old logs",
                           cfd->GetName().c_str());
            s = SwitchMemtable(cfd_stats, &context);
            FlushRequest req{flush_reason, {{cfd_stats, flush_memtable_id}}};
            flush_reqs.emplace_back(std::move(req));
            memtable_ids_to_wait.emplace_back(
                cfd_stats->imm()->GetLatestMemTableID());
          }
        }
      }
    }

    if (s.ok() && !flush_reqs.empty()) {
      // 对每个请求对应的 CF 标记 imm 为“已请求 flush”
      for (const auto& req : flush_reqs) {
        assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
        ColumnFamilyData* loop_cfd =
            req.cfd_to_max_mem_id_to_persist.begin()->first;
        loop_cfd->imm()->FlushRequested();
      }
      // 若调用方需要等待 flush 完成，先给相关 cfd 增加引用，防止等待期间被 drop
      if (flush_options.wait) {
        for (const auto& req : flush_reqs) {
          assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
          ColumnFamilyData* loop_cfd =
              req.cfd_to_max_mem_id_to_persist.begin()->first;
          loop_cfd->Ref();
        }
      }
      // 将 flush 请求入队
      for (const auto& req : flush_reqs) {
        SchedulePendingFlush(req);
      }
      // 尝试调度后台 flush/compaction 线程
      MaybeScheduleFlushOrCompaction();
    }

    if (needs_to_join_write_thread) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::FlushMemTable:BeforeWaitForBgFlush");
  // 若要求等待且前面成功，则阻塞直到这些 CF 的 flush 完成
  if (s.ok() && flush_options.wait) {
    autovector<ColumnFamilyData*> cfds;
    autovector<const uint64_t*> flush_memtable_ids;
    assert(flush_reqs.size() == memtable_ids_to_wait.size());
    for (size_t i = 0; i < flush_reqs.size(); ++i) {
      assert(flush_reqs[i].cfd_to_max_mem_id_to_persist.size() == 1);
      cfds.push_back(flush_reqs[i].cfd_to_max_mem_id_to_persist.begin()->first);
      flush_memtable_ids.push_back(&(memtable_ids_to_wait[i]));
    }
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        (flush_reason == FlushReason::kErrorRecovery ||
         flush_reason == FlushReason::kErrorRecoveryRetryFlush));
    InstrumentedMutexLock lock_guard(&mutex_);
    // 等待结束后释放之前为 wait 增加的 cfd 引用
    for (auto* tmp_cfd : cfds) {
      tmp_cfd->UnrefAndTryDelete();
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:FlushMemTableFinished");
  return s;
}

Status DBImpl::AtomicFlushMemTables(
    const FlushOptions& flush_options, FlushReason flush_reason,
    const autovector<ColumnFamilyData*>& provided_candidate_cfds,
    bool entered_write_thread) {
  assert(immutable_db_options_.atomic_flush);
  if (!flush_options.wait && write_controller_.IsStopped()) {
    std::ostringstream oss;
    oss << "Writes have been stopped, thus unable to perform manual flush. "
           "Please try again later after writes are resumed";
    return Status::TryAgain(oss.str());
  }
  Status s;
  autovector<ColumnFamilyData*> candidate_cfds;
  if (provided_candidate_cfds.empty()) {
    // Generate candidate cfds if not provided
    {
      InstrumentedMutexLock l(&mutex_);
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->IsDropped() && cfd->initialized()) {
          cfd->Ref();
          candidate_cfds.push_back(cfd);
        }
      }
    }
  } else {
    candidate_cfds = provided_candidate_cfds;
  }

  if (!flush_options.allow_write_stall) {
    int num_cfs_to_flush = 0;
    for (auto cfd : candidate_cfds) {
      bool flush_needed = true;
      s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
      if (!s.ok()) {
        // Unref the newly generated candidate cfds (when not provided) in
        // `candidate_cfds`
        if (provided_candidate_cfds.empty()) {
          for (auto candidate_cfd : candidate_cfds) {
            candidate_cfd->UnrefAndTryDelete();
          }
        }
        return s;
      } else if (flush_needed) {
        ++num_cfs_to_flush;
      }
    }
    if (0 == num_cfs_to_flush) {
      // Unref the newly generated candidate cfds (when not provided) in
      // `candidate_cfds`
      if (provided_candidate_cfds.empty()) {
        for (auto candidate_cfd : candidate_cfds) {
          candidate_cfd->UnrefAndTryDelete();
        }
      }
      return s;
    }
  }
  const bool needs_to_join_write_thread = !entered_write_thread;
  FlushRequest flush_req;
  autovector<ColumnFamilyData*> cfds;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (needs_to_join_write_thread) {
      write_thread_.EnterUnbatched(&w, &mutex_);
      if (two_write_queues_) {
        nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
      }
    }
    WaitForPendingWrites();

    SelectColumnFamiliesForAtomicFlush(&cfds, candidate_cfds);

    // Unref the newly generated candidate cfds (when not provided) in
    // `candidate_cfds`
    if (provided_candidate_cfds.empty()) {
      for (auto candidate_cfd : candidate_cfds) {
        candidate_cfd->UnrefAndTryDelete();
      }
    }

    for (auto cfd : cfds) {
      if ((cfd->mem()->IsEmpty() && cached_recoverable_state_empty_.load()) ||
          flush_reason == FlushReason::kErrorRecoveryRetryFlush) {
        continue;
      }
      cfd->Ref();
      s = SwitchMemtable(cfd, &context);
      cfd->UnrefAndTryDelete();
      if (!s.ok()) {
        break;
      }
    }
    if (s.ok()) {
      AssignAtomicFlushSeq(cfds);
      for (auto cfd : cfds) {
        cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (auto cfd : cfds) {
          cfd->Ref();
        }
      }
      GenerateFlushRequest(cfds, flush_reason, &flush_req);
      SchedulePendingFlush(flush_req);
      MaybeScheduleFlushOrCompaction();
    }

    if (needs_to_join_write_thread) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    autovector<const uint64_t*> flush_memtable_ids;
    for (auto& iter : flush_req.cfd_to_max_mem_id_to_persist) {
      flush_memtable_ids.push_back(&(iter.second));
    }
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        (flush_reason == FlushReason::kErrorRecovery ||
         flush_reason == FlushReason::kErrorRecoveryRetryFlush));
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* cfd : cfds) {
      cfd->UnrefAndTryDelete();
    }
  }
  return s;
}

// 在手动 flush 前等待，直到再执行一次 flush 不会引发写停顿。
Status DBImpl::WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                                 bool* flush_needed) {
  {
    *flush_needed = true;
    InstrumentedMutexLock l(&mutex_);
    // 记录当前 active memtable 的 id，用于后面判断是否已被刷掉
    uint64_t orig_active_memtable_id = cfd->mem()->GetID();
    WriteStallCondition write_stall_condition = WriteStallCondition::kNormal;
    do {
      if (write_stall_condition != WriteStallCondition::kNormal) {
        // 若有后台错误则不再等待，直接返回错误，避免死等
        if (error_handler_.IsBGWorkStopped()) {
          return error_handler_.GetBGError();
        }

        TEST_SYNC_POINT("DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait");
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[%s] WaitUntilFlushWouldNotStallWrites"
                       " waiting on stall conditions to clear",
                       cfd->GetName().c_str());
        // 在条件变量上等待，直到其它线程完成 flush/compaction 缓解停顿条件
        bg_cv_.Wait();
      }
      if (cfd->IsDropped()) {
        return Status::ColumnFamilyDropped();
      }
      if (shutting_down_.load(std::memory_order_acquire)) {
        return Status::ShutdownInProgress();
      }

      // 若等待期间原 active memtable 已被刷掉，则无需再 flush，直接返回
      uint64_t earliest_memtable_id =
          std::min(cfd->mem()->GetID(), cfd->imm()->GetEarliestMemTableID());
      if (earliest_memtable_id > orig_active_memtable_id) {
        *flush_needed = false;
        return Status::OK();
      }

      const auto& mutable_cf_options = *cfd->GetLatestMutableCFOptions();
      const auto* vstorage = cfd->current()->storage_info();

      // 若尚未达到自动 flush/compaction 的触发条件，则不再做停顿检查，直接放行
      if (cfd->imm()->NumNotFlushed() <
              cfd->ioptions()->min_write_buffer_number_to_merge &&
          vstorage->l0_delay_trigger_count() <
              mutable_cf_options.level0_file_num_compaction_trigger) {
        break;
      }

      // 模拟多一个 imm 或多一个 L0 文件，看是否会进入写停顿；若会则继续循环等待
      write_stall_condition = ColumnFamilyData::GetWriteStallConditionAndCause(
                                  cfd->imm()->NumNotFlushed() + 1,
                                  vstorage->l0_delay_trigger_count() + 1,
                                  vstorage->estimated_compaction_needed_bytes(),
                                  mutable_cf_options, *cfd->ioptions())
                                  .first;
    } while (write_stall_condition != WriteStallCondition::kNormal);
  }
  return Status::OK();
}

// Wait for memtables to be flushed for multiple column families.
// let N = cfds.size()
// 阻塞直到所列列族的待 flush memtable 全部被后台刷完（或 CF 被 drop/出错）。
// flush_memtable_ids[i] 非空表示要等到该 CF 中 id<=*flush_memtable_ids[i] 的 memtable 都刷完；为空表示等该 CF 所有 memtable 刷完。
Status DBImpl::WaitForFlushMemTables(
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const uint64_t*>& flush_memtable_ids,
    bool resuming_from_bg_err) {
  int num = static_cast<int>(cfds.size());
  InstrumentedMutexLock l(&mutex_);
  Status s;
  while (resuming_from_bg_err || !error_handler_.IsDBStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      s = Status::ShutdownInProgress();
      return s;
    }
    // 若恢复过程中已发生错误，不再等待，直接返回该错误
    if (!error_handler_.GetRecoveryError().ok()) {
      s = error_handler_.GetRecoveryError();
      break;
    }
    // 若非恢复流程且后台已停止（软错误等），直接返回后台错误
    if (!resuming_from_bg_err && error_handler_.IsBGWorkStopped() &&
        error_handler_.GetBGError().severity() < Status::Severity::kHardError) {
      s = error_handler_.GetBGError();
      return s;
    }

    int num_dropped = 0;
    int num_finished = 0;
    for (int i = 0; i < num; ++i) {
      if (cfds[i]->IsDropped()) {
        ++num_dropped;
      } else if (cfds[i]->imm()->NumNotFlushed() == 0 ||
                 (flush_memtable_ids[i] != nullptr &&
                  cfds[i]->imm()->GetEarliestMemTableID() >
                      *flush_memtable_ids[i])) {
        ++num_finished;
      }
    }
    // 若仅有一个 CF 且已被 drop，返回 ColumnFamilyDropped
    if (1 == num_dropped && 1 == num) {
      s = Status::ColumnFamilyDropped();
      return s;
    }
    // 所有涉及的 CF 要么已 drop 要么已刷完，则结束等待
    if (num_dropped + num_finished == num) {
      break;
    }
    bg_cv_.Wait();
  }
  if (!resuming_from_bg_err && error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }
  return s;
}

Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  Status s;
  for (auto cf_ptr : column_family_handles) {
    Status status =
        this->SetOptions(cf_ptr, {{"disable_auto_compactions", "false"}});
    if (!status.ok()) {
      s = status;
    }
  }

  return s;
}

// NOTE: Calling DisableManualCompaction() may overwrite the
// user-provided canceled variable in CompactRangeOptions
void DBImpl::DisableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  manual_compaction_paused_.fetch_add(1, std::memory_order_release);

  // Mark the canceled as true when the cancellation is triggered by
  // manual_compaction_paused (may overwrite user-provided `canceled`)
  for (const auto& manual_compaction : manual_compaction_dequeue_) {
    manual_compaction->canceled = true;
  }

  // Wake up manual compactions waiting to start.
  bg_cv_.SignalAll();

  // Wait for any pending manual compactions to finish (typically through
  // failing with `Status::Incomplete`) prior to returning. This way we are
  // guaranteed no pending manual compaction will commit while manual
  // compactions are "disabled".
  while (HasPendingManualCompaction()) {
    bg_cv_.Wait();
  }
}

// NOTE: In contrast to DisableManualCompaction(), calling
// EnableManualCompaction() does NOT overwrite the user-provided *canceled
// variable to be false since there is NO CHANCE a canceled compaction
// is uncanceled. In other words, a canceled compaction must have been
// dropped out of the manual compaction queue, when we disable it.
void DBImpl::EnableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  assert(manual_compaction_paused_ > 0);
  manual_compaction_paused_.fetch_sub(1, std::memory_order_release);
}

// 在持锁下检查是否有未调度的 flush/compaction，若有且未超限则向线程池提交后台任务。
void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();

  if (!opened_successfully_) {
    return;
  }

  if (bg_work_paused_ > 0) {
    return;
  } else if (error_handler_.IsBGWorkStopped() &&
             !error_handler_.IsRecoveryInProgress()) {
    return;
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  auto bg_job_limits = GetBGJobLimits();
  bool is_flush_pool_empty =
      env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;

  // 在有 flush 线程池、有未调度 flush 且未超限时，循环提交 flush 任务
  while (!is_flush_pool_empty && unscheduled_flushes_ > 0 &&
         bg_flush_scheduled_ < bg_job_limits.max_flushes) {
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:BeforeSchedule",
        &unscheduled_flushes_);

    bg_flush_scheduled_++;
    FlushThreadArg* fta = new FlushThreadArg;
    fta->db_ = this;
    fta->thread_pri_ = Env::Priority::HIGH;
    env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::HIGH, this,
                   &DBImpl::UnscheduleFlushCallback);
    --unscheduled_flushes_;

    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0",
        &unscheduled_flushes_);
  }

  // 特殊情况：如果高优先级（flush）线程池为空，则在低优先级（compaction）线程池中调度 flush
  if (is_flush_pool_empty) {
    // 循环：在低优先级线程池中调度 flush 任务
    // 条件：有未调度的 flush && 已调度的 flush + compaction 未达到 flush 上限
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ <
               bg_job_limits.max_flushes) {
      // 增加已调度的 flush 任务计数
      bg_flush_scheduled_++;

      // 创建 flush 线程参数对象
      FlushThreadArg* fta = new FlushThreadArg;

      // 设置参数中的数据库指针
      fta->db_ = this;

      // 设置线程优先级为低优先级
      fta->thread_pri_ = Env::Priority::LOW;

      // 在低优先级线程池中调度 flush 后台任务
      env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::LOW, this,
                     &DBImpl::UnscheduleFlushCallback);

      // 减少未调度的 flush 任务计数
      --unscheduled_flushes_;
    }
  }

  // 检查：如果后台压缩已暂停，直接返回
  if (bg_compaction_paused_ > 0) {
    // 后台压缩已被暂停
    return;
  } else if (error_handler_.IsBGWorkStopped()) {
    // 检查：后台工作已停止
    // 压缩不是从严重错误恢复序列的一部分
    // 我们可能到达这里是因为恢复可能执行 flush 并安装新的 super version，
    // 这会尝试调度挂起的压缩。在此退出，让更高层的恢复处理压缩
    return;
  }

  // 检查：是否有独占的手动压缩任务
  if (HasExclusiveManualCompaction()) {
    // 只允许手动压缩运行。不要调度自动压缩
    TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Conflict");
    return;
  }

  // 循环：调度低优先级（LOW）的 compaction 任务
  // 条件：已调度的 compaction + bottom compaction 未达到上限 && 有未调度的 compaction
  while (bg_compaction_scheduled_ + bg_bottom_compaction_scheduled_ <
             bg_job_limits.max_compactions &&
         unscheduled_compactions_ > 0) {
    // 创建 compaction 参数对象
    CompactionArg* ca = new CompactionArg;

    // 设置参数中的数据库指针
    ca->db = this;

    // 设置线程优先级为低优先级
    ca->compaction_pri_ = Env::Priority::LOW;

    // 设置预选的压缩任务为空（稍后会自动选择）
    ca->prepicked_compaction = nullptr;

    // 增加已调度的 compaction 任务计数
    bg_compaction_scheduled_++;

    // 减少未调度的 compaction 任务计数
    unscheduled_compactions_--;

    // 调度 compaction 后台任务
    // 参数：任务函数、任务参数、优先级、数据库实例、取消回调
    env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                   &DBImpl::UnscheduleCompactionCallback);
  }
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits() const {
  mutex_.AssertHeld();
  return GetBGJobLimits(mutable_db_options_.max_background_flushes,
                        mutable_db_options_.max_background_compactions,
                        mutable_db_options_.max_background_jobs,
                        write_controller_.NeedSpeedupCompaction());
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits(int max_background_flushes,
                                           int max_background_compactions,
                                           int max_background_jobs,
                                           bool parallelize_compactions) {
  BGJobLimits res;
  if (max_background_flushes == -1 && max_background_compactions == -1) {
    // for our first stab implementing max_background_jobs, simply allocate a
    // quarter of the threads to flushes.
    res.max_flushes = std::max(1, max_background_jobs / 4);
    res.max_compactions = std::max(1, max_background_jobs - res.max_flushes);
  } else {
    // compatibility code in case users haven't migrated to max_background_jobs,
    // which automatically computes flush/compaction limits
    res.max_flushes = std::max(1, max_background_flushes);
    res.max_compactions = std::max(1, max_background_compactions);
  }
  if (!parallelize_compactions) {
    // throttle background compactions until we deem necessary
    res.max_compactions = 1;
  }
  return res;
}

void DBImpl::AddToCompactionQueue(ColumnFamilyData* cfd) {
  assert(!cfd->queued_for_compaction());
  cfd->Ref();
  compaction_queue_.push_back(cfd);
  cfd->set_queued_for_compaction(true);
}

ColumnFamilyData* DBImpl::PopFirstFromCompactionQueue() {
  assert(!compaction_queue_.empty());
  auto cfd = *compaction_queue_.begin();
  compaction_queue_.pop_front();
  assert(cfd->queued_for_compaction());
  cfd->set_queued_for_compaction(false);
  return cfd;
}

DBImpl::FlushRequest DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  FlushRequest flush_req = std::move(flush_queue_.front());
  flush_queue_.pop_front();
  if (!immutable_db_options_.atomic_flush) {
    assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
  }
  for (const auto& elem : flush_req.cfd_to_max_mem_id_to_persist) {
    if (!immutable_db_options_.atomic_flush) {
      ColumnFamilyData* cfd = elem.first;
      assert(cfd);
      assert(cfd->queued_for_flush());
      cfd->set_queued_for_flush(false);
    }
  }
  return flush_req;
}

ColumnFamilyData* DBImpl::PickCompactionFromQueue(
    std::unique_ptr<TaskLimiterToken>* token, LogBuffer* log_buffer) {
  assert(!compaction_queue_.empty());
  assert(*token == nullptr);
  autovector<ColumnFamilyData*> throttled_candidates;
  ColumnFamilyData* cfd = nullptr;
  while (!compaction_queue_.empty()) {
    auto first_cfd = *compaction_queue_.begin();
    compaction_queue_.pop_front();
    assert(first_cfd->queued_for_compaction());
    if (!RequestCompactionToken(first_cfd, false, token, log_buffer)) {
      throttled_candidates.push_back(first_cfd);
      continue;
    }
    cfd = first_cfd;
    cfd->set_queued_for_compaction(false);
    break;
  }
  // Add throttled compaction candidates back to queue in the original order.
  for (auto iter = throttled_candidates.rbegin();
       iter != throttled_candidates.rend(); ++iter) {
    compaction_queue_.push_front(*iter);
  }
  return cfd;
}

// 将一次 flush 请求加入 flush 队列，供后台线程消费。
void DBImpl::SchedulePendingFlush(const FlushRequest& flush_req) {
  mutex_.AssertHeld();
  if (flush_req.cfd_to_max_mem_id_to_persist.empty()) {
    return;
  }
  if (!immutable_db_options_.atomic_flush) {//默认进这里
    // 非原子 flush 时，一次请求只包含一个列族
    assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
    ColumnFamilyData* cfd =
        flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
    assert(cfd);

    // 若该 CF 尚未入队且已有待刷的 imm，则入队并增加未调度 flush 计数
    if (!cfd->queued_for_flush() && cfd->imm()->IsFlushPending()) {
      cfd->Ref();
      cfd->set_queued_for_flush(true);
      ++unscheduled_flushes_;
      flush_queue_.push_back(flush_req);
    }
  } else {
    // 原子 flush：为涉及的所有 cfd 增加引用，然后整体入队
    for (auto& iter : flush_req.cfd_to_max_mem_id_to_persist) {
      ColumnFamilyData* cfd = iter.first;
      cfd->Ref();
    }
    ++unscheduled_flushes_;
    flush_queue_.push_back(flush_req);
  }
}

void DBImpl::SchedulePendingCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  if (!cfd->queued_for_compaction() && cfd->NeedsCompaction()) {
    AddToCompactionQueue(cfd);
    ++unscheduled_compactions_;
  }
}

void DBImpl::SchedulePendingPurge(std::string fname, std::string dir_to_sync,
                                  FileType type, uint64_t number, int job_id) {
  mutex_.AssertHeld();
  PurgeFileInfo file_info(fname, dir_to_sync, type, number, job_id);
  purge_files_.insert({{number, std::move(file_info)}});
}

// 后台 flush 线程入口：从线程池传入的 arg 取出 DB 和优先级，然后执行实际 flush。
void DBImpl::BGWorkFlush(void* arg) {
  // 拷贝参数内容后立即删除原指针，避免线程池释放后仍被使用
  FlushThreadArg fta = *(reinterpret_cast<FlushThreadArg*>(arg));
  delete reinterpret_cast<FlushThreadArg*>(arg);

  // 设置当前线程的 IO 统计线程池 ID（HIGH/LOW），便于按优先级统计
  IOSTATS_SET_THREAD_POOL_ID(fta.thread_pri_);

  TEST_SYNC_POINT("DBImpl::BGWorkFlush");

  // 调用真正的 flush 逻辑
  static_cast_with_check<DBImpl>(fta.db_)->BackgroundCallFlush(fta.thread_pri_);

  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  // 将参数转换为 CompactionArg 并复制内容
  // 使用值拷贝而不是指针，确保在删除原始参数后仍然可以访问数据
  CompactionArg ca = *(reinterpret_cast<CompactionArg*>(arg));

  // 删除传递进来的参数指针（已复制到 ca 中）
  // 这是必要的，因为线程池使用 new 分配的内存传递参数
  delete reinterpret_cast<CompactionArg*>(arg);

  // 设置 IO 统计中的线程池 ID 为低优先级
  // 用于区分不同优先级线程的 IO 操作统计
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);

  // 测试同步点：在压缩工作开始时
  // 用于测试和调试，可以在特定点注入延迟或检查状态
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");

  // 获取预选的压缩任务指针
  // 如果是自动压缩，可能为 nullptr；如果是手动压缩，会指向具体的压缩任务
  auto prepicked_compaction =
      static_cast<PrepickedCompaction*>(ca.prepicked_compaction);

  // 调用实际的压缩处理函数
  // 参数：预选的压缩任务、线程优先级（LOW）
  // 这里会将执行权转移给 BackgroundCallCompaction 函数
  static_cast_with_check<DBImpl>(ca.db)->BackgroundCallCompaction(
      prepicked_compaction, Env::Priority::LOW);

  // 删除预选的压缩任务对象（如果存在）
  // 释放动态分配的内存
  delete prepicked_compaction;
}

void DBImpl::BGWorkBottomCompaction(void* arg) {
  // ===== Bottom 优先级压缩后台工作线程入口函数 =====
  //
  // 该函数是 bottommost 压缩任务的后台线程入口，负责：
  // 1. 从参数中提取压缩任务信息
  // 2. 设置线程池 ID 用于 IO 统计
  // 3. 调用实际的压缩执行函数
  //
  // Bottommost 压缩特点：
  // - 指的是压缩到 LSM 树最底层的压缩操作
  // - 这些压缩通常涉及大量数据，需要更长时间
  // - 使用专门的 BOTTOM 优先级线程池（如果配置了）
  // - 与普通压缩分离，避免阻塞常规压缩任务
  //
  // 参数 arg：指向 CompactionArg 结构体的指针，包含压缩任务信息
  // 注意：调用者负责分配内存，此函数负责释放内存

  // 1. 从参数中提取并复制 CompactionArg 内容
  // 使用拷贝构造函数复制，以便立即释放原始参数内存
  // 这样做是为了避免在长时间运行的压缩过程中持有临时内存
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));

  // 2. 释放原始参数内存（由 RunManualCompaction 分配）
  delete static_cast<CompactionArg*>(arg);

  // 3. 设置 I/O 统计的线程池 ID
  // 这样 I/O 统计可以区分不同优先级线程池的 I/O 活动
  // 有助于性能分析和监控
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::BOTTOM);

  // 4. 测试同步点（用于单元测试）
  // 允许测试代码在此时插入自定义逻辑
  TEST_SYNC_POINT("DBImpl::BGWorkBottomCompaction");

  // 5. 提取预选的压缩任务
  // PrepickedCompaction 包含：
  // - compaction：压缩计划对象（包含输入文件、输出层等信息）
  // - manual_compaction_state：手动压缩状态（如果是手动压缩）
  // - task_token：任务限制器 token（用于控制并发任务数）
  auto* prepicked_compaction = ca.prepicked_compaction;

  // 6. 断言：确保压缩任务有效
  // prepicked_compaction 不能为空
  // compaction 也不能为空（必须有有效的压缩计划）
  assert(prepicked_compaction && prepicked_compaction->compaction);

  // 7. 调用实际的压缩执行函数
  // BackgroundCallCompaction 是压缩的核心执行函数，负责：
  // - 执行实际的压缩操作（读取输入文件、排序、写入输出文件）
  // - 更新版本信息（VersionEdit）
  // - 处理错误和重试逻辑
  // - 更新压缩状态（done、in_progress、incomplete 等）
  // - 释放压缩资源
  // - 唤醒等待的线程
  //
  // 参数：
  // - prepicked_compaction：压缩任务信息
  // - Env::Priority::BOTTOM：线程优先级标识
  ca.db->BackgroundCallCompaction(prepicked_compaction, Env::Priority::BOTTOM);

  // 8. 释放 prepicked_compaction 内存
  // 注意：compaction 对象的所有权在 BackgroundCallCompaction 中转移
  // 这里只释放 prepicked_compaction 结构体本身
  delete prepicked_compaction;
}

void DBImpl::BGWorkPurge(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:start");
  reinterpret_cast<DBImpl*>(db)->BackgroundCallPurge();
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:end");
}

void DBImpl::UnscheduleCompactionCallback(void* arg) {
  CompactionArg* ca_ptr = reinterpret_cast<CompactionArg*>(arg);
  Env::Priority compaction_pri = ca_ptr->compaction_pri_;
  if (Env::Priority::BOTTOM == compaction_pri) {
    // Decrement bg_bottom_compaction_scheduled_ if priority is BOTTOM
    ca_ptr->db->bg_bottom_compaction_scheduled_--;
  } else if (Env::Priority::LOW == compaction_pri) {
    // Decrement bg_compaction_scheduled_ if priority is LOW
    ca_ptr->db->bg_compaction_scheduled_--;
  }
  CompactionArg ca = *(ca_ptr);
  delete reinterpret_cast<CompactionArg*>(arg);
  if (ca.prepicked_compaction != nullptr) {
    // if it's a manual compaction, set status to ManualCompactionPaused
    if (ca.prepicked_compaction->manual_compaction_state) {
      ca.prepicked_compaction->manual_compaction_state->done = true;
      ca.prepicked_compaction->manual_compaction_state->status =
          Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    }
    if (ca.prepicked_compaction->compaction != nullptr) {
      ca.prepicked_compaction->compaction->ReleaseCompactionFiles(
          Status::Incomplete(Status::SubCode::kManualCompactionPaused));
      delete ca.prepicked_compaction->compaction;
    }
    delete ca.prepicked_compaction;
  }
  TEST_SYNC_POINT("DBImpl::UnscheduleCompactionCallback");
}

void DBImpl::UnscheduleFlushCallback(void* arg) {
  // Decrement bg_flush_scheduled_ in flush callback
  reinterpret_cast<FlushThreadArg*>(arg)->db_->bg_flush_scheduled_--;
  Env::Priority flush_pri = reinterpret_cast<FlushThreadArg*>(arg)->thread_pri_;
  if (Env::Priority::LOW == flush_pri) {
    TEST_SYNC_POINT("DBImpl::UnscheduleLowFlushCallback");
  } else if (Env::Priority::HIGH == flush_pri) {
    TEST_SYNC_POINT("DBImpl::UnscheduleHighFlushCallback");
  }
  delete reinterpret_cast<FlushThreadArg*>(arg);
  TEST_SYNC_POINT("DBImpl::UnscheduleFlushCallback");
}

// 从 flush 队列取出一批请求，组好参数后调用 FlushMemTablesToOutputFiles 执行实际刷盘。
Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context,
                               LogBuffer* log_buffer, FlushReason* reason,
                               Env::Priority thread_pri) {
  mutex_.AssertHeld(); // 确保当前线程持有互斥锁

  Status status; // 状态对象
  *reason = FlushReason::kOthers; // 默认Flush原因
  // If BG work is stopped due to an error, but a recovery is in progress,
  // that means this flush is part of the recovery. So allow it to go through
  // 如果后台工作因错误停止，但正在进行恢复，说明这个Flush是恢复的一部分，允许执行
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress(); // DB正在关闭
    }
  } else if (!error_handler_.IsRecoveryInProgress()) {
    status = error_handler_.GetBGError(); // 获取后台错误
  }

  if (!status.ok()) {
    return status; // 有错误则直接返回
  }

  autovector<BGFlushArg> bg_flush_args; // 后台Flush参数列表
  std::vector<SuperVersionContext>& superversion_contexts =
      job_context->superversion_contexts; // superversion上下文列表
  autovector<ColumnFamilyData*> column_families_not_to_flush; // 不需要Flush的列族列表

  // 从flush队列中选择需要Flush的列族
  while (!flush_queue_.empty()) {
    // This cfd is already referenced
    // 从队列中取出第一个Flush任务（该列族已被引用）
    auto [flush_reason, cfd_to_max_mem_id_to_persist] =
        PopFirstFromFlushQueue();
    superversion_contexts.clear(); // 清空superversion上下文
    superversion_contexts.reserve(cfd_to_max_mem_id_to_persist.size()); // 预分配空间

    // 遍历需要Flush的列族及其最大memtable ID
    for (const auto& [cfd, max_memtable_id] : cfd_to_max_mem_id_to_persist) {
      if (cfd->GetMempurgeUsed()) {
        // If imm() contains silent memtables (e.g.: because
        // MemPurge was activated), requesting a flush will
        // mark the imm_needed as true.
        // 如果imm()包含静默memtable（例如因为MemPurge被激活），
        // 请求Flush会将imm_needed标记为true
        cfd->imm()->FlushRequested();
      }

      if (cfd->IsDropped() || !cfd->imm()->IsFlushPending()) {
        // can't flush this CF, try next one
        // 列族已删除或没有待Flush的memtable，跳过
        column_families_not_to_flush.push_back(cfd);
        continue;
      }
      // 创建superversion上下文和Flush参数
      superversion_contexts.emplace_back(SuperVersionContext(true));
      bg_flush_args.emplace_back(cfd, max_memtable_id,
                                 &(superversion_contexts.back()), flush_reason);
    }
    if (!bg_flush_args.empty()) {
      break; // 找到需要Flush的列族，退出循环
    }
  }

  // 如果有需要Flush的列族
  if (!bg_flush_args.empty()) {
    auto bg_job_limits = GetBGJobLimits(); // 获取后台任务限制
    for (const auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "Calling FlushMemTableToOutputFile with column "
          "family [%s], flush slots available %d, compaction slots available "
          "%d, "
          "flush slots scheduled %d, compaction slots scheduled %d",
          cfd->GetName().c_str(), bg_job_limits.max_flushes,
          bg_job_limits.max_compactions, bg_flush_scheduled_,
          bg_compaction_scheduled_); // 记录日志
    }
    // 执行实际的Flush操作
    status = FlushMemTablesToOutputFiles(bg_flush_args, made_progress,
                                         job_context, log_buffer, thread_pri);
    TEST_SYNC_POINT("DBImpl::BackgroundFlush:BeforeFlush"); // 测试同步点
// All the CFD/bg_flush_arg in the FlushReq must have the same flush reason, so
// just grab the first one
// FlushReq中的所有列族/参数必须有相同的Flush原因，所以只需取第一个
#ifndef NDEBUG
    for (const auto& bg_flush_arg : bg_flush_args) {
      assert(bg_flush_arg.flush_reason_ == bg_flush_args[0].flush_reason_); // 断言所有Flush原因相同
    }
#endif /* !NDEBUG */
    *reason = bg_flush_args[0].flush_reason_; // 记录Flush原因
    // 释放列族引用
    for (auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      if (cfd->UnrefAndTryDelete()) {
        arg.cfd_ = nullptr; // 列族已删除，置空指针
      }
    }
  }
  // 释放不需要Flush的列族的引用
  for (auto cfd : column_families_not_to_flush) {
    cfd->UnrefAndTryDelete();
  }
  return status; // 返回状态
}

// 后台 flush 线程主函数：创建作业上下文与日志缓冲，持锁执行 flush，并做错误处理与清理。
void DBImpl::BackgroundCallFlush(Env::Priority thread_pri) {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);

  TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallFlush:start", nullptr);

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:Start:1");
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:Start:2");
  {
    InstrumentedMutexLock l(&mutex_);
    assert(bg_flush_scheduled_);
    num_running_flushes_++;

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs())); // 捕获当前文件号到pending_outputs中，用于清理
    FlushReason reason; // Flush的原因

    // 执行实际的Flush操作
    Status s = BackgroundFlush(&made_progress, &job_context, &log_buffer,
                               &reason, thread_pri);
    // 处理Flush错误：如果不是shutdown、列族删除或错误恢复的情况，等待一段时间后重试
    // 这样可以避免在环境问题导致持续失败的情况下消耗过多资源
    if (!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped() &&
        reason != FlushReason::kErrorRecovery) {
      // Wait a little bit before retrying background flush in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed flushes for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount(); // 增加并获取错误计数
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error // 唤醒等待的线程
      mutex_.Unlock(); // 释放互斥锁，允许其他操作进行
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background flush error: %s"
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      log_buffer.FlushBufferToLog(); // 刷新日志缓冲区
      LogFlush(immutable_db_options_.info_log); // 刷新日志
      immutable_db_options_.clock->SleepForMicroseconds(1000000); // 等待1秒
      mutex_.Lock(); // 重新获取互斥锁
    }

    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FlushFinish:0"); // 测试同步点
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem); // 从pending_outputs中释放文件号

    // If flush failed, we want to delete all temporary files that we might have
    // created. Thus, we force full scan in FindObsoleteFiles()
    // 如果Flush失败，强制完整扫描并删除所有临时文件
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                        !s.IsColumnFamilyDropped());

    // delete unnecessary files if any, this is done outside the mutex
    // 删除不需要的文件，这部分操作在互斥锁外执行以提高效率
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock(); // 释放互斥锁
      TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FilesFound"); // 测试同步点
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      // 必须在bg_flush_scheduled_递减之前刷新日志，因为一旦计数为0且锁被释放，
      // DB的析构函数可能启动并销毁所有DB状态，导致info_log不可用
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context); // 删除过期文件
      }
      job_context.Clean(); // 清理作业上下文
      mutex_.Lock(); // 重新获取互斥锁
    }
    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:ContextCleanedUp"); // 测试同步点

    assert(num_running_flushes_ > 0); // 确保运行计数正确
    num_running_flushes_--; // 减少正在运行的Flush计数
    bg_flush_scheduled_--; // 减少已调度的Flush计数
    // See if there's more work to be done
    // 检查是否还有更多工作需要执行
    MaybeScheduleFlushOrCompaction(); // 尝试调度新的Flush或Compaction
    atomic_flush_install_cv_.SignalAll(); // 唤醒等待原子Flush安装的线程
    bg_cv_.SignalAll(); // 唤醒所有等待的线程（包括DB析构函数）
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
    // 重要：调用SignalAll后不能再有代码，因为可能会触发DB析构，
    // 导致所有DB变量被释放，再访问它们会导致问题
  }
}

/**
 * @brief 后台Compaction（压缩）调用的主函数
 *
 * 该函数是后台Compaction线程的入口点，负责：
 * 1. 初始化Compaction作业上下文和日志缓冲区
 * 2. 在持有互斥锁的情况下执行Compaction操作
 * 3. 处理Compaction过程中的各种状态：
 *    - 忙碌状态（s.IsBusy()）：短暂等待避免热循环
 *    - 错误状态：等待后重试，避免环境问题消耗资源
 *    - 手动Compaction暂停状态：记录但不等待
 * 4. 清理临时文件和过期文件
 * 5. 减少调度计数并触发下一次调度
 * 6. 通知等待的线程（如DB析构函数、DelayWrite等）
 *
 * @param prepicked_compaction 预先选择的Compaction任务，可为nullptr表示自动选择
 * @param bg_thread_pri 后台线程的优先级（LOW或BOTTOM）
 *
 * @note 该函数必须在持有mutex_的情况下运行大部分逻辑
 * @note bg_compaction_scheduled_或bg_bottom_compaction_scheduled_必须大于0
 * @note 调用SignalAll后不能再访问DB变量，因为DB可能被析构
 */
void DBImpl::BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                      Env::Priority bg_thread_pri) {
  bool made_progress = false; // 是否实际完成了Compaction工作
  JobContext job_context(next_job_id_.fetch_add(1), true); // 创建新的作业上下文
  TEST_SYNC_POINT("BackgroundCallCompaction:0"); // 测试同步点
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get()); // 初始化日志缓冲区
  {
    InstrumentedMutexLock l(&mutex_); // 获取互斥锁

    num_running_compactions_++; // 增加正在运行的Compaction计数

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs())); // 捕获当前文件号到pending_outputs中，用于清理

    // 断言线程优先级和调度计数的一致性
    assert((bg_thread_pri == Env::Priority::BOTTOM &&
            bg_bottom_compaction_scheduled_) ||
           (bg_thread_pri == Env::Priority::LOW && bg_compaction_scheduled_));

    // 执行实际的Compaction操作
    Status s = BackgroundCompaction(&made_progress, &job_context, &log_buffer,
                                    prepicked_compaction, bg_thread_pri);
    TEST_SYNC_POINT("BackgroundCallCompaction:1"); // 测试同步点

    // 处理Compaction的各种状态
    if (s.IsBusy()) {
      // Compaction忙碌状态：短暂等待避免热循环
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error // 唤醒等待的线程
      mutex_.Unlock(); // 释放互斥锁
      immutable_db_options_.clock->SleepForMicroseconds(
          10000);  // prevent hot loop // 等待10毫秒避免热循环
      mutex_.Lock(); // 重新获取互斥锁
    } else if (!s.ok() && !s.IsShutdownInProgress() &&
               !s.IsManualCompactionPaused() && !s.IsColumnFamilyDropped()) {
      // Compaction错误状态：等待一段时间后重试，避免环境问题消耗资源
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount(); // 增加并获取错误计数
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error // 唤醒等待的线程
      mutex_.Unlock(); // 释放互斥锁
      log_buffer.FlushBufferToLog(); // 刷新日志缓冲区
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background compaction error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      LogFlush(immutable_db_options_.info_log); // 刷新日志
      immutable_db_options_.clock->SleepForMicroseconds(1000000); // 等待1秒
      mutex_.Lock(); // 重新获取互斥锁
    } else if (s.IsManualCompactionPaused()) {
      // 手动Compaction暂停状态：仅记录日志，不等待
      assert(prepicked_compaction);
      ManualCompactionState* m = prepicked_compaction->manual_compaction_state;
      assert(m);
      ROCKS_LOG_BUFFER(&log_buffer, "[%s] [JOB %d] Manual compaction paused",
                       m->cfd->GetName().c_str(), job_context.job_id);
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem); // 从pending_outputs中释放文件号

    // If compaction failed, we want to delete all temporary files that we
    // might have created (they might not be all recorded in job_context in
    // case of a failure). Thus, we force full scan in FindObsoleteFiles()
    // 如果Compaction失败，强制完整扫描并删除所有临时文件
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                        !s.IsManualCompactionPaused() &&
                                        !s.IsColumnFamilyDropped() &&
                                        !s.IsBusy());
    TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"); // 测试同步点

    // delete unnecessary files if any, this is done outside the mutex
    // 删除不需要的文件，这部分操作在互斥锁外执行以提高效率
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock(); // 释放互斥锁
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      // 必须在bg_compaction_scheduled_递减之前刷新日志，因为一旦计数为0且锁被释放，
      // DB的析构函数可能启动并销毁所有DB状态，导致info_log不可用
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context); // 删除过期文件
        TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles"); // 测试同步点
      }
      job_context.Clean(); // 清理作业上下文
      mutex_.Lock(); // 重新获取互斥锁
    }

    assert(num_running_compactions_ > 0); // 确保运行计数正确
    num_running_compactions_--; // 减少正在运行的Compaction计数

    // 根据线程优先级减少相应的调度计数
    if (bg_thread_pri == Env::Priority::LOW) {
      bg_compaction_scheduled_--;
    } else {
      assert(bg_thread_pri == Env::Priority::BOTTOM);
      bg_bottom_compaction_scheduled_--;
    }

    // See if there's more work to be done
    // 检查是否还有更多工作需要执行
    MaybeScheduleFlushOrCompaction(); // 尝试调度新的Flush或Compaction

    if (prepicked_compaction != nullptr &&
        prepicked_compaction->task_token != nullptr) {
      // Releasing task tokens affects (and asserts on) the DB state, so
      // must be done before we potentially signal the DB close process to
      // proceed below.
      // 释放任务令牌会影响DB状态（并有断言检查），因此必须在可能触发DB关闭流程之前完成
      prepicked_compaction->task_token.reset();
    }

    // 根据条件决定是否唤醒等待的线程
    if (made_progress ||
        (bg_compaction_scheduled_ == 0 &&
         bg_bottom_compaction_scheduled_ == 0) ||
        HasPendingManualCompaction() || unscheduled_compactions_ == 0) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_{bottom,}_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // * HasPendingManualCompaction -- need to wakeup RunManualCompaction
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      // 唤醒条件：
      // - made_progress: 需要唤醒DelayWrite（写限流）
      // - bg_compaction_scheduled_为0: 需要唤醒DB析构函数
      // - 有待处理的手动Compaction: 需要唤醒RunManualCompaction
      // - 无未调度的Compaction: 可能需要唤醒等待线程
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
    // 重要：调用SignalAll后不能再有代码，因为可能会触发DB析构，
    // 导致所有DB变量被释放，再访问它们会导致问题
  }
}

/**
 * @brief 后台Compaction（压缩）操作的实现函数
 *
 * 该函数负责执行实际的Compaction操作，包括：
 * 1. 检查是否应该执行Compaction（错误状态、shutdown状态、手动压缩取消等）
 * 2. 处理手动Compaction和自动Compaction
 * 3. 选择合适的Compaction任务（从队列或预选任务）
 * 4. 检查磁盘空间是否足够
 * 5. 执行三种类型的Compaction：
 *    - Deletion Compaction: FIFO风格，直接删除旧文件
 *    - Trivial Move: 文件可以直接移动到下一层，无需合并
 *    - Non-trivial Compaction: 需要实际合并数据的完整压缩
 * 6. 处理Compaction结果和错误
 *
 * @param made_progress 输出参数，表示是否实际完成了Compaction工作
 * @param job_context 作业上下文，包含任务状态和superversion上下文
 * @param log_buffer 日志缓冲区，用于记录Compaction过程
 * @param prepicked_compaction 预先选择的Compaction任务，可为nullptr
 * @param thread_pri 线程优先级
 * @return Status Compaction操作的状态
 *
 * @note 该函数必须在持有mutex_的情况下调用
 */
Status DBImpl::BackgroundCompaction(bool* made_progress, /*输出参数，表示是否实际完成了压缩工作*/
                                    JobContext* job_context, /*作业上下文，包含任务状态和 superversion 上下文*/
                                    LogBuffer* log_buffer, /*日志缓冲区，用于记录压缩过程*/
                                    PrepickedCompaction* prepicked_compaction, /*预先选择的压缩任务*/
                                    Env::Priority thread_pri /*线程优先级*/) {
  ManualCompactionState* manual_compaction =
      prepicked_compaction == nullptr
          ? nullptr
          : prepicked_compaction->manual_compaction_state; /*参数支持手动压缩和自动压缩的统一处理*/
  *made_progress = false;//默认没有完成压缩工作
  mutex_.AssertHeld();//确保当前线程持有锁
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start"); // 测试同步点

  const ReadOptions read_options(Env::IOActivity::kCompaction);//创建一个读选项对象，用于指定IO活动

  bool is_manual = (manual_compaction != nullptr); /*标志位用于区分预先选定的压缩和从队列中挑选的压缩*/
  std::unique_ptr<Compaction> c;//创建一个压缩对象
  if (prepicked_compaction != nullptr &&
      prepicked_compaction->compaction != nullptr) {
    c.reset(prepicked_compaction->compaction); //获取预先选择的压缩任务
  }
  bool is_prepicked = is_manual || c;//判断是否是预先选择的压缩任务

  // (manual_compaction->in_progress == false);
  bool trivial_move_disallowed =
      is_manual && manual_compaction->disallow_trivial_move; /*允许手动压缩时禁用 trivial move 优化*/

  CompactionJobStats compaction_job_stats;//创建一个压缩任务统计对象
  Status status;//状态对象
  if (!error_handler_.IsBGWorkStopped()) {//检查是否有错误
    if (shutting_down_.load(std::memory_order_acquire)) {//检查是否正在关闭
      status = Status::ShutdownInProgress();//返回一个关闭中状态
    } else if (is_manual &&
               manual_compaction->canceled.load(std::memory_order_acquire)) {//检查是否手动压缩被取消
      status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);//返回一个手动压缩被取消状态
    }
  } else {
    status = error_handler_.GetBGError();//获取错误信息
    // If we get here, it means a hard error happened after this compaction
    // was scheduled by MaybeScheduleFlushOrCompaction(), but before it got
    // a chance to execute. Since we didn't pop a cfd from the compaction
    // queue, increment unscheduled_compactions_
    // 如果到达这里，说明在MaybeScheduleFlushOrCompaction()调度后、执行前发生了严重错误。
    // 由于我们没有从compaction队列中弹出cfd，所以增加unscheduled_compactions_
    unscheduled_compactions_++;//增加未调度压缩任务数
  }

  if (!status.ok()) { //状态不是OK
    if (is_manual) { //手动压缩
      manual_compaction->status = status; //设置状态
      manual_compaction->done = true; //设置完成标志
      manual_compaction->in_progress = false; //设置正在执行标志
      manual_compaction = nullptr; //置空手动压缩状态
    }
    if (c) { //压缩任务存在
      c->ReleaseCompactionFiles(status); //释放压缩文件
      c.reset(); //置空压缩任务
    }
    return status; //返回状态
  }

  if (is_manual) { //手动压缩
    // another thread cannot pick up the same work
    // 防止其他线程抢占同一个手动压缩任务
    manual_compaction->in_progress = true; //设置正在执行标志
  }

  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:InProgress"); // 测试同步点

  std::unique_ptr<TaskLimiterToken> task_token; //任务限速令牌，用于控制并发压缩数量

  // InternalKey manual_end_storage;
  // InternalKey* manual_end = &manual_end_storage;
  bool sfm_reserved_compact_space = false; //存储空间是否被压缩
  if (is_manual) {  //手动压缩
    ManualCompactionState* m = manual_compaction; //获取手动压缩状态
    assert(m->in_progress); //确保正在执行
    if (!c) { //压缩任务不存在
      m->done = true; //设置完成标志
      m->manual_end = nullptr; //设置结束标志
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Manual compaction from level-%d from %s .. "
          "%s; nothing to do\n",
          m->cfd->GetName().c_str(), m->input_level,
          (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
          (m->end ? m->end->DebugString(true).c_str() : "(end)")); //打印日志
    } else {
      // 检查磁盘空间是否足够
      bool enough_room = EnoughRoomForCompaction(
          m->cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);//检查磁盘空间是否足够

      if (!enough_room) { //磁盘空间不足
        // Then don't do the compaction
        c->ReleaseCompactionFiles(status); //释放压缩文件
        c.reset(); //置空压缩任务
        // m's vars will get set properly at the end of this function,
        // as long as status == CompactionTooLarge
        status = Status::CompactionTooLarge(); //返回一个压缩任务过大状态
      } else {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Manual compaction from level-%d to level-%d from %s .. "
            "%s; will stop at %s\n",
            m->cfd->GetName().c_str(), m->input_level, c->output_level(),
            (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
            (m->end ? m->end->DebugString(true).c_str() : "(end)"),
            ((m->done || m->manual_end == nullptr)
                 ? "(end)"
                 : m->manual_end->DebugString(true).c_str()));//记录压缩计划
      }
    }
  } else if (!is_prepicked && !compaction_queue_.empty()) { //队列非空且非预选任务
    if (HasExclusiveManualCompaction()) { //存在独占性手动压缩
      // Can't compact right now, but try again later
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction()::Conflict"); // 测试同步点

      // 有独占性手动压缩，不能执行自动压缩
      unscheduled_compactions_++; //增加未调度压缩任务数

      return Status::OK(); //返回成功，稍后重试
    }

    // 从队列中选择列族进行压缩，使用任务限流控制并发压缩数量
    auto cfd = PickCompactionFromQueue(&task_token, log_buffer);
    if (cfd == nullptr) { //列空
      // Can't find any executable task from the compaction queue.
      // All tasks have been throttled by compaction thread limiter.
      // 无法从压缩队列中找到任何可执行的任务，所有任务都被压缩线程限流器限制
      ++unscheduled_compactions_; //增加未调度压缩任务数
      return Status::Busy(); //返回 Busy 状态让调用方稍后重试
    }

    // We unreference here because the following code will take a Ref() on
    // this cfd if it is going to use it (Compaction class holds a
    // reference).
    // This will all happen under a mutex so we don't have to be afraid of
    // somebody else deleting it.
    // 我们在这里取消引用，因为后续代码如果要使用这个cfd会调用Ref()
    // （Compaction类持有引用）。这都在互斥锁保护下，所以不用担心被其他人删除
    if (cfd->UnrefAndTryDelete()) { //列空
      // This was the last reference of the column family, so no need to
      // compact.
      // 这是列族的最后一个引用，无需再压缩
      return Status::OK(); //返回成功
    }

    // Pick up latest mutable CF Options and use it throughout the
    // compaction job
    // Compaction makes a copy of the latest MutableCFOptions. It should be used
    // throughout the compaction procedure to make sure consistency. It will
    // eventually be installed into SuperVersion
    // 获取最新的可变列族选项并在整个压缩任务中使用
    // Compaction会复制最新的MutableCFOptions，应该在压缩过程中使用它以确保一致性
    // 最终会被安装到SuperVersion中
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions(); //获取可变列族选项
    if (!mutable_cf_options->disable_auto_compactions && !cfd->IsDropped()) { //列族未禁用自动压缩且列族未删除
      // NOTE: try to avoid unnecessary copy of MutableCFOptions if
      // compaction is not necessary. Need to make sure mutex is held
      // until we make a copy in the following code
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():BeforePickCompaction"); // 测试同步点
      c.reset(cfd->PickCompaction(*mutable_cf_options, mutable_db_options_,
                                  log_buffer)); //尝试选择压缩任务
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():AfterPickCompaction"); // 测试同步点

      if (c != nullptr) { //压缩任务存在
        bool enough_room = EnoughRoomForCompaction(
            cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer); //检查磁盘空间是否足够

        if (!enough_room) { //磁盘空间不足
          // Then don't do the compaction
          c->ReleaseCompactionFiles(status); //释放压缩文件
          c->column_family_data()
              ->current()
              ->storage_info()
              ->ComputeCompactionScore(*(c->immutable_options()),
                                       *(c->mutable_cf_options())); //计算压缩分数
          AddToCompactionQueue(cfd); //添加到压缩队列
          ++unscheduled_compactions_; //增加未调度压缩任务数

          c.reset(); //置空压缩任务
          // Don't need to sleep here, because BackgroundCallCompaction
          // will sleep if !s.ok()
          status = Status::CompactionTooLarge(); //返回一个压缩任务过大状态
        } else {
          // update statistics
          size_t num_files = 0; //文件数量
          for (auto& each_level : *c->inputs()) { //遍历所有级别
            num_files += each_level.files.size(); //文件数量累加
          }
          RecordInHistogram(stats_, NUM_FILES_IN_SINGLE_COMPACTION, num_files); //更新文件数量统计

          // There are three things that can change compaction score:
          // 1) When flush or compaction finish. This case is covered by
          // InstallSuperVersionAndScheduleWork
          // 2) When MutableCFOptions changes. This case is also covered by
          // InstallSuperVersionAndScheduleWork, because this is when the new
          // options take effect.
          // 3) When we Pick a new compaction, we "remove" those files being
          // compacted from the calculation, which then influences compaction
          // score. Here we check if we need the new compaction even without the
          // files that are currently being compacted. If we need another
          // compaction, we might be able to execute it in parallel, so we add
          // it to the queue and schedule a new thread.
          // 有三件事可以改变压缩分数：
          // 1) Flush或Compaction完成，这种情况由InstallSuperVersionAndScheduleWork处理
          // 2) MutableCFOptions改变，这也由InstallSuperVersionAndScheduleWork处理，因为这是新选项生效的时候
          // 3) 当我们选择一个新的Compaction时，从计算中"移除"正在被压缩的文件，这会影响压缩分数
          //    这里检查即使没有当前正在被压缩的文件，是否需要新的Compaction。
          //    如果需要另一个Compaction，可能可以并行执行，所以我们添加到队列并调度新线程
          if (cfd->NeedsCompaction()) { //需要压缩
            // Yes, we need more compactions!
            AddToCompactionQueue(cfd); //添加到压缩队列
            ++unscheduled_compactions_; //增加未调度压缩任务数
            MaybeScheduleFlushOrCompaction(); //调度 flush 或压缩任务
          }
        }
      }
    }
  }

  IOStatus io_s; //输入输出状态
  if (!c) { //压缩任务不存在
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "Compaction nothing to do");
  } else if (c->deletion_compaction()) { // 删除压缩（FIFO压缩风格）
    // TODO(icanadi) Do we want to honor snapshots here? i.e. not delete old
    // file if there is alive snapshot pointing to it
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    assert(c->num_input_files(1) == 0); //输入文件数量为0
    assert(c->column_family_data()->ioptions()->compaction_style ==
           kCompactionStyleFIFO); //压缩风格为 FIFO

    compaction_job_stats.num_input_files = c->num_input_files(0); //输入文件数量

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id); //通知压缩开始

    for (const auto& f : *c->inputs(0)) { //遍历输入文件
      c->edit()->DeleteFile(c->level(), f->fd.GetNumber()); //删除文件
    } //避免读取和重写数据，直接在 manifest 中标记删除。
    status = versions_->LogAndApply(
        c->column_family_data(), *c->mutable_cf_options(), read_options,
        c->edit(), &mutex_, directories_.GetDbDir()); //应用日志并应用编辑
    io_s = versions_->io_status(); //获取 IO 状态
    InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                       &job_context->superversion_contexts[0],
                                       *c->mutable_cf_options()); //安装超级版本并调度工作
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Deleted %d files\n",
                     c->column_family_data()->GetName().c_str(),
                     c->num_input_files(0)); //删除文件数量
    *made_progress = true; //更新进度
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data()); //压缩结束
  } else if (!trivial_move_disallowed && c->IsTrivialMove()) { // Trivial Move：当文件可以直接移动到下一层时使用（没有重叠，不需要合并）
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:TrivialMove"); // 测试同步点
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    // Instrument for event update
    // TODO(yhchiang): add op details for showing trivial-move.
    ThreadStatusUtil::SetColumnFamily(c->column_family_data()); //设置列族
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION); //设置线程操作

    compaction_job_stats.num_input_files = c->num_input_files(0); //输入文件数量

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id); //通知压缩开始

    // Move files to next level
    int32_t moved_files = 0; //移动文件数量
    int64_t moved_bytes = 0; //移动字节数
    for (unsigned int l = 0; l < c->num_input_levels(); l++) { //遍历所有级别
      if (c->level(l) == c->output_level()) { //输出级别为当前级别
        continue;
      }
      for (size_t i = 0; i < c->num_input_files(l); i++) { //遍历输入文件
        FileMetaData* f = c->input(l, i); //获取文件元数据
        c->edit()->DeleteFile(c->level(l), f->fd.GetNumber()); //从当前级别删除文件
        c->edit()->AddFile(
            c->output_level(), f->fd.GetNumber(), f->fd.GetPathId(),
            f->fd.GetFileSize(), f->smallest, f->largest, f->fd.smallest_seqno,
            f->fd.largest_seqno, f->marked_for_compaction, f->temperature,
            f->oldest_blob_file_number, f->oldest_ancester_time,
            f->file_creation_time, f->epoch_number, f->file_checksum,
            f->file_checksum_func_name, f->unique_id,
            f->compensated_range_deletion_size, f->tail_size,
            f->user_defined_timestamps_persisted); //添加到输出级别

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Moving #%" PRIu64 " to level-%d %" PRIu64 " bytes\n",
            c->column_family_data()->GetName().c_str(), f->fd.GetNumber(),
            c->output_level(), f->fd.GetFileSize()); //打印移动文件
        ++moved_files; //移动文件数量++
        moved_bytes += f->fd.GetFileSize(); //统计移动字节数
      }
    }
    if (c->compaction_reason() == CompactionReason::kLevelMaxLevelSize && //压缩原因为 level 最大级别字节数
        c->immutable_options()->compaction_pri == kRoundRobin) { //优先级为轮转时
      int start_level = c->start_level(); //输入级别
      if (start_level > 0) { //输入级别大于 0
        auto vstorage = c->input_version()->storage_info(); //获取存储信息
        c->edit()->AddCompactCursor(
            start_level,
            vstorage->GetNextCompactCursor(start_level, c->num_input_files(0))); //添加游标
      }
    }
    status = versions_->LogAndApply(
        c->column_family_data(), *c->mutable_cf_options(), read_options,
        c->edit(), &mutex_, directories_.GetDbDir()); //应用日志并应用编辑
    io_s = versions_->io_status(); //获取 IO 状态
    // Use latest MutableCFOptions
    InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                       &job_context->superversion_contexts[0],
                                       *c->mutable_cf_options()); //安装超级版本并调度工作

    VersionStorageInfo::LevelSummaryStorage tmp; //存储级别信息
    c->column_family_data()->internal_stats()->IncBytesMoved(c->output_level(),
                                                             moved_bytes); //移动字节数
    {
      event_logger_.LogToBuffer(log_buffer)
          << "job" << job_context->job_id << "event"
          << "trivial_move"
          << "destination_level" << c->output_level() << "files" << moved_files
          << "total_files_size" << moved_bytes; //移动文件信息
    }
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Moved #%d files to level-%d %" PRIu64 " bytes %s: %s\n",
        c->column_family_data()->GetName().c_str(), moved_files,
        c->output_level(), moved_bytes, status.ToString().c_str(),
        c->column_family_data()->current()->storage_info()->LevelSummary(&tmp)); //移动文件信息
    *made_progress = true; //更新进度

    // Clear Instrument
    ThreadStatusUtil::ResetThreadStatus(); //清空
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (!is_prepicked && c->output_level() > 0 && /* 输出级别大于 0 */
             c->output_level() ==
                 c->column_family_data()
                     ->current()
                     ->storage_info()
                     ->MaxOutputLevel(
                         immutable_db_options_.allow_ingest_behind) && //输出级别等于最大级别
             env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) { //存在低优先级线程
    // Forward compactions involving last level to the bottom pool if it exists,
    // such that compactions unlikely to contribute to write stalls can be
    // delayed or deprioritized.
    // 将涉及最后一层的压缩转发到底层线程池（如果存在），
    // 使得不太可能导致写停顿的压缩可以被延迟或降级处理
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:ForwardToBottomPriPool"); // 测试同步点
    CompactionArg* ca = new CompactionArg; //创建 compaction 参数
    ca->db = this; //数据库
    ca->compaction_pri_ = Env::Priority::BOTTOM; //优先级
    ca->prepicked_compaction = new PrepickedCompaction; //创建预选 compaction
    ca->prepicked_compaction->compaction = c.release(); //添加 compaction
    ca->prepicked_compaction->manual_compaction_state = nullptr; //预选 compaction 状态
    // Transfer requested token, so it doesn't need to do it again.
    ca->prepicked_compaction->task_token = std::move(task_token); //添加任务令牌
    ++bg_bottom_compaction_scheduled_; //低优先级线程数++
    env_->Schedule(&DBImpl::BGWorkBottomCompaction, ca, Env::Priority::BOTTOM,
                   this, &DBImpl::UnscheduleCompactionCallback);//调度工作
  } else {
    // Non-trivial compaction: 需要实际合并数据的完整压缩
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    int output_level __attribute__((__unused__));//输出级别
    output_level = c->output_level();//获取输出级别
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:NonTrivial",
                             &output_level); // 测试同步点
    std::vector<SequenceNumber> snapshot_seqs; //快照序列
    SequenceNumber earliest_write_conflict_snapshot; //最早的写冲突快照
    SnapshotChecker* snapshot_checker; //快照检查
    GetSnapshotContext(job_context, &snapshot_seqs,
                       &earliest_write_conflict_snapshot, &snapshot_checker); // 获取快照上下文
    assert(is_snapshot_supported_ || snapshots_.empty());

    // 创建并配置压缩任务
    CompactionJob compaction_job(
        job_context->job_id, c.get(), immutable_db_options_,
        mutable_db_options_, file_options_for_compaction_, versions_.get(),
        &shutting_down_, log_buffer, directories_.GetDbDir(),
        GetDataDir(c->column_family_data(), c->output_path_id()),
        GetDataDir(c->column_family_data(), 0), stats_, &mutex_,
        &error_handler_, snapshot_seqs, earliest_write_conflict_snapshot,
        snapshot_checker, job_context, table_cache_, &event_logger_,
        c->mutable_cf_options()->paranoid_file_checks,
        c->mutable_cf_options()->report_bg_io_stats, dbname_,
        &compaction_job_stats, thread_pri, io_tracer_,
        is_manual ? manual_compaction->canceled
                  : kManualCompactionCanceledFalse_,
        db_id_, db_session_id_, c->column_family_data()->GetFullHistoryTsLow(),
        c->trim_ts(), &blob_callback_, &bg_compaction_scheduled_,
        &bg_bottom_compaction_scheduled_);  // 创建压缩任务
    compaction_job.Prepare(); // 准备

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id); //通知压缩开始
    mutex_.Unlock();//释放锁  避免阻塞其他线程
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun", nullptr); // 测试同步点
    // Should handle error?
    compaction_job.Run().PermitUncheckedError(); //运行压缩任务
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:NonTrivial:AfterRun"); // 测试同步点
    mutex_.Lock(); //获取锁

    status = compaction_job.Install(*c->mutable_cf_options()); //安装超级版本
    io_s = compaction_job.io_status(); //获取 IO 状态
    if (status.ok()) { //压缩成功
      InstallSuperVersionAndScheduleWork(c->column_family_data(),
                                         &job_context->superversion_contexts[0],
                                         *c->mutable_cf_options()); //安装超级版本并调度工作
    }
    *made_progress = true; //更新进度
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  }

  // 处理IO状态
  if (status.ok() && !io_s.ok()) { //IO 错误
    status = io_s; //更新错误状态
  } else {
    io_s.PermitUncheckedError(); //忽略 IO 错误
  }

  if (c != nullptr) { //compaction 不为空
    c->ReleaseCompactionFiles(status); //释放文件
    *made_progress = true; //更新进度

    // Need to make sure SstFileManager does its bookkeeping
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());//获取 SstFileManager
    if (sfm && sfm_reserved_compact_space) { //存在 SstFileManager
      sfm->OnCompactionCompletion(c.get()); //完成压缩
    }

    NotifyOnCompactionCompleted(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id); //通知压缩完成
  }

  // 处理各种Compaction结果状态
  if (status.ok() || status.IsCompactionTooLarge() ||
      status.IsManualCompactionPaused()) { //压缩成功或者压缩过大或者手动压缩暂停
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) { //列族被删除或者关闭
    // Ignore compaction errors found during shutting down
    // 忽略关闭期间发现的压缩错误
  } else {
    // 压缩错误处理
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "Compaction error: %s",
                   status.ToString().c_str());
    if (!io_s.ok()) { //IO 错误
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      // 写入MANIFEST时的错误。实际上，versions_->io_status()也可能是重命名CURRENT文件的结果。
      // 用当前代码很难区分。所以采取悲观策略，尝试写入新的MANIFEST。
      auto err_reason = versions_->io_status().ok()
                            ? BackgroundErrorReason::kCompaction
                            : BackgroundErrorReason::kManifestWrite; //错误原因
      error_handler_.SetBGError(io_s, err_reason); //设置错误
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction); //设置错误
    }
    if (c != nullptr && !is_manual && !error_handler_.IsBGWorkStopped()) { //compaction 不为空且非手动压缩且没有停止后台工作
      // Put this cfd back in the compaction queue so we can retry after some
      // time
      // 将cfd放回压缩队列，以便稍后重试
      auto cfd = c->column_family_data(); //获取列族数据
      assert(cfd != nullptr);
      // Since this compaction failed, we need to recompute the score so it
      // takes the original input files into account
      // 由于这个压缩失败，我们需要重新计算分数，以便将原始输入文件考虑进去
      c->column_family_data()
          ->current()
          ->storage_info()
          ->ComputeCompactionScore(*(c->immutable_options()),
                                   *(c->mutable_cf_options())); //重新计算压缩分数
      if (!cfd->queued_for_compaction()) { //不在队列中
        AddToCompactionQueue(cfd); //添加到压缩队列中
        ++unscheduled_compactions_; //未调度压缩数加1
      }
    }
  }
  // this will unref its input_version and column_family_data
  c.reset(); //释放compaction

  // 处理手动压缩的收尾工作
  if (is_manual) { //手动压缩
    ManualCompactionState* m = manual_compaction; //手动压缩状态
    if (!status.ok()) { //压缩失败
      m->status = status; //错误
      m->done = true; //完成
    }
    // For universal compaction:
    //   Because universal compaction always happens at level 0, so one
    //   compaction will pick up all overlapped files. No files will be
    //   filtered out due to size limit and left for a successive compaction.
    //   So we can safely conclude the current compaction.
    //
    //   Also note that, if we don't stop here, then the current compaction
    //   writes a new file back to level 0, which will be used in successive
    //   compaction. Hence the manual compaction will never finish.
    //
    // Stop the compaction if manual_end points to nullptr -- this means
    // that we compacted the whole range. manual_end should always point
    // to nullptr in case of universal compaction
    // 对于Universal压缩：
    //   因为Universal压缩总是在level 0发生，所以一次压缩会选取所有重叠的文件。
    //   不会有文件因为大小限制而被过滤掉并留给后续压缩。
    //   所以我们可以安全地结束当前压缩。
    //
    //   另外注意，如果我们不在这里停止，当前压缩会将新文件写回level 0，
    //   这个文件会被后续压缩使用。因此手动压缩将永远无法完成。
    //
    // 如果manual_end指向nullptr则停止压缩 -- 这意味着我们压缩了整个范围。
    // 对于Universal压缩，manual_end应该总是指向nullptr
    if (m->manual_end == nullptr) { //手动压缩结束为空
      m->done = true; //完成
    }
    if (!m->done) { //未完成
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // Universal and FIFO compactions should always compact the whole range
      // 我们只压缩了请求范围的一部分。更新*m到剩余需要压缩的范围。
      // Universal和FIFO压缩应该总是压缩整个范围
      assert(m->cfd->ioptions()->compaction_style !=
                 kCompactionStyleUniversal ||
             m->cfd->ioptions()->num_levels > 1);
      assert(m->cfd->ioptions()->compaction_style != kCompactionStyleFIFO);
      m->tmp_storage = *m->manual_end; //手动压缩结束
      m->begin = &m->tmp_storage; //手动压缩开始
      m->incomplete = true; //未完成
    }
    m->in_progress = false; //未在进度中  // not being processed anymore
  }
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Finish"); // 测试同步点
  return status; //返回状态
}

bool DBImpl::HasPendingManualCompaction() {
  return (!manual_compaction_dequeue_.empty());
}

void DBImpl::AddManualCompaction(DBImpl::ManualCompactionState* m) {
  assert(manual_compaction_paused_ == 0);
  manual_compaction_dequeue_.push_back(m);
}

void DBImpl::RemoveManualCompaction(DBImpl::ManualCompactionState* m) {
  // Remove from queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it = manual_compaction_dequeue_.erase(it);
      return;
    }
    ++it;
  }
  assert(false);
  return;
}

bool DBImpl::ShouldntRunManualCompaction(ManualCompactionState* m) {
  // 如果是排他性手动压缩，必须等待所有后台压缩完成
  // 这确保排他性压缩期间不会有任何其他压缩任务运行
  if (m->exclusive) {
    return (bg_bottom_compaction_scheduled_ > 0 ||
            bg_compaction_scheduled_ > 0);
  }

  // 对于非排他性手动压缩，检查是否存在冲突的其他手动压缩
  // 遍历手动压缩队列，检查是否需要在当前压缩之前运行其他压缩

  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  bool seen = false; // 标记是否已经遍历到当前压缩 m

  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      // 找到当前压缩，继续向后遍历
      ++it;
      seen = true;
      continue;
    } else if (MCOverlap(m, (*it)) && (!seen && !(*it)->in_progress)) {
      // Consider the other manual compaction *it, conflicts if:
      // - overlaps with m (与当前压缩重叠)
      // - and (*it) is ahead in the queue and is not yet in progress
      //   (在队列中位于当前压缩之前，且尚未开始执行)

      // 如果满足以下条件，则认为存在冲突：
      // 1. m 和 (*it) 重叠（通过 MCOverlap 判断）
      // 2. (*it) 在队列中位于 m 之前（seen=false）
      // 3. (*it) 尚未开始执行（in_progress=false）

      // 这种情况下，应该先执行 (*it)，当前压缩 m 需要等待
      return true;
    }
    ++it;
  }
  // 没有冲突，可以运行当前压缩
  return false;
}

bool DBImpl::HaveManualCompaction(ColumnFamilyData* cfd) {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    if ((cfd == (*it)->cfd) && (!((*it)->in_progress || (*it)->done))) {
      // Allow automatic compaction if manual compaction is
      // in progress
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::HasExclusiveManualCompaction() {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::MCOverlap(ManualCompactionState* m, ManualCompactionState* m1) {
  // 判断两个手动压缩是否存在冲突（是否重叠）
  //
  // 注意：从函数名 MCOverlap（Manual Compaction Overlap）来看，
  // 原本可能包含键范围重叠的检查，但当前实现已简化为：
  // 1. 排他性压缩与其他任何压缩都冲突
  // 2. 同一列族的压缩被视为冲突
  // 3. 不同列族的非排他性压缩不冲突

  // 如果任一手动压缩是排他性的，则认为它们重叠（冲突）
  // 排他性压缩要求独占整个数据库，不能与其他任何压缩同时进行
  if ((m->exclusive) || (m1->exclusive)) {
    return true;
  }

  // 如果两个压缩操作的是不同的列族，则不重叠（不冲突）
  // 不同列族的数据是独立的，可以同时压缩
  if (m->cfd != m1->cfd) {
    return false;
  }

  // 对于同一列族的非排他性压缩，当前实现总是返回 false（不冲突）
  // 这意味着同一列族的多个手动压缩可以并行执行（只要它们不是排他性的）
  //
  // 注意：这种实现假设了 RocksDB 的内部机制能够处理同一列族的并发压缩，
  // 例如通过版本控制和锁机制来保证数据一致性
  return false;
}

void DBImpl::BuildCompactionJobInfo(
    const ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id,
    const Version* current, CompactionJobInfo* compaction_job_info) const {
  assert(compaction_job_info != nullptr);
  compaction_job_info->cf_id = cfd->GetID();
  compaction_job_info->cf_name = cfd->GetName();
  compaction_job_info->status = st;
  compaction_job_info->thread_id = env_->GetThreadID();
  compaction_job_info->job_id = job_id;
  compaction_job_info->base_input_level = c->start_level();
  compaction_job_info->output_level = c->output_level();
  compaction_job_info->stats = compaction_job_stats;
  compaction_job_info->table_properties = c->GetOutputTableProperties();
  compaction_job_info->compaction_reason = c->compaction_reason();
  compaction_job_info->compression = c->output_compression();

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  for (size_t i = 0; i < c->num_input_levels(); ++i) {
    for (const auto fmd : *c->inputs(i)) {
      const FileDescriptor& desc = fmd->fd;
      const uint64_t file_number = desc.GetNumber();
      auto fn = TableFileName(c->immutable_options()->cf_paths, file_number,
                              desc.GetPathId());
      compaction_job_info->input_files.push_back(fn);
      compaction_job_info->input_file_infos.push_back(CompactionFileInfo{
          static_cast<int>(i), file_number, fmd->oldest_blob_file_number});
      if (compaction_job_info->table_properties.count(fn) == 0) {
        std::shared_ptr<const TableProperties> tp;
        auto s = current->GetTableProperties(read_options, &tp, fmd, &fn);
        if (s.ok()) {
          compaction_job_info->table_properties[fn] = tp;
        }
      }
    }
  }
  for (const auto& newf : c->edit()->GetNewFiles()) {
    const FileMetaData& meta = newf.second;
    const FileDescriptor& desc = meta.fd;
    const uint64_t file_number = desc.GetNumber();
    compaction_job_info->output_files.push_back(TableFileName(
        c->immutable_options()->cf_paths, file_number, desc.GetPathId()));
    compaction_job_info->output_file_infos.push_back(CompactionFileInfo{
        newf.first, file_number, meta.oldest_blob_file_number});
  }
  compaction_job_info->blob_compression_type =
      c->mutable_cf_options()->blob_compression_type;

  // Update BlobFilesInfo.
  for (const auto& blob_file : c->edit()->GetBlobFileAdditions()) {
    BlobFileAdditionInfo blob_file_addition_info(
        BlobFileName(c->immutable_options()->cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
        blob_file.GetTotalBlobBytes());
    compaction_job_info->blob_file_addition_infos.emplace_back(
        std::move(blob_file_addition_info));
  }

  // Update BlobFilesGarbageInfo.
  for (const auto& blob_file : c->edit()->GetBlobFileGarbages()) {
    BlobFileGarbageInfo blob_file_garbage_info(
        BlobFileName(c->immutable_options()->cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetGarbageBlobCount(),
        blob_file.GetGarbageBlobBytes());
    compaction_job_info->blob_file_garbage_infos.emplace_back(
        std::move(blob_file_garbage_info));
  }
}

// SuperVersionContext gets created and destructed outside of the lock --
// we use this conveniently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// 如果同一个 sv_context 两次调用 InstallSuperVersionAndScheduleWork()，我们不能复用
// 已分配的 SuperVersion()，因为第一次调用已经使用了它。在这种罕见情况下，
// 我们需要承担一点性能开销，在互斥锁内创建新的 SuperVersion()。
// 对于 superversion_to_free 我们也做类似处理。

/**
 * @brief 安装新的 SuperVersion 并调度后台工作（flush/compaction）
 *
 * 该函数是 RocksDB 版本管理的核心入口点，用于在配置或状态变更后更新列族的 SuperVersion，
 * 并触发必要的后台维护任务。SuperVersion 是读取操作的关键数据结构，包含了当前可用的
 * MemTableList、ImmutableMemTableList 和 Version 信息。
 *
 * @param cfd 目标列族数据指针，指定要更新 SuperVersion 的列族
 * @param sv_context SuperVersion 上下文对象，包含：
 *                   - new_superversion: 可复用的 SuperVersion 对象指针（可为空）
 *                   - superversions_to_free: 待释放的旧 SuperVersion 列表
 *                   - write_stall_notifications: 写停顿通知信息（如果启用）
 * @param mutable_cf_options 新的可变列族选项，包含 write_buffer_size、max_write_buffer_number 等配置
 *
 * @note 调用此函数时必须持有 mutex_（db_mutex）
 *
 * 设计要点：
 * 1. SuperVersion 复用优化：通过 SuperVersionContext 预分配 SuperVersion 对象，
 *    避免在持有互斥锁时进行内存分配（hot path 优化）
 * 2. 内存状态跟踪：更新 max_total_in_memory_state_ 以追踪所有列族的最大内存使用量
 * 3. 自适应触发：根据所有列族的 snapshot 情况更新 bottommost_files_mark_threshold_
 * 4. 后台任务调度：安装新版本后自动评估是否需要 flush 或 compaction
 *
 * 调用场景：
 * - Flush 完成后更新 SuperVersion（新的 immutable memtable 生成）
 * - Compaction 完成后更新 Version
 * - 配置变更（如 write_buffer_size 修改）
 * - MemTable 切换时
 * - DB 打开初始化时
 *
 * 性能考虑：
 * - 使用 UNLIKELY 分支处理罕见情况（sv_context->new_superversion 为空）
 * - 在锁外进行大部分准备工作（通过 sv_context 预分配）
 * - 批量处理 superversion 释放（延迟到后台线程）
 */
void DBImpl::InstallSuperVersionAndScheduleWork(
    ColumnFamilyData* cfd, SuperVersionContext* sv_context,
    const MutableCFOptions& mutable_cf_options) {
  // 断言：调用者必须持有数据库互斥锁
  mutex_.AssertHeld();

  // 更新 max_total_in_memory_state_：这是 DBImpl 跟踪所有列族最大内存使用量的全局变量
  // 用于 write buffer manager 判断是否需要触发写停顿或 flush
  size_t old_memtable_size = 0;
  auto* old_sv = cfd->GetSuperVersion();
  if (old_sv) {
    // 计算旧配置下的内存限制：单 buffer 大小 × buffer 数量
    old_memtable_size = old_sv->mutable_cf_options.write_buffer_size *
                        old_sv->mutable_cf_options.max_write_buffer_number;
  }

  // 创建新的 SuperVersion 对象（如果尚未创建）
  // 这是一个性能优化：通常情况下调用者会在锁外预创建 SuperVersion 对象
  // 只有在极少数情况下（如复用 sv_context）才会进入此分支
  // 注意：在锁内分配内存会增加延迟，但这个分支极少执行，影响可忽略
  if (UNLIKELY(sv_context->new_superversion == nullptr)) {
    sv_context->NewSuperVersion();
  }

  // 安装新的 SuperVersion
  // 此调用会：
  // 1. 保存当前的 SuperVersion 到 sv_context->superversions_to_free（延迟释放）
  // 2. 将新 SuperVersion 设置为当前版本
  // 3. 更新内部引用计数和版本号
  // 4. 可能触发写停顿状态变更（如果内存压力变化）
  cfd->InstallSuperVersion(sv_context, mutable_cf_options);

  // 更新 bottommost_files_mark_threshold_（底层文件标记阈值）
  // 这个值用于在 bottommost compaction 中判断哪些 key-range 包含活跃 snapshot
  // 只有大于此阈值的 sequence number 才需要在 SST 文件中保留
  //
  // 设计说明：
  // - 这里可能存在微小的数据竞争：snapshot 相关的 bottommost compaction
  //   可能在此时被释放。但假设会频繁创建和释放新的 snapshot，
  //   compaction 很快就会被重新触发，因此影响很小
  // - allow_ingest_behind 列族使用 ingest-behind 策略，不参与此计算
  //   因为它们有独立的 snapshot 处理逻辑
  bottommost_files_mark_threshold_ = kMaxSequenceNumber;
  for (auto* my_cfd : *versions_->GetColumnFamilySet()) {
    if (!my_cfd->ioptions()->allow_ingest_behind) {
      // 取所有列族的最小值作为全局阈值，确保任何列族的 snapshot 都不会被错误删除
      bottommost_files_mark_threshold_ = std::min(
          bottommost_files_mark_threshold_,
          my_cfd->current()->storage_info()->bottommost_files_mark_threshold());
    }
  }

  // 每次安装新的 SuperVersion 后，需要评估是否需要触发新的 flush 或 compaction
  // 这是因为 SuperVersion 的变化可能：
  // 1. 新增了可 flush 的 memtable（memtable 满了）
  // 2. 版本变更产生了新的 compaction 机会（如文件重叠度变化）
  // 3. 选项变更改变了触发条件
  SchedulePendingCompaction(cfd);
  MaybeScheduleFlushOrCompaction();

  // 更新 max_total_in_memory_state_：反映新配置下的内存限制
  // 公式：旧内存大小 - 旧配置内存 + 新配置内存
  // 这确保 max_total_in_memory_state_ 始终是所有列族配置的内存上限总和
  max_total_in_memory_state_ = max_total_in_memory_state_ - old_memtable_size +
                               mutable_cf_options.write_buffer_size *
                                   mutable_cf_options.max_write_buffer_number;
}

// ShouldPurge is called by FindObsoleteFiles when doing a full scan,
// and db mutex (mutex_) should already be held.
// Actually, the current implementation of FindObsoleteFiles with
// full_scan=true can issue I/O requests to obtain list of files in
// directories, e.g. env_->getChildren while holding db mutex.
bool DBImpl::ShouldPurge(uint64_t file_number) const {
  return files_grabbed_for_purge_.find(file_number) ==
             files_grabbed_for_purge_.end() &&
         purge_files_.find(file_number) == purge_files_.end();
}

// MarkAsGrabbedForPurge is called by FindObsoleteFiles, and db mutex
// (mutex_) should already be held.
void DBImpl::MarkAsGrabbedForPurge(uint64_t file_number) {
  files_grabbed_for_purge_.insert(file_number);
}

void DBImpl::SetSnapshotChecker(SnapshotChecker* snapshot_checker) {
  InstrumentedMutexLock l(&mutex_);
  // snapshot_checker_ should only set once. If we need to set it multiple
  // times, we need to make sure the old one is not deleted while it is still
  // using by a compaction job.
  assert(!snapshot_checker_);
  snapshot_checker_.reset(snapshot_checker);
}

void DBImpl::GetSnapshotContext(
    JobContext* job_context, std::vector<SequenceNumber>* snapshot_seqs,
    SequenceNumber* earliest_write_conflict_snapshot,
    SnapshotChecker** snapshot_checker_ptr) {
  mutex_.AssertHeld();
  assert(job_context != nullptr);
  assert(snapshot_seqs != nullptr);
  assert(earliest_write_conflict_snapshot != nullptr);
  assert(snapshot_checker_ptr != nullptr);

  *snapshot_checker_ptr = snapshot_checker_.get();
  if (use_custom_gc_ && *snapshot_checker_ptr == nullptr) {
    *snapshot_checker_ptr = DisableGCSnapshotChecker::Instance();
  }
  if (*snapshot_checker_ptr != nullptr) {
    // If snapshot_checker is used, that means the flush/compaction may
    // contain values not visible to snapshot taken after
    // flush/compaction job starts. Take a snapshot and it will appear
    // in snapshot_seqs and force compaction iterator to consider such
    // snapshots.
    const Snapshot* job_snapshot =
        GetSnapshotImpl(false /*write_conflict_boundary*/, false /*lock*/);
    job_context->job_snapshot.reset(new ManagedSnapshot(this, job_snapshot));
  }
  *snapshot_seqs = snapshots_.GetAll(earliest_write_conflict_snapshot);
}

Status DBImpl::WaitForCompact(
    const WaitForCompactOptions& wait_for_compact_options) {
  InstrumentedMutexLock l(&mutex_);
  if (wait_for_compact_options.flush) {
    Status s = DBImpl::FlushAllColumnFamilies(FlushOptions(),
                                              FlushReason::kManualFlush);
    if (!s.ok()) {
      return s;
    }
  }
  TEST_SYNC_POINT("DBImpl::WaitForCompact:StartWaiting");
  for (;;) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    if (bg_work_paused_ && wait_for_compact_options.abort_on_pause) {
      return Status::Aborted();
    }
    if ((bg_bottom_compaction_scheduled_ || bg_compaction_scheduled_ ||
         bg_flush_scheduled_ || unscheduled_compactions_ ||
         unscheduled_flushes_) &&
        (error_handler_.GetBGError().ok())) {
      bg_cv_.Wait();
    } else {
      return error_handler_.GetBGError();
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
