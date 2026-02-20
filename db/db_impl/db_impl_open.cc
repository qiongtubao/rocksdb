//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cinttypes>

#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/periodic_task_scheduler.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/persistent_stats_history.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/table.h"
#include "rocksdb/wal_filter.h"
#include "test_util/sync_point.h"
#include "util/rate_limiter_impl.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {
Options SanitizeOptions(const std::string& dbname, const Options& src,
                        bool read_only, Status* logger_creation_s) {
  auto db_options =
      SanitizeOptions(dbname, DBOptions(src), read_only, logger_creation_s);
  ImmutableDBOptions immutable_db_options(db_options);
  auto cf_options =
      SanitizeOptions(immutable_db_options, ColumnFamilyOptions(src));
  return Options(db_options, cf_options);
}

// ============================================================================
// 函数名: SanitizeOptions
// 功能描述: 清理和验证用户提供的数据库选项（DBOptions）
//
// 参数说明:
//   - dbname: 数据库的路径/名称，用于确定日志文件和数据库文件的存储位置
//   - src: 用户提供的原始 DBOptions 配置
//   - read_only: 是否以只读模式打开数据库
//     - true: 只读模式，不会创建日志记录器
//     - false: 读写模式，会创建日志记录器
//   - logger_creation_s: [输出参数] 用于返回日志记录器创建时的状态
//     - 如果日志创建失败，非空指针会保存失败状态
//     - 调用者可通过此状态判断日志是否可用
//
// 返回值:
//   - 返回经过清理和修正后的 DBOptions 对象
//   - 确保所有配置项都在合理范围内，避免用户错误配置导致的问题
//
// 主要功能:
//   1. 环境初始化: 设置默认的 Env（环境对象）
//   2. 文件句柄限制: 调整 max_open_files 在系统允许范围内
//   3. 日志系统创建: 为非只读模式创建信息日志记录器
//   4. 写缓冲区管理: 初始化 WriteBufferManager
//   5. 后台线程设置: 根据配置调整压缩和刷新的线程池大小
//   6. 速率限制配置: 设置默认的速率限制参数
//   7. WAL 配置冲突处理: 解决 WAL 配置之间的不兼容问题
//   8. 路径规范化: 处理数据库路径和 WAL 路径
//   9. 垃圾文件清理: 清理遗留的 .trash 文件
//  10. 两阶段提交适配: 调整与 2PC 相关的配置
//
// 使用场景:
//   - 在打开数据库（DB::Open）之前调用
//   - 确保用户提供的配置选项是有效的、合理的
//   - 防止无效配置导致运行时错误或性能问题
//
// 注意事项:
//   - 此函数会修改传入的 DBOptions，创建一个副本并返回
//   - 某些配置会被强制修改为默认值（如 bytes_per_sync、delayed_write_rate）
//   - 不兼容的配置会被自动禁用（如 recycle_log_file_num 与某些 WAL 恢复模式）
// ============================================================================
DBOptions SanitizeOptions(const std::string& dbname, const DBOptions& src,
                          bool read_only, Status* logger_creation_s) {
  // 复制原始配置，避免修改用户传入的配置对象
  DBOptions result(src);

  // === 环境初始化 ===
  // 如果用户没有设置 Env，使用默认的环境对象
  // Env 封装了文件系统操作、线程创建、时间获取等系统调用
  if (result.env == nullptr) {
    result.env = Env::Default();
  }

  // === 文件句柄限制调整 ===
  // -1 表示不限制打开的文件数量
  // 如果用户指定了具体数值，则确保该值在系统允许的范围内
  if (result.max_open_files != -1) {
    int max_max_open_files = port::GetMaxOpenFiles();
    // 如果无法获取系统最大值，使用一个较大的默认值 (4MB)
    if (max_max_open_files == -1) {
      max_max_open_files = 0x400000;
    }
    // 将 max_open_files 限制在 [20, max_max_open_files] 范围内
    // 下限 20 是为了确保基本的文件操作性能
    ClipToRange(&result.max_open_files, 20, max_max_open_files);
    // 测试同步点：用于单元测试验证此逻辑
    TEST_SYNC_POINT_CALLBACK("SanitizeOptions::AfterChangeMaxOpenFiles",
                             &result.max_open_files);
  }

  // === 日志记录器创建 ===
  // 仅在非只读模式下创建日志记录器
  // 只读模式不需要记录日志，也避免写入权限问题
  if (result.info_log == nullptr && !read_only) {
    Status s = CreateLoggerFromOptions(dbname, result, &result.info_log);
    if (!s.ok()) {
      // 没有合适的地方用于记录日志（可能是权限问题或路径不存在）
      result.info_log = nullptr;
      // 将日志创建失败的状态保存到输出参数
      if (logger_creation_s) {
        *logger_creation_s = s;
      }
    }
  }

  // === 写缓冲区管理器初始化 ===
  // 如果用户没有设置 write_buffer_manager，根据 db_write_buffer_size 创建默认的
  // WriteBufferManager 用于跟踪和控制所有列族的 memtable 内存使用
  if (!result.write_buffer_manager) {
    result.write_buffer_manager.reset(
        new WriteBufferManager(result.db_write_buffer_size));
  }

  // === 后台线程池调整 ===
  // 根据 flush、compaction 和 max_background_jobs 的配置计算实际的线程限制
  // parallelize_compactions = true 表示允许并行压缩
  auto bg_job_limits = DBImpl::GetBGJobLimits(
      result.max_background_flushes, result.max_background_compactions,
      result.max_background_jobs, true /* parallelize_compactions */);

  // 增加低优先级（压缩）后台线程
  // 压缩通常是后台任务，使用低优先级线程池
  result.env->IncBackgroundThreadsIfNeeded(bg_job_limits.max_compactions,
                                           Env::Priority::LOW);

  // 增加高优先级（刷新）后台线程
  // flush 是关键任务，影响写入性能，使用高优先级线程池
  result.env->IncBackgroundThreadsIfNeeded(bg_job_limits.max_flushes,
                                           Env::Priority::HIGH);

  // === 速率限制配置 ===
  // 如果用户配置了速率限制器，确保同步相关的配置不为 0
  if (result.rate_limiter.get() != nullptr) {
    if (result.bytes_per_sync == 0) {
      // 默认设置为 1MB，避免每次写入都同步，提高性能
      result.bytes_per_sync = 1024 * 1024;
    }
  }

  // === 延迟写入速率配置 ===
  // delayed_write_rate 用于写入流控，当系统压力大时限制写入速率
  if (result.delayed_write_rate == 0) {
    // 如果有速率限制器，使用速率限制器的配置
    if (result.rate_limiter.get() != nullptr) {
      result.delayed_write_rate = result.rate_limiter->GetBytesPerSecond();
    }
    // 如果仍然为 0（没有速率限制器或获取失败），使用默认值 16MB/s
    if (result.delayed_write_rate == 0) {
      result.delayed_write_rate = 16 * 1024 * 1024;
    }
  }

  // === WAL 回收与 WAL TTL/大小限制的互斥处理 ===
  // WAL TTL 或大小限制与 WAL 回收功能互斥
  // 原因：
  //   - WAL TTL (WAL_ttl_seconds) 根据时间删除 WAL
  //   - WAL 大小限制 (WAL_size_limit_MB) 根据大小删除 WAL
  //   - WAL 回收 (recycle_log_file_num) 重用旧的 WAL 文件名
  // 这两种机制冲突，如果启用了 TTL/大小限制，则禁用回收
  if (result.WAL_ttl_seconds > 0 || result.WAL_size_limit_MB > 0) {
    result.recycle_log_file_num = false;
  }

  // === WAL 回收与 WAL 恢复模式的兼容性检查 ===
  // 某些 WAL 恢复模式与 WAL 回收功能不兼容，需要禁用回收
  if (result.recycle_log_file_num &&
      (result.wal_recovery_mode ==
           WALRecoveryMode::kTolerateCorruptedTailRecords ||
       result.wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery ||
       result.wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency)) {
    // - kTolerateCorruptedTailRecords 与 WAL 回收不兼容：
    //   WAL 回收期望在遇到损坏记录时（新数据结束和回收数据残留处）
    //   能够成功恢复。但 kTolerateCorruptedTailRecords 必须在任何损坏记录处失败，
    //   因为它无法区分这是回收导致的假损坏还是真正的数据损坏。
    //   如果忽略真正的损坏，会导致已提交的更新被截断，违反恢复保证。
    //
    // - kPointInTimeRecovery 和 kAbsoluteConsistency 暂时不兼容：
    //   由于一个 bug 导致恢复的数据中可能存在空洞
    //   (https://github.com/facebook/rocksdb/pull/7252#issuecomment-673766236)
    //   除 bug 外，这两个功能在理论上应该是兼容的
    result.recycle_log_file_num = 0;
  }

  // === 数据库路径设置 ===
  // 如果用户没有指定 db_paths，使用 dbname 作为默认路径
  // 路径大小限制设置为最大值（uint64_max），表示不限制该路径的文件大小
  if (result.db_paths.size() == 0) {
    result.db_paths.emplace_back(dbname, std::numeric_limits<uint64_t>::max());
  } else if (result.wal_dir.empty()) {
    // 如果设置了 db_paths 但没有设置 wal_dir，使用 dbname 作为默认 WAL 目录
    result.wal_dir = dbname;
  }

  // === WAL 目录路径规范化 ===
  // 处理从旧版本选项文件读取的配置，其中强制设置了 wal_dir
  // 目的：如果 wal_dir、dbname 和 db_paths[0] 指向同一目录，则清空 wal_dir
  // 这样 wal_dir 就等于 dbname（空字符串表示使用数据库路径）
  if (!result.wal_dir.empty()) {
    // 检查 wal_dir 是否与 dbname 和 db_paths[0] 指向同一目录
    // NormalizePath 将路径规范化为标准形式（处理斜杠、相对路径等）
    auto npath = NormalizePath(dbname + "/");
    if (npath == NormalizePath(result.wal_dir + "/") &&
        npath == NormalizePath(result.db_paths[0].path + "/")) {
      // 三个路径相同，清空 wal_dir 使其使用数据库路径
      result.wal_dir.clear();
    }
  }

  // === 移除 WAL 目录路径末尾的斜杠 ===
  // 统一路径格式，避免后续处理中的路径比较问题
  if (!result.wal_dir.empty() && result.wal_dir.back() == '/') {
    result.wal_dir = result.wal_dir.substr(0, result.wal_dir.size() - 1);
  }

  // === 直接 I/O 预读大小配置 ===
  // 如果启用了直接读取（use_direct_reads）但没有设置压缩预读大小
  // 则设置一个合理的默认值（2MB）
  // 直接 I/O 需要较大的预读缓冲区来弥补缺乏操作系统页缓存的缺点
  if (result.use_direct_reads && result.compaction_readahead_size == 0) {
    TEST_SYNC_POINT_CALLBACK("SanitizeOptions:direct_io", nullptr);
    result.compaction_readahead_size = 1024 * 1024 * 2;  // 2MB
  }

  // === 两阶段提交 (2PC) 的恢复配置 ===
  // 如果启用了 2PC（allow_2pc = true），必须在数据库打开时强制执行 flush
  // 原因：
  //   - 在 2PC 模式下，无法保证连续的日志文件具有连续的序列号
  //   - 这会使 WAL 恢复过程变得复杂且不可靠
  //   - 通过在恢复时执行 flush，确保所有已提交的事务都持久化到 SST 文件
  //   - 这样可以从 SST 文件恢复，而不依赖可能不连续的 WAL
  if (result.allow_2pc) {
    result.avoid_flush_during_recovery = false;  // 强制在恢复时 flush
  }

  // === WAL 目录与数据库目录不同时的垃圾文件清理 ===
  // 创建不可变数据库选项用于路径判断
  ImmutableDBOptions immutable_db_options(result);
  if (!immutable_db_options.IsWalDirSameAsDBPath()) {
    // WAL 目录与数据库主目录不同（或无法确定是否相同）
    // 在这种情况下，显式清理 WAL 目录中的垃圾日志文件（.log.trash）
    // 绕过 DeleteScheduler，因为这些是立即删除而不是延迟调度
    // 优先执行此操作，确保即使稍后调用 DeleteScheduler::CleanupDirectory 也不会冲突
    std::vector<std::string> filenames;
    IOOptions io_opts;
    io_opts.do_not_recurse = true;  // 不递归遍历子目录
    auto wal_dir = immutable_db_options.GetWalDir();
    Status s = immutable_db_options.fs->GetChildren(
        wal_dir, io_opts, &filenames, /*IODebugContext*=*/nullptr);
    // 错误被忽略，因为这是清理操作，失败不应阻止数据库打开
    s.PermitUncheckedError();  // TODO: 需要确定如何处理错误

    // 查找并删除所有 .log.trash 文件
    // 这些文件是之前 WAL 回收过程中被标记为垃圾但未被删除的文件
    for (std::string& filename : filenames) {
      // 检查文件名是否以 ".log.trash" 结尾
      if (filename.find(".log.trash", filename.length() -
                                          std::string(".log.trash").length()) !=
          std::string::npos) {
        std::string trash_file = wal_dir + "/" + filename;
        // 立即删除垃圾文件，忽略删除失败
        result.env->DeleteFile(trash_file).PermitUncheckedError();
      }
    }
  }

  // === SST 垃圾文件清理 ===
  // 数据库停止时可能存在一些未删除的 .trash 文件（例如 SST 文件的垃圾文件）
  // 打开数据库时，查找这些 .trash 文件并安排删除（或立即删除）
  // 如果使用了 SstFileManager，则通过 DeleteScheduler 延迟删除
  // 如果没有使用，则立即删除
  auto sfm = static_cast<SstFileManagerImpl*>(result.sst_file_manager.get());
  for (size_t i = 0; i < result.db_paths.size(); i++) {
    // 对每个配置的数据库路径执行清理
    DeleteScheduler::CleanupDirectory(result.env, sfm, result.db_paths[i].path)
        .PermitUncheckedError();  // 忽略清理错误
  }

  // === SST 文件管理器创建 ===
  // 如果用户没有设置 sst_file_manager，创建一个默认的
  // SstFileManager 的作用：
  //   - 跟踪压缩操作产生/删除的 SST 文件大小
  //   - 在磁盘空间不足时帮助恢复（通过删除一些文件）
  //   - 控制总的 SST 文件大小
  if (result.sst_file_manager.get() == nullptr) {
    std::shared_ptr<SstFileManager> sst_file_manager(
        NewSstFileManager(result.env, result.info_log));
    result.sst_file_manager = sst_file_manager;
  }

  // === WAL 压缩类型支持检查 ===
  // 检查用户配置的 WAL 压缩类型是否受支持
  // 当前 RocksDB 只支持部分压缩类型（如 zstd）
  // 如果配置的压缩类型不支持，则禁用 WAL 压缩
  if (!StreamingCompressionTypeSupported(result.wal_compression)) {
    result.wal_compression = kNoCompression;
    ROCKS_LOG_WARN(result.info_log,
                   "wal_compression is disabled since only zstd is supported");
  }

  // === 偏执检查优化 ===
  // 如果用户没有启用偏执检查（paranoid_checks），则跳过打开时的 SST 文件大小检查
  // 这是一个性能优化：
  //   - paranoid_checks = false: 不进行严格的校验和检查
  //   - 此时不需要验证 SST 文件大小是否与 MANIFEST 中的记录一致
  //   - 跳过检查可以加快数据库打开速度
  if (!result.paranoid_checks) {
    result.skip_checking_sst_file_sizes_on_db_open = true;
    ROCKS_LOG_INFO(result.info_log,
                   "file size check will be skipped during open.");
  }

  // 返回经过清理和修正的 DBOptions
  return result;
}

namespace {
Status ValidateOptionsByTable(
    const DBOptions& db_opts,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto& cf : column_families) {
    s = ValidateOptions(db_opts, cf.options);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}
}  // namespace

Status DBImpl::ValidateOptions(
    const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& column_families) {
  Status s;
  for (auto& cfd : column_families) {
    s = ColumnFamilyData::ValidateOptions(db_options, cfd.options);
    if (!s.ok()) {
      return s;
    }
  }
  s = ValidateOptions(db_options);
  return s;
}

Status DBImpl::ValidateOptions(const DBOptions& db_options) {
  if (db_options.db_paths.size() > 4) {
    return Status::NotSupported(
        "More than four DB paths are not supported yet. ");
  }

  if (db_options.allow_mmap_reads && db_options.use_direct_reads) {
    // Protect against assert in PosixMMapReadableFile constructor
    return Status::NotSupported(
        "If memory mapped reads (allow_mmap_reads) are enabled "
        "then direct I/O reads (use_direct_reads) must be disabled. ");
  }

  if (db_options.allow_mmap_writes &&
      db_options.use_direct_io_for_flush_and_compaction) {
    return Status::NotSupported(
        "If memory mapped writes (allow_mmap_writes) are enabled "
        "then direct I/O writes (use_direct_io_for_flush_and_compaction) must "
        "be disabled. ");
  }

  if (db_options.keep_log_file_num == 0) {
    return Status::InvalidArgument("keep_log_file_num must be greater than 0");
  }

  if (db_options.unordered_write &&
      !db_options.allow_concurrent_memtable_write) {
    return Status::InvalidArgument(
        "unordered_write is incompatible with "
        "!allow_concurrent_memtable_write");
  }

  if (db_options.unordered_write && db_options.enable_pipelined_write) {
    return Status::InvalidArgument(
        "unordered_write is incompatible with enable_pipelined_write");
  }

  if (db_options.atomic_flush && db_options.enable_pipelined_write) {
    return Status::InvalidArgument(
        "atomic_flush is incompatible with enable_pipelined_write");
  }

  // TODO remove this restriction
  if (db_options.atomic_flush && db_options.best_efforts_recovery) {
    return Status::InvalidArgument(
        "atomic_flush is currently incompatible with best-efforts recovery");
  }

  if (db_options.use_direct_io_for_flush_and_compaction &&
      0 == db_options.writable_file_max_buffer_size) {
    return Status::InvalidArgument(
        "writes in direct IO require writable_file_max_buffer_size > 0");
  }

  return Status::OK();
}

Status DBImpl::NewDB(std::vector<std::string>* new_filenames) {
  VersionEdit new_db;
  Status s = SetIdentityFile(env_, dbname_);
  if (!s.ok()) {
    return s;
  }
  if (immutable_db_options_.write_dbid_to_manifest) {
    std::string temp_db_id;
    GetDbIdentityFromIdentityFile(&temp_db_id);
    new_db.SetDBId(temp_db_id);
  }
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  ROCKS_LOG_INFO(immutable_db_options_.info_log, "Creating manifest 1 \n");
  const std::string manifest = DescriptorFileName(dbname_, 1);
  {
    if (fs_->FileExists(manifest, IOOptions(), nullptr).ok()) {
      fs_->DeleteFile(manifest, IOOptions(), nullptr).PermitUncheckedError();
    }
    std::unique_ptr<FSWritableFile> file;
    FileOptions file_options = fs_->OptimizeForManifestWrite(file_options_);
    s = NewWritableFile(fs_.get(), manifest, &file, file_options);
    if (!s.ok()) {
      return s;
    }
    FileTypeSet tmp_set = immutable_db_options_.checksum_handoff_file_types;
    file->SetPreallocationBlockSize(
        immutable_db_options_.manifest_preallocation_size);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(file), manifest, file_options, immutable_db_options_.clock,
        io_tracer_, nullptr /* stats */, immutable_db_options_.listeners,
        nullptr, tmp_set.Contains(FileType::kDescriptorFile),
        tmp_set.Contains(FileType::kDescriptorFile)));
    log::Writer log(std::move(file_writer), 0, false);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
      s = SyncManifest(&immutable_db_options_, log.file());
    }
  }
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(fs_.get(), dbname_, 1, directories_.GetDbDir());
    if (new_filenames) {
      new_filenames->emplace_back(
          manifest.substr(manifest.find_last_of("/\\") + 1));
    }
  } else {
    fs_->DeleteFile(manifest, IOOptions(), nullptr).PermitUncheckedError();
  }
  return s;
}

IOStatus DBImpl::CreateAndNewDirectory(
    FileSystem* fs, const std::string& dirname,
    std::unique_ptr<FSDirectory>* directory) {
  // We call CreateDirIfMissing() as the directory may already exist (if we
  // are reopening a DB), when this happens we don't want creating the
  // directory to cause an error. However, we need to check if creating the
  // directory fails or else we may get an obscure message about the lock
  // file not existing. One real-world example of this occurring is if
  // env->CreateDirIfMissing() doesn't create intermediate directories, e.g.
  // when dbname_ is "dir/db" but when "dir" doesn't exist.
  IOStatus io_s = fs->CreateDirIfMissing(dirname, IOOptions(), nullptr);
  if (!io_s.ok()) {
    return io_s;
  }
  return fs->NewDirectory(dirname, IOOptions(), directory, nullptr);
}

IOStatus Directories::SetDirectories(FileSystem* fs, const std::string& dbname,
                                     const std::string& wal_dir,
                                     const std::vector<DbPath>& data_paths) {
  IOStatus io_s = DBImpl::CreateAndNewDirectory(fs, dbname, &db_dir_);
  if (!io_s.ok()) {
    return io_s;
  }
  if (!wal_dir.empty() && dbname != wal_dir) {
    io_s = DBImpl::CreateAndNewDirectory(fs, wal_dir, &wal_dir_);
    if (!io_s.ok()) {
      return io_s;
    }
  }

  data_dirs_.clear();
  for (auto& p : data_paths) {
    const std::string db_path = p.path;
    if (db_path == dbname) {
      data_dirs_.emplace_back(nullptr);
    } else {
      std::unique_ptr<FSDirectory> path_directory;
      io_s = DBImpl::CreateAndNewDirectory(fs, db_path, &path_directory);
      if (!io_s.ok()) {
        return io_s;
      }
      data_dirs_.emplace_back(path_directory.release());
    }
  }
  assert(data_dirs_.size() == data_paths.size());
  return IOStatus::OK();
}

Status DBImpl::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families, bool read_only,
    bool error_if_wal_file_exists, bool error_if_data_exists_in_wals,
    uint64_t* recovered_seq, RecoveryContext* recovery_ctx) {
  mutex_.AssertHeld();

  bool is_new_db = false;
  assert(db_lock_ == nullptr);
  std::vector<std::string> files_in_dbname;
  if (!read_only) {
    Status s = directories_.SetDirectories(fs_.get(), dbname_,
                                           immutable_db_options_.wal_dir,
                                           immutable_db_options_.db_paths);
    if (!s.ok()) {
      return s;
    }

    s = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!s.ok()) {
      return s;
    }

    std::string current_fname = CurrentFileName(dbname_);
    // Path to any MANIFEST file in the db dir. It does not matter which one.
    // Since best-efforts recovery ignores CURRENT file, existence of a
    // MANIFEST indicates the recovery to recover existing db. If no MANIFEST
    // can be found, a new db will be created.
    std::string manifest_path;
    if (!immutable_db_options_.best_efforts_recovery) {
      s = env_->FileExists(current_fname);
    } else {
      s = Status::NotFound();
      IOOptions io_opts;
      io_opts.do_not_recurse = true;
      Status io_s = immutable_db_options_.fs->GetChildren(
          dbname_, io_opts, &files_in_dbname, /*IODebugContext*=*/nullptr);
      if (!io_s.ok()) {
        s = io_s;
        files_in_dbname.clear();
      }
      for (const std::string& file : files_in_dbname) {
        uint64_t number = 0;
        FileType type = kWalFile;  // initialize
        if (ParseFileName(file, &number, &type) && type == kDescriptorFile) {
          uint64_t bytes;
          s = env_->GetFileSize(DescriptorFileName(dbname_, number), &bytes);
          if (s.ok() && bytes != 0) {
            // Found non-empty MANIFEST (descriptor log), thus best-efforts
            // recovery does not have to treat the db as empty.
            manifest_path = dbname_ + "/" + file;
            break;
          }
        }
      }
    }
    if (s.IsNotFound()) {
      if (immutable_db_options_.create_if_missing) {
        s = NewDB(&files_in_dbname);
        is_new_db = true;
        if (!s.ok()) {
          return s;
        }
      } else {
        return Status::InvalidArgument(
            current_fname, "does not exist (create_if_missing is false)");
      }
    } else if (s.ok()) {
      if (immutable_db_options_.error_if_exists) {
        return Status::InvalidArgument(dbname_,
                                       "exists (error_if_exists is true)");
      }
    } else {
      // Unexpected error reading file
      assert(s.IsIOError());
      return s;
    }
    // Verify compatibility of file_options_ and filesystem
    {
      std::unique_ptr<FSRandomAccessFile> idfile;
      FileOptions customized_fs(file_options_);
      customized_fs.use_direct_reads |=
          immutable_db_options_.use_direct_io_for_flush_and_compaction;
      const std::string& fname =
          manifest_path.empty() ? current_fname : manifest_path;
      s = fs_->NewRandomAccessFile(fname, customized_fs, &idfile, nullptr);
      if (!s.ok()) {
        std::string error_str = s.ToString();
        // Check if unsupported Direct I/O is the root cause
        customized_fs.use_direct_reads = false;
        s = fs_->NewRandomAccessFile(fname, customized_fs, &idfile, nullptr);
        if (s.ok()) {
          return Status::InvalidArgument(
              "Direct I/O is not supported by the specified DB.");
        } else {
          return Status::InvalidArgument(
              "Found options incompatible with filesystem", error_str.c_str());
        }
      }
    }
  } else if (immutable_db_options_.best_efforts_recovery) {
    assert(files_in_dbname.empty());
    IOOptions io_opts;
    io_opts.do_not_recurse = true;
    Status s = immutable_db_options_.fs->GetChildren(
        dbname_, io_opts, &files_in_dbname, /*IODebugContext*=*/nullptr);
    if (s.IsNotFound()) {
      return Status::InvalidArgument(dbname_,
                                     "does not exist (open for read only)");
    } else if (s.IsIOError()) {
      return s;
    }
    assert(s.ok());
  }
  assert(db_id_.empty());
  Status s;
  bool missing_table_file = false;
  if (!immutable_db_options_.best_efforts_recovery) {
    s = versions_->Recover(column_families, read_only, &db_id_);
  } else {
    assert(!files_in_dbname.empty());
    s = versions_->TryRecover(column_families, read_only, files_in_dbname,
                              &db_id_, &missing_table_file);
    if (s.ok()) {
      // TryRecover may delete previous column_family_set_.
      column_family_memtables_.reset(
          new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (s.ok() && !read_only) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      // Try to trivially move files down the LSM tree to start from bottommost
      // level when level_compaction_dynamic_level_bytes is enabled. This should
      // only be useful when user is migrating to turning on this option.
      // If a user is migrating from Level Compaction with a smaller level
      // multiplier or from Universal Compaction, there may be too many
      // non-empty levels and the trivial moves here are not sufficed for
      // migration. Additional compactions are needed to drain unnecessary
      // levels.
      //
      // Note that this step moves files down LSM without consulting
      // SSTPartitioner. Further compactions are still needed if
      // the user wants to partition SST files.
      // Note that files moved in this step may not respect the compression
      // option in target level.
      if (cfd->ioptions()->compaction_style ==
              CompactionStyle::kCompactionStyleLevel &&
          cfd->ioptions()->level_compaction_dynamic_level_bytes &&
          !cfd->GetLatestMutableCFOptions()->disable_auto_compactions) {
        int to_level = cfd->ioptions()->num_levels - 1;
        // last level is reserved
        // allow_ingest_behind does not support Level Compaction,
        // and per_key_placement can have infinite compaction loop for Level
        // Compaction. Adjust to_level here just to be safe.
        if (cfd->ioptions()->allow_ingest_behind ||
            cfd->ioptions()->preclude_last_level_data_seconds > 0) {
          to_level -= 1;
        }
        // Whether this column family has a level trivially moved
        bool moved = false;
        // Fill the LSM starting from to_level and going up one level at a time.
        // Some loop invariants (when last level is not reserved):
        // - levels in (from_level, to_level] are empty, and
        // - levels in (to_level, last_level] are non-empty.
        for (int from_level = to_level; from_level >= 0; --from_level) {
          const std::vector<FileMetaData*>& level_files =
              cfd->current()->storage_info()->LevelFiles(from_level);
          if (level_files.empty() || from_level == 0) {
            continue;
          }
          assert(from_level <= to_level);
          // Trivial move files from `from_level` to `to_level`
          if (from_level < to_level) {
            if (!moved) {
              // lsm_state will look like "[1,2,3,4,5,6,0]" for an LSM with
              // 7 levels
              std::string lsm_state = "[";
              for (int i = 0; i < cfd->ioptions()->num_levels; ++i) {
                lsm_state += std::to_string(
                    cfd->current()->storage_info()->NumLevelFiles(i));
                if (i < cfd->ioptions()->num_levels - 1) {
                  lsm_state += ",";
                }
              }
              lsm_state += "]";
              ROCKS_LOG_WARN(immutable_db_options_.info_log,
                             "[%s] Trivially move files down the LSM when open "
                             "with level_compaction_dynamic_level_bytes=true,"
                             " lsm_state: %s (Files are moved only if DB "
                             "Recovery is successful).",
                             cfd->GetName().c_str(), lsm_state.c_str());
              moved = true;
            }
            ROCKS_LOG_WARN(
                immutable_db_options_.info_log,
                "[%s] Moving %zu files from from_level-%d to from_level-%d",
                cfd->GetName().c_str(), level_files.size(), from_level,
                to_level);
            VersionEdit edit;
            edit.SetColumnFamily(cfd->GetID());
            for (const FileMetaData* f : level_files) {
              edit.DeleteFile(from_level, f->fd.GetNumber());
              edit.AddFile(to_level, f->fd.GetNumber(), f->fd.GetPathId(),
                           f->fd.GetFileSize(), f->smallest, f->largest,
                           f->fd.smallest_seqno, f->fd.largest_seqno,
                           f->marked_for_compaction,
                           f->temperature,  // this can be different from
                                            // `last_level_temperature`
                           f->oldest_blob_file_number, f->oldest_ancester_time,
                           f->file_creation_time, f->epoch_number,
                           f->file_checksum, f->file_checksum_func_name,
                           f->unique_id, f->compensated_range_deletion_size,
                           f->tail_size, f->user_defined_timestamps_persisted);
              ROCKS_LOG_WARN(immutable_db_options_.info_log,
                             "[%s] Moving #%" PRIu64
                             " from from_level-%d to from_level-%d %" PRIu64
                             " bytes\n",
                             cfd->GetName().c_str(), f->fd.GetNumber(),
                             from_level, to_level, f->fd.GetFileSize());
            }
            recovery_ctx->UpdateVersionEdits(cfd, edit);
          }
          --to_level;
        }
      }
    }
  }
  s = SetupDBId(read_only, recovery_ctx);
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "DB ID: %s\n", db_id_.c_str());
  if (s.ok() && !read_only) {
    s = DeleteUnreferencedSstFiles(recovery_ctx);
  }

  if (immutable_db_options_.paranoid_checks && s.ok()) {
    s = CheckConsistency();
  }
  if (s.ok() && !read_only) {
    // TODO: share file descriptors (FSDirectory) with SetDirectories above
    std::map<std::string, std::shared_ptr<FSDirectory>> created_dirs;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      s = cfd->AddDirectories(&created_dirs);
      if (!s.ok()) {
        return s;
      }
    }
  }

  std::vector<std::string> files_in_wal_dir;
  if (s.ok()) {
    // Initial max_total_in_memory_state_ before recovery wals. Log recovery
    // may check this value to decide whether to flush.
    max_total_in_memory_state_ = 0;
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
      max_total_in_memory_state_ += mutable_cf_options->write_buffer_size *
                                    mutable_cf_options->max_write_buffer_number;
    }

    SequenceNumber next_sequence(kMaxSequenceNumber);
    default_cf_handle_ = new ColumnFamilyHandleImpl(
        versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that prev_log_number() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of rocksdb.
    auto wal_dir = immutable_db_options_.GetWalDir();
    if (!immutable_db_options_.best_efforts_recovery) {
      IOOptions io_opts;
      io_opts.do_not_recurse = true;
      s = immutable_db_options_.fs->GetChildren(
          wal_dir, io_opts, &files_in_wal_dir, /*IODebugContext*=*/nullptr);
    }
    if (s.IsNotFound()) {
      return Status::InvalidArgument("wal_dir not found", wal_dir);
    } else if (!s.ok()) {
      return s;
    }

    std::unordered_map<uint64_t, std::string> wal_files;
    for (const auto& file : files_in_wal_dir) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        if (is_new_db) {
          return Status::Corruption(
              "While creating a new Db, wal_dir contains "
              "existing log file: ",
              file);
        } else {
          wal_files[number] = LogFileName(wal_dir, number);
        }
      }
    }

    if (immutable_db_options_.track_and_verify_wals_in_manifest) {
      if (!immutable_db_options_.best_efforts_recovery) {
        // Verify WALs in MANIFEST.
        s = versions_->GetWalSet().CheckWals(env_, wal_files);
      }  // else since best effort recovery does not recover from WALs, no need
         // to check WALs.
    } else if (!versions_->GetWalSet().GetWals().empty()) {
      // Tracking is disabled, clear previously tracked WALs from MANIFEST,
      // otherwise, in the future, if WAL tracking is enabled again,
      // since the WALs deleted when WAL tracking is disabled are not persisted
      // into MANIFEST, WAL check may fail.
      VersionEdit edit;
      WalNumber max_wal_number =
          versions_->GetWalSet().GetWals().rbegin()->first;
      edit.DeleteWalsBefore(max_wal_number + 1);
      assert(recovery_ctx != nullptr);
      assert(versions_->GetColumnFamilySet() != nullptr);
      recovery_ctx->UpdateVersionEdits(
          versions_->GetColumnFamilySet()->GetDefault(), edit);
    }
    if (!s.ok()) {
      return s;
    }

    if (!wal_files.empty()) {
      if (error_if_wal_file_exists) {
        return Status::Corruption(
            "The db was opened in readonly mode with error_if_wal_file_exists"
            "flag but a WAL file already exists");
      } else if (error_if_data_exists_in_wals) {
        for (auto& wal_file : wal_files) {
          uint64_t bytes;
          s = env_->GetFileSize(wal_file.second, &bytes);
          if (s.ok()) {
            if (bytes > 0) {
              return Status::Corruption(
                  "error_if_data_exists_in_wals is set but there are data "
                  " in WAL files.");
            }
          }
        }
      }
    }

    if (!wal_files.empty()) {
      // Recover in the order in which the wals were generated
      std::vector<uint64_t> wals;
      wals.reserve(wal_files.size());
      for (const auto& wal_file : wal_files) {
        wals.push_back(wal_file.first);
      }
      std::sort(wals.begin(), wals.end());

      bool corrupted_wal_found = false;
      s = RecoverLogFiles(wals, &next_sequence, read_only, &corrupted_wal_found,
                          recovery_ctx);
      if (corrupted_wal_found && recovered_seq != nullptr) {
        *recovered_seq = next_sequence;
      }
      if (!s.ok()) {
        // Clear memtables if recovery failed
        for (auto cfd : *versions_->GetColumnFamilySet()) {
          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 kMaxSequenceNumber);
        }
      }
    }
  }

  if (read_only) {
    // If we are opening as read-only, we need to update options_file_number_
    // to reflect the most recent OPTIONS file. It does not matter for regular
    // read-write db instance because options_file_number_ will later be
    // updated to versions_->NewFileNumber() in RenameTempFileToOptionsFile.
    std::vector<std::string> filenames;
    if (s.ok()) {
      const std::string normalized_dbname = NormalizePath(dbname_);
      const std::string normalized_wal_dir =
          NormalizePath(immutable_db_options_.GetWalDir());
      if (immutable_db_options_.best_efforts_recovery) {
        filenames = std::move(files_in_dbname);
      } else if (normalized_dbname == normalized_wal_dir) {
        filenames = std::move(files_in_wal_dir);
      } else {
        IOOptions io_opts;
        io_opts.do_not_recurse = true;
        s = immutable_db_options_.fs->GetChildren(
            GetName(), io_opts, &filenames, /*IODebugContext*=*/nullptr);
      }
    }
    if (s.ok()) {
      uint64_t number = 0;
      uint64_t options_file_number = 0;
      FileType type;
      for (const auto& fname : filenames) {
        if (ParseFileName(fname, &number, &type) && type == kOptionsFile) {
          options_file_number = std::max(number, options_file_number);
        }
      }
      versions_->options_file_number_ = options_file_number;
      uint64_t options_file_size = 0;
      if (options_file_number > 0) {
        s = env_->GetFileSize(OptionsFileName(GetName(), options_file_number),
                              &options_file_size);
      }
      versions_->options_file_size_ = options_file_size;
    }
  }
  return s;
}

Status DBImpl::PersistentStatsProcessFormatVersion() {
  mutex_.AssertHeld();
  Status s;
  // persist version when stats CF doesn't exist
  bool should_persist_format_version = !persistent_stats_cfd_exists_;
  mutex_.Unlock();
  if (persistent_stats_cfd_exists_) {
    // Check persistent stats format version compatibility. Drop and recreate
    // persistent stats CF if format version is incompatible
    uint64_t format_version_recovered = 0;
    Status s_format = DecodePersistentStatsVersionNumber(
        this, StatsVersionKeyType::kFormatVersion, &format_version_recovered);
    uint64_t compatible_version_recovered = 0;
    Status s_compatible = DecodePersistentStatsVersionNumber(
        this, StatsVersionKeyType::kCompatibleVersion,
        &compatible_version_recovered);
    // abort reading from existing stats CF if any of following is true:
    // 1. failed to read format version or compatible version from disk
    // 2. sst's format version is greater than current format version, meaning
    // this sst is encoded with a newer RocksDB release, and current compatible
    // version is below the sst's compatible version
    if (!s_format.ok() || !s_compatible.ok() ||
        (kStatsCFCurrentFormatVersion < format_version_recovered &&
         kStatsCFCompatibleFormatVersion < compatible_version_recovered)) {
      if (!s_format.ok() || !s_compatible.ok()) {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Recreating persistent stats column family since reading "
            "persistent stats version key failed. Format key: %s, compatible "
            "key: %s",
            s_format.ToString().c_str(), s_compatible.ToString().c_str());
      } else {
        ROCKS_LOG_WARN(
            immutable_db_options_.info_log,
            "Recreating persistent stats column family due to corrupted or "
            "incompatible format version. Recovered format: %" PRIu64
            "; recovered format compatible since: %" PRIu64 "\n",
            format_version_recovered, compatible_version_recovered);
      }
      s = DropColumnFamily(persist_stats_cf_handle_);
      if (s.ok()) {
        s = DestroyColumnFamilyHandle(persist_stats_cf_handle_);
      }
      ColumnFamilyHandle* handle = nullptr;
      if (s.ok()) {
        ColumnFamilyOptions cfo;
        OptimizeForPersistentStats(&cfo);
        s = CreateColumnFamily(cfo, kPersistentStatsColumnFamilyName, &handle);
      }
      if (s.ok()) {
        persist_stats_cf_handle_ = static_cast<ColumnFamilyHandleImpl*>(handle);
        // should also persist version here because old stats CF is discarded
        should_persist_format_version = true;
      }
    }
  }
  if (should_persist_format_version) {
    // Persistent stats CF being created for the first time, need to write
    // format version key
    WriteBatch batch;
    if (s.ok()) {
      s = batch.Put(persist_stats_cf_handle_, kFormatVersionKeyString,
                    std::to_string(kStatsCFCurrentFormatVersion));
    }
    if (s.ok()) {
      s = batch.Put(persist_stats_cf_handle_, kCompatibleVersionKeyString,
                    std::to_string(kStatsCFCompatibleFormatVersion));
    }
    if (s.ok()) {
      WriteOptions wo;
      wo.low_pri = true;
      wo.no_slowdown = true;
      wo.sync = false;
      s = Write(wo, &batch);
    }
  }
  mutex_.Lock();
  return s;
}

Status DBImpl::InitPersistStatsColumnFamily() {
  mutex_.AssertHeld();
  assert(!persist_stats_cf_handle_);
  ColumnFamilyData* persistent_stats_cfd =
      versions_->GetColumnFamilySet()->GetColumnFamily(
          kPersistentStatsColumnFamilyName);
  persistent_stats_cfd_exists_ = persistent_stats_cfd != nullptr;

  Status s;
  if (persistent_stats_cfd != nullptr) {
    // We are recovering from a DB which already contains persistent stats CF,
    // the CF is already created in VersionSet::ApplyOneVersionEdit, but
    // column family handle was not. Need to explicitly create handle here.
    persist_stats_cf_handle_ =
        new ColumnFamilyHandleImpl(persistent_stats_cfd, this, &mutex_);
  } else {
    mutex_.Unlock();
    ColumnFamilyHandle* handle = nullptr;
    ColumnFamilyOptions cfo;
    OptimizeForPersistentStats(&cfo);
    s = CreateColumnFamily(cfo, kPersistentStatsColumnFamilyName, &handle);
    persist_stats_cf_handle_ = static_cast<ColumnFamilyHandleImpl*>(handle);
    mutex_.Lock();
  }
  return s;
}

Status DBImpl::LogAndApplyForRecovery(const RecoveryContext& recovery_ctx) {
  mutex_.AssertHeld();
  assert(versions_->descriptor_log_ == nullptr);
  const ReadOptions read_options(Env::IOActivity::kDBOpen);
  Status s = versions_->LogAndApply(
      recovery_ctx.cfds_, recovery_ctx.mutable_cf_opts_, read_options,
      recovery_ctx.edit_lists_, &mutex_, directories_.GetDbDir());
  if (s.ok() && !(recovery_ctx.files_to_delete_.empty())) {
    mutex_.Unlock();
    for (const auto& fname : recovery_ctx.files_to_delete_) {
      s = env_->DeleteFile(fname);
      if (!s.ok()) {
        break;
      }
    }
    mutex_.Lock();
  }
  return s;
}

void DBImpl::InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap() {
  if (immutable_db_options_.wal_filter == nullptr) {
    return;
  }
  assert(immutable_db_options_.wal_filter != nullptr);
  WalFilter& wal_filter = *(immutable_db_options_.wal_filter);

  std::map<std::string, uint32_t> cf_name_id_map;
  std::map<uint32_t, uint64_t> cf_lognumber_map;
  assert(versions_);
  assert(versions_->GetColumnFamilySet());
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    assert(cfd);
    cf_name_id_map.insert(std::make_pair(cfd->GetName(), cfd->GetID()));
    cf_lognumber_map.insert(std::make_pair(cfd->GetID(), cfd->GetLogNumber()));
  }

  wal_filter.ColumnFamilyLogNumberMap(cf_lognumber_map, cf_name_id_map);
}

bool DBImpl::InvokeWalFilterIfNeededOnWalRecord(uint64_t wal_number,
                                                const std::string& wal_fname,
                                                log::Reader::Reporter& reporter,
                                                Status& status,
                                                bool& stop_replay,
                                                WriteBatch& batch) {
  if (immutable_db_options_.wal_filter == nullptr) {
    return true;
  }
  assert(immutable_db_options_.wal_filter != nullptr);
  WalFilter& wal_filter = *(immutable_db_options_.wal_filter);

  WriteBatch new_batch;
  bool batch_changed = false;

  bool process_current_record = true;

  WalFilter::WalProcessingOption wal_processing_option =
      wal_filter.LogRecordFound(wal_number, wal_fname, batch, &new_batch,
                                &batch_changed);

  switch (wal_processing_option) {
    case WalFilter::WalProcessingOption::kContinueProcessing:
      // do nothing, proceeed normally
      break;
    case WalFilter::WalProcessingOption::kIgnoreCurrentRecord:
      // skip current record
      process_current_record = false;
      break;
    case WalFilter::WalProcessingOption::kStopReplay:
      // skip current record and stop replay
      process_current_record = false;
      stop_replay = true;
      break;
    case WalFilter::WalProcessingOption::kCorruptedRecord: {
      status = Status::Corruption("Corruption reported by Wal Filter ",
                                  wal_filter.Name());
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        process_current_record = false;
        reporter.Corruption(batch.GetDataSize(), status);
      }
      break;
    }
    default: {
      // logical error which should not happen. If RocksDB throws, we would
      // just do `throw std::logic_error`.
      assert(false);
      status = Status::NotSupported(
          "Unknown WalProcessingOption returned by Wal Filter ",
          wal_filter.Name());
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        // Ignore the error with current record processing.
        stop_replay = true;
      }
      break;
    }
  }

  if (!process_current_record) {
    return false;
  }

  if (batch_changed) {
    // Make sure that the count in the new batch is
    // within the orignal count.
    int new_count = WriteBatchInternal::Count(&new_batch);
    int original_count = WriteBatchInternal::Count(&batch);
    if (new_count > original_count) {
      ROCKS_LOG_FATAL(
          immutable_db_options_.info_log,
          "Recovering log #%" PRIu64
          " mode %d log filter %s returned "
          "more records (%d) than original (%d) which is not allowed. "
          "Aborting recovery.",
          wal_number, static_cast<int>(immutable_db_options_.wal_recovery_mode),
          wal_filter.Name(), new_count, original_count);
      status = Status::NotSupported(
          "More than original # of records "
          "returned by Wal Filter ",
          wal_filter.Name());
      return false;
    }
    // Set the same sequence number in the new_batch
    // as the original batch.
    WriteBatchInternal::SetSequence(&new_batch,
                                    WriteBatchInternal::Sequence(&batch));
    batch = new_batch;
  }
  return true;
}

// REQUIRES: wal_numbers are sorted in ascending order
Status DBImpl::RecoverLogFiles(const std::vector<uint64_t>& wal_numbers,
                               SequenceNumber* next_sequence, bool read_only,
                               bool* corrupted_wal_found,
                               RecoveryContext* recovery_ctx) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      ROCKS_LOG_WARN(info_log, "%s%s: dropping %d bytes; %s",
                     (status == nullptr ? "(ignoring error) " : ""), fname,
                     static_cast<int>(bytes), s.ToString().c_str());
      if (status != nullptr && status->ok()) {
        *status = s;
      }
    }
  };

  mutex_.AssertHeld();
  Status status;
  std::unordered_map<int, VersionEdit> version_edits;
  // no need to refcount because iteration is under mutex
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    version_edits.insert({cfd->GetID(), edit});
  }
  int job_id = next_job_id_.fetch_add(1);
  {
    auto stream = event_logger_.Log();
    stream << "job" << job_id << "event"
           << "recovery_started";
    stream << "wal_files";
    stream.StartArray();
    for (auto wal_number : wal_numbers) {
      stream << wal_number;
    }
    stream.EndArray();
  }

  // No-op for immutable_db_options_.wal_filter == nullptr.
  InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap();

  bool stop_replay_by_wal_filter = false;
  bool stop_replay_for_corruption = false;
  bool flushed = false;
  uint64_t corrupted_wal_number = kMaxSequenceNumber;
  uint64_t min_wal_number = MinLogNumberToKeep();
  if (!allow_2pc()) {
    // In non-2pc mode, we skip WALs that do not back unflushed data.
    min_wal_number =
        std::max(min_wal_number, versions_->MinLogNumberWithUnflushedData());
  }
  for (auto wal_number : wal_numbers) {
    if (wal_number < min_wal_number) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Skipping log #%" PRIu64
                     " since it is older than min log to keep #%" PRIu64,
                     wal_number, min_wal_number);
      continue;
    }
    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(wal_number);
    // Open the log file
    std::string fname =
        LogFileName(immutable_db_options_.GetWalDir(), wal_number);

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Recovering log #%" PRIu64 " mode %d", wal_number,
                   static_cast<int>(immutable_db_options_.wal_recovery_mode));
    auto logFileDropped = [this, &fname]() {
      uint64_t bytes;
      if (env_->GetFileSize(fname, &bytes).ok()) {
        auto info_log = immutable_db_options_.info_log.get();
        ROCKS_LOG_WARN(info_log, "%s: dropping %d bytes", fname.c_str(),
                       static_cast<int>(bytes));
      }
    };
    if (stop_replay_by_wal_filter) {
      logFileDropped();
      continue;
    }

    std::unique_ptr<SequentialFileReader> file_reader;
    {
      std::unique_ptr<FSSequentialFile> file;
      status = fs_->NewSequentialFile(
          fname, fs_->OptimizeForLogRead(file_options_), &file, nullptr);
      if (!status.ok()) {
        MaybeIgnoreError(&status);
        if (!status.ok()) {
          return status;
        } else {
          // Fail with one log file, but that's ok.
          // Try next one.
          continue;
        }
      }
      file_reader.reset(new SequentialFileReader(
          std::move(file), fname, immutable_db_options_.log_readahead_size,
          io_tracer_));
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = immutable_db_options_.info_log.get();
    reporter.fname = fname.c_str();
    if (!immutable_db_options_.paranoid_checks ||
        immutable_db_options_.wal_recovery_mode ==
            WALRecoveryMode::kSkipAnyCorruptedRecords) {
      reporter.status = nullptr;
    } else {
      reporter.status = &status;
    }
    // We intentially make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(immutable_db_options_.info_log, std::move(file_reader),
                       &reporter, true /*checksum*/, wal_number);

    // Determine if we should tolerate incomplete records at the tail end of the
    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;

    const UnorderedMap<uint32_t, size_t>& running_ts_sz =
        versions_->GetRunningColumnFamiliesTimestampSize();

    TEST_SYNC_POINT_CALLBACK("DBImpl::RecoverLogFiles:BeforeReadWal",
                             /*arg=*/nullptr);
    uint64_t record_checksum;
    while (!stop_replay_by_wal_filter &&
           reader.ReadRecord(&record, &scratch,
                             immutable_db_options_.wal_recovery_mode,
                             &record_checksum) &&
           status.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(record.size(),
                            Status::Corruption("log record too small"));
        continue;
      }

      // We create a new batch and initialize with a valid prot_info_ to store
      // the data checksums
      WriteBatch batch;

      status = WriteBatchInternal::SetContents(&batch, record);
      if (!status.ok()) {
        return status;
      }

      const UnorderedMap<uint32_t, size_t>& record_ts_sz =
          reader.GetRecordedTimestampSize();
      // TODO(yuzhangyu): update mode to kReconcileInconsistency when user
      // comparator can be changed.
      status = HandleWriteBatchTimestampSizeDifference(
          &batch, running_ts_sz, record_ts_sz,
          TimestampSizeConsistencyMode::kVerifyConsistency);
      if (!status.ok()) {
        return status;
      }
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::RecoverLogFiles:BeforeUpdateProtectionInfo:batch", &batch);
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::RecoverLogFiles:BeforeUpdateProtectionInfo:checksum",
          &record_checksum);
      status = WriteBatchInternal::UpdateProtectionInfo(
          &batch, 8 /* bytes_per_key */, &record_checksum);
      if (!status.ok()) {
        return status;
      }

      SequenceNumber sequence = WriteBatchInternal::Sequence(&batch);

      if (immutable_db_options_.wal_recovery_mode ==
          WALRecoveryMode::kPointInTimeRecovery) {
        // In point-in-time recovery mode, if sequence id of log files are
        // consecutive, we continue recovery despite corruption. This could
        // happen when we open and write to a corrupted DB, where sequence id
        // will start from the last sequence id we recovered.
        if (sequence == *next_sequence) {
          stop_replay_for_corruption = false;
        }
        if (stop_replay_for_corruption) {
          logFileDropped();
          break;
        }
      }

      // For the default case of wal_filter == nullptr, always performs no-op
      // and returns true.
      if (!InvokeWalFilterIfNeededOnWalRecord(wal_number, fname, reporter,
                                              status, stop_replay_by_wal_filter,
                                              batch)) {
        continue;
      }

      // If column family was not found, it might mean that the WAL write
      // batch references to the column family that was dropped after the
      // insert. We don't want to fail the whole write batch in that case --
      // we just ignore the update.
      // That's why we set ignore missing column families to true
      bool has_valid_writes = false;
      status = WriteBatchInternal::InsertInto(
          &batch, column_family_memtables_.get(), &flush_scheduler_,
          &trim_history_scheduler_, true, wal_number, this,
          false /* concurrent_memtable_writes */, next_sequence,
          &has_valid_writes, seq_per_batch_, batch_per_txn_);
      MaybeIgnoreError(&status);
      if (!status.ok()) {
        // We are treating this as a failure while reading since we read valid
        // blocks that do not form coherent data
        reporter.Corruption(record.size(), status);
        continue;
      }

      if (has_valid_writes && !read_only) {
        // we can do this because this is called before client has access to the
        // DB and there is only a single thread operating on DB
        ColumnFamilyData* cfd;

        while ((cfd = flush_scheduler_.TakeNextColumnFamily()) != nullptr) {
          cfd->UnrefAndTryDelete();
          // If this asserts, it means that InsertInto failed in
          // filtering updates to already-flushed column families
          assert(cfd->GetLogNumber() <= wal_number);
          auto iter = version_edits.find(cfd->GetID());
          assert(iter != version_edits.end());
          VersionEdit* edit = &iter->second;
          status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit);
          if (!status.ok()) {
            // Reflect errors immediately so that conditions like full
            // file-systems cause the DB::Open() to fail.
            return status;
          }
          flushed = true;

          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 *next_sequence);
        }
      }
    }

    if (!status.ok()) {
      if (status.IsNotSupported()) {
        // We should not treat NotSupported as corruption. It is rather a clear
        // sign that we are processing a WAL that is produced by an incompatible
        // version of the code.
        return status;
      }
      if (immutable_db_options_.wal_recovery_mode ==
          WALRecoveryMode::kSkipAnyCorruptedRecords) {
        // We should ignore all errors unconditionally
        status = Status::OK();
      } else if (immutable_db_options_.wal_recovery_mode ==
                 WALRecoveryMode::kPointInTimeRecovery) {
        if (status.IsIOError()) {
          ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                          "IOError during point-in-time reading log #%" PRIu64
                          " seq #%" PRIu64
                          ". %s. This likely mean loss of synced WAL, "
                          "thus recovery fails.",
                          wal_number, *next_sequence,
                          status.ToString().c_str());
          return status;
        }
        // We should ignore the error but not continue replaying
        status = Status::OK();
        stop_replay_for_corruption = true;
        corrupted_wal_number = wal_number;
        if (corrupted_wal_found != nullptr) {
          *corrupted_wal_found = true;
        }
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "Point in time recovered to log #%" PRIu64
                       " seq #%" PRIu64,
                       wal_number, *next_sequence);
      } else {
        assert(immutable_db_options_.wal_recovery_mode ==
                   WALRecoveryMode::kTolerateCorruptedTailRecords ||
               immutable_db_options_.wal_recovery_mode ==
                   WALRecoveryMode::kAbsoluteConsistency);
        return status;
      }
    }

    flush_scheduler_.Clear();
    trim_history_scheduler_.Clear();
    auto last_sequence = *next_sequence - 1;
    if ((*next_sequence != kMaxSequenceNumber) &&
        (versions_->LastSequence() <= last_sequence)) {
      versions_->SetLastAllocatedSequence(last_sequence);
      versions_->SetLastPublishedSequence(last_sequence);
      versions_->SetLastSequence(last_sequence);
    }
  }
  // Compare the corrupted log number to all columnfamily's current log number.
  // Abort Open() if any column family's log number is greater than
  // the corrupted log number, which means CF contains data beyond the point of
  // corruption. This could during PIT recovery when the WAL is corrupted and
  // some (but not all) CFs are flushed
  // Exclude the PIT case where no log is dropped after the corruption point.
  // This is to cover the case for empty wals after corrupted log, in which we
  // don't reset stop_replay_for_corruption.
  if (stop_replay_for_corruption == true &&
      (immutable_db_options_.wal_recovery_mode ==
           WALRecoveryMode::kPointInTimeRecovery ||
       immutable_db_options_.wal_recovery_mode ==
           WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      // One special case cause cfd->GetLogNumber() > corrupted_wal_number but
      // the CF is still consistent: If a new column family is created during
      // the flush and the WAL sync fails at the same time, the new CF points to
      // the new WAL but the old WAL is curropted. Since the new CF is empty, it
      // is still consistent. We add the check of CF sst file size to avoid the
      // false positive alert.

      // Note that, the check of (cfd->GetLiveSstFilesSize() > 0) may leads to
      // the ignorance of a very rare inconsistency case caused in data
      // canclation. One CF is empty due to KV deletion. But those operations
      // are in the WAL. If the WAL is corrupted, the status of this CF might
      // not be consistent with others. However, the consistency check will be
      // bypassed due to empty CF.
      // TODO: a better and complete implementation is needed to ensure strict
      // consistency check in WAL recovery including hanlding the tailing
      // issues.
      if (cfd->GetLogNumber() > corrupted_wal_number &&
          cfd->GetLiveSstFilesSize() > 0) {
        ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                        "Column family inconsistency: SST file contains data"
                        " beyond the point of corruption.");
        return Status::Corruption("SST file is ahead of WALs in CF " +
                                  cfd->GetName());
      }
    }
  }

  // True if there's any data in the WALs; if not, we can skip re-processing
  // them later
  bool data_seen = false;
  if (!read_only) {
    // no need to refcount since client still doesn't have access
    // to the DB and can not drop column families while we iterate
    const WalNumber max_wal_number = wal_numbers.back();
    for (auto cfd : *versions_->GetColumnFamilySet()) {
      auto iter = version_edits.find(cfd->GetID());
      assert(iter != version_edits.end());
      VersionEdit* edit = &iter->second;

      if (cfd->GetLogNumber() > max_wal_number) {
        // Column family cfd has already flushed the data
        // from all wals. Memtable has to be empty because
        // we filter the updates based on wal_number
        // (in WriteBatch::InsertInto)
        assert(cfd->mem()->GetFirstSequenceNumber() == 0);
        assert(edit->NumEntries() == 0);
        continue;
      }

      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::RecoverLogFiles:BeforeFlushFinalMemtable", /*arg=*/nullptr);

      // flush the final memtable (if non-empty)
      if (cfd->mem()->GetFirstSequenceNumber() != 0) {
        // If flush happened in the middle of recovery (e.g. due to memtable
        // being full), we flush at the end. Otherwise we'll need to record
        // where we were on last flush, which make the logic complicated.
        if (flushed || !immutable_db_options_.avoid_flush_during_recovery) {
          status = WriteLevel0TableForRecovery(job_id, cfd, cfd->mem(), edit);
          if (!status.ok()) {
            // Recovery failed
            break;
          }
          flushed = true;

          cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                                 versions_->LastSequence());
        }
        data_seen = true;
      }

      // Update the log number info in the version edit corresponding to this
      // column family. Note that the version edits will be written to MANIFEST
      // together later.
      // writing wal_number in the manifest means that any log file
      // with number strongly less than (wal_number + 1) is already
      // recovered and should be ignored on next reincarnation.
      // Since we already recovered max_wal_number, we want all wals
      // with numbers `<= max_wal_number` (includes this one) to be ignored
      if (flushed || cfd->mem()->GetFirstSequenceNumber() == 0) {
        edit->SetLogNumber(max_wal_number + 1);
      }
    }
    if (status.ok()) {
      // we must mark the next log number as used, even though it's
      // not actually used. that is because VersionSet assumes
      // VersionSet::next_file_number_ always to be strictly greater than any
      // log number
      versions_->MarkFileNumberUsed(max_wal_number + 1);
      assert(recovery_ctx != nullptr);

      for (auto* cfd : *versions_->GetColumnFamilySet()) {
        auto iter = version_edits.find(cfd->GetID());
        assert(iter != version_edits.end());
        recovery_ctx->UpdateVersionEdits(cfd, iter->second);
      }

      if (flushed || !data_seen) {
        VersionEdit wal_deletion;
        if (immutable_db_options_.track_and_verify_wals_in_manifest) {
          wal_deletion.DeleteWalsBefore(max_wal_number + 1);
        }
        if (!allow_2pc()) {
          // In non-2pc mode, flushing the memtables of the column families
          // means we can advance min_log_number_to_keep.
          wal_deletion.SetMinLogNumberToKeep(max_wal_number + 1);
        }
        assert(versions_->GetColumnFamilySet() != nullptr);
        recovery_ctx->UpdateVersionEdits(
            versions_->GetColumnFamilySet()->GetDefault(), wal_deletion);
      }
    }
  }

  if (status.ok()) {
    if (data_seen && !flushed) {
      status = RestoreAliveLogFiles(wal_numbers);
    } else if (!wal_numbers.empty()) {  // If there's no data in the WAL, or we
                                        // flushed all the data, still
      // truncate the log file. If the process goes into a crash loop before
      // the file is deleted, the preallocated space will never get freed.
      const bool truncate = !read_only;
      GetLogSizeAndMaybeTruncate(wal_numbers.back(), truncate, nullptr)
          .PermitUncheckedError();
    }
  }

  event_logger_.Log() << "job" << job_id << "event"
                      << "recovery_finished";

  return status;
}

Status DBImpl::GetLogSizeAndMaybeTruncate(uint64_t wal_number, bool truncate,
                                          LogFileNumberSize* log_ptr) {
  LogFileNumberSize log(wal_number);
  std::string fname =
      LogFileName(immutable_db_options_.GetWalDir(), wal_number);
  Status s;
  // This gets the appear size of the wals, not including preallocated space.
  s = env_->GetFileSize(fname, &log.size);
  TEST_SYNC_POINT_CALLBACK("DBImpl::GetLogSizeAndMaybeTruncate:0", /*arg=*/&s);
  if (s.ok() && truncate) {
    std::unique_ptr<FSWritableFile> last_log;
    Status truncate_status = fs_->ReopenWritableFile(
        fname,
        fs_->OptimizeForLogWrite(
            file_options_,
            BuildDBOptions(immutable_db_options_, mutable_db_options_)),
        &last_log, nullptr);
    if (truncate_status.ok()) {
      truncate_status = last_log->Truncate(log.size, IOOptions(), nullptr);
    }
    if (truncate_status.ok()) {
      truncate_status = last_log->Close(IOOptions(), nullptr);
    }
    // Not a critical error if fail to truncate.
    if (!truncate_status.ok() && !truncate_status.IsNotSupported()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Failed to truncate log #%" PRIu64 ": %s", wal_number,
                     truncate_status.ToString().c_str());
    }
  }
  if (log_ptr) {
    *log_ptr = log;
  }
  return s;
}

Status DBImpl::RestoreAliveLogFiles(const std::vector<uint64_t>& wal_numbers) {
  if (wal_numbers.empty()) {
    return Status::OK();
  }
  Status s;
  mutex_.AssertHeld();
  assert(immutable_db_options_.avoid_flush_during_recovery);
  // Mark these as alive so they'll be considered for deletion later by
  // FindObsoleteFiles()
  total_log_size_ = 0;
  log_empty_ = false;
  uint64_t min_wal_with_unflushed_data =
      versions_->MinLogNumberWithUnflushedData();
  for (auto wal_number : wal_numbers) {
    if (!allow_2pc() && wal_number < min_wal_with_unflushed_data) {
      // In non-2pc mode, the WAL files not backing unflushed data are not
      // alive, thus should not be added to the alive_log_files_.
      continue;
    }
    // We preallocate space for wals, but then after a crash and restart, those
    // preallocated space are not needed anymore. It is likely only the last
    // log has such preallocated space, so we only truncate for the last log.
    LogFileNumberSize log;
    s = GetLogSizeAndMaybeTruncate(
        wal_number, /*truncate=*/(wal_number == wal_numbers.back()), &log);
    if (!s.ok()) {
      break;
    }
    total_log_size_ += log.size;
    alive_log_files_.push_back(log);
  }
  return s;
}

Status DBImpl::WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd,
                                           MemTable* mem, VersionEdit* edit) {
  mutex_.AssertHeld();
  assert(cfd);
  assert(cfd->imm());
  // The immutable memtable list must be empty.
  assert(std::numeric_limits<uint64_t>::max() ==
         cfd->imm()->GetEarliestMemTableID());

  const uint64_t start_micros = immutable_db_options_.clock->NowMicros();

  FileMetaData meta;
  std::vector<BlobFileAddition> blob_file_additions;

  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));
  meta.fd = FileDescriptor(versions_->NewFileNumber(), 0, 0);
  ReadOptions ro;
  ro.total_order_seek = true;
  ro.io_activity = Env::IOActivity::kDBOpen;
  Arena arena;
  Status s;
  TableProperties table_properties;
  {
    ScopedArenaIterator iter(mem->NewIterator(ro, &arena));
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] [WriteLevel0TableForRecovery]"
                    " Level-0 table #%" PRIu64 ": started",
                    cfd->GetName().c_str(), meta.fd.GetNumber());

    // Get the latest mutable cf options while the mutex is still locked
    const MutableCFOptions mutable_cf_options =
        *cfd->GetLatestMutableCFOptions();
    bool paranoid_file_checks =
        cfd->GetLatestMutableCFOptions()->paranoid_file_checks;

    int64_t _current_time = 0;
    immutable_db_options_.clock->GetCurrentTime(&_current_time)
        .PermitUncheckedError();  // ignore error
    const uint64_t current_time = static_cast<uint64_t>(_current_time);
    meta.oldest_ancester_time = current_time;
    meta.epoch_number = cfd->NewEpochNumber();
    {
      auto write_hint = cfd->CalculateSSTWriteHint(0);
      mutex_.Unlock();

      SequenceNumber earliest_write_conflict_snapshot;
      std::vector<SequenceNumber> snapshot_seqs =
          snapshots_.GetAll(&earliest_write_conflict_snapshot);
      auto snapshot_checker = snapshot_checker_.get();
      if (use_custom_gc_ && snapshot_checker == nullptr) {
        snapshot_checker = DisableGCSnapshotChecker::Instance();
      }
      std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
          range_del_iters;
      auto range_del_iter =
          // This is called during recovery, where a live memtable is flushed
          // directly. In this case, no fragmented tombstone list is cached in
          // this memtable yet.
          mem->NewRangeTombstoneIterator(ro, kMaxSequenceNumber,
                                         false /* immutable_memtable */);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }

      IOStatus io_s;
      TableBuilderOptions tboptions(
          *cfd->ioptions(), mutable_cf_options, cfd->internal_comparator(),
          cfd->int_tbl_prop_collector_factories(),
          GetCompressionFlush(*cfd->ioptions(), mutable_cf_options),
          mutable_cf_options.compression_opts, cfd->GetID(), cfd->GetName(),
          0 /* level */, false /* is_bottommost */,
          TableFileCreationReason::kRecovery, 0 /* oldest_key_time */,
          0 /* file_creation_time */, db_id_, db_session_id_,
          0 /* target_file_size */, meta.fd.GetNumber());
      SeqnoToTimeMapping empty_seqno_time_mapping;
      Version* version = cfd->current();
      version->Ref();
      const ReadOptions read_option(Env::IOActivity::kDBOpen);
      uint64_t num_input_entries = 0;
      s = BuildTable(
          dbname_, versions_.get(), immutable_db_options_, tboptions,
          file_options_for_compaction_, read_option, cfd->table_cache(),
          iter.get(), std::move(range_del_iters), &meta, &blob_file_additions,
          snapshot_seqs, earliest_write_conflict_snapshot, kMaxSequenceNumber,
          snapshot_checker, paranoid_file_checks, cfd->internal_stats(), &io_s,
          io_tracer_, BlobFileCreationReason::kRecovery,
          empty_seqno_time_mapping, &event_logger_, job_id, Env::IO_HIGH,
          nullptr /* table_properties */, write_hint,
          nullptr /*full_history_ts_low*/, &blob_callback_, version,
          &num_input_entries);
      version->Unref();
      LogFlush(immutable_db_options_.info_log);
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] [WriteLevel0TableForRecovery]"
                      " Level-0 table #%" PRIu64 ": %" PRIu64 " bytes %s",
                      cfd->GetName().c_str(), meta.fd.GetNumber(),
                      meta.fd.GetFileSize(), s.ToString().c_str());
      mutex_.Lock();

      // TODO(AR) is this ok?
      if (!io_s.ok() && s.ok()) {
        s = io_s;
      }

      uint64_t total_num_entries = mem->num_entries();
      if (s.ok() && total_num_entries != num_input_entries) {
        std::string msg = "Expected " + std::to_string(total_num_entries) +
                          " entries in memtable, but read " +
                          std::to_string(num_input_entries);
        ROCKS_LOG_WARN(immutable_db_options_.info_log,
                       "[%s] [JOB %d] Level-0 flush during recover: %s",
                       cfd->GetName().c_str(), job_id, msg.c_str());
        if (immutable_db_options_.flush_verify_memtable_count) {
          s = Status::Corruption(msg);
        }
      }
    }
  }
  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  const bool has_output = meta.fd.GetFileSize() > 0;

  constexpr int level = 0;

  if (s.ok() && has_output) {
    edit->AddFile(level, meta.fd.GetNumber(), meta.fd.GetPathId(),
                  meta.fd.GetFileSize(), meta.smallest, meta.largest,
                  meta.fd.smallest_seqno, meta.fd.largest_seqno,
                  meta.marked_for_compaction, meta.temperature,
                  meta.oldest_blob_file_number, meta.oldest_ancester_time,
                  meta.file_creation_time, meta.epoch_number,
                  meta.file_checksum, meta.file_checksum_func_name,
                  meta.unique_id, meta.compensated_range_deletion_size,
                  meta.tail_size, meta.user_defined_timestamps_persisted);

    for (const auto& blob : blob_file_additions) {
      edit->AddBlobFile(blob);
    }
  }

  InternalStats::CompactionStats stats(CompactionReason::kFlush, 1);
  stats.micros = immutable_db_options_.clock->NowMicros() - start_micros;

  if (has_output) {
    stats.bytes_written = meta.fd.GetFileSize();
    stats.num_output_files = 1;
  }

  const auto& blobs = edit->GetBlobFileAdditions();
  for (const auto& blob : blobs) {
    stats.bytes_written_blob += blob.GetTotalBlobBytes();
  }

  stats.num_output_files_blob = static_cast<int>(blobs.size());

  cfd->internal_stats()->AddCompactionStats(level, Env::Priority::USER, stats);
  cfd->internal_stats()->AddCFStats(
      InternalStats::BYTES_FLUSHED,
      stats.bytes_written + stats.bytes_written_blob);
  RecordTick(stats_, COMPACT_WRITE_BYTES, meta.fd.GetFileSize());
  return s;
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  if (db_options.persist_stats_to_disk) {
    column_families.push_back(
        ColumnFamilyDescriptor(kPersistentStatsColumnFamilyName, cf_options));
  }
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    if (db_options.persist_stats_to_disk) {
      assert(handles.size() == 2);
    } else {
      assert(handles.size() == 1);
    }
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    if (db_options.persist_stats_to_disk && handles[1] != nullptr) {
      delete handles[1];
    }
    delete handles[0];
  }
  return s;
}

Status DB::Open(const DBOptions& db_options, const std::string& dbname,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                std::vector<ColumnFamilyHandle*>* handles, DB** dbptr) {
  const bool kSeqPerBatch = true;
  const bool kBatchPerTxn = true;
  ThreadStatusUtil::SetEnableTracking(db_options.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_DBOPEN);
  Status s = DBImpl::Open(db_options, dbname, column_families, handles, dbptr,
                          !kSeqPerBatch, kBatchPerTxn);
  ThreadStatusUtil::ResetThreadStatus();
  return s;
}

// TODO: Implement the trimming in flush code path.
// TODO: Perform trimming before inserting into memtable during recovery.
// TODO: Pick files with max_timestamp > trim_ts by each file's timestamp meta
// info, and handle only these files to reduce io.
Status DB::OpenAndTrimHistory(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    std::string trim_ts) {
  assert(dbptr != nullptr);
  assert(handles != nullptr);
  auto validate_options = [&db_options] {
    if (db_options.avoid_flush_during_recovery) {
      return Status::InvalidArgument(
          "avoid_flush_during_recovery incompatible with "
          "OpenAndTrimHistory");
    }
    return Status::OK();
  };
  auto s = validate_options();
  if (!s.ok()) {
    return s;
  }

  DB* db = nullptr;
  s = DB::Open(db_options, dbname, column_families, handles, &db);
  if (!s.ok()) {
    return s;
  }
  assert(db);
  CompactRangeOptions options;
  options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  auto db_impl = static_cast_with_check<DBImpl>(db);
  for (auto handle : *handles) {
    assert(handle != nullptr);
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handle);
    auto cfd = cfh->cfd();
    assert(cfd != nullptr);
    // Only compact column families with timestamp enabled
    if (cfd->user_comparator() != nullptr &&
        cfd->user_comparator()->timestamp_size() > 0) {
      s = db_impl->CompactRangeInternal(options, handle, nullptr, nullptr,
                                        trim_ts);
      if (!s.ok()) {
        break;
      }
    }
  }
  auto clean_op = [&handles, &db] {
    for (auto handle : *handles) {
      auto temp_s = db->DestroyColumnFamilyHandle(handle);
      assert(temp_s.ok());
    }
    handles->clear();
    delete db;
  };
  if (!s.ok()) {
    clean_op();
    return s;
  }

  *dbptr = db;
  return s;
}

IOStatus DBImpl::CreateWAL(uint64_t log_file_num, uint64_t recycle_log_number,
                           size_t preallocate_block_size,
                           log::Writer** new_log) {
  IOStatus io_s;
  std::unique_ptr<FSWritableFile> lfile;

  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  FileOptions opt_file_options =
      fs_->OptimizeForLogWrite(file_options_, db_options);
  std::string wal_dir = immutable_db_options_.GetWalDir();
  std::string log_fname = LogFileName(wal_dir, log_file_num);

  if (recycle_log_number) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "reusing log %" PRIu64 " from recycle list\n",
                   recycle_log_number);
    std::string old_log_fname = LogFileName(wal_dir, recycle_log_number);
    TEST_SYNC_POINT("DBImpl::CreateWAL:BeforeReuseWritableFile1");
    TEST_SYNC_POINT("DBImpl::CreateWAL:BeforeReuseWritableFile2");
    io_s = fs_->ReuseWritableFile(log_fname, old_log_fname, opt_file_options,
                                  &lfile, /*dbg=*/nullptr);
  } else {
    io_s = NewWritableFile(fs_.get(), log_fname, &lfile, opt_file_options);
  }

  if (io_s.ok()) {
    lfile->SetWriteLifeTimeHint(CalculateWALWriteHint());
    lfile->SetPreallocationBlockSize(preallocate_block_size);

    const auto& listeners = immutable_db_options_.listeners;
    FileTypeSet tmp_set = immutable_db_options_.checksum_handoff_file_types;
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(lfile), log_fname, opt_file_options,
        immutable_db_options_.clock, io_tracer_, nullptr /* stats */, listeners,
        nullptr, tmp_set.Contains(FileType::kWalFile),
        tmp_set.Contains(FileType::kWalFile)));
    *new_log = new log::Writer(std::move(file_writer), log_file_num,
                               immutable_db_options_.recycle_log_file_num > 0,
                               immutable_db_options_.manual_wal_flush,
                               immutable_db_options_.wal_compression);
    io_s = (*new_log)->AddCompressionTypeRecord();
  }
  return io_s;
}

// ============================================================================
// 函数名: DBImpl::Open
// 功能描述: 打开一个 RocksDB 数据库实例
//
// 参数说明:
//   - db_options: 数据库级别的选项配置
//     包括环境、缓存、线程池、WAL 配置等
//   - dbname: 数据库的路径/名称
//   - column_families: 要打开的列族描述列表
//     包含列族名称和列族选项
//   - handles: [输出参数] 返回的列族句柄列表
//     每个句柄对应一个打开的列族，用于后续操作
//   - dbptr: [输出参数] 返回创建的数据库实例指针
//   - seq_per_batch: 是否每个批分配一个序列号
//     true: WriteBatch 的每个操作有独立的序列号
//     false: WriteBatch 的所有操作共享一个序列号（默认）
//   - batch_per_txn: 是否每个事务对应一个批
//     与事务机制相关
//
// 返回值:
//   - Status::OK(): 数据库成功打开
//   - Status::Error(): 打开失败（如选项无效、文件系统错误、恢复失败等）
//
// 主要功能流程:
//   1. 选项验证: 验证数据库和列族选项的有效性
//   2. 数据库实例创建: 创建 DBImpl 对象
//   3. 日志系统初始化: 创建或使用用户提供的日志记录器
//   4. 目录创建: 创建 WAL 目录、数据库目录、列族目录等
//   5. 数据恢复: 从 MANIFEST 和 WAL 文件恢复数据库状态
//   6. WAL 初始化: 创建新的 WAL 文件用于后续写入
//   7. 列族初始化: 创建或加载列族
//   8. SuperVersion 安装: 安装 SuperVersion 用于读写操作
//   9. 选项持久化: 将选项写入 OPTIONS 文件
//   10. 后台任务启动: 启动压缩、刷新等后台任务
//   11. SST 文件管理: 通知 SST 文件管理器已存在的文件
//
// 恢复机制:
//   - 读取 MANIFEST 文件获取数据库元数据
//   - 从 WAL 文件恢复未持久化的写入
//   - 支持多种 WAL 恢复模式（绝对一致性、时间点恢复等）
//   - 处理日志文件损坏和数据丢失情况
//
// 使用场景:
//   - 首次打开新创建的数据库
//   - 打开已存在的数据库（从持久化状态恢复）
//   - 带有多个列族的数据库打开
//   - 读写模式或只读模式的数据库打开
//
// 注意事项:
//   - 此函数是同步的，阻塞直到数据库完全打开
//   - 打开失败会清理所有已分配的资源
//   - 支持在打开后立即进行写入和查询
//   - 错误处理包括资源清理和状态回滚
// ============================================================================
Status DBImpl::Open(const DBOptions& db_options, const std::string& dbname,
                    const std::vector<ColumnFamilyDescriptor>& column_families,
                    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
                    const bool seq_per_batch, const bool batch_per_txn) {
  // === 选项验证 ===
  // 验证表（SST 表格式）相关的选项是否有效
  Status s = ValidateOptionsByTable(db_options, column_families);
  if (!s.ok()) {
    return s;
  }

  // 验证所有数据库和列族选项的有效性
  s = ValidateOptions(db_options, column_families);
  if (!s.ok()) {
    return s;
  }

  // === 初始化输出参数 ===
  *dbptr = nullptr;
  assert(handles);
  handles->clear();

  // === 计算最大写缓冲区大小 ===
  // 用于 WAL 预分配大小的计算
  // 所有列族的 write_buffer_size 中的最大值
  size_t max_write_buffer_size = 0;
  for (auto cf : column_families) {
    max_write_buffer_size =
        std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

  // === 创建数据库实例 ===
  // DBImpl 是 RocksDB 的核心实现类
  DBImpl* impl = new DBImpl(db_options, dbname, seq_per_batch, batch_per_txn);

  // === 验证日志记录器是否创建成功 ===
  // 日志记录器在 DBImpl 构造函数中通过 CreateLoggerFromOptions 创建
  // 如果创建失败，清理资源并返回错误
  if (!impl->immutable_db_options_.info_log) {
    s = impl->init_logger_creation_s_;
    delete impl;
    return s;
  } else {
    assert(impl->init_logger_creation_s_.ok());
  }

  // === 创建 WAL 目录 ===
  // 确保日志文件的存储目录存在
  s = impl->env_->CreateDirIfMissing(impl->immutable_db_options_.GetWalDir());
  if (s.ok()) {
    // === 创建所有需要的目录 ===
    std::vector<std::string> paths;
    // 添加数据库主路径
    for (auto& db_path : impl->immutable_db_options_.db_paths) {
      paths.emplace_back(db_path.path);
    }
    // 添加列族的存储路径
    for (auto& cf : column_families) {
      for (auto& cf_path : cf.options.cf_paths) {
        paths.emplace_back(cf_path.path);
      }
    }
    // 逐个创建目录
    for (auto& path : paths) {
      s = impl->env_->CreateDirIfMissing(path);
      if (!s.ok()) {
        break;
      }
    }

    // === 启用从磁盘空间不足错误自动恢复 ===
    // 只能处理数据库存储在单一路径的情况
    // 原因：多路径时，无法确定哪个路径空间不足，恢复策略复杂
    if (paths.size() <= 1) {
      impl->error_handler_.EnableAutoRecovery();
    }
  }

  // === 创建归档目录 ===
  // 用于存储被删除但未立即清理的 SST 文件
  if (s.ok()) {
    s = impl->CreateArchivalDirectory();
  }
  if (!s.ok()) {
    delete impl;
    return s;
  }

  // === 确定 WAL 是否在数据库路径中 ===
  // 这会影响垃圾文件清理和 WAL 管理策略
  impl->wal_in_db_path_ = impl->immutable_db_options_.IsWalDirSameAsDBPath();

  // === 准备恢复上下文 ===
  RecoveryContext recovery_ctx;

  // === 获取主锁 ===
  // 恢复过程需要独占访问数据库状态
  impl->mutex_.Lock();

  // === 数据库恢复 ===
  // 从 MANIFEST 和 WAL 文件恢复数据库状态
  // 处理 create_if_missing 和 error_if_exists 选项
  uint64_t recovered_seq(kMaxSequenceNumber);
  s = impl->Recover(column_families, false /* read_only */,
                    false /* error_if_wal_file_exists */,
                    false /* error_if_data_exists_in_wals */, &recovered_seq,
                    &recovery_ctx);

  // === 创建新的 WAL 文件 ===
  // 恢复成功后，创建新的 WAL 文件用于后续写入
  if (s.ok()) {
    // 分配新的 WAL 文件编号
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    log::Writer* new_log = nullptr;

    // 计算 WAL 预分配块大小
    // 预分配可以提高写入性能，减少文件系统分配开销
    const size_t preallocate_block_size =
        impl->GetWalPreallocateBlockSize(max_write_buffer_size);

    // 创建新的 WAL 文件
    s = impl->CreateWAL(new_log_number, 0 /*recycle_log_number*/,
                        preallocate_block_size, &new_log);

    if (s.ok()) {
      // 在日志写锁保护下更新日志文件信息
      InstrumentedMutexLock wl(&impl->log_write_mutex_);
      impl->logfile_number_ = new_log_number;
      assert(new_log != nullptr);
      assert(impl->logs_.empty());
      impl->logs_.emplace_back(new_log_number, new_log);
    }

    // === 写入虚拟 WAL 记录 ===
    // 在 WritePrepared 模式下，序列号可能不连续
    // 这会破坏 kPointInTimeRecovery 的假设：损坏日志后的第一个日志的序列号
    // 应比从 WAL 读取的最后序列号大 1
    // 为了使这个技巧仍然有效，我们在恢复后的第一个日志中写入一个虚拟记录
    // 在非 WritePrepared 模式下，新日志也可能是空的
    // 缺少连续序列号提示来区分日志中间损坏和恢复后残留的损坏日志
    // 这个情况也会通过虚拟写入解决
    if (s.ok()) {
      // 记录活跃的日志文件
      impl->alive_log_files_.push_back(
          DBImpl::LogFileNumberSize(impl->logfile_number_));

      // 只有在成功恢复到某个序列号时才写入虚拟记录
      if (recovered_seq != kMaxSequenceNumber) {
        WriteBatch empty_batch;
        // 设置虚拟批次的序列号为恢复的序列号
        WriteBatchInternal::SetSequence(&empty_batch, recovered_seq);
        WriteOptions write_options;
        uint64_t log_used, log_size;
        log::Writer* log_writer = impl->logs_.back().writer;
        LogFileNumberSize& log_file_number_size = impl->alive_log_files_.back();

        assert(log_writer->get_log_number() == log_file_number_size.number);
        impl->mutex_.AssertHeld();

        // 将空批次写入 WAL
        s = impl->WriteToWAL(empty_batch, log_writer, &log_used, &log_size,
                             Env::IO_TOTAL, log_file_number_size);
        if (s.ok()) {
          // 需要同步，否则断电后可能丢失
          s = impl->FlushWAL(false);
          TEST_SYNC_POINT_CALLBACK("DBImpl::Open::BeforeSyncWAL", /*arg=*/&s);
          if (s.ok()) {
            // 根据 use_fsync 选项选择 fsync 或 fdatasync
            s = log_writer->file()->Sync(impl->immutable_db_options_.use_fsync);
          }
        }
      }
    }
  }

  // === 应用恢复的版本编辑 ===
  // 将恢复过程中收集的版本编辑应用到 VersionSet
  if (s.ok()) {
    s = impl->LogAndApplyForRecovery(recovery_ctx);
  }

  // === 初始化持久化统计列族 ===
  // 如果启用了统计信息持久化，创建统计列族
  if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
    impl->mutex_.AssertHeld();
    s = impl->InitPersistStatsColumnFamily();
  }

  // === 创建列族句柄 ===
  // 为请求的每个列族创建句柄，供应用程序使用
  if (s.ok()) {
    // set column family handles
    for (auto cf : column_families) {
      // 从 VersionSet 获取列族
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (cfd != nullptr) {
        // 列族已存在，创建句柄
        handles->push_back(
            new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
        impl->NewThreadStatusCfInfo(cfd);
      } else {
        // 列族不存在
        if (db_options.create_missing_column_families) {
          // missing column family, create it
          // 允许自动创建缺失的列族
          ColumnFamilyHandle* handle = nullptr;
          // === 释放主锁 ===
  impl->mutex_.Unlock();  // 创建列族需要释放锁
          s = impl->CreateColumnFamily(cf.options, cf.name, &handle);
          impl->mutex_.Lock();  // 重新获取锁
          if (s.ok()) {
            handles->push_back(handle);
          } else {
            break;
          }
        } else {
          // 不允许自动创建，返回错误
          s = Status::InvalidArgument("Column family not found", cf.name);
          break;
        }
      }
    }
  }

  // === 安装 SuperVersion ===
  // SuperVersion 是读写操作的核心数据结构
  // 包含当前版本的 MemTable、Immutable MemTable 和 SST 文件列表
  if (s.ok()) {
    SuperVersionContext sv_context(/* create_superversion */ true);
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      impl->InstallSuperVersionAndScheduleWork(
          cfd, &sv_context, *cfd->GetLatestMutableCFOptions());
    }
    sv_context.Clean();
  }

  // === 处理持久化统计的格式版本 ===
  if (s.ok() && impl->immutable_db_options_.persist_stats_to_disk) {
    // try to read format version
    s = impl->PersistentStatsProcessFormatVersion();
  }

  // === 检查 MemTable 功能兼容性 ===
  // 验证 MemTable 是否支持快照和合并操作
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      // 检查快照支持
      if (!cfd->mem()->IsSnapshotSupported()) {
        impl->is_snapshot_supported_ = false;
      }
      // 检查合并操作支持
      if (cfd->ioptions()->merge_operator != nullptr &&
          !cfd->mem()->IsMergeOperatorSupported()) {
        s = Status::InvalidArgument(
            "The memtable of column family %s does not support merge operator "
            "its options.merge_operator is non-null",
            cfd->GetName().c_str());
      }
      if (!s.ok()) {
        break;
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::Open:Opened");
  Status persist_options_status;
  if (s.ok()) {
    // Persist RocksDB Options before scheduling the compaction.
    // The WriteOptionsFile() will release and lock the mutex internally.
    persist_options_status = impl->WriteOptionsFile(
        false /*need_mutex_lock*/, false /*need_enter_write_thread*/);

    *dbptr = impl;
    impl->opened_successfully_ = true;
    impl->DeleteObsoleteFiles();
    TEST_SYNC_POINT("DBImpl::Open:AfterDeleteFiles");
    impl->MaybeScheduleFlushOrCompaction();
  } else {
    persist_options_status.PermitUncheckedError();
  }
  // === 释放主锁 ===
  impl->mutex_.Unlock();

  auto sfm = static_cast<SstFileManagerImpl*>(
      impl->immutable_db_options_.sst_file_manager.get());
  if (s.ok() && sfm) {
    // Set Statistics ptr for SstFileManager to dump the stats of
    // DeleteScheduler.
    sfm->SetStatisticsPtr(impl->immutable_db_options_.statistics);
    ROCKS_LOG_INFO(impl->immutable_db_options_.info_log,
                   "SstFileManager instance %p", sfm);

    // Notify SstFileManager about all sst files that already exist in
    // db_paths[0] and cf_paths[0] when the DB is opened.

    // SstFileManagerImpl needs to know sizes of the files. For files whose size
    // we already know (sst files that appear in manifest - typically that's the
    // vast majority of all files), we'll pass the size to SstFileManager.
    // For all other files SstFileManager will query the size from filesystem.

    std::vector<ColumnFamilyMetaData> metadata;
    impl->GetAllColumnFamilyMetaData(&metadata);

    std::unordered_map<std::string, uint64_t> known_file_sizes;
    for (const auto& md : metadata) {
      for (const auto& lmd : md.levels) {
        for (const auto& fmd : lmd.files) {
          known_file_sizes[fmd.relative_filename] = fmd.size;
        }
      }
      for (const auto& bmd : md.blob_files) {
        std::string name = bmd.blob_file_name;
        // The BlobMetaData.blob_file_name may start with "/".
        if (!name.empty() && name[0] == '/') {
          name = name.substr(1);
        }
        known_file_sizes[name] = bmd.blob_file_size;
      }
    }

    std::vector<std::string> paths;
    paths.emplace_back(impl->immutable_db_options_.db_paths[0].path);
    for (auto& cf : column_families) {
      if (!cf.options.cf_paths.empty()) {
        paths.emplace_back(cf.options.cf_paths[0].path);
      }
    }
    // Remove duplicate paths.
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
    IOOptions io_opts;
    io_opts.do_not_recurse = true;
    for (auto& path : paths) {
      std::vector<std::string> existing_files;
      impl->immutable_db_options_.fs
          ->GetChildren(path, io_opts, &existing_files,
                        /*IODebugContext*=*/nullptr)
          .PermitUncheckedError();  //**TODO: What do to on error?
      for (auto& file_name : existing_files) {
        uint64_t file_number;
        FileType file_type;
        std::string file_path = path + "/" + file_name;
        if (ParseFileName(file_name, &file_number, &file_type) &&
            (file_type == kTableFile || file_type == kBlobFile)) {
          // TODO: Check for errors from OnAddFile?
          if (known_file_sizes.count(file_name)) {
            // We're assuming that each sst file name exists in at most one of
            // the paths.
            sfm->OnAddFile(file_path, known_file_sizes.at(file_name))
                .PermitUncheckedError();
          } else {
            sfm->OnAddFile(file_path).PermitUncheckedError();
          }
        }
      }
    }

    // === 预留磁盘缓冲区空间 ===
    // 这是一个启发式策略：当磁盘空间不足时，
    // 确保在恢复数据库写入之前至少有 write_buffer_size 大小的可用空间
    // 在低磁盘空间条件下，避免由于频繁的 WAL 写入失败和强制刷新
    // 导致产生大量小的 L0 文件
    // Reserve some disk buffer space. This is a heuristic - when we run out
    // of disk space, this ensures that there is atleast write_buffer_size
    // amount of free space before we resume DB writes. In low disk space
    // conditions, we want to avoid a lot of small L0 files due to frequent
    // WAL write failures and resultant forced flushes
    sfm->ReserveDiskBuffer(max_write_buffer_size,
                           impl->immutable_db_options_.db_paths[0].path);
  }

  // === 日志输出和 WAL 同步 ===
  if (s.ok()) {
    // 记录数据库指针信息
    ROCKS_LOG_HEADER(impl->immutable_db_options_.info_log, "DB pointer %p",
                     impl);
    LogFlush(impl->immutable_db_options_.info_log);

    // 如果 WAL 缓冲区不为空，刷新并同步
    if (!impl->WALBufferIsEmpty()) {
      s = impl->FlushWAL(false);
      if (s.ok()) {
        // 需要同步，否则断电后 WAL 缓冲数据可能丢失
        // Sync is needed otherwise WAL buffered data might get lost after a
        // power reset.
        log::Writer* log_writer = impl->logs_.back().writer;
        s = log_writer->file()->Sync(impl->immutable_db_options_.use_fsync);
      }
    }

    // 检查选项持久化状态
    // 即使之前的 s.ok()，如果选项持久化失败，返回错误
    if (s.ok() && !persist_options_status.ok()) {
      s = Status::IOError(
          "DB::Open() failed --- Unable to persist Options file",
          persist_options_status.ToString());
    }
  }

  // === 错误日志记录 ===
  if (!s.ok()) {
    ROCKS_LOG_WARN(impl->immutable_db_options_.info_log,
                   "DB::Open() failed: %s", s.ToString().c_str());
  }

  // === 启动周期性任务调度器 ===
  // 包括定期统计信息 dump、日志清理等后台任务
  if (s.ok()) {
    s = impl->StartPeriodicTaskScheduler();
  }

  // === 注册序列号时间记录工作器 ===
  // 用于跟踪序列号和时间戳的关系
  if (s.ok()) {
    s = impl->RegisterRecordSeqnoTimeWorker();
  }

  // === 错误清理 ===
  // 如果打开失败，清理所有已分配的资源
  if (!s.ok()) {
    // 删除所有创建的列族句柄
    for (auto* h : *handles) {
      delete h;
    }
    // 清空句柄列表
    handles->clear();
    // 删除数据库实例
    delete impl;
    *dbptr = nullptr;
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
