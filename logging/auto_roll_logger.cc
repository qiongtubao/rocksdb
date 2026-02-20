//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "logging/auto_roll_logger.h"

#include <algorithm>

#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// ============================================================================
// AutoRollLogger::AutoRollLogger 构造函数
// ============================================================================
// 函数名: AutoRollLogger::AutoRollLogger
// 功能描述: 创建一个支持自动滚动的日志记录器实例
//
// 参数说明:
//   - fs: 文件系统接口的共享指针，用于执行文件操作（创建、删除、重命名等）
//   - clock: 系统时钟接口的共享指针，用于获取当前时间（微秒级）
//   - dbname: 数据库名称/路径，用于确定日志文件的基路径
//   - db_log_dir: 日志文件存储目录，如果为空则使用数据库目录
//     * 支持将日志文件与数据库文件分离存储（如数据库在 SSD，日志在 HDD）
//   - log_max_size: 单个日志文件的最大大小（字节），超过此大小触发滚动
//     * 为 0 表示不按大小滚动
//     * 建议值：通常设置为 MB 或 GB 级别（如 64MB, 1GB）
//   - log_file_time_to_roll: 日志文件滚动的时间间隔（秒），超过此时间触发滚动
//     * 为 0 表示不按时间滚动
//     * 建议值：通常设置为小时或天级别（如 3600秒=1小时, 86400秒=1天）
//   - keep_log_file_num: 保留的旧日志文件数量
//     * 滚动时，超过此数量的旧日志文件会被自动删除
//     * 为 0 表示保留所有旧日志文件
//     * 建议值：通常设置为 5-10 个
//   - log_level: 日志记录级别（InfoLogLevel 枚举类型）
//     * DEBUG_LEVEL: 调试信息（最详细）
//     * INFO_LEVEL: 一般信息（默认）
//     * WARN_LEVEL: 警告信息
//     * ERROR_LEVEL: 错误信息
//     * FATAL_LEVEL: 致命错误（最少）
//     * HEADER_LEVEL: 头部信息
//
// 返回值: 无（构造函数）
//
// 初始化过程:
//   1. 基类初始化: 调用 Logger(log_level) 初始化基类
//   2. 成员变量初始化:
//      - 保存构造函数参数（fs, clock, dbname, db_log_dir 等）
//      - 初始化日志大小/时间限制和保留数量
//      - 初始化缓存时间相关变量（减少系统调用）
//      - 初始化互斥锁（用于线程安全）
//   3. 获取数据库绝对路径: 将 dbname 转换为绝对路径
//      * 用于生成唯一的日志文件名
//      * 处理文件系统不支持 GetAbsolutePath 的情况
//   4. 生成当前日志文件名: 使用 InfoLogFileName() 函数
//      * 命名规则：<db_log_dir>/LOG 或 <dbname>/LOG
//   5. 检查并滚动已存在的日志文件:
//      * 如果当前日志文件已存在，先将其滚动为旧日志
//      * 避免新日志被旧日志覆盖
//   6. 扫描已存在的旧日志文件:
//      * 读取日志目录中所有的旧日志文件
//      * 按时间戳排序，入队到 old_log_files_ 队列
//   7. 创建新的日志记录器: 调用 ResetLogger()
//      * 打开新的日志文件
//      * 验证底层 Logger 支持 GetLogFileSize()
//   8. 清理超出限制的旧日志文件: 调用 TrimOldLogFiles()
//      * 删除超出 keep_log_file_num 数量的旧日志
//      * 保证磁盘空间不会被旧日志占满
//
// 日志滚动机制:
//   - 按大小滚动: 当日志文件大小超过 log_max_size 时自动滚动
//   - 按时间滚动: 每隔 log_file_time_to_roll 秒自动滚动
//   - 滚动操作:
//     1. 关闭当前日志文件
//     2. 重命名为旧日志: <dbname>.<timestamp>.log
//     3. 创建新的日志文件
//     4. 写入 header 信息到新日志
//     5. 清理超出限制的旧日志文件
//
// 线程安全性:
//   - 使用 mutex_ 保护所有共享状态的访问
//   - Logv() 函数会自动检查滚动条件并执行滚动
//   - 滚动操作在互斥锁保护下进行
//
// 性能优化:
//   - 时间缓存: 使用 cached_now 变量缓存当前时间
//   - 减少 NowMicros() 调用: 每 100 条日志记录才更新一次时间
//   - call_NowMicros_every_N_records_: 控制更新频率（默认 100）
//
// 错误处理:
//   - 如果文件系统不支持 GetAbsolutePath，使用 dbname 作为绝对路径
//   - 创建 Logger 失败时，status_ 会被设置为错误状态
//   - 调用者可以通过 GetStatus() 检查初始化是否成功
//
// 使用场景:
//   - 在 CreateLoggerFromOptions() 中被调用
//   - 用于 RocksDB 数据库的日志记录
//   - 适用于需要长期运行且产生大量日志的应用
//
// 示例:
//   // 创建一个按大小（100MB）和时间（1天）滚动的 Logger，保留 10 个旧日志
//   AutoRollLogger logger(fs, clock, "/data/rocksdb",
//                          "/var/log/rocksdb",
//                          100 * 1024 * 1024,  // 100MB
//                          86400,              // 1天
//                          10,                 // 保留10个旧日志
//                          InfoLogLevel::INFO_LEVEL);
//
// 注意事项:
//   - db_log_dir 必须已存在或可创建，否则初始化失败
//   - log_max_size 和 log_file_time_to_roll 至少有一个大于 0
//   - 旧日志文件被删除时不会通过 DB 限流（rate limit）
//   - 日志滚动时可能会有短暂的日志丢失（关闭和重新打开日志文件的间隙）
// ============================================================================
AutoRollLogger::AutoRollLogger(const std::shared_ptr<FileSystem>& fs,
                               const std::shared_ptr<SystemClock>& clock,
                               const std::string& dbname,
                               const std::string& db_log_dir,
                               size_t log_max_size,
                               size_t log_file_time_to_roll,
                               size_t keep_log_file_num,
                               const InfoLogLevel log_level)
    : Logger(log_level),
      dbname_(dbname),
      db_log_dir_(db_log_dir),
      fs_(fs),
      clock_(clock),
      status_(Status::OK()),
      kMaxLogFileSize(log_max_size),
      kLogFileTimeToRoll(log_file_time_to_roll),
      kKeepLogFileNum(keep_log_file_num),
      cached_now(static_cast<uint64_t>(clock_->NowMicros() * 1e-6)),
      ctime_(cached_now),
      cached_now_access_count(0),
      call_NowMicros_every_N_records_(100),
      mutex_() {

  // === 获取数据库绝对路径 ===
  // 将相对路径 dbname 转换为绝对路径，用于生成唯一的日志文件名
  Status s = fs->GetAbsolutePath(dbname, io_options_, &db_absolute_path_,
                                 &io_context_);
  // 某些文件系统可能不支持 GetAbsolutePath 操作
  // 在这种情况下，直接使用 dbname 作为绝对路径
  if (s.IsNotSupported()) {
    db_absolute_path_ = dbname;
  } else {
    // 如果 GetAbsolutePath 失败（不是不支持），保存错误状态
    status_ = s;
  }

  // === 生成当前日志文件名 ===
  // 日志文件名规则:
  //   - 如果 db_log_dir 为空: <dbname>/LOG
  //   - 如果 db_log_dir 不为空: <db_log_dir>/LOG
  // InfoLogFileName() 会处理路径拼接逻辑
  log_fname_ = InfoLogFileName(dbname_, db_absolute_path_, db_log_dir_);

  // === 检查并滚动已存在的日志文件 ===
  // 如果当前日志文件已存在（可能是之前数据库运行时留下的），
  // 需要将其滚动为旧日志，避免新日志内容与旧日志混淆
  if (fs_->FileExists(log_fname_, io_options_, &io_context_).ok()) {
    // 调用 RollLogFile() 将当前日志重命名为带时间戳的旧日志
    RollLogFile();
  }

  // === 扫描并加载已存在的旧日志文件 ===
  // 读取日志目录中所有的旧日志文件，按时间戳排序后入队
  // 这对于后续 TrimOldLogFiles() 清理旧日志很重要
  GetExistingFiles();

  // === 创建新的日志记录器 ===
  // 打开新的日志文件，初始化底层 Logger 实例
  s = ResetLogger();

  // === 清理超出限制的旧日志文件 ===
  // 如果新 Logger 创建成功且当前状态正常，清理超出 kKeepLogFileNum 的旧日志
  // 如果 kKeepLogFileNum = 0，则保留所有旧日志文件
  if (s.ok() && status_.ok()) {
    status_ = TrimOldLogFiles();
  }
}

Status AutoRollLogger::ResetLogger() {
  TEST_SYNC_POINT("AutoRollLogger::ResetLogger:BeforeNewLogger");
  status_ = fs_->NewLogger(log_fname_, io_options_, &logger_, &io_context_);
  TEST_SYNC_POINT("AutoRollLogger::ResetLogger:AfterNewLogger");

  if (!status_.ok()) {
    return status_;
  }
  assert(logger_);
  logger_->SetInfoLogLevel(Logger::GetInfoLogLevel());

  if (logger_->GetLogFileSize() == Logger::kDoNotSupportGetLogFileSize) {
    status_ = Status::NotSupported(
        "The underlying logger doesn't support GetLogFileSize()");
  }
  if (status_.ok()) {
    cached_now = static_cast<uint64_t>(clock_->NowMicros() * 1e-6);
    ctime_ = cached_now;
    cached_now_access_count = 0;
  }

  return status_;
}

// ============================================================================
// 函数名: AutoRollLogger::RollLogFile
// 功能描述: 滚动日志文件（将当前日志文件重命名为旧日志文件）
//
// 参数说明: 无
//
// 返回值: 无（void）
//
// 函数功能:
//   1. 生成唯一的旧日志文件名（带时间戳）
//   2. 等待 Logger 引用计数降为 1（确保没有 Flush 操作正在使用）
//   3. 关闭当前 Logger（释放文件句柄）
//   4. 重命名当前日志文件为旧日志文件
//   5. 将旧日志文件名加入队列（用于后续清理）
//
// 日志文件名规则:
//   - 当前日志文件: LOG（或 LOG.old 等格式）
//   - 旧日志文件: <dbname>.<timestamp>.log
//   - 时间戳格式: 微秒级 Unix 时间戳
//
// 实现细节:
//   1. 防止文件名冲突:
//      - 获取当前微秒级时间戳
//      - 使用 OldInfoLogFileName() 生成旧日志文件名
//      - 如果文件名已存在，时间戳递增 1 微秒，重试
//      - 循环直到找到唯一的文件名
//
//   2. 等待 Flush 完成:
//      - Logger 可能有多个引用（shared_ptr）
//      - 如果有 Flush 操作正在进行，Logger 的引用计数 > 1
//      - 等待引用计数降为 1（只有主引用）
//      - 避免在 Flush 期间关闭 Logger 导致数据丢失
//
//   3. 关闭 Logger:
//      - 调用 logger_->Close() 释放底层文件句柄
//      - 使用 PermitUncheckedError() 忽略关闭失败（析构函数中）
//      - 即使关闭失败，也继续执行重命名操作
//
//   4. 重命名日志文件:
//      - 将当前日志文件（LOG）重命名为旧日志文件名
//      - 使用 FileSystem::RenameFile() 执行重命名
//      - 如果重命名失败，忽略错误（不影响后续操作）
//
//   5. 记录旧日志:
//      - 将旧日志文件名加入 old_log_files_ 队列
//      - TrimOldLogFiles() 会根据 keep_log_file_num 限制删除旧日志
//
// 调用时机:
//   1. AutoRollLogger 构造函数: 如果当前日志文件已存在，先滚动
//   2. Logv() 函数: 当日志文件大小超过 kMaxLogFileSize 时
//   3. Logv() 函数: 当日志文件超过 kLogFileTimeToRoll 时间限制时
//
// 线程安全性:
//   - 调用此函数前必须持有 mutex_ 锁（Logv() 中已持有）
//   - 等待 Flush 完成期间不持有锁（避免死锁）
//   - 文件系统操作（RenameFile）是原子的（通常由底层文件系统保证）
//
// 性能考虑:
//   - 等待 Flush 完成可能阻塞（通常很快）
//   - 文件名冲突检查使用 FileExists()，可能需要多次系统调用
//   - 避免在高频日志记录场景频繁滚动（调整 kMaxLogFileSize）
//
// 错误处理:
//   - 文件重命名失败时忽略错误（继续执行）
//   - Logger 关闭失败时忽略错误（使用 PermitUncheckedError）
//   - 错误不影响后续创建新日志文件（ResetLogger 会处理）
//
// 注意事项:
//   - 此函数不在日志滚动时删除旧文件（由 TrimOldLogFiles 处理）
//   - 滚动操作不是完全原子的（关闭和重命名是分开的）
//   - 在滚动间隙可能会有日志记录丢失（Logv 会检查滚动条件）
//   - 旧日志文件的删除不经过 DB 的 I/O 限流（直接使用 FileSystem）
//
// 相关函数:
//   - ResetLogger(): 创建新的日志记录器
//   - TrimOldLogFiles(): 清理超出限制的旧日志文件
//   - OldInfoLogFileName(): 生成旧日志文件名
//   - Logv(): 主日志记录函数（触发滚动）
// ============================================================================
void AutoRollLogger::RollLogFile() {
  // === 获取当前时间戳（微秒级） ===
  // 使用系统时钟的微秒级时间戳作为旧日志文件的文件名
  // 例如: 1234567890123456 -> dbname.1234567890123456.log
  uint64_t now = clock_->NowMicros();

  // === 生成唯一的旧日志文件名 ===
  // 问题: 如果短时间内（1微秒内）发生多次滚动，时间戳相同
  // 后果: 旧日志文件名会冲突，导致覆盖之前的旧日志
  // 解决: 循环递增时间戳，直到生成不存在的文件名
  std::string old_fname;
  do {
    // 使用当前时间戳生成旧日志文件名
    // OldInfoLogFileName() 格式: <db_path>/<dbname>.<timestamp>.log
    old_fname =
        OldInfoLogFileName(dbname_, now, db_absolute_path_, db_log_dir_);
    now++;  // 时间戳递增 1 微秒，避免文件名冲突
  } while (fs_->FileExists(old_fname, io_options_, &io_context_).ok());

  // === 等待 Flush 操作完成 ===
  // 问题: logger_ 可能有多个引用（shared_ptr），其中一个是主引用
  //       Flush 操作可能会临时增加引用计数，避免 Logger 被提前释放
  // 后果: 如果在 Flush 期间关闭 Logger，会导致写入失败或数据丢失
  // 解决: 等待引用计数降为 1（只剩下主引用）
  // 注意: 此循环不持有任何锁，允许 Flush 操作继续执行
  while (logger_.use_count() > 1) {
    // 等待 Flush 完成（引用计数降为 1）
    // 使用忙等待（busy wait），但通常很快（Flush 不会持续太久）
    // 可以考虑添加短暂休眠（如 std::this_thread::sleep_for）减少 CPU 占用
  }

  // === 关闭当前的 Logger ===
  // 必须在重命名文件之前关闭 Logger，原因:
  //   1. Logger 可能持有底层文件的句柄（FILE* 或文件描述符）
  //   2. 某些文件系统不允许重命名已打开的文件（尤其是 Windows）
  //   3. 即使允许，重命名操作可能导致后续写入到错误的文件
  //
  // 错误处理: 使用 PermitUncheckedError() 忽略关闭失败
  //   - 在析构函数或清理操作中，关闭失败通常无法恢复
  //   - 忽略错误可以让程序继续执行（后续操作可能仍然成功）
  //   - 如果关闭失败，重命名操作也可能失败，但不会造成数据损坏
  if (logger_) {
    logger_->Close().PermitUncheckedError();
  }

  // === 重命名日志文件 ===
  // 将当前日志文件（LOG）重命名为旧日志文件（<dbname>.<timestamp>.log）
  // 文件系统操作细节:
  //   - 原子性: 大多数文件系统的 RenameFile 是原子操作
  //   - 跨文件系统: 不支持跨文件系统重命名（返回错误）
  //   - 覆盖: 目标文件存在时会失败（已在上面的循环中检查）
  //
  // 错误处理: 忽略重命名失败
  //   - 如果重命名失败，旧日志文件会保留在原位置
  //   - ResetLogger() 会创建新的日志文件（可能覆盖旧文件）
  //   - 最坏情况: 部分日志可能丢失，但不会导致程序崩溃
  Status s = fs_->RenameFile(log_fname_, old_fname, io_options_, &io_context_);
  if (!s.ok()) {
    // 重命名失败，不进行特殊处理
    // 可能的原因:
    //   1. 文件系统不支持重命名操作
    //   2. 目标文件仍存在（循环检查和重命名之间的竞态）
    //   3. 权限不足
    //   4. 跨文件系统重命名（如从 SSD 重命名到 HDD）
  }

  // === 将旧日志文件名加入队列 ===
  // 作用: 记录所有旧日志文件，用于后续清理
  // TrimOldLogFiles() 会根据 kKeepLogFileNum 限制删除旧日志
  //   - 如果队列大小 >= kKeepLogFileNum，删除最旧的日志
  //   - 如果 kKeepLogFileNum = 0，保留所有旧日志
  // 注意: 队列按时间戳升序排列，队首是最旧的日志
  old_log_files_.push(old_fname);
}

// ============================================================================
// 函数名: AutoRollLogger::GetExistingFiles
// 功能描述: 扫描并加载日志目录中已存在的旧日志文件
//
// 参数说明: 无
//
// 返回值: 无（void），但会更新 status_ 成员变量
//
// 函数功能:
//   1. 清空旧日志文件队列（避免重复）
//   2. 扫描日志目录，获取所有旧日志文件列表
//   3. 按文件名排序（时间戳升序）
//   4. 将旧日志文件加入队列（按时间从旧到新）
//
// 日志文件识别规则:
//   - GetInfoLogFiles() 函数会扫描目录并识别以下文件:
//     * 当前日志文件: LOG
//     * 旧日志文件: <dbname>.<timestamp>.log
//     * 其他格式: LOG.old, LOG.old.1, LOG.old.2, ...
//
// 排序规则:
//   - 使用 std::sort() 按字典序排序文件名
//   - 由于文件名包含时间戳，字典序 = 时间戳升序
//   - 队列顺序: 队首是最旧的文件，队尾是最新的文件
//
// 排序示例:
//   假设扫描到以下旧日志文件:
//     mydb.1706234567000000.log  (时间戳: 1706234567)
//     mydb.1706234998000000.log  (时间戳: 1706234998)
//     mydb.1706235109000000.log  (时间戳: 1706235109)
//
//   排序后（升序）:
//     1. mydb.1706234567000000.log  (最旧)
//     2. mydb.1706234998000000.log
//     3. mydb.1706235109000000.log  (最新)
//
//   入队顺序（从队首到队尾）:
//     mydb.1706234567000000.log -> mydb.1706234998000000.log -> mydb.1706235109000000.log
//
// 调用时机:
//   - AutoRollLogger 构造函数中调用（初始化时扫描旧日志）
//
// 清空队列的原因:
//   - 避免重复条目: 如果此函数被多次调用，队列会累积重复的文件
//   - 确保一致性: 每次调用都反映文件系统的最新状态
//   - 简化逻辑: 不需要在入队前检查是否已存在
//
// 错误处理:
//   - GetInfoLogFiles() 失败时，更新 status_ 成员变量
//   - 只有在当前 status_.ok() 为 true 时才更新（保留之前的错误）
//   - 队列清空操作不检查状态（即使扫描失败也会清空）
//
// 线程安全性:
//   - 调用此函数前必须持有 mutex_ 锁（构造函数中已持有）
//   - std::swap 是线程安全的（在互斥锁保护下）
//   - GetInfoLogFiles() 和 std::sort() 不持有锁（文件系统操作）
//
// 性能考虑:
//   - 文件扫描: 目录中的文件数量可能很多，扫描时间可能较长
//   - 排序: 使用 std::sort()，时间复杂度 O(n log n)
//   - 建议: 如果旧日志文件很多（超过 1000），考虑限制扫描范围
//
// 路径处理:
//   - parent_dir: 日志文件所在的父目录
//     * 如果 db_log_dir 为空，parent_dir = db_absolute_path_
//     * 如果 db_log_dir 不为空，parent_dir = db_log_dir_
//   - info_log_files: 相对文件名（不含父目录）
//   - 入队时拼接完整路径: parent_dir + "/" + f
//
// 使用场景:
//   - 构造函数初始化: 扫描已存在的旧日志文件
//   - 为 TrimOldLogFiles() 提供文件列表（删除超出限制的旧日志）
//   - 在 AutoRollLogger 生命周期内，可能需要重新扫描（暂未实现）
//
// 注意事项:
//   - 此函数只识别 RocksDB 日志文件格式，不会识别其他文件
//   - 排序基于文件名字符串，如果时间戳格式改变，排序可能不准确
//   - 队列中的文件路径是绝对路径（parent_dir 是绝对路径）
//   - 如果文件系统不支持文件排序（如某些网络文件系统），可能出现意外行为
//
// 相关函数:
//   - GetInfoLogFiles(): 扫描目录并识别日志文件
//   - TrimOldLogFiles(): 删除超出限制的旧日志文件
//   - RollLogFile(): 滚动日志文件（生成新的旧日志）
//   - OldInfoLogFileName(): 生成旧日志文件名
// ============================================================================
void AutoRollLogger::GetExistingFiles() {
  // === 清空旧日志文件队列 ===
  // 目的: 避免重复条目，确保队列反映文件系统的最新状态
  // 实现: 使用 std::swap() 交换空队列，比逐个弹出元素更高效
  // 注意: 此操作在局部作用域内执行（使用花括号限制作用域）
  {
    // 创建一个空队列
    std::queue<std::string> empty;
    // 交换空队列和 old_log_files_（old_log_files_ 被清空）
    // std::swap 是高效的 O(1) 操作（只交换内部指针）
    std::swap(old_log_files_, empty);
    // empty 离开作用域时自动销毁（包含原来的队列元素）
  }

  // === 扫描日志目录，获取旧日志文件列表 ===
  // parent_dir: 日志文件所在的父目录（绝对路径）
  //   - 如果 db_log_dir_ 为空: parent_dir = db_absolute_path_（数据库目录）
  //   - 如果 db_log_dir_ 不为空: parent_dir = db_log_dir_（日志目录）
  // info_log_files: 旧日志文件名列表（相对路径，不含父目录）
  //   - 例如: {"mydb.1706234567000000.log", "mydb.1706234998000000.log", ...}
  std::string parent_dir;
  std::vector<std::string> info_log_files;
  Status s =
      GetInfoLogFiles(fs_, db_log_dir_, dbname_, &parent_dir, &info_log_files);

  // === 处理扫描错误 ===
  // 只有在当前 status_.ok() 为 true 时才更新 status_
  // 这样可以保留之前发生的错误，不会覆盖
  // 例如: 如果之前的 GetAbsolutePath() 失败，不覆盖这个错误
  if (status_.ok()) {
    status_ = s;
  }

  // === 按文件名排序（时间戳升序） ===
  // 为什么需要排序:
  //   1. 确保队列中的文件按时间从旧到新排列
  //   2. TrimOldLogFiles() 从队首删除最旧的文件
  //   3. 如果不排序，可能会删除错误的文件（如最新的文件）
  //
  // 排序方式: 使用 std::sort() 按字典序排序
  //   - 文件名格式: <dbname>.<timestamp>.log
  //   - 例如: "mydb.1706234567000000.log", "mydb.1706234998000000.log"
  //   - 字典序排序会按时间戳升序排列
  //   - 1706234567 < 1706234998，所以前者排在前面（更旧）
  std::sort(info_log_files.begin(), info_log_files.end());

  // === 将旧日志文件加入队列 ===
  // 拼接完整路径: parent_dir + "/" + f
  //   - parent_dir 是绝对路径（如 "/var/log/rocksdb"）
  //   - f 是相对文件名（如 "mydb.1706234567000000.log"）
  //   - 完整路径: "/var/log/rocksdb/mydb.1706234567000000.log"
  //
  // 入队顺序: 从最旧到最新（与排序顺序一致）
  //   - 队首是最旧的文件
  //   - 队尾是最新的文件
  //   - TrimOldLogFiles() 从队首删除，保证先删除最旧的文件
  for (const std::string& f : info_log_files) {
    old_log_files_.push(parent_dir + "/" + f);
  }
}

Status AutoRollLogger::TrimOldLogFiles() {
  // Here we directly list info files and delete them through FileSystem.
  // The deletion isn't going through DB, so there are shortcomes:
  // 1. the deletion is not rate limited by SstFileManager
  // 2. there is a chance that an I/O will be issued here
  // Since it's going to be complicated to pass DB object down to
  // here, we take a simple approach to keep the code easier to
  // maintain.

  // old_log_files_.empty() is helpful for the corner case that
  // kKeepLogFileNum == 0. We can instead check kKeepLogFileNum != 0 but
  // it's essentially the same thing, and checking empty before accessing
  // the queue feels safer.
  while (!old_log_files_.empty() && old_log_files_.size() >= kKeepLogFileNum) {
    Status s =
        fs_->DeleteFile(old_log_files_.front(), io_options_, &io_context_);
    // Remove the file from the tracking anyway. It's possible that
    // DB cleaned up the old log file, or people cleaned it up manually.
    old_log_files_.pop();
    // To make the file really go away, we should sync parent directory.
    // Since there isn't any consistency issue involved here, skipping
    // this part to avoid one I/O here.
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

std::string AutoRollLogger::ValistToString(const char* format,
                                           va_list args) const {
  // Any log messages longer than 1024 will get truncated.
  // The user is responsible for chopping longer messages into multi line log
  static const int MAXBUFFERSIZE = 1024;
  char buffer[MAXBUFFERSIZE];

  int count = vsnprintf(buffer, MAXBUFFERSIZE, format, args);
  (void)count;
  assert(count >= 0);

  return buffer;
}

void AutoRollLogger::LogInternal(const char* format, ...) {
  mutex_.AssertHeld();

  if (!logger_) {
    return;
  }

  va_list args;
  va_start(args, format);
  logger_->Logv(format, args);
  va_end(args);
}

void AutoRollLogger::Logv(const char* format, va_list ap) {
  assert(GetStatus().ok());
  if (!logger_) {
    return;
  }

  std::shared_ptr<Logger> logger;
  {
    MutexLock l(&mutex_);
    if ((kLogFileTimeToRoll > 0 && LogExpired()) ||
        (kMaxLogFileSize > 0 && logger_->GetLogFileSize() >= kMaxLogFileSize)) {
      RollLogFile();
      Status s = ResetLogger();
      Status s2 = TrimOldLogFiles();

      if (!s.ok()) {
        // can't really log the error if creating a new LOG file failed
        return;
      }

      WriteHeaderInfo();

      if (!s2.ok()) {
        ROCKS_LOG_WARN(logger.get(), "Fail to trim old info log file: %s",
                       s2.ToString().c_str());
      }
    }

    // pin down the current logger_ instance before releasing the mutex.
    logger = logger_;
  }

  // Another thread could have put a new Logger instance into logger_ by now.
  // However, since logger is still hanging on to the previous instance
  // (reference count is not zero), we don't have to worry about it being
  // deleted while we are accessing it.
  // Note that logv itself is not mutex protected to allow maximum concurrency,
  // as thread safety should have been handled by the underlying logger.
  logger->Logv(format, ap);
}

void AutoRollLogger::WriteHeaderInfo() {
  mutex_.AssertHeld();
  for (auto& header : headers_) {
    LogInternal("%s", header.c_str());
  }
}

void AutoRollLogger::LogHeader(const char* format, va_list args) {
  if (!logger_) {
    return;
  }

  // header message are to be retained in memory. Since we cannot make any
  // assumptions about the data contained in va_list, we will retain them as
  // strings
  va_list tmp;
  va_copy(tmp, args);
  std::string data = ValistToString(format, tmp);
  va_end(tmp);

  MutexLock l(&mutex_);
  headers_.push_back(data);

  // Log the original message to the current log
  logger_->Logv(format, args);
}

bool AutoRollLogger::LogExpired() {
  if (cached_now_access_count >= call_NowMicros_every_N_records_) {
    cached_now = static_cast<uint64_t>(clock_->NowMicros() * 1e-6);
    cached_now_access_count = 0;
  }

  ++cached_now_access_count;
  return cached_now >= ctime_ + kLogFileTimeToRoll;
}

// ============================================================================
// 函数名: CreateLoggerFromOptions
// 功能描述: 根据数据库选项创建或获取日志记录器（Logger）
//
// 参数说明:
//   - dbname: 数据库的路径/名称，用于确定日志文件的存储位置
//   - options: 数据库选项（DBOptions），包含日志相关的配置
//     关键选项:
//     * info_log: 用户自定义的日志记录器（如果已设置，直接使用）
//     * db_log_dir: 日志文件存储目录（如果为空，使用数据库目录）
//     * max_log_file_size: 单个日志文件的最大大小（触发滚动）
//     * log_file_time_to_roll: 日志文件滚动的时间间隔（秒）
//     * keep_log_file_num: 保留的旧日志文件数量
//     * info_log_level: 日志级别（DEBUG, INFO, WARN, ERROR 等）
//   - logger: [输出参数] 创建的日志记录器智能指针
//     * 调用者通过此指针使用日志记录器
//     * 如果用户已提供 info_log，则直接返回用户提供的记录器
//
// 返回值:
//   - Status::OK(): 日志记录器创建成功或使用用户提供的记录器
//   - Status::Error(): 创建失败（如目录不存在、权限不足、文件系统错误等）
//
// 主要功能:
//   1. 用户自定义 Logger 检查: 如果用户已提供 info_log，直接使用
//   2. 路径解析: 将相对路径转换为绝对路径
//   3. 目录创建: 创建数据库目录和日志目录（如需要）
//   4. 自动滚动 Logger: 当需要日志滚动时（按大小或时间）
//   5. 普通 Logger: 当不需要日志滚动时，创建简单的日志记录器
//   6. 日志文件重命名: 如果旧的日志文件存在，先重命名
//   7. 日志级别设置: 配置日志记录器的日志级别
//
// 日志滚动机制（AutoRollLogger）:
//   - 按大小滚动: 当日志文件大小超过 max_log_file_size 时滚动
//   - 按时间滚动: 每隔 log_file_time_to_roll 秒滚动一次
//   - 滚动后: 旧日志文件被重命名为 <dbname>.<timestamp>.log
//   - 清理旧文件: 只保留 keep_log_file_num 个旧日志文件
//
// 日志文件命名规则:
//   - 当前日志: <dbname>/LOG (或 db_log_dir/LOG)
//   - 旧日志: <dbname>/LOG.<timestamp>.log
//
// 使用场景:
//   - 在 DB::Open 之前调用（SanitizeOptions 中调用）
//   - 用于记录数据库的运行时信息（压缩、刷新、错误等）
//   - 支持日志滚动以避免单个日志文件过大
//
// 注意事项:
//   - "FileExists -> Rename" 操作序列不是原子的
//   - 在此期间如果文件被其他进程删除，会进行特殊处理
//   - db_log_dir 可能在不同的文件系统上（如独立磁盘）
//   - 日志记录器是共享指针，支持多个使用者共享同一个记录器
// ============================================================================
Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger) {
  // === 用户自定义 Logger 检查 ===
  // 如果用户已经提供了 info_log，直接使用用户提供的记录器
  // 这允许应用程序自定义日志行为（如输出到远程日志系统）
  if (options.info_log) {
    *logger = options.info_log;
    return Status::OK();
  }

  // === 获取环境对象和路径解析 ===
  // Env 封装了文件系统操作、线程创建等系统调用
  Env* env = options.env;
  // 将 dbname 转换为绝对路径
  // 这对于日志文件命名和路径比较很重要
  std::string db_absolute_path;
  Status s = env->GetAbsolutePath(dbname, &db_absolute_path);
  // 测试同步点：用于单元测试验证此逻辑
  TEST_SYNC_POINT_CALLBACK("rocksdb::CreateLoggerFromOptions:AfterGetPath", &s);
  if (!s.ok()) {
    return s;  // 路径解析失败，无法继续
  }

  // === 生成日志文件名 ===
  // 如果 db_log_dir 为空，日志文件在数据库目录下
  // 否则，日志文件在 db_log_dir 目录下
  std::string fname =
      InfoLogFileName(dbname, db_absolute_path, options.db_log_dir);

  // === 获取系统时钟 ===
  // 用于生成时间戳和日志滚动判断
  const auto& clock = env->GetSystemClock();

  // === 创建数据库目录 ===
  // 如果数据库目录不存在，尝试创建
  s = env->CreateDirIfMissing(dbname);
  if (!s.ok()) {
    if (options.db_log_dir.empty()) {
      // 如果没有指定独立的日志目录，则数据库目录必须存在
      // 创建失败则返回错误
      return s;
    } else {
      // 如果指定了独立的日志目录，则忽略数据库目录创建失败
      // 原因：
      //   - db_log_dir 和 dbname 可能在不同的文件系统上
      //   - 例如：数据库在 SSD 上，日志在 HDD 上
      //   - 在这种情况下，dbname 目录可能不存在（由另一个进程管理）
      //   - 只要 db_log_dir 能创建成功即可
      // 如果数据库目录创建失败是因为文件系统错误（不是不存在），
      // db_log_dir 的创建会在后面处理错误
      s = Status::OK();
    }
  }
  assert(s.ok());  // 确保状态是 OK

  // === 创建日志目录（如果指定） ===
  // 如果用户指定了独立的日志目录，确保该目录存在
  if (!options.db_log_dir.empty()) {
    s = env->CreateDirIfMissing(options.db_log_dir);
    if (!s.ok()) {
      return s;  // 日志目录创建失败，无法继续
    }
  }

  // === 创建自动滚动 Logger ===
  // 当前只支持基于时间和大小的日志滚动
  // 如果用户配置了任何一种滚动机制，使用 AutoRollLogger
  if (options.log_file_time_to_roll > 0 || options.max_log_file_size > 0) {
    // 创建 AutoRollLogger 实例
    // 参数说明:
    //   - env->GetFileSystem(): 文件系统接口
    //   - clock: 系统时钟
    //   - dbname: 数据库名称
    //   - options.db_log_dir: 日志目录
    //   - options.max_log_file_size: 触发滚动的大小阈值
    //   - options.log_file_time_to_roll: 触发滚动的时间间隔
    //   - options.keep_log_file_num: 保留的旧日志数量
    //   - options.info_log_level: 日志级别
    AutoRollLogger* result = new AutoRollLogger(
        env->GetFileSystem(), clock, dbname, options.db_log_dir,
        options.max_log_file_size, options.log_file_time_to_roll,
        options.keep_log_file_num, options.info_log_level);

    // 检查 AutoRollLogger 的初始化状态
    s = result->GetStatus();
    if (!s.ok()) {
      delete result;  // 初始化失败，释放内存
    } else {
      logger->reset(result);  // 初始化成功，返回给调用者
    }
    return s;
  }

  // === 创建普通 Logger（不需要滚动） ===
  // 在数据库目录下打开日志文件
  // 首先检查日志文件是否已存在
  s = env->FileExists(fname);
  if (s.ok()) {
    // 日志文件已存在，需要先重命名为旧日志文件
    // 使用当前时间戳生成旧日志文件名
    s = env->RenameFile(
        fname, OldInfoLogFileName(dbname, clock->NowMicros(), db_absolute_path,
                                  options.db_log_dir));

    // === 处理 "FileExists -> Rename" 非原子操作竞态条件 ===
    // "FileExists -> Rename" 操作序列不是原子的（两个独立的系统调用）
    // 可能的竞态条件：
    //   1. FileExists 返回 OK（文件存在）
    //   2. 在调用 Rename 之前，文件被其他进程/线程删除或重命名
    //   3. Rename 返回 IOError，子码为 PathNotFound
    //
    // 虽然这种情况很罕见，且应用程序应避免并发修改数据库目录，
    // 但我们仍然进行简单的处理以提高健壮性。处理逻辑：
    //
    // 1. 如果 Rename() 返回 PathNotFound 错误，检查源文件（即 LOG）是否还存在
    // 2. 如果 LOG 存在，说明 Rename() 失败是因为其他原因（如权限问题），报告错误
    // 3. 如果 LOG 不存在，说明文件已被其他人删除/重命名
    //    由于文件不存在，我们可以将状态重置为 OK，
    //    让调用者尝试创建新的 LOG 文件。如果成功，我们仍然允许它
    if (s.IsPathNotFound()) {
      // 再次检查日志文件是否存在
      s = env->FileExists(fname);
      if (s.IsNotFound()) {
        // 文件确实不存在，可能是被其他进程删除了
        // 重置为 OK，继续创建新的日志文件
        s = Status::OK();
      }
      // 如果文件存在，说明 Rename 失败有其他原因，保持错误状态
    }
  } else if (s.IsNotFound()) {
    // LOG 文件不存在是正常的（可能是新创建的数据库）
    // 重置为 OK，继续创建新的日志文件
    s = Status::OK();
  }
  // === 创建新的 Logger ===
  // 创建并打开日志文件
  if (s.ok()) {
    s = env->NewLogger(fname, logger);
  }

  // === 设置日志级别 ===
  // 如果 Logger 创建成功且非空，设置用户指定的日志级别
  // 日志级别控制哪些消息会被记录（DEBUG 级别最详细，FATAL 级别最少）
  if (s.ok() && logger->get() != nullptr) {
    (*logger)->SetInfoLogLevel(options.info_log_level);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
