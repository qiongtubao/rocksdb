//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "file/filename.h"

#include <ctype.h>
#include <stdio.h>

#include <cinttypes>
#include <vector>

#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

const std::string kCurrentFileName = "CURRENT";
const std::string kOptionsFileNamePrefix = "OPTIONS-";
const std::string kTempFileNameSuffix = "dbtmp";

static const std::string kRocksDbTFileExt = "sst";
static const std::string kLevelDbTFileExt = "ldb";
static const std::string kRocksDBBlobFileExt = "blob";
static const std::string kArchivalDirName = "archive";

// Given a path, flatten the path name by replacing all chars not in
// {[0-9,a-z,A-Z,-,_,.]} with _. And append '_LOG\0' at the end.
// Return the number of chars stored in dest not including the trailing '\0'.
// ============================================================================
// GetInfoLogPrefix 函数
// ============================================================================
// 函数名: GetInfoLogPrefix
// 功能描述: 将数据库路径编码为日志文件的唯一前缀
//
// 参数说明:
//   - path: 需要编码的数据库路径（已规范化）
//     * 通常是由 NormalizePath() 处理后的绝对路径
//     * 例如: "/data/rocksdb" 或 "var_lib_rocksdb"
//   - dest: 输出缓冲区，用于存储编码后的前缀
//     * 必须有足够的空间（建议至少 260 字节）
//     * 格式: "<encoded_path>_LOG\0"
//   - len: 输出缓冲区的大小（字节）
//     * 用于防止缓冲区溢出
//     * 必须至少为 sizeof("_LOG") + 1（即 6 字节）
//
// 返回值:
//   - 编码后的前缀字符串长度（不包含 '\0' 终止符）
//
// 编码规则:
//   1. 保留的字符（直接复制到输出）:
//      - 小写字母: a-z
//      - 大写字母: A-Z
//      - 数字: 0-9
//      - 特殊字符: 横杠(-)、点(.)、下划线(_)
//
//   2. 其他字符的处理:
//      - 替换为下划线(_)
//      - 但在路径开头（i=0）的特殊字符会被跳过
//
//   3. 后缀:
//      - 在编码后的路径末尾添加 "_LOG"
//      - 这是固定的日志文件标识
//
// 编码示例:
//   示例1: 简单路径
//     输入: "/data/rocksdb"
//     处理:
//       - '/' -> 跳过（开头）
//       - 'd', 'a', 't', 'a' -> 保留 -> "data"
//       - '/' -> '_'
//       - 'r', 'o', 'c', 'k', 's', 'd', 'b' -> 保留 -> "_rocksdb"
//       - 结果: "data_rocksdb_LOG"
//
//   示例2: 包含特殊字符的路径
//     输入: "/var/lib/mysql@prod"
//     处理:
//       - '/' -> 跳过（开头）
//       - 'v', 'a', 'r' -> 保留 -> "var"
//       - '/' -> '_'
//       - 'l', 'i', 'b' -> 保留 -> "_lib"
//       - '/' -> '_'
//       - 'm', 'y', 's', 'q', 'l' -> 保留 -> "_mysql"
//       - '@' -> '_'
//       - 'p', 'r', 'o', 'd' -> 保留 -> "_prod"
//       - 结果: "var_lib_mysql_prod_LOG"
//
//   示例3: 包含点的路径（如版本号）
//     输入: "/opt/rocksdb/v6.28.1"
//     处理:
//       - '/' -> 跳过（开头）
//       - 'o', 'p', 't' -> 保留 -> "opt"
//       - '/' -> '_'
//       - 'r', 'o', 'c', 'k', 's', 'd', 'b' -> 保留 -> "_rocksdb"
//       - '/' -> '_'
//       - 'v', '6' -> 保留 -> "_v6"
//       - '.' -> 保留（点保留） -> "."
//       - '2', '8' -> 保留 -> "28"
//       - '.' -> 保留 -> "."
//       - '1' -> 保留 -> "1"
//       - 结果: "opt_rocksdb_v6.28.1_LOG"
//
//   示例4: 包含横杠的路径（如带端口号）
//     输入: "/data/db-5432"
//     处理:
//       - '/' -> 跳过（开头）
//       - 'd', 'a', 't', 'a' -> 保留 -> "data"
//       - '/' -> '_'
//       - 'd', 'b' -> 保留 -> "_db"
//       - '-' -> 保留（横杠保留） -> "-"
//       - '5', '4', '3', '2' -> 保留 -> "5432"
//       - 结果: "data_db-5432_LOG"
//
// 缓冲区保护:
//   - while 循环条件: write_idx < len - sizeof(suffix)
//     * 确保留出足够的空间添加 "_LOG" 后缀
//     * sizeof(suffix) 是 "_LOG" 的长度（包含 '\0'），即 5
//     * 实际需要留 4 个字符（"_" "L" "O" "G"） + 1 个 '\0'
//
//   - snprintf 调用:
//     * 确保在 dest + write_idx 后追加 "_LOG"
//     * snprintf 会自动处理缓冲区边界和 '\0' 终止
//
// 使用场景:
//   - InfoLogPrefix 构造函数中调用
//   - 用于生成日志文件的唯一标识
//   - 确保多个数据库共享日志目录时不会冲突
//
// 输入验证:
//   - 如果 dest 太小，编码会被截断
//   - 如果 path 为空，只输出 "_LOG"
//   - 如果路径全部是特殊字符，只输出 "_LOG"（开头的特殊字符被跳过）
//
// 性能特点:
//   - 时间复杂度: O(n)，n 为 path 的长度
//   - 空间复杂度: O(1)，只使用局部变量
//   - 无动态内存分配
//
// 注意事项:
//   - 路径中连续的特殊字符会产生连续的下划线
//   - 但开头的特殊字符会被跳过，不会有前导下划线
//   - 输出不包含 '\0'，调用者需要确保 dest 有足够空间
//   - 不同的路径可能编码为相同的前缀（虽然罕见）
//
// 相关函数:
//   - InfoLogPrefix::InfoLogPrefix(): 调用此函数进行编码
//   - NormalizePath(): 在调用此函数前对路径进行规范化
// ============================================================================
static size_t GetInfoLogPrefix(const std::string& path, char* dest, int len) {
  // 日志后缀，固定为 "_LOG"
  const char suffix[] = "_LOG";

  // 初始化写入索引，指向 dest 的起始位置
  size_t write_idx = 0;

  // 初始化读取索引，从 path 的第一个字符开始
  size_t i = 0;

  // 获取源路径的长度
  size_t src_len = path.size();

  // === 主编码循环 ===
  // 遍历路径的每个字符，直到:
  //   1. 所有字符处理完毕 (i < src_len)
  //   2. 缓冲区空间不足 (write_idx < len - sizeof(suffix))
  //
  // sizeof(suffix) 是 5（"_" "L" "O" "G" "\0"）
  // len - sizeof(suffix) 确保留出足够空间添加 "_LOG" 后缀
  while (i < src_len && write_idx < len - sizeof(suffix)) {
    // === 检查当前字符是否是需要保留的字符 ===
    if ((path[i] >= 'a' && path[i] <= 'z') ||      // 小写字母
        (path[i] >= '0' && path[i] <= '9') ||      // 数字
        (path[i] >= 'A' && path[i] <= 'Z') ||      // 大写字母
        path[i] == '-' ||                           // 横杠（用于端口号等）
        path[i] == '.' ||                           // 点（用于版本号等）
        path[i] == '_') {                           // 下划线
      // 是保留字符，直接复制到目标缓冲区
      dest[write_idx++] = path[i];
    } else {
      // === 非保留字符的处理 ===
      // 包括: 斜杠(/)、反斜杠(\)、空格、特殊符号等
      // 将这些字符替换为下划线(_)

      // 特殊处理: 路径开头的非保留字符跳过，不生成下划线
      // 这可以避免前导下划线，使前缀更清晰
      if (i > 0) {
        dest[write_idx++] = '_';
      }
      // 如果 i == 0，直接跳过这个字符，不写入任何内容
    }
    // 移动到下一个字符
    i++;
  }

  // === 断言检查 ===
  // 确保缓冲区剩余空间足够存储 "_LOG" 后缀
  // 如果触发断言，说明缓冲区太小或路径太长
  assert(sizeof(suffix) <= len - write_idx);

  // === 添加日志后缀 ===
  // 在编码后的路径末尾追加 "_LOG"
  // snprintf 会自动在末尾添加 '\0' 终止符
  snprintf(dest + write_idx, len - write_idx, suffix);

  // 更新总长度（后缀长度是 sizeof(suffix) - 1，因为不包含 '\0'）
  write_idx += sizeof(suffix) - 1;

  // 返回编码后的前缀总长度（不包含 '\0'）
  return write_idx;
}

static std::string MakeFileName(uint64_t number, const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%06llu.%s",
           static_cast<unsigned long long>(number), suffix);
  return buf;
}

static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {
  return name + "/" + MakeFileName(number, suffix);
}

std::string LogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

std::string LogFileName(uint64_t number) {
  assert(number > 0);
  return MakeFileName(number, "log");
}

std::string BlobFileName(uint64_t number) {
  assert(number > 0);
  return MakeFileName(number, kRocksDBBlobFileExt.c_str());
}

std::string BlobFileName(const std::string& blobdirname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(blobdirname, number, kRocksDBBlobFileExt.c_str());
}

std::string BlobFileName(const std::string& dbname, const std::string& blob_dir,
                         uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname + "/" + blob_dir, number,
                      kRocksDBBlobFileExt.c_str());
}

std::string ArchivalDirectory(const std::string& dir) {
  return dir + "/" + kArchivalDirName;
}
std::string ArchivedLogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name + "/" + kArchivalDirName, number, "log");
}

std::string MakeTableFileName(const std::string& path, uint64_t number) {
  return MakeFileName(path, number, kRocksDbTFileExt.c_str());
}

std::string MakeTableFileName(uint64_t number) {
  return MakeFileName(number, kRocksDbTFileExt.c_str());
}

std::string Rocks2LevelTableFileName(const std::string& fullname) {
  assert(fullname.size() > kRocksDbTFileExt.size() + 1);
  if (fullname.size() <= kRocksDbTFileExt.size() + 1) {
    return "";
  }
  return fullname.substr(0, fullname.size() - kRocksDbTFileExt.size()) +
         kLevelDbTFileExt;
}

uint64_t TableFileNameToNumber(const std::string& name) {
  uint64_t number = 0;
  uint64_t base = 1;
  int pos = static_cast<int>(name.find_last_of('.'));
  while (--pos >= 0 && name[pos] >= '0' && name[pos] <= '9') {
    number += (name[pos] - '0') * base;
    base *= 10;
  }
  return number;
}

std::string TableFileName(const std::vector<DbPath>& db_paths, uint64_t number,
                          uint32_t path_id) {
  assert(number > 0);
  std::string path;
  if (path_id >= db_paths.size()) {
    path = db_paths.back().path;
  } else {
    path = db_paths[path_id].path;
  }
  return MakeTableFileName(path, number);
}

void FormatFileNumber(uint64_t number, uint32_t path_id, char* out_buf,
                      size_t out_buf_size) {
  if (path_id == 0) {
    snprintf(out_buf, out_buf_size, "%" PRIu64, number);
  } else {
    snprintf(out_buf, out_buf_size,
             "%" PRIu64
             "(path "
             "%" PRIu32 ")",
             number, path_id);
  }
}

std::string DescriptorFileName(uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return buf;
}

std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  return dbname + "/" + DescriptorFileName(number);
}

std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/" + kCurrentFileName;
}

std::string LockFileName(const std::string& dbname) { return dbname + "/LOCK"; }

std::string TempFileName(const std::string& dbname, uint64_t number) {
  return MakeFileName(dbname, number, kTempFileNameSuffix.c_str());
}

// ============================================================================
// InfoLogPrefix 构造函数
// ============================================================================
// 函数名: InfoLogPrefix::InfoLogPrefix
// 功能描述: 生成 RocksDB 日志文件的唯一前缀
//
// 参数说明:
//   - has_log_dir: 是否使用独立的日志目录
//     * true: 日志文件存储在独立的 log_dir 中，需要编码数据库路径
//     * false: 日志文件存储在数据库目录中，使用简单前缀 "LOG"
//   - db_absolute_path: 数据库的绝对路径
//     * 用于生成唯一的日志文件前缀
//     * 只有在 has_log_dir 为 true 时才使用
//     * 通过编码此路径来区分不同数据库的日志文件
//
// 返回值: 无（构造函数）
//
// 生成的数据成员:
//   - buf[260]: 包含日志文件前缀的字符数组
//   - prefix: 指向 buf 中有效前缀的 Slice
//
// 前缀生成逻辑:
//   情况1: has_log_dir = false
//     - 前缀: "LOG"
//     - 用途: 日志文件在数据库目录下，使用简单的前缀
//     - 最终文件名: "<dbname>/LOG"
//
//   情况2: has_log_dir = true
//     - 前缀: "<encoded_path>_LOG"
//     - encoded_path 是对数据库路径进行编码的结果
//     - 编码规则（通过 GetInfoLogPrefix 函数）:
//       * 保留字母、数字、点(.)、横杠(-)、下划线(_)
//       * 其他字符（如斜杠/）替换为下划线(_)
//       * 在末尾添加 "_LOG" 后缀
//     - 最终文件名: "<log_dir>/<encoded_path>_LOG"
//
// 编码示例（GetInfoLogPrefix 处理逻辑）:
//   输入: "/data/rocksdb"
//   处理过程:
//     1. NormalizePath: 将路径规范化（去除尾部斜杠等）
//     2. 逐字符扫描:
//        - '/' -> '_'
//        - 'd', 'a', 't', 'a', 'r', 'o', 'c', 'k', 's', 'd', 'b' -> 保留
//     3. 添加 "_LOG" 后缀
//   输出: "_data_rocksdb_LOG"
//
// 更复杂的示例:
//   输入: "/var/lib/rocksdb/production"
//   处理:
//     - '/' -> '_'
//     - 保留: var, lib, rocksdb, production
//     - 结果: "var_lib_rocksdb_production_LOG"
//   输出: "var_lib_rocksdb_production_LOG"
//
// 使用场景:
//   1. InfoLogFileName() 中调用，生成日志文件名的前缀
//   2. OldInfoLogFileName() 中调用，生成旧日志文件名的前缀
//   3. 当多个数据库共享同一个日志目录时，确保每个数据库有唯一的日志文件
//
// 多数据库共享日志目录的例子:
//   数据库1:
//     - db_absolute_path: "/ssd/db1"
//     - log_dir: "/hdd/logs"
//     - has_log_dir: true
//     - 编码后前缀: "ssd_db1_LOG"
//     - 日志文件: "/hdd/logs/ssd_db1_LOG"
//
//   数据库2:
//     - db_absolute_path: "/ssd/db2"
//     - log_dir: "/hdd/logs"
//     - has_log_dir: true
//     - 编码后前缀: "ssd_db2_LOG"
//     - 日志文件: "/hdd/logs/ssd_db2_LOG"
//
//   两个数据库共享 "/hdd/logs" 目录，但日志文件名不同，不会冲突
//
// 成员变量说明:
//   - buf[260]: 字符数组，存储生成的日志前缀
//     * 大小为 260 字节，足够存储编码后的路径 + "_LOG"
//     * 路径编码会限制长度以防止溢出
//   - prefix: Slice 类型，指向 buf 中有效的字符串部分
//     * 不需要拷贝字符串，高效引用 buf 中的数据
//     * Slice 包含指针和长度
//
// 性能考虑:
//   - 使用 buf 数组而非动态分配字符串，避免内存分配开销
//   - 使用 Slice 引用，避免字符串拷贝
//   - 编码算法是线性的 O(n)，n 为路径长度
//
// 注意事项:
//   - db_absolute_path 应该是绝对路径，确保编码的唯一性
//   - 如果 db_absolute_path 包含特殊字符，会被转换为下划线
//   - 不同路径可能编码为相同前缀（罕见但可能），影响日志隔离性
//   - buf 的大小限制（260字节）意味着非常长的路径可能被截断
//
// 相关函数:
//   - GetInfoLogPrefix(): 核心编码函数，将路径转换为前缀
//   - NormalizePath(): 路径规范化函数
//   - InfoLogFileName(): 使用此前缀生成完整的日志文件名
//   - OldInfoLogFileName(): 使用此前缀生成旧日志文件名
// ============================================================================
InfoLogPrefix::InfoLogPrefix(bool has_log_dir,
                             const std::string& db_absolute_path) {
  // === 情况1: 日志在数据库目录下（has_log_dir = false） ===
  // 使用简单的前缀 "LOG"，不需要编码数据库路径
  // 因为日志文件在数据库目录下，不同数据库的日志文件位于不同目录，不会冲突
  if (!has_log_dir) {
    // 定义日志文件的基础前缀为 "LOG"
    const char kInfoLogPrefix[] = "LOG";

    // 将 "LOG" 写入 buf 数组
    // snprintf 会自动在末尾添加 '\0' 终止符
    snprintf(buf, sizeof(buf), kInfoLogPrefix);

    // 创建 Slice 引用 buf 中的 "LOG" 字符串
    // sizeof(kInfoLogPrefix) - 1 是字符串长度（不包含 '\0'）
    prefix = Slice(buf, sizeof(kInfoLogPrefix) - 1);
  } else {
    // === 情况2: 日志在独立目录下（has_log_dir = true） ===
    // 需要编码数据库路径以生成唯一的前缀
    // 因为多个数据库可能共享同一个日志目录，前缀必须唯一
    //
    // 步骤1: 规范化数据库路径
    // NormalizePath() 会:
    //   - 移除尾部的斜杠（如 "/data/rocksdb/" -> "/data/rocksdb"）
    //   - 将相对路径转换为绝对路径（如果可能）
    //   - 统一路径分隔符（确保使用 '/'）
    //   - 移除冗余的路径段（如 "/a/../b" -> "/b"）
    //
    // 步骤2: 编码路径为日志前缀
    // GetInfoLogPrefix() 会:
    //   - 遍历路径的每个字符
    //   - 保留字母（a-z, A-Z）、数字（0-9）、点(.)、横杠(-)、下划线(_)
    //   - 其他字符（如斜杠/）替换为下划线(_)
    //   - 连续的特殊字符会压缩为单个下划线
    //   - 在末尾添加 "_LOG" 后缀
    //   - 确保结果长度不超过 buf 的大小（260字节）
    //
    // 编码示例:
    //   输入: "/data/rocksdb"
    //   输出: "_data_rocksdb_LOG"
    //
    //   输入: "/var/lib/mysql/production"
    //   输出: "var_lib_mysql_production_LOG"
    size_t len =
        GetInfoLogPrefix(NormalizePath(db_absolute_path), buf, sizeof(buf));

    // 创建 Slice 引用 buf 中有效的前缀部分
    // len 是 GetInfoLogPrefix 返回的实际长度
    prefix = Slice(buf, len);
  }
}

// ============================================================================
// InfoLogFileName 函数
// ============================================================================
// 函数名: InfoLogFileName
// 功能描述: 生成 RocksDB 信息日志文件的完整路径名称
//
// 参数说明:
//   - dbname: 数据库名称或路径
//     * 可以是相对路径或绝对路径
//     * 例如: "/data/rocksdb" 或 "rocksdb"
//   - db_path: 数据库的绝对路径（用于生成唯一的前缀）
//     * 当 log_dir 不为空时使用
//     * 用于确保多个数据库共享日志目录时，日志文件名不会冲突
//     * 如果为空或与 dbname 相同，可以省略此参数
//   - log_dir: 日志文件存储目录
//     * 为空字符串: 日志文件存储在数据库目录下
//     * 非空字符串: 日志文件存储在指定目录下
//     * 支持将日志与数据库文件分离存储（如数据库在 SSD，日志在 HDD）
//
// 返回值:
//   - 日志文件的完整路径字符串
//
// 命名规则:
//   情况1: log_dir 为空
//     - 返回: "<dbname>/LOG"
//     - 示例: "/data/rocksdb/LOG" 或 "rocksdb/LOG"
//
//   情况2: log_dir 不为空
//     - 返回: "<log_dir>/<info_log_prefix>"
//     - info_log_prefix 格式: "LOG.<db_path_hash>" 或 "LOG"
//     - db_path_hash 是对数据库绝对路径进行编码后的值
//     - 示例: "/var/log/rocksdb/LOG.1234567890"
//
// 日志文件前缀的生成逻辑（通过 InfoLogPrefix 类）:
//   当 log_dir 不为空时，需要生成唯一的日志文件名以避免冲突：
//   1. 对 db_path 进行路径规范化（ NormalizePath）
//   2. 使用 GetInfoLogPrefix() 将规范化后的路径编码为短前缀
//   3. 编码后的前缀包含数据库路径的哈希信息
//   4. 多个数据库可以共享同一个日志目录，因为每个数据库有唯一的前缀
//
// 使用场景:
//   1. AutoRollLogger 构造函数中调用，生成当前日志文件的路径
//   2. CreateLoggerFromOptions 中调用，生成日志文件名
//   3. 日志滚动时，用于确定当前日志文件的位置
//
// 多数据库共享日志目录的场景:
//   - 假设有两个数据库:
//     * db1: /ssd/database1
//     * db2: /ssd/database2
//   - 两个数据库都使用同一个日志目录: /hdd/logs
//   - InfoLogFileName 生成的日志文件名:
//     * db1 的日志: /hdd/logs/LOG.hash1
//     * db2 的日志: /hdd/logs/LOG.hash2
//   - 这样两个数据库的日志不会冲突
//
// 注意事项:
//   - dbname 参数在 log_dir 为空时才用于构建路径
//   - 当 log_dir 不为空时，实际路径由 log_dir 决定
//   - db_path 参数用于生成唯一标识，避免不同数据库的日志冲突
//   - 如果多个数据库使用相同的 log_dir，必须提供不同的 db_path
//
// 相关函数:
//   - OldInfoLogFileName(): 生成旧日志文件名（带时间戳）
//   - InfoLogPrefix 类: 生成日志文件前缀
//   - GetInfoLogPrefix(): 将数据库路径编码为前缀
//
// 示例:
//   // 示例1: 日志在数据库目录下
//   std::string fname1 = InfoLogFileName("/data/rocksdb", "", "");
//   // 返回: "/data/rocksdb/LOG"
//
//   // 示例2: 日志在独立目录下
//   std::string fname2 = InfoLogFileName("/data/rocksdb",
//                                        "/data/rocksdb",
//                                        "/var/log/rocksdb");
//   // 返回: "/var/log/rocksdb/LOG.1234567890" (假设哈希为 1234567890)
// ============================================================================
std::string InfoLogFileName(const std::string& dbname,
                            const std::string& db_path,
                            const std::string& log_dir) {
  // === 情况1: log_dir 为空 ===
  // 日志文件直接存储在数据库目录下，使用固定的 "LOG" 文件名
  // 这是最简单的场景，日志文件和数据库文件在同一个目录
  if (log_dir.empty()) {
    return dbname + "/LOG";
  }

  // === 情况2: log_dir 不为空 ===
  // 日志文件存储在指定的独立目录下
  // 需要生成包含数据库路径信息的唯一前缀，避免多数据库日志冲突
  //
  // InfoLogPrefix 构造过程:
  // 1. 参数 has_log_dir = true，表示需要编码数据库路径
  // 2. db_path 是数据库的绝对路径，用于生成唯一标识
  // 3. 内部调用 GetInfoLogPrefix() 将 db_path 编码为短字符串
  // 4. 编码后的前缀通常格式为 "LOG.<hash>"
  //
  // 例如:
  //   - db_path = "/data/rocksdb"
  //   - 编码后可能得到 "LOG.a1b2c3d4" (哈希值)
  InfoLogPrefix info_log_prefix(true, db_path);

  // 拼接日志目录和前缀，生成完整的日志文件路径
  // 格式: "<log_dir>/<info_log_prefix>"
  return log_dir + "/" + info_log_prefix.buf;
}

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname, uint64_t ts,
                               const std::string& db_path,
                               const std::string& log_dir) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%llu", static_cast<unsigned long long>(ts));

  if (log_dir.empty()) {
    return dbname + "/LOG.old." + buf;
  }

  InfoLogPrefix info_log_prefix(true, db_path);
  return log_dir + "/" + info_log_prefix.buf + ".old." + buf;
}

std::string OptionsFileName(uint64_t file_num) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "%s%06" PRIu64,
           kOptionsFileNamePrefix.c_str(), file_num);
  return buffer;
}
std::string OptionsFileName(const std::string& dbname, uint64_t file_num) {
  return dbname + "/" + OptionsFileName(file_num);
}

std::string TempOptionsFileName(const std::string& dbname, uint64_t file_num) {
  char buffer[256];
  snprintf(buffer, sizeof(buffer), "%s%06" PRIu64 ".%s",
           kOptionsFileNamePrefix.c_str(), file_num,
           kTempFileNameSuffix.c_str());
  return dbname + "/" + buffer;
}

std::string MetaDatabaseName(const std::string& dbname, uint64_t number) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/METADB-%llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

std::string IdentityFileName(const std::string& dbname) {
  return dbname + "/IDENTITY";
}

// Owned filenames have the form:
//    dbname/IDENTITY
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/<info_log_name_prefix>
//    dbname/<info_log_name_prefix>.old.[0-9]+
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|blob)
//    dbname/METADB-[0-9]+
//    dbname/OPTIONS-[0-9]+
//    dbname/OPTIONS-[0-9]+.dbtmp
//    Disregards / at the beginning
bool ParseFileName(const std::string& fname, uint64_t* number, FileType* type,
                   WalFileType* log_type) {
  return ParseFileName(fname, number, "", type, log_type);
}

bool ParseFileName(const std::string& fname, uint64_t* number,
                   const Slice& info_log_name_prefix, FileType* type,
                   WalFileType* log_type) {
  Slice rest(fname);
  if (fname.length() > 1 && fname[0] == '/') {
    rest.remove_prefix(1);
  }
  if (rest == "IDENTITY") {
    *number = 0;
    *type = kIdentityFile;
  } else if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (info_log_name_prefix.size() > 0 &&
             rest.starts_with(info_log_name_prefix)) {
    rest.remove_prefix(info_log_name_prefix.size());
    if (rest == "" || rest == ".old") {
      *number = 0;
      *type = kInfoLogFile;
    } else if (rest.starts_with(".old.")) {
      uint64_t ts_suffix;
      // sizeof also counts the trailing '\0'.
      rest.remove_prefix(sizeof(".old.") - 1);
      if (!ConsumeDecimalNumber(&rest, &ts_suffix)) {
        return false;
      }
      *number = ts_suffix;
      *type = kInfoLogFile;
    }
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else if (rest.starts_with("METADB-")) {
    rest.remove_prefix(strlen("METADB-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kMetaDatabase;
    *number = num;
  } else if (rest.starts_with(kOptionsFileNamePrefix)) {
    uint64_t ts_suffix;
    bool is_temp_file = false;
    rest.remove_prefix(kOptionsFileNamePrefix.size());
    const std::string kTempFileNameSuffixWithDot =
        std::string(".") + kTempFileNameSuffix;
    if (rest.ends_with(kTempFileNameSuffixWithDot)) {
      rest.remove_suffix(kTempFileNameSuffixWithDot.size());
      is_temp_file = true;
    }
    if (!ConsumeDecimalNumber(&rest, &ts_suffix)) {
      return false;
    }
    *number = ts_suffix;
    *type = is_temp_file ? kTempFile : kOptionsFile;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    bool archive_dir_found = false;
    if (rest.starts_with(kArchivalDirName)) {
      if (rest.size() <= kArchivalDirName.size()) {
        return false;
      }
      rest.remove_prefix(kArchivalDirName.size() +
                         1);  // Add 1 to remove / also
      if (log_type) {
        *log_type = kArchivedLogFile;
      }
      archive_dir_found = true;
    }
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (rest.size() <= 1 || rest[0] != '.') {
      return false;
    }
    rest.remove_prefix(1);

    Slice suffix = rest;
    if (suffix == Slice("log")) {
      *type = kWalFile;
      if (log_type && !archive_dir_found) {
        *log_type = kAliveLogFile;
      }
    } else if (archive_dir_found) {
      return false;  // Archive dir can contain only log files
    } else if (suffix == Slice(kRocksDbTFileExt) ||
               suffix == Slice(kLevelDbTFileExt)) {
      *type = kTableFile;
    } else if (suffix == Slice(kRocksDBBlobFileExt)) {
      *type = kBlobFile;
    } else if (suffix == Slice(kTempFileNameSuffix)) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

IOStatus SetCurrentFile(FileSystem* fs, const std::string& dbname,
                        uint64_t descriptor_number,
                        FSDirectory* dir_contains_current_file) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1);
  std::string tmp = TempFileName(dbname, descriptor_number);
  IOStatus s = WriteStringToFile(fs, contents.ToString() + "\n", tmp, true);
  TEST_SYNC_POINT_CALLBACK("SetCurrentFile:BeforeRename", &s);
  if (s.ok()) {
    TEST_KILL_RANDOM_WITH_WEIGHT("SetCurrentFile:0", REDUCE_ODDS2);
    s = fs->RenameFile(tmp, CurrentFileName(dbname), IOOptions(), nullptr);
    TEST_KILL_RANDOM_WITH_WEIGHT("SetCurrentFile:1", REDUCE_ODDS2);
    TEST_SYNC_POINT_CALLBACK("SetCurrentFile:AfterRename", &s);
  }
  if (s.ok()) {
    if (dir_contains_current_file != nullptr) {
      s = dir_contains_current_file->FsyncWithDirOptions(
          IOOptions(), nullptr, DirFsyncOptions(CurrentFileName(dbname)));
    }
  } else {
    fs->DeleteFile(tmp, IOOptions(), nullptr)
        .PermitUncheckedError();  // NOTE: PermitUncheckedError is acceptable
                                  // here as we are already handling an error
                                  // case, and this is just a best-attempt
                                  // effort at some cleanup
  }
  return s;
}

Status SetIdentityFile(Env* env, const std::string& dbname,
                       const std::string& db_id) {
  std::string id;
  if (db_id.empty()) {
    id = env->GenerateUniqueId();
  } else {
    id = db_id;
  }
  assert(!id.empty());
  // Reserve the filename dbname/000000.dbtmp for the temporary identity file
  std::string tmp = TempFileName(dbname, 0);
  std::string identify_file_name = IdentityFileName(dbname);
  Status s = WriteStringToFile(env, id, tmp, true);
  if (s.ok()) {
    s = env->RenameFile(tmp, identify_file_name);
  }
  std::unique_ptr<FSDirectory> dir_obj;
  if (s.ok()) {
    s = env->GetFileSystem()->NewDirectory(dbname, IOOptions(), &dir_obj,
                                           nullptr);
  }
  if (s.ok()) {
    s = dir_obj->FsyncWithDirOptions(IOOptions(), nullptr,
                                     DirFsyncOptions(identify_file_name));
  }

  // The default Close() could return "NotSupported" and we bypass it
  // if it is not impelmented. Detailed explanations can be found in
  // db/db_impl/db_impl.h
  if (s.ok()) {
    Status temp_s = dir_obj->Close(IOOptions(), nullptr);
    if (!temp_s.ok()) {
      if (temp_s.IsNotSupported()) {
        temp_s.PermitUncheckedError();
      } else {
        s = temp_s;
      }
    }
  }
  if (!s.ok()) {
    env->DeleteFile(tmp).PermitUncheckedError();
  }
  return s;
}

IOStatus SyncManifest(const ImmutableDBOptions* db_options,
                      WritableFileWriter* file) {
  TEST_KILL_RANDOM_WITH_WEIGHT("SyncManifest:0", REDUCE_ODDS2);
  StopWatch sw(db_options->clock, db_options->stats, MANIFEST_FILE_SYNC_MICROS);
  return file->Sync(db_options->use_fsync);
}

Status GetInfoLogFiles(const std::shared_ptr<FileSystem>& fs,
                       const std::string& db_log_dir, const std::string& dbname,
                       std::string* parent_dir,
                       std::vector<std::string>* info_log_list) {
  assert(parent_dir != nullptr);
  assert(info_log_list != nullptr);
  uint64_t number = 0;
  FileType type = kWalFile;

  if (!db_log_dir.empty()) {
    *parent_dir = db_log_dir;
  } else {
    *parent_dir = dbname;
  }

  InfoLogPrefix info_log_prefix(!db_log_dir.empty(), dbname);

  std::vector<std::string> file_names;
  Status s = fs->GetChildren(*parent_dir, IOOptions(), &file_names, nullptr);

  if (!s.ok()) {
    return s;
  }

  for (auto& f : file_names) {
    if (ParseFileName(f, &number, info_log_prefix.prefix, &type) &&
        (type == kInfoLogFile)) {
      info_log_list->push_back(f);
    }
  }
  return Status::OK();
}

std::string NormalizePath(const std::string& path) {
  std::string dst;

  if (path.length() > 2 && path[0] == kFilePathSeparator &&
      path[1] == kFilePathSeparator) {  // Handle UNC names
    dst.append(2, kFilePathSeparator);
  }

  for (auto c : path) {
    if (!dst.empty() && (c == kFilePathSeparator || c == '/') &&
        (dst.back() == kFilePathSeparator || dst.back() == '/')) {
      continue;
    }
    dst.push_back(c);
  }
  return dst;
}

}  // namespace ROCKSDB_NAMESPACE
