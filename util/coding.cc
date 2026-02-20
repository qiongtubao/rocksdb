//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

#include <algorithm>

#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

// conversion' conversion from 'type1' to 'type2', possible loss of data
// ============================================================================
// EncodeVarint32(char* dst, uint32_t v)
//
// 功能描述:
//   将一个 32 位无符号整数编码为 Varint 格式，并写入目标缓冲区。
//   这是 Varint 编码的核心实现，使用 if-else 链优化编码速度。
//
// Varint 编码规则（回顾）:
//   - 每字节的最高位（MSB）是继续位：
//     * MSB = 1: 后面还有字节
//     * MSB = 0: 这是最后一个字节
//   - 低 7 位存储实际数据
//   - 使用小端序（低字节在前）
//
// 参数说明:
//   dst (char*):
//     - 目标缓冲区指针，编码后的数据将写入此处
//     - 必须有足够的存储空间（至少 5 字节）
//     - 调用方负责确保缓冲区有效且有足够空间
//     - REQUIRES: dst 指向的缓冲区至少有 5 字节可用空间
//
//   v (uint32_t):
//     - 要编码的 32 位无符号整数
//     - 取值范围: 0 ≤ v ≤ 2^32 - 1
//
// 返回值:
//   char*:
//     - 指向编码后的最后一个字节的下一个位置
//     - 编码后的数据范围: [dst, 返回值)
//     - 编码长度 = 返回值 - dst（1 到 5 字节）
//
// 编码长度（按数值范围）:
//   - 0 ≤ v < 128         (2^7):   1 字节  (0b0xxxxxxx)
//   - 128 ≤ v < 16384     (2^14):  2 字节  (0b1xxxxxxx 0b0yyyyyyy)
//   - 16384 ≤ v < 2097152 (2^21):  3 字节  (0b1xxxxxxx 0b1yyyyyyy 0b0zzzzzzz)
//   - 2097152 ≤ v < 268435456 (2^28): 4 字节  (0b1xxxxxxx ... 0b0wwwwwww)
//   - 268435456 ≤ v < 2^32:         5 字节  (0b1xxxxxxx ... 0b0vvvvvvv)
//
// 实现细节:
//   1. 转换为无符号字符指针:
//      - unsigned char* ptr = reinterpret_cast<unsigned char*>(dst)
//      - 确保按无符号字节操作，避免符号扩展问题
//
//   2. 继续位常量:
//      - static const int B = 128 (0x80)
//      - 表示字节的最高位（继续位）
//      - v | B: 设置继续位为 1（后面还有字节）
//
//   3. if-else 链优化:
//      - 按数值范围分层判断，避免不必要的循环
//      - 每个分支处理特定的长度范围
//      - 小数值（最常见）的路径最短，性能最优
//
//   4. 位操作详解:
//      - v: 取低 7 位（bit 0-6）
//      - v | B: 设置 MSB 为 1（继续位）
//      - v >> 7: 右移 7 位（取 bit 7-13）
//      - v >> 14: 右移 14 位（取 bit 14-20）
//      - v >> 21: 右移 21 位（取 bit 21-27）
//      - v >> 28: 右移 28 位（取 bit 28-31，最多 4 位）
//
// 编码过程详解（以 v = 300 为例）:
//
//   v = 300 = 0b00000001 00101100
//
//   判断: v < 128? No (300 >= 128)
//         v < 16384? Yes (300 < 16384)
//
//   进入 2 字节分支:
//     1. *(ptr++) = v | B;
//        - v = 300 = 0b00101100
//        - v | B = 0b10101100 = 0xAC
//        - 写入字节: 0xAC (继续位=1, 数据=44)
//        - ptr 指向下一个位置
//
//     2. *(ptr++) = v >> 7;
//        - v >> 7 = 300 >> 7 = 2 = 0b00000010
//        - 写入字节: 0x02 (继续位=0, 数据=2)
//        - ptr 指向下一个位置
//
//   最终编码: [0xAC, 0x02] (2 字节)
//   返回: 指向最后一个字节后的位置
//
// 编码过程详解（以 v = 16384 为例）:
//
//   v = 16384 = 0x4000 = 0b00000000 00000000 01000000 00000000
//
//   判断: v < 128? No
//         v < 16384? No (16384 >= 16384)
//         v < 2097152? Yes (16384 < 2097152)
//
//   进入 3 字节分支:
//     1. *(ptr++) = v | B;
//        - v = 0x4000 = 0b00000000
//        - v | B = 0x80
//        - 写入字节: 0x80 (继续位=1, 数据=0)
//
//     2. *(ptr++) = (v >> 7) | B;
//        - v >> 7 = 16384 >> 7 = 128 = 0b10000000
//        - (v >> 7) | B = 0b10000000 | 0b10000000 = 0x80
//        - 写入字节: 0x80 (继续位=1, 数据=0)
//
//     3. *(ptr++) = v >> 14;
//        - v >> 14 = 16384 >> 14 = 1 = 0b00000001
//        - 写入字节: 0x01 (继续位=0, 数据=1)
//
//   最终编码: [0x80, 0x80, 0x01] (3 字节)
//   解码验证: 0 + (0 << 7) + (1 << 14) = 0 + 0 + 16384 = 16384 ✓
//
// 性能优化:
//   1. if-else 链 vs 循环:
//      - if-else 链: 分支预测友好，小数值路径短
//      - 循环: 代码紧凑，但分支预测可能不友好
//      - 实测表明 if-else 链在 RocksDB 场景下更快
//
//   2. 编译器优化:
//      - 内联展开（如果调用点可知）
//      - 常量传播（B = 128）
//      - 死代码消除（某些编译器）
//
//   3. 缓存友好:
//      - 连续写入，缓存命中率高
//      - 最多 5 字节，完全在缓存行内
//
// 线程安全性:
//   - 纯函数，无副作用
//   - 不修改共享状态
//   - 多线程同时调用是安全的（如果 dst 不重叠）
//
// 错误处理:
//   - 假设 dst 有足够空间（至少 5 字节）
//   - 如果空间不足，会导致缓冲区溢出（未定义行为）
//   - 调用方负责确保缓冲区足够大
//
// 相关函数:
//   - PutVarint32(std::string* dst, uint32_t v): 高层接口（使用此函数）
//   - GetVarint32(Slice* input, uint32_t* value): 解码 Varint32
//   - EncodeVarint64(char* dst, uint64_t v): 编码 64 位变长整数
//
// 注意事项:
//   1. 缓冲区大小:
//      - 必须至少分配 5 字节
//      - 小于 5 字节会导致缓冲区溢出
//
//   2. 无符号字符操作:
//      - 使用 unsigned char 避免符号扩展
//      - 确保位操作的正确性
//
//   3. 返回值使用:
//      - 返回指针可用于连续编码多个值
//      - 示例: ptr = EncodeVarint32(ptr, v1);
//              ptr = EncodeVarint32(ptr, v2);
//
//   4. 与 PutVarint32 的关系:
//      - 此函数是底层实现，直接操作字符缓冲区
//      - PutVarint32 是高层接口，操作 std::string
//      - PutVarint32 内部调用此函数
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4244)
#endif
char* EncodeVarint32(char* dst, uint32_t v) {
  // 将 char* 转换为 unsigned char*，确保按无符号字节操作
  // 这样可以避免符号扩展问题，确保位操作的正确性
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);

  // 继续位常量 B = 128 (0x80)
  // 用于设置字节的最高位（MSB）为 1，表示后面还有字节
  static const int B = 128;

  // 分支 1: v < 128 (2^7)，只需要 1 字节
  // 直接写入 v，无需设置继续位（MSB 自动为 0）
  // 这是最常见的情况（大多数数值都很小），放在最前面优化性能
  if (v < (1 << 7)) {
    *(ptr++) = v;
  }
  // 分支 2: 128 ≤ v < 16384 (2^14)，需要 2 字节
  // 第 1 字节: 低 7 位 + 继续位 (MSB=1)
  // 第 2 字节: 高 7 位，无需继续位 (MSB=0)
  else if (v < (1 << 14)) {
    *(ptr++) = v | B;        // 写入低 7 位，设置继续位
    *(ptr++) = v >> 7;       // 写入高 7 位
  }
  // 分支 3: 16384 ≤ v < 2097152 (2^21)，需要 3 字节
  // 第 1 字节: bit 0-6 + 继续位 (MSB=1)
  // 第 2 字节: bit 7-13 + 继续位 (MSB=1)
  // 第 3 字节: bit 14-20，无需继续位 (MSB=0)
  else if (v < (1 << 21)) {
    *(ptr++) = v | B;         // 写入 bit 0-6，设置继续位
    *(ptr++) = (v >> 7) | B;   // 写入 bit 7-13，设置继续位
    *(ptr++) = v >> 14;        // 写入 bit 14-20
  }
  // 分支 4: 2097152 ≤ v < 268435456 (2^28)，需要 4 字节
  // 前 3 个字节的 MSB=1，最后一个字节的 MSB=0
  else if (v < (1 << 28)) {
    *(ptr++) = v | B;         // 写入 bit 0-6，设置继续位
    *(ptr++) = (v >> 7) | B;   // 写入 bit 7-13，设置继续位
    *(ptr++) = (v >> 14) | B;  // 写入 bit 14-20，设置继续位
    *(ptr++) = v >> 21;        // 写入 bit 21-27
  }
  // 分支 5: 268435456 ≤ v < 2^32，需要 5 字节（最大长度）
  // 前 4 个字节的 MSB=1，最后一个字节的 MSB=0
  // 注意: bit 28-31 最多只有 4 位，所以最后一个字节的低 4 位为 0
  else {
    *(ptr++) = v | B;         // 写入 bit 0-6，设置继续位
    *(ptr++) = (v >> 7) | B;   // 写入 bit 7-13，设置继续位
    *(ptr++) = (v >> 14) | B;  // 写入 bit 14-20，设置继续位
    *(ptr++) = (v >> 21) | B;  // 写入 bit 21-27，设置继续位
    *(ptr++) = v >> 28;        // 写入 bit 28-31
  }

  // 返回指向编码后的最后一个字节的下一个位置
  // 编码长度 = 返回值 - dst（1 到 5 字节）
  // 转换回 char* 类型，与输入参数类型一致
  return reinterpret_cast<char*>(ptr);
}
#if defined(_MSC_VER)
#pragma warning(pop)
#endif

const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
