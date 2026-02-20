//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Encoding independent of machine byte order:
// * Fixed-length numbers are encoded with least-significant byte first
//   (little endian, native order on Intel and others)
// * In addition we support variable length "varint" encoding
// * Strings are encoded prefixed by their length in varint format
//
// Some related functions are provided in coding_lean.h

#pragma once
#include <algorithm>
#include <string>

#include "port/port.h"
#include "rocksdb/slice.h"
#include "util/coding_lean.h"

// Some processors does not allow unaligned access to memory
#if defined(__sparc)
#define PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED
#endif

namespace ROCKSDB_NAMESPACE {

// The maximum length of a varint in bytes for 64-bit.
const uint32_t kMaxVarint64Length = 10;

// Standard Put... routines append to a string
extern void PutFixed16(std::string* dst, uint16_t value);
extern void PutFixed32(std::string* dst, uint32_t value);
extern void PutFixed64(std::string* dst, uint64_t value);
extern void PutVarint32(std::string* dst, uint32_t value);
extern void PutVarint32Varint32(std::string* dst, uint32_t value1,
                                uint32_t value2);
extern void PutVarint32Varint32Varint32(std::string* dst, uint32_t value1,
                                        uint32_t value2, uint32_t value3);
extern void PutVarint64(std::string* dst, uint64_t value);
extern void PutVarint64Varint64(std::string* dst, uint64_t value1,
                                uint64_t value2);
extern void PutVarint32Varint64(std::string* dst, uint32_t value1,
                                uint64_t value2);
extern void PutVarint32Varint32Varint64(std::string* dst, uint32_t value1,
                                        uint32_t value2, uint64_t value3);
extern void PutLengthPrefixedSlice(std::string* dst, const Slice& value);
extern void PutLengthPrefixedSliceParts(std::string* dst,
                                        const SliceParts& slice_parts);
extern void PutLengthPrefixedSlicePartsWithPadding(
    std::string* dst, const SliceParts& slice_parts, size_t pad_sz);

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.
extern bool GetFixed64(Slice* input, uint64_t* value);
extern bool GetFixed32(Slice* input, uint32_t* value);
extern bool GetFixed16(Slice* input, uint16_t* value);
extern bool GetVarint32(Slice* input, uint32_t* value);
extern bool GetVarint64(Slice* input, uint64_t* value);
extern bool GetVarsignedint64(Slice* input, int64_t* value);
extern bool GetLengthPrefixedSlice(Slice* input, Slice* result);
// This function assumes data is well-formed.
extern Slice GetLengthPrefixedSlice(const char* data);

extern Slice GetSliceUntil(Slice* slice, char delimiter);

// Borrowed from
// https://github.com/facebook/fbthrift/blob/449a5f77f9f9bae72c9eb5e78093247eef185c04/thrift/lib/cpp/util/VarintUtils-inl.h#L202-L208
constexpr inline uint64_t i64ToZigzag(const int64_t l) {
  return (static_cast<uint64_t>(l) << 1) ^ static_cast<uint64_t>(l >> 63);
}
inline int64_t zigzagToI64(uint64_t n) {
  return (n >> 1) ^ -static_cast<int64_t>(n & 1);
}

// Pointer-based variants of GetVarint...  These either store a value
// in *v and return a pointer just past the parsed value, or return
// nullptr on error.  These routines only look at bytes in the range
// [p..limit-1]
extern const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* v);
extern const char* GetVarint64Ptr(const char* p, const char* limit,
                                  uint64_t* v);
inline const char* GetVarsignedint64Ptr(const char* p, const char* limit,
                                        int64_t* value) {
  uint64_t u = 0;
  const char* ret = GetVarint64Ptr(p, limit, &u);
  *value = zigzagToI64(u);
  return ret;
}

// Returns the length of the varint32 or varint64 encoding of "v"
extern int VarintLength(uint64_t v);

// Lower-level versions of Put... that write directly into a character buffer
// and return a pointer just past the last byte written.
// REQUIRES: dst has enough space for the value being written
extern char* EncodeVarint32(char* dst, uint32_t value);
extern char* EncodeVarint64(char* dst, uint64_t value);

// Internal routine for use by fallback path of GetVarint32Ptr
extern const char* GetVarint32PtrFallback(const char* p, const char* limit,
                                          uint32_t* value);
inline const char* GetVarint32Ptr(const char* p, const char* limit,
                                  uint32_t* value) {
  if (p < limit) {
    uint32_t result = *(reinterpret_cast<const unsigned char*>(p));
    if ((result & 128) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

// Pull the last 8 bits and cast it to a character
inline void PutFixed16(std::string* dst, uint16_t value) {
  if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed16(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

inline void PutFixed32(std::string* dst, uint32_t value) {
  if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed32(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

inline void PutFixed64(std::string* dst, uint64_t value) {
  if (port::kLittleEndian) {
    dst->append(const_cast<const char*>(reinterpret_cast<char*>(&value)),
                sizeof(value));
  } else {
    char buf[sizeof(value)];
    EncodeFixed64(buf, value);
    dst->append(buf, sizeof(buf));
  }
}

// ============================================================================
// PutVarint32(std::string* dst, uint32_t v)
//
// 功能描述:
//   将一个 32 位无符号整数使用 Varint（Variable-length Integer）编码格式
//   追加到目标字符串的末尾。Varint 是一种变长整数编码，能够高效地压缩
//   小数值的存储空间。
//
// 编码原理（Varint 格式）:
//   Varint 使用每字节的最高位（MSB）作为"继续位"（continuation bit）：
//   - MSB = 1 (0x80): 表示后面还有字节（非最后一个字节）
//   - MSB = 0 (0x00): 表示这是最后一个字节
//   - 低 7 位存储实际数据（使用小端序）
//
//   编码规则：
//   - 将 32 位整数按 7 位一组分割（每字节存储 7 位数据）
//   - 每组存储在一个字节中，低 7 位为数据，最高位为继续位
//   - 最后一个字节的最高位设置为 0，其他字节的最高位设置为 1
//   - 从低字节到高字节依次存储（小端序）
//
// 编码长度：
//   - 0 ≤ v < 128         (2^7):   1 字节
//   - 128 ≤ v < 16384     (2^14):  2 字节
//   - 16384 ≤ v < 2097152 (2^21):  3 字节
//   - 2097152 ≤ v < 268435456 (2^28): 4 字节
//   - 268435456 ≤ v < 2^32:         5 字节
//
// 编码示例：
//
//   示例 1: v = 42 (0x2A)
//   二进制: 00101010
//   编码后: 0x2A (1 字节)
//   解释: 42 < 128，只需要 1 字节，MSB = 0
//
//   示例 2: v = 300 (0x12C)
//   二进制: 00000001 00101100
//   分组: [00101100] [00000001] (7位一组)
//   编码后: [0xAC] [0x02] (2 字节)
//   解释:
//     - 第一字节: 0b10101100 = 0xAC (数据: 0b0101100=44, MSB=1)
//     - 第二字节: 0b00000010 = 0x02 (数据: 0b0000010=2, MSB=0)
//     - 解码: 44 + (2 << 7) = 44 + 256 = 300
//
//   示例 3: v = 16384 (0x4000)
//   二进制: 00000000 00000000 01000000 00000000
//   分组: [0000000] [0000000] [1000000] [0000010] (7位一组)
//   编码后: [0x80] [0x80] [0x80] [0x02] (4 字节)
//   解释: 需要 4 字节，前三个字节的 MSB=1，最后一个字节的 MSB=0
//
//   示例 4: v = 268435456 (0x10000000)
//   二进制: 00010000 00000000 00000000 00000000
//   分组: [0000000] [0000000] [0000000] [0000000] [0000100] (7位一组)
//   编码后: [0x80] [0x80] [0x80] [0x80] [0x10] (5 字节)
//   解释: 最大值需要 5 字节
//
// 参数说明:
//   dst (std::string*):
//     - 目标字符串指针，编码后的数据将追加到该字符串末尾
//     - 不能为 nullptr，调用方负责确保 dst 有效
//     - dst 的容量会自动扩展（如果需要）
//     - dst 的内容不会被修改，只会在末尾追加新数据
//
//   v (uint32_t):
//     - 要编码的 32 位无符号整数
//     - 取值范围: 0 ≤ v ≤ 2^32 - 1 (0 到 4294967295)
//     - 任何 32 位无符号整数都可以正确编码
//
// 实现细节:
//   1. 分配临时缓冲区（栈上）:
//      - char buf[5]: 5 字节缓冲区（uint32_t 的最大编码长度）
//      - 使用栈内存，避免堆分配（高效）
//      - 缓冲区大小固定，无需动态分配
//
//   2. 调用 EncodeVarint32 进行编码:
//      - EncodeVarint32(buf, v): 将 v 编码到 buf 中
//      - 返回值 ptr 指向编码后的最后一个字节的下一个位置
//      - 编码长度 = ptr - buf（1 到 5 字节）
//
//   3. 追加到目标字符串:
//      - dst->append(buf, ptr - buf): 将编码后的数据追加到 dst
//      - 使用指针差计算实际长度（高效）
//      - 避免了不必要的长度计算
//
// 时间复杂度:
//   - 时间复杂度: O(1)（最多 5 次操作）
//   - 空间复杂度: O(1)（5 字节栈缓冲区）
//
// 线程安全性:
//   - 如果多个线程同时调用此函数并操作同一个 dst，需要外部同步
//   - 如果操作不同的 dst，则是线程安全的
//   - dst 本身的线程安全性由 std::string 保证
//
// 错误处理:
//   - 不会抛出异常（假设内存分配成功）
//   - 如果 dst 为 nullptr，会导致未定义行为（段错误）
//   - 内存分配失败会抛出 std::bad_alloc（由 std::string 处理）
//
// 使用示例:
//
//   1. 基本使用:
//   std::string data;
//   PutVarint32(&data, 42);        // data = [0x2A]
//   PutVarint32(&data, 300);       // data = [0x2A, 0xAC, 0x02]
//   PutVarint32(&data, 16384);     // data = [0x2A, 0xAC, 0x02, 0x80, 0x80, 0x80, 0x02]
//
//   2. 编码字符串长度（WriteBatch 中使用）:
//   std::string key = "hello";
//   std::string encoded;
//   PutVarint32(&encoded, key.size());  // 先编码长度
//   encoded.append(key);                // 再追加数据
//   // encoded = [0x05] [h,e,l,l,o]
//
//   3. 在 WriteBatch 中的使用:
//   std::string rep;
//   PutVarint32(&rep, 5);       // key 长度
//   rep.append("key1", 4);     // key 数据
//   PutVarint32(&rep, 6);       // value 长度
//   rep.append("value1", 6);   // value 数据
//
//   4. 解码（使用 GetVarint32）:
//   std::string encoded;
//   PutVarint32(&encoded, 300);
//   Slice input(encoded);
//   uint32_t decoded;
//   GetVarint32(&input, &decoded);  // decoded = 300
//
// 性能考虑:
//   1. 空间效率:
//      - 小数值（<128）只需 1 字节，比固定 4 字节节省 75% 空间
//      - 适合存储大多数实际使用场景中的小整数
//      - 如字符串长度、计数器等通常较小的值
//
//   2. 性能优化:
//      - 使用栈缓冲区，避免堆分配
//      - 指针差计算长度，避免额外的遍历
//      - EncodeVarint32 是内联展开的（优化性能）
//
//   3. 适用场景:
//      - 字符串长度编码（最常见）
//      - 计数器编码
//      - 列族 ID 编码（通常较小）
//      - 任何可能较小的整数编码
//
// 注意事项:
//   1. 与固定长度的区别:
//      - Varint 是变长编码，节省空间但增加编解码开销
//      - 固定长度（如 PutFixed32）是定长编码，速度快但不节省空间
//      - 根据使用场景选择：小数值多用 Varint，大数值多用固定长度
//
//   2. 字节序无关:
//      - Varint 编码与机器字节序无关（跨平台兼容）
//      - 每个字节独立存储 7 位数据
//      - 不需要考虑大小端序问题
//
//   3. 最大编码长度:
//      - uint32_t 的最大编码长度为 5 字节
//      - 如果数值可能很大（接近 2^32），考虑使用固定长度编码
//
//   4. 与 PutVarint64 的区别:
//      - PutVarint32 用于 32 位整数（最大 5 字节）
//      - PutVarint64 用于 64 位整数（最大 10 字节）
//      - 编码格式相同，只是最大长度不同
//
// 相关函数:
//   - GetVarint32(Slice* input, uint32_t* value): 解码 Varint32
//   - EncodeVarint32(char* dst, uint32_t v): 底层编码函数
//   - PutVarint64(std::string* dst, uint64_t v): 编码 64 位变长整数
//   - PutFixed32(std::string* dst, uint32_t v): 编码 32 位固定长度整数
//   - VarintLength(uint64_t v): 计算 Varint 编码长度
//
// 内存布局示例:
//
//   编码 v = 300 后的内存布局:
//   +-----------------+-----------------+
//   |   Byte 0        |   Byte 1        |
//   |  1 0 1 0 1 1 0 0 |  0 0 0 0 0 0 1 0 |
//   |  ^MSB(1) 数据   |  ^MSB(0) 数据   |
//   |  =44            |  =2             |
//   +-----------------+-----------------+
//   解码: 44 + (2 << 7) = 44 + 256 = 300
// ============================================================================
inline void PutVarint32(std::string* dst, uint32_t v) {
  // 分配 5 字节栈缓冲区（uint32_t 的最大 Varint 编码长度）
  // 栈内存分配速度快，无需堆分配
  char buf[5];

  // 调用 EncodeVarint32 将 v 编码到 buf 中
  // 返回 ptr 指向编码后的最后一个字节的下一个位置
  // 编码后的数据范围: [buf, ptr)
  // 实际编码长度 = ptr - buf（1 到 5 字节）
  char* ptr = EncodeVarint32(buf, v);

  // 将编码后的数据从 buf 追加到 dst 的末尾
  // 使用指针差计算实际长度（高效，无需额外遍历）
  // dst->append() 会自动扩展 dst 的容量（如果需要）
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32(std::string* dst, uint32_t v1, uint32_t v2) {
  char buf[10];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint32(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint32_t v3) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint32(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline char* EncodeVarint64(char* dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  char buf[kMaxVarint64Length];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarsignedint64(std::string* dst, int64_t v) {
  char buf[kMaxVarint64Length];
  // Using Zigzag format to convert signed to unsigned
  char* ptr = EncodeVarint64(buf, i64ToZigzag(v));
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint64Varint64(std::string* dst, uint64_t v1, uint64_t v2) {
  char buf[20];
  char* ptr = EncodeVarint64(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint64(std::string* dst, uint32_t v1, uint64_t v2) {
  char buf[15];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint64(ptr, v2);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutVarint32Varint32Varint64(std::string* dst, uint32_t v1,
                                        uint32_t v2, uint64_t v3) {
  char buf[20];
  char* ptr = EncodeVarint32(buf, v1);
  ptr = EncodeVarint32(ptr, v2);
  ptr = EncodeVarint64(ptr, v3);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

inline void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, static_cast<uint32_t>(value.size()));
  dst->append(value.data(), value.size());
}

inline void PutLengthPrefixedSliceParts(std::string* dst, size_t total_bytes,
                                        const SliceParts& slice_parts) {
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    total_bytes += slice_parts.parts[i].size();
  }
  PutVarint32(dst, static_cast<uint32_t>(total_bytes));
  for (int i = 0; i < slice_parts.num_parts; ++i) {
    dst->append(slice_parts.parts[i].data(), slice_parts.parts[i].size());
  }
}

inline void PutLengthPrefixedSliceParts(std::string* dst,
                                        const SliceParts& slice_parts) {
  PutLengthPrefixedSliceParts(dst, /*total_bytes=*/0, slice_parts);
}

inline void PutLengthPrefixedSlicePartsWithPadding(
    std::string* dst, const SliceParts& slice_parts, size_t pad_sz) {
  PutLengthPrefixedSliceParts(dst, /*total_bytes=*/pad_sz, slice_parts);
  dst->append(pad_sz, '\0');
}

inline int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

inline bool GetFixed64(Slice* input, uint64_t* value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  input->remove_prefix(sizeof(uint64_t));
  return true;
}

inline bool GetFixed32(Slice* input, uint32_t* value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  input->remove_prefix(sizeof(uint32_t));
  return true;
}

inline bool GetFixed16(Slice* input, uint16_t* value) {
  if (input->size() < sizeof(uint16_t)) {
    return false;
  }
  *value = DecodeFixed16(input->data());
  input->remove_prefix(sizeof(uint16_t));
  return true;
}

inline bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetVarsignedint64(Slice* input, int64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarsignedint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}

inline bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len = 0;
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

inline Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len = 0;
  // +5: we assume "data" is not corrupted
  // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
  auto p = GetVarint32Ptr(data, data + 5 /* limit */, &len);
  return Slice(p, len);
}

inline Slice GetSliceUntil(Slice* slice, char delimiter) {
  uint32_t len = 0;
  for (len = 0; len < slice->size() && slice->data()[len] != delimiter; ++len) {
    // nothing
  }

  Slice ret(slice->data(), len);
  slice->remove_prefix(len + ((len < slice->size()) ? 1 : 0));
  return ret;
}

template <class T>
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
inline void
PutUnaligned(T* memory, const T& value) {
#if defined(PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED)
  char* nonAlignedMemory = reinterpret_cast<char*>(memory);
  memcpy(nonAlignedMemory, reinterpret_cast<const char*>(&value), sizeof(T));
#else
  *memory = value;
#endif
}

template <class T>
#ifdef ROCKSDB_UBSAN_RUN
#if defined(__clang__)
__attribute__((__no_sanitize__("alignment")))
#elif defined(__GNUC__)
__attribute__((__no_sanitize_undefined__))
#endif
#endif
inline void
GetUnaligned(const T* memory, T* value) {
#if defined(PLATFORM_UNALIGNED_ACCESS_NOT_ALLOWED)
  char* nonAlignedMemory = reinterpret_cast<char*>(value);
  memcpy(nonAlignedMemory, reinterpret_cast<const char*>(memory), sizeof(T));
#else
  *value = *memory;
#endif
}

}  // namespace ROCKSDB_NAMESPACE
