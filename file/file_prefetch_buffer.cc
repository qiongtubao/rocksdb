//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/file_prefetch_buffer.h"

#include <algorithm>
#include <cassert>

#include "file/random_access_file_reader.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {

void FilePrefetchBuffer::CalculateOffsetAndLen(size_t alignment,
                                               uint64_t offset,
                                               size_t roundup_len,
                                               uint32_t index, bool refit_tail,
                                               uint64_t& chunk_len) {
  uint64_t chunk_offset_in_buffer = 0;
  bool copy_data_to_new_buffer = false;
  // Check if requested bytes are in the existing buffer_.
  // If only a few bytes exist -- reuse them & read only what is really needed.
  //     This is typically the case of incremental reading of data.
  // If no bytes exist in buffer -- full pread.
  if (DoesBufferContainData(index) && IsOffsetInBuffer(offset, index)) { //buffer[index]有数据且offset在buffer[index]块范围内
    // Only a few requested bytes are in the buffer. memmove those chunk of
    // bytes to the beginning, and memcpy them back into the new buffer if a
    // new buffer is created.
    chunk_offset_in_buffer = Rounddown(
        static_cast<size_t>(offset - bufs_[index].offset_), alignment); //表示从 buffer 的哪个位置开始保留
    chunk_len = static_cast<uint64_t>(bufs_[index].buffer_.CurrentSize()) -
                chunk_offset_in_buffer;                                 //保存长度
    assert(chunk_offset_in_buffer % alignment == 0);
    assert(chunk_len % alignment == 0);
    assert(chunk_offset_in_buffer + chunk_len <=
           bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize());
    if (chunk_len > 0) {
      copy_data_to_new_buffer = true;
    } else {
      // this reset is not necessary, but just to be safe.
      chunk_offset_in_buffer = 0;
    }
  }

  // Create a new buffer only if current capacity is not sufficient, and memcopy
  // bytes from old buffer if needed (i.e., if chunk_len is greater than 0).
  if (bufs_[index].buffer_.Capacity() < roundup_len) { //buffer容量不足
    bufs_[index].buffer_.Alignment(alignment);//设置对齐4k
    bufs_[index].buffer_.AllocateNewBuffer(
        static_cast<size_t>(roundup_len), copy_data_to_new_buffer,
        chunk_offset_in_buffer, static_cast<size_t>(chunk_len)); //重新申请内存 且拷贝数据
  } else if (chunk_len > 0 && refit_tail) { //容量足够, 将数据往前移
    // New buffer not needed. But memmove bytes from tail to the beginning since
    // chunk_len is greater than 0.
    bufs_[index].buffer_.RefitTail(static_cast<size_t>(chunk_offset_in_buffer),
                                   static_cast<size_t>(chunk_len));
  } else if (chunk_len > 0) { //不支持数据迁移, 强制申请buffer
    // For async prefetching, it doesn't call RefitTail with chunk_len > 0.
    // Allocate new buffer if needed because aligned buffer calculate remaining
    // buffer as capacity_ - cursize_ which might not be the case in this as we
    // are not refitting.
    // TODO akanksha: Update the condition when asynchronous prefetching is
    // stable.
    bufs_[index].buffer_.Alignment(alignment);
    bufs_[index].buffer_.AllocateNewBuffer(
        static_cast<size_t>(roundup_len), copy_data_to_new_buffer,
        chunk_offset_in_buffer, static_cast<size_t>(chunk_len));
  }
}

Status FilePrefetchBuffer::Read(const IOOptions& opts,
                                RandomAccessFileReader* reader,
                                Env::IOPriority rate_limiter_priority,
                                uint64_t read_len, uint64_t chunk_len,
                                uint64_t rounddown_start, uint32_t index) {
  Slice result;
  Status s = reader->Read(opts, rounddown_start + chunk_len, read_len, &result,
                          bufs_[index].buffer_.BufferStart() + chunk_len,
                          /*aligned_buf=*/nullptr, rate_limiter_priority);
#ifndef NDEBUG
  if (result.size() < read_len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
#endif
  if (!s.ok()) {
    return s;
  }

  // Update the buffer offset and size.
  bufs_[index].offset_ = rounddown_start;
  bufs_[index].buffer_.Size(static_cast<size_t>(chunk_len) + result.size());
  return s;
}

Status FilePrefetchBuffer::ReadAsync(const IOOptions& opts,
                                     RandomAccessFileReader* reader,
                                     uint64_t read_len,
                                     uint64_t rounddown_start, uint32_t index) {
  TEST_SYNC_POINT("FilePrefetchBuffer::ReadAsync");
  // callback for async read request.
  auto fp = std::bind(&FilePrefetchBuffer::PrefetchAsyncCallback, this,
                      std::placeholders::_1, std::placeholders::_2);
  FSReadRequest req;
  Slice result;
  req.len = read_len;
  req.offset = rounddown_start;
  req.result = result;
  req.scratch = bufs_[index].buffer_.BufferStart();
  bufs_[index].async_req_len_ = req.len;

  Status s =
      reader->ReadAsync(req, opts, fp, &(bufs_[index].pos_),
                        &(bufs_[index].io_handle_), &(bufs_[index].del_fn_),
                        /*aligned_buf=*/nullptr);
  req.status.PermitUncheckedError();
  if (s.ok()) {
    bufs_[index].async_read_in_progress_ = true;
  }
  return s;
}

Status FilePrefetchBuffer::Prefetch(const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t n,
                                    Env::IOPriority rate_limiter_priority) {
  if (!enable_ || reader == nullptr) {
    return Status::OK();
  }
  TEST_SYNC_POINT("FilePrefetchBuffer::Prefetch:Start");

  if (offset + n <= bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    // All requested bytes are already in the curr_ buffer. So no need to Read
    // again.
    return Status::OK();
  }

  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t offset_ = static_cast<size_t>(offset);
  uint64_t rounddown_offset = Rounddown(offset_, alignment);
  uint64_t roundup_end = Roundup(offset_ + n, alignment);
  uint64_t roundup_len = roundup_end - rounddown_offset;
  assert(roundup_len >= alignment);
  assert(roundup_len % alignment == 0);

  uint64_t chunk_len = 0;
  CalculateOffsetAndLen(alignment, offset, roundup_len, curr_,
                        true /*refit_tail*/, chunk_len);
  size_t read_len = static_cast<size_t>(roundup_len - chunk_len);

  Status s = Read(opts, reader, rate_limiter_priority, read_len, chunk_len,
                  rounddown_offset, curr_);
  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && s.ok()) {
    RecordInHistogram(stats_, TABLE_OPEN_PREFETCH_TAIL_READ_BYTES, read_len);
  }
  return s;
}

// Copy data from src to third buffer.
void FilePrefetchBuffer::CopyDataToBuffer(uint32_t src, uint64_t& offset,
                                          size_t& length) {
  if (length == 0) {
    return;
  }
  uint64_t copy_offset = (offset - bufs_[src].offset_);
  size_t copy_len = 0;
  if (IsDataBlockInBuffer(offset, length, src)) { //数据块在buffer[src]内
    // All the bytes are in src.
    copy_len = length;  //全部长度
  } else {
    copy_len = bufs_[src].buffer_.CurrentSize() - copy_offset; //拷贝buffer[src]到结束的长度
  }

  memcpy(bufs_[2].buffer_.BufferStart() + bufs_[2].buffer_.CurrentSize(),
         bufs_[src].buffer_.BufferStart() + copy_offset, copy_len); //追加到buffer3后面

  bufs_[2].buffer_.Size(bufs_[2].buffer_.CurrentSize() + copy_len); //设置buffer3的当前长度

  // Update offset and length.
  offset += copy_len;//offset更新
  length -= copy_len;//需要读取的长度更新

  // length > 0 indicates it has consumed all data from the src buffer and it
  // still needs to read more other buffer.
  if (length > 0) { //buffer[src]数据已经读完 清空buffer
    bufs_[src].buffer_.Clear();
  }
}

// Clear the buffers if it contains outdated data. Outdated data can be
// because previous sequential reads were read from the cache instead of these
// buffer. In that case outdated IOs should be aborted.
void FilePrefetchBuffer::AbortIOIfNeeded(uint64_t offset) {
  uint32_t second = curr_ ^ 1;
  std::vector<void*> handles;
  autovector<uint32_t> buf_pos;
  if (IsBufferOutdatedWithAsyncProgress(offset, curr_)) { //curr_ buffer 是否过期且有异步请求在进行
    handles.emplace_back(bufs_[curr_].io_handle_);  //收集curr_ buffer io_handle_对象
    buf_pos.emplace_back(curr_);
  }
  if (IsBufferOutdatedWithAsyncProgress(offset, second)) {  //second buffer 是否过期且有异步请求在进行
    handles.emplace_back(bufs_[second].io_handle_); //收集second buffer io_handle_对象
    buf_pos.emplace_back(second);
  }
  if (!handles.empty()) { //收集完成后非空 则取消预读的请求
    StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
    Status s = fs_->AbortIO(handles);
    assert(s.ok());
  }

  for (auto& pos : buf_pos) {
    // Release io_handle.
    DestroyAndClearIOHandle(pos); //释放掉io_handle_
  }

  if (bufs_[second].io_handle_ == nullptr) { //被取消的预读  取消标记异步读取
    bufs_[second].async_read_in_progress_ = false; 
  }

  if (bufs_[curr_].io_handle_ == nullptr) {
    bufs_[curr_].async_read_in_progress_ = false;
  }
}

void FilePrefetchBuffer::AbortAllIOs() {
  uint32_t second = curr_ ^ 1;
  std::vector<void*> handles;
  for (uint32_t i = 0; i < 2; i++) {
    if (bufs_[i].async_read_in_progress_ && bufs_[i].io_handle_ != nullptr) {
      handles.emplace_back(bufs_[i].io_handle_);
    }
  }
  if (!handles.empty()) {
    StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
    Status s = fs_->AbortIO(handles);
    assert(s.ok());
  }

  // Release io_handles.
  if (bufs_[curr_].io_handle_ != nullptr && bufs_[curr_].del_fn_ != nullptr) {
    DestroyAndClearIOHandle(curr_);
  } else {
    bufs_[curr_].async_read_in_progress_ = false;
  }

  if (bufs_[second].io_handle_ != nullptr && bufs_[second].del_fn_ != nullptr) {
    DestroyAndClearIOHandle(second);
  } else {
    bufs_[second].async_read_in_progress_ = false;
  }
}

// Clear the buffers if it contains outdated data. Outdated data can be           //如果缓冲区包含过期数据，则清除缓冲区。过期数据可能
// because previous sequential reads were read from the cache instead of these    //因为之前的顺序读取是从缓存中读取的，而不是从这些
// buffer.                                                                        //缓冲区中读取的。
void FilePrefetchBuffer::UpdateBuffersIfNeeded(uint64_t offset) {
  uint32_t second = curr_ ^ 1;
  if (IsBufferOutdated(offset, curr_)) { //buffer1过时  清理
    bufs_[curr_].buffer_.Clear();
  }
  if (IsBufferOutdated(offset, second)) {//buffer2过时  清理
    bufs_[second].buffer_.Clear();
  }

  {
    // In case buffers do not align, reset second buffer. This can happen in
    // case readahead_size is set.
    if (!bufs_[second].async_read_in_progress_ &&
        !bufs_[curr_].async_read_in_progress_) { //buffer1和2 都没在异步请求的情况下
      if (DoesBufferContainData(curr_)) {  //buffer1有数据
        if (bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize() !=
            bufs_[second].offset_) { //buffer1 的结束点  != buffer2开始点 则清理buffer2
          bufs_[second].buffer_.Clear();
        }
      } else { //buffer1 没有数据
        if (!IsOffsetInBuffer(offset, second)) {  //offset 不在buffer2内  则清理buffer2  
          bufs_[second].buffer_.Clear();
        }
      }
    }
  }

  // If data starts from second buffer, make it curr_. Second buffer can be
  // either partial filled, full or async read is in progress.
  if (bufs_[second].async_read_in_progress_) { //buffer2 在异步获取数据 
    if (IsOffsetInBufferWithAsyncProgress(offset, second)) { //且当前offset 在buffer2内 则把buffer2 置换成buffer1
      curr_ = curr_ ^ 1;
    }
  } else { //buffer2 没在异步请求
    if (DoesBufferContainData(second) && IsOffsetInBuffer(offset, second)) { //buffer2 有数据 且 当前offset在buffer2中
      assert(bufs_[curr_].async_read_in_progress_ ||
             bufs_[curr_].buffer_.CurrentSize() == 0); //buffer1 正在异步请求或者 buffer1没有数据
      curr_ = curr_ ^ 1;    //buffer2 置换成buffer1
    }
  }
}

void FilePrefetchBuffer::PollAndUpdateBuffersIfNeeded(uint64_t offset) {
  if (bufs_[curr_].async_read_in_progress_/*buffer1 正在异步读取数据*/ && fs_ != nullptr /*文件非空*/) {
    if (bufs_[curr_].io_handle_ != nullptr) { //io_handle 请求非空
      // Wait for prefetch data to complete.
      // No mutex is needed as async_read_in_progress behaves as mutex and is
      // updated by main thread only.
      std::vector<void*> handles;
      handles.emplace_back(bufs_[curr_].io_handle_); //buffer1的 请求参数
      StopWatch sw(clock_, stats_, POLL_WAIT_MICROS);
      fs_->Poll(handles, 1).PermitUncheckedError(); //等待buffer1的请求结果
    }

    // Reset and Release io_handle after the Poll API as request has been
    // completed.
    DestroyAndClearIOHandle(curr_);  //请求完成后 清理io_handle
  }
  UpdateBuffersIfNeeded(offset); //更新buffer状态
}

Status FilePrefetchBuffer::HandleOverlappingData(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t length, size_t readahead_size,
    Env::IOPriority /*rate_limiter_priority*/, bool& copy_to_third_buffer,
    uint64_t& tmp_offset, size_t& tmp_length) { //异步预取机制中用于处理跨 buffer 数据读取的关键函数。
  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment(); //4k
  uint32_t second;

  // Check if the first buffer has the required offset and the async read is
  // still in progress. This should only happen if a prefetch was initiated
  // by Seek, but the next access is at another offset.
  if (bufs_[curr_].async_read_in_progress_/*buffer1异步请求中*/ &&
      IsOffsetInBufferWithAsyncProgress(offset, curr_)/*offset在buffer1中*/) {
    PollAndUpdateBuffersIfNeeded(offset); //阻塞等待buffer1数据
  }
  second = curr_ ^ 1;

  // If data is overlapping over two buffers, copy the data from curr_ and
  // call ReadAsync on curr_.
  if (!bufs_[curr_].async_read_in_progress_ /*buffer1不在请求中*/ && DoesBufferContainData(curr_) /*buffer1有数据*/ &&
      IsOffsetInBuffer(offset, curr_) /*offset在buffer1内*/ &&
      (/*Data extends over curr_ buffer and second buffer either has data or in
         process of population=*/
       (offset + length > bufs_[second].offset_) &&
       (bufs_[second].async_read_in_progress_ ||
        DoesBufferContainData(second)))/*跨数据buffer2*/) { //跨2块buffer数据  则创建buffer3 并设置数据拷贝到buffer3
    // Allocate new buffer to third buffer;
    bufs_[2].buffer_.Clear(); //清空buffer3
    bufs_[2].buffer_.Alignment(alignment);//对齐4k
    bufs_[2].buffer_.AllocateNewBuffer(length);//申请一块buffer长度为length
    bufs_[2].offset_ = offset; //记录偏移量offset
    copy_to_third_buffer = true;//设置数据拷贝到buffer3

    CopyDataToBuffer(curr_, tmp_offset, tmp_length); //拷贝buffer1数据到buffer3

    // Call async prefetching on curr_ since data has been consumed in curr_
    // only if data lies within second buffer.
    size_t second_size = bufs_[second].async_read_in_progress_
                             ? bufs_[second].async_req_len_
                             : bufs_[second].buffer_.CurrentSize(); //buffer的数据长度
    if (tmp_offset + tmp_length <= bufs_[second].offset_ + second_size) { //剩余数据在buffer2内
      uint64_t rounddown_start = bufs_[second].offset_ + second_size; //buffer2已有结束点对齐4k
      uint64_t roundup_end =
          Roundup(rounddown_start + readahead_size, alignment);       //buffer2预读结束点对齐4k
      uint64_t roundup_len = roundup_end - rounddown_start;           //读取长度
      uint64_t chunk_len = 0;
      CalculateOffsetAndLen(alignment, rounddown_start, roundup_len, curr_,
                            false, chunk_len);                        //刷新buffer[curr_]大小
      assert(chunk_len == 0);
      assert(roundup_len >= chunk_len);

      bufs_[curr_].offset_ = rounddown_start;                         //重置offset 为后续切换成buffer2做准备
      uint64_t read_len = static_cast<size_t>(roundup_len - chunk_len);
      s = ReadAsync(opts, reader, read_len, rounddown_start, curr_); //发起异步请求
      if (!s.ok()) { //失败
        DestroyAndClearIOHandle(curr_); //清空buffer1
        bufs_[curr_].buffer_.Clear();   //清空buffer数据
        return s;
      }
    }
    curr_ = curr_ ^ 1; // buffer1和2互换
  }
  return s;
}
// If async_io is enabled in case of sequential reads, PrefetchAsyncInternal is   // 如果在顺序读取的情况下启用了 async_io，则会调用 PrefetchAsyncInternal。
// called. When buffers are switched, we clear the curr_ buffer as we assume the  // 切换缓冲区时，我们会清除 curr_ 缓冲区，因为我们假设
// data has been consumed because of sequential reads.                            // 数据已由于顺序读取而被消耗
// Data in buffers will always be sequential with curr_ following second and      // 缓冲区中的数据始终是连续的，curr_ 紧随第二个缓冲区之后，
// not vice versa.                                                                // 反之亦然。
//
// Scenarios for prefetching asynchronously:                                      // 异步预取的场景：
// Case1: If both buffers are empty, prefetch n + readahead_size_/2 bytes         // 情况 1：如果两个缓冲区都为空，则在 curr_ 缓冲区中同步预取 n + readahead_size_/2 个字节，并在第二个缓冲区中异步预取 readahead_size_/2 个字节。
//        synchronously in curr_ and prefetch readahead_size_/2 async in second   
//        buffer.
// Case2: If second buffer has partial or full data, make it current and          // 情况 2：如果第二个缓冲区包含部分或全部数据，则将其设为当前缓冲区，并在第二个缓冲区中异步预取 readahead_size_/2 个字节。
//        prefetch readahead_size_/2 async in second buffer. In case of           // 如果是部分数据，则同步预取大小为 n 的剩余字节，以满足请求的字节数。
//        partial data, prefetch remaining bytes from size n synchronously to
//        fulfill the requested bytes request.
// Case3: If curr_ has partial data, prefetch remaining bytes from size n         // 情况 3：如果 curr_ 有部分数据，则在 curr_ 中同步预取大小为 n 的剩余字节，以满足请求的字节数，并
//        synchronously in curr_ to fulfill the requested bytes request and       // 在第二个缓冲区中异步预取 readahead_size_/2 字节。
//        prefetch readahead_size_/2 bytes async in second buffer.
// Case4: (Special case) If data is in both buffers, copy requested data from     // 情况 4：（特殊情况）如果两个缓冲区中都有数据，则从 curr_ 复制请求的数据，向 curr_ 发送异步请求，
//        curr_, send async request on curr_, wait for poll to fill second        // 等待轮询填充第二个缓冲区（如果有），然后将剩余数据从第二个缓冲区复制到第三个缓冲区。
//        buffer (if any), and copy remaining data from second buffer to third
//        buffer.
Status FilePrefetchBuffer::PrefetchAsyncInternal(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t length, size_t readahead_size, Env::IOPriority rate_limiter_priority,
    bool& copy_to_third_buffer) { //该函数用于在异步读取场景中进行数据的预取（prefetch）和缓冲管理，以提高顺序读取性能。它利用了两个缓冲区（curr_ 和 second），并支持同步与异步混合读取策略，确保当前请求的数据尽可能命中缓存。
  if (!enable_) {
    return Status::OK();
  }

  TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsyncInternal:Start");

  size_t alignment = reader->file()->GetRequiredBufferAlignment();//读取对齐4k 
  Status s;
  uint64_t tmp_offset = offset;
  size_t tmp_length = length;

  // 1. Abort IO and swap buffers if needed to point curr_ to first buffer with
  // data.
  if (!explicit_prefetch_submitted_) { //如果新的读取位置不连续（比如 Seek 后），需要取消之前提交的异步 I/O 请求
    AbortIOIfNeeded(offset);
  }
  UpdateBuffersIfNeeded(offset); //更新buffer状态

  // 2. Handle overlapping data over two buffers. If data is overlapping then   //2. 处理两个缓冲区的重叠数据。如果数据重叠，则
  //    during this call:                                                       // 在此调用期间：
  //   - data from curr_ is copied into third buffer,                           //- 将 curr_ 中的数据复制到第三个缓冲区；
  //   - curr_ is send for async prefetching of further data if second buffer   //- 如果第二个缓冲区包含剩余的请求数据或正在进行异步预取，则发送 curr_ 进行异步预取；
  //     contains remaining requested data or in progress for async prefetch,   //- 切换缓冲区，curr_ 现在指向第二个缓冲区以复制剩余的
  //   - switch buffers and curr_ now points to second buffer to copy remaining //数据
  //     data.
  s = HandleOverlappingData(opts, reader, offset, length, readahead_size,
                            rate_limiter_priority, copy_to_third_buffer,
                            tmp_offset, tmp_length); //跨buffer数据读取
  if (!s.ok()) { //处理失败返回
    return s;
  }

  // 3. Call Poll only if data is needed for the second buffer.
  //    - Return if whole data is in curr_ and second buffer is in progress or
  //      already full.
  //    - If second buffer is empty, it will go for ReadAsync for second buffer.
  if (!bufs_[curr_].async_read_in_progress_ && DoesBufferContainData(curr_) &&
      IsDataBlockInBuffer(offset, length, curr_)) { //buffer1 不再异步请求 且数据在buffer1内
    // Whole data is in curr_.
    UpdateBuffersIfNeeded(offset); //更新buffer状态
    if (!IsSecondBuffEligibleForPrefetching()) { //不需要预读buffer2 直接返回
      return s;
    }
  } else { //buffer1数据在异步请求中， 等待数据完成
    // After poll request, curr_ might be empty because of IOError in
    // callback while reading or may contain required data.
    PollAndUpdateBuffersIfNeeded(offset); //等待buffer1 异步完成 且更新buffer状态
  }

  if (copy_to_third_buffer) { //需要拷贝到,修改offset和长度
    offset = tmp_offset;
    length = tmp_length;
  }

  // 4. After polling and swapping buffers, if all the requested bytes are in
  // curr_, it will only go for async prefetching.
  // copy_to_third_buffer is a special case so it will be handled separately.
  if (!copy_to_third_buffer /*没有buffer3*/ && DoesBufferContainData(curr_) /*buffer1有数据*/ &&
      IsDataBlockInBuffer(offset, length, curr_) /*数据在buffer1*/) {
    offset += length;
    length = 0;

    // Since async request was submitted directly by calling PrefetchAsync in
    // last call, we don't need to prefetch further as this call is to poll
    // the data submitted in previous call.
    if (explicit_prefetch_submitted_) { /*数据连续直接返回*/
      return s;
    }
    if (!IsSecondBuffEligibleForPrefetching()) { //buffer2 不需要预取
      return s;
    }
  }

  uint32_t second = curr_ ^ 1;
  assert(!bufs_[curr_].async_read_in_progress_); //buffer1 未异步请求

  // In case because of some IOError curr_ got empty, abort IO for second as
  // well. Otherwise data might not align if more data needs to be read in curr_
  // which might overlap with second buffer.
  if (!DoesBufferContainData(curr_) && bufs_[second].async_read_in_progress_ ) { //buffer1没有数据 buffer2正在请求数据，则取消buffer2的请求
    if (bufs_[second].io_handle_ != nullptr) { //buffer2 正在异步预读
      std::vector<void*> handles;
      handles.emplace_back(bufs_[second].io_handle_);
      {
        StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
        Status status = fs_->AbortIO(handles); //取消buffer2
        assert(status.ok());
      }
    }
    DestroyAndClearIOHandle(second);  //清理buffer2的io_handle
    bufs_[second].buffer_.Clear();    //清理buffer2的数据
  }

  // 5. Data is overlapping i.e. some of the data has been copied to third
  // buffer and remaining will be updated below.
  if (copy_to_third_buffer && DoesBufferContainData(curr_)) { //需要拷贝到buffer3 且buffer1 有数据
    CopyDataToBuffer(curr_, offset, length);

    // Length == 0: All the requested data has been copied to third buffer and    // Length == 0：所有请求的数据都已复制到第三个缓冲区，并且
    // it has already gone for async prefetching. It can return without doing     // 它已经进行了异步预取。它可以立即返回，无需执行任何操作。
    // anything further.
    // Length > 0: More data needs to be consumed so it will continue async       // Length > 0：需要消耗更多数据，因此它将继续异步
    // and sync prefetching and copy the remaining data to third buffer in the    // 和同步预取，并在最后将剩余数据复制到第三个缓冲区。
    // end.
    if (length == 0) {  //数据拷贝完成 直接返回
      return s;
    }
  }

  // 6. Go for ReadAsync and Read (if needed).
  size_t prefetch_size = length + readahead_size; //预取大小   
  size_t _offset = static_cast<size_t>(offset);

  // offset and size alignment for curr_ buffer with synchronous prefetching
  uint64_t rounddown_start1 = Rounddown(_offset, alignment);            //下界对齐
  uint64_t roundup_end1 = Roundup(_offset + prefetch_size, alignment); //上界对齐
  uint64_t roundup_len1 = roundup_end1 - rounddown_start1;             //对齐后读取长度
  assert(roundup_len1 >= alignment);
  assert(roundup_len1 % alignment == 0);
  uint64_t chunk_len1 = 0;
  uint64_t read_len1 = 0;

  assert(!bufs_[second].async_read_in_progress_ &&
         !DoesBufferContainData(second)); //buffer2不在预读 且buffer2没有数据

  // For length == 0, skip the synchronous prefetching. read_len1 will be 0.
  if (length > 0) { //数据不足 需要同步预读
    CalculateOffsetAndLen(alignment, offset, roundup_len1, curr_,
                          false /*refit_tail*/, chunk_len1);
    assert(roundup_len1 >= chunk_len1);
    read_len1 = static_cast<size_t>(roundup_len1 - chunk_len1); //计算出最终读取长度
  }
  {
    // offset and size alignment for second buffer for asynchronous
    // prefetching
    uint64_t rounddown_start2 = roundup_end1;
    uint64_t roundup_end2 =
        Roundup(rounddown_start2 + readahead_size, alignment); //对齐4k

    // For length == 0, do the asynchronous prefetching in second instead of
    // synchronous prefetching in curr_.
    if (length == 0) { //数据在buffer1内 计算buffer2 预读长度
      rounddown_start2 =
          bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize();
      roundup_end2 = Roundup(rounddown_start2 + prefetch_size, alignment);
    }

    uint64_t roundup_len2 = roundup_end2 - rounddown_start2;
    uint64_t chunk_len2 = 0;
    CalculateOffsetAndLen(alignment, rounddown_start2, roundup_len2, second,
                          false /*refit_tail*/, chunk_len2);  //扩容buffer2
    assert(chunk_len2 == 0);
    // Update the buffer offset.
    bufs_[second].offset_ = rounddown_start2;
    assert(roundup_len2 >= chunk_len2);
    uint64_t read_len2 = static_cast<size_t>(roundup_len2 - chunk_len2);//最终预读长度
    s = ReadAsync(opts, reader, read_len2, rounddown_start2, second); //异步预读buffer2
    if (!s.ok()) {//预读失败
      DestroyAndClearIOHandle(second);//清理掉buffer2的io_handle
      bufs_[second].buffer_.Clear();  //清理掉buffer2数据
      return s;
    }
  }

  if (read_len1 > 0) {  //需要同步预读
    s = Read(opts, reader, rate_limiter_priority, read_len1, chunk_len1,
             rounddown_start1, curr_); //同步预读
    if (!s.ok()) { //同步预读失败
      if (bufs_[second].io_handle_ != nullptr) { //buffer2 正在异步预读，则终止
        std::vector<void*> handles;
        handles.emplace_back(bufs_[second].io_handle_);
        {
          StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
          Status status = fs_->AbortIO(handles);//取消buffer2
          assert(status.ok());
        }
      }
      DestroyAndClearIOHandle(second); //清理buffer2
      bufs_[second].buffer_.Clear();   //清理buffer2数据
      bufs_[curr_].buffer_.Clear();    //清理buffer1数据
      return s;
    }
  }
  // Copy remaining requested bytes to third_buffer.
  if (copy_to_third_buffer && length > 0) { //补充数据到buffer3
    CopyDataToBuffer(curr_, offset, length);
  }
  return s;
}

bool FilePrefetchBuffer::TryReadFromCache(const IOOptions& opts,
                                          RandomAccessFileReader* reader,
                                          uint64_t offset, size_t n,
                                          Slice* result, Status* status,
                                          Env::IOPriority rate_limiter_priority,
                                          bool for_compaction /* = false */) {
  bool ret = TryReadFromCacheUntracked(opts, reader, offset, n, result, status,
                                       rate_limiter_priority, for_compaction);
  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && enable_) {
    if (ret) {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_HIT);
    } else {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_MISS);
    }
  }
  return ret;
}

bool FilePrefetchBuffer::TryReadFromCacheUntracked(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t n, Slice* result, Status* status,
    Env::IOPriority rate_limiter_priority, bool for_compaction /* = false */) {
  if (track_min_offset_ && offset < min_offset_read_) {
    min_offset_read_ = static_cast<size_t>(offset);
  }
  if (!enable_ || (offset < bufs_[curr_].offset_)) {
    return false;
  }

  // If the buffer contains only a few of the requested bytes:
  //    If readahead is enabled: prefetch the remaining bytes + readahead bytes
  //        and satisfy the request.
  //    If readahead is not enabled: return false.
  TEST_SYNC_POINT_CALLBACK("FilePrefetchBuffer::TryReadFromCache",
                           &readahead_size_);
  if (offset + n > bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize()) {
    if (readahead_size_ > 0) {
      Status s;
      assert(reader != nullptr);
      assert(max_readahead_size_ >= readahead_size_);
      if (for_compaction) {
        s = Prefetch(opts, reader, offset, std::max(n, readahead_size_),
                     rate_limiter_priority);
      } else {
        if (implicit_auto_readahead_) {
          if (!IsEligibleForPrefetch(offset, n)) {
            // Ignore status as Prefetch is not called.
            s.PermitUncheckedError();
            return false;
          }
        }
        s = Prefetch(opts, reader, offset, n + readahead_size_,
                     rate_limiter_priority);
      }
      if (!s.ok()) {
        if (status) {
          *status = s;
        }
#ifndef NDEBUG
        IGNORE_STATUS_IF_ERROR(s);
#endif
        return false;
      }
      readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
    } else {
      return false;
    }
  }
  UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);

  uint64_t offset_in_buffer = offset - bufs_[curr_].offset_;
  *result = Slice(bufs_[curr_].buffer_.BufferStart() + offset_in_buffer, n);
  return true;
}

bool FilePrefetchBuffer::TryReadFromCacheAsync(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t n, Slice* result, Status* status,
    Env::IOPriority rate_limiter_priority) {
  bool ret = TryReadFromCacheAsyncUntracked(opts, reader, offset, n, result,
                                            status, rate_limiter_priority);
  if (usage_ == FilePrefetchBufferUsage::kTableOpenPrefetchTail && enable_) {
    if (ret) {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_HIT);
    } else {
      RecordTick(stats_, TABLE_OPEN_PREFETCH_TAIL_MISS);
    }
  }
  return ret;
}

bool FilePrefetchBuffer::TryReadFromCacheAsyncUntracked(
    const IOOptions& opts, RandomAccessFileReader* reader, uint64_t offset,
    size_t n, Slice* result, Status* status,
    Env::IOPriority rate_limiter_priority) {
  if (track_min_offset_ && offset < min_offset_read_) {
    min_offset_read_ = static_cast<size_t>(offset);
  }

  if (!enable_) {
    return false;
  }

  if (explicit_prefetch_submitted_) {
    // explicit_prefetch_submitted_ is special case where it expects request
    // submitted in PrefetchAsync should match with this request. Otherwise
    // buffers will be outdated.
    // Random offset called. So abort the IOs.
    if (prev_offset_ != offset) {
      AbortAllIOs();
      bufs_[curr_].buffer_.Clear();
      bufs_[curr_ ^ 1].buffer_.Clear();
      explicit_prefetch_submitted_ = false;
      return false;
    }
  }

  if (!explicit_prefetch_submitted_ && offset < bufs_[curr_].offset_) {
    return false;
  }

  bool prefetched = false;
  bool copy_to_third_buffer = false;
  // If the buffer contains only a few of the requested bytes:
  //    If readahead is enabled: prefetch the remaining bytes + readahead bytes
  //        and satisfy the request.
  //    If readahead is not enabled: return false.
  TEST_SYNC_POINT_CALLBACK("FilePrefetchBuffer::TryReadFromCache",
                           &readahead_size_);

  if (explicit_prefetch_submitted_ ||
      (bufs_[curr_].async_read_in_progress_ ||
       offset + n >
           bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize())) {
    if (readahead_size_ > 0) {
      Status s;
      assert(reader != nullptr);
      assert(max_readahead_size_ >= readahead_size_);

      if (implicit_auto_readahead_) {
        if (!IsEligibleForPrefetch(offset, n)) {
          // Ignore status as Prefetch is not called.
          s.PermitUncheckedError();
          return false;
        }
      }
      // Prefetch n + readahead_size_/2 synchronously as remaining
      // readahead_size_/2 will be prefetched asynchronously.
      s = PrefetchAsyncInternal(opts, reader, offset, n, readahead_size_ / 2,
                                rate_limiter_priority, copy_to_third_buffer);
      explicit_prefetch_submitted_ = false;
      if (!s.ok()) {
        if (status) {
          *status = s;
        }
#ifndef NDEBUG
        IGNORE_STATUS_IF_ERROR(s);
#endif
        return false;
      }
      prefetched = explicit_prefetch_submitted_ ? false : true;
    } else {
      return false;
    }
  }

  UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);

  uint32_t index = curr_;
  if (copy_to_third_buffer) {
    index = 2;
  }
  uint64_t offset_in_buffer = offset - bufs_[index].offset_;
  *result = Slice(bufs_[index].buffer_.BufferStart() + offset_in_buffer, n);
  if (prefetched) {
    readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
  }
  return true;
}

void FilePrefetchBuffer::PrefetchAsyncCallback(const FSReadRequest& req,
                                               void* cb_arg) {
  uint32_t index = *(static_cast<uint32_t*>(cb_arg));
#ifndef NDEBUG
  if (req.result.size() < req.len) {
    // Fake an IO error to force db_stress fault injection to ignore
    // truncated read errors
    IGNORE_STATUS_IF_ERROR(Status::IOError());
  }
  IGNORE_STATUS_IF_ERROR(req.status);
#endif

  if (req.status.ok()) {
    if (req.offset + req.result.size() <=
        bufs_[index].offset_ + bufs_[index].buffer_.CurrentSize()) {
      // All requested bytes are already in the buffer or no data is read
      // because of EOF. So no need to update.
      return;
    }
    if (req.offset < bufs_[index].offset_) {
      // Next block to be read has changed (Recent read was not a sequential
      // read). So ignore this read.
      return;
    }
    size_t current_size = bufs_[index].buffer_.CurrentSize();
    bufs_[index].buffer_.Size(current_size + req.result.size());
  }
}

Status FilePrefetchBuffer::PrefetchAsync(const IOOptions& opts,
                                         RandomAccessFileReader* reader,
                                         uint64_t offset, size_t n,
                                         Slice* result) {
  assert(reader != nullptr);
  if (!enable_) {
    return Status::NotSupported();
  }

  TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsync:Start");

  num_file_reads_ = 0;
  explicit_prefetch_submitted_ = false;
  bool is_eligible_for_prefetching = false;
  if (readahead_size_ > 0 &&
      (!implicit_auto_readahead_ ||
       num_file_reads_ >= num_file_reads_for_auto_readahead_)) {
    is_eligible_for_prefetching = true;
  }

  // 1. Cancel any pending async read to make code simpler as buffers can be out
  // of sync.
  AbortAllIOs();

  // 2. Clear outdated data.
  UpdateBuffersIfNeeded(offset);
  uint32_t second = curr_ ^ 1;
  // Since PrefetchAsync can be called on non sequential reads. So offset can
  // be less than curr_ buffers' offset. In that case also it clears both
  // buffers.
  if (DoesBufferContainData(curr_) && !IsOffsetInBuffer(offset, curr_)) {
    bufs_[curr_].buffer_.Clear();
    bufs_[second].buffer_.Clear();
  }

  UpdateReadPattern(offset, n, /*decrease_readaheadsize=*/false);

  bool data_found = false;

  // 3. If curr_ has full data.
  if (DoesBufferContainData(curr_) && IsDataBlockInBuffer(offset, n, curr_)) {
    uint64_t offset_in_buffer = offset - bufs_[curr_].offset_;
    *result = Slice(bufs_[curr_].buffer_.BufferStart() + offset_in_buffer, n);
    data_found = true;
    // Update num_file_reads_ as TryReadFromCacheAsync won't be called for
    // poll and update num_file_reads_ if data is found.
    num_file_reads_++;

    // 3.1 If second also has some data or is not eligible for prefetching,
    // return.
    if (!is_eligible_for_prefetching || DoesBufferContainData(second)) {
      return Status::OK();
    }
  } else {
    // Partial data in curr_.
    bufs_[curr_].buffer_.Clear();
  }
  bufs_[second].buffer_.Clear();

  Status s;
  size_t alignment = reader->file()->GetRequiredBufferAlignment();
  size_t prefetch_size = is_eligible_for_prefetching ? readahead_size_ / 2 : 0;
  size_t offset_to_read = static_cast<size_t>(offset);
  uint64_t rounddown_start1 = 0;
  uint64_t roundup_end1 = 0;
  uint64_t rounddown_start2 = 0;
  uint64_t roundup_end2 = 0;
  uint64_t chunk_len1 = 0;
  uint64_t chunk_len2 = 0;
  size_t read_len1 = 0;
  size_t read_len2 = 0;

  // - If curr_ is empty.
  //   - Call async read for full data +  prefetch_size on curr_.
  //   - Call async read for prefetch_size on second if eligible.
  // - If curr_ is filled.
  //   - prefetch_size on second.
  // Calculate length and offsets for reading.
  if (!DoesBufferContainData(curr_)) {
    // Prefetch full data + prefetch_size in curr_.
    rounddown_start1 = Rounddown(offset_to_read, alignment);
    roundup_end1 = Roundup(offset_to_read + n + prefetch_size, alignment);
    uint64_t roundup_len1 = roundup_end1 - rounddown_start1;
    assert(roundup_len1 >= alignment);
    assert(roundup_len1 % alignment == 0);

    CalculateOffsetAndLen(alignment, rounddown_start1, roundup_len1, curr_,
                          false, chunk_len1);
    assert(chunk_len1 == 0);
    assert(roundup_len1 >= chunk_len1);
    read_len1 = static_cast<size_t>(roundup_len1 - chunk_len1);
    bufs_[curr_].offset_ = rounddown_start1;
  }

  if (is_eligible_for_prefetching) {
    if (DoesBufferContainData(curr_)) {
      rounddown_start2 =
          bufs_[curr_].offset_ + bufs_[curr_].buffer_.CurrentSize();
    } else {
      rounddown_start2 = roundup_end1;
    }

    roundup_end2 = Roundup(rounddown_start2 + prefetch_size, alignment);
    uint64_t roundup_len2 = roundup_end2 - rounddown_start2;

    assert(roundup_len2 >= alignment);
    CalculateOffsetAndLen(alignment, rounddown_start2, roundup_len2, second,
                          false, chunk_len2);
    assert(chunk_len2 == 0);
    assert(roundup_len2 >= chunk_len2);
    read_len2 = static_cast<size_t>(roundup_len2 - chunk_len2);
    // Update the buffer offset.
    bufs_[second].offset_ = rounddown_start2;
  }

  if (read_len1) {
    s = ReadAsync(opts, reader, read_len1, rounddown_start1, curr_);
    if (!s.ok()) {
      DestroyAndClearIOHandle(curr_);
      bufs_[curr_].buffer_.Clear();
      return s;
    }
    explicit_prefetch_submitted_ = true;
    prev_len_ = 0;
  }
  if (read_len2) {
    TEST_SYNC_POINT("FilePrefetchBuffer::PrefetchAsync:ExtraPrefetching");
    s = ReadAsync(opts, reader, read_len2, rounddown_start2, second);
    if (!s.ok()) {
      DestroyAndClearIOHandle(second);
      bufs_[second].buffer_.Clear();
      return s;
    }
    readahead_size_ = std::min(max_readahead_size_, readahead_size_ * 2);
  }
  return (data_found ? Status::OK() : Status::TryAgain());
}

}  // namespace ROCKSDB_NAMESPACE
