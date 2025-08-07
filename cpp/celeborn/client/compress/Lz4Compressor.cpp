/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "celeborn/client/compress/Lz4Compressor.h"
#include <lz4.h>
#include <cstring>
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {
namespace compress {
Lz4Compressor::Lz4Compressor(const int initialBlockSize)
    : compressedTotalSize_(0) {
  xxhash_state_ = XXH32_createState();
  if (!xxhash_state_) {
    CELEBORN_FAIL("Failed to create XXH32 state.")
  }
  XXH32_reset(xxhash_state_, kDefaultSeed);

  initCompressBuffer(LZ4_compressBound(initialBlockSize));
}

Lz4Compressor::~Lz4Compressor() {
  if (xxhash_state_) {
    XXH32_freeState(xxhash_state_);
  }
}

void Lz4Compressor::initCompressBuffer(const int maxDestLength) {
  const int compressedBlockSize = kHeaderLength + maxDestLength;
  compressedBuffer_.resize(compressedBlockSize);
  std::copy_n(kMagic, kMagicLength, compressedBuffer_.begin());
}

void Lz4Compressor::compress(
    const uint8_t* data,
    const int offset,
    const int length) {
  XXH32_reset(xxhash_state_, kDefaultSeed);
  XXH32_update(xxhash_state_, data + offset, length);
  const uint32_t check = XXH32_digest(xxhash_state_) & 0xFFFFFFFL;

  const int maxDestLength = LZ4_compressBound(length);
  if (compressedBuffer_.size() - kHeaderLength < maxDestLength) {
    initCompressBuffer(maxDestLength);
  }

  const auto src = reinterpret_cast<const char*>(data + offset);
  const auto dest =
      reinterpret_cast<char*>(compressedBuffer_.data() + kHeaderLength);

  int compressedLength = LZ4_compress_default(src, dest, length, maxDestLength);

  int compressMethod;
  if (compressedLength <= 0 || compressedLength >= length) {
    compressMethod = kCompressionMethodRaw;
    compressedLength = length;
    std::copy_n(
        data + offset, length, compressedBuffer_.begin() + kHeaderLength);
  } else {
    compressMethod = kCompressionMethodLZ4;
  }

  compressedBuffer_[kMagicLength] = static_cast<uint8_t>(compressMethod);
  writeIntLE(compressedLength, compressedBuffer_.data(), kMagicLength + 1);
  writeIntLE(length, compressedBuffer_.data(), kMagicLength + 5);
  writeIntLE(
      static_cast<int>(check), compressedBuffer_.data(), kMagicLength + 9);

  compressedTotalSize_ = kHeaderLength + compressedLength;
}

size_t Lz4Compressor::getCompressedTotalSize() const {
  return compressedTotalSize_;
}

const std::vector<uint8_t>& Lz4Compressor::getCompressedBuffer() const {
  return compressedBuffer_;
}

} // namespace compress
} // namespace client
} // namespace celeborn
