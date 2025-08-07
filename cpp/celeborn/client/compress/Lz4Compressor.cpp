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
    : compressedTotalSize(0) {
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
  compressedBuffer.resize(compressedBlockSize);
  std::copy_n(kMagic, kMagicLength, compressedBuffer.begin());
}

void Lz4Compressor::compress(
    const uint8_t* data,
    const int offset,
    const int length) {
  XXH32_reset(xxhash_state_, kDefaultSeed);
  XXH32_update(xxhash_state_, data + offset, length);
  const uint32_t check = XXH32_digest(xxhash_state_) & 0xFFFFFFFL;

  const int maxDestLength = LZ4_compressBound(length);
  if (compressedBuffer.size() - kHeaderLength < maxDestLength) {
    initCompressBuffer(maxDestLength);
  }

  const auto src = reinterpret_cast<const char*>(data + offset);
  const auto dest =
      reinterpret_cast<char*>(compressedBuffer.data() + kHeaderLength);

  int compressedLength = LZ4_compress_default(src, dest, length, maxDestLength);

  int compressMethod;
  if (compressedLength <= 0 || compressedLength >= length) {
    compressMethod = kCompressionMethodRaw;
    compressedLength = length;
    std::copy_n(
        data + offset, length, compressedBuffer.begin() + kHeaderLength);
  } else {
    compressMethod = kCompressionMethodLZ4;
  }

  compressedBuffer[kMagicLength] = static_cast<uint8_t>(compressMethod);
  writeIntLE(compressedLength, compressedBuffer.data(), kMagicLength + 1);
  writeIntLE(length, compressedBuffer.data(), kMagicLength + 5);
  writeIntLE(
      static_cast<int>(check), compressedBuffer.data(), kMagicLength + 9);

  compressedTotalSize = kHeaderLength + compressedLength;
}

int Lz4Compressor::getCompressedTotalSize() const {
  return compressedTotalSize;
}

const std::vector<uint8_t>& Lz4Compressor::getCompressedBuffer() const {
  return compressedBuffer;
}

} // namespace compress
} // namespace client
} // namespace celeborn
