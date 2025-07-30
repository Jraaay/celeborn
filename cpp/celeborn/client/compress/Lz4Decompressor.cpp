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

#include "celeborn/client/compress/Lz4Decompressor.h"
#include <lz4.h>
#include <cstring>
#include <iostream>
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {
namespace compress {
Lz4Decompressor::Lz4Decompressor() {
  xxhash_state_ = XXH32_createState();
  if (!xxhash_state_) {
    CELEBORN_FAIL("Failed to create XXH32 state.")
  }
  XXH32_reset(xxhash_state_, DEFAULT_SEED);
}

Lz4Decompressor::~Lz4Decompressor() {
  if (xxhash_state_) {
    XXH32_freeState(xxhash_state_);
  }
}

int Lz4Decompressor::getOriginalLen(const char* src) {
  return readIntLE(src, MAGIC_LENGTH + 5);
}

int Lz4Decompressor::decompress(const char* src, char* dst, int dst_off) {
  const int compression_method = static_cast<unsigned char>(src[MAGIC_LENGTH]);
  const int compressed_len = readIntLE(src, MAGIC_LENGTH + 1);
  const int original_len = readIntLE(src, MAGIC_LENGTH + 5);
  const int expected_check = readIntLE(src, MAGIC_LENGTH + 9);

  const char* compressed_data_ptr = src + HEADER_LENGTH;
  char* dst_ptr = dst + dst_off;

  switch (compression_method) {
    case COMPRESSION_METHOD_RAW:
      std::memcpy(dst_ptr, compressed_data_ptr, original_len);
      break;
    case COMPRESSION_METHOD_LZ4: {
      const int decompressed_bytes = LZ4_decompress_safe(
          compressed_data_ptr, dst_ptr, compressed_len, original_len);

      if (decompressed_bytes != original_len) {
        CELEBORN_FAIL(
            std::string("Decompression failed! LZ4 error or size mismatch. ") +
            "Expected: " + std::to_string(original_len) +
            ", Got: " + std::to_string(decompressed_bytes));
      }
      break;
    }
    default:
      CELEBORN_FAIL(
          std::string("Unsupported compression method: ") +
          std::to_string(compression_method));
  }

  XXH32_reset(xxhash_state_, DEFAULT_SEED);
  XXH32_update(xxhash_state_, dst_ptr, original_len);
  const uint32_t actual_check = XXH32_digest(xxhash_state_) & 0xFFFFFFFL;

  if (static_cast<uint32_t>(expected_check) != actual_check) {
    CELEBORN_FAIL(
        std::string("Checksum mismatch! Expected: ") +
        std::to_string(expected_check) +
        ", Actual: " + std::to_string(actual_check));
  }

  return original_len;
}
} // namespace compress
} // namespace client
} // namespace celeborn
