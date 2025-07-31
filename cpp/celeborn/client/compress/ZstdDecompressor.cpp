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

#include "celeborn/client/compress/ZstdDecompressor.h"
#include <zlib.h>
#include <zstd.h>
#include <cstring>
#include <iostream>
#include "celeborn/utils/Exceptions.h"

namespace celeborn {
namespace client {
namespace compress {
int ZstdDecompressor::getOriginalLen(const char* src) {
  return readIntLE(src, MAGIC_LENGTH + 5);
}

int ZstdDecompressor::decompress(const char* src, char* dst, int dstOff) {
  const int compressionMethod = src[MAGIC_LENGTH];
  const int compressedLen = readIntLE(src, MAGIC_LENGTH + 1);
  const int originalLen = readIntLE(src, MAGIC_LENGTH + 5);
  const int expectedCheck = readIntLE(src, MAGIC_LENGTH + 9);

  const char* compressedDataPtr = src + HEADER_LENGTH;
  char* dstPtr = dst + dstOff;

  switch (compressionMethod) {
    case COMPRESSION_METHOD_RAW:
      std::memcpy(dstPtr, compressedDataPtr, originalLen);
      break;

    case COMPRESSION_METHOD_ZSTD: {
      const size_t decompressedBytes = ZSTD_decompress(
          dstPtr, originalLen, compressedDataPtr, compressedLen);

      if (decompressedBytes != originalLen) {
        CELEBORN_FAIL(
            std::string("Decompression failed! Zstd error or size mismatch. ") +
            "Expected: " + std::to_string(originalLen) +
            ", Got: " + std::to_string(decompressedBytes));
      }
      break;
    }
    default:
      CELEBORN_FAIL(
          std::string("Unsupported compression method: ") +
          std::to_string(compressionMethod));
  }

  uLong actualCheck = crc32(0L, Z_NULL, 0);
  actualCheck =
      crc32(actualCheck, reinterpret_cast<const Bytef*>(dstPtr), originalLen);

  if (static_cast<uint32_t>(actualCheck) != expectedCheck) {
    CELEBORN_FAIL(
        std::string("Checksum mismatch! Expected: ") +
        std::to_string(expectedCheck) +
        ", Actual: " + std::to_string(actualCheck));
  }

  return originalLen;
}
} // namespace compress
} // namespace client
} // namespace celeborn
