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

#pragma once

#include <memory>
#include <vector>

#include "conf/CelebornConf.h"

namespace celeborn {
namespace client {
namespace compress {

class Compressor {
 public:
  virtual ~Compressor() = default;

  virtual void initCompressBuffer(int maxDestLength) = 0;
  virtual void compress(const uint8_t* data, int offset, int length) = 0;
  virtual int getCompressedTotalSize() const = 0;
  virtual const std::vector<uint8_t>& getCompressedBuffer() const = 0;

  static std::unique_ptr<Compressor> createCompressor(
      const conf::CelebornConf& conf);

 protected:
  static void writeIntLE(const int i, uint8_t* buf, int off) {
    buf[off++] = static_cast<uint8_t>(i);
    buf[off++] = static_cast<uint8_t>(i >> 8);
    buf[off++] = static_cast<uint8_t>(i >> 16);
    buf[off++] = static_cast<uint8_t>(i >> 24);
  }
};

} // namespace compress
} // namespace client
} // namespace celeborn
