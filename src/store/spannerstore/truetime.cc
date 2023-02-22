// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/common/key_selector.h
 *   Provides random access to a given set of keys.
 *
 * Copyright 2018-2023 Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#include "store/spannerstore/truetime.h"

#include <chrono>
#include <type_traits>

#include "lib/message.h"

namespace spannerstore {

TrueTime::TrueTime() : error_{0} {}

TrueTime::TrueTime(std::uint64_t error_ms) : error_{error_ms * 1000} {
  Debug("TrueTime variance: error_ms=%lu", error_ms);
}

std::uint64_t TrueTime::GetTime() const {
  auto now = std::chrono::high_resolution_clock::now();
  long count = std::chrono::duration_cast<std::chrono::microseconds>(
                   now.time_since_epoch())
                   .count();

  return static_cast<std::uint64_t>(count);
}

TrueTimeInterval TrueTime::Now() const {
  std::uint64_t time = GetTime();
  return {time - error_, time + error_};
}

std::uint64_t TrueTime::TimeToWaitUntilMS(std::uint64_t ts) const {
  auto now = Now();
  std::uint64_t earliest = now.earliest();
  if (ts <= earliest) {
    return 0;
  }
  return (ts - earliest) / 1000;
}

std::uint64_t TrueTime::TimeToWaitUntilMicros(std::uint64_t ts) const {
  auto now = Now();
  std::uint64_t earliest = now.earliest();
  if (ts <= earliest) {
    return 0;
  }
  return ts - earliest;
}

}  // namespace spannerstore
