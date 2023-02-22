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
#pragma once

#include <bits/stdint-uintn.h>

#include <cstdint>

namespace spannerstore {

class TrueTimeInterval {
 public:
  TrueTimeInterval(std::uint64_t earliest, std::uint64_t latest)
      : earliest_{earliest}, latest_{latest} {}

  ~TrueTimeInterval() = default;

  std::uint64_t earliest() const { return earliest_; }
  std::uint64_t latest() const { return latest_; }
  std::uint64_t mid() const { return (latest_ + earliest_) / 2; }

 private:
  std::uint64_t earliest_{};
  std::uint64_t latest_{};
};

// TODO(matthelb): Combine with other TrueTime class
class TrueTime {
 public:
  TrueTime();
  explicit TrueTime(std::uint64_t error_ms);
  ~TrueTime() = default;

  std::uint64_t GetTime() const;

  TrueTimeInterval Now() const;

  std::uint64_t TimeToWaitUntilMS(std::uint64_t ts) const;
  std::uint64_t TimeToWaitUntilMicros(std::uint64_t ts) const;

 private:
  std::uint64_t error_;
};

}  // namespace spannerstore
