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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/truetime.cc:
 *   A simulated TrueTime module
 *
 **********************************************************************/

#include "store/common/truetime.h"

#include <bits/types/struct_timeval.h>
#include <sys/time.h>

#include <cstdlib>

#include "lib/message.h"

TrueTime::TrueTime() {
  simError = 0;
  simSkew = 0;
}

TrueTime::TrueTime(uint64_t skew, uint64_t errorBound) {
  simError = errorBound;
  if (skew == 0) {
    simSkew = 0;
  } else {
    struct timeval t1 {};
    gettimeofday(&t1, nullptr);
    srand(t1.tv_sec + t1.tv_usec);
    uint64_t r = rand();
    simSkew = (r % skew) - (skew / 2);
  }

  Debug("TrueTime variance: skew=%lu error=%lu", simSkew, simError);
}

uint64_t TrueTime::GetTime() {
  struct timeval now {};
  uint64_t timestamp;

  gettimeofday(&now, nullptr);

  now.tv_usec += simSkew;
  if (now.tv_usec > 999999) {
    now.tv_usec -= 1000000;
    now.tv_sec++;
  } else if (now.tv_usec < 0) {
    now.tv_usec += 1000000;
    now.tv_sec--;
  }

  timestamp = (static_cast<uint64_t>(now.tv_sec) << 32) |
              static_cast<uint64_t>(now.tv_usec);

  Debug("Time: %lx %lx %lx", now.tv_sec, now.tv_usec, timestamp);

  return timestamp;
}

void TrueTime::GetTimeAndError(uint64_t &time, uint64_t &error) {
  time = GetTime();
  error = simError;
}
