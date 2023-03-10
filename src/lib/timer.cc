// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lib/timer.cc:
 *    Object that measures the length of its lifetime.
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

#include "lib/timer.h"

#include <bits/stdint-uintn.h>
#include <sys/time.h>

#include <ctime>

#include "lib/message.h"

Timer::Timer(const char *name) : name(name) {
  if (clock_gettime(CLOCK_MONOTONIC, &start) < 0) {
    PPanic("Failed to get CLOCK_MONOTONIC");
  }
}

Timer::~Timer() {
  struct timespec end {};
  if (clock_gettime(CLOCK_MONOTONIC, &end) < 0) {
    PPanic("Failed to get CLOCK_MONOTONIC");
  }

  uint64_t delta;
  delta = end.tv_sec - start.tv_sec;
  delta *= 1000000000ll;
  if (end.tv_nsec < start.tv_nsec) {
    delta -= 1000000000ll;
    delta += (end.tv_nsec + 1000000000ll) - start.tv_nsec;
  } else {
    delta += end.tv_nsec - start.tv_nsec;
  }
  Debug("timer %s %lu ns", name, delta);
}
