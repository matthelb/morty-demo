// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * viewstamp.h:
 *   definition of types and utility functions for viewstamps and
 *   related types
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef REPLICATION_COMMON_VIEWSTAMP_H_
#define REPLICATION_COMMON_VIEWSTAMP_H_

#define __STDC_FORMAT_MACROS
#include <bits/stdint-uintn.h>

#include <cinttypes>

using view_t = uint64_t;
using opnum_t = uint64_t;
using sessnum_t = uint64_t;
using msgnum_t = uint64_t;
using shardnum_t = uint32_t;

struct viewstamp_t {
  view_t view{0};
  opnum_t opnum{0};
  sessnum_t sessnum{0};
  msgnum_t msgnum{0};
  shardnum_t shardnum{0};

  viewstamp_t() {}
  viewstamp_t(view_t view, opnum_t opnum, sessnum_t sessnum = 0,
              msgnum_t msgnum = 0, shardnum_t shardnum = 0)
      : view(view),
        opnum(opnum),
        sessnum(sessnum),
        msgnum(msgnum),
        shardnum(shardnum) {}
};

#define FMT_VIEW "%" PRIu64
#define FMT_OPNUM "%" PRIu64

#define FMT_VIEWSTAMP "<" FMT_VIEW "," FMT_OPNUM ">"
#define VA_VIEWSTAMP(x) x.view, x.opnum

static inline int Viewstamp_Compare(viewstamp_t a, viewstamp_t b) {
  if (a.view < b.view) {
    return -1;
  }
  if (a.view > b.view) {
    return 1;
  }
  if (a.opnum < b.opnum) {
    return -1;
  }
  if (a.opnum > b.opnum) {
    return 1;
  }
  if (a.sessnum < b.sessnum) {
    return -1;
  }
  if (a.sessnum > b.sessnum) {
    return 1;
  }
  if (a.msgnum < b.msgnum) {
    return -1;
  }
  if (a.msgnum > b.msgnum) {
    return 1;
  }
  if (a.shardnum < b.shardnum) {
    return -1;
  }
  if (a.shardnum > b.shardnum) {
    return 1;
  }
  return 0;
}

inline bool operator==(const viewstamp_t& lhs, const viewstamp_t& rhs) {
  return Viewstamp_Compare(lhs, rhs) == 0;
}
inline bool operator!=(const viewstamp_t& lhs, const viewstamp_t& rhs) {
  return !operator==(lhs, rhs);
}
inline bool operator<(const viewstamp_t& lhs, const viewstamp_t& rhs) {
  return Viewstamp_Compare(lhs, rhs) < 0;
}
inline bool operator>(const viewstamp_t& lhs, const viewstamp_t& rhs) {
  return operator<(rhs, lhs);
}
inline bool operator<=(const viewstamp_t& lhs, const viewstamp_t& rhs) {
  return !operator>(lhs, rhs);
}
inline bool operator>=(const viewstamp_t& lhs, const viewstamp_t& rhs) {
  return !operator<(lhs, rhs);
}

#endif  // REPLICATION_COMMON_VIEWSTAMP_H_
