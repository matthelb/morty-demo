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
 * common/timestamp.h
 *   A transaction timestamp
 *
 **********************************************************************/

#ifndef _TIMESTAMP_H_
#define _TIMESTAMP_H_

#include <bits/stdint-uintn.h>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common-proto.pb.h"

class Timestamp {
 public:
  const static Timestamp MAX;

  Timestamp() {}
  Timestamp(uint64_t t) : timestamp(t), id(0) {}
  Timestamp(uint64_t t, uint64_t i) : timestamp(t), id(i) {}
  Timestamp(const TimestampMessage &msg)
      : timestamp(msg.timestamp()), id(msg.id()) {}
  ~Timestamp() = default;
  void operator=(const Timestamp &t);
  bool operator==(const Timestamp &t) const;
  bool operator!=(const Timestamp &t) const;
  bool operator>(const Timestamp &t) const;
  bool operator<(const Timestamp &t) const;
  bool operator>=(const Timestamp &t) const;
  bool operator<=(const Timestamp &t) const;
  bool isValid() const;
  uint64_t getID() const { return id; }
  uint64_t getTimestamp() const { return timestamp; }
  void setTimestamp(uint64_t t) { timestamp = t; }
  void serialize(TimestampMessage *msg) const;

 private:
  uint64_t timestamp{0};
  uint64_t id{0};
};

#endif /* _TIMESTAMP_H_ */
