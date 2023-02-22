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
 * timeserver/timeserver.h:
 *   Timeserver API
 *
 **********************************************************************/

#ifndef TIMESERVER_TIMESERVER_H_
#define TIMESERVER_TIMESERVER_H_

#include <bits/stdint-uintn.h>

#include <string>

#include "lib/configuration.h"
#include "lib/udptransport.h"
#include "replication/common/replica.h"
#include "replication/common/viewstamp.h"
#include "replication/vr/replica.h"

class TimeStampServer : public replication::AppReplica {
 public:
  TimeStampServer();
  ~TimeStampServer() override;

  void ReplicaUpcall(opnum_t opnum, const std::string &str1, std::string &str2);

 private:
  uint32_t ts;
  std::string newTimeStamp();
};

#endif  // TIMESERVER_TIMESERVER_H_
