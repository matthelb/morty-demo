// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * log.h:
 *   a replica's log of pending and committed operations
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

#ifndef REPLICATION_COMMON_LOG_H_
#define REPLICATION_COMMON_LOG_H_

#include <google/protobuf/message.h>

#include <cstddef>
#include <map>
#include <string>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/common/request.pb.h"
#include "replication/common/viewstamp.h"

namespace replication {

enum LogEntryState {
  LOG_STATE_COMMITTED,
  LOG_STATE_PREPARED,
  LOG_STATE_SPECULATIVE,  // specpaxos only
  LOG_STATE_FASTPREPARED  // fastpaxos only
};

struct LogEntry {
  viewstamp_t viewstamp;
  LogEntryState state;
  Request request;
  string hash{};
  // Speculative client table stuff
  opnum_t prevClientReqOpnum{};
  ::google::protobuf::Message *replyMessage;

  LogEntry() { replyMessage = nullptr; }
  LogEntry(const LogEntry &x)
      : viewstamp(x.viewstamp),
        state(x.state),
        request(x.request),
        hash(x.hash),
        prevClientReqOpnum(x.prevClientReqOpnum) {
    if (x.replyMessage != nullptr) {
      replyMessage = x.replyMessage->New();
      replyMessage->CopyFrom(*x.replyMessage);
    } else {
      replyMessage = nullptr;
    }
  }
  LogEntry(viewstamp_t viewstamp, LogEntryState state, const Request &request,
           const string &hash)
      : viewstamp(viewstamp),
        state(state),
        request(request),
        hash(hash),
        replyMessage(nullptr) {}
  virtual ~LogEntry() {
    if (replyMessage != nullptr) {
      delete replyMessage;
    }
  }
};

class Log {
 public:
  explicit Log(bool useHash, opnum_t start = 1,
               const string &initialHash = EMPTY_HASH);
  LogEntry &Append(viewstamp_t vs, const Request &req, LogEntryState state);
  LogEntry *Find(opnum_t opnum);
  bool SetStatus(opnum_t op, LogEntryState state);
  bool SetRequest(opnum_t op, const Request &req);
  void RemoveAfter(opnum_t op);
  LogEntry *Last();
  viewstamp_t LastViewstamp() const;  // deprecated
  opnum_t LastOpnum() const;
  opnum_t FirstOpnum() const;
  bool Empty() const;
  template <class T>
  void Dump(opnum_t from, T out);
  template <class iter>
  void Install(iter start, iter end);
  const string &LastHash() const;

  static string ComputeHash(const string &lastHash, const LogEntry &entry);
  static const char EMPTY_HASH[];

 private:
  std::vector<LogEntry> entries;
  string initialHash;
  opnum_t start;
  bool useHash;
};

#include "replication/common/log-impl.h"

}  // namespace replication

#endif  // REPLICATION_COMMON_LOG_H_
