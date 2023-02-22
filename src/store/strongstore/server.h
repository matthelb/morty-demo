// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/server.h:
 *   A single transactional server replica.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#ifndef _STRONG_SERVER_H_
#define _STRONG_SERVER_H_

#include <bits/stdint-uintn.h>

#include <string>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/udptransport.h"
#include "replication/common/replica.h"
#include "replication/common/viewstamp.h"
#include "replication/vr/replica.h"
#include "store/common/backend/txnstore.h"
#include "store/common/stats.h"
#include "store/common/truetime.h"
#include "store/server.h"
#include "store/strongstore/common.h"
#include "store/strongstore/lockstore.h"
#include "store/strongstore/occstore.h"
#include "store/strongstore/strong-proto.pb.h"

class Timestamp;

namespace strongstore {

class Server : public replication::AppReplica, public ::Server {
 public:
  Server(Mode mode, uint64_t skew, uint64_t error);
  ~Server() override;

  virtual void LeaderUpcall(opnum_t opnum, const string &str1, bool &replicate,
                            string &str2);
  virtual void ReplicaUpcall(opnum_t opnum, const string &str1, string &str2);
  virtual void UnloggedUpcall(const string &str1, string &str2);
  void Load(string &&key, string &&value, Timestamp &&timestamp) override;
  inline Stats &GetStats() override { return store->GetStats(); }

 private:
  Mode mode;
  TxnStore *store;
  TrueTime timeServer;
  Stats stats;
  proto::Request request;
  proto::Reply reply;
  Latency_t readLat{};
};

}  // namespace strongstore

#endif /* _STRONG_SERVER_H_ */
