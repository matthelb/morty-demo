// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
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

#include "store/tapirstore/server.h"

#include <bits/stdint-uintn.h>

#include <unordered_map>
#include <utility>

#include "lib/message.h"
#include "store/common/common-proto.pb.h"
#include "store/common/timestamp.h"
#include "store/tapirstore/store.h"

namespace tapirstore {

using namespace std;

Server::Server(bool linearizable) { store = new Store(linearizable); }

Server::~Server() { delete store; }

void Server::ExecInconsistentUpcall(const string &str1) {
  Debug("Received Inconsistent Request: %s", str1.substr(0, 10).c_str());

  request.ParseFromString(str1);

  std::unordered_map<uint64_t, int> statuses;
  switch (request.op()) {
    case tapirstore::proto::Request::COMMIT: {
      Timestamp ts(request.commit().timestamp());
      store->Commit(request.txnid(), ts, statuses);
      break;
    }
    case tapirstore::proto::Request::ABORT:
      store->Abort(request.txnid(), Transaction(request.abort().txn()),
                   statuses);
      break;
    default:
      Panic("Unrecognized inconsisternt operation %d.", request.op());
  }
}

void Server::ExecConsensusUpcall(const std::string &str1, std::string *str2) {
  Debug("Received Consensus Request: %s", str1.substr(0, 10).c_str());

  int status;
  Timestamp proposed;

  request.ParseFromString(str1);

  switch (request.op()) {
    case tapirstore::proto::Request::PREPARE: {
      Transaction txn(request.prepare().txn());
      status =
          store->Prepare(request.txnid(), txn,
                         Timestamp(request.prepare().timestamp()), proposed);
      reply.set_status(status);
      if (proposed.isValid()) {
        proposed.serialize(reply.mutable_timestamp());
      }
      reply.SerializeToString(str2);
      break;
    }
    default:
      Panic("Unrecognized consensus operation.");
  }
}

void Server::UnloggedUpcall(const std::string &str1, std::string *str2) {
  Debug("Received Unlogged Request: %s", str1.substr(0, 10).c_str());

  int status;

  request.ParseFromString(str1);

  std::unordered_map<uint64_t, int> statuses;
  switch (request.op()) {
    case tapirstore::proto::Request::PING:
      *reply.mutable_ping() = request.ping();
      reply.set_status(REPLY_OK);
      reply.SerializeToString(str2);
      break;
    case tapirstore::proto::Request::GET:
      if (request.get().has_timestamp()) {
        pair<Timestamp, string> val;
        status = store->Get(request.txnid(), request.get().key(),
                            request.get().timestamp(), val, statuses);
        if (status == 0) {
          reply.set_value(val.second);
        }
      } else {
        pair<Timestamp, string> val;
        status = store->Get(request.txnid(), request.get().key(), val);
        if (status == 0) {
          reply.set_value(val.second);
          val.first.serialize(reply.mutable_timestamp());
        }
      }
      reply.set_status(status);
      reply.SerializeToString(str2);
      break;
    default:
      Panic("Unrecognized Unlogged request.");
  }
}

void Server::Sync(const std::map<opid_t, RecordEntry> &record) {
  Panic("Unimplemented!");
}

std::map<opid_t, std::string> Server::Merge(
    const std::map<opid_t, std::vector<RecordEntry>> &d,
    const std::map<opid_t, std::vector<RecordEntry>> &u,
    const std::map<opid_t, std::string> &majority_results_in_d) {
  Panic("Unimplemented!");
}

void Server::Load(string &&key, string &&value, Timestamp &&timestamp) {
  store->Load(key, value, timestamp);
}

}  // namespace tapirstore
