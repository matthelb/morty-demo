// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/server.cc:
 *   Implementation of a single transactional key-value server with strong
 *consistency.
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

#include "store/strongstore/server.h"

#include <unordered_map>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common-proto.pb.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/strongstore/lockstore.h"
#include "store/strongstore/occstore.h"

namespace strongstore {

using namespace std;
using namespace proto;

Server::Server(Mode mode, uint64_t skew, uint64_t error) : mode(mode) {
  timeServer = TrueTime(skew, error);

  switch (mode) {
    case MODE_LOCK:
    case MODE_SPAN_LOCK:
      store = new strongstore::LockStore();
      break;
    case MODE_OCC:
    case MODE_SPAN_OCC:
      store = new strongstore::OCCStore();
      break;
    default:
      NOT_REACHABLE();
  }
  _Latency_Init(&readLat, "read_lat");
}

Server::~Server() {
  Latency_Dump(&readLat);
  delete store;
}

void Server::LeaderUpcall(opnum_t opnum, const string &str1, bool &replicate,
                          string &str2) {
  Debug("Received LeaderUpcall: %lu %s", opnum, str1.c_str());

  int status;

  request.Clear();
  reply.Clear();
  request.ParseFromString(str1);

  switch (request.op()) {
    case strongstore::proto::Request::GET:
      // Latency_Start(&readLat);
      if (request.get().has_timestamp()) {
        std::unordered_map<uint64_t, int> statuses;
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
      replicate = false;
      reply.set_status(status);
      reply.SerializeToString(&str2);
      // Latency_End(&readLat);
      break;
    case strongstore::proto::Request::PREPARE: {
      std::unordered_map<uint64_t, int> statuses;
      // Prepare is the only case that is conditionally run at the leader

      status = store->Prepare(request.txnid(),
                              Transaction(request.prepare().txn()), statuses);

      // if prepared, then replicate result
      if (status == 0) {
        replicate = true;
        // get a prepare timestamp and send along to replicas
        if (mode == MODE_SPAN_LOCK || mode == MODE_SPAN_OCC) {
          request.mutable_prepare()->mutable_timestamp()->set_timestamp(
              timeServer.GetTime());
          request.mutable_prepare()->mutable_timestamp()->set_id(0);
        }
        request.SerializeToString(&str2);
      } else {
        // if abort, don't replicate
        replicate = false;
        reply.set_status(status);
        reply.SerializeToString(&str2);
      }
      break;
    }
    case strongstore::proto::Request::COMMIT:
      replicate = true;
      str2 = str1;
      break;
    case strongstore::proto::Request::ABORT:
      replicate = true;
      str2 = str1;
      break;
    default:
      Panic("Unrecognized operation.");
  }
}

/* Gets called when a command is issued using client.Invoke(...) to this
 * replica group.
 * opnum is the operation number.
 * str1 is the request string passed by the client.
 * str2 is the reply which will be sent back to the client.
 */
void Server::ReplicaUpcall(opnum_t opnum, const string &str1, string &str2) {
  Debug("Received Upcall: %lu %s", opnum, str1.c_str());
  int status = 0;

  request.Clear();
  reply.Clear();

  request.ParseFromString(str1);

  std::unordered_map<uint64_t, int> statuses;
  switch (request.op()) {
    case strongstore::proto::Request::GET:
      return;
    case strongstore::proto::Request::PREPARE: {
      // get a prepare timestamp and return to client
      store->Prepare(request.txnid(), Transaction(request.prepare().txn()),
                     statuses);
      if (mode == MODE_SPAN_LOCK || mode == MODE_SPAN_OCC) {
        *reply.mutable_timestamp() = request.prepare().timestamp();
      }
      break;
    }
    case strongstore::proto::Request::COMMIT:
      store->Commit(request.txnid(), request.commit().timestamp(), statuses);
      break;
    case strongstore::proto::Request::ABORT:
      store->Abort(request.txnid(), Transaction(request.abort().txn()),
                   statuses);
      break;
    default:
      Panic("Unrecognized operation.");
  }
  reply.set_status(status);
  reply.SerializeToString(&str2);
}

void Server::UnloggedUpcall(const string &str1, string &str2) {
  // Latency_Start(&readLat);
  int status;
  request.Clear();
  reply.Clear();

  request.ParseFromString(str1);

  UW_ASSERT(request.op() == strongstore::proto::Request::GET);

  if (request.get().has_timestamp()) {
    std::unordered_map<uint64_t, int> statuses;

    std::pair<Timestamp, std::string> val;
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
  reply.SerializeToString(&str2);

  // Latency_End(&readLat);
}

void Server::Load(string &&key, string &&value, Timestamp &&timestamp) {
  store->Load(key, value, timestamp);
}

}  // namespace strongstore
