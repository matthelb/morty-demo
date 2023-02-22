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
#include "store/strongstore/unreplicatedserver.h"

#include <google/protobuf/message.h>
#include <google/protobuf/stubs/port.h>

#include <cstdint>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/strongstore/mvtsostore.h"
#include "store/strongstore/occstore.h"

namespace strongstore {

UnreplicatedServer::UnreplicatedServer(Mode mode, Transport *transport,
                                       transport::Configuration &config,
                                       uint64_t groupIdx, int64_t maxDepDepth)
    : transport(transport) {
  transport->Register(this, config, groupIdx, 0);

  switch (mode) {
    case MODE_MVTSO:
      store = new MVTSOStore(maxDepDepth);
      break;
    case MODE_OCC:
      store = new OCCStore();
      break;
    default:
      Panic("Not implemented mode %d.", mode);
  }

  RegisterHandler(&request, static_cast<MessageServer::MessageHandler>(
                                &UnreplicatedServer::HandleRequest));
}

UnreplicatedServer::~UnreplicatedServer() { delete store; }

void UnreplicatedServer::Load(std::string &&key, std::string &&val,
                              Timestamp &&ts) {
  store->Load(key, val, ts);
}

void UnreplicatedServer::HandleRequest(const TransportAddress &remote,
                                       google::protobuf::Message *m) {
  proto::Request &msg = *dynamic_cast<proto::Request *>(m);

  reply.Clear();

  int status = REPLY_OK;
  std::unordered_map<uint64_t, int> statuses;
  switch (msg.op()) {
    case proto::Request::GET: {
      std::pair<Timestamp, std::string> val;
      if (request.get().has_timestamp()) {
        status = store->Get(msg.txnid(), request.get().key(),
                            request.get().timestamp(), val, statuses);
      } else {
        status = store->Get(msg.txnid(), request.get().key(), val);
      }

      if (status == REPLY_OK) {
        reply.set_value(val.second);
        val.first.serialize(reply.mutable_timestamp());
      }
      break;
    }
    case proto::Request::PUT:
      status = store->Put(msg.txnid(), request.put().key(), request.put().val(),
                          request.put().timestamp(), statuses);
      break;
    case proto::Request::PREPARE: {
      status = store->Prepare(msg.txnid(), request.prepare().txn(), statuses);

      if (status == REPLY_WAITING) {
        UW_ASSERT(msg.has_reqid());
        waitingPrepares.insert(std::make_pair(
            msg.txnid(), std::make_pair(msg.reqid(), remote.clone())));
      }

      break;
    }
    case proto::Request::COMMIT:
      store->Commit(msg.txnid(), request.commit().timestamp(), statuses);
      break;
    case proto::Request::ABORT:
      store->Abort(msg.txnid(), request.abort().txn(), statuses);
      break;
    default:
      NOT_REACHABLE();
  }

  if (msg.op() != proto::Request::COMMIT && msg.op() != proto::Request::ABORT &&
      status != REPLY_WAITING) {
    reply.set_status(status);
    if (msg.has_reqid()) {
      reply.set_reqid(msg.reqid());
    }
    transport->SendMessage(this, remote, reply);
  }

  for (const auto &waitingStatus : statuses) {
    auto waitingItr = waitingPrepares.find(waitingStatus.first);
    UW_ASSERT(waitingItr != waitingPrepares.end());

    reply.Clear();

    reply.set_status(status);
    reply.set_reqid(waitingItr->second.first);
    transport->SendMessage(this, *waitingItr->second.second, reply);

    delete waitingItr->second.second;
    waitingPrepares.erase(waitingItr);
  }
}

}  // namespace strongstore
