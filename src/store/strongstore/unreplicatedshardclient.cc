// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/txnstore/shardclient.h:
 *   Single shard transactional client interface.
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

#include "store/strongstore/unreplicatedshardclient.h"

#include <bits/std_function.h>

#include <cstdint>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common-proto.pb.h"

namespace strongstore {

UnreplicatedShardClient::UnreplicatedShardClient(
    Mode mode, transport::Configuration *config, Transport *transport,
    uint64_t client_id, int shard)
    : config(config), transport(transport), client_id(client_id), shard(shard) {
  transport->Register(this, *config, -1, -1);
}

UnreplicatedShardClient::~UnreplicatedShardClient() = default;

void UnreplicatedShardClient::ReceiveMessage(const TransportAddress &remote,
                                             std::string *type,
                                             std::string *data,
                                             void *meta_data) {
  if (*type == reply.GetTypeName()) {
    reply.ParseFromString(*data);

    if (!reply.has_reqid()) {
      Panic("Reply missing reqid.");
    }

    auto reqItr = pendingReqs.find(reply.reqid());
    UW_ASSERT(reqItr != pendingReqs.end());

    PendingRequest *req = reqItr->second;

    switch (req->type) {
      case proto::Request::GET:
        HandleGetReply(dynamic_cast<PendingGet *>(req), reply);
        break;
      case proto::Request::PUT:
        HandlePutReply(dynamic_cast<PendingPut *>(req), reply);
        break;
      case proto::Request::PREPARE:
        HandlePrepareReply(dynamic_cast<PendingPrepare *>(req), reply);
        break;
      default:
        NOT_REACHABLE();
    }

    delete req;
    pendingReqs.erase(reqItr);
  } else {
    Panic("Unknown message type %s.", type->c_str());
  }
}

void UnreplicatedShardClient::Begin(uint64_t id) {
  Debug("[shard %i] BEGIN: %lu", shard, id);
}

void UnreplicatedShardClient::Get(uint64_t id, const std::string &key,
                                  get_callback gcb, get_timeout_callback gtcb,
                                  uint32_t timeout) {
  Debug("[shard %i] Sending GET [%s]", shard, key.c_str());

  request.Clear();

  uint64_t reqId = lastReqId++;

  request.set_op(proto::Request::GET);
  request.set_txnid(id);
  request.mutable_get()->set_key(key);
  request.set_reqid(reqId);

  PendingGet *pendingGet = new PendingGet(reqId, Timestamp(), key, gcb, gtcb);
  pendingReqs[reqId] = pendingGet;

  transport->SendMessageToReplica(this, shard, 0, request);
}

void UnreplicatedShardClient::Get(uint64_t id, const std::string &key,
                                  const Timestamp &timestamp, get_callback gcb,
                                  get_timeout_callback gtcb, uint32_t timeout) {
  Debug("[shard %i] Sending GET [%s]", shard, key.c_str());

  request.Clear();

  uint64_t reqId = lastReqId++;

  request.set_op(proto::Request::GET);
  request.set_txnid(id);
  request.mutable_get()->set_key(key);
  timestamp.serialize(request.mutable_get()->mutable_timestamp());
  request.set_reqid(reqId);

  PendingGet *pendingGet = new PendingGet(reqId, timestamp, key, gcb, gtcb);
  pendingReqs[reqId] = pendingGet;

  transport->SendMessageToReplica(this, shard, 0, request);
}

void UnreplicatedShardClient::Put(uint64_t id, const std::string &key,
                                  const std::string &value, put_callback pcb,
                                  put_timeout_callback ptcb, uint32_t timeout) {
  Debug("[shard %i] Sending PUT [%s]", shard, key.c_str());

  request.Clear();

  uint64_t reqId = lastReqId++;

  request.set_op(proto::Request::PUT);
  request.set_txnid(id);
  request.mutable_put()->set_key(key);
  request.mutable_put()->set_val(value);
  request.set_reqid(reqId);

  PendingPut *pendingPut = new PendingPut(reqId, key, value, pcb, ptcb);
  pendingReqs[reqId] = pendingPut;

  transport->SendMessageToReplica(this, shard, 0, request);
}

void UnreplicatedShardClient::Put(uint64_t id, const std::string &key,
                                  const std::string &value, const Timestamp &ts,
                                  put_callback pcb, put_timeout_callback ptcb,
                                  uint32_t timeout) {
  Debug("[shard %i] Sending PUT [%s]", shard, key.c_str());

  request.Clear();

  uint64_t reqId = lastReqId++;

  request.set_op(proto::Request::PUT);
  request.set_txnid(id);
  request.mutable_put()->set_key(key);
  request.mutable_put()->set_val(value);
  ts.serialize(request.mutable_put()->mutable_timestamp());
  request.set_reqid(reqId);

  PendingPut *pendingPut = new PendingPut(reqId, key, value, pcb, ptcb);
  pendingReqs[reqId] = pendingPut;

  transport->SendMessageToReplica(this, shard, 0, request);
}

void UnreplicatedShardClient::Prepare(uint64_t id, const Transaction &txn,
                                      const Timestamp &timestamp,
                                      prepare_callback pcb,
                                      prepare_timeout_callback ptcb,
                                      uint32_t timeout) {
  Debug("[shard %i] Sending PREPARE: %lu", shard, id);

  request.Clear();

  uint64_t reqId = lastReqId++;

  request.set_op(proto::Request::PREPARE);
  request.set_txnid(id);
  txn.serialize(request.mutable_prepare()->mutable_txn());
  request.set_reqid(reqId);

  PendingPrepare *pendingPrepare = new PendingPrepare(reqId, pcb, ptcb);
  pendingReqs[reqId] = pendingPrepare;

  transport->SendMessageToReplica(this, shard, 0, request);
}

void UnreplicatedShardClient::Commit(uint64_t id, const Transaction &txn,
                                     const Timestamp &timestamp,
                                     commit_callback ccb,
                                     commit_timeout_callback ctcb,
                                     uint32_t timeout) {
  Debug("[shard %i] Sending COMMIT: %lu", shard, id);

  request.Clear();

  request.set_op(proto::Request::COMMIT);
  request.set_txnid(id);
  timestamp.serialize(request.mutable_commit()->mutable_timestamp());

  transport->SendMessageToReplica(this, shard, 0, request);
  ccb(COMMITTED);
}

void UnreplicatedShardClient::Abort(uint64_t id, const Transaction &txn,
                                    abort_callback acb,
                                    abort_timeout_callback atcb,
                                    uint32_t timeout) {
  Debug("[shard %i] Sending ABORT: %lu", shard, id);

  request.Clear();

  request.set_op(proto::Request::ABORT);
  request.set_txnid(id);
  txn.serialize(request.mutable_abort()->mutable_txn());

  transport->SendMessageToReplica(this, shard, 0, request);
  acb();
}

void UnreplicatedShardClient::HandleGetReply(PendingGet *get,
                                             const proto::Reply &msg) {
  Debug("[shard %lu:%i] GET callback [%d] %lx %lu.%lu", client_id, shard,
        msg.status(), *((const uint64_t *)get->key.c_str()),
        msg.timestamp().timestamp(), msg.timestamp().id());
  if (msg.has_timestamp()) {
    get->gcb(msg.status(), get->key, msg.value(), msg.timestamp());
  } else {
    get->gcb(msg.status(), get->key, msg.value(), Timestamp());
  }
}

void UnreplicatedShardClient::HandlePutReply(PendingPut *put,
                                             const proto::Reply &msg) {
  put->pcb(msg.status(), put->key, put->value);
}

void UnreplicatedShardClient::HandlePrepareReply(PendingPrepare *prepare,
                                                 const proto::Reply &msg) {
  Debug("[shard %i] Received PREPARE callback [%d]", shard, msg.status());

  if (msg.has_timestamp()) {
    prepare->pcb(msg.status(), Timestamp(msg.timestamp()));
  } else {
    prepare->pcb(msg.status(), Timestamp());
  }
}

/*void UnreplicatedShardClient::GetTimeout(uint64_t reqId) {
  Warning("[shard %i] GET[%lu] timeout.", shard, reqId);
  auto itr = this->pendingGets.find(reqId);
  if (itr != this->pendingGets.end()) {
    PendingGet *pendingGet = itr->second;
    get_timeout_callback gtcb = pendingGet->gtcb;
    std::string key = pendingGet->key;
    this->pendingGets.erase(itr);
    delete pendingGet;
    gtcb(REPLY_TIMEOUT, key);
  }
}*/

}  // namespace strongstore
