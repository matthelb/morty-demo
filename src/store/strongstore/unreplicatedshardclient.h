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
#ifndef STORE_STRONGSTORE_UNREPLICATEDSHARDCLIENT_H_
#define STORE_STRONGSTORE_UNREPLICATEDSHARDCLIENT_H_

#include <bits/stdint-uintn.h>

#include <string>
#include <unordered_map>
#include <utility>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/strongstore/common.h"
#include "store/strongstore/strong-proto.pb.h"

namespace transport {
class Configuration;
}  // namespace transport

namespace strongstore {

class UnreplicatedShardClient : public TxnClient, public TransportReceiver {
 public:
  UnreplicatedShardClient(Mode mode, transport::Configuration *config,
                          Transport *transport, uint64_t client_id, int shard);
  ~UnreplicatedShardClient() override;

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  void Begin(uint64_t id) override;
  void Get(uint64_t id, const std::string &key, get_callback gcb,
           get_timeout_callback gtcb, uint32_t timeout) override;
  void Get(uint64_t id, const std::string &key, const Timestamp &timestamp,
           get_callback gcb, get_timeout_callback gtcb,
           uint32_t timeout) override;
  void Put(uint64_t id, const std::string &key, const std::string &value,
           put_callback pcb, put_timeout_callback ptcb,
           uint32_t timeout) override;
  void Put(uint64_t id, const std::string &key, const std::string &value,
           const Timestamp &ts, put_callback pcb, put_timeout_callback ptcb,
           uint32_t timeout) override;
  void Commit(uint64_t id, const Transaction &txn, const Timestamp &timestamp,
              commit_callback ccb, commit_timeout_callback ctcb,
              uint32_t timeout) override;
  void Abort(uint64_t id, const Transaction &txn, abort_callback acb,
             abort_timeout_callback atcb, uint32_t timeout) override;
  void Prepare(uint64_t id, const Transaction &txn, const Timestamp &timestamp,
               prepare_callback pcb, prepare_timeout_callback ptcb,
               uint32_t timeout) override;

 private:
  struct PendingRequest {
    PendingRequest(uint64_t reqId, proto::Request::Operation type)
        : reqId(reqId), type(type) {}
    virtual ~PendingRequest() = default;

    uint64_t reqId;
    proto::Request::Operation type;
  };
  struct PendingGet : public PendingRequest {
    PendingGet(uint64_t reqId, const Timestamp &ts, const std::string &key,
               get_callback gcb, get_timeout_callback gtcb)
        : PendingRequest(reqId, proto::Request::GET),
          ts(ts),
          key(key),
          gcb(std::move(gcb)),
          gtcb(gtcb) {}
    Timestamp ts;
    std::string key{};
    get_callback gcb{};
    get_timeout_callback gtcb;
  };
  struct PendingPut : public PendingRequest {
    PendingPut(uint64_t reqId, const std::string &key, const std::string &value,
               put_callback pcb, put_timeout_callback ptcb)
        : PendingRequest(reqId, proto::Request::PUT),
          key(key),
          value(value),
          pcb(std::move(pcb)),
          ptcb(ptcb) {}
    std::string key{};
    std::string value{};
    put_callback pcb{};
    put_timeout_callback ptcb;
  };
  struct PendingPrepare : public PendingRequest {
    PendingPrepare(uint64_t reqId, prepare_callback pcb,
                   prepare_timeout_callback ptcb)
        : PendingRequest(reqId, proto::Request::PREPARE),
          pcb(pcb),
          ptcb(ptcb) {}
    Timestamp ts;
    prepare_callback pcb;
    prepare_timeout_callback ptcb;
  };

  void HandleGetReply(PendingGet *get, const proto::Reply &msg);
  void HandlePutReply(PendingPut *put, const proto::Reply &msg);
  void HandlePrepareReply(PendingPrepare *prepare, const proto::Reply &msg);

  transport::Configuration *config;
  Transport *transport;
  const uint64_t client_id;
  const int shard;

  std::unordered_map<uint64_t, PendingRequest *> pendingReqs;

  proto::Request request;
  proto::Reply reply;
};

}  // namespace strongstore

#endif  // STRONGSTORE_UNREPLICATED_SHARDCLIENT_H_
