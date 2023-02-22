// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/client.h:
 *   Transactional client interface.
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
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

#ifndef _STRONG_CLIENT_H_
#define _STRONG_CLIENT_H_

#include <bits/stdint-uintn.h>

#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txncontext.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"
#include "store/strongstore/common.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"
#include "store/strongstore/unreplicatedshardclient.h"

class AppContext;
class BufferClient;
class Partitioner;
class Transport;
class TxnClient;
namespace transport {
class Configuration;
}  // namespace transport

namespace replication {
namespace vr {
class VRClient;
}  // namespace vr
}  // namespace replication

namespace strongstore {

class Client : public context::Client {
 public:
  Client(Mode mode, transport::Configuration *config, uint64_t id, int nshards,
         int ngroups, int closestReplica, Transport *transport,
         Partitioner *part, bool unreplicated, const TrueTime &timeServer);
  ~Client() override;

  void Begin(std::unique_ptr<AppContext> &context,
             context::abort_callback acb) override;

  void Get(std::unique_ptr<AppContext> context, const std::string &key,
           context::op_callback ocb) override;

  void Put(std::unique_ptr<AppContext> &context, const std::string &key,
           const std::string &val) override;

  void Commit(std::unique_ptr<AppContext> context,
              context::commit_callback ccb) override;

  void Abort(std::unique_ptr<AppContext> &context) override;

 private:
  struct StrongContext : public TxnContext {
    StrongContext();
    StrongContext(const StrongContext &other);
    ~StrongContext() override;

    virtual std::unique_ptr<TxnContext> Clone() const;

    uint64_t t_id{};
    Timestamp ts;
    std::set<int> participants;
    context::abort_callback acb;

    struct CommitMetadata {
      CommitMetadata()

      {}

      int outstandingPrepares{0};
      int commitTries{0};
      uint64_t maxRepliedTs{0UL};
      int prepareStatus{REPLY_OK};
      bool callbackInvoked{false};
      bool spannerSleep{false};
      context::commit_callback ccb{};
    };
    std::unique_ptr<CommitMetadata> commit_metadata;
  };

  void GetCallback(AppContext *ctx, const ::context::op_callback &ocb,
                   int status, const std::string &key, const std::string &val,
                   const Timestamp &ts);
  void GetTimeoutCallback(int status, const std::string &key);
  void PutCallback(AppContext *ctx, int status, const std::string &key,
                   const std::string &val);
  void PutTimeoutCallback(int status, const std::string &key,
                          const std::string &val);

  // timestamp server call back
  bool tssCallback(uint64_t t_id, const string &request, const string &reply);

  void Prepare(StrongContext *txn_context);
  void PrepareCallback(uint64_t t_id, int status, const Timestamp &respTs);
  void HandleAllPreparesReceived(StrongContext *txn_context);
  void AbortInternal(StrongContext *txn_context, const abort_callback &acb,
                     const abort_timeout_callback &atcb, uint32_t timeout);

  transport::Configuration *config;

  // Unique ID for this client.
  uint64_t client_id;

  // Ongoing transaction ID.
  uint64_t last_t_id;

  // Number of shards in SpanStore.
  int nshards;
  int ngroups;

  // List of participants in the ongoing transaction.

  // Transport used by paxos client proxies.
  Transport *transport;

  // Buffering client for each shard.
  std::vector<BufferClient *> bclient;

  // Mode in which spanstore runs.
  Mode mode;

  // Timestamp server shard.
  replication::vr::VRClient *tss{};

  Partitioner *part;

  // TrueTime server.
  TrueTime timeServer;
  bool remoteTimeServer;

  std::unordered_map<uint64_t, std::unique_ptr<AppContext>> pendingReqs;
  std::unordered_map<std::string, uint32_t> statInts;

  std::vector<TxnClient *> sclients;
  Latency_t opLat{};
};

}  // namespace strongstore

#endif /* _STRONG_CLIENT_H_ */
