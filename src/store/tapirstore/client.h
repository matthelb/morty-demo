// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/client.h:
 *   Tapir client interface.
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
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

#ifndef _TAPIR_CLIENT_H_
#define _TAPIR_CLIENT_H_

#include <bits/stdint-uintn.h>

#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/ir/client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txncontext.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/common/truetime.h"
#include "store/tapirstore/shardclient.h"
#include "store/tapirstore/tapir-proto.pb.h"

class AppContext;
class BufferClient;
class Partitioner;
class Transport;
namespace transport {
class Configuration;
}  // namespace transport

namespace tapirstore {
class ShardClient;
}  // namespace tapirstore

#define RESULT_COMMITTED 0
#define RESULT_USER_ABORTED 1
#define RESULT_SYSTEM_ABORTED 2
#define RESULT_MAX_RETRIES 3

namespace tapirstore {

class Client : public context::Client {
 public:
  Client(transport::Configuration *config, uint64_t id, int nShards,
         int nGroups, int closestReplica, Transport *transport,
         Partitioner *part, bool pingReplicas, bool syncCommit,
         bool resend_on_timeout, const TrueTime &timeServer = TrueTime(0, 0));
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
  class TapirContext : public TxnContext {
   public:
    TapirContext();
    TapirContext(const TapirContext &other);
    ~TapirContext() override;

    virtual std::unique_ptr<TxnContext> Clone() const;

    uint64_t t_id{};
    context::abort_callback acb;
    std::set<int> participants;

    struct CommitMetadata {
      CommitMetadata()

      {}

      context::commit_callback ccb{};
      int outstanding_prepares{0};
      int commit_tries{0};
      uint64_t max_replied_ts{0};
      int prepare_status{REPLY_OK};
      Timestamp prepare_ts;
    };
    std::unique_ptr<CommitMetadata> commit_metadata;
  };

  void GetCallback(AppContext *ctx, const context::op_callback &ocb, int status,
                   const std::string &key, const std::string &val,
                   const Timestamp &ts);
  void GetTimeoutCallback(int status, const std::string &key);
  void PutCallback(int status, const std::string &key, const std::string &val);
  void PutTimeoutCallback(int status, const std::string &key,
                          const std::string &vak);

  // Prepare function
  void Prepare(TapirContext *txn_context);
  void PrepareCallback(uint64_t t_id, int status, const Timestamp &ts);
  void HandleAllPreparesReceived(TapirContext *txn_context);
  void AbortInternal(TapirContext *txn_context, const abort_callback &acb,
                     const abort_timeout_callback &atcb, uint32_t timeout);

  transport::Configuration *config;
  // Unique ID for this client.
  uint64_t client_id;

  // Ongoing transaction ID.
  uint64_t t_id{};

  // Number of shards.
  uint64_t nshards;
  uint64_t ngroups;

  // Number of retries for current transaction.
  long retries{};

  // List of participants in the ongoing transaction.
  std::set<int> participants;

  // Transport used by IR client proxies.
  Transport *transport;

  // Buffering client for each shard.
  std::vector<BufferClient *> bclient;
  std::vector<ShardClient *> sclient;

  Partitioner *part;

  const bool pingReplicas;
  const bool syncCommit;

  // TrueTime server.
  TrueTime timeServer;

  uint64_t lastReqId;
  std::unordered_map<uint64_t, std::unique_ptr<AppContext>> pendingReqs;
  std::unordered_map<std::string, uint32_t> statInts;

  bool first;
  bool startedPings;
};

}  // namespace tapirstore

#endif /* _TAPIR_CLIENT_H_ */
