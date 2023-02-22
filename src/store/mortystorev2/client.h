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
#ifndef _MORTYV2_CLIENT_H_
#define _MORTYV2_CLIENT_H_

#include <bits/stdint-uintn.h>

#include <map>
#include <memory>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/ir/client.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txncontext.h"
#include "store/common/partialorder.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/mortystorev2/common.h"
#include "store/mortystorev2/morty-proto.pb.h"
#include "store/mortystorev2/shardclient.h"
#include "store/mortystorev2/version.h"

class Partitioner;
class Transport;
namespace transport {
class Configuration;
}  // namespace transport

namespace mortystorev2 {

class ShardClient;

class Client : public context::Client {
 public:
  Client(transport::Configuration *config, uint64_t client_id, int num_shards,
         int num_groups, uint64_t branch_backoff_factor,
         uint64_t branch_backoff_max, bool logical_timestamps,
         bool fast_path_enabled, bool pipeline_commit, bool reexecution_enabled,
         uint64_t slow_path_timeout_ms, uint64_t server_num_workers,
         int closest_replica, Transport *transport, Partitioner *part);

  ~Client() override;

  virtual void Begin(std::unique_ptr<AppContext> &context,
                     context::abort_callback acb);

  void Get(std::unique_ptr<AppContext> context, const std::string &key,
           context::op_callback ocb) override;

  void Put(std::unique_ptr<AppContext> &context, const std::string &key,
           const std::string &val) override;

  void Commit(std::unique_ptr<AppContext> context,
              context::commit_callback ccb) override;

  void Abort(std::unique_ptr<AppContext> &context) override;

 private:
  class MortyContext : public TxnContext {
   public:
    MortyContext();
    MortyContext(const MortyContext &other);
    ~MortyContext() override;

    virtual std::unique_ptr<TxnContext> Clone() const;

    uint64_t txn_seq;
    uint64_t exec_id;

    bool user_aborted;
    std::vector<int> participants;
    std::map<int, std::unordered_map<
                      const std::string *,
                      std::tuple<const std::string *, uint64_t,
                                 ::mortystorev2::Version, uint64_t, bool>>>
        read_set;
    std::map<int, std::unordered_map<const std::string *,
                                     std::tuple<const std::string *, uint64_t>>>
        write_set;
    // std::map<int, std::set<Version>> deps;
    PartialOrder op_order;

    struct CommitMetadata {
      enum CommitState {
        COMMIT_STATE_PREPARING = 0,
        COMMIT_STATE_FINALIZING = 1,
        COMMIT_STATE_DECIDED = 2
      };
      CommitMetadata() {}
      CommitState state;
      proto::Decision decision{proto::DECISION_COMMIT};
      bool fast{true};
      std::map<int, proto::Decision> group_decisions{};
      std::set<int> groups_finalized{};
      std::set<int> groups_decided{};
      std::map<int, proto::Transaction *> group_txns{};
      context::commit_callback ccb{};
    };
    std::unique_ptr<CommitMetadata> commit_metadata;
  };

  struct Continuation {
    explicit Continuation(std::unique_ptr<AppContext> c) : ctx(std::move(c)) {}
    std::unique_ptr<AppContext> ctx{};
    context::op_callback ocb{};
  };

  struct PendingTransaction {
    PendingTransaction(uint64_t seq, uint64_t client_id)
        : version(client_id, seq),
          last_cont_id(0UL),
          last_exec_id(0UL),
          executing_context(nullptr),
          write_val_counter(0UL),
          sent_decide_or_finalize(false) {}

    virtual ~PendingTransaction() = default;

    Version version;

    uint64_t last_cont_id;
    std::unordered_map<uint64_t, std::unique_ptr<Continuation>> continuations;
    std::set<int> participants{};
    uint64_t last_exec_id;
    std::unordered_map<uint64_t, PartialOrder> executions{};
    context::abort_callback acb{};

    const MortyContext *executing_context;
    std::unordered_map<uint64_t, std::unique_ptr<AppContext>>
        committing_contexts;
    uint64_t write_val_counter;
    bool sent_decide_or_finalize;
    std::unordered_map<const std::string *,
                       std::unordered_map<const std::string *, uint64_t>>
        write_val_cache;

    std::unordered_set<std::string> keys{};
    std::unordered_set<std::string> vals{};
  };

  void AbortContext(uint64_t req_id, uint64_t cont_id,
                    std::unique_ptr<PendingTransaction> &txn,
                    MortyContext *txn_context);

  void Prepare(std::unique_ptr<PendingTransaction> &txn,
               MortyContext *txn_context);
  void Finalize(std::unique_ptr<PendingTransaction> &txn,
                MortyContext *txn_context);
  void Decide(std::unique_ptr<PendingTransaction> &txn,
              MortyContext *txn_context);

  void GetCallback(int group, uint64_t txn_seq, uint64_t cont_id,
                   const std::string *key_ptr,
                   context::transaction_op_status_t status,
                   const std::string &val, uint64_t val_id,
                   const Version &version, bool committed, uint64_t req_id,
                   bool version_below_watermark);
  void PrepareCallback(int group, uint64_t txn_seq, uint64_t exec_id,
                       proto::Decision decision, bool fast);
  void FinalizeCallback(int group, uint64_t txn_seq, uint64_t exec_id,
                        proto::Decision decision);
  void DecideCallback(int group, uint64_t txn_seq, uint64_t exec_id,
                      proto::Decision decision, bool deps_changed_decision);

  void SerializeTransaction(MortyContext *txn_context, proto::Transaction *&txn,
                            int group);
  void CleanupTransactionContext(MortyContext *txn_context);
  void CleanupTransaction(std::unique_ptr<PendingTransaction> &txn);
  void FreeTransaction(proto::Transaction *txn);

  transport::Configuration *config_;
  const uint64_t id_;
  const uint64_t num_shards_;
  const uint64_t num_groups_;
  const uint64_t branch_backoff_factor_;
  const uint64_t branch_backoff_max_;
  const bool fast_path_enabled_;
  const bool pipeline_commit_;
  const bool reexecution_enabled_;
  const uint64_t server_num_workers_;
  std::mt19937 rand_;
  Transport *transport_;
  Partitioner *part_;
  std::vector<std::unique_ptr<ShardClient>> shard_clients_;

  uint64_t last_seq_;
  std::unordered_map<uint64_t, std::unique_ptr<PendingTransaction>>
      pending_txns_;
  TrueTime time_;

  std::queue<proto::Transaction *> txn_queue_;
};

}  // namespace mortystorev2

#endif /* _MORTYV2_CLIENT_H_ */
