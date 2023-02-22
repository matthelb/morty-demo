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
#pragma once

#include <bits/stdint-uintn.h>

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txncontext.h"
#include "store/common/partitioner.h"
#include "store/common/timestamp.h"
#include "store/spannerstore/common.h"
#include "store/spannerstore/preparedtransaction.h"
#include "store/spannerstore/shardclient.h"
#include "store/spannerstore/spanner-proto.pb.h"
#include "store/spannerstore/truetime.h"

class AppContext;
class Partitioner;
class Transport;
namespace transport {
class Configuration;
}  // namespace transport

namespace spannerstore {

class ShardClient;
class TrueTime;

class SpannerContext : public TxnContext {
 public:
  explicit SpannerContext(std::uint64_t transaction_id);
  ~SpannerContext() override;

  std::unique_ptr<TxnContext> Clone() const override;

  std::uint64_t transaction_id() const { return transaction_id_; }

 private:
  std::uint64_t transaction_id_;
};

class SpannerSession {
 public:
  explicit SpannerSession(const Timestamp &start_ts);
  ~SpannerSession();

  const Timestamp &start_ts() const { return start_ts_; }

  const std::set<int> &participants() const { return participants_; }
  const std::unordered_map<std::uint64_t, PreparedTransaction> prepares()
      const {
    return prepares_;
  }

  const Timestamp &snapshot_ts() const { return snapshot_ts_; }

 protected:
  friend class Client;

  void retry_transaction() {
    participants_.clear();
    prepares_.clear();
    values_.clear();
    snapshot_ts_ = Timestamp();
    current_participant_ = -1;
    state_ = EXECUTING;
  }

  enum State {
    EXECUTING = 0,
    GETTING,
    PUTTING,
    COMMITTING,
    NEEDS_ABORT,
    ABORTING
  };

  State state() const { return state_; }

  bool executing() const { return (state_ == EXECUTING); }
  bool needs_aborts() const { return (state_ == NEEDS_ABORT); }

  int current_participant() const { return current_participant_; }

  void set_executing() {
    current_participant_ = -1;
    state_ = EXECUTING;
  }

  void set_getting(int p) {
    current_participant_ = p;
    state_ = GETTING;
  }

  void set_putting(int p) {
    current_participant_ = p;
    state_ = PUTTING;
  }

  void set_committing() { state_ = COMMITTING; }
  void set_needs_abort() { state_ = NEEDS_ABORT; }
  void set_aborting() { state_ = ABORTING; }

  void add_participant(int p) { participants_.insert(p); }
  void clear_participants() { participants_.clear(); }

  std::unordered_map<std::uint64_t, PreparedTransaction> &mutable_prepares() {
    return prepares_;
  }
  std::unordered_map<std::string, std::list<Value>> &mutable_values() {
    return values_;
  }

  void set_snapshot_ts(const Timestamp &ts) { snapshot_ts_ = ts; }

 private:
  Timestamp start_ts_;
  std::set<int> participants_;
  std::unordered_map<std::uint64_t, PreparedTransaction> prepares_;
  std::unordered_map<std::string, std::list<Value>> values_;
  Timestamp snapshot_ts_;
  int current_participant_;
  State state_;
};

// class CommittedTransaction {
//  public:
//   Timestamp commit_ts;
//   bool committed;
// };

enum SnapshotState { WAIT, COMMIT };

struct SnapshotResult {
  SnapshotState state;
  Timestamp max_read_ts;
  std::unordered_map<std::string, std::string> kv_{};
};

class Client : public context::Client {
 public:
  Client(transport::Configuration *config, std::uint64_t client_id,
         int n_shards, int closest_replica, Transport *transport,
         Partitioner *part, TrueTime &tt, CoordinatorSelection coord_select,
         bool debug_stats);
  ~Client() override;

  void Begin(std::unique_ptr<AppContext> &context,
             context::abort_callback acb) override;
  void Retry(std::unique_ptr<AppContext> &context,
             context::abort_callback acb) override;

  void Get(std::unique_ptr<AppContext> context, const std::string &key,
           context::op_callback ocb) override;
  void GetForUpdate(std::unique_ptr<AppContext> context, const std::string &key,
                    context::op_callback ocb) override;

  void Put(std::unique_ptr<AppContext> &context, const std::string &key,
           const std::string &val) override;

  void Commit(std::unique_ptr<AppContext> context,
              context::commit_callback ccb) override;

  void Abort(std::unique_ptr<AppContext> &context) override;

  bool SupportsRO() override { return true; }
  void ROBegin(std::unique_ptr<AppContext> &context,
               context::abort_callback acb) override;
  void ROGet(std::unique_ptr<AppContext> context,
             const std::unordered_set<std::string> &keys,
             context::bulk_op_callback ocb) override;
  void ROCommit(std::unique_ptr<AppContext> context,
                context::commit_callback ccb) override;

  void Cleanup() override;

 private:
  const static std::size_t MAX_SHARDS = 16;

  struct PendingRequest {
    explicit PendingRequest(std::uint64_t id)
        : id{id}, outstanding_prepares{0} {}

    ~PendingRequest() = default;

    context::commit_callback ccb{};
    // abort_callback acb;
    // abort_timeout_callback atcb;
    std::uint64_t id{};
    int outstanding_prepares;
  };

  void GetCallback(AppContext *context, const context::op_callback &ocb,
                   int status, const std::string &key, const std::string &val,
                   const Timestamp &ts);
  void GetTimeoutCallback(AppContext *context, int status,
                          const std::string &key);

  void PutCallback(int status, const std::string &key, const std::string &val);
  void PutTimeoutCallback(int status, const std::string &key,
                          const std::string &val);

  void CommitCallback(AppContext *context, std::uint64_t req_id, int status,
                      const Timestamp &commit_ts, const Timestamp &nonblock_ts);

  void AbortCallback(std::uint64_t transaction_id, std::uint64_t req_id);

  void ROGetCallback(AppContext *context, std::uint64_t req_id,
                     const context::bulk_op_callback &ocb,
                     const std::vector<Value> &values);

  void HandleWound(std::uint64_t transaction_id);

  // choose coordinator from participants
  int ChooseCoordinator(std::uint64_t tid,
                        std::unique_ptr<SpannerSession> &session);

  //   // For tracking RO reply progress
  void AddValues(std::unique_ptr<SpannerSession> &session,
                 const std::vector<Value> &vs);

  Timestamp last_start_ts_;
  std::unordered_map<std::uint64_t, std::unique_ptr<SpannerSession>> sessions_;
  const std::string client_region_;
  const std::string service_name_;
  transport::Configuration *config_;

  // Unique ID for this client.
  std::uint64_t client_id_;

  // Number of shards in SpanStore.
  std::uint64_t n_shards_;

  // Transport used by paxos client proxies.
  Transport *transport_;

  // Client for each shard.
  std::vector<ShardClient *> sclients_;

  // Partitioner
  Partitioner *part_;

  // TrueTime server.
  TrueTime &tt_;

  CoordinatorSelection coord_select_;

  std::uint64_t next_transaction_id_;

  std::uint64_t last_req_id_{};
  std::unordered_map<std::uint64_t, PendingRequest *> pending_reqs_;

  Latency_t op_lat_{};
  Latency_t commit_lat_{};

  Consistency consistency_;

  double nb_time_alpha_;

  bool debug_stats_;
};

}  // namespace spannerstore
