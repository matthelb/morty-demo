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

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/spannerstore/spanner-proto.pb.h"

class Transport;
namespace transport {
class Configuration;
}  // namespace transport

namespace replication {
namespace vr {
class VRClient;
}  // namespace vr
}  // namespace replication

namespace spannerstore {

class ReplicaClient {
  using prepare_callback = std::function<void(int, Timestamp)>;
  using prepare_timeout_callback = std::function<void(int, Timestamp)>;

  using commit_callback = std::function<void(transaction_status_t)>;
  using commit_timeout_callback = std::function<void()>;

  using abort_callback = std::function<void()>;
  using abort_timeout_callback = std::function<void()>;

 public:
  /* Constructor needs path to shard config. */
  ReplicaClient(const transport::Configuration &config, Transport *transport,
                uint64_t client_id, int shard);
  virtual ~ReplicaClient();

  void Prepare(uint64_t transaction_id, const Transaction &transaction,
               const Timestamp &prepare_ts, int coordinator,
               const Timestamp &nonblock_ts, prepare_callback pcb,
               prepare_timeout_callback ptcb, uint32_t timeout);

  void CoordinatorCommit(uint64_t transaction_id, const Timestamp &start_ts,
                         int coordinator, std::unordered_set<int> participants,
                         const Transaction &transaction,
                         const Timestamp &nonblock_ts,
                         const Timestamp &commit_ts, commit_callback ccb,
                         commit_timeout_callback ctcb, uint32_t timeout);

  void Commit(uint64_t transaction_id, Timestamp &commit_timestamp,
              commit_callback ccb, commit_timeout_callback ctcb,
              uint32_t timeout);

  void Abort(uint64_t transaction_id, abort_callback acb,
             abort_timeout_callback atcb, uint32_t timeout);

 private:
  struct PendingRequest {
    explicit PendingRequest(uint64_t reqId) : reqId(reqId) {}
    uint64_t reqId;
  };
  struct PendingPrepare : public PendingRequest {
    explicit PendingPrepare(uint64_t reqId) : PendingRequest(reqId) {}
    prepare_callback pcb{};
    prepare_timeout_callback ptcb{};
  };
  struct PendingCommit : public PendingRequest {
    explicit PendingCommit(uint64_t reqId) : PendingRequest(reqId) {}
    commit_callback ccb{};
    commit_timeout_callback ctcb{};
  };
  struct PendingAbort : public PendingRequest {
    explicit PendingAbort(uint64_t reqId) : PendingRequest(reqId) {}
    abort_callback acb{};
    abort_timeout_callback atcb{};
  };

  bool PrepareCallback(uint64_t reqId, const std::string &,
                       const std::string &);

  bool CommitCallback(uint64_t reqId, const std::string &, const std::string &);

  bool AbortCallback(uint64_t reqId, const std::string &, const std::string &);

  const transport::Configuration &config_;
  Transport *transport_;  // Transport layer.
  uint64_t client_id_;    // Unique ID for this client.
  int shard_idx_;         // which shard this client accesses

  replication::vr::VRClient *client;  // Client proxy.

  std::unordered_map<uint64_t, PendingPrepare *> pendingPrepares;
  std::unordered_map<uint64_t, PendingCommit *> pendingCommits;
  std::unordered_map<uint64_t, PendingAbort *> pendingAborts;

  uint64_t lastReqId;
};

}  // namespace spannerstore
