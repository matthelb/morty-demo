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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
#ifndef _JANUS_CLIENT_H_
#define _JANUS_CLIENT_H_

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/ir/client.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/one_shot_client.h"
#include "store/common/stats.h"
#include "store/janusstore/janus-proto.pb.h"
#include "store/janusstore/shardclient.h"
#include "store/janusstore/transaction.h"

class OneShotTransaction;
class Transport;
namespace transport {
class Configuration;
}  // namespace transport

namespace janusstore {

// callback for output commit
class ShardClient;
class Transaction;

namespace proto {
class Reply;
}  // namespace proto

using output_commit_callback = std::function<void(uint64_t)>;
using read_callback = std::function<void()>;

class Client : public OneShotClient {
 public:
  Client(const std::string &configPath, int nShards, int closestReplica,
         Transport *transport);
  Client(transport::Configuration *config, int nShards, int closestReplica,
         Transport *transport);
  ~Client() override;

  void Execute(OneShotTransaction *txn, execute_callback ecb) override;

  // read
  void Read(const string &key, read_callback rcb);

  // begins PreAccept phase
  void PreAccept(Transaction *txn, uint64_t ballot, execute_callback ecb);

  // called from PreAcceptCallback when a fast quorum is not obtained
  void Accept(uint64_t txn_id, const std::set<uint64_t> &deps, uint64_t ballot);

  // different from the public Commit() function; this is a Janus commit
  void Commit(uint64_t txn_id, const std::set<uint64_t> &deps);

  struct PendingRequest {
    PendingRequest(uint64_t txn_id, execute_callback ccb)
        : txn_id(txn_id),
          ccb(ccb),
          has_fast_quorum(false),
          output_committed(false) {}
    ~PendingRequest() = default;

    uint64_t txn_id;
    execute_callback ccb;
    bool has_fast_quorum;
    bool output_committed;
    std::set<uint64_t> aggregated_deps{};
    std::map<uint64_t, std::set<int>> aggregated_depmeta;
    std::set<uint64_t> participant_shards{};
    std::set<uint64_t> responded_shards{};
    std::map<int, std::set<uint64_t>> per_shard_aggregated_deps;
  };

  // private: // make all fields public for testing
  // Unique ID for this client.
  uint64_t client_id;

  // Number of shards.
  uint64_t nshards;

  // Transport used by IR client proxies.
  Transport *transport;

  // Current highest ballot
  uint64_t ballot;

  // Next available transaction ID.
  uint64_t next_txn_id;

  // aggregated map to remove the above fields/maps up to aggregated_deps
  std::unordered_map<uint64_t, PendingRequest *> pendingReqs;

  // Buffering client for each shard.
  std::vector<ShardClient *> bclient;

  uint64_t keyToShard(const string &key, uint64_t nshards);

  void setParticipants(Transaction *txn);

  /* * coordinator role (co-located with client but not visible to client) * */
  std::unordered_map<string, read_callback> readReqs;
  // read the key
  void ReadCallback(const string &key, const string &value);
  // callback when all participants for a shard have replied
  // shardclient aggregates all replies before invoking the callback
  // replies is a vector of all replies for this shard
  // deps is a map from replica ID to its deps for T (a list of other t_ids)
  void PreAcceptCallback(uint64_t txn_id, int shard,
                         std::vector<janusstore::proto::Reply> replies);

  // callback when majority of replicas in each shard returns Accept-OK
  void AcceptCallback(uint64_t txn_id, int shard,
                      std::vector<janusstore::proto::Reply> replies);

  // TODO(matthelb): maybe change the type of [results]
  void CommitCallback(uint64_t txn_id, int shard,
                      const std::vector<janusstore::proto::Reply> &replies);
  transport::Configuration *config;

  Stats stats;
  inline Stats &GetStats() { return stats; }
};

}  // namespace janusstore

#endif /* _JANUS_CLIENT_H_ */
