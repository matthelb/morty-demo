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
#ifndef _JANUS_SHARDCLIENT_H_
#define _JANUS_SHARDCLIENT_H_

#include <bits/std_function.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/ir/client.h"
#include "store/common/frontend/txnclient.h"
#include "store/janusstore/janus-proto.pb.h"
#include "store/janusstore/transaction.h"

class Transport;
namespace janusstore {
namespace proto {
class Reply;
}  // namespace proto
}  // namespace janusstore
namespace transport {
class Configuration;
}  // namespace transport

namespace replication {
namespace ir {
class IRClient;
}  // namespace ir
}  // namespace replication

namespace janusstore {

// client callbacks
using client_preaccept_callback =
    std::function<void(int, std::vector<janusstore::proto::Reply>)>;
using client_accept_callback =
    std::function<void(int, std::vector<janusstore::proto::Reply>)>;
using client_commit_callback =
    std::function<void(int, std::vector<janusstore::proto::Reply>)>;
using client_read_callback = std::function<void(std::string, std::string)>;

class ShardClient {
 public:
  /* Constructor needs path to shard config. */
  ShardClient(transport::Configuration *config, Transport *transport,
              uint64_t client_id, int shard, int closestReplica);
  virtual ~ShardClient();

  /* * coordinator role (co-located with client but not visible to client) * */

  // Initiate the PreAccept phase for this shard.
  virtual void PreAccept(const Transaction &txn, uint64_t ballot,
                         client_preaccept_callback pcb);

  // Initiate the Accept phase for this shard.
  virtual void Accept(uint64_t txn_id, std::vector<uint64_t> deps,
                      uint64_t ballot, client_accept_callback acb);

  // Initiate the Commit phase for this shard.
  virtual void Commit(uint64_t txn_id, std::vector<uint64_t> deps,
                      std::map<uint64_t, std::set<int>> aggregated_depmeta,
                      client_commit_callback ccb);

  virtual void Read(std::string key, client_read_callback pcb);

  // private: // made public for testing
  uint64_t client_id;    // Unique ID for this client.
  Transport *transport;  // Transport layer.
  transport::Configuration *config;
  int shard;  // which shard this client accesses
  int num_replicas;
  int replica;  // which replica to use for reads

  struct PendingRequest {
    explicit PendingRequest(uint64_t txn_id) : txn_id(txn_id), responded(0) {}
    ~PendingRequest() = default;

    uint64_t txn_id;
    client_preaccept_callback cpcb{};
    client_accept_callback cacb{};
    client_commit_callback cccb{};
    std::set<uint64_t> aggregated_deps{};
    std::vector<janusstore::proto::Reply> preaccept_replies{};
    std::vector<janusstore::proto::Reply> accept_replies{};
    std::vector<janusstore::proto::Reply> commit_replies{};
    std::unordered_set<int> participant_shards{};
    int64_t responded;
  };

  replication::ir::IRClient *client;  // Client proxy.

  std::unordered_map<uint64_t, PendingRequest *> pendingReqs;
  std::unordered_map<std::string, client_read_callback> pendingReads;

  /* Callbacks for hearing back from a shard for a Janus phase. */
  void PreAcceptCallback(uint64_t txn_id, const janusstore::proto::Reply &reply,
                         const client_preaccept_callback &pcb);
  //
  void AcceptCallback(uint64_t txn_id, const janusstore::proto::Reply &reply,
                      const client_accept_callback &acb);
  void CommitCallback(uint64_t txn_id, const janusstore::proto::Reply &reply,
                      const client_commit_callback &ccb);
  void ReadCallback(string key, string value, const client_read_callback &rcb);

  bool PreAcceptContinuation(const string &request_str,
                             const string &reply_str);
  bool AcceptContinuation(const string &request_str, const string &reply_str);
  bool CommitContinuation(const string &request_str, const string &reply_str);
  bool ReadContinuation(const string &request_str, const string &reply_str);

  int responded;
  int txn_id{};
};

}  // namespace janusstore

#endif /* _JANUS_SHARDCLIENT_H_ */
