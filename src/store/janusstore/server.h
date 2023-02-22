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
#ifndef _JANUS_SERVER_H_
#define _JANUS_SERVER_H_

#include <bits/stdint-uintn.h>

#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lib/configuration.h"
#include "lib/tcptransport.h"
#include "lib/transport.h"
#include "replication/common/replica.h"
#include "replication/ir/ir-proto.pb.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/janusstore/janus-proto.pb.h"
#include "store/janusstore/store.h"
#include "store/janusstore/transaction.h"
#include "store/server.h"

class Timestamp;

namespace replication {
namespace ir {
namespace proto {
class UnloggedReplyMessage;
}  // namespace proto
}  // namespace ir
}  // namespace replication

namespace janusstore {

class Store;

namespace proto {
class AcceptMessage;
class CommitMessage;
class InquireMessage;
class InquireOKMessage;
class PreAcceptMessage;
class Reply;
}  // namespace proto

class Server : public TransportReceiver, public ::Server {
 public:
  Server(transport::Configuration &config, int groupIdx, int myIdx,
         Transport *transport);
  ~Server() override;

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  void HandlePreAccept(
      const TransportAddress &remote, const proto::PreAcceptMessage &pa_msg,
      replication::ir::proto::UnloggedReplyMessage *unlogged_reply);

  void HandleAccept(
      const TransportAddress &remote, const proto::AcceptMessage &a_msg,
      replication::ir::proto::UnloggedReplyMessage *unlogged_reply);

  void HandleCommit(
      const TransportAddress &remote, const proto::CommitMessage &c_msg,
      replication::ir::proto::UnloggedReplyMessage *unlogged_reply);

  void HandleInquire(const TransportAddress &remote,
                     const proto::InquireMessage &i_msg, proto::Reply *reply);
  void HandleInquireReply(const proto::InquireOKMessage &i_ok_msg);

  Transport *transport;
  int groupIdx;
  int myIdx;

  // simple key-value store
  Store *store;

  transport::Configuration config;

  // highest ballot accepted per txn id
  std::unordered_map<uint64_t, uint64_t> accepted_ballots;

  // maps Transaction ids in the graph to ancestor Transaction ids
  std::unordered_map<uint64_t, std::vector<uint64_t>> dep_map;

  // maps Transaction ids to another map from ancestor txn ids to their relevant
  // shards
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::vector<int>>>
      depshards_map;

  // maps Transaction ids to Transaction objects
  std::unordered_map<uint64_t, Transaction> id_txn_map;

  // maps Txn to locally processed status
  std::unordered_map<uint64_t, bool> processed;

  // maps keys to transaction ids that read it
  // TODO(matthelb): ensure that this map is cleared per transaction
  std::unordered_map<std::string, std::set<uint64_t>> read_key_txn_map;

  // maps keys to transaction ids that write to it
  // TODO(matthelb): ensure that this map is cleared per transaction
  std::unordered_map<std::string, std::set<uint64_t>> write_key_txn_map;

  // maps txn_id -> list[other_ids] being blocked by txn_id
  std::unordered_map<uint64_t, std::set<uint64_t>> blocking_ids;
  std::unordered_map<
      uint64_t,
      std::vector<std::pair<const TransportAddress *, proto::InquireMessage>>>
      inquired_ids;

  // functions to process shardclient requests
  // must take in a full Transaction object in order to correctly bookkeep and
  // commit returns the list of dependencies for given txn, NULL if
  // PREACCEPT-NOT-OK
  std::vector<uint64_t> *BuildDepList(Transaction &txn, uint64_t ballot);

  // TODO(matthelb): figure out what T.abandon is
  void _HandleCommit(
      uint64_t txn_id, const TransportAddress &remote,
      replication::ir::proto::UnloggedReplyMessage *unlogged_reply);

  void _SendInquiry(uint64_t txn_id, uint64_t blocking_txn_id);

  std::unordered_map<std::string, std::string> WaitAndInquire(uint64_t txn_id);
  void _ExecutePhase(
      uint64_t txn_id, const TransportAddress &remote,
      replication::ir::proto::UnloggedReplyMessage *unlogged_reply);
  std::vector<uint64_t> _StronglyConnectedComponent(uint64_t txn_id);

  // unblocks txns that are potentially blocked by this txn_id
  void _UnblockTxns(uint64_t txn_id);
  bool _ReadyToProcess(const Transaction &txn);

  std::unordered_map<std::string, std::string> Execute(const Transaction &txn);
  // for cyclic dependency case, compute SCCs and execute in order
  // to be called during the Commit phase from HandleCommitJanusTxn()
  void ResolveContention(std::vector<uint64_t> &scc);

  Stats stats;
  void Load(std::string &&key, std::string &&value,
            Timestamp &&timestamp) override;
  inline Stats &GetStats() override { return stats; }
};
}  // namespace janusstore

#endif /* _JANUS_SERVER_H_ */
