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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/transport.h"
#include "replication/common/replica.h"
#include "replication/common/viewstamp.h"
#include "replication/vr/client.h"
#include "replication/vr/replica.h"
#include "store/common/backend/pingserver.h"
#include "store/common/backend/versionstore.h"
#include "store/common/common-proto.pb.h"
#include "store/common/frontend/client.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"
#include "store/server.h"
#include "store/spannerstore/common.h"
#include "store/spannerstore/locktable.h"
#include "store/spannerstore/replicaclient.h"
#include "store/spannerstore/shardclient.h"
#include "store/spannerstore/spanner-proto.pb.h"
#include "store/spannerstore/transactionstore.h"
#include "store/spannerstore/truetime.h"

namespace transport {
class Configuration;
}  // namespace transport

namespace spannerstore {

class ReplicaClient;
class ShardClient;

class RequestID {
 public:
  RequestID(uint64_t client_id, uint64_t client_req_id, TransportAddress *addr)
      : client_id_{client_id}, client_req_id_{client_req_id}, addr_{addr} {}
  ~RequestID() = default;

  const uint64_t client_id() const { return client_id_; }
  const uint64_t client_req_id() const { return client_req_id_; }
  const TransportAddress *addr() const { return addr_; }

 private:
  uint64_t client_id_;
  uint64_t client_req_id_;
  TransportAddress *addr_;
};

inline bool operator==(const spannerstore::RequestID &lhs,
                       const spannerstore::RequestID &rhs) {
  return lhs.client_id() == rhs.client_id() &&
         lhs.client_req_id() == rhs.client_req_id();
}
}  // namespace spannerstore

namespace std {
template <>
struct hash<spannerstore::RequestID> {
  std::size_t operator()(spannerstore::RequestID const &rid) const noexcept {
    std::size_t h1 = std::hash<std::uint64_t>{}(rid.client_id());
    std::size_t h2 = std::hash<std::uint64_t>{}(rid.client_req_id());
    return h1 ^ (h2 << 1);  // or use boost::hash_combine
  }
};
}  // namespace std

namespace spannerstore {

class Server : public TransportReceiver,
               public ::Server,
               public replication::AppReplica,
               public PingServer {
 public:
  Server(Consistency consistency, const transport::Configuration &shard_config,
         const transport::Configuration &replica_config, uint64_t server_id,
         int shard_idx, int replica_idx, Transport *transport,
         const TrueTime &tt, bool debug_stats);
  ~Server() override;

  // Override TransportReceiver
  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  // Override AppReplica
  void LeaderUpcall(opnum_t opnum, const std::string &op, bool *replicate,
                    std::string *response) override;
  void ReplicaUpcall(opnum_t opnum, const std::string &op,
                     std::string *response) override;

  void UnloggedUpcall(const std::string &op, std::string *response) override;

  // Override Server
  void Load(std::string &&key, std::string &&value,
            Timestamp &&timestamp) override;

  Stats &GetStats() override;

 private:
  class PendingRWCommitCoordinatorReply {
   public:
    PendingRWCommitCoordinatorReply(uint64_t client_id, uint64_t client_req_id,
                                    TransportAddress *remote)
        : rid{client_id, client_req_id, remote} {}
    RequestID rid;
  };
  class PendingRWCommitParticipantReply {
   public:
    PendingRWCommitParticipantReply(uint64_t client_id, uint64_t client_req_id,
                                    TransportAddress *remote)
        : rid{client_id, client_req_id, remote} {}
    RequestID rid;
  };
  class PendingPrepareOKReply {
   public:
    PendingPrepareOKReply(uint64_t client_id, uint64_t client_req_id,
                          TransportAddress *remote)
        : rids{{client_id, client_req_id, remote}} {}
    std::unordered_set<RequestID> rids{};
  };
  class PendingROCommitReply {
   public:
    PendingROCommitReply(uint64_t client_id, uint64_t client_req_id,
                         TransportAddress *remote)
        : rid{client_id, client_req_id, remote} {}
    RequestID rid;
    Latency_Frame_t wait_lat{};
  };
  class PendingGetReply {
   public:
    PendingGetReply(uint64_t client_id, uint64_t client_req_id,
                    TransportAddress *remote)
        : rid{client_id, client_req_id, remote} {}
    RequestID rid;
    std::string key{};
  };

  struct TimestampID {
    Timestamp timestamp;
    uint64_t transaction_id;

    friend bool operator>(const TimestampID &t1, const TimestampID &t2) {
      return t1.timestamp > t2.timestamp;
    }
    friend bool operator<(const TimestampID &t1, const TimestampID &t2) {
      return t1.timestamp < t2.timestamp;
    }
  };

  void HandleGet(const TransportAddress &remote, proto::Get &msg);

  void HandleROCommit(const TransportAddress &remote, proto::ROCommit &msg);

  void HandleRWCommitCoordinator(const TransportAddress &remote,
                                 proto::RWCommitCoordinator &msg);

  void SendRWCommmitCoordinatorReplyOK(uint64_t transaction_id,
                                       const Timestamp &commit_ts,
                                       const Timestamp &nonblock_ts);
  void SendRWCommmitCoordinatorReplyFail(const TransportAddress &remote,
                                         uint64_t client_id,
                                         uint64_t client_req_id);

  void SendRWCommmitParticipantReplyOK(uint64_t transaction_id);
  void SendRWCommmitParticipantReplyFail(uint64_t transaction_id);

  void SendRWCommmitParticipantReplyFail(const TransportAddress &remote,
                                         uint64_t client_id,
                                         uint64_t client_req_id);

  void SendPrepareOKRepliesOK(uint64_t transaction_id,
                              const Timestamp &commit_ts);
  void SendPrepareOKRepliesFail(PendingPrepareOKReply *reply);

  void HandleRWCommitParticipant(const TransportAddress &remote,
                                 proto::RWCommitParticipant &msg);

  void HandleAbort(const TransportAddress &remote, proto::Abort &msg);
  void HandleWound(const TransportAddress &remote, proto::Wound &msg);

  void SendAbortParticipants(uint64_t transaction_id,
                             const std::unordered_set<int> &participants);

  void HandlePrepareOK(const TransportAddress &remote, proto::PrepareOK &msg);
  void HandlePrepareAbort(const TransportAddress &remote,
                          proto::PrepareAbort &msg);

  void PrepareCallback(uint64_t transaction_id, int status,
                       const Timestamp &timestamp);
  void PrepareOKCallback(uint64_t transaction_id, int status,
                         Timestamp commit_ts);
  void PrepareAbortCallback(uint64_t transaction_id, int status,
                            const Timestamp &timestamp);

  void CommitCoordinatorCallback(uint64_t transaction_id,
                                 transaction_status_t status);
  void CommitParticipantCallback(uint64_t transaction_id,
                                 transaction_status_t status);
  void AbortParticipantCallback(uint64_t transaction_id);

  void WoundPendingRWs(uint64_t transaction_id,
                       const std::unordered_set<uint64_t> &rws);

  void NotifyPendingRWs(uint64_t transaction_id,
                        const std::unordered_set<uint64_t> &rws);
  void ContinueGet(uint64_t transaction_id);
  void ContinueCoordinatorPrepare(uint64_t transaction_id);
  void ContinueParticipantPrepare(uint64_t transaction_id);

  void NotifyPendingROs(const std::unordered_set<uint64_t> &ros);
  void ContinueROCommit(uint64_t transaction_id);

  const Timestamp GetPrepareTimestamp(uint64_t client_id);
  void CoordinatorCommitTransaction(uint64_t transaction_id,
                                    const Timestamp &commit_ts);
  void ParticipantCommitTransaction(uint64_t transaction_id,
                                    const Timestamp &commit_ts);

  const TrueTime &tt_;
  TransactionStore transactions_;
  LockTable locks_;
  VersionedKVStore<TimestampID, std::string> store_;

  const transport::Configuration &shard_config_;
  const transport::Configuration &replica_config_;

  std::vector<ShardClient *> shard_clients_;
  ReplicaClient *replica_client_;

  Transport *transport_;

  uint64_t server_id_;

  std::unordered_map<uint64_t, PendingRWCommitCoordinatorReply *>
      pending_rw_commit_c_replies_;
  std::unordered_map<uint64_t, PendingRWCommitParticipantReply *>
      pending_rw_commit_p_replies_;
  std::unordered_map<uint64_t, PendingPrepareOKReply *>
      pending_prepare_ok_replies_;
  std::unordered_map<uint64_t, PendingROCommitReply *>
      pending_ro_commit_replies_;
  std::unordered_map<uint64_t, PendingGetReply *> pending_get_replies_;

  proto::Get get_;
  proto::RWCommitCoordinator rw_commit_c_;
  proto::RWCommitParticipant rw_commit_p_;
  proto::PrepareOK prepare_ok_;
  proto::PrepareAbort prepare_abort_;
  proto::ROCommit ro_commit_;
  proto::Abort abort_;

  proto::GetReply get_reply_;
  proto::RWCommitCoordinatorReply rw_commit_c_reply_;
  proto::RWCommitParticipantReply rw_commit_p_reply_;
  proto::PrepareOKReply prepare_ok_reply_;
  proto::PrepareAbortReply prepare_abort_reply_;
  proto::ROCommitReply ro_commit_reply_;
  proto::AbortReply abort_reply_;
  PingMessage ping_;
  proto::Wound wound_;

  Stats stats_;

  Latency_t ro_wait_lat_{};

  Timestamp min_prepare_timestamp_;
  int shard_idx_;
  int replica_idx_;
  Consistency consistency_;
  bool debug_stats_;
};

}  // namespace spannerstore
