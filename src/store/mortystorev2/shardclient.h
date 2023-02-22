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
#ifndef _MORTYV2_SHARDCLIENT_H_
#define _MORTYV2_SHARDCLIENT_H_

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <cstdint>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/frontend/client.h"
#include "store/common/timestamp.h"
#include "store/mortystorev2/common.h"
#include "store/mortystorev2/morty-proto.pb.h"
#include "store/mortystorev2/version.h"

namespace transport {
class Configuration;
}  // namespace transport

namespace mortystorev2 {

using get_callback =
    std::function<void(context::transaction_op_status_t, const std::string &,
                       uint64_t, const Version &, bool, uint64_t, bool)>;

using prepare_callback = std::function<void(proto::Decision, bool)>;

using finalize_callback = std::function<void(proto::Decision)>;

using decide_callback = std::function<void(proto::Decision, bool)>;

using paxos_prepare_callback = std::function<void(proto::Decision, bool)>;

class Client;

class ShardClient : public TransportReceiver {
 public:
  ShardClient(transport::Configuration *config, Transport *transport,
              uint64_t client_id, uint64_t group, int read_replica,
              uint64_t slow_path_timeout_ms, bool pipeline_commit,
              uint64_t server_num_workers);
  ~ShardClient() override;

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  void Get(const Version &txn_version, const std::string &key,
           get_callback gcb);
  void Put(const Version &version, const std::string &key,
           const std::string &value, uint64_t val_id);
  void Prepare(const Version &version, const proto::Transaction &txn,
               prepare_callback pcb);
  void Finalize(const Version &version, const proto::Transaction &txn,
                proto::Decision decision, finalize_callback fcb);
  void Decide(const Version &version, const proto::Transaction &txn,
              proto::Decision decision, decide_callback dcb);
  void PaxosPrepare(const Version &version, const proto::Transaction &txn,
                    uint64_t view, paxos_prepare_callback ppcb);
  void CleanPending(const Version &version);
  void CleanPendingDecides(const Version &version);

 private:
  typedef std::map<proto::Vote, std::set<uint64_t>> PrepareVotesMap;
  typedef std::map<std::pair<uint64_t, proto::Decision>, std::set<uint64_t>>
      FinalizeViewsMap;
  struct PendingRead {
    PendingRead(uint64_t req_id, const Version &txn_version, get_callback gcb)
        : req_id(req_id), txn_version(txn_version), gcb(std::move(gcb)) {}
    uint64_t req_id;
    Version txn_version;
    get_callback gcb{};
  };
  struct PendingPrepare {
    PendingPrepare(uint64_t req_id, const Version &txn_version,
                   prepare_callback pcb)
        : req_id(req_id), txn_version(txn_version), pcb(std::move(pcb)) {}
    uint64_t req_id;
    Version txn_version;
    prepare_callback pcb{};
    PrepareVotesMap replica_votes{};
    std::unique_ptr<Timeout> slow_path_timeout{};
  };
  struct PendingFinalize {
    PendingFinalize(uint64_t req_id, const Version &txn_version,
                    finalize_callback fcb, uint64_t view,
                    proto::Decision decision)
        : req_id(req_id),
          txn_version(txn_version),
          fcb(std::move(fcb)),
          view(view),
          decision(decision) {}
    uint64_t req_id;
    Version txn_version;
    finalize_callback fcb{};
    uint64_t view;
    proto::Decision decision;
    FinalizeViewsMap replica_views{};
    std::unique_ptr<Timeout> recovery_timeout{};
  };
  struct PendingDecide {
    PendingDecide(uint64_t req_id, const Version &txn_version,
                  decide_callback dcb, proto::Decision decision)
        : req_id(req_id),
          txn_version(txn_version),
          dcb(std::move(dcb)),
          decision(decision) {}
    uint64_t req_id;
    Version txn_version;
    decide_callback dcb{};
    proto::Decision decision;
  };
  struct PendingPaxosPrepare {
    PendingPaxosPrepare(uint64_t req_id, const Version &txn_version,
                        paxos_prepare_callback ppcb)
        : req_id(req_id), txn_version(txn_version), ppcb(std::move(ppcb)) {}
    uint64_t req_id;
    Version txn_version;
    paxos_prepare_callback ppcb{};
    PrepareVotesMap replica_votes{};
    FinalizeViewsMap replica_views{};
    std::unique_ptr<Timeout> slow_path_timeout{};
  };

  void HandleReadReply(const proto::ReadReply &read_reply);
  void HandlePrepareReply(const proto::PrepareReply &prepare_reply);
  void HandleFinalizeReply(const proto::FinalizeReply &finalize_reply);
  void HandleDecideReply(const proto::DecideReply &decide_reply);
  void HandlePaxosPrepareReply(
      const proto::PaxosPrepareReply &paxos_prepare_reply);

  void PrepareSlowPathTimeout(uint64_t req_id);
  void FinalizeRecoveryTimeout(uint64_t req_id);

  std::tuple<proto::Decision, bool, uint64_t> CountReplicaVote(
      uint64_t req_id, uint64_t replica_id, proto::Vote vote,
      PrepareVotesMap &replica_votes);

  transport::Configuration *config_;
  Transport *transport_;
  const uint64_t client_id_;
  const uint64_t group_;
  const int read_replica_;
  const uint64_t slow_path_timeout_length_ms_;
  const uint64_t recovery_timeout_length_ms_;
  const bool pipeline_commit_;
  const uint64_t server_num_workers_;

  std::mt19937_64 rand_;
  uint64_t last_req_id_;

  std::unordered_map<uint64_t, std::unique_ptr<PendingRead>> pending_reads_;
  std::unordered_map<uint64_t, std::unique_ptr<PendingPrepare>>
      pending_prepares_;
  std::unordered_map<uint64_t, std::unique_ptr<PendingFinalize>>
      pending_finalizes_;
  std::unordered_map<uint64_t, std::unique_ptr<PendingDecide>> pending_decides_;
  std::unordered_map<uint64_t, std::unique_ptr<PendingPaxosPrepare>>
      pending_paxos_prepares_;

  proto::Read read_;
  proto::Write write_;
  proto::Prepare prepare_;
  proto::Finalize finalize_;
  proto::Decide decide_;
  proto::PaxosPrepare paxos_prepare_;

  proto::ReadReply read_reply_;
  proto::PrepareReply prepare_reply_;
  proto::FinalizeReply finalize_reply_;
  proto::DecideReply decide_reply_;
  proto::PaxosPrepareReply paxos_prepare_reply_;
};

}  // namespace mortystorev2

#endif /* _MORTYV2_SHARDCLIENT_H_ */
