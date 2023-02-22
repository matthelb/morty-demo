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
#include "store/mortystorev2/shardclient.h"

#include <google/protobuf/stubs/port.h>

#include <functional>

#include "lib/message.h"
#include "store/common/common.h"

namespace mortystorev2 {

ShardClient::ShardClient(transport::Configuration *config, Transport *transport,
                         uint64_t client_id, uint64_t group, int read_replica,
                         uint64_t slow_path_timeout_ms, bool pipeline_commit,
                         uint64_t server_num_workers)
    : config_(config),
      transport_(transport),
      client_id_(client_id),
      group_(group),
      read_replica_(read_replica),
      slow_path_timeout_length_ms_(slow_path_timeout_ms),
      recovery_timeout_length_ms_(0),
      pipeline_commit_(pipeline_commit),
      server_num_workers_(server_num_workers),
      rand_(client_id),
      last_req_id_(0UL) {
  transport_->Register(this, *config, -1, -1);
}

ShardClient::~ShardClient() = default;

void ShardClient::ReceiveMessage(const TransportAddress &remote,
                                 std::string *type, std::string *data,
                                 void *meta_data) {
  if (*type == read_reply_.GetTypeName()) {
    read_reply_.ParseFromString(*data);
    HandleReadReply(read_reply_);
  } else if (*type == prepare_reply_.GetTypeName()) {
    prepare_reply_.ParseFromString(*data);
    HandlePrepareReply(prepare_reply_);
  } else if (*type == finalize_reply_.GetTypeName()) {
    finalize_reply_.ParseFromString(*data);
    HandleFinalizeReply(finalize_reply_);
  } else if (*type == paxos_prepare_reply_.GetTypeName()) {
    paxos_prepare_reply_.ParseFromString(*data);
    HandlePaxosPrepareReply(paxos_prepare_reply_);
  } else if (*type == decide_reply_.GetTypeName()) {
    decide_reply_.ParseFromString(*data);
    HandleDecideReply(decide_reply_);
  } else {
    Panic("Received unexpected message *type: %s", type->c_str());
  }
}

void ShardClient::Get(const Version &txn_version, const std::string &key,
                      get_callback gcb) {
  std::unique_ptr<PendingRead> req = std::make_unique<PendingRead>(
      last_req_id_ + 1, txn_version, std::move(gcb));
  last_req_id_ = req->req_id;

  Debug("[%lu][%lu][%lu] Read of key %s (%lu) for txn %lu.%lu.", client_id_,
        group_, req->req_id, BytesToHex(key, 16).c_str(), key.length(),
        txn_version.GetClientId(), txn_version.GetClientSeq());

  read_.Clear();
  read_.set_req_id(req->req_id);
  txn_version.Serialize(read_.mutable_txn_version());
  read_.set_key(key);

  pending_reads_[req->req_id] = std::move(req);

  // uint64_t thread_id = rand_();
  uint64_t thread_id = txn_version.GetClientSeq() % server_num_workers_;
  if (read_replica_ == -1) {
    int replica_idx =
        (txn_version.GetClientId() % config_->n) + (config_->n * thread_id);
    Debug("[%lu][%lu][%lu] Sending to replica %d core %lu.", client_id_, group_,
          last_req_id_, replica_idx, thread_id);
    transport_->SendMessageToReplica(this, group_, replica_idx, read_,
                                     sizeof(thread_id), &thread_id);
  } else {
    int replica_idx = read_replica_ + (config_->n * thread_id);
    Debug("[%lu][%lu][%lu] Sending to replica %d core %lu.", client_id_, group_,
          last_req_id_, replica_idx, thread_id);
    transport_->SendMessageToReplica(this, group_, replica_idx, read_,
                                     sizeof(thread_id), &thread_id);
  }
}

void ShardClient::Put(const Version &version, const std::string &key,
                      const std::string &value, uint64_t val_id) {
  Debug("[%lu][%lu] Write to key %s for txn %lu.%lu.", client_id_, group_,
        BytesToHex(key, 16).c_str(), version.GetClientId(),
        version.GetClientSeq());

  write_.Clear();

  version.Serialize(write_.mutable_txn_version());
  write_.set_key(key);
  write_.set_value(value);
  write_.set_val_id(val_id);

  // uint64_t thread_id = rand_();
  uint64_t thread_id = version.GetClientSeq() % server_num_workers_;
  // transport_->SendMessageToGroup(this, group_, write_, sizeof(thread_id),
  // &thread_id);
  for (int i = 0; i < config_->n; ++i) {
    transport_->SendMessageToReplica(this, group_, i + config_->n * thread_id,
                                     write_, sizeof(thread_id), &thread_id);
  }
}

void ShardClient::Prepare(const Version &version, const proto::Transaction &txn,
                          prepare_callback pcb) {
  std::unique_ptr<PendingPrepare> req = std::make_unique<PendingPrepare>(
      last_req_id_ + 1, version, std::move(pcb));
  last_req_id_ = req->req_id;

  Debug("[%lu][%lu][%lu] Prepare for txn %lu.%lu.", client_id_, group_,
        req->req_id, version.GetClientId(), version.GetClientSeq());

  prepare_.Clear();
  prepare_.set_req_id(req->req_id);
  prepare_.set_allocated_txn(
      &const_cast<::mortystorev2::proto::Transaction &>(txn));

  pending_prepares_[req->req_id] = std::move(req);

  uint64_t thread_id = txn.version().client_seq() % server_num_workers_;
  // transport_->SendMessageToGroup(this, group_, prepare_, sizeof(thread_id),
  //    &thread_id);
  for (int i = 0; i < config_->n; ++i) {
    int replica_idx = i + config_->n * thread_id;
    Debug("[%lu][%lu][%lu] Sending to replica %d core %lu.", client_id_, group_,
          last_req_id_, replica_idx, thread_id);
    transport_->SendMessageToReplica(this, group_, replica_idx, prepare_,
                                     sizeof(thread_id), &thread_id);
  }

  prepare_.release_txn();
}

void ShardClient::Finalize(const Version &version,
                           const proto::Transaction &txn,
                           proto::Decision decision, finalize_callback fcb) {
  // TODO(matthelb): implement recovery with potentially larger views
  uint64_t view = 0;

  std::unique_ptr<PendingFinalize> req = std::make_unique<PendingFinalize>(
      last_req_id_ + 1, version, std::move(fcb), view, decision);
  last_req_id_ = req->req_id;

  Debug("[%lu][%lu][%lu] Finalize for txn %lu.%lu.", client_id_, group_,
        req->req_id, version.GetClientId(), version.GetClientSeq());

  finalize_.Clear();
  finalize_.set_req_id(req->req_id);
  version.Serialize(finalize_.mutable_txn_version());
  finalize_.set_exec_id(txn.exec_id());
  // finalize_.set_allocated_txn(&const_cast<::mortystorev2::proto::Transaction
  // &>(
  //      txn));
  finalize_.set_view(view);
  finalize_.set_decision(decision);

  pending_finalizes_[req->req_id] = std::move(req);
  uint64_t thread_id = txn.version().client_seq() % server_num_workers_;
  // transport_->SendMessageToGroup(this, group_, finalize_, sizeof(thread_id),
  //    &thread_id);
  for (int i = 0; i < config_->n; ++i) {
    transport_->SendMessageToReplica(this, group_, i + config_->n * thread_id,
                                     finalize_, sizeof(thread_id), &thread_id);
  }

  // finalize_.release_txn();
}

void ShardClient::Decide(const Version &version, const proto::Transaction &txn,
                         proto::Decision decision, decide_callback dcb) {
  Debug("[%lu][%lu] Decide %d for txn %lu.%lu.", client_id_, group_, decision,
        version.GetClientId(), version.GetClientSeq());

  if (pipeline_commit_) {
    std::unique_ptr<PendingDecide> req = std::make_unique<PendingDecide>(
        last_req_id_ + 1, version, std::move(dcb), decision);
    last_req_id_ = req->req_id;
    pending_decides_[last_req_id_] = std::move(req);
  }

  decide_.Clear();
  decide_.set_req_id(last_req_id_);
  // decide_.set_allocated_txn(&const_cast<::mortystorev2::proto::Transaction
  // &>(
  //      txn));
  version.Serialize(decide_.mutable_txn_version());
  decide_.set_exec_id(txn.exec_id());
  decide_.set_decision(decision);

  uint64_t thread_id = txn.version().client_seq() % server_num_workers_;
  // transport_->SendMessageToGroup(this, group_, decide_, sizeof(thread_id),
  //    &thread_id);
  for (int i = 0; i < config_->n; ++i) {
    transport_->SendMessageToReplica(this, group_, i + config_->n * thread_id,
                                     decide_, sizeof(thread_id), &thread_id);
  }

  // decide_.release_txn();

  if (!pipeline_commit_ || decision == proto::DECISION_ABORT2) {
    CleanPending(version);
  }
}

void ShardClient::PaxosPrepare(const Version &version,
                               const proto::Transaction &txn, uint64_t view,
                               paxos_prepare_callback ppcb) {
  std::unique_ptr<PendingPaxosPrepare> req =
      std::make_unique<PendingPaxosPrepare>(last_req_id_ + 1, version,
                                            std::move(ppcb));
  last_req_id_ = req->req_id;

  Debug("[%lu][%lu][%lu] PaxosPrepare for txn %lu.%lu.", client_id_, group_,
        req->req_id, version.GetClientId(), version.GetClientSeq());

  paxos_prepare_.Clear();
  paxos_prepare_.set_req_id(req->req_id);
  paxos_prepare_.set_allocated_txn(
      &const_cast<::mortystorev2::proto::Transaction &>(txn));
  paxos_prepare_.set_view(view);

  pending_paxos_prepares_[req->req_id] = std::move(req);
  uint64_t thread_id = txn.version().client_seq() % server_num_workers_;
  // transport_->SendMessageToGroup(this, group_, paxos_prepare_,
  // sizeof(thread_id),
  //    &thread_id);
  for (int i = 0; i < config_->n; ++i) {
    transport_->SendMessageToReplica(this, group_, i + config_->n * thread_id,
                                     paxos_prepare_, sizeof(thread_id),
                                     &thread_id);
  }

  paxos_prepare_.release_txn();
}

void ShardClient::HandleReadReply(const proto::ReadReply &read_reply) {
  Debug("[%lu][%lu][%lu] Handle ReadReply.", client_id_, group_,
        read_reply.req_id());

  auto itr = pending_reads_.find(read_reply.req_id());
  if (itr == pending_reads_.end()) {
    Debug("[%lu][%lu][%lu] ReadReply for stale request.", client_id_, group_,
          read_reply.req_id());
    return;  // stale request
  }

  auto &req = itr->second;

  Debug("[%lu][%lu][%lu] Read returned value from txn %lu.%lu.", client_id_,
        group_, read_reply.req_id(), read_reply.version().client_id(),
        read_reply.version().client_seq());
  req->gcb(read_reply.key_exists() ? context::OK : context::FAIL,
           read_reply.val(), read_reply.val_id(), Version(read_reply.version()),
           read_reply.committed(), read_reply.req_id(),
           read_reply.has_version_below_watermark() &&
               read_reply.version_below_watermark());
}

void ShardClient::HandlePrepareReply(const proto::PrepareReply &prepare_reply) {
  Debug("[%lu][%lu][%lu] Handle PrepareReply from %lu.", client_id_, group_,
        prepare_reply.req_id(), prepare_reply.replica_id());

  auto itr = pending_prepares_.find(prepare_reply.req_id());
  if (itr == pending_prepares_.end()) {
    Debug("[%lu][%lu][%lu] PrepareReply for stale request.", client_id_, group_,
          prepare_reply.req_id());
    return;  // stale request
  }

  Debug("[%lu][%lu][%lu] PrepareReply from replica %lu with vote %d.",
        client_id_, group_, prepare_reply.req_id(), prepare_reply.replica_id(),
        prepare_reply.vote());

  auto &pending_prepare = itr->second;

  auto decision =
      CountReplicaVote(prepare_reply.req_id(), prepare_reply.replica_id(),
                       prepare_reply.vote(), pending_prepare->replica_votes);

  if (std::get<0>(decision) != proto::DECISION_NONE) {
    pending_prepare->pcb(std::get<0>(decision), std::get<1>(decision));
    // pcb may destroy reference to itr. better to use safe erase-by-key.
    pending_prepares_.erase(prepare_reply.req_id());
  } else if (std::get<2>(decision) >= static_cast<uint64_t>(config_->f + 1) &&
             pending_prepare->slow_path_timeout == nullptr) {
    Debug("[%lu][%lu][%lu] Group starting slow path timer.", client_id_, group_,
          prepare_reply.req_id());
    pending_prepare->slow_path_timeout = std::make_unique<Timeout>(
        transport_, slow_path_timeout_length_ms_,
        std::bind(&ShardClient::PrepareSlowPathTimeout, this,
                  prepare_reply_.req_id()));
    pending_prepare->slow_path_timeout->Start();
  }
}

std::tuple<proto::Decision, bool, uint64_t> ShardClient::CountReplicaVote(
    uint64_t req_id, uint64_t replica_id, proto::Vote vote,
    PrepareVotesMap &replica_votes) {
  replica_votes[vote].insert(replica_id);
  uint64_t total_votes = 0;
  for (const auto &vote_replicas : replica_votes) {
    total_votes += vote_replicas.second.size();
  }

  if (!replica_votes[proto::VOTE_ABANDON_FAST].empty()) {
    Debug("[%lu][%lu][%lu] Group deciding fast abort.", client_id_, group_,
          req_id);
    return std::make_tuple(proto::DECISION_ABANDON, true, total_votes);
  }
  if (replica_votes[proto::VOTE_ABANDON_SLOW].size() >=
      static_cast<uint64_t>(config_->f + 1)) {
    Debug("[%lu][%lu][%lu] Group deciding slow abort.", client_id_, group_,
          req_id);
    return std::make_tuple(proto::DECISION_ABANDON, false, total_votes);
  }
  if (replica_votes[proto::VOTE_COMMIT].size() >=
          static_cast<uint64_t>(config_->f + 1) &&
      !replica_votes[proto::VOTE_ABANDON_SLOW].empty()) {
    Debug("[%lu][%lu][%lu] Group deciding slow commit.", client_id_, group_,
          req_id);
    return std::make_tuple(proto::DECISION_COMMIT, false, total_votes);
  } else if (replica_votes[proto::VOTE_COMMIT].size() ==
             static_cast<uint64_t>(config_->n)) {
    Debug("[%lu][%lu][%lu] Group deciding fast commit.", client_id_, group_,
          req_id);
    return std::make_tuple(proto::DECISION_COMMIT, true, total_votes);
  } else {
    return std::make_tuple(proto::DECISION_NONE, false, total_votes);
  }
}

void ShardClient::HandleFinalizeReply(
    const proto::FinalizeReply &finalize_reply) {
  Debug("[%lu][%lu][%lu] Handle FinalizeReply.", client_id_, group_,
        finalize_reply.req_id());

  auto itr = pending_finalizes_.find(finalize_reply.req_id());
  if (itr == pending_finalizes_.end()) {
    Debug("[%lu][%lu][%lu] FinalizeReply for stale request.", client_id_,
          group_, finalize_reply.req_id());
    return;  // stale request
  }

  auto &pending_finalize = itr->second;

  pending_finalize
      ->replica_views[std::make_pair(finalize_reply.view(),
                                     finalize_reply.decision())]
      .insert(finalize_reply.replica_id());

  uint64_t total_replies = 0;
  for (const auto &replica_view : pending_finalize->replica_views) {
    total_replies += replica_view.second.size();
  }

  // TODO(matthelb): what is the purpose of this first (commented for now)
  //   condition?
  // if ((finalize_reply.decision() == proto::DECISION_ABANDON &&
  //     pending_finalize->decision == proto::DECISION_COMMIT) ||
  if (pending_finalize
          ->replica_views[std::make_pair(finalize_reply.view(),
                                         finalize_reply.decision())]
          .size() >= static_cast<uint64_t>(config_->f + 1)) {
    pending_finalize->fcb(finalize_reply.decision());
    // fcb may destroy reference to itr. better to use safe erase-by-key.
    pending_finalizes_.erase(finalize_reply.req_id());
  } else if (total_replies >= static_cast<uint64_t>(config_->f + 1) &&
             pending_finalize->recovery_timeout == nullptr) {
    Debug("[%lu][%lu][%lu] Group starting recovery timer.", client_id_, group_,
          finalize_reply.req_id());
    pending_finalize->recovery_timeout = std::make_unique<Timeout>(
        transport_, recovery_timeout_length_ms_,
        std::bind(&ShardClient::FinalizeRecoveryTimeout, this,
                  finalize_reply_.req_id()));
    pending_finalize->recovery_timeout->Start();
  }

  // TODO(matthelb): implement recovery when finalize not accepted by a majority
}

void ShardClient::HandleDecideReply(const proto::DecideReply &decide_reply) {
  Debug("[%lu][%lu][%lu] Handle DecideReply.", client_id_, group_,
        decide_reply.req_id());

  auto itr = pending_decides_.find(decide_reply.req_id());
  if (itr == pending_decides_.end()) {
    Debug("[%lu][%lu][%lu] DecideReply for stale request.", client_id_, group_,
          decide_reply.req_id());
    return;  // stale request
  }

  auto &pending_decide = itr->second;

  pending_decide->dcb(decide_reply.decision(),
                      decide_reply.deps_changed_decision());

  if (decide_reply.decision() == proto::DECISION_COMMIT) {
    CleanPending(pending_decide->txn_version);
  }
  pending_decides_.erase(decide_reply.req_id());
}

void ShardClient::HandlePaxosPrepareReply(
    const proto::PaxosPrepareReply &paxos_prepare_reply) {
  Debug("[%lu][%lu][%lu] Handle PaxosPrepareReply.", client_id_, group_,
        paxos_prepare_reply.req_id());

  auto itr = pending_paxos_prepares_.find(paxos_prepare_reply.req_id());
  if (itr == pending_paxos_prepares_.end()) {
    Debug("[%lu][%lu][%lu] PaxosPrepareReply for stale request.", client_id_,
          group_, paxos_prepare_reply.req_id());
    return;  // stale request
  }

  auto &pending_paxos_prepare = itr->second;

  if (paxos_prepare_reply.decision() != proto::DECISION_NONE) {
    pending_paxos_prepare->ppcb(paxos_prepare_reply.decision(), true);
    // ppcb may destroy reference to itr. better to use safe erase-by-key.
    pending_paxos_prepares_.erase(paxos_prepare_reply.req_id());
    return;
  }

  auto decision = CountReplicaVote(
      paxos_prepare_reply.req_id(), paxos_prepare_reply.replica_id(),
      paxos_prepare_reply.vote(), pending_paxos_prepare->replica_votes);

  if (std::get<0>(decision) != proto::DECISION_NONE) {
    pending_paxos_prepare->ppcb(std::get<0>(decision), std::get<1>(decision));
    // pcb may destroy reference to itr. better to use safe erase-by-key.
    pending_paxos_prepares_.erase(paxos_prepare_reply.req_id());
    return;
  }
  if (std::get<2>(decision) >= static_cast<uint64_t>(config_->f + 1) &&
      pending_paxos_prepare->slow_path_timeout == nullptr) {
    Debug("[%lu][%lu][%lu] Group starting slow path timer.", client_id_, group_,
          paxos_prepare_reply.req_id());
    pending_paxos_prepare->slow_path_timeout = std::make_unique<Timeout>(
        transport_, slow_path_timeout_length_ms_,
        std::bind(&ShardClient::PrepareSlowPathTimeout, this,
                  paxos_prepare_reply_.req_id()));
    pending_paxos_prepare->slow_path_timeout->Start();
  }
}

void ShardClient::CleanPending(const Version &version) {
  for (auto itr = pending_reads_.begin(); itr != pending_reads_.end();) {
    if (itr->second->txn_version == version) {
      itr = pending_reads_.erase(itr);
    } else {
      itr++;
    }
  }
  for (auto itr = pending_prepares_.begin(); itr != pending_prepares_.end();) {
    if (itr->second->txn_version == version) {
      itr = pending_prepares_.erase(itr);
    } else {
      itr++;
    }
  }
  for (auto itr = pending_finalizes_.begin();
       itr != pending_finalizes_.end();) {
    if (itr->second->txn_version == version) {
      itr = pending_finalizes_.erase(itr);
    } else {
      itr++;
    }
  }
  for (auto itr = pending_paxos_prepares_.begin();
       itr != pending_paxos_prepares_.end();) {
    if (itr->second->txn_version == version) {
      itr = pending_paxos_prepares_.erase(itr);
    } else {
      itr++;
    }
  }
}

void ShardClient::CleanPendingDecides(const Version &version) {
  for (auto itr = pending_decides_.begin(); itr != pending_decides_.end();) {
    if (itr->second->txn_version == version) {
      itr = pending_decides_.erase(itr);
    } else {
      itr++;
    }
  }
}

void ShardClient::PrepareSlowPathTimeout(uint64_t req_id) {
  auto prepares_itr = pending_prepares_.find(req_id);

  if (prepares_itr == pending_prepares_.end()) {
    return;
  }

  auto &pending_prepare = prepares_itr->second;

  proto::Decision decision;
  if (pending_prepare->replica_votes[proto::VOTE_COMMIT].size() >=
      static_cast<uint64_t>(config_->f + 1)) {
    decision = proto::DECISION_COMMIT;
  } else {
    decision = proto::DECISION_ABANDON;
  }

  prepares_itr->second->pcb(decision, false);
  // pcb may destroy reference to preapres_itr. better to use safe erase-by-key.
  pending_prepares_.erase(req_id);
}

void ShardClient::FinalizeRecoveryTimeout(uint64_t req_id) {}

}  // namespace mortystorev2
