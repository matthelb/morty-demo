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
#include "store/mortystorev2/client.h"

#include <bits/std_function.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <type_traits>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/common.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/mortystorev2/shardclient.h"

#define TIMEOUT 1000

namespace mortystorev2 {

Client::Client(transport::Configuration *config, uint64_t client_id,
               int num_shards, int num_groups, uint64_t branch_backoff_factor,
               uint64_t branch_backoff_max, bool logical_timestamps,
               bool fast_path_enabled, bool pipeline_commit,
               bool reexecution_enabled, uint64_t slow_path_timeout_ms,
               uint64_t server_num_workers, int closest_replica,
               Transport *transport, Partitioner *part)
    : config_(config),
      id_(client_id),
      num_shards_(num_shards),
      num_groups_(num_groups),
      branch_backoff_factor_(branch_backoff_factor),
      branch_backoff_max_(branch_backoff_max),
      fast_path_enabled_(fast_path_enabled),
      pipeline_commit_(pipeline_commit),
      reexecution_enabled_(reexecution_enabled),
      server_num_workers_(server_num_workers),
      rand_(client_id),
      transport_(transport),
      part_(part),
      last_seq_(0UL) {
  Debug("Initializing Morty client with id [%lu] %d", id_, num_groups);

  for (int i = 0; i < num_groups; i++) {
    shard_clients_.push_back(std::move(std::make_unique<ShardClient>(
        config_, transport_, id_, i, closest_replica, slow_path_timeout_ms,
        pipeline_commit, server_num_workers)));
  }

  Debug("Morty client [%lu] created! %d", id_, num_groups);
}

Client::~Client() {
  while (!txn_queue_.empty()) {
    proto::Transaction *txn = txn_queue_.front();
    delete txn;
    txn_queue_.pop();
  }
}

void Client::Begin(std::unique_ptr<AppContext> &context,
                   context::abort_callback acb) {
  uint64_t txn_seq = std::max(time_.GetTime(), last_seq_ + 1);
  txn_seq =
      txn_seq - txn_seq % server_num_workers_ + (id_ % server_num_workers_);
  auto insert_itr = pending_txns_.insert(std::make_pair(
      txn_seq, std::make_unique<PendingTransaction>(txn_seq, id_)));
  last_seq_ = txn_seq;

  auto *txn_context = new MortyContext();
  txn_context->txn_seq = txn_seq;
  context->set_txn_context(std::unique_ptr<TxnContext>(txn_context));

  insert_itr.first->second->executions.insert(
      std::make_pair(txn_context->exec_id, txn_context->op_order));
  insert_itr.first->second->acb = std::move(acb);
}

void Client::Get(std::unique_ptr<AppContext> context, const std::string &key,
                 context::op_callback ocb) {
  auto *txn_context = dynamic_cast<MortyContext *>(context->txn_context());

  auto txn_itr = pending_txns_.find(txn_context->txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Panic("[%lu] Couldn't find txn %lu in pending map.", id_,
          txn_context->txn_seq);
  }

  auto &txn = txn_itr->second;

  Debug("[%lu][%lu][%lu] Get for key %s.", txn->version.GetClientId(),
        txn->version.GetClientSeq(), txn_context->exec_id,
        BytesToHex(key, 16).c_str());

  // to avoid copying a large amount of strings during branching, we first
  //    substitute a string pointer for the key string
  const std::string *key_ptr;
  auto keys_itr = txn->keys.insert(key);
  key_ptr = &(*keys_itr.first);

  // add this get to the partial order of operations in this txn
  txn_context->op_order.AddElement();
  txn->executions[txn_context->exec_id] = txn_context->op_order;
  txn->executing_context = txn_context;

  // determine the server group that stores this key
  auto group_for_key =
      (*part_)(key, num_shards_, -1, txn_context->participants) % num_groups_;
  if (std::find(txn_context->participants.begin(),
                txn_context->participants.end(),
                group_for_key) == txn_context->participants.end()) {
    txn_context->participants.push_back(group_for_key);
  }

  // check that we haven't already written this key
  // if we have, return that same value that we wrote.
  auto write_set_itr = txn_context->write_set[group_for_key].find(key_ptr);
  if (write_set_itr != txn_context->write_set[group_for_key].end()) {
    ocb(std::move(context), context::OK, key,
        *std::get<0>(write_set_itr->second));
    return;
  }

  // check that we haven't already read this key
  // if we have, return that same value that we previously read
  auto read_set_itr = txn_context->read_set[group_for_key].find(key_ptr);
  if (read_set_itr != txn_context->read_set[group_for_key].end()) {
    ocb(std::move(context), context::OK, key,
        *std::get<0>(read_set_itr->second));
    return;
  }

  // also add group to set of participants across all contexts for this txn.
  // this set will be used if the txn is aborted to notify all groups of the
  // abort.
  if (txn->participants.find(group_for_key) == txn->participants.end()) {
    txn->participants.insert(group_for_key);
  }

  // explicitly store the invoking context. this copy will be used as the base
  // context when the server group sends responses for this operation.
  uint64_t cont_id = txn->last_cont_id + 1;
  txn->last_cont_id = cont_id;
  auto cont_insert_itr = txn->continuations.insert(std::make_pair(
      cont_id, std::make_unique<Continuation>(std::move(context))));
  cont_insert_itr.first->second->ocb = ocb;

  // send get to server group and set up callback function to handle responses
  auto gcb = std::bind(
      &Client::GetCallback, this, group_for_key, txn_context->txn_seq, cont_id,
      key_ptr, std::placeholders::_1, std::placeholders::_2,
      std::placeholders::_3, std::placeholders::_4, std::placeholders::_5,
      std::placeholders::_6, std::placeholders::_7);

  shard_clients_[group_for_key]->Get(txn->version, key, std::move(gcb));
}

void Client::Put(std::unique_ptr<AppContext> &context, const std::string &key,
                 const std::string &val) {
  auto *txn_context = dynamic_cast<MortyContext *>(context->txn_context());

  auto txn_itr = pending_txns_.find(txn_context->txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Panic("[%lu] Couldn't find txn %lu in pending map.", id_,
          txn_context->txn_seq);
  }

  auto &txn = txn_itr->second;

  Debug("[%lu][%lu][%lu] Put for key %s with val %s.",
        txn->version.GetClientId(), txn->version.GetClientSeq(),
        txn_context->exec_id, BytesToHex(key, 16).c_str(),
        BytesToHex(val, 16).c_str());

  // to avoid copying a large amount of strings during branching, we first
  //    substitute a string pointer for the key string and value string
  const std::string *key_ptr;
  auto keys_itr = txn->keys.insert(key);
  key_ptr = &(*keys_itr.first);

  const std::string *val_ptr;
  auto vals_itr = txn->vals.insert(val);
  val_ptr = &(*vals_itr.first);

  // add this put to the partial order of operations in this txn
  txn_context->op_order.AddElement();
  txn->executions[txn_context->exec_id] = txn_context->op_order;
  txn->executing_context = txn_context;

  // determine the server group that stores this key
  auto group_for_key =
      (*part_)(key, num_shards_, -1, txn_context->participants) % num_groups_;
  if (std::find(txn_context->participants.begin(),
                txn_context->participants.end(),
                group_for_key) == txn_context->participants.end()) {
    txn_context->participants.push_back(group_for_key);
  }

  // also add group to set of participants across all contexts for this txn.
  // this set will be used if the txn is aborted to notify all groups of the
  // abort.
  if (txn->participants.find(group_for_key) == txn->participants.end()) {
    txn->participants.insert(group_for_key);
  }

  // add put to write set
  // N.B. to support applications that write to the same key multiple times in
  //   the same txn, we must overwrite the existing write in the write set
  uint64_t val_id;
  auto &write_val_cache = txn->write_val_cache[key_ptr];
  auto write_val_itr = write_val_cache.find(val_ptr);
  bool send_writes = false;
  if (write_val_itr == write_val_cache.end()) {
    val_id = txn->write_val_counter;
    txn->write_val_counter++;
    write_val_cache.insert(std::make_pair(val_ptr, val_id));
    send_writes = true;
  } else {
    val_id = write_val_itr->second;
    send_writes = false;
  }

  txn_context->write_set[group_for_key][key_ptr] =
      std::make_tuple(val_ptr, val_id);

  if (send_writes) {
    shard_clients_[group_for_key]->Put(txn->version, key, val, val_id);
  }
}

void Client::Commit(std::unique_ptr<AppContext> context,
                    context::commit_callback ccb) {
  auto *txn_context = dynamic_cast<MortyContext *>(context->txn_context());

  auto txn_itr = pending_txns_.find(txn_context->txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Panic("Couldn't find transaction %lu in pending map.",
          txn_context->txn_seq);
  }

  auto &txn = txn_itr->second;

  Debug("[%lu][%lu][%lu] Commit.", txn->version.GetClientId(),
        txn->version.GetClientSeq(), txn_context->exec_id);

  for (const auto &committing_context : txn->committing_contexts) {
    auto &app_context = committing_context.second;
    auto *committing_txn_context =
        dynamic_cast<MortyContext *>(app_context->txn_context());

    if (committing_txn_context->commit_metadata->state ==
        MortyContext::CommitMetadata::COMMIT_STATE_PREPARING) {
      committing_txn_context->commit_metadata->decision =
          proto::DECISION_ABANDON;
      Finalize(txn, committing_txn_context);
    }
  }

  txn_context->op_order.AddElement();
  txn->executions[txn_context->exec_id] = txn_context->op_order;
  txn->executing_context = txn_context;

  txn_context->commit_metadata =
      std::make_unique<MortyContext::CommitMetadata>();
  txn_context->commit_metadata->ccb = std::move(ccb);
  txn_context->commit_metadata->state =
      MortyContext::CommitMetadata::COMMIT_STATE_PREPARING;
  txn->committing_contexts.insert(
      std::make_pair(txn_context->exec_id, std::move(context)));

  Prepare(txn, txn_context);
}

void Client::Abort(std::unique_ptr<AppContext> &context) {
  auto *txn_context = dynamic_cast<MortyContext *>(context->txn_context());

  auto txn_itr = pending_txns_.find(txn_context->txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Panic("Couldn't find transaction %lu in pending map.",
          txn_context->txn_seq);
  }

  // TODO(matthelb): how to handle user abort

  pending_txns_.erase(txn_itr);
}

Client::MortyContext::MortyContext()
    : txn_seq(0UL), exec_id(0UL), user_aborted(false) {}

Client::MortyContext::MortyContext(const MortyContext &other)
    : txn_seq(other.txn_seq),
      exec_id(other.exec_id),
      user_aborted(other.user_aborted),
      participants(other.participants),
      read_set(other.read_set),
      write_set(other.write_set),
      // deps(other.deps),
      op_order(other.op_order) {}

Client::MortyContext::~MortyContext() = default;

std::unique_ptr<TxnContext> Client::MortyContext::Clone() const {
  return std::make_unique<Client::MortyContext>(*this);
}

void Client::AbortContext(uint64_t req_id, uint64_t cont_id,
                          std::unique_ptr<PendingTransaction> &txn,
                          MortyContext *txn_context) {
  if (txn_context->exec_id < txn->executing_context->exec_id) {
    return;
  }

  Debug("[%lu][%lu] Fully aborting txn after ctx abort.",
        txn->version.GetClientId(), txn->version.GetClientSeq());

  for (const auto &group : txn->participants) {
    auto empty_dcb = [](proto::Decision decision, bool deps_changed_decision) {
    };
    shard_clients_[group]->Decide(
        txn->version, *txn_context->commit_metadata->group_txns[group],
        proto::DECISION_ABORT2, std::move(empty_dcb));
    // shard_clients_[group]->CleanPending(txn->version);
  }

  CleanupTransaction(txn);
  auto committing_ctx_itr = txn->committing_contexts.find(txn_context->exec_id);
  if (committing_ctx_itr != txn->committing_contexts.end()) {
    GetStats().Increment("total_reexecutions", txn->last_exec_id);
    txn_context->commit_metadata->ccb(
        std::move(committing_ctx_itr->second), context::OK,
        txn_context->user_aborted ? ABORTED_USER : ABORTED_SYSTEM);
  }

  pending_txns_.erase(req_id);
}

void Client::Prepare(std::unique_ptr<PendingTransaction> &txn,
                     MortyContext *txn_context) {
  GetStats().IncrementList("txn_groups", txn_context->participants.size());

  for (const auto &group : txn_context->participants) {
    SerializeTransaction(
        txn_context, txn_context->commit_metadata->group_txns[group], group);
    auto pcb = std::bind(&Client::PrepareCallback, this, group,
                         txn_context->txn_seq, txn_context->exec_id,
                         std::placeholders::_1, std::placeholders::_2);
    shard_clients_[group]->Prepare(
        txn->version, *txn_context->commit_metadata->group_txns[group],
        std::move(pcb));
  }
}

void Client::Finalize(std::unique_ptr<PendingTransaction> &txn,
                      MortyContext *txn_context) {
  txn_context->commit_metadata->state =
      MortyContext::CommitMetadata::COMMIT_STATE_FINALIZING;

  txn->sent_decide_or_finalize = true;

  for (const auto &group : txn_context->participants) {
    auto fcb =
        std::bind(&Client::FinalizeCallback, this, group, txn_context->txn_seq,
                  txn_context->exec_id, std::placeholders::_1);
    shard_clients_[group]->Finalize(
        txn->version, *txn_context->commit_metadata->group_txns[group],
        txn_context->commit_metadata->decision, std::move(fcb));
  }
}

void Client::Decide(std::unique_ptr<PendingTransaction> &txn,
                    MortyContext *txn_context) {
  if (txn_context->exec_id < txn->executing_context->exec_id) {
    // txn->committing_contexts.erase(txn_context->exec_id);
    Debug("[%lu][%lu][%lu] Decide is stale because of newer exec %lu.",
          txn->version.GetClientId(), txn->version.GetClientSeq(),
          txn_context->exec_id, txn->executing_context->exec_id);
    return;
  }

  txn_context->commit_metadata->state =
      MortyContext::CommitMetadata::COMMIT_STATE_DECIDED;

  if (txn_context->commit_metadata->decision == proto::DECISION_COMMIT) {
    txn->sent_decide_or_finalize = true;
    for (const auto &group : txn_context->participants) {
      auto dcb = std::bind(&Client::DecideCallback, this, group,
                           txn_context->txn_seq, txn_context->exec_id,
                           std::placeholders::_1, std::placeholders::_2);
      shard_clients_[group]->Decide(
          txn->version, *txn_context->commit_metadata->group_txns[group],
          proto::DECISION_COMMIT, std::move(dcb));
    }

    GetStats().Increment("total_reexecutions", txn->last_exec_id);

    if (pipeline_commit_ && txn_context->commit_metadata->fast) {
      return;
    }

    auto committing_ctx_itr =
        txn->committing_contexts.find(txn_context->exec_id);
    if (committing_ctx_itr == txn->committing_contexts.end()) {
      Panic("Panic");
    }

    uint64_t txn_seq = txn_context->txn_seq;

    CleanupTransaction(txn);
    txn_context->commit_metadata->ccb(std::move(committing_ctx_itr->second),
                                      context::OK, COMMITTED);

    pending_txns_.erase(txn_seq);
  } else {
    AbortContext(txn_context->txn_seq, txn_context->exec_id, txn, txn_context);
  }
}

void Client::GetCallback(int group, uint64_t txn_seq, uint64_t cont_id,
                         const std::string *key_ptr,
                         context::transaction_op_status_t status,
                         const std::string &val, uint64_t val_id,
                         const Version &version, bool committed,
                         uint64_t req_id, bool version_below_watermark) {
  auto txn_itr = pending_txns_.find(txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Debug("Received Get response for stale request %lu.", txn_seq);
    return;
  }

  auto &txn = txn_itr->second;

  if (pipeline_commit_ && txn->sent_decide_or_finalize) {
    // no re-executions allowed once we sent decide with pipelining
    return;
  }

  auto cont_itr = txn->continuations.find(cont_id);
  if (cont_itr == txn->continuations.end()) {
    Panic("Received Get response for stale/unknown context %lu.", cont_id);
    return;
  }

  auto &cont = cont_itr->second;
  auto *txn_context = dynamic_cast<MortyContext *>(cont->ctx->txn_context());

  Debug(
      "[%lu][%lu][%lu] GetCallback for key %s with val %lu from version "
      "%lu.%lu",
      txn->version.GetClientId(), txn->version.GetClientSeq(),
      txn_context->exec_id, BytesToHex(*key_ptr, 16).c_str(), val_id,
      version.GetClientId(), version.GetClientSeq());

  if (version_below_watermark) {
    AbortContext(txn_context->txn_seq, txn_context->exec_id, txn, txn_context);
    return;
  }

  if (reexecution_enabled_) {
    if (txn->executing_context->commit_metadata != nullptr &&
        txn->executing_context->commit_metadata->state !=
            MortyContext::CommitMetadata::COMMIT_STATE_PREPARING) {
      Debug(
          "[%lu][%lu][%lu] Cannot re-execute b/c current context is beyond "
          "Prepare phase.",
          txn->version.GetClientId(), txn->version.GetClientSeq(),
          txn_context->exec_id);
      return;
    }

    // check that the read set of this context is a subset of the read set of
    // the executing context. if not, the context is stale.
    for (const auto &group_read_set : txn_context->read_set) {
      auto group_read_set_itr =
          txn->executing_context->read_set.find(group_read_set.first);
      if (group_read_set_itr == txn->executing_context->read_set.end()) {
        return;
      }

      for (const auto &read_set_entry : group_read_set.second) {
        auto read_set_itr =
            group_read_set_itr->second.find(read_set_entry.first);
        if (read_set_itr == group_read_set_itr->second.end()) {
          return;
        }

        if (!(read_set_itr->second == read_set_entry.second)) {
          return;
        }
      }
    }
  }

  // to avoid copying a large amount of strings during branching, we
  //    substitute a string pointer for the value string
  const std::string *val_ptr;
  auto vals_itr = txn->vals.insert(val);
  val_ptr = &(*vals_itr.first);

  std::unique_ptr<AppContext> ctx_clone;
  if (reexecution_enabled_) {
    ctx_clone = cont->ctx->Clone();
  } else {
    ctx_clone = std::move(cont->ctx);
  }

  auto *txn_context_clone =
      dynamic_cast<MortyContext *>(ctx_clone->txn_context());

  auto exec_itr = txn->executions.find(txn_context_clone->exec_id);
  UW_ASSERT(exec_itr != txn->executions.end());
  bool branched = false;
  if (txn_context_clone->op_order.GetIdx() != exec_itr->second.GetIdx()) {
    UW_ASSERT(txn_context_clone->op_order.GetIdx() < exec_itr->second.GetIdx());
    txn_context_clone->exec_id = txn->last_exec_id + 1;
    txn->last_exec_id++;
    txn->executions.insert(std::make_pair(txn_context_clone->exec_id,
                                          txn_context_clone->op_order));
    txn->executing_context = txn_context_clone;
    branched = true;
  }

  txn_context_clone->read_set[group][key_ptr] =
      std::make_tuple(val_ptr, val_id, version, req_id, committed);
  /*if (!committed) {
    txn_context_clone->deps[group].insert(version);
  }*/

  if (branched && branch_backoff_factor_ > 0) {
    uint64_t exp = std::min(txn_context_clone->exec_id - 1UL, 56UL);
    uint64_t upper =
        std::min((1UL << exp) * branch_backoff_factor_, branch_backoff_max_);
    uint64_t backoff =
        std::uniform_int_distribution<uint64_t>(0UL, upper)(rand_);
    transport_->TimerMicro(
        backoff, [this, txn_seq, cont_id, ctx = ctx_clone.release(), status,
                  key_ptr, val_ptr]() {
          auto txn_itr = pending_txns_.find(txn_seq);
          if (txn_itr == pending_txns_.end()) {
            Debug("Request %lu for backed off branch is stale.", txn_seq);
            return;
          }

          auto cont_itr = txn_itr->second->continuations.find(cont_id);
          if (cont_itr == txn_itr->second->continuations.end()) {
            Debug("[%lu][%lu] Continuation %lu for backed off branch is stale.",
                  txn_itr->second->version.GetClientId(),
                  txn_itr->second->version.GetClientSeq(), cont_id);
            return;
          }

          auto *txn_context = dynamic_cast<MortyContext *>(ctx->txn_context());
          if (txn_context->exec_id < txn_itr->second->last_exec_id) {
            Debug("[%lu][%lu] Branch %lu is not latest branch %lu.",
                  txn_itr->second->version.GetClientId(),
                  txn_itr->second->version.GetClientSeq(), txn_context->exec_id,
                  txn_itr->second->last_exec_id);
            return;
          }

          cont_itr->second->ocb(std::move(std::unique_ptr<AppContext>(ctx)),
                                status, *key_ptr, *val_ptr);
        });
  } else {
    cont->ocb(std::move(ctx_clone), status, *key_ptr, val);
  }
}

void Client::PrepareCallback(int group, uint64_t txn_seq, uint64_t exec_id,
                             proto::Decision decision, bool fast) {
  auto txn_itr = pending_txns_.find(txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Debug("[%lu] Received Prepare response for stale request %lu.", id_,
          txn_seq);
    return;
  }

  auto &txn = txn_itr->second;

  auto ctx_itr = txn->committing_contexts.find(exec_id);
  if (ctx_itr == txn->committing_contexts.end()) {
    Debug(
        "[%lu][%lu] Received Prepare response for stale/unknown execution %lu.",
        id_, txn_seq, exec_id);
    return;
  }

  auto &ctx = ctx_itr->second;

  auto *txn_context = dynamic_cast<MortyContext *>(ctx->txn_context());

  Debug("[%lu][%lu][%lu] PrepareCallback with %s decision %d",
        txn->version.GetClientId(), txn->version.GetClientSeq(),
        txn_context->exec_id, fast ? "fast" : "slow", decision);

  if (txn_context->commit_metadata->state !=
      MortyContext::CommitMetadata::COMMIT_STATE_PREPARING) {
    Debug("[%lu][%lu][%lu] Ignoring PrepareCallback because commit state %d.",
          txn->version.GetClientId(), txn->version.GetClientSeq(),
          txn_context->exec_id, txn_context->commit_metadata->state);
    return;
  }

  txn_context->commit_metadata->group_decisions.insert(
      std::make_pair(group, decision));
  // 2PC: all participants must agree to commit, otherwise must abort
  if (decision == proto::DECISION_ABANDON) {
    txn_context->commit_metadata->decision = proto::DECISION_ABANDON;
  }

  if (!fast || !fast_path_enabled_) {
    txn_context->commit_metadata->fast = false;
  }

  if (txn_context->commit_metadata->group_decisions.size() !=
          txn_context->participants.size() &&
      txn_context->commit_metadata->decision != proto::DECISION_ABANDON) {
    return;
  }

  if (fast) {
    GetStats().Increment("fast_path");
    Decide(txn, txn_context);
  } else {
    GetStats().Increment("slow_path");
    Finalize(txn, txn_context);
  }
}

void Client::FinalizeCallback(int group, uint64_t txn_seq, uint64_t exec_id,
                              proto::Decision decision) {
  auto txn_itr = pending_txns_.find(txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Debug("[%lu] FinalizeCallback for stale transaction %lu.", id_, txn_seq);
    return;
  }

  auto &txn = txn_itr->second;

  auto ctx_itr = txn->committing_contexts.find(exec_id);
  if (ctx_itr == txn->committing_contexts.end()) {
    Debug("[%lu][%lu] FinalizeCallback for stale/unknown execution %lu.", id_,
          txn_seq, exec_id);
    return;
  }

  auto &ctx = ctx_itr->second;

  auto *txn_context = dynamic_cast<MortyContext *>(ctx->txn_context());

  Debug("[%lu][%lu][%lu] FinalizeCallback with decision %d",
        txn->version.GetClientId(), txn->version.GetClientSeq(),
        txn_context->exec_id, decision);

  if (txn_context->commit_metadata->state !=
      MortyContext::CommitMetadata::COMMIT_STATE_FINALIZING) {
    Debug("[%lu][%lu][%lu] Ignoring FinalizeCallback because commit state %d.",
          txn->version.GetClientId(), txn->version.GetClientSeq(),
          txn_context->exec_id, txn_context->commit_metadata->state);
    return;
  }

  txn_context->commit_metadata->groups_finalized.insert(group);

  if (decision == proto::DECISION_ABANDON &&
      txn_context->commit_metadata->decision == proto::DECISION_COMMIT) {
    txn_context->commit_metadata->decision = decision;
  }

  if (txn_context->commit_metadata->groups_finalized.size() !=
      txn_context->participants.size()) {
    Debug("[%lu][%lu][%lu] Only received %lu out of %lu Finalize acks.",
          txn->version.GetClientId(), txn->version.GetClientSeq(),
          txn_context->exec_id,
          txn_context->commit_metadata->groups_finalized.size(),
          txn_context->participants.size());
    return;
  }

  Decide(txn, txn_context);
}

void Client::DecideCallback(int group, uint64_t txn_seq, uint64_t exec_id,
                            proto::Decision decision,
                            bool deps_changed_decision) {
  // N.B. only invoked when pipeline_commit is enabled - otherwise, original
  // coordinator immediately returns to application after sending decides (does
  // not wait for DecideReplies)
  auto txn_itr = pending_txns_.find(txn_seq);
  if (txn_itr == pending_txns_.end()) {
    Debug("[%lu] DecideCallback for stale transaction %lu.", id_, txn_seq);
    return;
  }

  auto &txn = txn_itr->second;

  auto ctx_itr = txn->committing_contexts.find(exec_id);
  if (ctx_itr == txn->committing_contexts.end()) {
    Debug("[%lu][%lu] DecideCallback for stale/unknown execution %lu.", id_,
          txn_seq, exec_id);
    return;
  }

  auto &ctx = ctx_itr->second;

  auto *txn_context = dynamic_cast<MortyContext *>(ctx->txn_context());

  Debug("[%lu][%lu][%lu] DecideCallback with decision %d",
        txn->version.GetClientId(), txn->version.GetClientSeq(),
        txn_context->exec_id, decision);

  if (decision == proto::DECISION_ABANDON) {
    // TODO(matthelb): we want similar logic to AbortContext, however, we've
    // already finalized the abort decision. Basically, we just need to notify
    // the application if there are no remaining executions that could commit.
    if (txn_context->exec_id < txn->executing_context->exec_id) {
      return;
    }

    if (deps_changed_decision) {
      for (const auto &group : txn->participants) {
        auto empty_dcb = [](proto::Decision decision,
                            bool deps_changed_decision) {};
        shard_clients_[group]->Decide(
            txn->version, *txn_context->commit_metadata->group_txns[group],
            proto::DECISION_ABORT2, std::move(empty_dcb));
        // shard_clients_[group]->CleanPending(txn->version);
      }
    }

    uint64_t txn_seq = txn_context->txn_seq;

    CleanupTransaction(txn);
    auto committing_ctx_itr =
        txn->committing_contexts.find(txn_context->exec_id);
    if (committing_ctx_itr != txn->committing_contexts.end()) {
      GetStats().Increment("total_reexecutions", txn->last_exec_id);
      txn_context->commit_metadata->ccb(std::move(committing_ctx_itr->second),
                                        context::OK, ABORTED_SYSTEM);
    }

    pending_txns_.erase(txn_seq);
    return;
  }

  txn_context->commit_metadata->groups_decided.insert(group);

  if (txn_context->commit_metadata->groups_decided.size() !=
      txn_context->participants.size()) {
    return;
  }

  CleanupTransaction(txn);
  txn_context->commit_metadata->ccb(std::move(ctx), context::OK, COMMITTED);

  pending_txns_.erase(txn_seq);
}

void Client::SerializeTransaction(MortyContext *txn_context,
                                  proto::Transaction *&txn, int group) {
  if (!txn_queue_.empty()) {
    txn = txn_queue_.front();
    txn_queue_.pop();
  } else {
    txn = new proto::Transaction();
  }

  txn->Clear();

  Debug("[%lu][%lu][%lu] Building txn for group %d.", id_, txn_context->txn_seq,
        txn_context->exec_id, group);

  txn->mutable_version()->set_client_seq(txn_context->txn_seq);
  txn->mutable_version()->set_client_id(id_);
  txn->set_exec_id(txn_context->exec_id);

  /*for (const auto &dep : txn_context->deps[group]) {
    Debug("[%lu][%lu][%lu][%d] Adding dep %lu.%lu.", id_, txn_context->txn_seq,
        txn_context->exec_id,
        group,
        dep.GetClientId(),
        dep.GetClientSeq());
    proto::Dependency *dep_proto = txn->add_deps();
    dep.Serialize(dep_proto->mutable_version());
  }*/

  for (const auto &read_set_pair : txn_context->read_set[group]) {
    proto::ReadSetEntry *read_set_entry = txn->add_read_set();
    read_set_entry->set_key(*read_set_pair.first);
    read_set_entry->set_val_id(std::get<1>(read_set_pair.second));
    std::get<2>(read_set_pair.second)
        .Serialize(read_set_entry->mutable_version());
    read_set_entry->set_req_id(std::get<3>(read_set_pair.second));
    read_set_entry->set_committed(std::get<4>(read_set_pair.second));
  }

  for (const auto &write_set_pair : txn_context->write_set[group]) {
    proto::WriteSetEntry *write_set_entry = txn->add_write_set();
    write_set_entry->set_key(*write_set_pair.first);
    write_set_entry->set_val(*std::get<0>(write_set_pair.second));
    write_set_entry->set_val_id(std::get<1>(write_set_pair.second));
  }
}

void Client::CleanupTransactionContext(MortyContext *txn_context) {
  if (!txn_context->commit_metadata) {
    return;
  }

  for (const auto &group_txn : txn_context->commit_metadata->group_txns) {
    FreeTransaction(group_txn.second);
  }
}

void Client::CleanupTransaction(std::unique_ptr<PendingTransaction> &txn) {
  for (auto &committing_context : txn->committing_contexts) {
    auto *txn_context =
        dynamic_cast<MortyContext *>(committing_context.second->txn_context());
    CleanupTransactionContext(txn_context);
  }
}

void Client::FreeTransaction(proto::Transaction *txn) { txn_queue_.push(txn); }

}  // namespace mortystorev2
