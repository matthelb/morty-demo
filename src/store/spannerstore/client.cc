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
#include "store/spannerstore/client.h"

#include <bits/refwrap.h>
#include <bits/std_function.h>

#include <algorithm>
#include <climits>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/common/transaction.h"
#include "store/spannerstore/shardclient.h"
#include "store/spannerstore/truetime.h"

class Transport;

namespace spannerstore {

SpannerContext::SpannerContext(std::uint64_t transaction_id)
    : transaction_id_{transaction_id} {}

SpannerContext::~SpannerContext() = default;

std::unique_ptr<TxnContext> SpannerContext::Clone() const {
  return std::make_unique<SpannerContext>(*this);
}

SpannerSession::SpannerSession(const Timestamp& start_ts)
    : start_ts_{start_ts},

      current_participant_{-1},
      state_{EXECUTING} {
  Debug("SpannerSession created: %lu.%lu", start_ts_.getTimestamp(),
        start_ts_.getID());
}

SpannerSession::~SpannerSession() {
  Debug("SpannerSession destroyed: %lu.%lu", start_ts_.getTimestamp(),
        start_ts_.getID());
}

Client::Client(transport::Configuration* config, std::uint64_t client_id,
               int n_shards, int closest_replica, Transport* transport,
               Partitioner* part, TrueTime& tt,
               CoordinatorSelection coord_select, bool debug_stats)
    : last_start_ts_{0, 0},

      config_{config},
      client_id_{client_id},
      n_shards_{static_cast<std::uint64_t>(n_shards)},
      transport_{transport},
      part_{part},
      tt_{tt},
      coord_select_{coord_select},
      next_transaction_id_{client_id_ << 26},
      consistency_{Consistency::SS},
      nb_time_alpha_{1.0},
      debug_stats_{debug_stats} {
  Debug("Initializing Spanner client with id [%lu]", client_id_);

  auto wcb = std::bind(&Client::HandleWound, this, std::placeholders::_1);

  /* Start a client for each shard. */
  for (std::uint64_t i = 0; i < n_shards_; i++) {
    auto* s = new ShardClient(*config_, transport_, client_id_, i, wcb);
    sclients_.push_back(s);
  }

  Debug("SpanStore client [%lu] created!", client_id_);

  if (debug_stats_) {
    _Latency_Init(&op_lat_, "op_lat");
    _Latency_Init(&commit_lat_, "commit_lat");
  }
}

Client::~Client() {
  if (debug_stats_) {
    Latency_Dump(&op_lat_);
    Latency_Dump(&commit_lat_);
  }

  for (auto* s : sclients_) {
    delete s;
  }
}

int Client::ChooseCoordinator(std::uint64_t tid,
                              std::unique_ptr<SpannerSession>& session) {
  auto& participants = session->participants();
  UW_ASSERT(!participants.empty());

  int coord = -1;

  switch (coord_select_) {
    case CoordinatorSelection::MIN_SHARD: {
      coord = INT_MAX;
      for (auto p : participants) {
        coord = std::min(coord, p);
      }
      break;
    }
    case CoordinatorSelection::MIN_KEY: {
      std::string min = min;
      for (auto p : participants) {
        auto maybe_t = sclients_[p]->FindTransaction(tid);
        if (maybe_t) {
          auto& t = maybe_t->get();

          for (auto& r : t.getReadSet()) {
            auto& key = r.first;
            if (min.empty() || key < min) {
              min = key;
              coord = p;
            }
          }

          for (auto& w : t.getWriteSet()) {
            auto& key = w.first;
            if (min.empty() || key < min) {
              min = key;
              coord = p;
            }
          }
        }
      }
      break;
    }
    case CoordinatorSelection::RANDOM: {
      auto it = participants.cbegin();
      std::advance(it, tid % participants.size());
      coord = *it;
      break;
    }
    default:
      Panic("Unexpected coordinator selection: %s",
            std::to_string(coord_select_).c_str());
  }

  return coord;
}

void Client::Cleanup() {
  Debug("Starting cleanup");

  for (auto& kv : sessions_) {
    auto tid = kv.first;
    HandleWound(tid);
  }
}

void Client::HandleWound(std::uint64_t transaction_id) {
  Debug("[%lu] Handling wound", transaction_id);

  auto search = sessions_.find(transaction_id);
  if (search == sessions_.end()) {
    Debug("[%lu] Transaction already finished", transaction_id);
    return;
  }
  auto& session = search->second;

  if (session->participants().empty()) {
    Debug("[%lu] RO Transaction...letting finish", transaction_id);
    return;
  }

  int p = -1;
  int coordinator = -1;
  Debug("[%lu] client state: %d", transaction_id, session->state());
  switch (session->state()) {
    case SpannerSession::EXECUTING:
      session->set_needs_abort();
      break;
    case SpannerSession::GETTING:
      p = session->current_participant();
      sclients_[p]->AbortGet(transaction_id);
      break;
    case SpannerSession::PUTTING:
      p = session->current_participant();
      sclients_[p]->AbortPut(transaction_id);
      break;
    case SpannerSession::COMMITTING:
      coordinator = ChooseCoordinator(transaction_id, session);
      // TODO(matthelb): Need better fix. Add an RO_COMMITTING state?
      if (coordinator == -1) {
        Debug("[%lu] Letting RO transaction finish", transaction_id);
      } else {
        Debug("[%lu] Forwarding wound to coordinator: %d", transaction_id,
              coordinator);
        sclients_[coordinator]->Wound(transaction_id);
      }
      break;
    case SpannerSession::ABORTING:
      Debug("[%lu] Already aborted", transaction_id);
      break;
    default:
      Panic("Unexpected state: %d", session->state());
  }
}

void Client::Begin(std::unique_ptr<AppContext>& context,
                   context::abort_callback acb) {
  auto tid = next_transaction_id_++;
  Timestamp start_ts{tt_.Now().latest(), client_id_};
  last_start_ts_ = start_ts;

  Debug("[%lu] Begin: %lu.%lu", tid, start_ts.getTimestamp(), start_ts.getID());

  sessions_.emplace(tid, std::make_unique<SpannerSession>(start_ts));

  for (std::uint64_t i = 0; i < n_shards_; i++) {
    sclients_[i]->Begin(tid, start_ts);
  }

  context->set_txn_context(std::make_unique<SpannerContext>(tid));
}

void Client::Retry(std::unique_ptr<AppContext>& context,
                   context::abort_callback acb) {
  auto tid = next_transaction_id_++;
  auto start_ts = last_start_ts_;

  Debug("[%lu] Retry: %lu.%lu", tid, start_ts.getTimestamp(), start_ts.getID());

  sessions_.emplace(tid, std::make_unique<SpannerSession>(start_ts));

  for (std::uint64_t i = 0; i < n_shards_; i++) {
    sclients_[i]->Begin(tid, start_ts);
  }

  context->set_txn_context(std::make_unique<SpannerContext>(tid));
}

void Client::Get(std::unique_ptr<AppContext> context, const std::string& key,
                 context::op_callback ocb) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("GET [%lu : %s]", tid, key.c_str());

  auto& session = sessions_.at(tid);

  if (session->needs_aborts()) {
    Debug("[%lu] Need to abort", tid);
    ocb(std::move(context), context::FAIL, "", "");
    return;
  }

  UW_ASSERT(session->executing());

  // Contact the appropriate shard to get the value.
  auto i = (*part_)(key, n_shards_, -1, session->participants());

  session->set_getting(i);

  // Add this shard to set of participants
  session->add_participant(i);

  auto gcb = std::bind(&Client::GetCallback, this, context.release(), ocb,
                       std::placeholders::_1, std::placeholders::_2,
                       std::placeholders::_3, std::placeholders::_4);

  auto gtcb = std::bind(&Client::GetTimeoutCallback, this, context.release(),
                        std::placeholders::_1, std::placeholders::_2);

  // Send the GET operation to appropriate shard.
  sclients_[i]->Get(tid, key, gcb, gtcb, 10000);
}

void Client::GetForUpdate(std::unique_ptr<AppContext> context,
                          const std::string& key, context::op_callback ocb) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("GET FOR UPDATE [%lu : %s]", tid, key.c_str());

  auto& session = sessions_.at(tid);

  if (session->needs_aborts()) {
    Debug("[%lu] Need to abort", tid);
    ocb(std::move(context), context::FAIL, "", "");
    return;
  }

  UW_ASSERT(session->executing());

  // Contact the appropriate shard to get the value.
  auto i = (*part_)(key, n_shards_, -1, session->participants());

  session->set_getting(i);

  // Add this shard to set of participants
  session->add_participant(i);

  auto gcb = std::bind(&Client::GetCallback, this, context.release(), ocb,
                       std::placeholders::_1, std::placeholders::_2,
                       std::placeholders::_3, std::placeholders::_4);

  auto gtcb = std::bind(&Client::GetTimeoutCallback, this, context.release(),
                        std::placeholders::_1, std::placeholders::_2);

  // Send the GET FOR UPDATE operation to appropriate shard.
  sclients_[i]->GetForUpdate(tid, key, gcb, gtcb, 10000);
}

void Client::GetCallback(AppContext* context, const context::op_callback& ocb,
                         int status, const std::string& key,
                         const std::string& val, const Timestamp& ts) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  auto& session = sessions_.at(tid);
  session->set_executing();

  context::transaction_op_status_t op_status;
  switch (status) {
    case REPLY_OK:
      op_status = context::OK;
      break;
    default:
      op_status = context::FAIL;
      break;
  }

  ocb(std::unique_ptr<AppContext>(context), op_status, key, val);
}

void Client::GetTimeoutCallback(AppContext* context, int status,
                                const std::string& key) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  auto& session = sessions_.at(tid);

  session->set_executing();
}

void Client::Put(std::unique_ptr<AppContext>& context, const std::string& key,
                 const std::string& val) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  auto& session = sessions_.at(tid);

  Debug("PUT [%lu : %s]", tid, key.c_str());

  if (session->needs_aborts()) {
    Debug("[%lu] Need to abort", tid);
    // ocb(std::move(context), context::FAIL, "", "");
    return;
  }

  UW_ASSERT(session->executing());

  // Contact the appropriate shard to set the value.
  auto i = (*part_)(key, n_shards_, -1, session->participants());

  session->set_putting(i);

  // Add this shard to set of participants
  session->add_participant(i);

  auto pcb = std::bind(&Client::PutCallback, this, std::placeholders::_1,
                       std::placeholders::_2, std::placeholders::_3);

  auto ptcb =
      std::bind(&Client::PutTimeoutCallback, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3);

  sclients_[i]->Put(tid, key, val, pcb, ptcb, 10000);

  session->set_executing();
}

void Client::PutCallback(int status, const std::string& key,
                         const std::string& val) {}

void Client::PutTimeoutCallback(int status, const std::string& key,
                                const std::string& val) {}

void Client::Commit(std::unique_ptr<AppContext> context,
                    context::commit_callback ccb) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  auto& session = sessions_.at(tid);

  Debug("[%lu] COMMIT", tid);

  if (session->needs_aborts()) {
    Debug("[%lu] Need to abort", tid);
    ccb(std::move(context), context::FAIL, ABORTED_SYSTEM);
    return;
  }

  UW_ASSERT(session->executing());
  session->set_committing();

  auto req_id = last_req_id_++;
  auto* req = new PendingRequest(req_id);
  pending_reqs_[req_id] = req;
  req->ccb = ccb;

  auto& participants = session->participants();

  GetStats().IncrementList("txn_groups", participants.size());

  Debug("[%lu] PREPARE", tid);
  UW_ASSERT(!participants.empty());

  req->outstanding_prepares = 0;

  auto coordinator_shard = ChooseCoordinator(tid, session);

  Timestamp nonblock_timestamp{};

  auto cccb = std::bind(&Client::CommitCallback, this, context.release(),
                        req->id, std::placeholders::_1, std::placeholders::_2,
                        std::placeholders::_3);
  auto cctcb = [](int) {};

  auto pccb = [transaction_id = tid](int status) {
    Debug("[%lu] PREPARE callback status %d", transaction_id, status);
  };
  auto pctcb = [](int) {};

  for (auto p : participants) {
    if (p == coordinator_shard) {
      sclients_[p]->RWCommitCoordinator(tid, participants, nonblock_timestamp,
                                        cccb, cctcb, 10000);
    } else {
      sclients_[p]->RWCommitParticipant(tid, coordinator_shard,
                                        nonblock_timestamp, pccb, pctcb, 10000);
    }
  }
}

void Client::CommitCallback(AppContext* context, std::uint64_t req_id,
                            int status, const Timestamp& commit_ts,
                            const Timestamp& nonblock_ts) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("[%lu] COMMIT callback status %d", tid, status);

  auto search = pending_reqs_.find(req_id);
  if (search == pending_reqs_.end()) {
    Debug("[%lu] Transaction already finished", tid);
    return;
  }
  auto* req = search->second;

  transaction_status_t tstatus;
  switch (status) {
    case REPLY_OK:
      Debug("[%lu] COMMIT OK", tid);
      tstatus = COMMITTED;
      break;
    default:
      // abort!
      Debug("[%lu] COMMIT ABORT", tid);
      tstatus = ABORTED_SYSTEM;
      break;
  }

  auto ccb = req->ccb;
  pending_reqs_.erase(req_id);
  delete req;

  sessions_.erase(tid);

  ccb(std::unique_ptr<AppContext>(context), context::OK, tstatus);
}

void Client::Abort(std::unique_ptr<AppContext>& context) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("[%lu] ABORT", tid);

  auto& session = sessions_.at(tid);

  UW_ASSERT(session->needs_aborts() || session->executing());
  session->set_aborting();

  auto& participants = session->participants();

  auto req_id = last_req_id_++;
  auto* req = new PendingRequest(req_id);
  pending_reqs_[req_id] = req;
  req->outstanding_prepares = participants.size();

  auto cb = std::bind(&Client::AbortCallback, this, tid, req->id);
  auto tcb = []() {};

  for (auto p : participants) {
    sclients_[p]->Abort(tid, cb, tcb, 10000);
  }
}

void Client::AbortCallback(std::uint64_t transaction_id, std::uint64_t req_id) {
  Debug("[%lu] Abort callback", transaction_id);

  auto search = pending_reqs_.find(req_id);
  if (search == pending_reqs_.end()) {
    Debug("[%lu] Transaction already finished", transaction_id);
    return;
  }

  auto* req = search->second;
  --req->outstanding_prepares;
  if (req->outstanding_prepares == 0) {
    pending_reqs_.erase(req_id);
    delete req;

    sessions_.erase(transaction_id);

    Debug("[%lu] Abort finished", transaction_id);
  }
}

void Client::ROBegin(std::unique_ptr<AppContext>& context,
                     context::abort_callback acb) {
  Begin(context, acb);

  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  auto& session = sessions_.at(tid);
  session->set_committing();

  Timestamp snapshot_ts{tt_.Now().latest(), client_id_};
  session->set_snapshot_ts(snapshot_ts);
}

void Client::ROGet(std::unique_ptr<AppContext> context,
                   const std::unordered_set<std::string>& keys,
                   context::bulk_op_callback ocb) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("[%lu] RO GET", tid);

  auto& session = sessions_.at(tid);

  auto& participants = session->participants();

  std::unordered_map<int, std::vector<std::string>> sharded_keys;
  for (auto& key : keys) {
    int i = (*part_)(key, n_shards_, -1, participants);
    sharded_keys[i].push_back(key);
    session->add_participant(i);
  }

  auto req_id = last_req_id_++;
  auto* req = new PendingRequest(req_id);
  pending_reqs_[req_id] = req;
  req->outstanding_prepares = sharded_keys.size();

  GetStats().IncrementList("txn_groups", sharded_keys.size());

  UW_ASSERT(!sharded_keys.empty());

  auto& snapshot_ts = session->snapshot_ts();
  Debug("[%lu] snapshot_ts: %lu.%lu", tid, snapshot_ts.getTimestamp(),
        snapshot_ts.getID());

  auto roccb = std::bind(&Client::ROGetCallback, this, context.release(),
                         req->id, ocb, std::placeholders::_1);
  auto roctcb = []() {};  // TODO(matthelb): Handle timeout

  for (auto& s : sharded_keys) {
    sclients_[s.first]->ROCommit(tid, s.second, snapshot_ts, roccb, roctcb,
                                 10000);
  }
}

void Client::ROGetCallback(AppContext* context, std::uint64_t req_id,
                           const context::bulk_op_callback& ocb,
                           const std::vector<Value>& values) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("[%lu] ROGet callback", tid);

  auto search = pending_reqs_.find(req_id);
  if (search == pending_reqs_.end()) {
    Debug("[%lu] ROGetCallback for terminated request id %lu", tid, req_id);
    return;
  }

  auto& session = sessions_.at(tid);
  AddValues(session, values);

  auto* req = search->second;
  --req->outstanding_prepares;
  if (req->outstanding_prepares == 0) {
    pending_reqs_.erase(search);
    delete req;

    std::unordered_map<std::string, std::string> kvs;
    kvs.reserve(values.size());

    auto& values = session->mutable_values();
    for (auto& vls : values) {
      auto& key = vls.first;
      auto& vals = vls.second;
      UW_ASSERT(vals.size() == 1);

      kvs[key] = vals.front().val();
    }

    ocb(std::unique_ptr<AppContext>(context), context::OK, kvs);
  } else {
    Debug("[%lu] Waiting for more RO responses", tid);
  }
}

void Client::ROCommit(std::unique_ptr<AppContext> context,
                      context::commit_callback ccb) {
  const auto* ctx = dynamic_cast<const SpannerContext*>(context->txn_context());
  auto tid = ctx->transaction_id();

  Debug("[%lu] RO COMMIT", tid);

  sessions_.erase(tid);

  Debug("[%lu] COMMIT OK", tid);
  ccb(std::move(context), context::OK, COMMITTED);
}

void Client::AddValues(std::unique_ptr<SpannerSession>& session,
                       const std::vector<Value>& vs) {
  auto& values = session->mutable_values();

  for (auto& v : vs) {
    auto& l = values[v.key()];

    auto it = l.begin();
    for (; it != l.end(); ++it) {
      auto& v2 = *it;
      if (v2.ts() < v.ts()) {
        break;
      }
    }

    l.insert(it, v);
  }
}

}  // namespace spannerstore
