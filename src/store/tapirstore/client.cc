// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/client.cc:
 *   Client to TAPIR transactional storage system.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#include "store/tapirstore/client.h"

#include <bits/std_function.h>

#include <cstdint>
#include <functional>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/tapirstore/shardclient.h"

class Transport;

namespace tapirstore {

using namespace std;

Client::Client(transport::Configuration *config, uint64_t id, int nShards,
               int nGroups, int closestReplica, Transport *transport,
               Partitioner *part, bool pingReplicas, bool syncCommit,
               bool resend_on_timeout, const TrueTime &timeServer)
    : config(config),
      client_id(id),
      nshards(nShards),
      ngroups(nGroups),
      transport(transport),
      part(part),
      pingReplicas(pingReplicas),
      syncCommit(syncCommit),
      timeServer(timeServer),
      lastReqId(0UL),
      first(true),
      startedPings(false) {
  bclient.reserve(nshards);

  if (client_id > 0xFFFFFFFF) {
    Panic("Only support 32-bit client id. %lu is too large.", client_id);
  }

  Debug("Initializing Tapir client with id [%lu] %lu", client_id, nshards);

  /* Start a client for each shard. */
  for (uint64_t i = 0; i < ngroups; i++) {
    auto *shardclient =
        new ShardClient(config, transport, client_id, i, closestReplica,
                        pingReplicas, resend_on_timeout);
    bclient[i] = new BufferClient(shardclient);
    sclient.push_back(shardclient);
  }

  Debug("Tapir client [%lu] created! %lu %lu", client_id, nshards,
        bclient.size());
}

Client::~Client() {
  for (auto b : bclient) {
    delete b;
  }
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void Client::Begin(std::unique_ptr<AppContext> &context,
                   context::abort_callback acb) {
  if (pingReplicas) {
    if (!first && !startedPings) {
      startedPings = true;
      for (auto s : sclient) {
        s->StartPings();
      }
    }
    first = false;
  }

  Debug("BEGIN [%lu]", t_id + 1);
  t_id++;

  auto *txn_context = new TapirContext();
  txn_context->t_id = (t_id << 32) | (client_id & 0xFFFFFFFF);
  txn_context->acb = std::move(acb);
  context->set_txn_context(std::unique_ptr<TxnContext>(txn_context));
}

void Client::Get(std::unique_ptr<AppContext> context, const std::string &key,
                 context::op_callback ocb) {
  auto *txn_context = dynamic_cast<TapirContext *>(context->txn_context());

  Debug("GET [%lu : %s]", t_id, key.c_str());
  // Contact the appropriate shard to get the value.
  auto i = (*part)(key, nshards, -1, txn_context->participants);
  Debug("GET shard %lu group %lu", i, i % ngroups);
  i = i % ngroups;

  // If needed, add this shard to set of participants and send BEGIN.
  if (txn_context->participants.find(i) == txn_context->participants.end()) {
    txn_context->participants.insert(i);
    bclient[i]->Begin(txn_context->t_id);
  }

  // Send the GET operation to appropriate shard.
  bclient[i]->Get(key,
                  std::bind(&Client::GetCallback, this, context.release(), ocb,
                            std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3, std::placeholders::_4),
                  std::bind(&Client::GetTimeoutCallback, this,
                            std::placeholders::_1, std::placeholders::_2),
                  10000);
}

void Client::Put(std::unique_ptr<AppContext> &context, const std::string &key,
                 const std::string &val) {
  auto *txn_context = dynamic_cast<TapirContext *>(context->txn_context());

  Debug("PUT [%lu : %s]", t_id, key.c_str());
  // Contact the appropriate shard to set the value.
  auto i = (*part)(key, nshards, -1, txn_context->participants);
  Debug("PUT shard %lu group %lu", i, i % ngroups);
  i = i % ngroups;

  // If needed, add this shard to set of participants and send BEGIN.
  if (txn_context->participants.find(i) == txn_context->participants.end()) {
    txn_context->participants.insert(i);
    bclient[i]->Begin(txn_context->t_id);
  }

  // Buffering, so no need to wait.
  bclient[i]->Put(
      key, val,
      std::bind(&Client::PutCallback, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3),
      std::bind(&Client::PutTimeoutCallback, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3),
      10000);
}

void Client::Commit(std::unique_ptr<AppContext> context,
                    ::context::commit_callback ccb) {
  auto *txn_context = dynamic_cast<TapirContext *>(context->txn_context());

  pendingReqs[txn_context->t_id] = std::move(context);

  txn_context->commit_metadata =
      std::make_unique<TapirContext::CommitMetadata>();
  txn_context->commit_metadata->ccb = ccb;
  txn_context->commit_metadata->prepare_ts =
      Timestamp(timeServer.GetTime(), client_id);

  GetStats().IncrementList("txn_groups", txn_context->participants.size());
  for (auto participant : txn_context->participants) {
    GetStats().IncrementList("txn_participants", participant);
  }

  Prepare(txn_context);
}

void Client::Abort(std::unique_ptr<AppContext> &context) {
  auto *txn_context = dynamic_cast<TapirContext *>(context->txn_context());

  AbortInternal(txn_context, []() {}, []() {}, 10000);
}

void Client::GetCallback(AppContext *ctx, const ::context::op_callback &ocb,
                         int status, const std::string &key,
                         const std::string &val, const Timestamp &ts) {
  context::transaction_op_status_t op_status;
  switch (status) {
    case REPLY_OK:
      op_status = context::OK;
      break;
    default:
      op_status = context::FAIL;
      break;
  }
  ocb(std::unique_ptr<AppContext>(ctx), op_status, key, val);
}

void Client::GetTimeoutCallback(int status, const std::string &key) {}

void Client::PutCallback(int status, const std::string &key,
                         const std::string &val) {}

void Client::PutTimeoutCallback(int status, const std::string &key,
                                const std::string &val) {}

void Client::Prepare(TapirContext *txn_context) {
  Debug("PREPARE [%lu] at %lu.%lu", txn_context->t_id,
        txn_context->commit_metadata->prepare_ts.getTimestamp(),
        txn_context->commit_metadata->prepare_ts.getID());
  UW_ASSERT(!txn_context->participants.empty());

  txn_context->commit_metadata->outstanding_prepares = 0;
  txn_context->commit_metadata->prepare_status = REPLY_OK;
  txn_context->commit_metadata->max_replied_ts = 0UL;

  for (auto p : txn_context->participants) {
    bclient[p]->Prepare(
        txn_context->commit_metadata->prepare_ts,
        std::bind(&Client::PrepareCallback, this, txn_context->t_id,
                  std::placeholders::_1, std::placeholders::_2),
        std::bind(&Client::PrepareCallback, this, txn_context->t_id,
                  std::placeholders::_1, std::placeholders::_2),
        10000UL);
    txn_context->commit_metadata->outstanding_prepares++;
  }
}

void Client::PrepareCallback(uint64_t t_id, int status, const Timestamp &ts) {
  auto req_itr = pendingReqs.find(t_id);
  if (req_itr == pendingReqs.end()) {
    Debug(
        "PrepareCallback for terminated request id %lu (txn already committed "
        "or aborted).",
        t_id);
    return;
  }
  auto &req = req_itr->second;

  auto *txn_context = dynamic_cast<TapirContext *>(req->txn_context());

  Debug("PREPARE [%lu] callback status %d and ts %lu.%lu", t_id, status,
        ts.getTimestamp(), ts.getID());

  uint64_t proposed = ts.getTimestamp();

  --txn_context->commit_metadata->outstanding_prepares;
  switch (status) {
    case REPLY_OK:
      Debug("PREPARE [%lu] OK", t_id);
      break;
    case REPLY_FAIL:
    case REPLY_ABSTAIN:
      // abort!
      Debug("PREPARE [%lu] ABORT", t_id);
      txn_context->commit_metadata->prepare_status = REPLY_FAIL;
      txn_context->commit_metadata->outstanding_prepares = 0;
      break;
    case REPLY_RETRY:
      txn_context->commit_metadata->prepare_status = REPLY_RETRY;
      if (proposed > txn_context->commit_metadata->max_replied_ts) {
        Debug("PREPARE [%lu] update max reply ts from %lu to %lu.", t_id,
              txn_context->commit_metadata->max_replied_ts, proposed);
        txn_context->commit_metadata->max_replied_ts = proposed;
      }
      break;
    case REPLY_TIMEOUT:
      txn_context->commit_metadata->prepare_status = REPLY_RETRY;
      break;
    default:
      Panic("PREPARE [%lu] unexpected status %d.", t_id, status);
      break;
  }

  if (txn_context->commit_metadata->outstanding_prepares == 0) {
    HandleAllPreparesReceived(txn_context);
  }
}

void Client::HandleAllPreparesReceived(TapirContext *txn_context) {
  Debug("All PREPARE's [%lu] received", txn_context->t_id);

  transaction_status_t abortResult = COMMITTED;
  switch (txn_context->commit_metadata->prepare_status) {
    case REPLY_OK: {
      Debug("COMMIT [%lu]", txn_context->t_id);
      commit_callback ccb = [this, t_id = txn_context->t_id](bool committed) {
        auto req_itr = pendingReqs.find(t_id);
        if (req_itr == pendingReqs.end()) {
          return;
        }

        auto &req = req_itr->second;
        auto *txn_context = dynamic_cast<TapirContext *>(req->txn_context());

        auto ctx = std::move(req);
        pendingReqs.erase(req_itr);
        txn_context->commit_metadata->ccb(std::move(ctx), context::OK,
                                          COMMITTED);
      };
      commit_timeout_callback ctcb = [t_id = txn_context->t_id]() {
        Warning("COMMIT[%lu] timeout.", t_id);
      };
      for (auto p : txn_context->participants) {
        bclient[p]->Commit(txn_context->commit_metadata->prepare_ts, ccb, ctcb,
                           10000);
      }
      if (!syncCommit) {
        auto req_itr = pendingReqs.find(txn_context->t_id);
        UW_ASSERT(req_itr != pendingReqs.end());
        auto ctx = std::move(req_itr->second);
        pendingReqs.erase(req_itr);
        txn_context->commit_metadata->ccb(std::move(ctx), context::OK,
                                          COMMITTED);
      }
      break;
    }
    case REPLY_RETRY: {
      ++txn_context->commit_metadata->commit_tries;
      if (txn_context->commit_metadata->commit_tries < COMMIT_RETRIES) {
        statInts["retries"] += 1;
        uint64_t now = timeServer.GetTime();
        if (now > txn_context->commit_metadata->max_replied_ts) {
          txn_context->commit_metadata->prepare_ts.setTimestamp(now);
        } else {
          // !!! important that the retry timestamp is larger than the max reply
          //   otherwise we will continue retrying with a smaller timestamp for
          //   some small number of rounds when clocks are not tightly
          //   synchronized
          txn_context->commit_metadata->prepare_ts.setTimestamp(
              txn_context->commit_metadata->max_replied_ts + 1);
        }
        Debug("RETRY [%lu] at [%lu.%lu]", txn_context->t_id,
              txn_context->commit_metadata->prepare_ts.getTimestamp(),
              txn_context->commit_metadata->prepare_ts.getID());

        Prepare(txn_context);
        break;
      }
      statInts["aborts_max_retries"] += 1;
      abortResult = ABORTED_MAX_RETRIES;
      break;
    }
    case REPLY_FAIL: {
      abortResult = ABORTED_SYSTEM;
      break;
    }
    default: { break; }
  }

  if (abortResult != COMMITTED) {
    // application doesn't need to be notified when abort has been acknowledged,
    // so we use empty callback functions and directly call the commit callback
    // function (with commit=false indicating an abort)
    AbortInternal(txn_context, []() {}, []() {}, 10000);

    auto req_itr = pendingReqs.find(txn_context->t_id);
    UW_ASSERT(req_itr != pendingReqs.end());
    auto ctx = std::move(req_itr->second);
    pendingReqs.erase(req_itr);
    txn_context->commit_metadata->ccb(std::move(ctx), context::OK, abortResult);
  }
}

void Client::AbortInternal(TapirContext *txn_context, const abort_callback &acb,
                           const abort_timeout_callback &atcb,
                           uint32_t timeout) {
  Debug("ABORT [%lu]", txn_context->t_id);
  for (auto p : txn_context->participants) {
    bclient[p]->Abort(acb, atcb, timeout);
  }
}

Client::TapirContext::TapirContext() = default;

Client::TapirContext::TapirContext(const TapirContext &other) {}

Client::TapirContext::~TapirContext() = default;

std::unique_ptr<TxnContext> Client::TapirContext::Clone() const {
  return std::make_unique<Client::TapirContext>(*this);
}

}  // namespace tapirstore
