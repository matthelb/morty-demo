// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/client.cc:
 *   Client to transactional storage system with strong consistency
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

#include "store/strongstore/client.h"

#include <bits/std_function.h>

#include <cstdint>
#include <functional>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/common/common.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/unreplicatedshardclient.h"

namespace strongstore {

Client::Client(Mode mode, transport::Configuration *config, uint64_t id,
               int nshards, int ngroups, int closestReplica,
               Transport *transport, Partitioner *part, bool unreplicated,
               const TrueTime &timeServer)
    : config(config),
      client_id(id),
      nshards(nshards),
      ngroups(ngroups),
      transport(transport),
      mode(mode),
      part(part),
      timeServer(timeServer),
      remoteTimeServer(false) {
  last_t_id = client_id << 26;

  Debug("Initializing StrongStore client with id [%lu]", client_id);

  /* Start a client for each shard. */
  for (int i = 0; i < nshards; i++) {
    TxnClient *shardclient;
    if (unreplicated || mode == MODE_MVTSO) {
      shardclient =
          new UnreplicatedShardClient(mode, config, transport, client_id, i);
    } else {
      shardclient = new ShardClient(mode, config, transport, client_id, i,
                                    closestReplica);
    }
    bclient.push_back(new BufferClient(shardclient, mode != MODE_MVTSO));
    sclients.push_back(shardclient);
  }

  Debug("SpanStore client [%lu] created!", client_id);
  _Latency_Init(&opLat, "op_lat");
}

Client::~Client() {
  Latency_Dump(&opLat);
  for (auto b : bclient) {
    delete b;
  }
  for (auto s : sclients) {
    delete s;
  }
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void Client::Begin(std::unique_ptr<AppContext> &context,
                   context::abort_callback acb) {
  Debug("BEGIN [%lu]", last_t_id + 1);
  last_t_id++;

  auto *txn_context = new StrongContext();
  txn_context->t_id = last_t_id;
  txn_context->acb = std::move(acb);
  context->set_txn_context(std::unique_ptr<TxnContext>(txn_context));

  if (mode == MODE_MVTSO) {
    txn_context->ts = Timestamp(timeServer.GetTime(), txn_context->t_id);
  } else {
    txn_context->ts = Timestamp(0, txn_context->t_id);
  }
}

/* Returns the value corresponding to the supplied key. */
void Client::Get(std::unique_ptr<AppContext> context, const std::string &key,
                 ::context::op_callback ocb) {
  auto *txn_context = dynamic_cast<StrongContext *>(context->txn_context());

  Latency_Start(&opLat);
  Debug("GET [%lu : %s]", txn_context->t_id, BytesToHex(key, 16).c_str());

  // Contact the appropriate shard to get the value.
  auto i = (*part)(key, nshards, -1, txn_context->participants);

  Debug("GET shard %lu group %lu", i, i % ngroups);
  i = i % ngroups;

  // GetStats().IncrementList("get_groups", i);

  // If needed, add this shard to set of participants and send BEGIN.
  if (txn_context->participants.find(i) == txn_context->participants.end()) {
    txn_context->participants.insert(i);
    bclient[i]->Begin(txn_context->t_id);
  }

  // Send the GET operation to appropriate shard.
  if (mode == MODE_MVTSO) {
    bclient[i]->Get(key, txn_context->ts,
                    std::bind(&Client::GetCallback, this, context.release(),
                              ocb, std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3, std::placeholders::_4),
                    std::bind(&Client::GetTimeoutCallback, this,
                              std::placeholders::_1, std::placeholders::_2),
                    10000);
  } else {
    bclient[i]->Get(key,
                    std::bind(&Client::GetCallback, this, context.release(),
                              ocb, std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3, std::placeholders::_4),
                    std::bind(&Client::GetTimeoutCallback, this,
                              std::placeholders::_1, std::placeholders::_2),
                    10000);
  }
}

/* Sets the value corresponding to the supplied key. */
void Client::Put(std::unique_ptr<AppContext> &context, const std::string &key,
                 const std::string &val) {
  auto *txn_context = dynamic_cast<StrongContext *>(context->txn_context());

  Latency_Start(&opLat);
  Debug("PUT [%lu : %s]", txn_context->t_id, BytesToHex(key, 16).c_str());

  // Contact the appropriate shard to get the value.
  auto i = (*part)(key, nshards, -1, txn_context->participants);

  Debug("PUT shard %lu group %lu", i, i % ngroups);
  i = i % ngroups;

  // GetStats().IncrementList("put_groups", i);

  // If needed, add this shard to set of participants and send BEGIN.
  if (txn_context->participants.find(i) == txn_context->participants.end()) {
    txn_context->participants.insert(i);
    bclient[i]->Begin(txn_context->t_id);
  }

  bclient[i]->Put(
      key, val, txn_context->ts,
      std::bind(&Client::PutCallback, this, context.release(),
                std::placeholders::_1, std::placeholders::_2,
                std::placeholders::_3),
      std::bind(&Client::PutTimeoutCallback, this, std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3),
      10000);
}

/* Attempts to commit the ongoing transaction. */
void Client::Commit(std::unique_ptr<AppContext> context,
                    ::context::commit_callback ccb) {
  auto *txn_context = dynamic_cast<StrongContext *>(context->txn_context());

  pendingReqs[txn_context->t_id] = std::move(context);

  txn_context->commit_metadata =
      std::make_unique<StrongContext::CommitMetadata>();
  txn_context->commit_metadata->ccb = ccb;

  GetStats().IncrementList("txn_groups", txn_context->participants.size());
  std::string grps;
  auto itr = txn_context->participants.begin();
  while (itr != txn_context->participants.end()) {
    grps += std::to_string(*itr);
    itr++;
    if (itr != txn_context->participants.end()) {
      grps += "_";
    }
  }
  GetStats().Increment("txn_groups_" + grps);

  Prepare(txn_context);
}

/* Aborts the ongoing transaction. */
void Client::Abort(std::unique_ptr<AppContext> &context) {
  auto *txn_context = dynamic_cast<StrongContext *>(context->txn_context());

  AbortInternal(txn_context, []() {}, []() {}, 10000);
}

void Client::GetCallback(AppContext *ctx, const ::context::op_callback &ocb,
                         int status, const std::string &key,
                         const std::string &val, const Timestamp &ts) {
  auto *txn_context = dynamic_cast<StrongContext *>(ctx->txn_context());

  Latency_End(&opLat);

  if (mode == MODE_MVTSO && status == REPLY_FAIL) {
    AbortInternal(txn_context, []() {}, []() {},
                  10000);  // we don't really care about the timeout here
    txn_context->acb(std::unique_ptr<AppContext>(ctx));
    return;
  }

  ::context::transaction_op_status_t op_status;
  switch (status) {
    case REPLY_OK:
      op_status = ::context::OK;
      break;
    default:
      op_status = ::context::FAIL;
      break;
  }
  ocb(std::unique_ptr<AppContext>(ctx), op_status, key, val);
}

void Client::GetTimeoutCallback(int status, const std::string &key) {}

void Client::PutCallback(AppContext *ctx, int status, const std::string &key,
                         const std::string &val) {
  auto *txn_context = dynamic_cast<StrongContext *>(ctx->txn_context());

  Latency_End(&opLat);

  if (mode == MODE_MVTSO && status == REPLY_FAIL) {
    AbortInternal(txn_context, []() {}, []() {},
                  10000);  // we don't really care about the timeout here
    txn_context->acb(std::unique_ptr<AppContext>(ctx));
    return;
  }
}

void Client::PutTimeoutCallback(int status, const std::string &key,
                                const std::string &val) {}

void Client::AbortInternal(StrongContext *txn_context,
                           const abort_callback &acb,
                           const abort_timeout_callback &atcb,
                           uint32_t timeout) {
  Debug("ABORT [%lu]", txn_context->t_id);

  for (auto p : txn_context->participants) {
    bclient[p]->Abort(acb, atcb, timeout);
  }
}

void Client::Prepare(StrongContext *txn_context) {
  Debug("PREPARE [%lu]", txn_context->t_id);
  UW_ASSERT(!txn_context->participants.empty());

  txn_context->commit_metadata->outstandingPrepares = 0;
  txn_context->commit_metadata->prepareStatus = REPLY_OK;
  txn_context->commit_metadata->maxRepliedTs = 0UL;

  for (auto p : txn_context->participants) {
    bclient[p]->Prepare(
        txn_context->ts,
        std::bind(&Client::PrepareCallback, this, txn_context->t_id,
                  std::placeholders::_1, std::placeholders::_2),
        std::bind(&Client::PrepareCallback, this, txn_context->t_id,
                  std::placeholders::_1, std::placeholders::_2),
        10000UL);
    txn_context->commit_metadata->outstandingPrepares++;
  }

  if (mode == MODE_OCC) {
    if (remoteTimeServer) {
      tss->Invoke("", bind(&Client::tssCallback, this, txn_context->t_id,
                           std::placeholders::_1, std::placeholders::_2));
    } else {
      txn_context->commit_metadata->maxRepliedTs = timeServer.GetTime();
    }
  }
}

void Client::PrepareCallback(uint64_t t_id, int status,
                             const Timestamp &respTs) {
  auto req_itr = pendingReqs.find(t_id);
  if (req_itr == pendingReqs.end()) {
    Debug("PrepareCallback for stale txn %lu.", t_id);
    return;
  }
  auto &req = req_itr->second;

  auto *txn_context = dynamic_cast<StrongContext *>(req->txn_context());

  Debug("PREPARE [%lu] callback status %d and ts %lu.%lu", t_id, status,
        txn_context->ts.getTimestamp(), txn_context->ts.getID());

  if (status == REPLY_WAITING) {
    Debug("PREPARE [%lu] is waiting for dependencies.", t_id);
    return;
  }

  uint64_t proposed = respTs.getTimestamp();
  if (proposed > txn_context->commit_metadata->maxRepliedTs) {
    Debug("PREPARE [%lu] update max reply ts from %lu to %lu.", t_id,
          txn_context->commit_metadata->maxRepliedTs, proposed);
    txn_context->commit_metadata->maxRepliedTs = proposed;
  }

  --txn_context->commit_metadata->outstandingPrepares;
  switch (status) {
    case REPLY_OK:
      Debug("PREPARE [%lu] OK", t_id);
      break;
    default:
      // abort!
      Debug("PREPARE [%lu] ABORT", t_id);
      txn_context->commit_metadata->prepareStatus = REPLY_FAIL;
      txn_context->commit_metadata->outstandingPrepares = 0;
      break;
  }

  if (txn_context->commit_metadata->outstandingPrepares == 0) {
    HandleAllPreparesReceived(txn_context);
  }
}

void Client::HandleAllPreparesReceived(StrongContext *txn_context) {
  Debug("All PREPARE's [%lu] received", txn_context->t_id);

  transaction_status_t abortResult = COMMITTED;
  switch (txn_context->commit_metadata->prepareStatus) {
    case REPLY_OK: {
      // For Spanner like systems, calculate timestamp.
      if ((mode == MODE_SPAN_OCC || mode == MODE_SPAN_LOCK) &&
          !txn_context->commit_metadata->spannerSleep) {
        uint64_t now, err;

        timeServer.GetTimeAndError(now, err);

        if (now > txn_context->commit_metadata->maxRepliedTs) {
          txn_context->commit_metadata->maxRepliedTs = now;
        } else {
          uint64_t diff =
              ((txn_context->commit_metadata->maxRepliedTs >> 32) -
               (now >> 32)) *
                  1000000 +
              ((txn_context->commit_metadata->maxRepliedTs & 0xffffffff) -
               (txn_context->commit_metadata->maxRepliedTs & 0xffffffff));
          err += diff;
        }

        txn_context->commit_metadata->spannerSleep = true;
        transport->TimerMicro(
            err + 10,
            std::bind(&Client::HandleAllPreparesReceived, this, txn_context));
        return;
      }

      Debug("COMMIT [%lu]", txn_context->t_id);

      // TODO(matthelb): 2PC is only necessary for distributed txns. we could
      // optimize
      //   this and have server immediately commit data during prepare if single
      //   shard txn
      for (auto p : txn_context->participants) {
        bclient[p]->Commit(txn_context->ts, [](transaction_status_t status) {},
                           []() {}, 10000);
      }

      auto req_itr = pendingReqs.find(txn_context->t_id);
      UW_ASSERT(req_itr != pendingReqs.end());
      auto ctx = std::move(req_itr->second);
      pendingReqs.erase(req_itr);
      txn_context->commit_metadata->ccb(std::move(ctx), context::OK, COMMITTED);
      break;
    }
    default: {
      abortResult = ABORTED_SYSTEM;
      break;
    }
  }

  if (abortResult != COMMITTED) {
    // application doesn't need to be notified when abort has been acknowledged,
    // so we use empty callback functions and directly call the commit callback
    // function (with commit=false indicating an abort)
    AbortInternal(txn_context, []() {}, []() {},
                  10000);  // we don't really care about the timeout here

    auto req_itr = pendingReqs.find(txn_context->t_id);
    UW_ASSERT(req_itr != pendingReqs.end());
    auto ctx = std::move(req_itr->second);
    pendingReqs.erase(req_itr);
    txn_context->commit_metadata->ccb(std::move(ctx), context::OK, abortResult);
  }
}

Client::StrongContext::StrongContext() = default;

Client::StrongContext::StrongContext(const StrongContext &other)
    : t_id(other.t_id), ts(other.ts), participants(other.participants) {}

Client::StrongContext::~StrongContext() = default;

std::unique_ptr<TxnContext> Client::StrongContext::Clone() const {
  return std::make_unique<Client::StrongContext>(*this);
}

/* Callback from a tss replica upon any request. */
bool Client::tssCallback(uint64_t t_id, const string &request,
                         const string &reply) {
  auto req_itr = this->pendingReqs.find(t_id);
  if (req_itr == this->pendingReqs.end()) {
    Debug(
        "PrepareCallback for terminated request id %lu (txn already committed"
        " or aborted.",
        t_id);
    return false;
  }
  auto &req = req_itr->second;

  auto *txn_context = dynamic_cast<StrongContext *>(req->txn_context());

  uint64_t ts = stol(reply, nullptr, 10);
  if (ts > txn_context->commit_metadata->maxRepliedTs) {
    txn_context->commit_metadata->maxRepliedTs = ts;
  }
  return true;
}

}  // namespace strongstore
