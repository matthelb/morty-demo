// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * ir/client.cc:
 *   Inconsistent replication client
 *
 * Copyright 2013-2015 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                     Irene Zhang Ports  <iyzhang@cs.washington.edu>
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

#include "replication/common/client.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/common/request.pb.h"
#include "replication/ir/client.h"
#include "replication/ir/ir-proto.pb.h"

namespace replication {
namespace ir {

IRClient::IRClient(const transport::Configuration &config, Transport *transport,
                   int group, uint64_t clientid, bool resend_on_timeout)
    : Client(config, transport, group, clientid),
      lastReqId(0),
      resend_on_timeout_(resend_on_timeout) {}

IRClient::~IRClient() {
  for (auto kv : pendingReqs) {
    delete kv.second;
  }
}

void IRClient::Invoke(const std::string &request, continuation_t continuation,
                      error_continuation_t error_continuation) {
  InvokeInconsistent(request, continuation, error_continuation);
}

void IRClient::InvokeInconsistent(const std::string &request,
                                  continuation_t continuation,
                                  error_continuation_t error_continuation) {
  // TODO(matthelb): Use error_continuation.
  (void)error_continuation;

  // Bump the request ID
  uint64_t reqId = ++lastReqId;
  // Create new timer
  auto timer = std::make_unique<Timeout>(
      transport, 1000, [this, reqId]() { ResendInconsistent(reqId); });
  PendingInconsistentRequest *req =
      new PendingInconsistentRequest(request, reqId, std::move(continuation),
                                     std::move(timer), config.QuorumSize());

  pendingReqs[reqId] = req;
  SendInconsistent(req);
}

void IRClient::SendInconsistent(const PendingInconsistentRequest *req) {
  Debug("[%lu:%lu] Sending inconsistent op.", clientid, req->clientReqId);
  proto::ProposeInconsistentMessage reqMsg;
  reqMsg.mutable_req()->set_op(req->request);
  reqMsg.mutable_req()->set_clientid(clientid);
  reqMsg.mutable_req()->set_clientreqid(req->clientReqId);

  if (transport->SendMessageToGroup(this, group, reqMsg)) {
    req->timer->Reset();
  } else {
    Warning("[%lu:%lu] Could not send inconsistent request to replicas",
            clientid, req->clientReqId);
    pendingReqs.erase(req->clientReqId);
    delete req;
  }
}

void IRClient::InvokeConsensus(const std::string &request, decide_t decide,
                               continuation_t continuation,
                               error_continuation_t error_continuation) {
  uint64_t reqId = ++lastReqId;
  auto timer = std::make_unique<Timeout>(
      transport, 1000, [this, reqId]() { ResendConsensus(reqId); });
  auto transition_to_slow_path_timer = std::make_unique<Timeout>(
      transport, 1000,
      [this, reqId]() { TransitionToConsensusSlowPath(reqId); });

  PendingConsensusRequest *req = new PendingConsensusRequest(
      request, reqId, std::move(continuation), std::move(timer),
      std::move(transition_to_slow_path_timer), config.QuorumSize(),
      config.FastQuorumSize(), std::move(decide),
      std::move(error_continuation));

  pendingReqs[reqId] = req;
  req->transition_to_slow_path_timer->Start();
  SendConsensus(req);
}

void IRClient::SendConsensus(const PendingConsensusRequest *req) {
  Debug("[%lu:%lu] Sending consensus op.", clientid, req->clientReqId);
  proto::ProposeConsensusMessage reqMsg;
  reqMsg.mutable_req()->set_op(req->request);
  reqMsg.mutable_req()->set_clientid(clientid);
  reqMsg.mutable_req()->set_clientreqid(req->clientReqId);

  if (transport->SendMessageToGroup(this, group, reqMsg)) {
    req->timer->Reset();
  } else {
    Warning("Could not send consensus request to replicas");
    pendingReqs.erase(req->clientReqId);
    delete req;
  }
}

void IRClient::InvokeUnlogged(int replicaIdx, const std::string &request,
                              continuation_t continuation,
                              error_continuation_t error_continuation,
                              uint32_t timeout) {
  uint64_t reqId = ++lastReqId;
  auto timer = std::make_unique<Timeout>(transport, timeout, [this, reqId]() {
    UnloggedRequestTimeoutCallback(reqId);
  });

  PendingUnloggedRequest *req = new PendingUnloggedRequest(
      request, reqId, continuation, error_continuation, std::move(timer));

  proto::UnloggedRequestMessage reqMsg;
  reqMsg.mutable_req()->set_op(request);
  reqMsg.mutable_req()->set_clientid(clientid);
  reqMsg.mutable_req()->set_clientreqid(reqId);

  if (transport->SendMessageToReplica(this, group, replicaIdx, reqMsg)) {
    req->timer->Start();
    pendingReqs[reqId] = req;
  } else {
    Warning("[%lu:%lu] Could not send unlogged request to replica", clientid,
            req->clientReqId);
    delete req;
  }
}

void IRClient::InvokeUnloggedAll(const std::string &request,
                                 continuation_t continuation,
                                 error_continuation_t error_continuation,
                                 uint32_t timeout) {
  uint64_t reqId = ++lastReqId;
  auto timer = std::make_unique<Timeout>(transport, timeout, [this, reqId]() {
    UnloggedRequestTimeoutCallback(reqId);
  });

  PendingUnloggedRequest *req = new PendingUnloggedRequest(
      request, reqId, continuation, error_continuation, std::move(timer));

  proto::UnloggedRequestMessage reqMsg;
  reqMsg.mutable_req()->set_op(request);
  reqMsg.mutable_req()->set_clientid(clientid);
  reqMsg.mutable_req()->set_clientreqid(reqId);

  if (transport->SendMessageToGroup(this, group, reqMsg)) {
    req->timer->Reset();
    pendingReqs[reqId] = req;
  } else {
    Warning("[%lu:%lu] Could not send unlogged request to replica", clientid,
            req->clientReqId);
    delete req;
  }
}

void IRClient::ResendInconsistent(const uint64_t reqId) {
  if (!resend_on_timeout_) {
    return;
  }

  Warning("[%lu:%lu] Client timeout; resending inconsistent request", clientid,
          reqId);
  SendInconsistent(
      dynamic_cast<PendingInconsistentRequest *>(pendingReqs[reqId]));
}

void IRClient::ResendConsensus(const uint64_t reqId) {
  if (!resend_on_timeout_) {
    return;
  }

  Warning("[%lu:%lu] Client timeout; resending consensus request", clientid,
          reqId);
  SendConsensus(dynamic_cast<PendingConsensusRequest *>(pendingReqs[reqId]));
}

void IRClient::TransitionToConsensusSlowPath(const uint64_t reqId) {
  Debug("[%lu:%lu] Client timeout; taking consensus slow path", clientid,
        reqId);
  auto *req = dynamic_cast<PendingConsensusRequest *>(pendingReqs[reqId]);
  UW_ASSERT(req != nullptr);
  req->on_slow_path = true;

  // We've already transitioned into the slow path, so don't transition into
  // the slow-path again.
  UW_ASSERT(req->transition_to_slow_path_timer);
  req->transition_to_slow_path_timer.reset();

  // It's possible that we already have a quorum of responses (but not a
  // super quorum).
  const std::map<int, proto::ReplyConsensusMessage> *quorum =
      req->consensusReplyQuorum.CheckForQuorum();
  if (quorum != nullptr) {
    HandleSlowPathConsensus(reqId, *quorum, false, req);
  }
}

void IRClient::HandleSlowPathConsensus(
    const uint64_t reqid,
    const std::map<int, proto::ReplyConsensusMessage> &msgs,
    const bool finalized_result_found, PendingConsensusRequest *req) {
  UW_ASSERT(finalized_result_found || msgs.size() >= req->quorumSize);
  Debug("[%lu:%lu] Handling slow path for request", clientid, reqid);

  // If a finalized result wasn't found, call decide to determine the
  // finalized result.
  if (!finalized_result_found) {
    uint64_t view = 0;
    std::map<std::string, std::size_t> results;
    for (const auto &p : msgs) {
      const proto::ReplyConsensusMessage &msg = p.second;
      results[msg.result()] += 1;

      // All messages should have the same view.
      if (view == 0) {
        view = msg.view();
      }
      UW_ASSERT(msg.view() == view);
    }

    // Upcall into the application, and put the result in the request
    // to store for later retries.
    UW_ASSERT(req->decide != nullptr);
    req->decideResult = req->decide(results);
    req->reply_consensus_view = view;
  }

  // Set up a new timer for the finalize phase.
  req->timer = std::make_unique<Timeout>(transport, 500, [this, reqid]() {  //
    ResendConfirmation(reqid, true);
  });

  // Send finalize message.
  Debug("[%lu:%lu] Sending finalize consensus op", clientid, reqid);
  proto::FinalizeConsensusMessage response;
  response.mutable_opid()->set_clientid(clientid);
  response.mutable_opid()->set_clientreqid(reqid);
  response.set_result(req->decideResult);
  if (transport->SendMessageToGroup(this, group, response)) {
    req->sent_confirms = true;
    req->timer->Start();
  } else {
    Warning("Could not send finalize message to replicas");
    pendingReqs.erase(reqid);
    delete req;
  }
}

void IRClient::HandleFastPathConsensus(
    const uint64_t reqid,
    const std::map<int, proto::ReplyConsensusMessage> &msgs,
    PendingConsensusRequest *req) {
  UW_ASSERT(msgs.size() >= req->superQuorumSize);
  Debug("[%lu:%lu] Handling fast path for request", clientid, reqid);

  // We've received a super quorum of responses. Now, we have to check to see
  // if we have a super quorum of _matching_ responses.
  std::map<std::string, size_t> results;
  for (const auto &m : msgs) {
    const std::string &result = m.second.result();
    results[result]++;
  }

  for (const auto &result : results) {
    if (result.second < req->superQuorumSize) {
      continue;
    }

    // A super quorum of matching requests was found!
    Debug("[%lu:%lu] A super quorum of matching requests was found.", clientid,
          reqid);
    req->decideResult = result.first;

    // Set up a new timeout for the finalize phase.
    req->timer = std::make_unique<Timeout>(
        transport, 500, [this, reqid]() { ResendConfirmation(reqid, true); });

    // Asynchronously send the finalize message.
    Debug("[%lu:%lu] Sending finalize consensus op", clientid, reqid);
    proto::FinalizeConsensusMessage response;
    response.mutable_opid()->set_clientid(clientid);
    response.mutable_opid()->set_clientreqid(reqid);
    response.set_result(result.first);
    if (transport->SendMessageToGroup(this, group, response)) {
      Debug("[%lu:%lu] FinalizeConsensusMessages sent", clientid, reqid);
      req->sent_confirms = true;
      req->timer->Start();
    } else {
      Warning("Could not send finalize message to replicas");
      pendingReqs.erase(reqid);
      delete req;
    }

    // Return to the client.
    if (!req->continuationInvoked) {
      req->continuation(req->request, req->decideResult);
      req->continuationInvoked = true;
    }
    return;
  }

  // There was not a super quorum of matching results, so we transition into
  // the slow path.
  Debug("[%lu:%lu] A super quorum of matching requests was NOT found.",
        clientid, reqid);
  req->on_slow_path = true;
  if (req->transition_to_slow_path_timer) {
    req->transition_to_slow_path_timer.reset();
  }
  HandleSlowPathConsensus(reqid, msgs, false, req);
}

void IRClient::ResendConfirmation(const uint64_t reqId, bool isConsensus) {
  if (!resend_on_timeout_) {
    return;
  }

  if (pendingReqs.find(reqId) == pendingReqs.end()) {
    Debug("[%lu:%lu] Received resend request when no request was pending",
          clientid, reqId);
    return;
  }

  if (isConsensus) {
    auto *req = dynamic_cast<PendingConsensusRequest *>(pendingReqs[reqId]);
    UW_ASSERT(req != nullptr);

    Debug("[%lu:%lu] Sending finalize consensus op", clientid,
          req->clientReqId);
    proto::FinalizeConsensusMessage response;
    response.mutable_opid()->set_clientid(clientid);
    response.mutable_opid()->set_clientreqid(req->clientReqId);
    response.set_result(req->decideResult);

    if (transport->SendMessageToGroup(this, group, response)) {
      req->timer->Reset();
    } else {
      Warning("Could not send finalize message to replicas");
      // give up and clean up
      pendingReqs.erase(reqId);
      delete req;
    }
  } else {
    auto *req = dynamic_cast<PendingInconsistentRequest *>(pendingReqs[reqId]);
    UW_ASSERT(req != nullptr);

    Debug("[%lu:%lu] Sending finalize inconsistent op.", clientid,
          req->clientReqId);
    proto::FinalizeInconsistentMessage response;
    response.mutable_opid()->set_clientid(clientid);
    response.mutable_opid()->set_clientreqid(req->clientReqId);

    if (transport->SendMessageToGroup(this, group, response)) {
      req->timer->Reset();
    } else {
      Warning("Could not send finalize message to replicas");
      pendingReqs.erase(reqId);
      delete req;
    }
  }
}

void IRClient::ReceiveMessage(const TransportAddress &remote, std::string *type,
                              std::string *data, void *meta_data) {
  proto::ReplyInconsistentMessage replyInconsistent;
  proto::ReplyConsensusMessage replyConsensus;
  proto::ConfirmMessage confirm;
  proto::UnloggedReplyMessage unloggedReply;

  if (*type == replyInconsistent.GetTypeName()) {
    replyInconsistent.ParseFromString(*data);
    HandleInconsistentReply(remote, replyInconsistent);
  } else if (*type == replyConsensus.GetTypeName()) {
    replyConsensus.ParseFromString(*data);
    HandleConsensusReply(remote, replyConsensus);
  } else if (*type == confirm.GetTypeName()) {
    confirm.ParseFromString(*data);
    HandleConfirm(remote, confirm);
  } else if (*type == unloggedReply.GetTypeName()) {
    unloggedReply.ParseFromString(*data);
    HandleUnloggedReply(remote, unloggedReply);
  } else {
    Client::ReceiveMessage(remote, type, data, meta_data);
  }
}

void IRClient::HandleInconsistentReply(
    const TransportAddress &remote,
    const proto::ReplyInconsistentMessage &msg) {
  uint64_t reqId = msg.opid().clientreqid();
  auto it = pendingReqs.find(reqId);
  if (it == pendingReqs.end()) {
    Debug("[%lu:%lu] Received inconsistent reply when no request was pending",
          msg.opid().clientid(), reqId);
    return;
  }

  auto *req = dynamic_cast<PendingInconsistentRequest *>(it->second);
  // Make sure the dynamic cast worked
  UW_ASSERT(req != nullptr);

  Debug("[%lu:%lu] Received inconsistent reply (required replies %i)",
        msg.opid().clientid(), reqId,
        req->inconsistentReplyQuorum.NumRequired());

  // Record replies
  viewstamp_t vs = {msg.view(), reqId};
  if (req->inconsistentReplyQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(),
                                                        msg) != nullptr) {
    // TODO(matthelb): Some of the ReplyInconsistentMessages might already be
    // finalized. If this is the case, then we don't have to send finalize
    // messages to them. It's not incorrect to send them anyway (which this
    // code does) but it's less efficient.

    // If all quorum received, then send finalize and return to client
    // Return to client
    if (!req->continuationInvoked) {
      req->timer = std::make_unique<Timeout>(transport, 500, [this, reqId]() {
        ResendConfirmation(reqId, false);
      });

      // asynchronously send the finalize message
      Debug("[%lu:%lu] Sending finalize inconsistent op.",
            msg.opid().clientid(), msg.opid().clientreqid());
      proto::FinalizeInconsistentMessage response;
      *(response.mutable_opid()) = msg.opid();

      if (transport->SendMessageToGroup(this, group, response)) {
        req->timer->Start();
      } else {
        Warning("Could not send finalize message to replicas");
      }

      req->continuation(req->request, "");
      req->continuationInvoked = true;
    }
  }
}

void IRClient::HandleConsensusReply(const TransportAddress &remote,
                                    const proto::ReplyConsensusMessage &msg) {
  uint64_t reqId = msg.opid().clientreqid();
  Debug("[%lu:%lu] Received ReplyConsensusMessage from replica %i in view %lu.",
        clientid, reqId, msg.replicaidx(), msg.view());

  auto it = pendingReqs.find(reqId);
  if (it == pendingReqs.end()) {
    Debug(
        "[%lu:%lu] Not expecting a ReplyConsensusMessage, "
        "so ignoring the request.",
        clientid, reqId);
    return;
  }

  auto *req = dynamic_cast<PendingConsensusRequest *>(it->second);
  UW_ASSERT(req != nullptr);

  if (req->sent_confirms) {
    Debug(
        "[%lu:%lu] Client has already received a quorum or super quorum of "
        "HandleConsensusReply and has already sent out "
        "ConfirmMessages.",
        clientid, reqId);
    return;
  }

  req->consensusReplyQuorum.Add(msg.view(), msg.replicaidx(), msg);
  const std::map<int, proto::ReplyConsensusMessage> &msgs =
      req->consensusReplyQuorum.GetMessages(msg.view());

  if (msg.finalized()) {
    // total_consensus_counter++;
    Debug("[%lu:%lu] The HandleConsensusReply was finalized.", clientid, reqId);
    // If we receive a finalized message, then we immediately transition
    // into the slow path.
    req->on_slow_path = true;
    if (req->transition_to_slow_path_timer) {
      req->transition_to_slow_path_timer.reset();
    }

    req->decideResult = msg.result();
    req->reply_consensus_view = msg.view();
    HandleSlowPathConsensus(reqId, msgs, true, req);
  } else if (req->on_slow_path && msgs.size() >= req->quorumSize) {
    HandleSlowPathConsensus(reqId, msgs, false, req);
  } else if (!req->on_slow_path && msgs.size() >= req->superQuorumSize) {
    // fast_path_counter++;
    HandleFastPathConsensus(reqId, msgs, req);
  }
}

void IRClient::HandleConfirm(const TransportAddress &remote,
                             const proto::ConfirmMessage &msg) {
  uint64_t reqId = msg.opid().clientreqid();
  auto it = pendingReqs.find(reqId);
  if (it == pendingReqs.end()) {
    Debug(
        "[%lu:%lu] We received a ConfirmMessage, but we weren't "
        "waiting for any ConfirmMessages. We are ignoring the message.",
        clientid, reqId);
    return;
  }

  PendingRequest *req = it->second;

  viewstamp_t vs = {msg.view(), reqId};
  if (req->confirmQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg) !=
      nullptr) {
    req->timer->Stop();
    pendingReqs.erase(it);
    if (!req->continuationInvoked) {
      // Return to the client. ConfirmMessages are sent by replicas in
      // response to FinalizeInconsistentMessages and
      // FinalizeConsensusMessage, but inconsistent operations are
      // invoked before FinalizeInconsistentMessages are ever sent. Thus,
      // req->continuationInvoked can only be false if req is a
      // PendingConsensusRequest, so it's safe to cast it here.
      auto *r2 = dynamic_cast<PendingConsensusRequest *>(req);
      UW_ASSERT(r2 != nullptr);
      if (vs.view == r2->reply_consensus_view) {
        r2->continuation(r2->request, r2->decideResult);
      } else {
        Debug(
            "[%lu:%lu] We received a majority of ConfirmMessages "
            "with view %lu, but the view from ReplyConsensusMessages "
            "was %lu.",
            clientid, reqId, vs.view, r2->reply_consensus_view);
        if (r2->error_continuation) {
          r2->error_continuation(r2->request,
                                 ErrorCode::MISMATCHED_CONSENSUS_VIEWS);
        }
      }
    }
    delete req;
  }
}

void IRClient::HandleUnloggedReply(const TransportAddress &remote,
                                   const proto::UnloggedReplyMessage &msg) {
  uint64_t reqId = msg.clientreqid();
  auto it = pendingReqs.find(reqId);
  if (it == pendingReqs.end()) {
    Debug("[%lu:%lu] Received unlogged reply when no request was pending",
          clientid, reqId);
    return;
  }

  PendingRequest *req = it->second;

  if (req->continuation(req->request, msg.reply())) {
    // delete timer event
    req->timer->Stop();
    // remove from pending list
    pendingReqs.erase(it);
    // invoke application callback
    delete req;
  }
}

void IRClient::UnloggedRequestTimeoutCallback(const uint64_t reqId) {
  if (!resend_on_timeout_) {
    return;
  }

  auto it = pendingReqs.find(reqId);
  if (it == pendingReqs.end()) {
    Debug("[%lu:%lu] Received timeout when no request was pending", clientid,
          reqId);
    return;
  }

  auto *req = dynamic_cast<PendingUnloggedRequest *>(it->second);
  UW_ASSERT(req != nullptr);

  Warning("[%lu:%lu] Unlogged request timed out", clientid, reqId);
  // Panic("Unlogged request %lu timed out", reqId);
  // delete timer event
  req->timer->Stop();
  // remove from pending list
  pendingReqs.erase(it);
  // invoke application callback
  if (req->error_continuation) {
    req->error_continuation(req->request, ErrorCode::TIMEOUT);
  }
  delete req;
}

}  // namespace ir
}  // namespace replication
