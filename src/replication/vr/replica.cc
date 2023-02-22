// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/replica.cc:
 *   Viewstamped Replication protocol
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#include "replication/common/replica.h"

#include <google/protobuf/stubs/port.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/common/log-impl.h"
#include "replication/common/replica-inl.h"
#include "replication/vr/replica.h"
#include "replication/vr/vr-proto.pb.h"

#define RDebug(fmt, ...) Debug("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, myIdx, ##__VA_ARGS__)

namespace replication {
namespace vr {

VRReplica::VRReplica(const transport::Configuration &config, int groupIdx,
                     int myIdx, Transport *transport, unsigned int batchSize,
                     AppReplica *app)
    : Replica(config, groupIdx, myIdx, transport, app),
      batchSize(batchSize),
      log(false),
      prepareOKQuorum(config.QuorumSize() - 1),
      startViewChangeQuorum(config.QuorumSize() - 1),
      doViewChangeQuorum(config.QuorumSize() - 1) {
  this->status = STATUS_NORMAL;
  this->view = 0;
  this->lastOp = 0;
  this->lastCommitted = 0;
  this->lastRequestStateTransferView = 0;
  this->lastRequestStateTransferOpnum = 0;
  lastBatchEnd = 0;

  if (batchSize > 1) {
    Notice("Batching enabled; batch size %d", batchSize);
  }

  this->viewChangeTimeout =
      new Timeout(transport, 5000, [this]() { StartViewChange(view + 1); });
  this->nullCommitTimeout =
      new Timeout(transport, 1000, [this]() { SendNullCommit(); });
  this->stateTransferTimeout = new Timeout(transport, 1000, [this]() {
    this->lastRequestStateTransferView = 0;
    this->lastRequestStateTransferOpnum = 0;
  });
  this->stateTransferTimeout->Start();
  this->resendPrepareTimeout =
      new Timeout(transport, 500, [this]() { ResendPrepare(); });
  this->closeBatchTimeout =
      new Timeout(transport, 300, [this]() { CloseBatch(); });

  if (AmLeader()) {
    nullCommitTimeout->Start();
  } else {
    viewChangeTimeout->Start();
  }
}

VRReplica::~VRReplica() {
  delete viewChangeTimeout;
  delete nullCommitTimeout;
  delete stateTransferTimeout;
  delete resendPrepareTimeout;
  delete closeBatchTimeout;

  for (auto &kv : pendingPrepares) {
    delete kv.first;
  }
}

bool VRReplica::AmLeader() const {
  return (configuration.GetLeaderIndex(view) == myIdx);
}

void VRReplica::CommitUpTo(opnum_t upto) {
  while (lastCommitted < upto) {
    lastCommitted++;

    /* Find operation in log */
    const LogEntry *entry = log.Find(lastCommitted);
    if (entry == nullptr) {
      RPanic("Did not find operation " FMT_OPNUM " in log", lastCommitted);
    }

    /* Execute it */
    RDebug("Executing request " FMT_OPNUM, lastCommitted);
    // requests must be idempotent, otherwise leader executes twice w/
    //    LeaderUpcall and ReplicaUpcall

    // Store reply in the client table
    ClientTableEntry &cte = clientTable[entry->request.clientid()];
    Execute(lastCommitted, entry->request, &cte.reply);

    cte.reply.set_view(entry->viewstamp.view);
    cte.reply.set_opnum(entry->viewstamp.opnum);
    cte.reply.set_clientreqid(entry->request.clientreqid());

    /* Mark it as committed */
    log.SetStatus(lastCommitted, LOG_STATE_COMMITTED);

    if (cte.lastReqId <= entry->request.clientreqid()) {
      cte.lastReqId = entry->request.clientreqid();
      cte.replied = true;
    } else {
      // We've subsequently prepared another operation from the
      // same client. So this request must have been completed
      // at the client, and there's no need to record the
      // result.
    }

    /* Send reply */
    auto iter = clientAddresses.find(entry->request.clientid());
    if (iter != clientAddresses.end()) {
      transport->SendMessage(this, *iter->second, cte.reply);
    }
  }
}

void VRReplica::SendPrepareOKs(opnum_t oldLastOp) {
  /* Send PREPAREOKs for new uncommitted operations */
  for (opnum_t i = oldLastOp; i <= lastOp; i++) {
    /* It has to be new *and* uncommitted */
    if (i <= lastCommitted) {
      continue;
    }

    const LogEntry *entry = log.Find(i);
    if (entry == nullptr) {
      RPanic("Did not find operation " FMT_OPNUM " in log", i);
    }
    UW_ASSERT(entry->state == LOG_STATE_PREPARED);
    UpdateClientTable(entry->request);

    proto::PrepareOKMessage reply;
    reply.set_view(view);
    reply.set_opnum(i);
    reply.set_replicaidx(myIdx);

    RDebug("Sending PREPAREOK " FMT_VIEWSTAMP " for new uncommitted operation",
           reply.view(), reply.opnum());

    if (!(transport->SendMessageToReplica(
            this, configuration.GetLeaderIndex(view), reply))) {
      RWarning("Failed to send proto::PrepareOK message to leader");
    }
  }
}

void VRReplica::RequestStateTransfer() {
  proto::RequestStateTransferMessage m;
  m.set_view(view);
  m.set_opnum(lastCommitted);

  if ((lastRequestStateTransferOpnum != 0) &&
      (lastRequestStateTransferView == view) &&
      (lastRequestStateTransferOpnum == lastCommitted)) {
    RDebug("Skipping state transfer request " FMT_VIEWSTAMP
           " because we already requested it",
           view, lastCommitted);
    return;
  }

  RNotice("Requesting state transfer: " FMT_VIEWSTAMP, view, lastCommitted);

  this->lastRequestStateTransferView = view;
  this->lastRequestStateTransferOpnum = lastCommitted;

  if (!transport->SendMessageToGroup(this, groupIdx, m)) {
    RWarning(
        "Failed to send proto::RequestStateTransfer message to all replicas");
  }
}

void VRReplica::EnterView(view_t newview) {
  RNotice("Entering new view " FMT_VIEW, newview);

  view = newview;
  status = STATUS_NORMAL;
  lastBatchEnd = lastOp;

  if (AmLeader()) {
    viewChangeTimeout->Stop();
    nullCommitTimeout->Start();
  } else {
    viewChangeTimeout->Start();
    nullCommitTimeout->Stop();
    resendPrepareTimeout->Stop();
    closeBatchTimeout->Stop();
  }

  prepareOKQuorum.Clear();
  startViewChangeQuorum.Clear();
  doViewChangeQuorum.Clear();
}

void VRReplica::StartViewChange(view_t newview) {
  // RNotice("Starting view change for view " FMT_VIEW, newview);

  view = newview;
  status = STATUS_VIEW_CHANGE;

  viewChangeTimeout->Reset();
  nullCommitTimeout->Stop();
  resendPrepareTimeout->Stop();
  closeBatchTimeout->Stop();

  proto::StartViewChangeMessage m;
  m.set_view(newview);
  m.set_replicaidx(myIdx);
  m.set_lastcommitted(lastCommitted);

  if (!transport->SendMessageToGroup(this, groupIdx, m)) {
    RWarning("Failed to send proto::StartViewChange message to all replicas");
  }
}

void VRReplica::SendNullCommit() {
  proto::CommitMessage cm;
  cm.set_view(this->view);
  cm.set_opnum(this->lastCommitted);

  UW_ASSERT(AmLeader());

  if (!(transport->SendMessageToGroup(this, groupIdx, cm))) {
    RWarning("Failed to send null COMMIT message to all replicas");
  }

  nullCommitTimeout->Reset();
}

void VRReplica::UpdateClientTable(const Request &req) {
  ClientTableEntry &entry = clientTable[req.clientid()];

  UW_ASSERT(entry.lastReqId <= req.clientreqid());

  if (entry.lastReqId == req.clientreqid()) {
    return;
  }

  entry.lastReqId = req.clientreqid();
  entry.replied = false;
  entry.reply.Clear();
}

void VRReplica::ResendPrepare() {
  UW_ASSERT(AmLeader());
  if (lastOp == lastCommitted) {
    return;
  }
  RNotice("Resending prepare");
  if (!(transport->SendMessageToGroup(this, groupIdx, lastPrepare))) {
    RWarning("Failed to ressend prepare message to all replicas");
  }
}

void VRReplica::CloseBatch() {
  UW_ASSERT(AmLeader());
  UW_ASSERT(lastBatchEnd < lastOp);

  opnum_t batchStart = lastBatchEnd + 1;

  RDebug("Sending batched prepare from " FMT_OPNUM " to " FMT_OPNUM, batchStart,
         lastOp);
  /* Send prepare messages */
  lastPrepare.Clear();
  lastPrepare.set_view(view);
  lastPrepare.set_opnum(lastOp);
  lastPrepare.set_batchstart(batchStart);

  for (opnum_t i = batchStart; i <= lastOp; i++) {
    Request *r = lastPrepare.add_request();
    const LogEntry *entry = log.Find(i);
    UW_ASSERT(entry != nullptr);
    UW_ASSERT(entry->viewstamp.view == view);
    UW_ASSERT(entry->viewstamp.opnum == i);
    *r = entry->request;
  }

  if (!(transport->SendMessageToGroup(this, groupIdx, lastPrepare))) {
    RWarning("Failed to send prepare message to all replicas");
  }
  lastBatchEnd = lastOp;

  resendPrepareTimeout->Reset();
  closeBatchTimeout->Stop();

  if (configuration.f == 0) {
    CommitUpTo(lastOp);
  }
}

void VRReplica::ReceiveMessage(const TransportAddress &remote,
                               std::string *type, string *data,
                               void *meta_data) {
  if (*type == requestMessage.GetTypeName()) {
    requestMessage.Clear();
    requestMessage.ParseFromString(*data);
    HandleRequest(remote, requestMessage);
  } else if (*type == unloggedRequest.GetTypeName()) {
    unloggedRequest.Clear();
    unloggedRequest.ParseFromString(*data);
    HandleUnloggedRequest(remote, unloggedRequest);
  } else if (*type == prepare.GetTypeName()) {
    prepare.Clear();
    prepare.ParseFromString(*data);
    HandlePrepare(remote, prepare);
  } else if (*type == prepareOK.GetTypeName()) {
    prepareOK.Clear();
    prepareOK.ParseFromString(*data);
    HandlePrepareOK(remote, prepareOK);
  } else if (*type == commit.GetTypeName()) {
    commit.Clear();
    commit.ParseFromString(*data);
    HandleCommit(remote, commit);
  } else if (*type == requestStateTransfer.GetTypeName()) {
    requestStateTransfer.Clear();
    requestStateTransfer.ParseFromString(*data);
    HandleRequestStateTransfer(remote, requestStateTransfer);
  } else if (*type == stateTransfer.GetTypeName()) {
    stateTransfer.Clear();
    stateTransfer.ParseFromString(*data);
    HandleStateTransfer(remote, stateTransfer);
  } else if (*type == startViewChange.GetTypeName()) {
    startViewChange.Clear();
    startViewChange.ParseFromString(*data);
    HandleStartViewChange(remote, startViewChange);
  } else if (*type == doViewChange.GetTypeName()) {
    doViewChange.Clear();
    doViewChange.ParseFromString(*data);
    HandleDoViewChange(remote, doViewChange);
  } else if (*type == startView.GetTypeName()) {
    startView.Clear();
    startView.ParseFromString(*data);
    HandleStartView(remote, startView);
  } else {
    RPanic("Received unexpected message type in VR proto: %s", type->c_str());
  }
}

void VRReplica::HandleRequest(const TransportAddress &remote,
                              const proto::RequestMessage &msg) {
  viewstamp_t v;

  if (status != STATUS_NORMAL) {
    RNotice("Ignoring request due to abnormal status");
    return;
  }

  if (!AmLeader()) {
    RDebug("Ignoring request because I'm not the leader");
    return;
  }

  // Save the client's address
  // TODO(matthelb): for now we assume clientid-clientAddress mapping is fixed.
  auto addrItr = clientAddresses.find(msg.req().clientid());
  // bool insert = false;
  if (addrItr == clientAddresses.end()) {
    clientAddresses.insert(
        std::make_pair(msg.req().clientid(),
                       std::unique_ptr<TransportAddress>(remote.clone())));
    // insert = true;
  }  // else if (!(*addrItr->second == remote)) {
     // clientAddresses.erase(addrItr);
     // insert = true;
  // }
  // if (insert) {
  // clientAddresses.insert(std::make_pair(msg.req().clientid(),
  //       std::unique_ptr<TransportAddress>(remote.clone())));
  // }

  // Check the client table to see if this is a duplicate request
  auto kv = clientTable.find(msg.req().clientid());
  if (kv != clientTable.end()) {
    const ClientTableEntry &entry = kv->second;
    if (msg.req().clientreqid() < entry.lastReqId) {
      RNotice("Ignoring stale request");
      return;
    }
    if (msg.req().clientreqid() == entry.lastReqId) {
      // This is a duplicate request. Resend the reply if we
      // have one. We might not have a reply to resend if we're
      // waiting for the other replicas; in that case, just
      // discard the request.
      if (entry.replied) {
        RNotice("Received duplicate request; resending reply");
        if (!(transport->SendMessage(this, remote, entry.reply))) {
          RWarning("Failed to resend reply to client");
        }
        return;
      }
      RNotice("Received duplicate request but no reply available; ignoring");
      return;
    }
  }

  // Update the client table
  UpdateClientTable(msg.req());

  // Leader Upcall
  bool replicate = false;
  std::string res;
  LeaderUpcall(lastCommitted, msg.req().op(), &replicate, &res);
  ClientTableEntry &cte = clientTable[msg.req().clientid()];

  // Check whether this request should be committed to replicas
  if (!replicate) {
    RDebug("Executing request failed. Not committing to replicas");

    cte.reply.set_reply(res);
    cte.reply.set_view(0);
    cte.reply.set_opnum(0);
    cte.reply.set_clientreqid(msg.req().clientreqid());
    cte.replied = true;
    transport->SendMessage(this, remote, cte.reply);
  } else {
    request.Clear();
    request.set_op(res);
    request.set_clientid(msg.req().clientid());
    request.set_clientreqid(msg.req().clientreqid());

    /* Assign it an opnum */
    ++this->lastOp;
    v.view = this->view;
    v.opnum = this->lastOp;

    RDebug("Received REQUEST, assigning " FMT_VIEWSTAMP, VA_VIEWSTAMP(v));

    /* Add the request to my log */
    log.Append(v, request, LOG_STATE_PREPARED);

    if (lastOp - lastBatchEnd + 1 > batchSize) {
      CloseBatch();
    } else {
      RDebug("Keeping in batch");
      if (!closeBatchTimeout->Active()) {
        closeBatchTimeout->Start();
      }
    }

    nullCommitTimeout->Reset();
  }
}

void VRReplica::HandleUnloggedRequest(
    const TransportAddress &remote, const proto::UnloggedRequestMessage &msg) {
  if (status != STATUS_NORMAL) {
    // Not clear if we should ignore this or just let the request
    // go ahead, but this seems reasonable.
    RNotice("Ignoring unlogged request due to abnormal status");
    return;
  }

  proto::UnloggedReplyMessage reply;

  Debug("Received unlogged request %s", msg.req().op().c_str());

  ExecuteUnlogged(msg.req(), &reply);
  reply.set_clientreqid(msg.req().clientreqid());

  if (!(transport->SendMessage(this, remote, reply))) {
    Warning("Failed to send reply message");
  }
}

void VRReplica::HandlePrepare(const TransportAddress &remote,
                              const proto::PrepareMessage &msg) {
  RDebug("Received PREPARE <" FMT_VIEW "," FMT_OPNUM "-" FMT_OPNUM ">",
         msg.view(), msg.batchstart(), msg.opnum());

  if (this->status != STATUS_NORMAL) {
    RDebug("Ignoring PREPARE due to abnormal status");
    return;
  }

  if (msg.view() < this->view) {
    RDebug("Ignoring PREPARE due to stale view");
    return;
  }

  if (msg.view() > this->view) {
    RequestStateTransfer();
    pendingPrepares.emplace_back(remote.clone(), msg);
    return;
  }

  if (AmLeader()) {
    RPanic("Unexpected PREPARE: I'm the leader of this view");
  }

  UW_ASSERT(msg.batchstart() <= msg.opnum());
  UW_ASSERT_EQ(msg.opnum() - msg.batchstart() + 1,
               (unsigned int)msg.request_size());

  viewChangeTimeout->Reset();

  if (msg.opnum() <= this->lastOp) {
    RDebug("Ignoring PREPARE; already prepared that operation");
    // Resend the prepareOK message
    proto::PrepareOKMessage reply;
    reply.set_view(msg.view());
    reply.set_opnum(msg.opnum());
    reply.set_replicaidx(myIdx);
    if (!(transport->SendMessageToReplica(
            this, configuration.GetLeaderIndex(view), reply))) {
      RWarning("Failed to send proto::PrepareOK message to leader");
    }
    return;
  }

  if (msg.batchstart() > this->lastOp + 1) {
    RequestStateTransfer();
    pendingPrepares.emplace_back(remote.clone(), msg);
    return;
  }

  /* Add operations to the log */
  opnum_t op = msg.batchstart() - 1;
  for (auto &req : msg.request()) {
    op++;
    if (op <= lastOp) {
      continue;
    }
    this->lastOp++;
    log.Append(viewstamp_t(msg.view(), op), req, LOG_STATE_PREPARED);
    UpdateClientTable(req);
  }
  UW_ASSERT(op == msg.opnum());

  /* Build reply and send it to the leader */
  proto::PrepareOKMessage reply;
  reply.set_view(msg.view());
  reply.set_opnum(msg.opnum());
  reply.set_replicaidx(myIdx);

  if (!(transport->SendMessageToReplica(
          this, configuration.GetLeaderIndex(view), reply))) {
    RWarning("Failed to send proto::PrepareOK message to leader");
  }
}

void VRReplica::HandlePrepareOK(const TransportAddress &remote,
                                const proto::PrepareOKMessage &msg) {
  RDebug("Received PREPAREOK <" FMT_VIEW ", " FMT_OPNUM "> from replica %d",
         msg.view(), msg.opnum(), msg.replicaidx());

  if (this->status != STATUS_NORMAL) {
    RDebug("Ignoring PREPAREOK due to abnormal status");
    return;
  }

  if (msg.view() < this->view) {
    RDebug("Ignoring PREPAREOK due to stale view");
    return;
  }

  if (msg.view() > this->view) {
    RequestStateTransfer();
    return;
  }

  if (!AmLeader()) {
    RWarning("Ignoring PREPAREOK because I'm not the leader");
    return;
  }

  viewstamp_t vs = {msg.view(), msg.opnum()};
  if (auto msgs =
          (prepareOKQuorum.AddAndCheckForQuorum(vs, msg.replicaidx(), msg))) {
    /*
     * We have a quorum of PrepareOK messages for this
     * opnumber. Execute it and all previous operations.
     *
     * (Note that we might have already executed it. That's fine,
     * we just won't do anything.)
     *
     * This also notifies the client of the result.
     */
    CommitUpTo(msg.opnum());

    if (msgs->size() >= static_cast<unsigned int>(configuration.QuorumSize())) {
      return;
    }

    /*
     * Send COMMIT message to the other replicas.
     *
     * This can be done asynchronously, so it really ought to be
     * piggybacked on the next PREPARE or something.
     */
    proto::CommitMessage cm;
    cm.set_view(this->view);
    cm.set_opnum(this->lastCommitted);

    if (!(transport->SendMessageToGroup(this, groupIdx, cm))) {
      RWarning("Failed to send COMMIT message to all replicas");
    }

    nullCommitTimeout->Reset();
  }
}

void VRReplica::HandleCommit(const TransportAddress &remote,
                             const proto::CommitMessage &msg) {
  RDebug("Received COMMIT " FMT_VIEWSTAMP, msg.view(), msg.opnum());

  if (this->status != STATUS_NORMAL) {
    RDebug("Ignoring COMMIT due to abnormal status");
    return;
  }

  if (msg.view() < this->view) {
    RDebug("Ignoring COMMIT due to stale view");
    return;
  }

  if (msg.view() > this->view) {
    RequestStateTransfer();
    return;
  }

  if (AmLeader()) {
    RPanic("Unexpected COMMIT: I'm the leader of this view");
  }

  viewChangeTimeout->Reset();

  if (msg.opnum() <= this->lastCommitted) {
    RDebug("Ignoring COMMIT; already committed that operation");
    return;
  }

  if (msg.opnum() > this->lastOp) {
    RequestStateTransfer();
    return;
  }

  CommitUpTo(msg.opnum());
}

void VRReplica::HandleRequestStateTransfer(
    const TransportAddress &remote,
    const proto::RequestStateTransferMessage &msg) {
  RDebug("Received REQUESTSTATETRANSFER " FMT_VIEWSTAMP, msg.view(),
         msg.opnum());

  if (status != STATUS_NORMAL) {
    RDebug("Ignoring REQUESTSTATETRANSFER due to abnormal status");
    return;
  }

  if (msg.view() > view) {
    RequestStateTransfer();
    return;
  }

  RNotice("Sending state transfer from " FMT_VIEWSTAMP " to " FMT_VIEWSTAMP,
          msg.view(), msg.opnum(), view, lastCommitted);

  proto::StateTransferMessage reply;
  reply.set_view(view);
  reply.set_opnum(lastCommitted);

  log.Dump(msg.opnum() + 1, reply.mutable_entries());

  transport->SendMessage(this, remote, reply);
}

void VRReplica::HandleStateTransfer(const TransportAddress &remote,
                                    const proto::StateTransferMessage &msg) {
  RDebug("Received STATETRANSFER " FMT_VIEWSTAMP, msg.view(), msg.opnum());

  if (msg.view() < view) {
    RWarning("Ignoring state transfer for older view");
    return;
  }

  opnum_t oldLastOp = lastOp;

  /* Install the new log entries */
  for (const auto &newEntry : msg.entries()) {
    if (newEntry.opnum() <= lastCommitted) {
      // Already committed this operation; nothing to be done.
#if PARANOID
      const LogEntry *entry = log.Find(newEntry.opnum());
      UW_ASSERT(entry->viewstamp.opnum == newEntry.opnum());
      UW_ASSERT(entry->viewstamp.view == newEntry.view());
//          UW_ASSERT(entry->request == newEntry.request());
#endif
    } else if (newEntry.opnum() <= lastOp) {
      // We already have an entry with this opnum, but maybe
      // it's from an older view?
      const LogEntry *entry = log.Find(newEntry.opnum());
      UW_ASSERT(entry->viewstamp.opnum == newEntry.opnum());
      UW_ASSERT(entry->viewstamp.view <= newEntry.view());

      if (entry->viewstamp.view == newEntry.view()) {
        // We already have this operation in our log.
        UW_ASSERT(entry->state == LOG_STATE_PREPARED);
      } else {
        // Our operation was from an older view, so obviously
        // it didn't survive a view change. Throw out any
        // later log entries and replace with this one.
        UW_ASSERT(entry->state != LOG_STATE_COMMITTED);
        log.RemoveAfter(newEntry.opnum());
        lastOp = newEntry.opnum();
        oldLastOp = lastOp;

        viewstamp_t vs = {newEntry.view(), newEntry.opnum()};
        log.Append(vs, newEntry.request(), LOG_STATE_PREPARED);
      }
    } else {
      // This is a new operation to us. Add it to the log.
      UW_ASSERT(newEntry.opnum() == lastOp + 1);

      lastOp++;
      viewstamp_t vs = {newEntry.view(), newEntry.opnum()};
      log.Append(vs, newEntry.request(), LOG_STATE_PREPARED);
    }
  }

  if (msg.view() > view) {
    EnterView(msg.view());
  }

  /* Execute committed operations */
  UW_ASSERT(msg.opnum() <= lastOp);
  CommitUpTo(msg.opnum());
  SendPrepareOKs(oldLastOp);

  // Process pending prepares
  std::list<std::pair<TransportAddress *, proto::PrepareMessage> > pending =
      pendingPrepares;
  pendingPrepares.clear();
  for (auto &msgpair : pendingPrepares) {
    RDebug("Processing pending prepare message");
    HandlePrepare(*msgpair.first, msgpair.second);
    delete msgpair.first;
  }
}

void VRReplica::HandleStartViewChange(
    const TransportAddress &remote, const proto::StartViewChangeMessage &msg) {
  RDebug("Received STARTVIEWCHANGE " FMT_VIEW " from replica %d", msg.view(),
         msg.replicaidx());

  if (msg.view() < view) {
    RDebug("Ignoring STARTVIEWCHANGE for older view");
    return;
  }

  if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
    RDebug("Ignoring STARTVIEWCHANGE for current view");
    return;
  }

  if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
    StartViewChange(msg.view());
  }

  UW_ASSERT(msg.view() == view);

  if (auto msgs = startViewChangeQuorum.AddAndCheckForQuorum(
          msg.view(), msg.replicaidx(), msg)) {
    int leader = configuration.GetLeaderIndex(view);
    // Don't try to send a DoViewChange message to ourselves
    if (leader != myIdx) {
      proto::DoViewChangeMessage dvc;
      dvc.set_view(view);
      dvc.set_lastnormalview(log.LastViewstamp().view);
      dvc.set_lastop(lastOp);
      dvc.set_lastcommitted(lastCommitted);
      dvc.set_replicaidx(myIdx);

      // Figure out how much of the log to include
      opnum_t minCommitted =
          std::min_element(
              msgs->begin(), msgs->end(),
              [](decltype(*msgs->begin()) a, decltype(*msgs->begin()) b) {
                return a.second.lastcommitted() < b.second.lastcommitted();
              })
              ->second.lastcommitted();
      minCommitted = std::min(minCommitted, lastCommitted);

      log.Dump(minCommitted, dvc.mutable_entries());

      if (!(transport->SendMessageToReplica(this, leader, dvc))) {
        RWarning("Failed to send DoViewChange message to leader of new view");
      }
    }
  }
}

void VRReplica::HandleDoViewChange(const TransportAddress &remote,
                                   const proto::DoViewChangeMessage &msg) {
  RDebug("Received DOVIEWCHANGE " FMT_VIEW
         " from replica %d, "
         "lastnormalview=" FMT_VIEW " op=" FMT_OPNUM " committed=" FMT_OPNUM,
         msg.view(), msg.replicaidx(), msg.lastnormalview(), msg.lastop(),
         msg.lastcommitted());

  if (msg.view() < view) {
    RDebug("Ignoring DOVIEWCHANGE for older view");
    return;
  }

  if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
    RDebug("Ignoring DOVIEWCHANGE for current view");
    return;
  }

  if ((status != STATUS_VIEW_CHANGE) || (msg.view() > view)) {
    // It's superfluous to send the StartViewChange messages here,
    // but harmless...
    StartViewChange(msg.view());
  }

  UW_ASSERT(configuration.GetLeaderIndex(msg.view()) == myIdx);

  auto msgs = doViewChangeQuorum.AddAndCheckForQuorum(msg.view(),
                                                      msg.replicaidx(), msg);
  if (msgs != nullptr) {
    // Find the response with the most up to date log, i.e. the
    // one with the latest viewstamp
    view_t latestView = log.LastViewstamp().view;
    opnum_t latestOp = log.LastViewstamp().opnum;
    proto::DoViewChangeMessage *latestMsg = nullptr;

    for (auto kv : *msgs) {
      proto::DoViewChangeMessage &x = kv.second;
      if ((x.lastnormalview() > latestView) ||
          (((x.lastnormalview() == latestView) && (x.lastop() > latestOp)))) {
        latestView = x.lastnormalview();
        latestOp = x.lastop();
        latestMsg = &x;
      }
    }

    // Install the new log. We might not need to do this, if our
    // log was the most current one.
    if (latestMsg != nullptr) {
      RDebug("Selected log from replica %d with lastop=" FMT_OPNUM,
             latestMsg->replicaidx(), latestMsg->lastop());
      if (latestMsg->entries_size() == 0) {
        // There weren't actually any entries in the
        // log. That should only happen in the corner case
        // that everyone already had the entire log, maybe
        // because it actually is empty.
        UW_ASSERT(lastCommitted == msg.lastcommitted());
        UW_ASSERT(msg.lastop() == msg.lastcommitted());
      } else {
        if (latestMsg->entries(0).opnum() > lastCommitted + 1) {
          RPanic(
              "Received log that didn't include enough entries to install it");
        }

        log.RemoveAfter(latestMsg->lastop() + 1);
        log.Install(latestMsg->entries().begin(), latestMsg->entries().end());
      }
    } else {
      RDebug("My log is most current, lastnormalview=" FMT_VIEW
             " lastop=" FMT_OPNUM,
             log.LastViewstamp().view, lastOp);
    }

    // How much of the log should we include when we send the
    // STARTVIEW message? Start from the lowest committed opnum of
    // any of the STARTVIEWCHANGE or DOVIEWCHANGE messages we got.
    //
    // We need to compute this before we enter the new view
    // because the saved messages will go away.
    auto svcs = startViewChangeQuorum.GetMessages(view);
    opnum_t minCommittedSVC =
        std::min_element(
            svcs.begin(), svcs.end(),
            [](decltype(*svcs.begin()) a, decltype(*svcs.begin()) b) {
              return a.second.lastcommitted() < b.second.lastcommitted();
            })
            ->second.lastcommitted();
    opnum_t minCommittedDVC =
        std::min_element(
            msgs->begin(), msgs->end(),
            [](decltype(*msgs->begin()) a, decltype(*msgs->begin()) b) {
              return a.second.lastcommitted() < b.second.lastcommitted();
            })
            ->second.lastcommitted();
    opnum_t minCommitted = std::min(minCommittedSVC, minCommittedDVC);
    minCommitted = std::min(minCommitted, lastCommitted);

    EnterView(msg.view());

    UW_ASSERT(AmLeader());

    lastOp = latestOp;
    if (latestMsg != nullptr) {
      CommitUpTo(latestMsg->lastcommitted());
    }

    // Send a STARTVIEW message with the new log
    proto::StartViewMessage sv;
    sv.set_view(view);
    sv.set_lastop(lastOp);
    sv.set_lastcommitted(lastCommitted);

    log.Dump(minCommitted, sv.mutable_entries());

    if (!(transport->SendMessageToGroup(this, groupIdx, sv))) {
      RWarning("Failed to send StartView message to all replicas");
    }
  }
}

void VRReplica::HandleStartView(const TransportAddress &remote,
                                const proto::StartViewMessage &msg) {
  RDebug("Received STARTVIEW " FMT_VIEW " op=" FMT_OPNUM " committed=" FMT_OPNUM
         " entries=%d",
         msg.view(), msg.lastop(), msg.lastcommitted(), msg.entries_size());
  RDebug("Currently in view " FMT_VIEW " op " FMT_OPNUM " committed " FMT_OPNUM,
         view, lastOp, lastCommitted);

  if (msg.view() < view) {
    RWarning("Ignoring STARTVIEW for older view");
    return;
  }

  if ((msg.view() == view) && (status != STATUS_VIEW_CHANGE)) {
    RWarning("Ignoring STARTVIEW for current view");
    return;
  }

  UW_ASSERT(configuration.GetLeaderIndex(msg.view()) != myIdx);

  if (msg.entries_size() == 0) {
    UW_ASSERT(msg.lastcommitted() == lastCommitted);
    UW_ASSERT(msg.lastop() == msg.lastcommitted());
  } else {
    if (msg.entries(0).opnum() > lastCommitted + 1) {
      RPanic("Not enough entries in STARTVIEW message to install new log");
    }

    // Install the new log
    log.RemoveAfter(msg.lastop() + 1);
    log.Install(msg.entries().begin(), msg.entries().end());
  }

  EnterView(msg.view());
  opnum_t oldLastOp = lastOp;
  lastOp = msg.lastop();

  UW_ASSERT(!AmLeader());

  CommitUpTo(msg.lastcommitted());
  SendPrepareOKs(oldLastOp);
}

}  // namespace vr
}  // namespace replication
