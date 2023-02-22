// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/client.h:
 *   Inconsistent replication replica
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

#include "replication/ir/replica.h"

#include <bits/stdint-uintn.h>
#include <inttypes.h>
#include <math.h>

#include <algorithm>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "replication/common/request.pb.h"

namespace replication {
namespace ir {

IRReplica::IRReplica(transport::Configuration config, int groupIdx, int myIdx,
                     Transport *transport, IRAppReplica *app)
    : config(config),
      groupIdx(groupIdx),
      myIdx(myIdx),
      transport(transport),
      app(app),
      status(STATUS_NORMAL),
      view(0),
      latest_normal_view(0),
      // TODO(matthelb): Take these filenames in via the command line?
      persistent_view_info(config.replica(groupIdx, myIdx).host + ":" +
                           config.replica(groupIdx, myIdx).port + "_" +
                           std::to_string(myIdx) + ".bin"),
      // Note that a leader waits for DO-VIEW-CHANGE messages from f other
      // replicas (as opposed to f + 1) for a total of f + 1 replicas.
      do_view_change_quorum(config.f) {
  transport->Register(this, config, groupIdx, myIdx);

  // If our view info was previously initialized, then we are being started
  // in recovery mode. If our view info has never been initialized, then this
  // is the first time we are being run.
  if (persistent_view_info.Initialized()) {
    Debug("View information found in %s. Starting recovery.",
          persistent_view_info.Filename().c_str());
    status = STATUS_RECOVERING;
    RecoverViewInfo();
    Debug("Recovered view = %" PRIu64 " latest_normal_view = %" PRIu64 ".",
          view, latest_normal_view);
    ++view;
    if (myIdx == config.GetLeaderIndex(view)) {
      // A recoverying replica should not be the leader.
      ++view;
    }
    PersistViewInfo();
    BroadcastDoViewChangeMessages();
  } else {
    // PersistViewInfo();
  }

  // TODO(matthelb): Figure out a good view change timeout.
  const uint64_t view_change_timeout_ms = 10 * 1000;
  view_change_timeout =
      std::make_unique<Timeout>(transport, view_change_timeout_ms,
                                [this]() { this->HandleViewChangeTimeout(); });
  // TODO(matthelb): I guess we can comment this if Irene did xD
  // view_change_timeout->Start();
  msg_types.insert(std::make_pair(proposeInconsistent.GetTypeName(),
                                  MESSAGE_TYPE_PROPOSE_INCONSISTENT));
  msg_types.insert(std::make_pair(finalizeInconsistent.GetTypeName(),
                                  MESSAGE_TYPE_FINALIZE_INCONSISTENT));
  msg_types.insert(std::make_pair(proposeConsensus.GetTypeName(),
                                  MESSAGE_TYPE_PROPOSE_CONSENSUS));
  msg_types.insert(std::make_pair(finalizeConsensus.GetTypeName(),
                                  MESSAGE_TYPE_FINALIZE_CONSENSUS));
  msg_types.insert(
      std::make_pair(unloggedRequest.GetTypeName(), MESSAGE_TYPE_UNLOGGED));
  msg_types.insert(
      std::make_pair(doViewChange.GetTypeName(), MESSAGE_TYPE_DO_VIEW_CHANGE));
  msg_types.insert(
      std::make_pair(startView.GetTypeName(), MESSAGE_TYPE_START_VIEW));
}

IRReplica::~IRReplica() = default;

void IRReplica::ReceiveMessage(const TransportAddress &remote,
                               std::string *type, std::string *data,
                               void *meta_data) {
  HandleMessage(remote, *type, *data);
}

void IRReplica::HandleMessage(const TransportAddress &remote,
                              const std::string &type,
                              const std::string &data) {
  auto msg_types_itr = msg_types.find(type);
  if (msg_types_itr == msg_types.end()) {
    Panic("Received unknown message type %s.", type.c_str());
    return;
  }

  switch (msg_types_itr->second) {
    case MESSAGE_TYPE_PROPOSE_INCONSISTENT: {
      proposeInconsistent.ParseFromString(data);
      HandleProposeInconsistent(remote, &proposeInconsistent);
      break;
    }
    case MESSAGE_TYPE_FINALIZE_INCONSISTENT: {
      finalizeInconsistent.ParseFromString(data);
      HandleFinalizeInconsistent(remote, finalizeInconsistent);
      break;
    }
    case MESSAGE_TYPE_PROPOSE_CONSENSUS: {
      proposeConsensus.ParseFromString(data);
      HandleProposeConsensus(remote, &proposeConsensus);
      break;
    }
    case MESSAGE_TYPE_FINALIZE_CONSENSUS: {
      finalizeConsensus.ParseFromString(data);
      HandleFinalizeConsensus(remote, finalizeConsensus);
      break;
    }
    case MESSAGE_TYPE_DO_VIEW_CHANGE: {
      doViewChange.ParseFromString(data);
      HandleDoViewChange(remote, doViewChange);
      break;
    }
    case MESSAGE_TYPE_START_VIEW: {
      startView.ParseFromString(data);
      HandleStartView(remote, startView);
      break;
    }
    case MESSAGE_TYPE_UNLOGGED: {
      unloggedRequest.ParseFromString(data);
      HandleUnlogged(remote, unloggedRequest);
      break;
    }
    default:
      NOT_REACHABLE();
  }
}

void IRReplica::HandleProposeInconsistent(
    const TransportAddress &remote,
    proto::ProposeInconsistentMessage *msg) {
  uint64_t clientid = msg->req().clientid();
  uint64_t clientreqid = msg->req().clientreqid();

  Debug("%lu:%lu Received inconsistent op: %s", clientid, clientreqid,
        msg->req().op().substr(0, 10).c_str());

  opid_t opid = std::make_pair(clientid, clientreqid);

  // Check record if we've already handled this request
  RecordEntry *entry = record.Find(opid);
  if (entry != nullptr) {
    Debug("Already have op %lu:%lu in record.", clientid, clientreqid);
    // If we already have this op in our record, then just return it
    replyInconsistent.set_view(entry->view);
    replyInconsistent.set_replicaidx(myIdx);
    replyInconsistent.mutable_opid()->set_clientid(clientid);
    replyInconsistent.mutable_opid()->set_clientreqid(clientreqid);
    replyInconsistent.set_finalized(
        entry->state == proto::RECORD_STATE_FINALIZED);
  } else {
    Debug("Adding op %lu:%lu to record as tentative.", clientid, clientreqid);
    // Otherwise, put it in our record as tentative
    record.Add(view, opid, std::move(*msg->release_req()),
               proto::RECORD_STATE_TENTATIVE,
               proto::RECORD_TYPE_INCONSISTENT);

    // 3. Return Reply
    replyInconsistent.set_view(view);
    replyInconsistent.set_replicaidx(myIdx);
    replyInconsistent.mutable_opid()->set_clientid(clientid);
    replyInconsistent.mutable_opid()->set_clientreqid(clientreqid);
    replyInconsistent.set_finalized(false);
  }

  // Send the replyInconsistent
  transport->SendMessage(this, remote, replyInconsistent);
}

void IRReplica::HandleFinalizeInconsistent(
    const TransportAddress &remote,
    const proto::FinalizeInconsistentMessage &msg) {
  uint64_t clientid = msg.opid().clientid();
  uint64_t clientreqid = msg.opid().clientreqid();

  Debug("%lu:%lu Received finalize inconsistent op", clientid, clientreqid);

  opid_t opid = std::make_pair(clientid, clientreqid);

  // Check record for the request
  RecordEntry *entry = record.Find(opid);
  if (entry != nullptr &&
      entry->state == proto::RECORD_STATE_TENTATIVE) {
    // Mark entry as finalized
    record.SetStatus(opid, proto::RECORD_STATE_FINALIZED);

    // Execute the operation
    app->ExecInconsistentUpcall(entry->request.op());

    // Send the reply
    confirm.set_view(view);
    confirm.set_replicaidx(myIdx);
    *confirm.mutable_opid() = msg.opid();

    transport->SendMessage(this, remote, confirm);
  } else {
    // Ignore?
  }
}

void IRReplica::HandleProposeConsensus(
    const TransportAddress &remote,
    proto::ProposeConsensusMessage *msg) {
  uint64_t clientid = msg->req().clientid();
  uint64_t clientreqid = msg->req().clientreqid();

  Debug("%lu:%lu Received consensus op: %s", clientid, clientreqid,
        msg->req().op().substr(0, 10).c_str());

  opid_t opid = std::make_pair(clientid, clientreqid);

  // Check record if we've already handled this request
  RecordEntry *entry = record.Find(opid);
  if (entry != nullptr) {
    // If we already have this op in our record, then just return it
    replyConsensus.set_view(entry->view);
    replyConsensus.set_replicaidx(myIdx);
    replyConsensus.mutable_opid()->set_clientid(clientid);
    replyConsensus.mutable_opid()->set_clientreqid(clientreqid);
    replyConsensus.set_result(entry->result);
    replyConsensus.set_finalized(
        entry->state == proto::RECORD_STATE_FINALIZED);
  } else {
    // Execute op
    std::string result;

    app->ExecConsensusUpcall(msg->req().op(), &result);

    // Put it in our record as tentative
    record.Add(view, opid, std::move(*msg->release_req()),
               proto::RECORD_STATE_TENTATIVE,
               proto::RECORD_TYPE_CONSENSUS,
               std::move(result));

    // 3. Return proto::Reply
    replyConsensus.set_view(view);
    replyConsensus.set_replicaidx(myIdx);
    replyConsensus.mutable_opid()->set_clientid(clientid);
    replyConsensus.mutable_opid()->set_clientreqid(clientreqid);
    replyConsensus.set_result(result);
    replyConsensus.set_finalized(false);
  }

  // Send the replyConsensus
  if (transport->SendMessage(this, remote, replyConsensus)) {
    Debug("%lu:%lu Replied to ProposeConsensusMessage", clientid, clientreqid);
  } else {
    Warning("%lu:%lu Failed to send ReplyConsensusMessage", clientid,
            clientreqid);
  }
}

void IRReplica::HandleFinalizeConsensus(
    const TransportAddress &remote,
    const proto::FinalizeConsensusMessage &msg) {
  uint64_t clientid = msg.opid().clientid();
  uint64_t clientreqid = msg.opid().clientreqid();

  Debug("%lu:%lu Received finalize consensus op", clientid, clientreqid);

  opid_t opid = std::make_pair(clientid, clientreqid);

  // Check record for the request
  RecordEntry *entry = record.Find(opid);
  if (entry != nullptr) {
    // Mark entry as finalized
    record.SetStatus(opid, proto::RECORD_STATE_FINALIZED);

    if (msg.result() != entry->result) {
      // Update the result
      entry->result = msg.result();
    }

    // Send the reply
    confirm.set_view(view);
    confirm.set_replicaidx(myIdx);
    *confirm.mutable_opid() = msg.opid();

    if (transport->SendMessage(this, remote, confirm)) {
      Debug("%lu:%lu Replied to finalize consensus op", clientid, clientreqid);
    } else {
      Warning("%lu:%lu Failed to send confirm message", clientid, clientreqid);
    }
  } else {
    // Ignore?
    Warning("Finalize request for unknown consensus operation");
  }
}

void IRReplica::HandleDoViewChange(const TransportAddress &remote,
                                   const proto::DoViewChangeMessage &msg) {
  Debug("Received DoViewChangeMessage from replica %d with new_view = %" PRIu64
        ", latest_normal_view = %" PRIu64 ", has_record = %d.",
        msg.replicaidx(), msg.new_view(), msg.latest_normal_view(),
        msg.has_record());

  if (msg.new_view() < view) {
    Debug("Ignoring DO-VIEW-CHANGE for view %" PRIu64 " < %" PRIu64 ". ",
          msg.new_view(), view);
    return;
  }
  if (msg.new_view() == view) {
    // If we're NORMAL, then we've already completed this view change.
    if (status == STATUS_NORMAL) {
      Debug("Ignoring DO-VIEW-CHANGE for view %" PRIu64
            " because our status is NORMAL.",
            view);
      return;
    }

    // If we're a recovering node, we don't want to be the leader.
    if (status == STATUS_NORMAL) {
      Debug("Ignoring DO-VIEW-CHANGE for view %" PRIu64
            " because our status is RECOVERING.",
            view);
      return;
    }
  } else {
    UW_ASSERT(msg.new_view() > view);

    // Update and persist our view.
    view = msg.new_view();
    PersistViewInfo();

    // Update our status. If we're NORMAL, then we transition into
    // VIEW_CHANGE.  If we're VIEW_CHANGE or RECOVERING, we want to stay in
    // VIEW_CHANGE or RECOVERING. Note that it would be a bug to transition
    // from RECOVERING to VIEW_CHANGE before we finish recovering.
    if (status == STATUS_NORMAL) {
      status = STATUS_VIEW_CHANGE;
    }

    // We just began a new view change, so we need to broadcast
    // DO-VIEW-CHANGE messages to everyone.
    BroadcastDoViewChangeMessages();

    // Restart our view change timer. We don't to perform a view change
    // right after we just performed a view change.
    view_change_timeout->Reset();
  }

  UW_ASSERT(msg.new_view() == view);

  // If we're not the leader of this view change, then we have nothing to do.
  if (myIdx != config.GetLeaderIndex(view)) {
    return;
  }

  // Replicas should send their records to the leader.
  UW_ASSERT(msg.has_record());
  const std::map<int, proto::DoViewChangeMessage> *quorum =
      do_view_change_quorum.AddAndCheckForQuorum(msg.new_view(),
                                                 msg.replicaidx(), msg);
  if (quorum == nullptr) {
    // There is no quorum yet.
    return;
  }
  Debug(
      "Received a quourum of DoViewChangeMessages. Initiating "
      "IR-MERGE-RECORDS.");

  // Update our record, status, and view.
  record = IrMergeRecords(*quorum);
  status = STATUS_NORMAL;
  view = msg.new_view();
  latest_normal_view = view;
  PersistViewInfo();

  // Notify all replicas of the new view.
  proto::StartViewMessage start_view_msg;
  record.ToProto(start_view_msg.mutable_record());
  start_view_msg.set_new_view(view);
  // TODO(matthelb): Don't send this message to myself. It's not incorrect, but
  // it's unnecessary.
  // TODO(matthelb): Acknowledge StartViewMessage messages, and rebroadcast them
  // after a timeout.
  Debug("Sending StartViewMessages to all replicas.");
  bool success = transport->SendMessageToAll(this, start_view_msg);
  if (!success) {
    Warning("Could not send StartViewMessage.");
  }
}

void IRReplica::HandleStartView(const TransportAddress &remote,
                                const proto::StartViewMessage &msg) {
  Debug("Received StartViewMessage with new_view = %" PRIu64 ".",
        msg.new_view());

  // A leader should not be sending START-VIEW messages to themselves.
  UW_ASSERT(myIdx != config.GetLeaderIndex(msg.new_view()));

  if (msg.new_view() < view) {
    Debug("Ignoring START-VIEW for view %" PRIu64 " < %" PRIu64 ". ",
          msg.new_view(), view);
    return;
  }

  // If new_view == view and we're NORMAL, then we've already completed this
  // view change, and we don't want to do it again.
  if (msg.new_view() == view && status == STATUS_NORMAL) {
    Debug("Ignoring START-VIEW for view %" PRIu64
          " because our status is NORMAL.",
          view);
    return;
  }

  UW_ASSERT((msg.new_view() >= view) ||
            (msg.new_view() == view && status != STATUS_NORMAL));

  // Throw away our record for the new master record and call sync.
  record = Record(msg.record());
  app->Sync(record.Entries());

  status = STATUS_NORMAL;
  view = msg.new_view();
  latest_normal_view = view;
  PersistViewInfo();
}

void IRReplica::HandleUnlogged(
    const TransportAddress &remote,
    const proto::UnloggedRequestMessage &msg) {
  std::string res;

  Debug("Received unlogged request %lu: %s", msg.req().clientreqid(),
        msg.req().op().substr(0, 10).c_str());

  app->UnloggedUpcall(msg.req().op(), &res);
  unloggedReply.set_reply(res);
  unloggedReply.set_clientreqid(msg.req().clientreqid());
  if (transport->SendMessage(this, remote, unloggedReply)) {
    Debug("Sent UnloggedReplyMessage for %lu", msg.req().clientreqid());
  } else {
    Warning("Failed to send unloggedReply message for %lu",
            msg.req().clientreqid());
  }
}

void IRReplica::HandleViewChangeTimeout() {
  Debug("HandleViewChangeTimeout fired.");
  if (status == STATUS_NORMAL) {
    status = STATUS_VIEW_CHANGE;
  }
  ++view;
  PersistViewInfo();
  BroadcastDoViewChangeMessages();
}

void IRReplica::PersistViewInfo() {
  proto::PersistedViewInfo view_info;
  view_info.set_view(view);
  view_info.set_latest_normal_view(latest_normal_view);
  std::string output;
  UW_ASSERT(view_info.SerializeToString(&output));
  persistent_view_info.Write(output);
}

void IRReplica::RecoverViewInfo() {
  proto::PersistedViewInfo view_info;
  view_info.ParseFromString(persistent_view_info.Read());
  view = view_info.view();
  latest_normal_view = view_info.latest_normal_view();
}

void IRReplica::BroadcastDoViewChangeMessages() {
  // Send a DoViewChangeMessage _without_ our record to all replicas except
  // ourselves and the leader.
  proto::DoViewChangeMessage msg;
  msg.set_replicaidx(myIdx);
  msg.clear_record();
  msg.set_new_view(view);
  msg.set_latest_normal_view(latest_normal_view);

  const int leader_idx = config.GetLeaderIndex(view);
  Debug(
      "Broadcasting DoViewChangeMessages to replicas with leader id = %d, "
      "view = %" PRIu64 ", latest_normal_view = %" PRIu64 ".",
      leader_idx, view, latest_normal_view);

  for (int i = 0; i < config.n; ++i) {
    if (i == myIdx || i == leader_idx) {
      continue;
    }

    bool success = transport->SendMessageToReplica(this, i, msg);
    if (!success) {
      Warning("Could not send DoViewChangeMessage to replica %d.", i);
    }
  }

  // Send a DoViewChangeMessage _with_ our record to the leader (unless we
  // are the leader).
  record.ToProto(msg.mutable_record());
  if (leader_idx != myIdx) {
    bool success = transport->SendMessageToReplica(this, leader_idx, msg);
    if (!success) {
      Warning("Could not send DoViewChangeMessage to leader %d.", leader_idx);
    }
  }
}

Record IRReplica::IrMergeRecords(
    const std::map<int, proto::DoViewChangeMessage> &records) {
  // TODO(matthelb): This implementation of IrMergeRecords is not the most
  // efficient in the world. It could be optimized a bit if it happens to be a
  // bottleneck. For example, Merge could take in pointers to the record entry
  // vectors.

  // Create a type alias to save some typing.
  using RecordEntryVec = std::vector<RecordEntry>;

  // Find the largest latest_normal_view.
  view_t max_latest_normal_view = latest_normal_view;
  for (const std::pair<const int, proto::DoViewChangeMessage>
           &p : records) {
    const proto::DoViewChangeMessage &msg = p.second;
    max_latest_normal_view =
        std::max(max_latest_normal_view, msg.latest_normal_view());
  }

  // Collect the records with largest latest_normal_view.
  std::vector<Record> latest_records;
  for (const std::pair<const int, proto::DoViewChangeMessage>
           &p : records) {
    const proto::DoViewChangeMessage &msg = p.second;
    if (msg.latest_normal_view() == max_latest_normal_view) {
      UW_ASSERT(msg.has_record());
      latest_records.emplace_back(msg.record());
    }
  }
  if (latest_normal_view == max_latest_normal_view) {
    latest_records.push_back(std::move(record));
  }

  // Group together all the entries from all the records in latest_records.
  // We'll use this to build d and u. Simultaneously populate R.
  // TODO(matthelb): Avoid redundant copies.
  Record R;
  std::map<opid_t, RecordEntryVec> entries_by_opid;
  for (const Record &r : latest_records) {
    for (const std::pair<const opid_t, RecordEntry> &p : r.Entries()) {
      const opid_t &opid = p.first;
      const RecordEntry &entry = p.second;
      UW_ASSERT(opid == entry.opid);

      if (entry.type == proto::RECORD_TYPE_INCONSISTENT) {
        // TODO(matthelb): Do we have to update the view here?
        if (R.Find(opid) == nullptr) {
          R.Add(entry);
        }
      } else if (entry.state ==
                 proto::RECORD_STATE_FINALIZED) {
        // TODO(matthelb): Do we have to update the view here?
        if (R.Find(opid) == nullptr) {
          R.Add(entry);
        }
        entries_by_opid.erase(opid);
      } else {
        UW_ASSERT(entry.type == proto::RECORD_TYPE_CONSENSUS &&
                  entry.state ==
                      proto::RECORD_STATE_TENTATIVE);
        // If R already contains this operation, then we don't group
        // it.
        if (R.Entries().count(entry.opid) == 0) {
          entries_by_opid[entry.opid].push_back(entry);
        }
      }
    }
  }

  // Build d and u.
  std::map<opid_t, RecordEntryVec> d;
  std::map<opid_t, RecordEntryVec> u;
  std::map<opid_t, std::string> majority_results_in_d;
  for (const std::pair<const opid_t, RecordEntryVec> &p : entries_by_opid) {
    const opid_t &opid = p.first;
    const RecordEntryVec &entries = p.second;

    // Count the frequency of each response.
    std::map<std::string, std::size_t> result_counts;
    for (const RecordEntry &entry : entries) {
      result_counts[entry.result] += 1;
    }

    // Check if any response occurs ceil(f/2) + 1 times or more.
    bool in_d = false;
    std::string majority_result_in_d = majority_result_in_d;
    for (const std::pair<const std::string, std::size_t> &c : result_counts) {
      if (c.second >= ceil(0.5 * config.f) + 1) {
        majority_result_in_d = c.first;
        in_d = true;
        break;
      }
    }

    // TODO(matthelb): Avoid redundant copies.
    if (in_d) {
      d[opid] = entries;
      majority_results_in_d[opid] = majority_result_in_d;
    } else {
      u[opid] = entries;
    }
  }

  // Sync.
  app->Sync(R.Entries());

  // Merge.
  std::map<opid_t, std::string> results_by_opid =
      app->Merge(d, u, majority_results_in_d);

  // Sanity check Merge results. Every opid should be present.
  UW_ASSERT(results_by_opid.size() == d.size() + u.size());
  for (const std::pair<const opid_t, std::string> &p : results_by_opid) {
    const opid_t &opid = p.first;
    UW_ASSERT(d.count(opid) + u.count(opid) == 1);
  }

  // Convert Merge results into a Record.
  Record merged;
  for (std::pair<const opid_t, std::string> &p : results_by_opid) {
    const opid_t &opid = p.first;
    std::string &result = p.second;

    const std::vector<RecordEntry> entries = entries_by_opid[opid];
    UW_ASSERT(!records.empty());
    const RecordEntry &entry = entries[0];

    // TODO(matthelb): Is this view correct?
    merged.Add(view, opid, entry.request,
               proto::RECORD_STATE_FINALIZED, entry.type,
               result);
  }

  // R = R cup merged.
  for (const std::pair<const opid_t, RecordEntry> &r : merged.Entries()) {
    // TODO(matthelb): Avoid copy.
    R.Add(r.second);
  }
  return R;
}

}  // namespace ir
}  // namespace replication
