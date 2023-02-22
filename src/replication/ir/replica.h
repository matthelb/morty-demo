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

#ifndef REPLICATION_IR_REPLICA_H_
#define REPLICATION_IR_REPLICA_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/persistent_register.h"
#include "lib/transport.h"
#include "lib/udptransport.h"
#include "replication/common/quorumset.h"
#include "replication/common/replica.h"
#include "replication/common/viewstamp.h"
#include "replication/ir/ir-proto.pb.h"
#include "replication/ir/record.h"
#include "store/common/timestamp.h"

namespace replication {
namespace ir {

class IRAppReplica {
 public:
  IRAppReplica() = default;
  virtual ~IRAppReplica() = default;
  // Invoke inconsistent operation, no return value
  virtual void ExecInconsistentUpcall(const std::string &str1) {}
  // Invoke consensus operation
  virtual void ExecConsensusUpcall(const std::string &str1, std::string *str2) {
  }
  // Invoke unreplicated operation
  virtual void UnloggedUpcall(const std::string &str1, std::string *str2) {}
  // Sync
  virtual void Sync(const std::map<opid_t, RecordEntry> &record) {}
  // Merge
  virtual std::map<opid_t, std::string> Merge(
      const std::map<opid_t, std::vector<RecordEntry>> &d,
      const std::map<opid_t, std::vector<RecordEntry>> &u,
      const std::map<opid_t, std::string> &majority_results_in_d) {
    return {};
  }
};

class IRReplica : public TransportReceiver {
 public:
  IRReplica(transport::Configuration config, int groupIdx, int myIdx,
            Transport *transport, IRAppReplica *app);
  ~IRReplica() override;

  // Message handlers.
  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data);
  void HandleMessage(const TransportAddress &remote, const std::string &type,
                     const std::string &data);
  void HandleProposeInconsistent(const TransportAddress &remote,
                                 proto::ProposeInconsistentMessage *msg);
  void HandleFinalizeInconsistent(
      const TransportAddress &remote,
      const proto::FinalizeInconsistentMessage &msg);
  void HandleProposeConsensus(const TransportAddress &remote,
                              proto::ProposeConsensusMessage *msg);
  void HandleFinalizeConsensus(const TransportAddress &remote,
                               const proto::FinalizeConsensusMessage &msg);
  void HandleDoViewChange(const TransportAddress &remote,
                          const proto::DoViewChangeMessage &msg);
  void HandleStartView(const TransportAddress &remote,
                       const proto::StartViewMessage &msg);
  void HandleUnlogged(const TransportAddress &remote,
                      const proto::UnloggedRequestMessage &msg);

  // Timeout handlers.
  void HandleViewChangeTimeout();

 private:
  // Persist `view` and `latest_normal_view` to disk using
  // `persistent_view_info`.
  void PersistViewInfo();

  // Recover `view` and `latest_normal_view` from disk using
  // `persistent_view_info`.
  void RecoverViewInfo();

  // Broadcast DO-VIEW-CHANGE messages to all other replicas with our record
  // included only in the message to the leader.
  void BroadcastDoViewChangeMessages();

  // IrMergeRecords implements Figure 5 of the TAPIR paper.
  Record IrMergeRecords(
      const std::map<int, proto::DoViewChangeMessage> &records);

  transport::Configuration config;
  int groupIdx;
  int myIdx;  // Replica index into config.
  Transport *transport;
  IRAppReplica *app;

  ReplicaStatus status;

  // For the view change and recovery protocol, a replica stores its view and
  // latest normal view to disk. We store this info in view and
  // latest_normal_view and use persistent_view_info to persist it to disk.
  view_t view;
  view_t latest_normal_view;
  PersistentRegister persistent_view_info;

  Record record;
  std::unique_ptr<Timeout> view_change_timeout;

  // The leader of a view-change waits to receive a quorum of DO-VIEW-CHANGE
  // messages before merging and syncing and sending out START-VIEW messages.
  // do_view_change_quorum is used to wait for this quorum.
  //
  // TODO(matthelb): Garbage collect old view change quorums. Once we've entered
  // view v, we should be able to garbage collect all quorums for views less
  // than v.
  QuorumSet<view_t, proto::DoViewChangeMessage> do_view_change_quorum;

  enum MessageType {
    MESSAGE_TYPE_PROPOSE_INCONSISTENT = 0,
    MESSAGE_TYPE_FINALIZE_INCONSISTENT = 1,
    MESSAGE_TYPE_PROPOSE_CONSENSUS = 2,
    MESSAGE_TYPE_FINALIZE_CONSENSUS = 3,
    MESSAGE_TYPE_UNLOGGED = 4,
    MESSAGE_TYPE_DO_VIEW_CHANGE = 5,
    MESSAGE_TYPE_START_VIEW = 6
  };
  std::unordered_map<std::string, MessageType> msg_types;
  proto::ProposeInconsistentMessage proposeInconsistent;
  proto::FinalizeInconsistentMessage finalizeInconsistent;
  proto::ProposeConsensusMessage proposeConsensus;
  proto::FinalizeConsensusMessage finalizeConsensus;
  proto::UnloggedRequestMessage unloggedRequest;
  proto::DoViewChangeMessage doViewChange;
  proto::StartViewMessage startView;

  proto::ReplyInconsistentMessage replyInconsistent;
  proto::ConfirmMessage confirm;
  proto::ReplyConsensusMessage replyConsensus;
  proto::UnloggedReplyMessage unloggedReply;
};

}  // namespace ir
}  // namespace replication

#endif  // REPLICATION_IR_REPLICA_H_
