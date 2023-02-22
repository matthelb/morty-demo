// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/client.h:
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

#ifndef REPLICATION_IR_CLIENT_H_
#define REPLICATION_IR_CLIENT_H_

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "replication/common/client.h"
#include "replication/common/quorumset.h"
#include "replication/common/viewstamp.h"
#include "replication/ir/ir-proto.pb.h"

class Transport;
class TransportAddress;
namespace transport {
class Configuration;
}  // namespace transport

namespace replication {
namespace ir {

namespace proto {
class ConfirmMessage;
class ReplyConsensusMessage;
class ReplyInconsistentMessage;
class UnloggedReplyMessage;
}  // namespace proto

class IRClient : public Client {
 public:
  int total_consensus_counter{};
  int fast_path_counter{};

  using result_set_t = std::map<string, std::size_t>;
  using decide_t = std::function<string(const result_set_t &)>;

  IRClient(const transport::Configuration &config, Transport *transport,
           int group, uint64_t clientid, bool resend_on_timeout);
  ~IRClient() override;

  void Invoke(const string &request, continuation_t continuation,
              error_continuation_t error_continuation = nullptr) override;
  void InvokeUnlogged(int replicaIdx, const string &request,
                      continuation_t continuation,
                      error_continuation_t error_continuation = nullptr,
                      uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
  void InvokeUnloggedAll(
      const string &request, continuation_t continuation,
      error_continuation_t error_continuation = nullptr,
      uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT) override;
  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  virtual void InvokeInconsistent(
      const string &request, continuation_t continuation,
      error_continuation_t error_continuation = nullptr);
  virtual void InvokeConsensus(
      const string &request, decide_t decide, continuation_t continuation,
      error_continuation_t error_continuation = nullptr);

 protected:
  struct PendingRequest {
    string request{};
    uint64_t clientReqId;
    continuation_t continuation{};
    bool continuationInvoked = false;
    std::unique_ptr<Timeout> timer{};
    QuorumSet<viewstamp_t, proto::ConfirmMessage> confirmQuorum;

    inline PendingRequest(string request, uint64_t clientReqId,
                          continuation_t continuation,
                          std::unique_ptr<Timeout> timer, int quorumSize)
        : request(request),
          clientReqId(clientReqId),
          continuation(continuation),
          timer(std::move(timer)),
          confirmQuorum(quorumSize) {}
    virtual ~PendingRequest() = default;
  };

  struct PendingUnloggedRequest : public PendingRequest {
    error_continuation_t error_continuation{};

    inline PendingUnloggedRequest(string request, uint64_t clientReqId,
                                  continuation_t continuation,
                                  error_continuation_t error_continuation,
                                  std::unique_ptr<Timeout> timer)
        : PendingRequest(request, clientReqId, continuation, std::move(timer),
                         1),
          error_continuation(error_continuation) {}
  };

  struct PendingInconsistentRequest : public PendingRequest {
    QuorumSet<viewstamp_t, proto::ReplyInconsistentMessage>
        inconsistentReplyQuorum;

    inline PendingInconsistentRequest(string request, uint64_t clientReqId,
                                      continuation_t continuation,
                                      std::unique_ptr<Timeout> timer,
                                      int quorumSize)
        : PendingRequest(request, clientReqId, continuation, std::move(timer),
                         quorumSize),
          inconsistentReplyQuorum(quorumSize) {}
  };

  struct PendingConsensusRequest : public PendingRequest {
    QuorumSet<opnum_t, proto::ReplyConsensusMessage> consensusReplyQuorum;
    decide_t decide{};
    string decideResult{};
    const std::size_t quorumSize{};
    const std::size_t superQuorumSize{};
    bool on_slow_path;
    error_continuation_t error_continuation{};

    // The timer to give up on the fast path and transition to the slow
    // path. After this timer is run for the first time, it is nulled.
    std::unique_ptr<Timeout> transition_to_slow_path_timer{};

    // The view for which a majority result (or finalized result) was
    // found. The view of a majority of confirms must match this view.
    uint64_t reply_consensus_view = 0;

    // True when a consensus request has already received a quorum or super
    // quorum of replies and has already transitioned into the confirm
    // phase.
    bool sent_confirms = false;

    inline PendingConsensusRequest(
        string request, uint64_t clientReqId, continuation_t continuation,
        std::unique_ptr<Timeout> timer,
        std::unique_ptr<Timeout> transition_to_slow_path_timer, int quorumSize,
        int superQuorum, decide_t decide,
        error_continuation_t error_continuation)
        : PendingRequest(request, clientReqId, continuation, std::move(timer),
                         quorumSize),
          consensusReplyQuorum(quorumSize),
          decide(decide),
          quorumSize(quorumSize),
          superQuorumSize(superQuorum),
          on_slow_path(false),
          error_continuation(error_continuation),
          transition_to_slow_path_timer(
              std::move(transition_to_slow_path_timer)) {}
  };

  uint64_t lastReqId;
  const bool resend_on_timeout_;
  std::unordered_map<uint64_t, PendingRequest *> pendingReqs;

  void SendInconsistent(const PendingInconsistentRequest *req);
  void ResendInconsistent(uint64_t reqId);
  void SendConsensus(const PendingConsensusRequest *req);
  void ResendConsensus(uint64_t reqId);

  // `TransitionToConsensusSlowPath` is called after a timeout to end the
  // possibility of taking the fast path and transition into taking the slow
  // path.
  void TransitionToConsensusSlowPath(uint64_t reqId);

  // HandleSlowPathConsensus is called in one of two scenarios:
  //
  //   1. A finalized ReplyConsensusMessage was received. In this case, we
  //      immediately enter the slow path and use the finalized result. If
  //      finalized is true, req has already been populated with the
  //      finalized result.
  //   2. We're in the slow path and receive a majority of
  //      ReplyConsensusMessages in the same view. In this case, we call
  //      decide to determine the final result.
  //
  // In either case, HandleSlowPathConsensus intitiates the finalize phase of
  // a consensus request.
  void HandleSlowPathConsensus(
      uint64_t reqid, const std::map<int, proto::ReplyConsensusMessage> &msgs,
      bool finalized_result_found, PendingConsensusRequest *req);

  // HandleFastPathConsensus is called when we're on the fast path and
  // receive a super quorum of responses from the same view.
  // HandleFastPathConsensus will check to see if there is a superquorum of
  // matching responses. If there is, it will return to the user and
  // asynchronously intitiate the finalize phase of a consensus request.
  // Otherwise, it transitions into the slow path which will also initiate
  // the finalize phase of a consensus request, but not yet return to the
  // user.
  void HandleFastPathConsensus(
      uint64_t reqid, const std::map<int, proto::ReplyConsensusMessage> &msgs,
      PendingConsensusRequest *req);

  void ResendConfirmation(uint64_t reqId, bool isConsensus);
  void HandleInconsistentReply(const TransportAddress &remote,
                               const proto::ReplyInconsistentMessage &msg);
  void HandleConsensusReply(const TransportAddress &remote,
                            const proto::ReplyConsensusMessage &msg);
  void HandleConfirm(const TransportAddress &remote,
                     const proto::ConfirmMessage &msg);
  void HandleUnloggedReply(const TransportAddress &remote,
                           const proto::UnloggedReplyMessage &msg);
  void UnloggedRequestTimeoutCallback(uint64_t reqId);
};

}  // namespace ir
}  // namespace replication

#endif  // REPLICATION_IR_CLIENT_H_
