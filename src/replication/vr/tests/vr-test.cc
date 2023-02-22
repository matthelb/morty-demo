// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr-test.cc:
 *   test cases for Viewstamped Replication protocol
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

#include <bits/stdint-uintn.h>
#include <ext/alloc_traits.h>
#include <google/protobuf/message.h>
#include <time.h>

#include <cstdlib>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest-message.h"
#include "gtest/gtest-param-test.h"
#include "gtest/gtest-test-part.h"
#include "gtest/gtest_pred_impl.h"
#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/simtransport.h"
#include "replication/common/client.h"
#include "replication/common/replica.h"
#include "replication/common/viewstamp.h"
#include "replication/vr/client.h"
#include "replication/vr/replica.h"
#include "replication/vr/vr-proto.pb.h"

class TransportReceiver;

class VRApp : public replication::AppReplica {
  std::vector<std::string> *ops;
  std::vector<std::string> *unloggedOps;

 public:
  VRApp(std::vector<std::string> *o, std::vector<std::string> *u)
      : ops(o), unloggedOps(u) {}

  void ReplicaUpcall(opnum_t opnum, const std::string &req,
                     std::string *reply) override {
    ops->push_back(req);
    *reply = "reply: " + req;
  }

  void UnloggedUpcall(const std::string &req, std::string *reply) override {
    unloggedOps->push_back(req);
    *reply = "unlreply: " + req;
  }
};

class VRTest : public ::testing::TestWithParam<int> {
 public:
  static std::string replicaLastOp;
  static std::string clientLastOp;
  static std::string clientLastReply;

 protected:
  std::vector<replication::vr::VRReplica *> replicas;
  replication::vr::VRClient *client{};
  SimulatedTransport *transport{};
  transport::Configuration *config{};
  std::vector<std::vector<std::string>> ops;
  std::vector<std::vector<std::string>> unloggedOps;
  int requestNum{};

  void SetUp() override {
    std::map<int, std::vector<transport::ReplicaAddress>> replicaAddrs = {
        {0,
         {{"localhost", "12345"},
          {"localhost", "12346"},
          {"localhost", "12347"}}}};
    config = new transport::Configuration(1, 3, 1, replicaAddrs);

    transport = new SimulatedTransport();

    ops.resize(config->n);
    unloggedOps.resize(config->n);

    for (int i = 0; i < config->n; i++) {
      replicas.push_back(
          new replication::vr::VRReplica(*config, 0, i, transport, GetParam(),
                                         new VRApp(&ops[i], &unloggedOps[i])));
    }

    client = new replication::vr::VRClient(*config, transport, 0, 0);
    requestNum = -1;

    // Only let tests run for a simulated minute. This prevents
    // infinite retry loops, etc.
    //        transport->Timer(60000, [&]() {
    //                transport->CancelAllTimers();
    //            });
  }

  virtual std::string RequestOp(int n) {
    std::stringstream stream;
    stream << "test: " << n;
    return stream.str();
  }

  virtual std::string LastRequestOp() { return RequestOp(requestNum); }

  virtual void ClientSendNext(replication::Client::continuation_t upcall) {
    requestNum++;
    client->Invoke(LastRequestOp(), std::move(upcall));
  }

  virtual void ClientSendNextUnlogged(
      int idx, replication::Client::continuation_t upcall,
      replication::Client::error_continuation_t error_continuation = nullptr,
      uint32_t timeout = replication::Client::DEFAULT_UNLOGGED_OP_TIMEOUT) {
    requestNum++;
    client->InvokeUnlogged(idx, LastRequestOp(), std::move(upcall),
                           std::move(error_continuation), timeout);
  }

  void TearDown() override {
    for (auto x : replicas) {
      delete x;
    }

    replicas.clear();
    ops.clear();
    unloggedOps.clear();

    delete client;
    delete transport;
    delete config;
  }
};

TEST_P(VRTest, OneOp) {
  auto upcall = [this](const std::string &req, const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());

    // Not guaranteed that any replicas except the leader have
    // executed this request.
    EXPECT_EQ(ops[0].back(), req);
    transport->CancelAllTimers();
    return true;
  };

  ClientSendNext(upcall);
  transport->Run();

  // By now, they all should have executed the last request.
  for (int i = 0; i < config->n; i++) {
    EXPECT_EQ(ops[i].size(), 1UL);
    EXPECT_EQ(ops[i].back(), LastRequestOp());
  }
}

TEST_P(VRTest, Unlogged) {
  auto upcall = [this](const std::string &req, const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "unlreply: " + LastRequestOp());

    EXPECT_EQ(unloggedOps[1].back(), req);
    transport->CancelAllTimers();
    return true;
  };
  int timeouts = 0;
  auto timeout = [&](const std::string &req, replication::ErrorCode) {
    timeouts++;
  };

  ClientSendNextUnlogged(1, upcall, timeout);
  transport->Run();

  for (unsigned int i = 0; i < ops.size(); i++) {
    EXPECT_EQ(0UL, ops[i].size());
    EXPECT_EQ((i == 1UL ? 1UL : 0UL), unloggedOps[i].size());
  }
  EXPECT_EQ(0, timeouts);
}

TEST_P(VRTest, UnloggedTimeout) {
  auto upcall = [this](const std::string &req,
                       const std::string &reply) -> bool {
    transport->CancelAllTimers();
    return true;
  };
  int timeouts = 0;
  auto timeout = [&](const std::string &req, replication::ErrorCode) {
    timeouts++;
  };

  // Drop messages to or from replica 1
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m,
             uint64_t *delay) { return !((srcIdx == 1) || (dstIdx == 1)); });

  // Run for 10 seconds
  transport->Timer(10000, [&]() { transport->CancelAllTimers(); });

  ClientSendNextUnlogged(1, upcall, timeout);
  transport->Run();

  for (unsigned int i = 0; i < ops.size(); i++) {
    EXPECT_EQ(0UL, ops[i].size());
    EXPECT_EQ(0UL, unloggedOps[i].size());
  }
  EXPECT_EQ(1, timeouts);
}

TEST_P(VRTest, ManyOps) {
  replication::Client::continuation_t upcall = [&](const std::string &req,
                                                   const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());

    // Not guaranteed that any replicas except the leader have
    // executed this request.
    EXPECT_EQ(ops[0].back(), req);

    if (requestNum < 9) {
      ClientSendNext(upcall);
    } else {
      transport->CancelAllTimers();
    }
    return true;
  };

  ClientSendNext(upcall);
  transport->Run();

  // By now, they all should have executed the last request.
  for (int i = 0; i < config->n; i++) {
    EXPECT_EQ(10UL, ops[i].size());
    for (int j = 0; j < 10; j++) {
      EXPECT_EQ(RequestOp(j), ops[i][j]);
    }
  }
}

TEST_P(VRTest, FailedReplica) {
  replication::Client::continuation_t upcall = [&](const std::string &req,
                                                   const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());

    // Not guaranteed that any replicas except the leader have
    // executed this request.
    EXPECT_EQ(ops[0].back(), req);

    if (requestNum < 9) {
      ClientSendNext(upcall);
    } else {
      transport->CancelAllTimers();
    }
    return true;
  };

  ClientSendNext(upcall);

  // Drop messages to or from replica 1
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m,
             uint64_t *delay) { return !((srcIdx == 1) || (dstIdx == 1)); });

  transport->Run();

  // By now, they all should have executed the last request.
  for (int i = 0; i < config->n; i++) {
    if (i == 1) {
      continue;
    }
    EXPECT_EQ(10UL, ops[i].size());
    for (int j = 0; j < 10; j++) {
      EXPECT_EQ(RequestOp(j), ops[i][j]);
    }
  }
}

TEST_P(VRTest, StateTransfer) {
  replication::Client::continuation_t upcall = [&](const std::string &req,
                                                   const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());

    // Not guaranteed that any replicas except the leader have
    // executed this request.
    EXPECT_EQ(ops[0].back(), req);

    if (requestNum == 5) {
      // Restore replica 1
      transport->RemoveFilter(10);
    }

    if (requestNum < 9) {
      ClientSendNext(upcall);
    } else {
      transport->CancelAllTimers();
    }
    return true;
  };

  ClientSendNext(upcall);

  // Drop messages to or from replica 1
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m,
             uint64_t *delay) { return !((srcIdx == 1) || (dstIdx == 1)); });

  transport->Run();

  // By now, they all should have executed the last request.
  for (int i = 0; i < config->n; i++) {
    EXPECT_EQ(10UL, ops[i].size());
    for (int j = 0; j < 10; j++) {
      EXPECT_EQ(RequestOp(j), ops[i][j]);
    }
  }
}

TEST_P(VRTest, FailedLeader) {
  replication::Client::continuation_t upcall = [&](const std::string &req,
                                                   const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());

    if (requestNum == 5) {
      // Drop messages to or from replica 0
      transport->AddFilter(
          10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
                 int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
            return !((srcIdx == 0) || (dstIdx == 0));
          });
    }
    if (requestNum < 9) {
      ClientSendNext(upcall);
    } else {
      transport->CancelAllTimers();
    }
    return true;
  };

  ClientSendNext(upcall);

  transport->Run();

  // By now, they all should have executed the last request.
  for (int i = 0; i < config->n; i++) {
    if (i == 0) {
      continue;
    }
    EXPECT_EQ(10UL, ops[i].size());
    for (int j = 0; j < 10; j++) {
      EXPECT_EQ(RequestOp(j), ops[i][j]);
    }
  }
}

TEST_P(VRTest, DroppedReply) {
  bool received = false;
  replication::Client::continuation_t upcall = [&](const std::string &req,
                                                   const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());
    transport->CancelAllTimers();
    received = true;
    return true;
  };

  // Drop the first ReplyMessage
  bool dropped = false;
  transport->AddFilter(
      10,
      [&dropped](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
                 int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        replication::vr::proto::ReplyMessage r;
        if (m.GetTypeName() == r.GetTypeName()) {
          if (!dropped) {
            dropped = true;
            return false;
          }
        }
        return true;
      });
  ClientSendNext(upcall);

  transport->Run();

  EXPECT_TRUE(received);

  // Each replica should have executed only one request
  for (int i = 0; i < config->n; i++) {
    EXPECT_EQ(1UL, ops[i].size());
  }
}

TEST_P(VRTest, DroppedReplyThenFailedLeader) {
  bool received = false;
  replication::Client::continuation_t upcall = [&](const std::string &req,
                                                   const std::string &reply) {
    EXPECT_EQ(req, LastRequestOp());
    EXPECT_EQ(reply, "reply: " + LastRequestOp());
    transport->CancelAllTimers();
    received = true;
    return true;
  };

  // Drop the first ReplyMessage
  bool dropped = false;
  transport->AddFilter(
      10,
      [&dropped](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
                 int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        replication::vr::proto::ReplyMessage r;
        if (m.GetTypeName() == r.GetTypeName()) {
          if (!dropped) {
            dropped = true;
            return false;
          }
        }
        return true;
      });

  // ...and after we've done that, fail the leader altogether
  transport->AddFilter(
      20,
      [&dropped](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
                 int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        if ((srcIdx == 0) || (dstIdx == 0)) {
          return !dropped;
        }
        return true;
      });

  ClientSendNext(upcall);

  transport->Run();

  EXPECT_TRUE(received);

  // Each replica should have executed only one request
  // (and actually the faulty one should too, but don't check that)
  for (int i = 0; i < config->n; i++) {
    if (i != 0) {
      EXPECT_EQ(1UL, ops[i].size());
    }
  }
}

TEST_P(VRTest, ManyClients) {
  const int NUM_CLIENTS = 10;
  const int MAX_REQS = 100;

  std::vector<replication::vr::VRClient *> clients;
  std::vector<int> lastReq;
  std::vector<replication::Client::continuation_t> upcalls;
  for (int i = 0; i < NUM_CLIENTS; i++) {
    clients.push_back(new replication::vr::VRClient(*config, transport, 0, i));
    lastReq.push_back(0);
    upcalls.emplace_back(
        [&, i](const std::string &req, const std::string &reply) {
          EXPECT_EQ("reply: " + RequestOp(lastReq[i]), reply);
          lastReq[i] += 1;
          if (lastReq[i] < MAX_REQS) {
            clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
          }
          return true;
        });
    clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
  }

  // This could take a while; simulate two hours
  transport->Timer(7200000, [&]() { transport->CancelAllTimers(); });

  transport->Run();

  for (int i = 0; i < config->n; i++) {
    UW_ASSERT_EQ(NUM_CLIENTS * MAX_REQS, ops[i].size());
  }

  for (int i = 0; i < NUM_CLIENTS * MAX_REQS; i++) {
    for (int j = 0; j < config->n; j++) {
      UW_ASSERT_EQ(ops[0][i], ops[j][i]);
    }
  }

  for (replication::vr::VRClient *c : clients) {
    delete c;
  }
}

TEST_P(VRTest, Stress) {
  const int NUM_CLIENTS = 10;
  const int MAX_REQS = 100;
  const int MAX_DELAY = 1;
  const int DROP_PROBABILITY = 10;  // 1/x

  std::vector<replication::vr::VRClient *> clients;
  std::vector<int> lastReq;
  std::vector<replication::Client::continuation_t> upcalls;
  for (int i = 0; i < NUM_CLIENTS; i++) {
    clients.push_back(new replication::vr::VRClient(*config, transport, 0, i));
    lastReq.push_back(0);
    upcalls.emplace_back(
        [&, i](const std::string &req, const std::string &reply) {
          EXPECT_EQ("reply: " + RequestOp(lastReq[i]), reply);
          lastReq[i] += 1;
          if (lastReq[i] < MAX_REQS) {
            clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
          }
          return true;
        });
    clients[i]->Invoke(RequestOp(lastReq[i]), upcalls[i]);
  }

  srand(time(nullptr));
  uint32_t seedp;

  // Delay messages from clients by a random amount, and drop some
  // of them
  transport->AddFilter(
      10,
      [=, &seedp](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
                  int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        if (srcIdx == -1) {
          *delay = rand_r(&seedp) % MAX_DELAY;
        }
        return ((rand_r(&seedp) % DROP_PROBABILITY) != 0);
      });

  // This could take a while; simulate two hours
  transport->Timer(7200000, [&]() { transport->CancelAllTimers(); });

  transport->Run();

  for (int i = 0; i < config->n; i++) {
    UW_ASSERT_EQ(NUM_CLIENTS * MAX_REQS, ops[i].size());
  }

  for (int i = 0; i < NUM_CLIENTS * MAX_REQS; i++) {
    for (int j = 0; j < config->n; j++) {
      UW_ASSERT_EQ(ops[0][i], ops[j][i]);
    }
  }

  for (replication::vr::VRClient *c : clients) {
    delete c;
  }
}

INSTANTIATE_TEST_CASE_P(Batching, VRTest, ::testing::Values(1, 8));
