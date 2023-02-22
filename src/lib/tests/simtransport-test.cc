// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * simtransport-test.h:
 *   test cases for simulated network transport
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

#include "lib/simtransport.h"

#include <bits/stdint-uintn.h>
#include <google/protobuf/message.h>

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest-message.h"
#include "gtest/gtest-test-part.h"
#include "gtest/gtest_pred_impl.h"
#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/tests/simtransport-testmessage.pb.h"
#include "lib/transport.h"

class TestReceiver : public TransportReceiver {
 public:
  TestReceiver();
  void ReceiveMessage(const TransportAddress &src, string &type, string &data,
                      void *meta_data) override;

  int numReceived;
  transport::test::TestMessage lastMsg;
};

TestReceiver::TestReceiver() { numReceived = 0; }

void TestReceiver::ReceiveMessage(const TransportAddress &src, string &type,
                                  string &data, void *meta_data) {
  UW_ASSERT_EQ(type, lastMsg.GetTypeName());
  lastMsg.ParseFromString(data);
  numReceived++;
}

class SimTransportTest : public testing::Test {
 protected:
  std::vector<transport::ReplicaAddress> replicaAddrs;

  transport::Configuration *config{};

  TestReceiver *receiver0{};
  TestReceiver *receiver1{};
  TestReceiver *receiver2{};

  SimulatedTransport *transport{};

  void SetUp() override {
    replicaAddrs.push_back(
        *(new transport::ReplicaAddress("localhost", "12345")));
    replicaAddrs.push_back(
        *(new transport::ReplicaAddress("localhost", "12346")));
    replicaAddrs.push_back(
        *(new transport::ReplicaAddress("localhost", "12347")));

    receiver0 = new TestReceiver();
    receiver1 = new TestReceiver();
    receiver2 = new TestReceiver();

    std::map<int, std::vector<transport::ReplicaAddress>> replicas;
    replicas[0] = replicaAddrs;
    config = new transport::Configuration(1, 3, 1, replicas);
    transport = new SimulatedTransport();
    transport->Register(receiver0, *config, 0);
    transport->Register(receiver1, *config, 1);
    transport->Register(receiver2, *config, 2);
  }

  void TearDown() override {
    delete receiver0;
    delete receiver1;
    delete receiver2;
    delete transport;
  }
};

TEST_F(SimTransportTest, Basic) {
  transport::test::TestMessage msg;
  msg.set_test("foo");

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 1);
  EXPECT_EQ(receiver2->numReceived, 0);
  EXPECT_EQ(receiver1->lastMsg.test(), "foo");

  transport::test::TestMessage msg2;
  msg2.set_test("bar");

  transport->SendMessageToAll(receiver0, msg2);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 2);
  EXPECT_EQ(receiver2->numReceived, 1);
  EXPECT_EQ(receiver1->lastMsg.test(), "bar");
  EXPECT_EQ(receiver2->lastMsg.test(), "bar");
}

TEST_F(SimTransportTest, Filter) {
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m,
             uint64_t *delay) { return dstIdx != 1; });

  transport::test::TestMessage msg;
  msg.set_test("foo");

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 0);
  EXPECT_EQ(receiver2->numReceived, 0);

  transport::test::TestMessage msg2;
  msg2.set_test("bar");

  transport->SendMessageToAll(receiver0, msg2);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 0);
  EXPECT_EQ(receiver2->numReceived, 1);
  EXPECT_EQ(receiver2->lastMsg.test(), "bar");

  transport->RemoveFilter(10);

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 1);
  EXPECT_EQ(receiver2->numReceived, 1);
  EXPECT_EQ(receiver1->lastMsg.test(), "foo");
  EXPECT_EQ(receiver2->lastMsg.test(), "bar");
}

TEST_F(SimTransportTest, FilterModify) {
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        auto &tm = dynamic_cast<transport::test::TestMessage &>(m);
        tm.set_test("baz");
        return true;
      });

  transport::test::TestMessage msg;
  msg.set_test("foo");

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 1);
  EXPECT_EQ(receiver2->numReceived, 0);
  EXPECT_EQ(receiver1->lastMsg.test(), "baz");

  transport::test::TestMessage msg2;
  msg2.set_test("bar");

  transport->SendMessageToAll(receiver0, msg2);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 2);
  EXPECT_EQ(receiver2->numReceived, 1);
  EXPECT_EQ(receiver1->lastMsg.test(), "baz");
  EXPECT_EQ(receiver2->lastMsg.test(), "baz");
}

TEST_F(SimTransportTest, FilterDelay) {
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        auto &tm = dynamic_cast<transport::test::TestMessage &>(m);
        if (tm.test() == "foo") {
          *delay = 1000;
        }
        return true;
      });

  transport::test::TestMessage msg;
  msg.set_test("foo");

  transport->SendMessageToAll(receiver0, msg);

  transport::test::TestMessage msg2;
  msg2.set_test("bar");

  transport->SendMessageToAll(receiver0, msg2);

  transport->Run();

  // We should have received both messages, but the first was
  // delayed, so it should be our last message.
  EXPECT_EQ(receiver1->numReceived, 2);
  EXPECT_EQ(receiver2->numReceived, 2);
  EXPECT_EQ(receiver1->lastMsg.test(), "foo");
  EXPECT_EQ(receiver2->lastMsg.test(), "foo");
}

TEST_F(SimTransportTest, FilterPriority) {
  transport->AddFilter(
      10, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        auto &tm = dynamic_cast<transport::test::TestMessage &>(m);
        return tm.test() != "foo";
      });

  transport->AddFilter(
      20, [](TransportReceiver *src, int srcIdx, TransportReceiver *dst,
             int dstIdx, google::protobuf::Message &m, uint64_t *delay) {
        auto &tm = dynamic_cast<transport::test::TestMessage &>(m);
        tm.set_test("baz");
        return true;
      });

  transport::test::TestMessage msg;
  msg.set_test("foo");

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 0);
  EXPECT_EQ(receiver2->numReceived, 0);

  transport::test::TestMessage msg2;
  msg2.set_test("bar");

  transport->SendMessageToAll(receiver0, msg2);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 1);
  EXPECT_EQ(receiver2->numReceived, 1);
  EXPECT_EQ(receiver1->lastMsg.test(), "baz");
  EXPECT_EQ(receiver2->lastMsg.test(), "baz");
}

TEST_F(SimTransportTest, Timer) {
  transport::test::TestMessage msg;
  msg.set_test("foo");
  bool firstTimerCalled = false;
  bool secondTimerCalled = false;

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  transport->Timer(20, [&]() {
    EXPECT_TRUE(firstTimerCalled);
    EXPECT_FALSE(secondTimerCalled);
    EXPECT_EQ(receiver0->numReceived, 0);
    EXPECT_EQ(receiver1->numReceived, 1);
    EXPECT_EQ(receiver2->numReceived, 0);
    secondTimerCalled = true;
  });
  transport->Timer(10, [&]() {
    EXPECT_FALSE(firstTimerCalled);
    EXPECT_FALSE(secondTimerCalled);
    EXPECT_EQ(receiver0->numReceived, 0);
    EXPECT_EQ(receiver1->numReceived, 1);
    EXPECT_EQ(receiver2->numReceived, 0);
    firstTimerCalled = true;
  });
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 1);
  EXPECT_EQ(receiver2->numReceived, 0);
  EXPECT_EQ(receiver1->lastMsg.test(), "foo");
  EXPECT_TRUE(firstTimerCalled);
  EXPECT_TRUE(secondTimerCalled);
}

TEST_F(SimTransportTest, TimerCancel) {
  transport::test::TestMessage msg;
  msg.set_test("foo");
  bool firstTimerCalled = false;
  bool secondTimerCalled = false;
  int id2;

  transport->SendMessageToReplica(receiver0, 0, 1, msg);
  id2 = transport->Timer(20, [&]() { secondTimerCalled = true; });
  transport->Timer(10, [&]() {
    EXPECT_FALSE(firstTimerCalled);
    EXPECT_FALSE(secondTimerCalled);
    EXPECT_EQ(receiver0->numReceived, 0);
    EXPECT_EQ(receiver1->numReceived, 1);
    EXPECT_EQ(receiver2->numReceived, 0);
    firstTimerCalled = true;
  });
  bool found = transport->CancelTimer(id2);
  EXPECT_TRUE(found);
  transport->Run();

  EXPECT_EQ(receiver0->numReceived, 0);
  EXPECT_EQ(receiver1->numReceived, 1);
  EXPECT_EQ(receiver2->numReceived, 0);
  EXPECT_EQ(receiver1->lastMsg.test(), "foo");
  EXPECT_TRUE(firstTimerCalled);
  EXPECT_FALSE(secondTimerCalled);
}

TEST_F(SimTransportTest, Timeout) {
  int n = 0;

  Timeout t(transport, 1000, [&]() {
    n++;
    if (n == 2) {
      t.Stop();
    }
  });

  transport->Run();
  EXPECT_EQ(0, n);

  t.Start();
  transport->Run();
  EXPECT_EQ(2, n);

  t.Start();
  t.Stop();
  transport->Run();
  EXPECT_EQ(2, n);
}
