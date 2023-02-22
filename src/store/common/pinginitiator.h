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
#ifndef PING_INITIATOR_H
#define PING_INITIATOR_H

#include <bits/stdint-uintn.h>

#include <ctime>
#include <map>
#include <random>
#include <utility>
#include <vector>

#include "lib/transport.h"
#include "store/common/common-proto.pb.h"

class Transport;

class PingTransport {
 public:
  PingTransport() = default;
  virtual ~PingTransport() = default;

  virtual bool SendPing(size_t replica, const PingMessage &ping) = 0;
};

class PingInitiator {
 public:
  PingInitiator(PingTransport *pingTransport, Transport *transport,
                size_t numReplicas);
  virtual ~PingInitiator();

  void StartPings();

 protected:
  inline const std::vector<size_t> &GetOrderedReplicas() const {
    return orderedReplicas;
  }

  void HandlePingResponse(const PingMessage &ping);

 private:
  void SendPing(size_t replica);

  static uint64_t timespec_delta(const struct timespec &a,
                                 const struct timespec &b);

  PingTransport *pingTransport;
  Transport *transport;
  const size_t numReplicas;
  const double alpha;
  const uint64_t length;

  bool done{};
  std::mt19937 rd;
  std::map<uint64_t, std::pair<size_t, struct timespec>> outstandingSalts;
  std::map<size_t, uint64_t> roundTripEstimates;

  std::vector<size_t> orderedReplicas;

  PingMessage ping;
};

#endif /* PING_INITIATOR_H */
