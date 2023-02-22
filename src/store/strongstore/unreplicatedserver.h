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
#ifndef STRONGSTORE_UNREPLICATED_SERVER_H
#define STRONGSTORE_UNREPLICATED_SERVER_H

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <string>
#include <unordered_map>
#include <utility>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "store/common/backend/messageserver.h"
#include "store/common/backend/txnstore.h"
#include "store/strongstore/common.h"
#include "store/strongstore/strong-proto.pb.h"

class Stats;
class Timestamp;
class Transport;
class TransportAddress;
namespace transport {
class Configuration;
}  // namespace transport

namespace google {
namespace protobuf {
class Message;
}  // namespace protobuf
}  // namespace google

namespace strongstore {

class UnreplicatedServer : public MessageServer {
 public:
  UnreplicatedServer(Mode mode, Transport *transport,
                     transport::Configuration &config, uint64_t groupIdx,
                     int64_t maxDepDepth);
  ~UnreplicatedServer() override;

  void Load(std::string &&key, std::string &&val, Timestamp &&ts) override;

  inline Stats &GetStats() override { return store->GetStats(); }

 private:
  void HandleRequest(const TransportAddress &remote,
                     google::protobuf::Message *m);

  Transport *transport;
  TxnStore *store;
  proto::Request request;
  proto::Reply reply;
  std::unordered_map<uint64_t, std::pair<uint64_t, const TransportAddress *>>
      waitingPrepares;
};

}  // namespace strongstore

#endif /* STRONGSTORE_UNREPLICATED_SERVER_H */
