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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * weakstore/weakclient.h
 *   Client-side module for talking to a single shard weak consistency storage
 *server
 *
 **********************************************************************/

#ifndef _WEAK_SHARDCLIENT_H_
#define _WEAK_SHARDCLIENT_H_

#include <bits/stdint-uintn.h>

#include <string>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/udptransport.h"
#include "store/common/frontend/txnclient.h"
#include "store/weakstore/weak-proto.pb.h"

class Promise;
namespace transport {
class Configuration;
}  // namespace transport

#define COMMIT_RETRIES 5

namespace weakstore {

class ShardClient : public TransportReceiver {
 public:
  ShardClient(const std::string &configPath, Transport *transport,
              uint64_t client_id, int shard, int closestReplica);
  ~ShardClient() override;

  void Get(uint64_t id, const std::string &key, Promise *promise);
  void Put(uint64_t id, const std::string &key, const std::string &value,
           Promise *promise);

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

 private:
  transport::Configuration *config;
  Transport *transport;  // Transport to replicas

  uint64_t client_id;
  int shard;
  int replica;

  Timeout *timeout;  // Timeout for general requests

  int totalReplies{};
  Promise *waiting;

  void RequestTimedOut();
};

}  // namespace weakstore

#endif /* _WEAK_SHARDCLIENT_H_ */
