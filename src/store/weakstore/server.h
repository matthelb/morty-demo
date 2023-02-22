// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/weakstore/server.h:
 *   Storage server executable and dispatch code
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#ifndef _WEAK_SERVER_H_
#define _WEAK_SERVER_H_

#include <string>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/udptransport.h"
#include "store/common/stats.h"
#include "store/server.h"
#include "store/weakstore/store.h"
#include "store/weakstore/weak-proto.pb.h"

class Timestamp;

namespace weakstore {

namespace proto {
class GetMessage;
class PutMessage;
}  // namespace proto

class Server : TransportReceiver, public ::Server {
 private:
  // Underlying single node transactional key-value store.
  Store store;

  // Configuration of replicas.
  transport::Configuration configuration;

  // Index of 'this' replica, and handle to transport layer.
  Transport *transport;

 public:
  Server(const transport::Configuration &configuration, int groupIdx, int myIdx,
         Transport *transport);
  ~Server() override;

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  void HandleMessage(const TransportAddress &remote, const std::string &type,
                     const std::string &data);
  void HandleGet(const TransportAddress &remote, const proto::GetMessage &msg);
  void HandlePut(const TransportAddress &remote, const proto::PutMessage &msg);

  void Load(std::string &&key, std::string &&value,
            Timestamp &&timestamp) override;
  inline Stats &GetStats() override { return stats; }

 private:
  Stats stats;
};

}  // namespace weakstore

#endif /* _WEAK_SERVER_H_ */
