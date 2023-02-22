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
#ifndef COMMON_MESSAGE_SERVER_H
#define COMMON_MESSAGE_SERVER_H

#include <string>
#include <unordered_map>
#include <utility>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "store/common/stats.h"
#include "store/server.h"

namespace google {
namespace protobuf {
class Message;
}  // namespace protobuf
}  // namespace google

class MessageServer : public TransportReceiver, public ::Server {
 public:
  MessageServer();
  ~MessageServer() override;

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  inline Stats &GetStats() override { return stats; };

 protected:
  using MessageHandler = void (MessageServer::*)(const TransportAddress &,
                                                 google::protobuf::Message *);

  void RegisterHandler(google::protobuf::Message *message,
                       MessageServer::MessageHandler handler);

  Stats stats;

 private:
  std::unordered_map<std::string, std::pair<google::protobuf::Message *,
                                            MessageServer::MessageHandler>>
      msgHandlers;
};

#endif /* COMMON_MESSAGE_SERVER_H */
