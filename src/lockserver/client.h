// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lockserver/client.h:
 *   A lockserver client interface.
 *
 * Copyright 2015 Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#ifndef LOCKSERVER_CLIENT_H_
#define LOCKSERVER_CLIENT_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <map>
#include <random>
#include <set>
#include <string>
#include <thread>  // NOLINT[build/c++11]

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lockserver/locks-proto.pb.h"
#include "replication/common/client.h"
#include "replication/ir/client.h"
#include "store/common/promise.h"

class Promise;
class Transport;
namespace transport {
class Configuration;
}  // namespace transport

namespace replication {
namespace ir {
class IRClient;
}  // namespace ir
}  // namespace replication

namespace lockserver {

class LockClient {
 public:
  LockClient(Transport *transport, const transport::Configuration &config);
  ~LockClient();

  // Synchronously lock and unlock. Calling lock (or unlock) will block until
  // the lock (or unlock) request is fully processed.
  bool lock(const std::string &key);
  void unlock(const std::string &key);

  // Asynchronously lock and unlock. Calling lock_async or unlock_async will
  // not block. Calling lock_wait (or unlock_wait) will block for the
  // previous invocation of lock_async (or unlock_async) to complete.
  //
  // All async calls must be followed by a corresponding wait call. It is an
  // error to issue multiple async requests without waiting. It is also
  // erroneous to wait for a request which was never issued.
  void lock_async(const std::string &key);
  bool lock_wait();
  void unlock_async(const std::string &key);
  void unlock_wait();

 private:
  /* Unique ID for this client. */
  uint64_t client_id;

  /* Transport layer and thread. */
  Transport *transport;

  /* Function to run the transport thread. */
  void run_client();

  /* Decide function for a lock server. */
  string Decide(const std::map<string, std::size_t> &results);

  /* IR client proxy. */
  replication::ir::IRClient *client;

  /* Promise to wait for pending operation. */
  Promise *waiting = nullptr;

  /* Callbacks for hearing back for an operation. */
  bool LockCallback(const std::string &, const std::string &);
  bool UnlockCallback(const std::string &, const std::string &);
  bool ErrorCallback(const std::string &, replication::ErrorCode);
};

}  // namespace lockserver

#endif  // LOCKSERVER_CLIENT_H_
