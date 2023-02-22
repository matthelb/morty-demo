// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/weakstore/client.h:
 *   Weak consistency store client-side logic and APIs
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#ifndef _WEAK_CLIENT_H_
#define _WEAK_CLIENT_H_

#include <bits/stdint-uintn.h>

#include <set>
#include <string>
#include <thread>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/common/client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "store/weakstore/shardclient.h"
#include "store/weakstore/weak-proto.pb.h"

namespace weakstore {

class ShardClient;

class Client {
 public:
  Client(const std::string &configPath, int nShards, int closestReplica,
         partitioner part);
  ~Client();

  // Begin a transaction.
  // virtual void Begin();

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
                   get_timeout_callback gtcb, uint32_t timeout = GET_TIMEOUT);

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
                   put_callback pcb, put_timeout_callback ptcb,
                   uint32_t timeout = PUT_TIMEOUT);

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout);

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
                     uint32_t timeout);

  virtual std::vector<int> Stats();

 private:
  /* Private helper functions. */
  void run_client();  // Runs the transport event loop.

  // Unique ID for this client.
  uint64_t client_id;

  // Number of shards in this deployment
  uint64_t nshards;

  // Transport used by shard clients.
  UDPTransport transport;

  partitioner part;

  // Thread running the transport event loop.
  std::thread *clientTransport;

  // Client for each shard.
  std::vector<ShardClient *> bclient;
};

}  // namespace weakstore

#endif /* _WEAK_CLIENT_H_ */
