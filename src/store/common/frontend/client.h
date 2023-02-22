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
 * common/client.h:
 *   Interface for a multiple shard transactional client.
 *
 **********************************************************************/

#ifndef _CLIENT_API_H_
#define _CLIENT_API_H_

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"

class Timestamp;

enum transaction_status_t {
  COMMITTED = 0,
  ABORTED_USER,
  ABORTED_SYSTEM,
  ABORTED_MAX_RETRIES
};

using begin_callback = std::function<void(uint64_t)>;
using begin_timeout_callback = std::function<void()>;

using get_callback = std::function<void(int, const std::string &,
                                        const std::string &, Timestamp)>;
using get_timeout_callback = std::function<void(int, const std::string &)>;

using put_callback =
    std::function<void(int, const std::string &, const std::string &)>;
using put_timeout_callback =
    std::function<void(int, const std::string &, const std::string &)>;

using commit_callback = std::function<void(transaction_status_t)>;
using commit_timeout_callback = std::function<void()>;

using abort_callback = std::function<void()>;
using abort_timeout_callback = std::function<void()>;

class Stats;

class Client {
 public:
  Client() { _Latency_Init(&clientLat, "client_lat"); }
  virtual ~Client() = default;

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
                     uint32_t timeout, bool retry = false) = 0;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
                   get_timeout_callback gtcb, uint32_t timeout) = 0;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
                   put_callback pcb, put_timeout_callback ptcb,
                   uint32_t timeout) = 0;

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) = 0;

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
                     uint32_t timeout) = 0;

  inline Stats &GetStats() { return stats; }

 protected:
  void StartRecLatency();
  void EndRecLatency(const std::string &str);
  Stats stats;

 private:
  Latency_t clientLat{};
};

namespace context {

enum transaction_op_status_t {
  OK = 0,
  FAIL = 1,
  RETRY = 2,
  TIMEOUT = 4,
  NETWORK_FAILURE = 5,
  WAITING = 6
};

using begin_callback = std::function<void(std::unique_ptr<AppContext>)>;

using op_callback =
    std::function<void(std::unique_ptr<AppContext>, transaction_op_status_t,
                       const std::string &, const std::string &)>;

using bulk_op_callback =
    std::function<void(std::unique_ptr<AppContext>, transaction_op_status_t,
                       const std::unordered_map<std::string, std::string> &)>;

using commit_callback =
    std::function<void(std::unique_ptr<AppContext>, transaction_op_status_t,
                       transaction_status_t)>;

using abort_callback = std::function<void(std::unique_ptr<AppContext>)>;

class Client {
 public:
  Client() = default;
  virtual ~Client() = default;

  virtual void Begin(std::unique_ptr<AppContext> &context,
                     abort_callback acb) = 0;
  virtual void Retry(std::unique_ptr<AppContext> &context, abort_callback acb) {
    Begin(context, acb);
  }

  virtual void Get(std::unique_ptr<AppContext> context, const std::string &key,
                   op_callback ocb) = 0;

  virtual void GetForUpdate(std::unique_ptr<AppContext> context,
                            const std::string &key, op_callback ocb) {
    return Get(std::move(context), key, ocb);
  }

  virtual void Put(std::unique_ptr<AppContext> &context, const std::string &key,
                   const std::string &val) = 0;

  virtual void Commit(std::unique_ptr<AppContext> context,
                      commit_callback ccb) = 0;

  virtual void Abort(std::unique_ptr<AppContext> &context) = 0;

  virtual bool SupportsRO() { return false; }
  virtual void ROBegin(std::unique_ptr<AppContext> &context,
                       abort_callback acb) {
    Panic("ROBegin not implemented!");
  }
  virtual void ROGet(std::unique_ptr<AppContext> context,
                     const std::unordered_set<std::string> &keys,
                     bulk_op_callback ocb) {
    Panic("ROGet not implemented!");
  }
  virtual void ROCommit(std::unique_ptr<AppContext> context,
                        commit_callback ccb) {
    Panic("ROCommit not implemented!");
  }

  virtual void Cleanup() { Debug("Cleanup not implemented."); }

  inline Stats &GetStats() { return stats; }

 private:
  Stats stats;
};

}  // namespace context

#endif /* _CLIENT_API_H_ */
