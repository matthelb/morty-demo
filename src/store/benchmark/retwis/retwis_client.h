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
#ifndef STORE_BENCHMARK_RETWIS_RETWIS_CLIENT_H_
#define STORE_BENCHMARK_RETWIS_RETWIS_CLIENT_H_

#include <bits/stdint-uintn.h>

#include <string>
#include <vector>

#include "lib/configuration.h"
#include "store/benchmark/bench_client.h"
#include "store/benchmark/common/key_selector.h"
#include "store/benchmark/retwis/retwis_transaction.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

class AsyncClient;
class KeySelector;
class Transport;

namespace context {
class AsyncTransaction;
}  // namespace context

namespace retwis {

enum KeySelection { UNIFORM, ZIPF };

class RetwisClient : public BenchClient {
 public:
  RetwisClient(const std::vector<KeySelector *> &keySelector, bool single_shard,
               AsyncClient *client, ::context::Client *context_client,
               Transport &transport, uint64_t id, int numRequests,
               int expDuration, uint64_t delay, int warmupSec, int cooldownSec,
               int tputInterval, uint32_t abortBackoff, bool retryAborted,
               uint32_t maxBackoff, uint32_t maxAttempts,
               bool output_measurements = true,
               const std::string &latencyFilename = "");

  ~RetwisClient() override;

 protected:
  ::context::AsyncTransaction *GetNextContextTransaction(
      context::commit_callback ccb, context::abort_callback acb) override;
  std::string GetLastOp() const override;

 private:
  const std::vector<KeySelector *> &keySelector;
  const bool single_shard_;
  std::string lastOp;
};

}  // namespace retwis

#endif  // STORE_BENCHMARK_RETWIS_CLIENT_H_
