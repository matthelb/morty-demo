// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * benchmark.h:
 *   simple replication benchmark client
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
#ifndef STORE_BENCHMARK_BENCH_CLIENT_H_
#define STORE_BENCHMARK_BENCH_CLIENT_H_

#include <bits/std_function.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <bits/types/struct_timeval.h>

#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/transport.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"
#include "store/common/stats.h"

class AppContext;
class Transport;

using bench_done_callback = std::function<void()>;

class BenchClient {
 public:
  BenchClient(::context::Client *context_client, Transport &transport,
              uint64_t id, int numRequests, int expDuration, uint64_t delay,
              int warmupSec, int cooldownSec, int tputInterval,
              uint64_t abortBackoff, bool retryAborted, uint64_t maxBackoff,
              int64_t maxAttempts, bool output_measurements = false,
              std::string latencyFilename = "");
  virtual ~BenchClient();

  void Start(bench_done_callback bdcb);
  void OnReply(int result);

  void StartLatency();
  void SendNext();
  void IncrementSent(int result);
  inline bool IsFullyDone() { return done; }

  void NotifyCooldownStart();

  struct Latency_t latency {};
  bool started;
  bool done;
  bool cooldownStarted;
  std::vector<uint64_t> latencies;

  inline const Stats &GetStats() const { return stats; }
  virtual ::context::AsyncTransaction *GetNextContextTransaction(
      context::commit_callback ccb, context::abort_callback acb) = 0;

 protected:
  virtual std::string GetLastOp() const = 0;

  inline std::mt19937 &GetRand() { return rand; }

  enum BenchState { WARM_UP = 0, MEASURE = 1, COOL_DOWN = 2, DONE = 3 };
  BenchState GetBenchState(struct timeval *diff) const;
  BenchState GetBenchState() const;

  Stats stats;
  ::context::Client *context_client;
  Transport &transport;

 private:
  void CooldownStart();
  void WarmupDone();
  void CooldownDone();
  void TimeInterval();

  void ExecuteCallback(transaction_status_t result,
                       const ReadValueMap &readValues);

  void ContextExecuteCallback(std::unique_ptr<AppContext> context,
                              ::context::transaction_op_status_t status,
                              transaction_status_t result);

  const uint64_t id;
  int tputInterval;
  std::mt19937 rand;
  int numRequests;
  int expDuration;
  uint64_t delay;
  int n{};
  int warmupSec;
  int cooldownSec;
  struct timeval startTime {};
  struct timeval endTime {};
  struct timeval startMeasureTime {};
  int msSinceStart{};
  int opLastInterval{};
  bench_done_callback curr_bdcb;

  uint64_t maxBackoff;
  uint64_t abortBackoff;
  bool retryAborted;
  int64_t maxAttempts;
  ::context::AsyncTransaction *context_txn;
  uint64_t currTxnAttempts;

  const bool output_measurements_;
  string latencyFilename;
  std::uniform_int_distribution<uint64_t> randDelayDist;
};

#endif  // STORE_BENCHMARK_BENCH_CLIENT_H_
