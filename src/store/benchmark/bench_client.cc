// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 5 -*-
/***********************************************************************
 *
 * benchmark.cpp:
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

#include "store/benchmark/bench_client.h"

#include <bits/types/struct_timespec.h>
#include <sys/time.h>
#include <time.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/timeval.h"
#include "lib/transport.h"
#include "store/common/frontend/appcontext.h"

DEFINE_LATENCY(op);

BenchClient::BenchClient(::context::Client *context_client,
                         Transport &transport, uint64_t id, int numRequests,
                         int expDuration, uint64_t delay, int warmupSec,
                         int cooldownSec, int tputInterval,
                         uint64_t abortBackoff, bool retryAborted,
                         uint64_t maxBackoff, int64_t maxAttempts,
                         bool output_measurements, std::string latencyFilename)
    : context_client(context_client),
      transport(transport),
      id(id),
      tputInterval(tputInterval),
      rand(id),
      numRequests(numRequests),
      expDuration(expDuration),
      delay(delay),
      warmupSec(warmupSec),
      cooldownSec(cooldownSec),
      maxBackoff(maxBackoff),
      abortBackoff(abortBackoff),
      retryAborted(retryAborted),
      maxAttempts(maxAttempts),
      context_txn(nullptr),
      currTxnAttempts(0UL),
      output_measurements_(output_measurements),
      latencyFilename(std::move(latencyFilename)),
      randDelayDist(0, delay * 1000) {
  if (delay != 0) {
    Notice("Delay between requests: %ld ms", delay);
  } else {
    Notice("No delay between requests.");
  }
  started = false;
  done = false;
  cooldownStarted = false;
  if (numRequests > 0) {
    latencies.reserve(numRequests);
  }
  _Latency_Init(&latency, "txn");
}

BenchClient::~BenchClient() = default;

void BenchClient::Start(bench_done_callback bdcb) {
  n = 0;
  curr_bdcb = std::move(bdcb);
  transport.Timer(warmupSec * 1000, std::bind(&BenchClient::WarmupDone, this));

  if (numRequests == -1) {
    transport.Timer((expDuration - cooldownSec) * 1000,
                    std::bind(&BenchClient::CooldownStart, this));
  }

  gettimeofday(&startTime, nullptr);

  if (tputInterval > 0) {
    msSinceStart = 0;
    opLastInterval = n;
    transport.Timer(tputInterval, std::bind(&BenchClient::TimeInterval, this));
  }
  Latency_Start(&latency);
  SendNext();
}

void BenchClient::TimeInterval() {
  if (done) {
    return;
  }

  struct timeval tv {};
  gettimeofday(&tv, nullptr);
  msSinceStart += tputInterval;
  Notice("Completed %d requests at %lu ms", n - opLastInterval,
         (((tv.tv_sec * 1000000 + tv.tv_usec) / 1000) / 10) * 10);
  opLastInterval = n;
  transport.Timer(tputInterval, std::bind(&BenchClient::TimeInterval, this));
}

void BenchClient::WarmupDone() {
  started = true;
  Notice("Completed warmup period of %d seconds with %d requests", warmupSec,
         n);
  n = 0;
}

void BenchClient::CooldownDone() {
  Debug("Finished cooldown after %d seconds.", cooldownSec);

  done = true;

  char buf[1024];
  Notice("Finished cooldown period.");
  std::sort(latencies.begin(), latencies.end());

  if (!latencies.empty()) {
    uint64_t ns = latencies[latencies.size() / 2];
    LatencyFmtNS(ns, buf, sizeof(buf));
    Notice("Median latency is %ld ns (%s)", ns, buf);

    ns = 0;
    for (auto latency : latencies) {
      ns += latency;
    }
    ns = ns / latencies.size();
    LatencyFmtNS(ns, buf, sizeof(buf));
    Notice("Average latency is %ld ns (%s)", ns, buf);

    ns = latencies[latencies.size() * 90 / 100];
    LatencyFmtNS(ns, buf, sizeof(buf));
    Notice("90th percentile latency is %ld ns (%s)", ns, buf);

    ns = latencies[latencies.size() * 95 / 100];
    LatencyFmtNS(ns, buf, sizeof(buf));
    Notice("95th percentile latency is %ld ns (%s)", ns, buf);

    ns = latencies[latencies.size() * 99 / 100];
    LatencyFmtNS(ns, buf, sizeof(buf));
    Notice("99th percentile latency is %ld ns (%s)", ns, buf);
  }
  curr_bdcb();
}

void BenchClient::OnReply(int result) {
  IncrementSent(result);

  if (cooldownStarted || done) {
    return;
  }

  if (delay == 0) {
    Latency_Start(&latency);
    SendNext();
  } else {
    uint64_t rdelay = randDelayDist(rand);
    transport.TimerMicro(rdelay, std::bind(&BenchClient::SendNext, this));
  }
}

void BenchClient::StartLatency() { Latency_Start(&latency); }

void BenchClient::IncrementSent(int result) {
  if (started) {
    struct timeval diff {};
    // TODO(matthelb): is state undefined if we are running until a certain # of
    // requests
    //   instead of a certain amount of time?
    BenchState state = GetBenchState(&diff);

    // record latency
    if (!cooldownStarted) {
      uint64_t ns = Latency_End(&latency);
      // TODO(matthelb): use standard definitions across all clients for
      // success/commit and failure/abort
      if (state == MEASURE && result == 0) {  // only record result if success
        struct timespec curr {};
        clock_gettime(CLOCK_REALTIME, &curr);
        if (latencies.empty()) {
          gettimeofday(&startMeasureTime, nullptr);
          startMeasureTime.tv_sec -= ns / 1000000000ULL;
          startMeasureTime.tv_usec -= (ns % 1000000000ULL) / 1000ULL;
          // std::cout << "#start," << startMeasureTime.tv_sec << "," <<
          // startMeasureTime.tv_usec << std::endl;
        }
        uint64_t currNanos = curr.tv_sec * 1000000000ULL + curr.tv_nsec;
        if (output_measurements_) {
          std::cout << GetLastOp() << ',' << ns << ',' << currNanos << ',' << id
                    << std::endl;
        }
        latencies.push_back(ns);
      }
    }

    if (numRequests == -1) {
      // Debug("Not done after %ld seconds.", diff.tv_sec);
    } else if (n >= numRequests) {
      CooldownDone();
    }
  }
  n++;
}

BenchClient::BenchState BenchClient::GetBenchState(struct timeval *diff) const {
  struct timeval currTime {};
  gettimeofday(&currTime, nullptr);

  *diff = timeval_sub(currTime, startTime);
  if (diff->tv_sec >= expDuration) {
    return DONE;
  }
  if (diff->tv_sec >= expDuration - cooldownSec) {
    return COOL_DOWN;
  }
  if (started) {
    return MEASURE;
  } else {
    return WARM_UP;
  }
}

BenchClient::BenchState BenchClient::GetBenchState() const {
  struct timeval diff {};
  return GetBenchState(&diff);
}

void BenchClient::CooldownStart() {
  gettimeofday(&endTime, nullptr);
  struct timeval diff = timeval_sub(endTime, startMeasureTime);

  Debug("Starting cooldown after %ld seconds.", diff.tv_sec);

  std::cout << "#end," << diff.tv_sec << "," << diff.tv_usec << "," << id
            << std::endl;

  Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds", n,
         VA_TIMEVAL_DIFF(diff));

  if (!latencyFilename.empty()) {
    Latency_FlushTo(latencyFilename.c_str());
  }

  NotifyCooldownStart();

  if (numRequests == -1) {
    cooldownStarted = true;
    transport.Timer(cooldownSec * 1000,
                    std::bind(&BenchClient::CooldownDone, this));
  } else {
    CooldownDone();
  }
}

void BenchClient::NotifyCooldownStart() {
  UW_ASSERT(context_client != nullptr);
  context_client->Cleanup();
}

void BenchClient::SendNext() {
  UW_ASSERT(context_client != nullptr);

  delete context_txn;

  auto ccb = std::bind(&BenchClient::ContextExecuteCallback, this,
                       std::placeholders::_1, std::placeholders::_2,
                       std::placeholders::_3);
  auto acb = [this](std::unique_ptr<AppContext> ctx) {
    ContextExecuteCallback(std::move(ctx), context::OK, ABORTED_SYSTEM);
  };
  context_txn = GetNextContextTransaction(ccb, acb);
  stats.Increment(GetLastOp() + "_attempts", 1);
  currTxnAttempts = 0;

  Latency_Start(&latency);

  context_txn->Execute(context_client, 10000);
}

void BenchClient::ExecuteCallback(transaction_status_t result,
                                  const ReadValueMap &readValues) {
  Debug("ExecuteCallback with result %d.", result);
  ++currTxnAttempts;
  if (result == COMMITTED || result == ABORTED_USER ||
      (maxAttempts != -1 &&
       currTxnAttempts >= static_cast<uint64_t>(maxAttempts)) ||
      !retryAborted) {
    if (result == COMMITTED) {
      stats.Increment(GetLastOp() + "_committed", 1);
    }
    if (result == ABORTED_USER) {
      stats.Increment(GetLastOp() + "_aborted_user", 1);
    }
    if (retryAborted) {
      // stats.Add(GetLastOp() + "_attempts_list", currTxnAttempts);  //TODO:
      // uncomment if want to collect attempt stats
    }
    OnReply(result);
  } else {
    stats.Increment(GetLastOp() + "_" + std::to_string(result), 1);
    BenchClient::BenchState state = GetBenchState();
    Debug("Current bench state: %d.", state);
    if (state == DONE) {
      OnReply(ABORTED_SYSTEM);
    } else {
      uint64_t backoff = 0;
      if (abortBackoff > 0) {
        uint64_t exp = std::min(currTxnAttempts - 1UL, 56UL);
        Debug("Exp is %lu (min of %lu and 56.", exp, currTxnAttempts - 1UL);
        uint64_t upper = std::min((1UL << exp) * abortBackoff, maxBackoff);
        Debug("Upper is %lu (min of %lu and %lu.", upper,
              (1UL << exp) * abortBackoff, maxBackoff);
        backoff =
            std::uniform_int_distribution<uint64_t>(0UL, upper)(GetRand());
        stats.Increment(GetLastOp() + "_backoff", backoff);
        Debug("Backing off for %lums", backoff);
      }
      transport.TimerMicro(backoff, [this]() {
        stats.Increment(GetLastOp() + "_attempts", 1);
        context_txn->Retry(context_client, 10000);
      });
    }
  }
}

void BenchClient::ContextExecuteCallback(
    std::unique_ptr<AppContext> context,
    ::context::transaction_op_status_t status, transaction_status_t result) {
  ExecuteCallback(result, ReadValueMap());
}
