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
//
// Created by Janice Chan on 9/24/19.
//

#ifndef MORTY_TAPIR_SMALLBANK_CLIENT_H
#define MORTY_TAPIR_SMALLBANK_CLIENT_H

#include <bits/stdint-uintn.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/tcptransport.h"
#include "lib/timeval.h"
#include "store/benchmark/bench_client.h"
#include "store/benchmark/smallbank/smallbank_client.h"
#include "store/benchmark/smallbank/smallbank_transaction.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/truetime.h"
#include "store/tapirstore/client.h"

class SyncTransaction;
class Transport;

namespace context {
class Client;
}  // namespace context

namespace smallbank {

constexpr int num_customers_ = 18000;

class SmallbankClient : public BenchClient {
 public:
  SmallbankClient(::context::Client *context_client, Transport &transport,
                  uint64_t id, int numRequests, int expDuration, uint64_t delay,
                  int warmupSec, int cooldownSec, int tputInterval,
                  uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff,
                  uint32_t maxAttempts, uint32_t timeout,
                  uint32_t balance_ratio, uint32_t deposit_checking_ratio,
                  uint32_t transact_saving_ratio, uint32_t amalgamate_ratio,
                  uint32_t num_hotspot_keys, uint32_t num_non_hotspot_keys,
                  double hotspot_probability,
                  const std::string &customer_name_file_path,
                  const std::string &latencyFilename = "");
  ~SmallbankClient() override;
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;
  std::string GetCustomerKey();
  std::pair<string, string> GetCustomerKeyPair();
  void SetCustomerKeys(std::vector<std::string> keys);

 protected:
 private:
  uint32_t timeout_;
  uint32_t balance_ratio_;
  uint32_t deposit_checking_ratio_;
  uint32_t transact_saving_ratio_;
  uint32_t amalgamate_ratio_;
  uint32_t num_hotspot_keys_;
  uint32_t num_non_hotspot_keys_;
  double hotspot_probability_;
  std::vector<std::string> all_keys_;
  std::string last_op_;
};

}  // namespace smallbank

#endif  // MORTY_TAPIR_SMALLBANK_CLIENT_H
