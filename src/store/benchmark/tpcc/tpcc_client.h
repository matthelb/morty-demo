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
#ifndef TPCC_CLIENT_H
#define TPCC_CLIENT_H

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <random>
#include <string>

#include "lib/configuration.h"
#include "store/benchmark/bench_client.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

class Transport;

namespace context {
class AsyncTransaction;
}  // namespace context

namespace tpcc {

enum TPCCTransactionType {
  TXN_NEW_ORDER = 0,
  TXN_PAYMENT,
  TXN_ORDER_STATUS,
  TXN_STOCK_LEVEL,
  TXN_DELIVERY,
  NUM_TXN_TYPES
};

class TPCCClient : public BenchClient {
 public:
  TPCCClient(::context::Client *context_client, Transport &transport,
             uint64_t id, int numRequests, int expDuration, uint64_t delay,
             int warmupSec, int cooldownSec, int tputInterval,
             uint64_t abortBackoff, bool retryAborted, uint64_t maxBackoff,
             int64_t maxAttempts, uint32_t num_warehouses, uint32_t w_id,
             uint32_t C_c_id, uint32_t C_c_last, uint32_t new_order_ratio,
             uint32_t delivery_ratio, uint32_t payment_ratio,
             uint32_t order_status_ratio, uint32_t stock_level_ratio,
             bool static_w_id, bool user_aborts, bool distributed_txns,
             bool output_measurements = false,
             std::string latencyFilename = "");

  ~TPCCClient() override;

 protected:
  virtual TPCCTransactionType GetNextTransaction(uint32_t *wid, uint32_t *did,
                                                 std::mt19937 &gen);
  std::string GetLastTransaction() const;

  ::context::AsyncTransaction *GetNextContextTransaction(
      context::commit_callback ccb, context::abort_callback acb) override;
  virtual std::string GetLastOp() const { return GetLastTransaction(); }

  const uint32_t num_warehouses;
  uint32_t w_id;
  const uint32_t C_c_id;
  const uint32_t C_c_last;
  const uint32_t new_order_ratio;
  const uint32_t delivery_ratio;
  const uint32_t payment_ratio;
  const uint32_t order_status_ratio;
  const uint32_t stock_level_ratio;
  const bool static_w_id;
  const bool user_aborts;
  const bool distributed_txns;
  uint32_t stockLevelDId;
  std::string lastOp;

 private:
  bool delivery;
  uint32_t deliveryWId{};
  uint32_t deliveryDId{};
};

}  // namespace tpcc

#endif /* TPCC_CLIENT_H */
