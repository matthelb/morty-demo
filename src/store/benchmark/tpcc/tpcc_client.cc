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
#include "store/benchmark/tpcc/tpcc_client.h"

#include <cstdint>
#include <random>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/benchmark/tpcc/delivery.h"
#include "store/benchmark/tpcc/new_order.h"
#include "store/benchmark/tpcc/order_status.h"
#include "store/benchmark/tpcc/payment.h"
#include "store/benchmark/tpcc/stock_level.h"
#include "store/common/frontend/async_transaction.h"

class Transport;

namespace tpcc {

TPCCClient::TPCCClient(::context::Client *context_client, Transport &transport,
                       uint64_t id, int numRequests, int expDuration,
                       uint64_t delay, int warmupSec, int cooldownSec,
                       int tputInterval, uint64_t abortBackoff,
                       bool retryAborted, uint64_t maxBackoff,
                       int64_t maxAttempts, uint32_t num_warehouses,
                       uint32_t w_id, uint32_t C_c_id, uint32_t C_c_last,
                       uint32_t new_order_ratio, uint32_t delivery_ratio,
                       uint32_t payment_ratio, uint32_t order_status_ratio,
                       uint32_t stock_level_ratio, bool static_w_id,
                       bool user_aborts, bool distributed_txns,
                       bool output_measurements, std::string latencyFilename)
    : BenchClient(context_client, transport, id, numRequests, expDuration,
                  delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
                  retryAborted, maxBackoff, maxAttempts, output_measurements,
                  std::move(latencyFilename)),
      num_warehouses(num_warehouses),
      w_id(w_id),
      C_c_id(C_c_id),
      C_c_last(C_c_last),
      new_order_ratio(new_order_ratio),
      delivery_ratio(delivery_ratio),
      payment_ratio(payment_ratio),
      order_status_ratio(order_status_ratio),
      stock_level_ratio(stock_level_ratio),
      static_w_id(static_w_id),
      user_aborts(user_aborts),
      distributed_txns(distributed_txns),
      delivery(false) {
  stockLevelDId = std::uniform_int_distribution<uint32_t>(1, 10)(GetRand());
  Debug("num_warehouses: %u", num_warehouses);
}

TPCCClient::~TPCCClient() = default;

TPCCTransactionType TPCCClient::GetNextTransaction(uint32_t *wid, uint32_t *did,
                                                   std::mt19937 &gen) {
  if (delivery && deliveryDId < 10) {
    deliveryDId++;
    *wid = deliveryWId;
    *did = deliveryDId;
    return TXN_DELIVERY;
  }
  delivery = false;

  uint32_t total = new_order_ratio + delivery_ratio + payment_ratio +
                   order_status_ratio + stock_level_ratio;
  uint32_t ttype = std::uniform_int_distribution<uint32_t>(0, total - 1)(gen);
  if (static_w_id) {
    *wid = w_id;
  } else {
    *wid = std::uniform_int_distribution<uint32_t>(1, num_warehouses)(gen);
  }
  if (ttype < new_order_ratio) {
    lastOp = "new_order";
    return TXN_NEW_ORDER;  // new NewOrder(wid, C_c_id, num_warehouses, gen);
  }
  if (ttype < new_order_ratio + payment_ratio) {
    lastOp = "payment";
    return TXN_PAYMENT;  // new Payment(wid, C_c_last, C_c_id, num_warehouses,
                         // gen);
  }
  if (ttype < new_order_ratio + payment_ratio + order_status_ratio) {
    lastOp = "order_status";
    return TXN_ORDER_STATUS;  // new OrderStatus(wid, C_c_last, C_c_id, gen);
  } else if (ttype < new_order_ratio + payment_ratio + order_status_ratio +
                         stock_level_ratio) {
    if (static_w_id) {
      *did = stockLevelDId;
    } else {
      *did = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
    }
    lastOp = "stock_level";
    return TXN_STOCK_LEVEL;  // new StockLevel(wid, did, gen);
  } else {
    deliveryDId = 1;
    deliveryWId = *wid;
    *did = deliveryDId;
    delivery = true;
    lastOp = "delivery";
    return TXN_DELIVERY;
  }
}

::context::AsyncTransaction *TPCCClient::GetNextContextTransaction(
    context::commit_callback ccb, context::abort_callback acb) {
  uint32_t wid, did;
  TPCCTransactionType ttype =
      TPCCClient::GetNextTransaction(&wid, &did, GetRand());
  // stats.IncrementList("tpcc_warehouse", wid);
  Debug("TPC-C txn %d with warehouse %u.", ttype, wid);
  switch (ttype) {
    case TXN_NEW_ORDER: {
      auto txn = new ::context::tpcc::NewOrder(
          std::move(ccb), std::move(acb), wid, C_c_id, num_warehouses,
          user_aborts, distributed_txns, GetRand());
      /*for (const auto supply_w_id : txn->o_ol_supply_w_ids) {
        stats.IncrementList("tpcc_no_supply_warehouse", supply_w_id);
      }*/
      return txn;
    }
    case TXN_PAYMENT: {
      auto txn = new ::context::tpcc::Payment(
          std::move(ccb), std::move(acb), wid, C_c_last, C_c_id, num_warehouses,
          distributed_txns, GetRand());
      // stats.IncrementList("tpcc_p_cust_warehouse", txn->c_w_id);
      return txn;
    }
    case TXN_ORDER_STATUS: {
      auto txn = new ::context::tpcc::OrderStatus(
          std::move(ccb), std::move(acb), wid, C_c_last, C_c_id, GetRand());
      return txn;
    }
    case TXN_STOCK_LEVEL: {
      auto txn = new ::context::tpcc::StockLevel(std::move(ccb), std::move(acb),
                                                 wid, did, GetRand());
      // stats.IncrementList("tpcc_p_cust_warehouse", txn->c_w_id);
      return txn;
    }
    case TXN_DELIVERY: {
      auto txn = new ::context::tpcc::Delivery(std::move(ccb), std::move(acb),
                                               wid, did, GetRand());
      // stats.IncrementList("tpcc_p_cust_warehouse", txn->c_w_id);
      return txn;
    }
    default:
      NOT_REACHABLE();
  }
}

std::string TPCCClient::GetLastTransaction() const { return lastOp; }

}  // namespace tpcc
