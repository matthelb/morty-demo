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
#ifndef PAYMENT_H
#define PAYMENT_H

#include <bits/stdint-uintn.h>

#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace context {

namespace tpcc {

class Payment : public AsyncTransaction {
 public:
  Payment(commit_callback ccb, abort_callback acb, uint32_t w_id,
          uint32_t c_c_last, uint32_t c_c_id, uint32_t num_warehouses,
          bool distributed_txns, std::mt19937 &gen);
  ~Payment() override;

  void Execute(Client *client, uint32_t timeout) override;
  void Retry(Client *client, uint32_t timeout) override;

  // protected:
  uint32_t w_id;
  uint32_t d_id;
  uint32_t d_w_id;
  uint32_t c_w_id;
  uint32_t c_d_id;
  uint32_t c_id;
  uint32_t h_amount;
  uint32_t h_date;
  bool c_by_last_name;
  std::string c_last;
  uint64_t h_id;

 private:
  class PaymentContext : public AppContext {
   public:
    PaymentContext();
    PaymentContext(const PaymentContext &other);
    ~PaymentContext() override;

    std::string w_name_;
    std::string d_name_;
    uint32_t c_id_{};

   protected:
    virtual std::unique_ptr<AppContext> CloneInternal() const;
  };

  void ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx);

  void UpdateCustomerYTD(Client *client, std::unique_ptr<AppContext> app_ctx);
};

}  // namespace tpcc

}  // namespace context

#endif /* PAYMENT_H */
