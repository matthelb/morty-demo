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
#ifndef ORDER_STATUS_H
#define ORDER_STATUS_H

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace context {

namespace tpcc {

class OrderStatus : public AsyncTransaction {
 public:
  OrderStatus(commit_callback ccb, abort_callback acb, uint32_t w_id,
              uint32_t c_c_last, uint32_t c_c_id, std::mt19937 &gen);
  ~OrderStatus() override;

  void Execute(Client *client, uint32_t timeout) override;

 protected:
  uint32_t w_id;
  uint32_t d_id;
  uint32_t c_w_id;
  uint32_t c_d_id;
  uint32_t c_id;
  uint32_t o_id{};
  bool c_by_last_name;
  std::string c_last;

 private:
  class OrderStatusContext : public AppContext {
   public:
    OrderStatusContext();
    OrderStatusContext(const OrderStatusContext &other);
    ~OrderStatusContext() override;

    uint32_t c_id{};
    uint32_t o_id{};
    size_t ol_cnt{};

   protected:
    virtual std::unique_ptr<AppContext> CloneInternal() const;
  };

  void GetCustomer(Client *client, std::unique_ptr<AppContext> ctx);
  void GetOrderForCustomer(Client *client, std::unique_ptr<AppContext> app_ctx);
  void GetOrderLineForOrder(Client *client, std::unique_ptr<AppContext> app_ctx,
                            size_t ol_number);

  void GetCustomerRO(Client *client, std::unique_ptr<AppContext> ctx);
  void GetOrderForCustomerRO(Client *client,
                             std::unique_ptr<AppContext> app_ctx);
  void GetOrderLinesRO(Client *client, std::unique_ptr<AppContext> app_ctx);
};

}  // namespace tpcc

}  // namespace context

#endif /* ORDER_STATUS_H */
