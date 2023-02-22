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
#ifndef DELIVERY_H
#define DELIVERY_H

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace context {
namespace tpcc {

class Delivery : public AsyncTransaction {
 public:
  Delivery(commit_callback ccb, abort_callback acb, uint32_t w_id,
           uint32_t d_id, std::mt19937 &gen);
  ~Delivery() override;

  void Execute(Client *client, uint32_t timeout) override;
  void Retry(Client *client, uint32_t timeout) override;

 private:
  class DeliveryContext : public AppContext {
   public:
    DeliveryContext();
    DeliveryContext(const DeliveryContext &other);
    ~DeliveryContext() override;

    uint32_t o_id{};
    size_t ol_cnt{};
    uint32_t c_id{};
    int32_t total_amount{};

   protected:
    virtual std::unique_ptr<AppContext> CloneInternal() const;
  };

  void ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx);

  void UpdateOrderLine(Client *client, std::unique_ptr<AppContext> app_ctx,
                       size_t ol_number);

  uint32_t w_id;
  uint32_t d_id;
  uint32_t o_carrier_id;
  uint32_t ol_delivery_d;
};

}  // namespace tpcc
}  // namespace context

#endif /* DELIVERY_H */
