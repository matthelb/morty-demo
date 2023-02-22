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
#ifndef STOCK_LEVEL_H
#define STOCK_LEVEL_H

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <random>
#include <vector>

#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace context {

namespace tpcc {

class StockLevel : public AsyncTransaction {
 public:
  StockLevel(commit_callback ccb, abort_callback acb, uint32_t w_id,
             uint32_t d_id, std::mt19937 &gen);
  ~StockLevel() override;

  void Execute(Client *client, uint32_t timeout) override;

 protected:
  uint32_t w_id;
  uint32_t d_id;
  uint8_t min_quantity;

 private:
  class StockLevelContext : public AppContext {
   public:
    StockLevelContext();
    StockLevelContext(const StockLevelContext &other);
    ~StockLevelContext() override;

    uint32_t next_o_id{};
    uint32_t curr_o_id{};
    size_t curr_ol_cnt{};

   protected:
    virtual std::unique_ptr<AppContext> CloneInternal() const;
  };

  void GetDistrict(Client *client, std::unique_ptr<AppContext> ctx);
  void CheckOrder(Client *client, std::unique_ptr<AppContext> app_ctx);
  void CheckOrderLine(Client *client, std::unique_ptr<AppContext> app_ctx,
                      size_t ol_number);

  void GetDistrictRO(Client *client, std::unique_ptr<AppContext> ctx);
  void CheckOrdersRO(Client *client, std::unique_ptr<AppContext> app_ctx);
  void CheckOrderLinesRO(Client *client, std::unique_ptr<AppContext> app_ctx,
                         std::vector<std::uint32_t> ol_cnts);
};

}  // namespace tpcc

}  // namespace context

#endif /* STOCK_LEVEL_H */
