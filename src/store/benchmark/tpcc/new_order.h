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
#ifndef NEW_ORDER_H
#define NEW_ORDER_H

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <random>
#include <vector>

#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace context {

namespace tpcc {

class NewOrder : public AsyncTransaction {
 public:
  NewOrder(commit_callback ccb, abort_callback acb, uint32_t w_id, uint32_t C,
           uint32_t num_warehouses, bool user_aborts, bool distributed_txns,
           std::mt19937 &gen);
  ~NewOrder() override;

  void Execute(Client *client, uint32_t timeout) override;
  void Retry(Client *client, uint32_t timeout) override;

  // protected:
  uint32_t w_id;
  uint32_t d_id;
  uint32_t c_id;
  uint8_t ol_cnt;
  uint8_t rbk;
  std::vector<uint32_t> o_ol_i_ids;
  std::vector<uint32_t> o_ol_supply_w_ids;
  std::vector<uint8_t> o_ol_quantities;
  uint32_t o_entry_d;
  bool all_local;

 private:
  class NewOrderContext : public AppContext {
   public:
    NewOrderContext();
    NewOrderContext(const NewOrderContext &other);
    ~NewOrderContext() override;

    uint32_t o_id_{};
    ::tpcc::StockRow curr_s_row;
    ::tpcc::ItemRow curr_i_row;

   protected:
    virtual std::unique_ptr<AppContext> CloneInternal() const;
  };

  void ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx);

  void CreateOrderLine(Client *client, std::unique_ptr<AppContext> ctx,
                       size_t ol_number);
};

}  // namespace tpcc

}  // namespace context

#endif /* NEW_ORDER_H */
