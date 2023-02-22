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
#include "store/benchmark/tpcc/new_order.h"

#include <bits/std_function.h>

#include <cstdint>
#include <ctime>
#include <string>
#include <utility>

#include "lib/message.h"
#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/tpcc/tpcc_utils.h"

namespace context {

namespace tpcc {

NewOrder::NewOrder(commit_callback ccb, abort_callback acb, uint32_t w_id,
                   uint32_t C, uint32_t num_warehouses, bool user_aborts,
                   bool distributed_txns, std::mt19937 &gen)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  c_id = ::tpcc::NURand(static_cast<uint32_t>(1023), static_cast<uint32_t>(1),
                        static_cast<uint32_t>(3000), C, gen);
  // ol_cnt = std::uniform_int_distribution<uint8_t>(5, 15)(gen);
  ol_cnt = 10;
  rbk = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
  all_local = true;
  for (uint8_t i = 0; i < ol_cnt; ++i) {
    if (user_aborts && rbk == 1 && i == ol_cnt - 1) {
      o_ol_i_ids.push_back(0);
    } else {
      o_ol_i_ids.push_back(
          ::tpcc::NURand(static_cast<uint32_t>(8191), static_cast<uint32_t>(1),
                         static_cast<uint32_t>(100000), C, gen));
    }
    uint8_t x = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
    if (distributed_txns && x == 1 && num_warehouses > 1) {
      uint32_t remote_w_id =
          std::uniform_int_distribution<uint32_t>(1, num_warehouses - 1)(gen);
      if (remote_w_id == w_id) {
        remote_w_id =
            num_warehouses;  // simple swap to ensure uniform distribution
      }
      o_ol_supply_w_ids.push_back(remote_w_id);
      all_local = false;
    } else {
      o_ol_supply_w_ids.push_back(w_id);
    }
    o_ol_quantities.push_back(
        std::uniform_int_distribution<uint8_t>(1, 10)(gen));
  }
  o_entry_d = std::time(nullptr);
}

NewOrder::~NewOrder() = default;

void NewOrder::Execute(Client *client, uint32_t timeout) {
  Debug("NEW_ORDER");
  std::unique_ptr<AppContext> ctx(new NewOrderContext());
  client->Begin(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void NewOrder::Retry(Client *client, uint32_t timeout) {
  Debug("NEW_ORDER");
  std::unique_ptr<AppContext> ctx(new NewOrderContext());
  client->Retry(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void NewOrder::ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx) {
  client->Get(
      std::move(ctx), ::tpcc::WarehouseRowKey(w_id),
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status, const std::string &key,
                     const std::string &val) {
        if (status == FAIL) {
          client->Abort(app_ctx);
          acb_(std::move(app_ctx));
          return;
        }

        client->GetForUpdate(
            std::move(app_ctx), ::tpcc::DistrictRowKey(w_id, d_id),
            [this, client](std::unique_ptr<AppContext> app_ctx,
                           transaction_op_status_t status,
                           const std::string &key, const std::string &val) {
              if (status == FAIL) {
                client->Abort(app_ctx);
                acb_(std::move(app_ctx));
                return;
              }

              auto *ctx = dynamic_cast<NewOrderContext *>(app_ctx.get());

              ::tpcc::DistrictRow d_row;
              d_row.ParseFromString(val);

              ctx->o_id_ = d_row.next_o_id();

              d_row.set_next_o_id(d_row.next_o_id() + 1);

              std::string d_row_ser;
              d_row.SerializeToString(&d_row_ser);
              client->Put(app_ctx, key, d_row_ser);

              client->Get(
                  std::move(app_ctx), ::tpcc::CustomerRowKey(w_id, d_id, c_id),
                  [this, client](std::unique_ptr<AppContext> app_ctx,
                                 transaction_op_status_t status,
                                 const std::string &key,
                                 const std::string &val) {
                    if (status == FAIL) {
                      client->Abort(app_ctx);
                      acb_(std::move(app_ctx));
                      return;
                    }

                    auto *ctx = dynamic_cast<NewOrderContext *>(app_ctx.get());

                    ::tpcc::NewOrderRow no_row;
                    no_row.set_o_id(ctx->o_id_);
                    no_row.set_d_id(d_id);
                    no_row.set_w_id(w_id);

                    std::string no_row_ser;
                    no_row.SerializeToString(&no_row_ser);
                    client->Put(app_ctx,
                                ::tpcc::NewOrderRowKey(w_id, d_id, ctx->o_id_),
                                no_row_ser);

                    ::tpcc::OrderRow o_row;
                    o_row.set_id(ctx->o_id_);
                    o_row.set_d_id(d_id);
                    o_row.set_w_id(w_id);
                    o_row.set_c_id(c_id);
                    o_row.set_entry_d(o_entry_d);
                    o_row.set_carrier_id(0);
                    o_row.set_ol_cnt(ol_cnt);
                    o_row.set_all_local(all_local);

                    std::string o_row_ser;
                    o_row.SerializeToString(&o_row_ser);
                    client->Put(app_ctx,
                                ::tpcc::OrderRowKey(w_id, d_id, ctx->o_id_),
                                o_row_ser);

                    ::tpcc::OrderByCustomerRow obc_row;
                    obc_row.set_w_id(w_id);
                    obc_row.set_d_id(d_id);
                    obc_row.set_c_id(c_id);
                    obc_row.set_o_id(ctx->o_id_);

                    std::string obc_row_ser;
                    obc_row.SerializeToString(&obc_row_ser);
                    client->Put(app_ctx,
                                ::tpcc::OrderByCustomerRowKey(w_id, d_id, c_id),
                                obc_row_ser);

                    CreateOrderLine(client, std::move(app_ctx), 0UL);
                  });
            });
      });
}

void NewOrder::CreateOrderLine(Client *client, std::unique_ptr<AppContext> ctx,
                               size_t ol_number) {
  if (ol_number == ol_cnt) {
    client->Commit(std::move(ctx), ccb_);

  } else {
    client->Get(
        std::move(ctx), ::tpcc::ItemRowKey(o_ol_i_ids[ol_number]),
        [this, client, ol_number](
            std::unique_ptr<AppContext> app_ctx, transaction_op_status_t status,
            const std::string &key, const std::string &val) {
          if (status == FAIL) {
            client->Abort(app_ctx);
            acb_(std::move(app_ctx));
            return;
          }

          auto *ctx = dynamic_cast<NewOrderContext *>(app_ctx.get());

          if (val.empty()) {
            client->Abort(app_ctx);
          } else {
            ctx->curr_i_row.ParseFromString(val);

            client->GetForUpdate(
                std::move(app_ctx),
                ::tpcc::StockRowKey(o_ol_supply_w_ids[ol_number],
                                    o_ol_i_ids[ol_number]),
                [this, client, ol_number](std::unique_ptr<AppContext> app_ctx,
                                          transaction_op_status_t status,
                                          const std::string &key,
                                          const std::string &val) {
                  if (status == FAIL) {
                    client->Abort(app_ctx);
                    acb_(std::move(app_ctx));
                    return;
                  }

                  auto *ctx = dynamic_cast<NewOrderContext *>(app_ctx.get());

                  ctx->curr_s_row.ParseFromString(val);

                  if (ctx->curr_s_row.quantity() - o_ol_quantities[ol_number] >=
                      10) {
                    ctx->curr_s_row.set_quantity(ctx->curr_s_row.quantity() -
                                                 o_ol_quantities[ol_number]);
                  } else {
                    ctx->curr_s_row.set_quantity(ctx->curr_s_row.quantity() -
                                                 o_ol_quantities[ol_number] +
                                                 91);
                  }
                  ctx->curr_s_row.set_ytd(ctx->curr_s_row.ytd() +
                                          o_ol_quantities[ol_number]);
                  ctx->curr_s_row.set_order_cnt(ctx->curr_s_row.order_cnt() +
                                                1);
                  if (w_id != o_ol_supply_w_ids[ol_number]) {
                    ctx->curr_s_row.set_remote_cnt(
                        ctx->curr_s_row.remote_cnt() + 1);
                  }

                  std::string s_row_ser;
                  ctx->curr_s_row.SerializeToString(&s_row_ser);
                  client->Put(app_ctx, key, s_row_ser);

                  ::tpcc::OrderLineRow ol_row;
                  ol_row.set_o_id(ctx->o_id_);
                  ol_row.set_d_id(d_id);
                  ol_row.set_w_id(w_id);
                  ol_row.set_number(ol_number);
                  ol_row.set_i_id(o_ol_i_ids[ol_number]);
                  ol_row.set_supply_w_id(o_ol_supply_w_ids[ol_number]);
                  ol_row.set_delivery_d(0);
                  ol_row.set_quantity(o_ol_quantities[ol_number]);
                  ol_row.set_amount(o_ol_quantities[ol_number] *
                                    ctx->curr_i_row.price());
                  switch (d_id) {
                    case 1:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_01());
                      break;
                    case 2:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_02());
                      break;
                    case 3:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_03());
                      break;
                    case 4:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_04());
                      break;
                    case 5:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_05());
                      break;
                    case 6:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_06());
                      break;
                    case 7:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_07());
                      break;
                    case 8:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_08());
                      break;
                    case 9:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_09());
                      break;
                    case 10:
                      ol_row.set_dist_info(ctx->curr_s_row.dist_10());
                      break;
                  }

                  std::string ol_row_ser;
                  ol_row.SerializeToString(&ol_row_ser);
                  client->Put(app_ctx,
                              ::tpcc::OrderLineRowKey(w_id, d_id, ctx->o_id_,
                                                      ol_number),
                              ol_row_ser);

                  CreateOrderLine(client, std::move(app_ctx), ol_number + 1);
                });
          }
        });
  }
}

NewOrder::NewOrderContext::NewOrderContext() = default;

NewOrder::NewOrderContext::NewOrderContext(const NewOrderContext &other)
    : o_id_(other.o_id_),
      curr_s_row(other.curr_s_row),
      curr_i_row(other.curr_i_row) {}

NewOrder::NewOrderContext::~NewOrderContext() = default;

std::unique_ptr<AppContext> NewOrder::NewOrderContext::CloneInternal() const {
  return std::make_unique<NewOrderContext>(*this);
}

}  // namespace tpcc

}  // namespace context
