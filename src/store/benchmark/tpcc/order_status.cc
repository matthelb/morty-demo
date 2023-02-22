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
#include "store/benchmark/tpcc/order_status.h"

#include <bits/std_function.h>

#include <cstdint>
#include <ctime>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/tpcc/tpcc_utils.h"

namespace context {

namespace tpcc {

OrderStatus::OrderStatus(commit_callback ccb, abort_callback acb, uint32_t w_id,
                         uint32_t c_c_last, uint32_t c_c_id, std::mt19937 &gen)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  int y = std::uniform_int_distribution<int>(1, 100)(gen);
  c_w_id = w_id;
  c_d_id = d_id;
  if (y <= 60) {
    int last = ::tpcc::NURand(255, 0, 999, static_cast<int>(c_c_last), gen);
    c_last = ::tpcc::GenerateCustomerLastName(last);
    c_by_last_name = true;
  } else {
    c_id = ::tpcc::NURand(1023, 1, 3000, static_cast<int>(c_c_id), gen);
    c_by_last_name = false;
  }
}

OrderStatus::~OrderStatus() = default;

void OrderStatus::Execute(Client *client, uint32_t timeout) {
  Debug("ORDER_STATUS");

  std::unique_ptr<AppContext> ctx{new OrderStatusContext()};

  if (client->SupportsRO()) {
    client->ROBegin(ctx, acb_);
    GetCustomerRO(client, std::move(ctx));
  } else {
    client->Begin(ctx, acb_);
    GetCustomer(client, std::move(ctx));
  }
}

void OrderStatus::GetCustomer(Client *client, std::unique_ptr<AppContext> ctx) {
  if (c_by_last_name) {
    client->Get(std::move(ctx),
                ::tpcc::CustomerByNameRowKey(c_w_id, c_d_id, c_last),
                [this, client](std::unique_ptr<AppContext> app_ctx,
                               transaction_op_status_t status,
                               const std::string &key, const std::string &val) {
                  if (status == FAIL) {
                    client->Abort(app_ctx);
                    acb_(std::move(app_ctx));
                    return;
                  }

                  auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

                  ::tpcc::CustomerByNameRow cbn_row;
                  UW_ASSERT(cbn_row.ParseFromString(val));
                  int idx = (cbn_row.ids_size() + 1) / 2;
                  if (idx == cbn_row.ids_size()) {
                    idx = cbn_row.ids_size() - 1;
                  }
                  ctx->c_id = cbn_row.ids(idx);
                  GetOrderForCustomer(client, std::move(app_ctx));
                });
  } else {
    auto *osc_ctx = dynamic_cast<OrderStatusContext *>(ctx.get());
    osc_ctx->c_id = c_id;
    GetOrderForCustomer(client, std::move(ctx));
  }
}

void OrderStatus::GetOrderForCustomer(Client *client,
                                      std::unique_ptr<AppContext> app_ctx) {
  auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());
  client->Get(
      std::move(app_ctx), ::tpcc::CustomerRowKey(c_w_id, c_d_id, ctx->c_id),
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status, const std::string &key,
                     const std::string &val) {
        if (status == FAIL) {
          client->Abort(app_ctx);
          acb_(std::move(app_ctx));
          return;
        }

        auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

        ::tpcc::CustomerRow c_row;
        UW_ASSERT(c_row.ParseFromString(val));

        client->Get(
            std::move(app_ctx),
            ::tpcc::OrderByCustomerRowKey(c_w_id, c_d_id, ctx->c_id),
            [this, client](std::unique_ptr<AppContext> app_ctx,
                           transaction_op_status_t status,
                           const std::string &key, const std::string &val) {
              if (status == FAIL) {
                client->Abort(app_ctx);
                acb_(std::move(app_ctx));
                return;
              }

              auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

              ::tpcc::OrderByCustomerRow obc_row;
              UW_ASSERT(obc_row.ParseFromString(val));
              ctx->o_id = obc_row.o_id();

              client->Get(
                  std::move(app_ctx),
                  ::tpcc::OrderRowKey(c_w_id, c_d_id, ctx->o_id),
                  [this, client](std::unique_ptr<AppContext> app_ctx,
                                 transaction_op_status_t status,
                                 const std::string &key,
                                 const std::string &val) {
                    if (status == FAIL) {
                      client->Abort(app_ctx);
                      acb_(std::move(app_ctx));
                      return;
                    }

                    auto *ctx =
                        dynamic_cast<OrderStatusContext *>(app_ctx.get());

                    if (val.empty()) {
                      client->Abort(app_ctx);
                    } else {
                      ::tpcc::OrderRow o_row;
                      UW_ASSERT(o_row.ParseFromString(val));
                      ctx->ol_cnt = o_row.ol_cnt();

                      GetOrderLineForOrder(client, std::move(app_ctx), 0);
                    }
                  });
            });
      });
}

void OrderStatus::GetOrderLineForOrder(Client *client,
                                       std::unique_ptr<AppContext> app_ctx,
                                       size_t ol_number) {
  auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

  if (ol_number < ctx->ol_cnt) {
    client->Get(
        std::move(app_ctx),
        ::tpcc::OrderLineRowKey(c_w_id, c_d_id, ctx->o_id, ol_number),
        [this, client, ol_number](
            std::unique_ptr<AppContext> app_ctx, transaction_op_status_t status,
            const std::string &key, const std::string &val) {
          if (status == FAIL) {
            client->Abort(app_ctx);
            acb_(std::move(app_ctx));
            return;
          }

          if (val.empty()) {
          } else {
            ::tpcc::OrderLineRow ol_row;
            UW_ASSERT(ol_row.ParseFromString(val));
          }
          GetOrderLineForOrder(client, std::move(app_ctx), ol_number + 1);
        });
  } else {
    client->Commit(std::move(app_ctx), ccb_);
  }
}

void OrderStatus::GetCustomerRO(Client *client,
                                std::unique_ptr<AppContext> ctx) {
  if (c_by_last_name) {
    std::unordered_set<std::string> keys = {
        ::tpcc::CustomerByNameRowKey(c_w_id, c_d_id, c_last),
    };

    client->ROGet(
        std::move(ctx), keys,
        [this, client](
            std::unique_ptr<AppContext> app_ctx, transaction_op_status_t status,
            const std::unordered_map<std::string, std::string> &kvs) {
          auto key = ::tpcc::CustomerByNameRowKey(c_w_id, c_d_id, c_last);
          auto &val = kvs.at(key);

          auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

          ::tpcc::CustomerByNameRow cbn_row;
          UW_ASSERT(cbn_row.ParseFromString(val));
          int idx = (cbn_row.ids_size() + 1) / 2;
          if (idx == cbn_row.ids_size()) {
            idx = cbn_row.ids_size() - 1;
          }
          ctx->c_id = cbn_row.ids(idx);
          GetOrderForCustomerRO(client, std::move(app_ctx));
        });
  } else {
    auto *osc_ctx = dynamic_cast<OrderStatusContext *>(ctx.get());
    osc_ctx->c_id = c_id;
    GetOrderForCustomerRO(client, std::move(ctx));
  }
}

void OrderStatus::GetOrderForCustomerRO(Client *client,
                                        std::unique_ptr<AppContext> app_ctx) {
  auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());
  std::unordered_set<std::string> keys = {
      ::tpcc::CustomerRowKey(c_w_id, c_d_id, ctx->c_id),
      ::tpcc::OrderByCustomerRowKey(c_w_id, c_d_id, ctx->c_id),
  };

  client->ROGet(
      std::move(app_ctx), keys,
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status,
                     const std::unordered_map<std::string, std::string> &kvs) {
        auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

        auto c_key = ::tpcc::CustomerRowKey(c_w_id, c_d_id, ctx->c_id);
        auto &c_val = kvs.at(c_key);

        ::tpcc::CustomerRow c_row;
        UW_ASSERT(c_row.ParseFromString(c_val));

        auto obc_key = ::tpcc::OrderByCustomerRowKey(c_w_id, c_d_id, ctx->c_id);
        auto &obc_val = kvs.at(obc_key);

        ::tpcc::OrderByCustomerRow obc_row;
        UW_ASSERT(obc_row.ParseFromString(obc_val));
        ctx->o_id = obc_row.o_id();

        std::unordered_set<std::string> keys = {
            ::tpcc::OrderRowKey(c_w_id, c_d_id, ctx->o_id),
        };
        client->ROGet(
            std::move(app_ctx), keys,
            [this, client](
                std::unique_ptr<AppContext> app_ctx,
                transaction_op_status_t status,
                const std::unordered_map<std::string, std::string> &kvs) {
              auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

              auto o_key = ::tpcc::OrderRowKey(c_w_id, c_d_id, ctx->o_id);
              auto &o_val = kvs.at(o_key);

              if (o_val.empty()) {
                client->Abort(app_ctx);
              } else {
                ::tpcc::OrderRow o_row;
                UW_ASSERT(o_row.ParseFromString(o_val));
                ctx->ol_cnt = o_row.ol_cnt();

                GetOrderLinesRO(client, std::move(app_ctx));
              }
            });
      });
}

void OrderStatus::GetOrderLinesRO(Client *client,
                                  std::unique_ptr<AppContext> app_ctx) {
  auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

  std::unordered_set<std::string> keys;
  keys.reserve(ctx->ol_cnt);
  for (std::size_t i = 0; i < ctx->ol_cnt; ++i) {
    keys.emplace(::tpcc::OrderLineRowKey(c_w_id, c_d_id, ctx->o_id, i));
  }

  client->ROGet(
      std::move(app_ctx), keys,
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status,
                     const std::unordered_map<std::string, std::string> &kvs) {
        auto *ctx = dynamic_cast<OrderStatusContext *>(app_ctx.get());

        for (std::size_t i = 0; i < ctx->ol_cnt; ++i) {
          auto key = ::tpcc::OrderLineRowKey(c_w_id, c_d_id, ctx->o_id, i);
          auto &val = kvs.at(key);

          if (val.empty()) {
          } else {
            ::tpcc::OrderLineRow ol_row;
            UW_ASSERT(ol_row.ParseFromString(val));
          }
        }

        client->ROCommit(std::move(app_ctx), ccb_);
      });
}

OrderStatus::OrderStatusContext::OrderStatusContext() = default;

OrderStatus::OrderStatusContext::OrderStatusContext(
    const OrderStatusContext &other)
    : c_id(other.c_id), o_id(other.o_id), ol_cnt(other.ol_cnt) {}

OrderStatus::OrderStatusContext::~OrderStatusContext() = default;

std::unique_ptr<AppContext> OrderStatus::OrderStatusContext::CloneInternal()
    const {
  return std::make_unique<OrderStatusContext>(*this);
}

}  // namespace tpcc

}  // namespace context
