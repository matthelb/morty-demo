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
#include "store/benchmark/tpcc/delivery.h"

#include <bits/std_function.h>

#include <cstdint>
#include <ctime>
#include <string>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/tpcc/tpcc_utils.h"

namespace context {

namespace tpcc {

Delivery::Delivery(commit_callback ccb, abort_callback acb, uint32_t w_id,
                   uint32_t d_id, std::mt19937 &gen)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)),
      w_id(w_id),
      d_id(d_id) {
  o_carrier_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  ol_delivery_d = std::time(nullptr);
}

Delivery::~Delivery() = default;

void Delivery::Execute(Client *client, uint32_t timeout) {
  Debug("DELIVERY");
  std::unique_ptr<AppContext> ctx(new DeliveryContext());
  client->Begin(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void Delivery::Retry(Client *client, uint32_t timeout) {
  Debug("DELIVERY");
  std::unique_ptr<AppContext> ctx(new DeliveryContext());
  client->Retry(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void Delivery::ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx) {
  client->Get(
      std::move(ctx), ::tpcc::EarliestNewOrderRowKey(w_id, d_id),
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status, const std::string &key,
                     const std::string &val) {
        if (status == FAIL) {
          client->Abort(app_ctx);
          acb_(std::move(app_ctx));
          return;
        }

        auto *ctx = dynamic_cast<DeliveryContext *>(app_ctx.get());

        if (val.empty()) {
          client->Commit(std::move(app_ctx), ccb_);
        } else {
          ::tpcc::EarliestNewOrderRow eno_row;
          UW_ASSERT(eno_row.ParseFromString(val));

          ctx->o_id = eno_row.o_id();
          eno_row.set_o_id(ctx->o_id + 1);

          std::string eno_row_ser;
          eno_row.SerializeToString(&eno_row_ser);
          client->Put(app_ctx, key, eno_row_ser);

          client->GetForUpdate(
              std::move(app_ctx), ::tpcc::OrderRowKey(w_id, d_id, ctx->o_id),
              [this, client](std::unique_ptr<AppContext> app_ctx,
                             transaction_op_status_t status,
                             const std::string &key, const std::string &val) {
                if (status == FAIL) {
                  client->Abort(app_ctx);
                  acb_(std::move(app_ctx));
                  return;
                }

                auto *ctx = dynamic_cast<DeliveryContext *>(app_ctx.get());

                if (val.empty()) {
                  // OrderRow is empty if EarliestNewOrderRow contained invalid
                  // o_id
                  client->Commit(std::move(app_ctx), ccb_);
                } else {
                  ::tpcc::OrderRow o_row;
                  UW_ASSERT(o_row.ParseFromString(val));
                  o_row.set_carrier_id(o_carrier_id);
                  ctx->ol_cnt = o_row.ol_cnt();
                  ctx->c_id = o_row.c_id();

                  std::string o_row_ser;
                  o_row.SerializeToString(&o_row_ser);
                  client->Put(app_ctx, key, o_row_ser);

                  client->Put(app_ctx,
                              ::tpcc::NewOrderRowKey(w_id, d_id, ctx->o_id),
                              "");

                  UpdateOrderLine(client, std::move(app_ctx), 0);
                }
              });
        }
      });
}

void Delivery::UpdateOrderLine(Client *client,
                               std::unique_ptr<AppContext> app_ctx,
                               size_t ol_number) {
  auto *ctx = dynamic_cast<DeliveryContext *>(app_ctx.get());

  if (ol_number < ctx->ol_cnt) {
    client->GetForUpdate(
        std::move(app_ctx),
        ::tpcc::OrderLineRowKey(w_id, d_id, ctx->o_id, ol_number),
        [this, client, ol_number](
            std::unique_ptr<AppContext> app_ctx, transaction_op_status_t status,
            const std::string &key, const std::string &val) {
          if (status == FAIL) {
            client->Abort(app_ctx);
            acb_(std::move(app_ctx));
            return;
          }

          auto *ctx = dynamic_cast<DeliveryContext *>(app_ctx.get());

          ::tpcc::OrderLineRow ol_row;
          UW_ASSERT(ol_row.ParseFromString(val));
          ctx->total_amount += ol_row.amount();
          ol_row.set_delivery_d(ol_delivery_d);

          std::string ol_row_ser;
          ol_row.SerializeToString(&ol_row_ser);

          client->Put(app_ctx, key, ol_row_ser);

          UpdateOrderLine(client, std::move(app_ctx), ol_number + 1);
        });
  } else {
    client->GetForUpdate(
        std::move(app_ctx), ::tpcc::CustomerRowKey(w_id, d_id, ctx->c_id),
        [this, client](std::unique_ptr<AppContext> app_ctx,
                       transaction_op_status_t status, const std::string &key,
                       const std::string &val) {
          if (status == FAIL) {
            client->Abort(app_ctx);
            acb_(std::move(app_ctx));
            return;
          }

          auto *ctx = dynamic_cast<DeliveryContext *>(app_ctx.get());

          ::tpcc::CustomerRow c_row;
          UW_ASSERT(c_row.ParseFromString(val));
          c_row.set_balance(c_row.balance() + ctx->total_amount);
          c_row.set_delivery_cnt(c_row.delivery_cnt() + 1);

          std::string c_row_ser;
          c_row.SerializeToString(&c_row_ser);
          client->Put(app_ctx, key, c_row_ser);

          client->Commit(std::move(app_ctx), ccb_);
        });
  }
}

Delivery::DeliveryContext::DeliveryContext() = default;

Delivery::DeliveryContext::DeliveryContext(const DeliveryContext &other)
    : o_id(other.o_id),
      ol_cnt(other.ol_cnt),
      c_id(other.c_id),
      total_amount(other.total_amount) {}

Delivery::DeliveryContext::~DeliveryContext() = default;

std::unique_ptr<AppContext> Delivery::DeliveryContext::CloneInternal() const {
  return std::make_unique<DeliveryContext>(*this);
}

}  // namespace tpcc

}  // namespace context
