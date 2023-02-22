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
#include "store/benchmark/tpcc/payment.h"

#include <bits/std_function.h>

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <sstream>
#include <utility>

#include "lib/message.h"
#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/tpcc/tpcc_utils.h"

namespace context {

namespace tpcc {

Payment::Payment(commit_callback ccb, abort_callback acb, uint32_t w_id,
                 uint32_t c_c_last, uint32_t c_c_id, uint32_t num_warehouses,
                 bool distributed_txns, std::mt19937 &gen)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  d_w_id = w_id;
  int x = std::uniform_int_distribution<int>(1, 100)(gen);
  int y = std::uniform_int_distribution<int>(1, 100)(gen);
  Debug("w_id: %u", w_id);
  Debug("this->w_id: %u", this->w_id);
  Debug("x: %d; num_warehouses: %u", x, num_warehouses);
  if (distributed_txns && x > 85 && num_warehouses > 1) {
    c_w_id =
        std::uniform_int_distribution<uint32_t>(1, num_warehouses - 1)(gen);
    Debug("c_w_id: %u", c_w_id);
    if (c_w_id == w_id) {
      c_w_id = num_warehouses;  // simple swap to ensure uniform distribution
      Debug("c_w_id: %u", c_w_id);
    }
    c_d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  } else {
    c_w_id = w_id;
    c_d_id = d_id;
  }
  if (y <= 60) {
    int last = ::tpcc::NURand(255, 0, 999, static_cast<int>(c_c_last), gen);
    c_last = ::tpcc::GenerateCustomerLastName(last);
    c_by_last_name = true;
  } else {
    c_id = ::tpcc::NURand(1023, 1, 3000, static_cast<int>(c_c_id), gen);
    c_by_last_name = false;
  }
  h_amount = std::uniform_int_distribution<uint32_t>(100, 500000)(gen);
  h_date = std::time(nullptr);
  h_id = gen();
}

Payment::~Payment() = default;

void Payment::Execute(Client *client, uint32_t timeout) {
  Debug("PAYMENT");
  std::unique_ptr<AppContext> ctx(new PaymentContext());
  client->Begin(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void Payment::Retry(Client *client, uint32_t timeout) {
  Debug("PAYMENT");
  std::unique_ptr<AppContext> ctx(new PaymentContext());
  client->Retry(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void Payment::ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx) {
  client->GetForUpdate(
      std::move(ctx), ::tpcc::WarehouseRowKey(w_id),
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status, const std::string &key,
                     const std::string &val) {
        if (status == FAIL) {
          client->Abort(app_ctx);
          acb_(std::move(app_ctx));
          return;
        }

        auto *ctx = dynamic_cast<PaymentContext *>(app_ctx.get());

        ::tpcc::WarehouseRow w_row;
        w_row.ParseFromString(val);

        w_row.set_ytd(w_row.ytd() + h_amount);
        ctx->w_name_ = w_row.name();

        std::string w_row_ser;
        w_row.SerializeToString(&w_row_ser);
        client->Put(app_ctx, key, w_row_ser);

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

              auto *ctx = dynamic_cast<PaymentContext *>(app_ctx.get());

              ::tpcc::DistrictRow d_row;
              d_row.ParseFromString(val);

              d_row.set_ytd(d_row.ytd() + h_amount);
              ctx->d_name_ = d_row.name();

              std::string d_row_ser;
              d_row.SerializeToString(&d_row_ser);
              client->Put(app_ctx, key, d_row_ser);

              if (c_by_last_name) {
                client->Get(
                    std::move(app_ctx),
                    ::tpcc::CustomerByNameRowKey(c_w_id, c_d_id, c_last),
                    [this, client](std::unique_ptr<AppContext> app_ctx,
                                   transaction_op_status_t status,
                                   const std::string &key,
                                   const std::string &val) {
                      if (status == FAIL) {
                        client->Abort(app_ctx);
                        acb_(std::move(app_ctx));
                        return;
                      }

                      auto *ctx = dynamic_cast<PaymentContext *>(app_ctx.get());

                      ::tpcc::CustomerByNameRow cbn_row;
                      cbn_row.ParseFromString(val);

                      int idx = (cbn_row.ids_size() + 1) / 2;
                      if (idx == cbn_row.ids_size()) {
                        idx = cbn_row.ids_size() - 1;
                      }
                      ctx->c_id_ = cbn_row.ids(idx);

                      UpdateCustomerYTD(client, std::move(app_ctx));
                    });
              } else {
                ctx->c_id_ = c_id;

                UpdateCustomerYTD(client, std::move(app_ctx));
              }
            });
      });
}

void Payment::UpdateCustomerYTD(Client *client,
                                std::unique_ptr<AppContext> app_ctx) {
  auto *ctx = dynamic_cast<PaymentContext *>(app_ctx.get());

  client->GetForUpdate(
      std::move(app_ctx), ::tpcc::CustomerRowKey(c_w_id, c_d_id, ctx->c_id_),
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status, const std::string &key,
                     const std::string &val) {
        if (status == FAIL) {
          client->Abort(app_ctx);
          acb_(std::move(app_ctx));
          return;
        }

        auto *ctx = dynamic_cast<PaymentContext *>(app_ctx.get());

        ::tpcc::CustomerRow c_row;
        c_row.ParseFromString(val);

        c_row.set_balance(c_row.balance() - h_amount);
        c_row.set_ytd_payment(c_row.ytd_payment() + h_amount);
        c_row.set_payment_cnt(c_row.payment_cnt() + 1);
        if (c_row.credit() == "BC") {
          std::stringstream ss;
          ss << c_id << "," << c_d_id << "," << c_w_id << "," << d_id << ","
             << w_id << "," << h_amount;
          std::string new_data = ss.str() + c_row.data();
          new_data = new_data.substr(std::min(new_data.size(), 500UL));
          c_row.set_data(new_data);
        }

        std::string c_row_ser;
        c_row.SerializeToString(&c_row_ser);
        client->Put(app_ctx, key, c_row_ser);

        ::tpcc::HistoryRow h_row;
        h_row.set_id(h_id);
        h_row.set_c_id(ctx->c_id_);
        h_row.set_c_d_id(c_d_id);
        h_row.set_c_w_id(c_w_id);
        h_row.set_d_id(d_id);
        h_row.set_w_id(w_id);
        h_row.set_data(ctx->w_name_ + "    " + ctx->d_name_);

        std::string h_row_ser;
        h_row.SerializeToString(&h_row_ser);
        client->Put(app_ctx, ::tpcc::HistoryRowKey(h_id, w_id), h_row_ser);

        client->Commit(std::move(app_ctx), ccb_);
      });
}

Payment::PaymentContext::PaymentContext() = default;

Payment::PaymentContext::PaymentContext(const PaymentContext &other)
    : w_name_(other.w_name_), d_name_(other.d_name_), c_id_(other.c_id_) {}

Payment::PaymentContext::~PaymentContext() = default;

std::unique_ptr<AppContext> Payment::PaymentContext::CloneInternal() const {
  return std::make_unique<PaymentContext>(*this);
}

}  // namespace tpcc

}  // namespace context
