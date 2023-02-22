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
#include "store/benchmark/tpcc/stock_level.h"

#include <bits/std_function.h>

#include <cstdint>
#include <ctime>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/benchmark/tpcc/tpcc-proto.pb.h"
#include "store/benchmark/tpcc/tpcc_utils.h"

namespace context {

namespace tpcc {

StockLevel::StockLevel(commit_callback ccb, abort_callback acb, uint32_t w_id,
                       uint32_t d_id, std::mt19937 &gen)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)),
      w_id(w_id),
      d_id(d_id) {
  min_quantity = std::uniform_int_distribution<uint8_t>(10, 20)(gen);
}

StockLevel::~StockLevel() = default;

void StockLevel::Execute(Client *client, uint32_t timeout) {
  Debug("STOCK_LEVEL");

  std::unique_ptr<AppContext> ctx(new StockLevelContext());

  if (client->SupportsRO()) {
    client->ROBegin(ctx, acb_);
    GetDistrictRO(client, std::move(ctx));
  } else {
    client->Begin(ctx, acb_);
    GetDistrict(client, std::move(ctx));
  }
}

void StockLevel::GetDistrict(Client *client, std::unique_ptr<AppContext> ctx) {
  client->Get(std::move(ctx), ::tpcc::DistrictRowKey(w_id, d_id),
              [this, client](std::unique_ptr<AppContext> app_ctx,
                             transaction_op_status_t status,
                             const std::string &key, const std::string &val) {
                if (status == FAIL) {
                  client->Abort(app_ctx);
                  acb_(std::move(app_ctx));
                  return;
                }

                auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

                ::tpcc::DistrictRow d_row;
                UW_ASSERT(d_row.ParseFromString(val));

                ctx->next_o_id = d_row.next_o_id();
                ctx->curr_o_id = ctx->next_o_id - 20;
                CheckOrder(client, std::move(app_ctx));
              });
}

void StockLevel::CheckOrder(Client *client,
                            std::unique_ptr<AppContext> app_ctx) {
  auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

  if (ctx->curr_o_id < ctx->next_o_id) {
    client->Get(std::move(app_ctx),
                ::tpcc::OrderRowKey(w_id, d_id, ctx->curr_o_id),
                [this, client](std::unique_ptr<AppContext> app_ctx,
                               transaction_op_status_t status,
                               const std::string &key, const std::string &val) {
                  if (status == FAIL) {
                    client->Abort(app_ctx);
                    acb_(std::move(app_ctx));
                    return;
                  }

                  auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

                  if (val.empty()) {
                    // this should only happen for systems that do not guarantee
                    // opacity (e.g., MVTSO)
                    ctx->curr_o_id++;
                    CheckOrder(client, std::move(app_ctx));
                  } else {
                    ::tpcc::OrderRow o_row;
                    UW_ASSERT(o_row.ParseFromString(val));
                    ctx->curr_ol_cnt = o_row.ol_cnt();

                    CheckOrderLine(client, std::move(app_ctx), 0);
                  }
                });
  } else {
    client->Commit(std::move(app_ctx), ccb_);
  }
}

void StockLevel::CheckOrderLine(Client *client,
                                std::unique_ptr<AppContext> app_ctx,
                                size_t ol_number) {
  auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

  if (ol_number < ctx->curr_ol_cnt) {
    client->Get(
        std::move(app_ctx),
        ::tpcc::OrderLineRowKey(w_id, d_id, ctx->curr_o_id, ol_number),
        [this, client, ol_number](
            std::unique_ptr<AppContext> app_ctx, transaction_op_status_t status,
            const std::string &key, const std::string &val) {
          if (status == FAIL) {
            client->Abort(app_ctx);
            acb_(std::move(app_ctx));
            return;
          }

          auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

          if (val.empty()) {
            // this should only happen for systems that do not guarantee opacity
            // (e.g., MVTSO)
            ctx->curr_o_id++;
            CheckOrder(client, std::move(app_ctx));
          } else {
            ::tpcc::OrderLineRow ol_row;
            UW_ASSERT(ol_row.ParseFromString(val));

            client->Get(
                std::move(app_ctx), ::tpcc::StockRowKey(w_id, ol_row.i_id()),
                [this, client, ol_number](std::unique_ptr<AppContext> app_ctx,
                                          transaction_op_status_t status,
                                          const std::string &key,
                                          const std::string &val) {
                  if (status == FAIL) {
                    client->Abort(app_ctx);
                    acb_(std::move(app_ctx));
                    return;
                  }

                  ::tpcc::StockRow s_row;
                  UW_ASSERT(s_row.ParseFromString(val));

                  CheckOrderLine(client, std::move(app_ctx), ol_number + 1);
                });
          }
        });
  } else {
    ctx->curr_o_id++;
    CheckOrder(client, std::move(app_ctx));
  }
}

void StockLevel::GetDistrictRO(Client *client,
                               std::unique_ptr<AppContext> ctx) {
  std::unordered_set<std::string> keys = {
      ::tpcc::DistrictRowKey(w_id, d_id),
  };

  client->ROGet(
      std::move(ctx), keys,
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status,
                     const std::unordered_map<std::string, std::string> &kvs) {
        auto key = ::tpcc::DistrictRowKey(w_id, d_id);
        auto &val = kvs.at(key);

        auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

        ::tpcc::DistrictRow d_row;
        UW_ASSERT(d_row.ParseFromString(val));

        ctx->next_o_id = d_row.next_o_id();
        ctx->curr_o_id = ctx->next_o_id - 20;
        CheckOrdersRO(client, std::move(app_ctx));
      });
}

void StockLevel::CheckOrdersRO(Client *client,
                               std::unique_ptr<AppContext> app_ctx) {
  auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

  std::unordered_set<std::string> keys;
  keys.reserve(ctx->next_o_id - ctx->curr_o_id);
  for (auto i = ctx->curr_o_id; i < ctx->next_o_id; ++i) {
    keys.emplace(::tpcc::OrderRowKey(w_id, d_id, i));
  }

  client->ROGet(
      std::move(app_ctx), keys,
      [this, client](std::unique_ptr<AppContext> app_ctx,
                     transaction_op_status_t status,
                     const std::unordered_map<std::string, std::string> &kvs) {
        auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

        std::vector<std::uint32_t> ol_cnts;
        ol_cnts.reserve(ctx->next_o_id - ctx->curr_o_id);
        for (auto i = ctx->curr_o_id; i < ctx->next_o_id; ++i) {
          auto key = ::tpcc::OrderRowKey(w_id, d_id, i);
          auto &val = kvs.at(key);

          if (val.empty()) {
          } else {
            ::tpcc::OrderRow o_row;
            UW_ASSERT(o_row.ParseFromString(val));
            ol_cnts.emplace_back(o_row.ol_cnt());
          }
        }

        CheckOrderLinesRO(client, std::move(app_ctx), ol_cnts);
      });
}

void StockLevel::CheckOrderLinesRO(Client *client,
                                   std::unique_ptr<AppContext> app_ctx,
                                   std::vector<std::uint32_t> ol_cnts) {
  auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

  std::unordered_set<std::string> keys;
  for (auto i = ctx->curr_o_id; i < ctx->next_o_id; ++i) {
    auto ol_cnt = ol_cnts[i - ctx->curr_o_id];
    for (std::uint32_t j = 0; j < ol_cnt; ++j) {
      keys.emplace(::tpcc::OrderLineRowKey(w_id, d_id, i, j));
    }
  }

  client->ROGet(
      std::move(app_ctx), keys,
      [this, client, ol_cnts](
          std::unique_ptr<AppContext> app_ctx, transaction_op_status_t status,
          const std::unordered_map<std::string, std::string> &kvs) {
        auto *ctx = dynamic_cast<StockLevelContext *>(app_ctx.get());

        std::unordered_set<std::string> i_keys;
        for (auto i = ctx->curr_o_id; i < ctx->next_o_id; ++i) {
          auto ol_cnt = ol_cnts[i - ctx->curr_o_id];
          for (std::uint32_t j = 0; j < ol_cnt; ++j) {
            auto key = ::tpcc::OrderLineRowKey(w_id, d_id, i, j);
            auto &val = kvs.at(key);

            if (val.empty()) {
            } else {
              ::tpcc::OrderLineRow ol_row;
              UW_ASSERT(ol_row.ParseFromString(val));
              i_keys.emplace(::tpcc::StockRowKey(w_id, ol_row.i_id()));
            }
          }
        }

        client->ROGet(
            std::move(app_ctx), i_keys,
            [this, client, i_keys](
                std::unique_ptr<AppContext> app_ctx,
                transaction_op_status_t status,
                const std::unordered_map<std::string, std::string> &kvs) {
              for (auto &i_key : i_keys) {
                auto &val = kvs.at(i_key);
                ::tpcc::StockRow s_row;
                UW_ASSERT(s_row.ParseFromString(val));
              }

              client->ROCommit(std::move(app_ctx), ccb_);
            });
      });
}

StockLevel::StockLevelContext::StockLevelContext() = default;

StockLevel::StockLevelContext::StockLevelContext(const StockLevelContext &other)
    : next_o_id(other.next_o_id),
      curr_o_id(other.curr_o_id),
      curr_ol_cnt(other.curr_ol_cnt) {}

StockLevel::StockLevelContext::~StockLevelContext() = default;

std::unique_ptr<AppContext> StockLevel::StockLevelContext::CloneInternal()
    const {
  return std::make_unique<StockLevelContext>(*this);
}
}  // namespace tpcc
}  // namespace context
