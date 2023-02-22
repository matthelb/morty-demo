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
#include "store/benchmark/rw/rw_transaction.h"

#include <ext/alloc_traits.h>

#include <cstdint>
#include <utility>

#include "lib/assert.h"
#include "store/benchmark/common/key_selector.h"

namespace context {

namespace rw {

RWTransaction::RWTransaction(commit_callback ccb, abort_callback acb,
                             const std::vector<KeySelector *> &keySelector,
                             size_t numOps, std::mt19937 *rand)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)),
      numOps(numOps) {
  UW_ASSERT(numOps % 2 == 0);
  UW_ASSERT(numOps > 0);
  // perform a read and a write for each key
  uint64_t ks_idx = (*rand)() % keySelector.size();
  for (size_t i = 0; i < numOps / 2; ++i) {
    keys.push_back(
        &keySelector[ks_idx]->GetKey(keySelector[ks_idx]->GetKey(rand)));
  }
}

RWTransaction::~RWTransaction() = default;

void RWTransaction::Execute(Client *client, uint32_t timeout) {
  std::unique_ptr<AppContext> ctx(new RWContext());
  client->Begin(ctx, acb_);
  ReadModifyWrite(client, std::move(ctx), 0);
}

void RWTransaction::ReadModifyWrite(Client *client,
                                    std::unique_ptr<AppContext> ctx,
                                    size_t keyIdx) {
  client->Get(
      std::move(ctx), *keys[keyIdx],
      [this, client, keyIdx](std::unique_ptr<AppContext> tctx,
                             transaction_op_status_t status,
                             const std::string &key, const std::string &val) {
        auto *ctx = dynamic_cast<RWContext *>(tctx.get());
        ctx->readValInt.push_back(0);
        if (val.length() != 0) {
          UW_ASSERT(val.length() == sizeof(ctx->readValInt[keyIdx]));
          ctx->readValInt[keyIdx] =
              *reinterpret_cast<const uint64_t *>(val.data());
        }
        ctx->writeValInt.push_back(ctx->readValInt[keyIdx] + 1);
        std::string putVal(
            reinterpret_cast<const char *>(&ctx->writeValInt[keyIdx]),
            sizeof(ctx->writeValInt[keyIdx]));
        client->Put(tctx, *keys[keyIdx], putVal);
        if (2 * (keyIdx + 1) < numOps) {
          ReadModifyWrite(client, std::move(tctx), keyIdx + 1);
        } else {
          client->Commit(std::move(tctx), ccb_);
        }
      });
}

RWTransaction::RWContext::RWContext() = default;

RWTransaction::RWContext::RWContext(const RWContext &other)
    : readValInt(other.readValInt), writeValInt(other.writeValInt) {}

RWTransaction::RWContext::~RWContext() = default;

std::unique_ptr<AppContext> RWTransaction::RWContext::CloneInternal() const {
  return std::make_unique<RWContext>(*this);
}

}  // namespace rw

}  // namespace context
