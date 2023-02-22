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
#ifndef STORE_BENCHMARK_RETWIS_GET_TIMELINE_H_
#define STORE_BENCHMARK_RETWIS_GET_TIMELINE_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "store/benchmark/retwis/retwis_transaction.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/txncontext.h"

class KeySelector;

namespace context {

namespace retwis {

class GetTimeline : public AsyncTransaction,
                    public ::retwis::RetwisTransaction {
 public:
  GetTimeline(commit_callback ccb, abort_callback acb,
              const std::vector<KeySelector *> &keySelector, bool single_shard,
              std::mt19937 *rand);
  ~GetTimeline() override;

  void Execute(Client *client, uint32_t timeout) override;

 private:
  class GetTimelineContext : public AppContext {
   public:
    GetTimelineContext();
    GetTimelineContext(const GetTimelineContext &other);
    ~GetTimelineContext() override;

   protected:
    virtual std::unique_ptr<AppContext> CloneInternal() const;
  };

  void GetTimelineItemsRO(Client *client, std::unique_ptr<AppContext> ctx);
  void GetTimelineItem(Client *client, std::unique_ptr<AppContext> ctx,
                       size_t item_idx);
};

}  // namespace retwis

}  // namespace context

#endif  // STORE_BENCHMARK_RETWIS_GET_TIMELINE_H_