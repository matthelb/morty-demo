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
#include "store/benchmark/retwis/get_timeline.h"

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/message.h"

class KeySelector;

namespace context {

namespace retwis {

GetTimeline::GetTimeline(commit_callback ccb, abort_callback acb,
                         const std::vector<KeySelector *> &keySelector,
                         bool single_shard, std::mt19937 *rand)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)),
      ::retwis::RetwisTransaction(keySelector, 1 + (*rand)() % 10, single_shard,
                                  rand) {}

GetTimeline::~GetTimeline() = default;

void GetTimeline::Execute(Client *client, uint32_t timeout) {
  Debug("GET_TIMELINE");
  std::unique_ptr<AppContext> ctx(new GetTimelineContext());
  if (client->SupportsRO()) {
    client->ROBegin(ctx, acb_);
    GetTimelineItemsRO(client, std::move(ctx));
  } else {
    client->Begin(ctx, acb_);
    GetTimelineItem(client, std::move(ctx), 0);
  }
}

GetTimeline::GetTimelineContext::GetTimelineContext() = default;

GetTimeline::GetTimelineContext::GetTimelineContext(
    const GetTimelineContext &other) {}

GetTimeline::GetTimelineContext::~GetTimelineContext() = default;

std::unique_ptr<AppContext> GetTimeline::GetTimelineContext::CloneInternal()
    const {
  return std::make_unique<GetTimelineContext>(*this);
}

void GetTimeline::GetTimelineItemsRO(Client *client,
                                     std::unique_ptr<AppContext> ctx) {
  std::unordered_set<std::string> keys{};
  keys.reserve(GetNumKeys());
  for (std::size_t i = 0; i < GetNumKeys(); ++i) {
    keys.emplace(GetKey(i));
  }

  client->ROGet(
      std::move(ctx), keys,
      [this, client](std::unique_ptr<AppContext> ctx,
                     transaction_op_status_t status,
                     const std::unordered_map<std::string, std::string> &kvs) {
        client->ROCommit(std::move(ctx), ccb_);
      });
}

void GetTimeline::GetTimelineItem(Client *client,
                                  std::unique_ptr<AppContext> ctx,
                                  size_t item_idx) {
  if (item_idx >= GetNumKeys()) {
    client->Commit(std::move(ctx), ccb_);
  } else {
    client->Get(std::move(ctx), GetKey(item_idx),
                [this, client, item_idx](std::unique_ptr<AppContext> ctx,
                                         transaction_op_status_t status,
                                         const std::string &key,
                                         const std::string &val) {
                  GetTimelineItem(client, std::move(ctx), item_idx + 1);
                });
  }
}

}  // namespace retwis

}  // namespace context
