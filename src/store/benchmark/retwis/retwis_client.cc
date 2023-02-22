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
#include "store/benchmark/retwis/retwis_client.h"

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "store/benchmark/retwis/add_user.h"
#include "store/benchmark/retwis/follow.h"
#include "store/benchmark/retwis/get_timeline.h"
#include "store/benchmark/retwis/post_tweet.h"
#include "store/common/frontend/async_transaction.h"

class AsyncClient;
class KeySelector;
class Transport;

namespace retwis {

RetwisClient::RetwisClient(const std::vector<KeySelector *> &keySelector,
                           bool single_shard, AsyncClient *client,
                           ::context::Client *context_client,
                           Transport &transport, uint64_t id, int numRequests,
                           int expDuration, uint64_t delay, int warmupSec,
                           int cooldownSec, int tputInterval,
                           uint32_t abortBackoff, bool retryAborted,
                           uint32_t maxBackoff, uint32_t maxAttempts,
                           bool output_measurements,
                           const std::string &latencyFilename)
    : BenchClient(context_client, transport, id, numRequests, expDuration,
                  delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
                  retryAborted, maxBackoff, maxAttempts, output_measurements,
                  latencyFilename),
      keySelector(keySelector),
      single_shard_(single_shard) {}

RetwisClient::~RetwisClient() = default;

::context::AsyncTransaction *RetwisClient::GetNextContextTransaction(
    context::commit_callback ccb, context::abort_callback acb) {
  uint32_t ttype = std::uniform_int_distribution<uint32_t>(0, 99)(GetRand());
  if (ttype < 5UL) {
    lastOp = "add_user";
    return new ::context::retwis::AddUser(ccb, acb, keySelector, single_shard_,
                                          &GetRand());
  }
  if (ttype < 20UL) {
    lastOp = "follow";
    return new ::context::retwis::Follow(ccb, acb, keySelector, single_shard_,
                                         &GetRand());
  }
  if (ttype < 50UL) {  // change to 100/true to avoid any timeline.
    lastOp = "post_tweet";
    return new ::context::retwis::PostTweet(ccb, acb, keySelector,
                                            single_shard_, &GetRand());
  } else {
    lastOp = "get_timeline";
    return new ::context::retwis::GetTimeline(ccb, acb, keySelector,
                                              single_shard_, &GetRand());
  }
}

std::string RetwisClient::GetLastOp() const { return lastOp; }

}  // namespace retwis
