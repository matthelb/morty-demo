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
#include "store/benchmark/rw/rw_client.h"

#include "store/benchmark/rw/rw_transaction.h"

class KeySelector;
class Transport;

namespace rw {

RWClient::RWClient(const std::vector<KeySelector *> &keySelector,
                   uint64_t numKeys, ::context::Client *context_client,
                   Transport &transport, uint64_t id, int numRequests,
                   int expDuration, uint64_t delay, int warmupSec,
                   int cooldownSec, int tputInterval, uint32_t abortBackoff,
                   bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
                   bool output_measurements, const std::string &latencyFilename)
    : BenchClient(context_client, transport, id, numRequests, expDuration,
                  delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
                  retryAborted, maxBackoff, maxAttempts, output_measurements,
                  latencyFilename),
      keySelector(keySelector),
      numKeys(numKeys) {}

RWClient::~RWClient() = default;

::context::AsyncTransaction *RWClient::GetNextContextTransaction(
    context::commit_callback ccb, context::abort_callback acb) {
  return new ::context::rw::RWTransaction(ccb, acb, keySelector, numKeys,
                                          &GetRand());
}

std::string RWClient::GetLastOp() const { return "rw"; }

}  // namespace rw
