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
#include "store/benchmark/retwis/retwis_transaction.h"

#include <cstdint>

namespace retwis {

RetwisTransaction::RetwisTransaction(
    const std::vector<KeySelector *> &keySelector, int numKeys,
    bool single_shard, std::mt19937 *rand)
    : keySelector(keySelector) {
  std::uniform_int_distribution<uint64_t> dist(0, keySelector.size() - 1);
  uint64_t ks_idx = dist(*rand);
  for (int i = 0; i < numKeys; ++i) {
    keyIdxs.emplace_back(ks_idx, keySelector[ks_idx]->GetKey(rand));
    if (!single_shard && keySelector.size() > 1UL) {
      ks_idx = dist(*rand);
    }
  }
}

RetwisTransaction::~RetwisTransaction() = default;

}  // namespace retwis
