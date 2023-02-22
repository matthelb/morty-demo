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

#ifndef STORE_BENCHMARK_RETWIS_RETWIS_TRANSACTION_H_
#define STORE_BENCHMARK_RETWIS_RETWIS_TRANSACTION_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "store/benchmark/common/key_selector.h"
#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"

namespace retwis {

class RetwisTransaction {
 public:
  RetwisTransaction(const std::vector<KeySelector *> &keySelector, int numKeys,
                    bool single_shard, std::mt19937 *rand);
  virtual ~RetwisTransaction();

 protected:
  inline const std::string &GetKey(int i) const {
    return keySelector[keyIdxs[i].first]->GetKey(keyIdxs[i].second);
  }

  inline const size_t GetNumKeys() const { return keyIdxs.size(); }

  const std::vector<KeySelector *> &keySelector;

 private:
  std::vector<std::pair<uint64_t, int>> keyIdxs;
};

}  // namespace retwis

#endif  // STORE_BENCHMARK_RETWIS_TRANSACTION_H_
