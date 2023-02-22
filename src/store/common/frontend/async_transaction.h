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
#ifndef _ASYNC_TRANSACTION_H_
#define _ASYNC_TRANSACTION_H_

#include <bits/stdint-uintn.h>

#include <functional>
#include <map>
#include <string>

#include "store/common/frontend/client.h"
#include "store/common/frontend/transaction_utils.h"

struct StringPointerComp {
  bool operator()(const std::string *a, const std::string *b) const {
    return *a < *b;
  }
};

typedef std::map<const std::string *, const std::string *, StringPointerComp>
    ReadValueMap;

class AsyncTransaction {
 public:
  AsyncTransaction() = default;
  virtual ~AsyncTransaction() = default;

  virtual Operation GetNextOperation(size_t outstandingOpCount,
                                     size_t finishedOpCount,
                                     const ReadValueMap &readValues) = 0;
};

namespace context {

using execute_callback = int;

class AsyncTransaction {
 public:
  AsyncTransaction(commit_callback ccb, abort_callback acb)
      : ccb_(ccb), acb_(acb) {}
  virtual ~AsyncTransaction() = default;

  virtual void Execute(Client *client, uint32_t timeout) = 0;
  virtual void Retry(Client *client, uint32_t timeout) {
    Execute(client, timeout);
  }

 protected:
  commit_callback ccb_;
  abort_callback acb_;
};

}  // namespace context

#endif
