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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
#include "store/janusstore/store.h"

#include <utility>

#include "store/common/transaction.h"

namespace janusstore {

using namespace std;

Store::Store() {}

Store::~Store() { /* TODO delete kv_store? */
}

int Store::Get(uint64_t id, const string& key, string& value) {
  // Debug("[%llu] GET %s", id, key.c_str());

  // TODO(matthelb): unsure if we need to deref key?
  auto ret = kv_store.find(key);

  if (ret == kv_store.end()) {
    // TODO(matthelb): this debug complains about type of key at compile time
    // Debug("Cannot find value for key %s", key.c_str());
    return REPLY_FAIL;
  }
  // Debug("Value: %s", ret->second.c_str());
  value = ret->second;
  return REPLY_OK;
}

int Store::Put(uint64_t id, const string& key, string value) {
  // Debug("[%llu] PUT <%s, %s>", id, key.c_str(), value.c_str());

  kv_store[key] = std::move(value);
  return REPLY_OK;
}

string Store::Read(const string& key) {
  if (kv_store.find(key) == kv_store.end()) {
    return "NOT FOUND";
  }
  return kv_store[key];
}
}  // namespace janusstore
