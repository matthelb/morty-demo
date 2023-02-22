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
#ifndef _JANUS_STORE_H_
#define _JANUS_STORE_H_

#include <bits/stdint-uintn.h>

#include <string>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "store/janusstore/transaction.h"

namespace janusstore {

class Store {
 public:
  Store();
  ~Store();

  int Get(uint64_t id, const std::string& key, std::string& value);
  int Put(uint64_t id, const std::string& key, std::string value);
  string Read(const std::string& key);

 private:
  // unversioned data store (for a particular shard, i would think)
  std::unordered_map<std::string, std::string> kv_store;

  // TODO(matthelb): in janus, this store doesnt seem very interesting because
  // commit logic for a transaction is in the replica (server.h) that wraps
  // this store
};

}  // namespace janusstore

#endif /* _JANUS_STORE_H_ */
