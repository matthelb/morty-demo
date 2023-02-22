// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/store.h:
 *   Key-value store with support for transactions using TAPIR.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#ifndef _TAPIR_STORE_H_
#define _TAPIR_STORE_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/assert.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "store/common/backend/txnstore.h"
#include "store/common/backend/versionstore.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace tapirstore {

class Store : public TxnStore {
 public:
  explicit Store(bool linearizable);
  ~Store() override;

  // Overriding from TxnStore
  int Get(uint64_t id, const std::string &key,
          std::pair<Timestamp, std::string> &value) override;
  int Get(uint64_t id, const std::string &key, const Timestamp &timestamp,
          std::pair<Timestamp, std::string> &value,
          std::unordered_map<uint64_t, int> &statuses) override;
  int Prepare(uint64_t id, const Transaction &txn, const Timestamp &timestamp,
              Timestamp &proposedTimestamp) override;
  void Commit(uint64_t id, const Timestamp &timestamp,
              std::unordered_map<uint64_t, int> &statuses) override;
  void Abort(uint64_t id, const Transaction &txn,
             std::unordered_map<uint64_t, int> &statuses) override;
  void Load(const std::string &key, const std::string &value,
            const Timestamp &timestamp) override;

 private:
  // Are we running in linearizable (vs serializable) mode?
  bool linearizable;

  // Data store
  VersionedKVStore<Timestamp, std::string> store;

  // TODO(matthelb): comment this.
  std::unordered_map<uint64_t, std::pair<Timestamp, const Transaction *>>
      prepared;
  std::unordered_map<std::string, Latency_t *> prepared_time;
  std::unordered_map<std::string, Latency_t *> conflict_windows;

  std::unordered_map<uint64_t, Transaction> ongoing;
  std::unordered_map<std::string, std::set<Timestamp>> prepared_writes;
  std::unordered_map<std::string, std::set<Timestamp>> prepared_reads;

  void GetPreparedWrites(
      std::unordered_map<std::string, std::set<Timestamp>> &writes);
  void GetPreparedReads(
      std::unordered_map<std::string, std::set<Timestamp>> &reads);
  void Commit(const Timestamp &timestamp, const Transaction &txn);
  void Cleanup(uint64_t txnId);
  size_t debug_counter = 0;
};

}  // namespace tapirstore

#endif /* _TAPIR_STORE_H_ */
