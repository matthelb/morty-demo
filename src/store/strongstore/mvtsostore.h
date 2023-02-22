// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/occstore.h:
 *   Key-value store with support for transactions with strong consistency using
 *OCC.
 *
 * Copyright 2013-2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                     Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *                     Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _STRONG_MVTSO_STORE_H_
#define _STRONG_MVTSO_STORE_H_

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/backend/store.h"
#include "store/common/backend/txnstore.h"
#include "store/common/backend/versionstore.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace strongstore {

class MVTSOStore : public TxnStore {
 public:
  explicit MVTSOStore(int64_t maxDepDepth);
  ~MVTSOStore() override;

  int Get(uint64_t id, const std::string &key, const Timestamp &timestamp,
          std::pair<Timestamp, std::string> &value,
          std::unordered_map<uint64_t, int> &statuses) override;

  int Put(uint64_t id, const std::string &key, const std::string &value,
          const Timestamp &timestamp,
          std::unordered_map<uint64_t, int> &statuses) override;

  int Prepare(uint64_t id, const Transaction &txn,
              std::unordered_map<uint64_t, int> &statuses) override;
  void Commit(uint64_t id, const Timestamp &ts,
              std::unordered_map<uint64_t, int> &statuses) override;
  void Abort(uint64_t id, const Transaction &txn,
             std::unordered_map<uint64_t, int> &statuses) override;
  void Load(const std::string &key, const std::string &value,
            const Timestamp &timestamp) override;

 private:
  void PrepareTransaction(uint64_t id, const Transaction &txn);
  void Clean(uint64_t id);
  void CommitInternal(uint64_t id, std::unordered_map<uint64_t, int> &statuses);
  void AbortInternal(uint64_t id, const Timestamp &ts,
                     const std::unordered_set<std::string> &writes,
                     std::unordered_map<uint64_t, int> &statuses);
  void CleanInternal(uint64_t id, const Transaction &txn);
  void UpdateDepDepth(uint64_t id);
  void CheckDependents(uint64_t id,
                       std::unordered_map<uint64_t, int> &statuses);

  const int64_t maxDepDepth;

  VersionedKVStore<Timestamp, std::string> store;
  std::unordered_map<std::string, Timestamp> rts;
  std::unordered_map<std::string, Timestamp> committedTs;
  std::unordered_map<uint64_t, Transaction> waiting;
  std::unordered_map<uint64_t, std::unordered_set<uint64_t>> dependents;
  std::unordered_map<uint64_t, std::unordered_set<uint64_t>>
      outstandingDependencies;
  std::unordered_set<uint64_t> committed;
  std::unordered_set<uint64_t> aborted;
  std::unordered_map<uint64_t,
                     std::pair<Timestamp, std::unordered_set<std::string>>>
      txnWrites;
  std::unordered_map<uint64_t, int64_t> depDepth;

  std::unordered_map<uint64_t, Transaction> prepared;
  std::unordered_map<std::string, std::unordered_set<uint64_t>> pWrites;
  std::unordered_map<std::string, std::unordered_set<uint64_t>> pRW;
};

}  // namespace strongstore

#endif /* _STRONG_MVTSO_STORE_H_ */
