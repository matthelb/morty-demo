// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/lockstore.h:
 *    Single-node Key-value store with support for 2PC locking-based
 *    transactions using S2PL
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

#ifndef _STRONG_LOCK_STORE_H_
#define _STRONG_LOCK_STORE_H_

#include <bits/stdint-uintn.h>

#include <map>
#include <string>
#include <unordered_map>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/backend/kvstore.h"
#include "store/common/backend/lockserver.h"
#include "store/common/backend/txnstore.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace strongstore {

class LockStore : public TxnStore {
 public:
  LockStore();
  ~LockStore() override;

  int Get(uint64_t id, const std::string &key,
          std::pair<Timestamp, std::string> &value) override;
  int Get(uint64_t id, const std::string &key, const Timestamp &timestamp,
          std::pair<Timestamp, std::string> &value,
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
  // Data store.
  KVStore store;

  // Locks manager.
  LockServer locks;

  std::map<uint64_t, Transaction> prepared;

  void DropLocks(uint64_t id, const Transaction &txn);
  bool GetLocks(uint64_t id, const Transaction &txn);
};

}  // namespace strongstore

#endif /* _STRONG_LOCK_STORE_H_ */
