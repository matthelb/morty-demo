// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/strongstore/occstore.cc:
 *   Key-value store with support for strong consistency using MVTSO
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

#include "store/strongstore/mvtsostore.h"

#include <cstdint>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common.h"
#include "store/common/stats.h"

namespace strongstore {

MVTSOStore::MVTSOStore(int64_t maxDepDepth) : maxDepDepth(maxDepDepth) {
  committed.insert(0);
  depDepth.insert(std::make_pair(0, -1));
}

MVTSOStore::~MVTSOStore() = default;

int MVTSOStore::Get(uint64_t id, const std::string &key,
                    const Timestamp &timestamp,
                    std::pair<Timestamp, std::string> &value,
                    std::unordered_map<uint64_t, int> &statuses) {
  auto txnWritesItr = txnWrites.find(id);
  // TODO(matthelb): need to record timestamp even if txn has no writes. for
  // now, add
  //   to same structure that records txn ts+writes.
  if (txnWritesItr == txnWrites.end()) {
    Debug("[%lu] GET inserting empty set into txn writes.", id);
    auto insertItr = txnWrites.insert(std::make_pair(
        id, std::make_pair(timestamp, std::unordered_set<std::string>())));
    txnWritesItr = insertItr.first;
  }

  bool found = store.get(key, timestamp, value);

  if (!found) {
    auto rtsItr = rts.find(key);
    if (rtsItr == rts.end()) {
      rts.insert(std::make_pair(key, timestamp));
    } else if (rtsItr->second < timestamp) {
      rtsItr->second = timestamp;
    }

    return REPLY_OK;
  }

  Debug("[%lu] GET with ts %lu.%lu for key %s; return ts %lu.%lu.", id,
        timestamp.getTimestamp(), timestamp.getID(),
        BytesToHex(key, 16).c_str(), value.first.getTimestamp(),
        value.first.getID());

  if (maxDepDepth > -1) {
    auto depthItr = depDepth.find(value.first.getID());
    UW_ASSERT(depthItr != depDepth.end());

    int64_t depth = depthItr->second;

    if (depth + 1 > maxDepDepth) {
      AbortInternal(id, txnWritesItr->second.first, txnWritesItr->second.second,
                    statuses);
      return REPLY_FAIL;
    }

    depthItr = depDepth.find(id);
    int64_t myDepth = depthItr == depDepth.end() ? 0 : depthItr->second;

    if (depth + 1 > myDepth) {
      depDepth[id] = depth + 1;
    }
  }

  auto rtsItr = rts.find(key);
  if (rtsItr == rts.end()) {
    rts.insert(std::make_pair(key, timestamp));
  } else if (rtsItr->second < timestamp) {
    rtsItr->second = timestamp;
  }

  return REPLY_OK;
}

int MVTSOStore::Put(uint64_t id, const std::string &key,
                    const std::string &value, const Timestamp &timestamp,
                    std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] PUT with ts %lu.%lu for key %s.", id, timestamp.getTimestamp(),
        timestamp.getID(), BytesToHex(key, 16).c_str());

  auto rtsItr = rts.find(key);
  if (rtsItr != rts.end() && timestamp < rtsItr->second) {
    Debug("[%lu] ABORT wts %lu.%lu < rts %lu.%lu for key %s.", id,
          timestamp.getTimestamp(), timestamp.getID(),
          rtsItr->second.getTimestamp(), rtsItr->second.getID(),
          BytesToHex(key, 16).c_str());
    stats.Increment("abort_wts_smaller_than_rts");
    auto txnWritesItr = txnWrites.find(id);
    if (txnWritesItr == txnWrites.end()) {
      AbortInternal(id, Timestamp(), std::unordered_set<std::string>(),
                    statuses);
    } else {
      AbortInternal(id, txnWritesItr->second.first, txnWritesItr->second.second,
                    statuses);
    }
    return REPLY_FAIL;
  }

  auto txnWritesItr = txnWrites.find(id);
  if (txnWritesItr == txnWrites.end()) {
    Debug("[%lu] PUT inserting empty set into txn writes.", id);
    auto insertItr = txnWrites.insert(std::make_pair(
        id, std::make_pair(timestamp, std::unordered_set<std::string>())));
    txnWritesItr = insertItr.first;
  }
  txnWritesItr->second.second.insert(key);

  if (maxDepDepth > -1 && depDepth.find(id) == depDepth.end()) {
    depDepth[id] = 0;
  }

  store.put(key, value, timestamp);
  return REPLY_OK;
}

int MVTSOStore::Prepare(uint64_t id, const Transaction &txn,
                        std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] PREPARE.", id);

  auto waitingItr = waiting.find(id);
  if (waitingItr != waiting.end() || committed.find(id) != committed.end()) {
    return REPLY_OK;
  }

  if (aborted.find(id) != aborted.end()) {
    return REPLY_FAIL;
  }

  waiting.insert(std::make_pair(id, txn));
  bool outstandingDeps = false;
  for (const auto &read : txn.getReadSet()) {
    if (aborted.find(read.second.getID()) != aborted.end()) {
      auto txnWritesItr = txnWrites.find(id);
      UW_ASSERT(txnWritesItr != txnWrites.end());
      stats.Increment("abort_dep_aborted");
      Debug("[%lu] ABORT dep %lu is aborted.", id, read.second.getID());
      AbortInternal(id, txnWritesItr->second.first, txnWritesItr->second.second,
                    statuses);
      return REPLY_FAIL;
    }
    if (committed.find(read.second.getID()) == committed.end()) {
      Debug("[%lu] Dep %lu is not committed.", id, read.second.getID());
      outstandingDependencies[id].insert(read.second.getID());
      dependents[read.second.getID()].insert(id);
      outstandingDeps = true;
    } else {
      Debug("[%lu] Dep %lu is committed.", id, read.second.getID());
    }
  }

  if (outstandingDeps) {
    return REPLY_WAITING;
  }

  return REPLY_OK;
}

void MVTSOStore::CommitInternal(uint64_t id,
                                std::unordered_map<uint64_t, int> &statuses) {
  committed.insert(id);
  depDepth[id] = -1;
  // Debug("[%lu] COMMIT Removing writes from txn %lu.", id, id);
  txnWrites.erase(id);
  CheckDependents(id, statuses);
}

void MVTSOStore::CheckDependents(uint64_t id,
                                 std::unordered_map<uint64_t, int> &statuses) {
  auto dependentsItr = dependents.find(id);
  if (dependentsItr != dependents.end()) {
    for (const auto &dependent : dependentsItr->second) {
      auto outstandingDepsItr = outstandingDependencies.find(dependent);
      if (outstandingDepsItr == outstandingDependencies.end()) {
        UW_ASSERT(aborted.find(dependent) != aborted.end());
        continue;
      }

      outstandingDepsItr->second.erase(id);
      if (outstandingDepsItr->second.empty()) {
        statuses.insert(std::make_pair(dependent, REPLY_OK));
        CheckDependents(dependent, statuses);
        outstandingDependencies.erase(outstandingDepsItr);
      } else if (maxDepDepth > -1) {
        UpdateDepDepth(dependent);
      }
    }
    dependents.erase(dependentsItr);
  }
  waiting.erase(id);
}

void MVTSOStore::UpdateDepDepth(uint64_t id) {
  int64_t maxDepth = 0;
  auto outstandingDepsItr = outstandingDependencies.find(id);
  if (outstandingDepsItr == outstandingDependencies.end()) {
    return;
  }

  for (const auto &dependency : outstandingDepsItr->second) {
    auto depDepthItr = depDepth.find(dependency);
    UW_ASSERT(depDepthItr != depDepth.end());
    if (depDepthItr->second > maxDepth) {
      maxDepth = depDepthItr->second;
    }
  }
  depDepth[id] = maxDepth + 1;

  auto dependentsItr = dependents.find(id);
  if (dependentsItr != dependents.end()) {
    for (const auto &dependent : dependentsItr->second) {
      UpdateDepDepth(dependent);
    }
  }
}

void MVTSOStore::AbortInternal(uint64_t id, const Timestamp &ts,
                               const std::unordered_set<std::string> &writes,
                               std::unordered_map<uint64_t, int> &statuses) {
  auto aborted_itr = aborted.insert(id);
  if (!aborted_itr.second) {
    return;
  }

  depDepth.erase(id);
  for (const auto &write : writes) {
    // Debug("[%lu] ABORT Removing write to key %s with ts %lu.%lu.", id,
    //    BytesToHex(write, 16).c_str(), ts.getTimestamp(), ts.getID());
    UW_ASSERT(store.remove(write, ts));
  }
  // Debug("[%lu] ABORT Removing writes from txn %lu.", id, id);
  txnWrites.erase(id);
  auto dependentsItr = dependents.find(id);
  if (dependentsItr != dependents.end()) {
    for (const auto &dependent : dependentsItr->second) {
      if (aborted.find(dependent) != aborted.end()) {
        // already aborted
        continue;
      }

      statuses.insert(std::make_pair(dependent, REPLY_FAIL));
      Debug("[%lu] ABORT dep %lu aborted.", id, dependent);
      stats.Increment("abort_dep_aborted");

      auto txnWritesItr = txnWrites.find(dependent);
      if (txnWritesItr == txnWrites.end()) {
        // we can use an empty timestamp because the timestamp is only used
        //   to remove the associated writes. we should only be in this case if
        //   the txn has no writes.
        AbortInternal(dependent, Timestamp(), std::unordered_set<std::string>(),
                      statuses);
      } else {
        AbortInternal(dependent, txnWritesItr->second.first,
                      txnWritesItr->second.second, statuses);
      }
    }
    dependents.erase(dependentsItr);
  }
  outstandingDependencies.erase(id);
  waiting.erase(id);
}

void MVTSOStore::CleanInternal(uint64_t id, const Transaction &txn) {}

void MVTSOStore::Commit(uint64_t id, const Timestamp &ts,
                        std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] COMMIT.", id);

  CommitInternal(id, statuses);
}

void MVTSOStore::Abort(uint64_t id, const Transaction &txn,
                       std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] ABORT.", id);

  auto txnWritesItr = txnWrites.find(id);
  if (txnWritesItr == txnWrites.end()) {
    Debug("[%lu] ABORT with no writes at this replica.", id);
    AbortInternal(id, Timestamp(), std::unordered_set<std::string>(), statuses);
  } else {
    Debug("[%lu] ABORT with %lu writes at this replica.", id,
          txnWritesItr->second.second.size());
    AbortInternal(id, txnWritesItr->second.first, txnWritesItr->second.second,
                  statuses);
  }
}

void MVTSOStore::Load(const std::string &key, const std::string &value,
                      const Timestamp &timestamp) {
  store.put(key, value, timestamp);
  committed.insert(timestamp.getID());
}

void MVTSOStore::PrepareTransaction(uint64_t id, const Transaction &txn) {
  prepared.insert(std::make_pair(id, txn));

  for (auto &write : txn.getWriteSet()) {
    pWrites[write.first].insert(id);
    pRW[write.first].insert(id);
  }
  for (auto &read : txn.getReadSet()) {
    pRW[read.first].insert(id);
  }
}

void MVTSOStore::Clean(uint64_t id) {
  auto preparedItr = prepared.find(id);
  if (preparedItr != prepared.end()) {
    for (const auto &write : preparedItr->second.getWriteSet()) {
      auto preparedWriteItr = pWrites.find(write.first);
      if (preparedWriteItr != pWrites.end()) {
        preparedWriteItr->second.erase(id);
        if (preparedWriteItr->second.empty()) {
          pWrites.erase(preparedWriteItr);
        }
      }
      auto preparedReadWriteItr = pRW.find(write.first);
      if (preparedReadWriteItr != pRW.end()) {
        preparedReadWriteItr->second.erase(id);
        if (preparedReadWriteItr->second.empty()) {
          pRW.erase(preparedReadWriteItr);
        }
      }
    }
    for (const auto &read : preparedItr->second.getReadSet()) {
      auto preparedReadItr = pRW.find(read.first);
      if (preparedReadItr != pRW.end()) {
        preparedReadItr->second.erase(id);
        if (preparedReadItr->second.empty()) {
          pRW.erase(preparedReadItr);
        }
      }
    }
    prepared.erase(preparedItr);
  }
}

}  // namespace strongstore
