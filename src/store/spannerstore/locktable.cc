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
#include "store/spannerstore/locktable.h"

#include <unordered_map>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"

namespace spannerstore {

LockTable::LockTable() = default;

LockTable::~LockTable() = default;

LockStatus LockTable::ConvertToResultStatus(int status) {
  if (status == REPLY_OK) {
    return ACQUIRED;
  }
  if (status == REPLY_WAITING) {
    return WAITING;
  }
  if (status == REPLY_FAIL) {
    return FAIL;
  } else {
    NOT_REACHABLE();
  }
}

LockAcquireResult LockTable::AcquireReadLock(uint64_t transaction_id,
                                             const Timestamp &ts,
                                             const std::string &key) {
  LockAcquireResult r;
  int status = locks_.LockForRead(key, transaction_id, ts, r.wound_rws);
  Debug("[%lu] LockForRead %lu: %d", transaction_id, *((uint64_t *)key.c_str()),
        status);
  r.status = ConvertToResultStatus(status);
  return r;
}

bool LockTable::HasReadLock(uint64_t transaction_id, const std::string &key) {
  return locks_.HasReadLock(key, transaction_id);
}

LockAcquireResult LockTable::AcquireReadWriteLock(uint64_t transaction_id,
                                                  const Timestamp &ts,
                                                  const std::string &key) {
  LockAcquireResult r;

  int status = locks_.LockForWrite(key, transaction_id, ts, r.wound_rws);
  // Debug("[%lu] LockForWrite returned status %d", transaction_id, status);
  int status2 = locks_.LockForRead(key, transaction_id, ts, r.wound_rws);
  // Debug("[%lu] LockForRead returned status %d", transaction_id, status2);
  UW_ASSERT(status == status2);

  Debug("[%lu] LockForReadWrite %lu: %d", transaction_id,
        *((uint64_t *)key.c_str()), status);

  r.status = ConvertToResultStatus(status);
  return r;
}

LockAcquireResult LockTable::AcquireLocks(uint64_t transaction_id,
                                          const Transaction &transaction) {
  const Timestamp &start_ts = transaction.start_time();
  // Debug("[%lu] start_time: %lu.%lu", transaction_id,
  // start_ts.getTimestamp(), start_ts.getID());

  LockAcquireResult r;
  int ret = REPLY_OK;

  // get read locks
  for (auto &read : transaction.getReadSet()) {
    int status =
        locks_.LockForRead(read.first, transaction_id, start_ts, r.wound_rws);
    Debug("[%lu] LockForRead %lu: %d", transaction_id,
          *((uint64_t *)read.first.c_str()), status);
    if (ret == REPLY_OK && status == REPLY_WAITING) {
      ret = REPLY_WAITING;
    } else if (status == REPLY_FAIL) {
      ret = REPLY_FAIL;
    }
  }

  // get write locks
  for (auto &write : transaction.getWriteSet()) {
    int status =
        locks_.LockForWrite(write.first, transaction_id, start_ts, r.wound_rws);
    Debug("[%lu] LockForWrite %lu: %d", transaction_id,
          *((uint64_t *)write.first.c_str()), status);
    if (ret == REPLY_OK && status == REPLY_WAITING) {
      ret = REPLY_WAITING;
    } else if (status == REPLY_FAIL) {
      ret = REPLY_FAIL;
    }
  }

  r.status = ConvertToResultStatus(ret);
  return r;
}

LockReleaseResult LockTable::ReleaseLocks(uint64_t transaction_id,
                                          const Transaction &transaction) {
  LockReleaseResult r;

  for (auto &write : transaction.getWriteSet()) {
    locks_.ReleaseForWrite(write.first, transaction_id, r.notify_rws);
    Debug("[%lu] ReleaseForWrite %lu", transaction_id,
          *((uint64_t *)write.first.c_str()));
  }

  for (auto &read : transaction.getReadSet()) {
    locks_.ReleaseForRead(read.first, transaction_id, r.notify_rws);
    Debug("[%lu] ReleaseForRead %lu", transaction_id,
          *((uint64_t *)read.first.c_str()));
  }

  return r;
}

}  // namespace spannerstore
