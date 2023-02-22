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
#pragma once

#include <bits/stdint-uintn.h>
#include <sys/time.h>

#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace spannerstore {

enum LockState {
  UNLOCKED,
  LOCKED_FOR_READ,
  LOCKED_FOR_WRITE,
  LOCKED_FOR_READ_WRITE
};

class WoundWait {
 public:
  WoundWait();
  ~WoundWait();

  LockState GetLockState(const std::string &lock) const;
  bool HasReadLock(const std::string &lock, uint64_t requester) const;

  int LockForRead(const std::string &lock, uint64_t requester,
                  const Timestamp &ts, std::unordered_set<uint64_t> &wound);
  void ReleaseForRead(const std::string &lock, uint64_t holder,
                      std::unordered_set<uint64_t> &notify);

  int LockForWrite(const std::string &lock, uint64_t requester,
                   const Timestamp &ts, std::unordered_set<uint64_t> &wound);
  void ReleaseForWrite(const std::string &lock, uint64_t holder,
                       std::unordered_set<uint64_t> &notify);

 private:
  class Waiter {
   public:
    Waiter() : first_waiter_{static_cast<uint64_t>(-1)}, write_{false} {}
    Waiter(bool r, bool w, uint64_t waiter, const Timestamp &ts)
        : waiters_{}, write_{w}, read_{r} {
      add_waiter(waiter, ts);
    }

    bool isread() const { return read_; }
    void set_read(bool r) { read_ = r; }

    bool iswrite() const { return write_; }
    void set_write(bool w) { write_ = w; }

    uint64_t first_waiter() const { return first_waiter_; }

    void add_waiter(uint64_t w, const Timestamp &ts) {
      if (waiters_.empty()) {
        first_waiter_ = w;
      }

      waiters_[w] = ts;
    }

    void remove_waiter(uint64_t w) { waiters_.erase(w); }

    const std::unordered_map<uint64_t, Timestamp> &waiters() const {
      return waiters_;
    }

   private:
    std::unordered_map<uint64_t, Timestamp> waiters_{};
    uint64_t first_waiter_{};
    bool write_;
    bool read_{};
  };

  class Lock {
   public:
    Lock();

    int TryAcquireReadLock(uint64_t requester, const Timestamp &ts,
                           std::unordered_set<uint64_t> &wound);
    void ReleaseReadLock(uint64_t holder, std::unordered_set<uint64_t> &notify);

    int TryAcquireWriteLock(uint64_t requester, const Timestamp &ts,
                            std::unordered_set<uint64_t> &wound);
    void ReleaseWriteLock(uint64_t holder,
                          std::unordered_set<uint64_t> &notify);

    const LockState state() const { return state_; }

    const std::unordered_map<uint64_t, Timestamp> &holders() const {
      return holders_;
    }

   private:
    LockState state_;
    std::unordered_map<uint64_t, Timestamp> holders_;
    std::deque<uint64_t> wait_q_;
    std::unordered_map<uint64_t, std::shared_ptr<Waiter>> waiters_;

    bool isWriteNext();

    void ReadWait(uint64_t requester, const Timestamp &ts,
                  std::unordered_set<uint64_t> &wound);
    void WriteWait(uint64_t requester, const Timestamp &ts,
                   std::unordered_set<uint64_t> &wound);

    void AddReadWaiter(uint64_t requester, const Timestamp &ts);
    void AddWriteWaiter(uint64_t requester, const Timestamp &ts);
    void AddReadWriteWaiter(uint64_t requester, const Timestamp &ts);

    void PopWaiter(std::unordered_set<uint64_t> &notify);

    bool SafeUpgradeToRW(const Timestamp &ts);
  };

  std::unordered_map<std::string, Lock> locks_;
};

};  // namespace spannerstore
