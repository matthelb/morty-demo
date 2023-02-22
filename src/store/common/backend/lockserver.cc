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
/***********************************************************************
 *
 * spanstore/lockserver.cc:
 *   Simple multi-reader, single-writer lock server
 *
 **********************************************************************/

#include "store/common/backend/lockserver.h"

#include "lib/assert.h"
#include "lib/message.h"

using namespace std;

LockServer::LockServer() {
  readers = 0;
  writers = 0;
}

LockServer::~LockServer() = default;

bool LockServer::Waiter::checkTimeout(const struct timeval &now) {
  if (now.tv_sec > waitTime.tv_sec) {
    return true;
  }
  UW_ASSERT(now.tv_usec > waitTime.tv_usec && now.tv_sec == waitTime.tv_sec);

  return now.tv_usec - waitTime.tv_usec > LOCK_WAIT_TIMEOUT;
}

void LockServer::Lock::waitForLock(uint64_t requester, bool write) {
  if (waiters.find(requester) != waiters.end()) {
    // Already waiting
    return;
  }

  Debug("[%lu] Adding me to the queue ...", requester);
  // Otherwise
  waiters[requester] = Waiter(write);
  waitQ.push(requester);
}

bool LockServer::Lock::tryAcquireLock(uint64_t requester, bool write) {
  if (waitQ.empty()) {
    return true;
  }

  Debug("[%lu] Trying to get lock for %d", requester, (int)write);
  struct timeval now {};
  uint64_t w = waitQ.front();

  gettimeofday(&now, nullptr);
  // prune old requests out of the wait queue
  while (waiters[w].checkTimeout(now)) {
    waiters.erase(w);
    waitQ.pop();

    // if everyone else was old ...
    if (waitQ.empty()) {
      return true;
    }

    w = waitQ.front();
    UW_ASSERT(waiters.find(w) != waiters.end());
  }

  if (waitQ.front() == requester) {
    // this lock is being reserved for the requester
    waitQ.pop();
    UW_ASSERT(waiters.find(requester) != waiters.end());
    UW_ASSERT(waiters[requester].write == write);
    waiters.erase(requester);
    return true;
  }
  // otherwise, add me to the list
  waitForLock(requester, write);
  return false;
}

bool LockServer::Lock::isWriteNext() {
  if (waitQ.empty()) {
    return false;
  }

  struct timeval now {};
  uint64_t w = waitQ.front();

  gettimeofday(&now, nullptr);
  // prune old requests out of the wait queue
  while (waiters[w].checkTimeout(now)) {
    waiters.erase(w);
    waitQ.pop();

    // if everyone else was old ...
    if (waitQ.empty()) {
      return false;
    }

    w = waitQ.front();
    UW_ASSERT(waiters.find(w) != waiters.end());
  }

  UW_ASSERT(waiters.find(waitQ.front()) != waiters.end());
  return waiters[waitQ.front()].write;
}

bool LockServer::lockForRead(const string &lock, uint64_t requester) {
  Lock &l = locks[lock];
  Debug("Lock for Read: %s [%lu %lu %lu %lu]", lock.c_str(), readers, writers,
        l.holders.size(), l.waiters.size());

  switch (l.state) {
    case UNLOCKED:
      // if you are next in the queue
      if (l.tryAcquireLock(requester, false)) {
        Debug("[%lu] I have acquired the read lock!", requester);
        l.state = LOCKED_FOR_READ;
        UW_ASSERT(l.holders.empty());
        l.holders.insert(requester);
        readers++;
        return true;
      }
      return false;
    case LOCKED_FOR_READ:
      // if you already hold this lock
      if (l.holders.find(requester) != l.holders.end()) {
        return true;
      }

      // There is a write waiting, let's give up the lock
      if (l.isWriteNext()) {
        Debug("[%lu] Waiting on lock because there is a pending write request",
              requester);
        l.waitForLock(requester, false);
        return false;
      }

      l.holders.insert(requester);
      readers++;
      return true;
    case LOCKED_FOR_WRITE:
    case LOCKED_FOR_READ_WRITE:
      if (l.holders.count(requester) > 0) {
        l.state = LOCKED_FOR_READ_WRITE;
        readers++;
        return true;
      }
      UW_ASSERT(l.holders.size() == 1);
      Debug("Locked for write, held by %lu", *(l.holders.begin()));
      l.waitForLock(requester, false);
      return false;
  }
  NOT_REACHABLE();
  return false;
}

bool LockServer::lockForWrite(const string &lock, uint64_t requester) {
  Lock &l = locks[lock];

  Debug("Lock for Write: %s [%lu %lu %lu %lu]", lock.c_str(), readers, writers,
        l.holders.size(), l.waiters.size());

  switch (l.state) {
    case UNLOCKED:
      // Got it!
      if (l.tryAcquireLock(requester, true)) {
        Debug("[%lu] I have acquired the write lock!", requester);
        l.state = LOCKED_FOR_WRITE;
        UW_ASSERT(l.holders.empty());
        l.holders.insert(requester);
        writers++;
        return true;
      }
      return false;
    case LOCKED_FOR_READ:
      if (l.holders.size() == 1 && l.holders.count(requester) > 0) {
        // if there is one holder of this read lock and it is the
        // requester, then upgrade the lock
        l.state = LOCKED_FOR_READ_WRITE;
        writers++;
        return true;
      }

      Debug("Locked for read by%s%lu other people",
            l.holders.count(requester) > 0 ? "you" : "", l.holders.size());
      l.waitForLock(requester, true);
      return false;
    case LOCKED_FOR_WRITE:
    case LOCKED_FOR_READ_WRITE:
      UW_ASSERT(l.holders.size() == 1);
      if (l.holders.count(requester) > 0) {
        return true;
      }

      Debug("Held by %lu for %s", *(l.holders.begin()),
            (l.state == LOCKED_FOR_WRITE) ? "write" : "read-write");
      l.waitForLock(requester, true);
      return false;
  }
  NOT_REACHABLE();
  return false;
}

void LockServer::releaseForRead(const string &lock, uint64_t holder) {
  if (locks.find(lock) == locks.end()) {
    return;
  }

  Lock &l = locks[lock];

  if (l.holders.count(holder) == 0) {
    Warning("[%ld] Releasing unheld read lock: %s", holder, lock.c_str());
    return;
  }

  switch (l.state) {
    case UNLOCKED:
    case LOCKED_FOR_WRITE:
      return;
    case LOCKED_FOR_READ:
      readers--;
      if (l.holders.erase(holder) < 1) {
        Warning("[%ld] Releasing unheld read lock: %s", holder, lock.c_str());
      }
      if (l.holders.empty()) {
        l.state = UNLOCKED;
      }
      return;
    case LOCKED_FOR_READ_WRITE:
      readers--;
      l.state = LOCKED_FOR_WRITE;
      return;
  }
}

void LockServer::releaseForWrite(const string &lock, uint64_t holder) {
  if (locks.find(lock) == locks.end()) {
    return;
  }

  Lock &l = locks[lock];

  if (l.holders.count(holder) == 0) {
    Warning("[%ld] Releasing unheld write lock: %s", holder, lock.c_str());
    return;
  }

  switch (l.state) {
    case UNLOCKED:
    case LOCKED_FOR_READ:
      return;
    case LOCKED_FOR_WRITE:
      writers--;
      l.holders.erase(holder);
      UW_ASSERT(l.holders.empty());
      l.state = UNLOCKED;
      return;
    case LOCKED_FOR_READ_WRITE:
      writers--;
      l.state = LOCKED_FOR_READ;
      UW_ASSERT(l.holders.size() == 1);
      return;
  }
}
