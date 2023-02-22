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
 * common/transaction.h:
 *   A Transaction representation.
 *
 **********************************************************************/

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include <cstddef>
#include <string>
#include <unordered_map>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common-proto.pb.h"
#include "store/common/timestamp.h"

class TransactionMessage;

// Reply types
#define REPLY_OK 0
#define REPLY_FAIL 1
#define REPLY_RETRY 2
#define REPLY_ABSTAIN 3
#define REPLY_TIMEOUT 4
#define REPLY_NETWORK_FAILURE 5
#define REPLY_WAITING 6
#define REPLY_MAX 7

class Transaction {
 private:
  // map between key and timestamp at
  // which the read happened and how
  // many times this key has been read
  std::unordered_map<std::string, Timestamp> readSet;

  // map between key and value(s)
  std::unordered_map<std::string, std::string> writeSet;

  // Start time (used for deadlock prevention)
  Timestamp start_time_;

 public:
  Transaction();
  Transaction(const TransactionMessage &msg);
  ~Transaction();

  const Timestamp &start_time() const;
  const std::unordered_map<std::string, Timestamp> &getReadSet() const;
  const std::unordered_map<std::string, std::string> &getWriteSet() const;
  std::unordered_map<std::string, std::string> &getWriteSet();

  inline size_t GetNumRead() const { return readSet.size(); }
  inline size_t GetNumWritten() const { return writeSet.size(); }

  void addReadSet(const std::string &key, const Timestamp &readTime);
  void addWriteSet(const std::string &key, const std::string &value);
  void add_read_write_sets(const Transaction &other);
  void set_start_time(const Timestamp &ts);

  void serialize(TransactionMessage *msg) const;
};

#endif /* _TRANSACTION_H_ */
