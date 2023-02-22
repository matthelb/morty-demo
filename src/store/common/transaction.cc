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
 * common/transaction.cc
 *   A transaction implementation.
 *
 **********************************************************************/

#include "store/common/transaction.h"

#include <utility>

#include "store/common/common-proto.pb.h"

using namespace std;

Transaction::Transaction() = default;

Transaction::Transaction(const TransactionMessage &msg)
    : start_time_{msg.starttime()} {
  for (int i = 0; i < msg.readset_size(); i++) {
    const ReadMessage &readMsg = msg.readset(i);
    readSet[readMsg.key()] = Timestamp(readMsg.readtime());
  }

  for (int i = 0; i < msg.writeset_size(); i++) {
    const WriteMessage &writeMsg = msg.writeset(i);
    writeSet[writeMsg.key()] = writeMsg.value();
  }
}

Transaction::~Transaction() = default;

const unordered_map<string, Timestamp> &Transaction::getReadSet() const {
  return readSet;
}

const unordered_map<string, string> &Transaction::getWriteSet() const {
  return writeSet;
}

unordered_map<string, string> &Transaction::getWriteSet() { return writeSet; }

const Timestamp &Transaction::start_time() const { return start_time_; }

void Transaction::set_start_time(const Timestamp &ts) { start_time_ = ts; }

void Transaction::addReadSet(const string &key, const Timestamp &readTime) {
  readSet[key] = readTime;
}

void Transaction::addWriteSet(const string &key, const string &value) {
  writeSet[key] = value;
}

void Transaction::add_read_write_sets(const Transaction &other) {
  for (auto &kt : other.getReadSet()) {
    readSet[kt.first] = kt.second;
  }

  for (auto &kv : other.getWriteSet()) {
    writeSet[kv.first] = kv.second;
  }
}

void Transaction::serialize(TransactionMessage *msg) const {
  start_time_.serialize(msg->mutable_starttime());
  for (auto read : readSet) {
    ReadMessage *readMsg = msg->add_readset();
    readMsg->set_key(read.first);
    read.second.serialize(readMsg->mutable_readtime());
  }

  for (auto write : writeSet) {
    WriteMessage *writeMsg = msg->add_writeset();
    writeMsg->set_key(write.first);
    writeMsg->set_value(write.second);
  }
}
