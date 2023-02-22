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
#include "store/spannerstore/preparedtransaction.h"

#include <utility>

#include "store/common/common-proto.pb.h"
#include "store/spannerstore/spanner-proto.pb.h"

namespace spannerstore {

Value::Value(uint64_t transaction_id, const Timestamp &ts, std::string key,
             std::string val)
    : transaction_id_{transaction_id},
      ts_{ts},
      key_{std::move(key)},
      val_{std::move(val)} {}

Value::Value(const proto::ReadReply &msg)
    : transaction_id_{msg.transaction_id()},
      ts_{msg.timestamp()},
      key_{msg.key()},
      val_{msg.val()} {}

Value::~Value() = default;

PreparedTransaction::PreparedTransaction(uint64_t transaction_id,
                                         const Timestamp &prepare_ts)
    : transaction_id_{transaction_id}, prepare_ts_{prepare_ts} {}

PreparedTransaction::PreparedTransaction(
    const proto::PreparedTransactionMessage &msg)
    : transaction_id_{msg.transaction_id()},
      prepare_ts_{msg.prepare_timestamp()} {
  for (auto &w : msg.write_set()) {
    write_set_.emplace(w.key(), w.value());
  }
}

PreparedTransaction::~PreparedTransaction() = default;

void PreparedTransaction::serialize(
    proto::PreparedTransactionMessage *msg) const {
  msg->set_transaction_id(transaction_id_);
  prepare_ts_.serialize(msg->mutable_prepare_timestamp());

  for (auto write : write_set_) {
    WriteMessage *writeMsg = msg->add_write_set();
    writeMsg->set_key(write.first);
    writeMsg->set_value(write.second);
  }
}

};  // namespace spannerstore
