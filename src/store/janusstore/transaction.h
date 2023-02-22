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
// note, rewriting common/transaction.h because unneeded timestamp
// #include "store/common/transaction.h"

#ifndef _JANUS_TRANSACTION_H_
#define _JANUS_TRANSACTION_H_

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/tcptransport.h"
#include "store/common/common-proto.pb.h"
#include "store/common/transaction.h"
#include "store/janusstore/janus-proto.pb.h"

class TransportAddress;

namespace janusstore {

/* Defines a transaction that is forwarded to a particular replica */
class Transaction {
 private:
  // the unique transaction ID
  uint64_t txn_id{};
  janusstore::proto::TransactionMessage::Status status;
  std::unordered_map<uint64_t, std::unordered_set<std::string>> sharded_readset;
  std::unordered_map<uint64_t, std::unordered_map<std::string, std::string>>
      sharded_writeset;

 public:
  uint64_t request_id{};
  std::set<uint64_t> blocked_by_list{};
  std::set<const TransportAddress *> client_addrs{};

  // the groups (shards) that this txn is associated with
  std::unordered_set<int> groups{};

  Transaction() = default;
  explicit Transaction(uint64_t txn_id);
  Transaction(uint64_t txn_id,
              const janusstore::proto::TransactionMessage &msg);
  ~Transaction();

  // set of keys to be read
  std::unordered_set<std::string> read_set{};

  // map between key and value(s)
  std::unordered_map<std::string, std::string> write_set{};

  // the result of ths transaction
  std::unordered_map<std::string, std::string> result{};
  void setResult(std::unordered_map<std::string, std::string> result);

  void setTransactionId(uint64_t txn_id);
  const uint64_t getTransactionId() const;
  void setTransactionStatus(
      janusstore::proto::TransactionMessage::Status status);
  const janusstore::proto::TransactionMessage::Status getTransactionStatus()
      const;
  const std::unordered_set<std::string> &getReadSet() const;
  const std::unordered_map<std::string, std::string> &getWriteSet() const;

  inline size_t GetNumRead() const { return read_set.size(); }
  inline size_t GetNumWritten() const { return write_set.size(); }

  void addReadSet(const std::string &key);
  void addWriteSet(const std::string &key, const std::string &value);
  void addShardedReadSet(const std::string &key, uint64_t shard);
  void addShardedWriteSet(const std::string &key, const std::string &value,
                          uint64_t shard);
  void serialize(janusstore::proto::TransactionMessage *msg,
                 uint64_t shard) const;
};

}  // namespace janusstore

#endif /* _JANUS_TRANSACTION_H_ */
