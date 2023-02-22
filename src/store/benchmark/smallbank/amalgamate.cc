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
#include "store/benchmark/smallbank/amalgamate.h"

#include <bits/stdint-intn.h>

#include <utility>

#include "lib/message.h"
#include "store/benchmark/smallbank/smallbank-proto.pb.h"
#include "store/benchmark/smallbank/smallbank_transaction.h"
#include "store/benchmark/smallbank/utils.h"
#include "store/common/frontend/sync_client.h"

namespace smallbank {

Amalgamate::Amalgamate(std::string cust1, std::string cust2,
                       const uint32_t timeout)
    : SmallbankTransaction(AMALGAMATE),
      cust1(std::move(cust1)),
      cust2(std::move(cust2)),
      timeout(timeout) {}

Amalgamate::~Amalgamate() = default;

transaction_status_t Amalgamate::Execute(SyncClient &client) {
  proto::AccountRow accountRow1;
  proto::AccountRow accountRow2;

  proto::CheckingRow checkingRow1;
  proto::SavingRow savingRow1;
  proto::CheckingRow checkingRow2;

  client.Begin(timeout);
  Debug("Amalgamate for names %s %s", cust1.c_str(), cust2.c_str());
  if (!ReadAccountRow(client, cust1, accountRow1, timeout) ||
      !ReadAccountRow(client, cust2, accountRow2, timeout)) {
    client.Abort(timeout);
    Debug("Aborted Amalgamate (AccountRow)");
    return ABORTED_USER;
  }
  const uint32_t customerId1 = accountRow1.customer_id();
  const uint32_t customerId2 = accountRow2.customer_id();
  if (!ReadCheckingRow(client, customerId2, checkingRow2, timeout)) {
    client.Abort(timeout);
    Debug("Aborted Amalgamate (CheckingRow)");
    return ABORTED_USER;
  }
  const int32_t balance2 = checkingRow2.checking_balance();
  if (!ReadCheckingRow(client, customerId1, checkingRow1, timeout) ||
      !ReadSavingRow(client, customerId1, savingRow1, timeout)) {
    client.Abort(timeout);
    Debug("Aborted Amalgamate (2nd Balance)");
    return ABORTED_USER;
  }
  InsertCheckingRow(
      client, customerId2,
      balance2 + checkingRow1.checking_balance() + savingRow1.saving_balance(),
      timeout);
  InsertSavingRow(client, customerId1, 0, timeout);
  InsertCheckingRow(client, customerId1, 0, timeout);
  return client.Commit(timeout);
}

}  // namespace smallbank
