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
//
// Created by Janice Chan on 10/12/19.
//
#include "store/benchmark/smallbank/bal.h"

#include "gmock/gmock-actions.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock-more-actions.h"
#include "gmock/gmock-spec-builders.h"
#include "gtest/gtest-message.h"
#include "gtest/gtest-test-part.h"
#include "gtest/gtest_pred_impl.h"
#include "store/benchmark/smallbank/smallbank-proto.pb.h"
#include "store/benchmark/smallbank/tests/smallbank_test_utils.h"
#include "store/benchmark/smallbank/utils.h"
#include "testing/base/public/gunit.h"

namespace smallbank {
TEST(Bal, ReadAccountFailure) {
  std::string cust = "cust1";
  int timeout = 0;

  MockSyncClient mockSyncClient;
  Bal smallbankTransaction(cust, timeout);
  EXPECT_CALL(mockSyncClient, Begin(timeout)).Times(1);
  EXPECT_CALL(mockSyncClient, Get(AccountRowKey(cust), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(""));
  EXPECT_CALL(mockSyncClient, Abort(timeout)).Times(1);
  EXPECT_EQ(smallbankTransaction.Execute(mockSyncClient), 1);
}

TEST(Bal, ReadSavingFailure) {
  std::string cust = "cust1";
  uint32_t customerId = 10;
  int timeout = 0;

  MockSyncClient mockSyncClient;
  Bal smallbankTransaction(cust, timeout);
  EXPECT_CALL(mockSyncClient, Begin(timeout)).Times(1);
  proto::AccountRow accountRow;
  accountRow.set_name(cust);
  accountRow.set_customer_id(customerId);
  std::string accountRowSerialized;
  accountRow.SerializeToString(&accountRowSerialized);
  EXPECT_CALL(mockSyncClient, Get(AccountRowKey(cust), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(accountRowSerialized));
  EXPECT_CALL(mockSyncClient,
              Get(SavingRowKey(customerId), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(""));
  EXPECT_CALL(mockSyncClient, Abort(timeout)).Times(1);
  EXPECT_EQ(smallbankTransaction.Execute(mockSyncClient), 1);
}

TEST(Bal, ReadCheckingFailure) {
  std::string cust = "cust1";
  uint32_t customerId = 10;
  int timeout = 0;
  int savingBalance = 200;

  MockSyncClient mockSyncClient;
  Bal smallbankTransaction(cust, timeout);
  EXPECT_CALL(mockSyncClient, Begin(timeout)).Times(1);
  proto::AccountRow accountRow;
  accountRow.set_name(cust);
  accountRow.set_customer_id(customerId);
  std::string accountRowSerialized;
  accountRow.SerializeToString(&accountRowSerialized);
  proto::SavingRow savingRow;
  savingRow.set_customer_id(customerId);
  savingRow.set_saving_balance(savingBalance);
  std::string savingRowSerialized;
  savingRow.SerializeToString(&savingRowSerialized);
  EXPECT_CALL(mockSyncClient, Get(AccountRowKey(cust), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(accountRowSerialized));
  EXPECT_CALL(mockSyncClient,
              Get(SavingRowKey(customerId), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(savingRowSerialized));
  EXPECT_CALL(mockSyncClient,
              Get(CheckingRowKey(customerId), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(""));
  EXPECT_CALL(mockSyncClient, Abort(timeout)).Times(1);
  EXPECT_EQ(smallbankTransaction.Execute(mockSyncClient), 1);
}
TEST(Bal, Success) {
  std::string cust = "cust1";
  uint32_t customerId = 10;
  int timeout = 0;
  int savingBalance = 200;
  int checkingBalance = 1000;

  MockSyncClient mockSyncClient;
  Bal smallbankTransaction(cust, timeout);
  EXPECT_CALL(mockSyncClient, Begin(timeout)).Times(1);
  proto::AccountRow accountRow;
  accountRow.set_name(cust);
  accountRow.set_customer_id(customerId);
  std::string accountRowSerialized;
  accountRow.SerializeToString(&accountRowSerialized);
  proto::SavingRow savingRow;
  savingRow.set_customer_id(customerId);
  savingRow.set_saving_balance(savingBalance);
  std::string savingRowSerialized;
  savingRow.SerializeToString(&savingRowSerialized);
  proto::CheckingRow checkingRow;
  checkingRow.set_customer_id(customerId);
  checkingRow.set_checking_balance(checkingBalance);
  std::string checkingRowSerialized;
  checkingRow.SerializeToString(&checkingRowSerialized);
  EXPECT_CALL(mockSyncClient, Get(AccountRowKey(cust), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(accountRowSerialized));
  EXPECT_CALL(mockSyncClient,
              Get(SavingRowKey(customerId), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(savingRowSerialized));
  EXPECT_CALL(mockSyncClient,
              Get(CheckingRowKey(customerId), testing::_, timeout))
      .WillOnce(testing::SetArgReferee<1>(checkingRowSerialized));
  EXPECT_CALL(mockSyncClient, Commit(timeout))
      .Times(1)
      .WillOnce(testing::Return(transaction_status_t::ABORTED_USER));
  EXPECT_EQ(smallbankTransaction.Execute(mockSyncClient), 1);
}
}  // namespace smallbank
