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
#ifndef SMALLBANK_UTILS_H
#define SMALLBANK_UTILS_H

#include <bits/stdint-uintn.h>

#include <string>

#include "lib/configuration.h"
#include "store/benchmark/smallbank/smallbank-proto.pb.h"
#include "store/common/frontend/sync_client.h"

class SyncClient;

namespace smallbank {

namespace proto {
class AccountRow;
class CheckingRow;
class SavingRow;
}  // namespace proto

std::string AccountRowKey(const std::string &name);

std::string SavingRowKey(const uint32_t customer_id);

std::string CheckingRowKey(const uint32_t customer_id);

bool ReadAccountRow(SyncClient &client, const std::string &name,
                    proto::AccountRow &accountRow, uint32_t timeout);

bool ReadCheckingRow(SyncClient &client, uint32_t customer_id,
                     proto::CheckingRow &checkingRow, uint32_t timeout);

bool ReadSavingRow(SyncClient &client, uint32_t customer_id,
                   proto::SavingRow &savingRow, uint32_t timeout);

void InsertAccountRow(SyncClient &client, const std::string &name,
                      uint32_t customer_id, uint32_t timeout);

void InsertSavingRow(SyncClient &client, uint32_t customer_id, uint32_t balance,
                     uint32_t timeout);

void InsertCheckingRow(SyncClient &client, uint32_t customer_id,
                       uint32_t balance, uint32_t timeout);

}  // namespace smallbank

#endif /* SMALLBANK_UTILS_H */
