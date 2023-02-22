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
#include "store/common/frontend/async_one_shot_adapter_client.h"

#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/one_shot_client.h"
#include "store/common/frontend/one_shot_transaction.h"
#include "store/common/frontend/transaction_utils.h"

AsyncOneShotAdapterClient::AsyncOneShotAdapterClient(OneShotClient *client)
    : client(client) {}

AsyncOneShotAdapterClient::~AsyncOneShotAdapterClient() = default;

void AsyncOneShotAdapterClient::Execute(AsyncTransaction *txn,
                                        execute_callback ecb, bool retry) {
  OneShotTransaction oneShotTxn;
  Operation op;
  ReadValueMap emptyReadValues;
  for (size_t opCount = 0;; ++opCount) {
    op = txn->GetNextOperation(opCount, 0UL, emptyReadValues);
    if (op.type == COMMIT || op.type == ABORT) {
      break;
    }
    if (op.type == GET) {
      oneShotTxn.AddRead(op.key);
    } else if (op.type == PUT) {
      oneShotTxn.AddWrite(op.key, op.value);
    }
  }
  if (op.type == COMMIT) {
    // TODO(matthelb): oneShotTxn is destructed after calling Execute; need to
    // make sure all clients avoid assuming that oneShotTxn will live long
    client->Execute(&oneShotTxn, ecb);
  }
}
