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
#include "store/benchmark/retwis/add_user.h"

#include <bits/std_function.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "lib/message.h"

class KeySelector;

namespace context {

namespace retwis {

AddUser::AddUser(commit_callback ccb, abort_callback acb,
                 const std::vector<KeySelector *> &keySelector,
                 bool single_shard, std::mt19937 *rand)
    : context::AsyncTransaction(std::move(ccb), std::move(acb)),
      ::retwis::RetwisTransaction(keySelector, 4, single_shard, rand) {}

AddUser::~AddUser() = default;

void AddUser::Execute(Client *client, uint32_t timeout) {
  Debug("ADD_USER");
  std::unique_ptr<AppContext> ctx(new AddUserContext());
  client->Begin(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void AddUser::Retry(Client *client, uint32_t timeout) {
  Debug("ADD_USER");
  std::unique_ptr<AppContext> ctx(new AddUserContext());
  client->Retry(ctx, acb_);
  ExecuteOps(client, std::move(ctx));
}

void AddUser::ExecuteOps(Client *client, std::unique_ptr<AppContext> ctx) {
  client->GetForUpdate(
      std::move(ctx), GetKey(0),
      [this, client](std::unique_ptr<AppContext> ctx,
                     transaction_op_status_t status, const std::string &key,
                     const std::string &val) {
        if (status == FAIL) {
          client->Abort(ctx);
          acb_(std::move(ctx));
          return;
        }

        client->Put(ctx, GetKey(1), GetKey(1));
        client->Put(ctx, GetKey(2), GetKey(2));
        client->Put(ctx, GetKey(3), GetKey(3));

        client->Commit(std::move(ctx), ccb_);
      });
}

AddUser::AddUserContext::AddUserContext() = default;

AddUser::AddUserContext::AddUserContext(const AddUserContext &other) {}

AddUser::AddUserContext::~AddUserContext() = default;

std::unique_ptr<AppContext> AddUser::AddUserContext::CloneInternal() const {
  return std::make_unique<AddUserContext>(*this);
}

}  // namespace retwis

}  // namespace context
