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
#include "store/benchmark/tpcc/trace_client.h"

#include <bits/std_function.h>

#include <iostream>
#include <type_traits>
#include <utility>

#include "lib/io_utils.h"
#include "lib/transport.h"
#include "store/common/common.h"
#include "store/common/frontend/appcontext.h"
#include "store/common/timestamp.h"

TraceClient::TraceClient(Transport *transport,
                         const std::string &data_file_path)
    : transport_(transport) {
  std::ifstream in;
  in.open(data_file_path);
  if (!in) {
    std::cerr << "Could not read data from: " << data_file_path << std::endl;
  }
  while (!in.eof()) {
    std::string key;
    std::string value;
    Timestamp ts(0, 0);
    int i = ReadBytesFromStream(&in, &key);
    if (i == 0) {
      ReadBytesFromStream(&in, &value);
      store_.insert(std::make_pair(std::move(key), std::move(value)));
    }
  }
}

TraceClient::~TraceClient() = default;

void TraceClient::Begin(std::unique_ptr<AppContext> &context,
                        ::context::abort_callback acb) {
  std::cout << "b,";
}

void TraceClient::Get(std::unique_ptr<AppContext> context,
                      const std::string &key, ::context::op_callback ocb) {
  std::cout << "g:" << BytesToHex(key, key.length()) << ",";
  transport_->Timer(0, [this, ctx = context.release(), key, ocb]() {
    std::unique_ptr<AppContext> context(ctx);
    ocb(std::move(context), ::context::OK, key, store_[key]);
  });
}

void TraceClient::Put(std::unique_ptr<AppContext> &context,
                      const std::string &key, const std::string &val) {
  std::cout << "p:" << BytesToHex(key, key.length()) << ",";
  store_[key] = val;
}

void TraceClient::Commit(std::unique_ptr<AppContext> context,
                         ::context::commit_callback ccb) {
  std::cout << "c" << std::endl;
  ccb(std::move(context), ::context::OK, COMMITTED);
}

void TraceClient::Abort(std::unique_ptr<AppContext> &context) {
  std::cout << "a" << std::endl;
}
