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
#ifndef _TRACE_CLIENT_H_
#define _TRACE_CLIENT_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "store/common/frontend/client.h"

class AppContext;
class Transport;

class TraceClient : public ::context::Client {
 public:
  TraceClient(Transport *transport, const std::string &data_file_path);
  ~TraceClient() override;

  void Begin(std::unique_ptr<AppContext> &context,
             ::context::abort_callback acb) override;

  void Get(std::unique_ptr<AppContext> context, const std::string &key,
           ::context::op_callback ocb) override;

  void Put(std::unique_ptr<AppContext> &context, const std::string &key,
           const std::string &val) override;

  void Commit(std::unique_ptr<AppContext> context,
              ::context::commit_callback ccb) override;

  void Abort(std::unique_ptr<AppContext> &context) override;

 private:
  Transport *transport_;
  std::unordered_map<std::string, std::string> store_;
};
#endif /* _TRACE_CLIENT_H_ */
