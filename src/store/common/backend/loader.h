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
#ifndef _LOADER_H_
#define _LOADER_H_

#include <bits/stdint-uintn.h>

#include <iosfwd>
#include <string>

#include "store/common/partitioner.h"
#include "store/server.h"

class Partitioner;
class Server;

class Loader {
 public:
  Loader(Server *server, Partitioner *part, uint64_t num_shards,
         uint64_t num_groups, uint64_t group_idx, uint64_t multi_thread);
  ~Loader();

  int Load(const std::string &data_file_path);

 private:
  int LoadInternal(std::ifstream &in);

  Server *server_;
  Partitioner *part_;
  const uint64_t num_shards_;
  const uint64_t num_groups_;
  const uint64_t group_idx_;
  const uint64_t multi_thread_;
};

#endif /* _LOADER_H_ */
