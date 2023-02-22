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
#ifndef _STATS_H_
#define _STATS_H_

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <cstddef>
#include <mutex>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

class Stats {
 public:
  Stats();
  virtual ~Stats();

  // int64_t Get(const std::string &key);
  void Increment(const std::string &key, int amount = 1);
  void IncrementList(const std::string &key, size_t idx, int amount = 1);
  void Add(const std::string &key, int64_t value);
  void AddList(const std::string &key, size_t idx, uint64_t value);

  void ExportJSON(std::ostream &os);
  void ExportJSON(const std::string &file);
  void Merge(const Stats &other);

 private:
  std::mutex mtx;
  std::unordered_map<std::string, int64_t> statInts;
  std::unordered_map<std::string, std::vector<int64_t>> statLists;
  std::unordered_map<std::string, std::vector<int64_t>> statIncLists;
  std::unordered_map<std::string, std::vector<std::vector<uint64_t>>> statLoLs;
};
#endif /* _STATS_H_ */
