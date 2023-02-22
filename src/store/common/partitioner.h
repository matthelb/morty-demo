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
#ifndef PARTITIONER_H
#define PARTITIONER_H

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <functional>
#include <random>
#include <set>
#include <string>
#include <vector>

enum partitioner_t {
  DEFAULT = 0,
  INT,
  WAREHOUSE_DIST_ITEMS,
  WAREHOUSE,
  WAREHOUSE_FINE_GRAIN,
};

class Partitioner {
 public:
  Partitioner() = default;
  virtual ~Partitioner() = default;
  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
                              int group, const std::vector<int> &txnGroups) = 0;
  uint64_t operator()(const std::string &key, uint64_t num_shards, int group,
                      const std::set<int> &txn_groups);
};

class DefaultPartitioner : public Partitioner {
 public:
  DefaultPartitioner() = default;
  ~DefaultPartitioner() override = default;

  virtual uint64_t operator()(const std::string &key, uint64_t nshards,
                              int group, const std::vector<int> &txnGroups);

 private:
  std::hash<std::string> hash{};
};

class IntPartitioner : public Partitioner {
 public:
  explicit IntPartitioner(uint64_t num_keys) : num_keys_(num_keys) {}
  ~IntPartitioner() override = default;

  virtual uint64_t operator()(const std::string &key, uint64_t nshards,
                              int group, const std::vector<int> &txnGroups);

 private:
  const uint64_t num_keys_;
};

class WarehouseDistItemsPartitioner : public Partitioner {
 public:
  explicit WarehouseDistItemsPartitioner(uint64_t numWarehouses)
      : numWarehouses(numWarehouses) {}
  ~WarehouseDistItemsPartitioner() override = default;
  virtual uint64_t operator()(const std::string &key, uint64_t nshards,
                              int group, const std::vector<int> &txnGroups);

 private:
  const uint64_t numWarehouses;
};

class WarehousePartitioner : public Partitioner {
 public:
  WarehousePartitioner(uint64_t numWarehouses, std::mt19937 &rd)
      : numWarehouses(numWarehouses), rd(rd) {}
  ~WarehousePartitioner() override = default;

  virtual uint64_t operator()(const std::string &key, uint64_t nshards,
                              int group, const std::vector<int> &txnGroups);

 private:
  const uint64_t numWarehouses;
  std::mt19937 &rd;
};

class WarehouseFineGrainPartitioner : public Partitioner {
 public:
  WarehouseFineGrainPartitioner(uint64_t num_warehouses, std::mt19937 &rd)
      : num_warehouses_(num_warehouses), rd_(rd) {}
  ~WarehouseFineGrainPartitioner() override = default;

  virtual uint64_t operator()(const std::string &key, uint64_t num_shards,
                              int group, const std::vector<int> &txn_groups);

 private:
  const uint64_t num_warehouses_;
  std::mt19937 &rd_;
};

using partitioner = std::function<uint64_t(const std::string &, uint64_t, int,
                                           const std::vector<int> &)>;

extern partitioner default_partitioner;
extern partitioner warehouse_partitioner;

partitioner warehouse_district_partitioner_dist_items(uint64_t num_warehouses);
partitioner warehouse_district_partitioner(uint64_t num_warehouses,
                                           std::mt19937 &rd);

#endif /* PARTITIONER_H */
