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
#include "store/common/backend/loader.h"

#include <pthread.h>
#include <sched.h>
#include <stddef.h>

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "lib/io_utils.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "store/common/common.h"
#include "store/common/partitioner.h"
#include "store/common/timestamp.h"
#include "store/server.h"

Loader::Loader(Server *server, Partitioner *part, uint64_t num_shards,
               uint64_t num_groups, uint64_t group_idx, uint64_t multi_thread)
    : server_(server),
      part_(part),
      num_shards_(num_shards),
      num_groups_(num_groups),
      group_idx_(group_idx),
      multi_thread_(multi_thread) {}

Loader::~Loader() = default;

int Loader::Load(const std::string &data_file_path) {
  // check if data file is split into parts
  std::string data_file_path_part(data_file_path);
  data_file_path_part += ".0";
  std::ifstream data_file_part_in(data_file_path_part);
  if (data_file_part_in) {
    if (multi_thread_ != 0u) {
      data_file_part_in.close();

      std::vector<std::thread *> threads;
      int i = 0;

      std::mutex thread_mtx;
      uint64_t threads_active = 0;
      std::condition_variable done_cv;

      while (true) {
        std::string part_file = data_file_path + "." + std::to_string(i);
        auto *in = new std::ifstream(part_file);
        if (!*in) {
          break;
        }

        Debug("Loading data from file %s.", part_file.c_str());

        {
          std::unique_lock<std::mutex> lock(thread_mtx);
          threads_active++;
        }

        std::thread *thread =
            new std::thread([thread_id = i, this, in, &thread_mtx,
                             &threads_active, &done_cv]() {
              LoadInternal(*in);
              delete in;

              std::unique_lock<std::mutex> lock(thread_mtx);
              threads_active--;
              done_cv.notify_one();
            });

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        for (size_t j = 0; j < std::thread::hardware_concurrency() / 2; ++j) {
          CPU_SET(j, &cpuset);
        }
        pthread_setaffinity_np(thread->native_handle(), sizeof(cpu_set_t),
                               &cpuset);

        threads.push_back(thread);
        i++;

        {
          std::unique_lock<std::mutex> lock(thread_mtx);
          while (threads_active == multi_thread_) {
            done_cv.wait(lock);
          }
        }
      }

      {
        std::unique_lock<std::mutex> lock(thread_mtx);
        while (threads_active != 0) {
          done_cv.wait(lock);
        }
      }

      for (auto &thread : threads) {
        thread->join();
        delete thread;
      }

      return 0;
    }
    int ret = 0;
    int i = 0;
    while (data_file_part_in) {
      ret = LoadInternal(data_file_part_in);
      if (ret == 1) {
        return 1;
      }
      data_file_part_in.close();
      i++;
      data_file_part_in.open(data_file_path + "." + std::to_string(i));
    }
    return 0;
  }
  // data file is not split into parts
  std::ifstream data_file_in(data_file_path);
  if (data_file_in) {
    return LoadInternal(data_file_in);
  }
  return 1;
}

int Loader::LoadInternal(std::ifstream &in) {
  Latency_t total_load_time;
  /*Latency_t read_time;
  Latency_t populate_time;
  Latency_t misc_time;
  Latency_t part_time;*/
  std::string latency_name =
      "total_load_time_" +
      std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
  _Latency_Init(&total_load_time, latency_name.c_str());
  /*_Latency_Init(&read_time, "read_time");
  _Latency_Init(&populate_time, "populate_time");
  _Latency_Init(&misc_time, "misc_time");
  _Latency_Init(&part_time, "part_time");*/

  Latency_Start(&total_load_time);

  // Latency_Start(&misc_time);

  size_t loaded = 0;
  size_t stored = 0;
  // Debug("Populating with data from %s.", data_file_path.c_str());
  std::vector<int> txn_groups;
  std::vector<size_t> shard_counts(num_shards_, 0UL);

  // Latency_End(&misc_time);

  while (!in.eof()) {
    std::string key;
    std::string value;
    Timestamp ts(0, 0);
    // Latency_Start(&read_time);
    int i = ReadBytesFromStream(&in, &key);
    if (i == 0) {
      ReadBytesFromStream(&in, &value);
      // Latency_End(&read_time);
      // Latency_Start(&part_time);
      uint64_t shard = (*part_)(key, num_shards_, group_idx_, txn_groups);

      if (loaded % 100000 == 0) {
        Debug("  Loaded key %s.", BytesToHex(key, key.length()).c_str());
      }
      // Latency_End(&part_time);
      shard_counts[shard]++;
      if (shard % num_groups_ == group_idx_) {
        // Latency_Start(&populate_time);
        server_->Load(std::move(key), std::move(value), std::move(ts));
        // Latency_End(&populate_time);
        ++stored;
      }

      ++loaded;
    } else {
      // Latency_End(&read_time);
    }
  }
  uint64_t total = Latency_End(&total_load_time);

  /*Latency_Dump(&total_load_time);
  Latency_Dump(&read_time);
  Latency_Dump(&populate_time);
  Latency_Dump(&misc_time);
  Latency_Dump(&part_time);*/
  Debug("Total time: %lu ms", total / 1000000UL);

  for (size_t i = 0; i < shard_counts.size(); ++i) {
    Debug("  Shard %lu: %lu entries.", i, shard_counts[i]);
  }
  Debug("Stored %lu out of %lu key-value pairs.", stored, loaded);
  // Debug("Stored %lu out of %lu key-value pairs from file %s.", stored,
  //     loaded, data_file_path.c_str());
  return 0;
}
