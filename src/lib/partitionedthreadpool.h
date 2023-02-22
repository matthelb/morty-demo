// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lib/partitionedthreadpool.h:
 *    Thread pool that allows tasks to be run on specific workers.
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

#ifndef LIB_PARTITIONEDTHREADPOOL_H_
#define LIB_PARTITIONEDTHREADPOOL_H_

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>

#include <condition_variable>  // NOLINT[build/c++11]
#include <functional>
#include <mutex>  // NOLINT[build/c++11]
#include <queue>
#include <thread>  // NOLINT[build/c++11]
#include <utility>
#include <vector>

#include "concurrentqueue/blockingconcurrentqueue.h"
#include "tbb/concurrent_queue.h"

using Task = std::function<void()>;
class PartitionedThreadPool {
 public:
  PartitionedThreadPool();
  virtual ~PartitionedThreadPool();

  void Start(uint64_t num_workers, uint64_t start_pin_core);
  void Stop();

  void Dispatch(Task task);
  void Dispatch(uint64_t thread_id, Task task);

 private:
  void RunWorker(uint64_t thread_id);

  uint64_t num_workers_{};
  bool running_;
  tbb::concurrent_queue<Task> shared_tasks_;
  std::vector<std::thread*> workers_;
  std::vector<moodycamel::BlockingConcurrentQueue<Task>> worker_tasks_;
  std::mutex* worker_task_mtx_;
  std::condition_variable* worker_task_cv_;
};

#endif  // LIB_PARTITIONEDTHREADPOOL_H_
