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

#include "lib/partitionedthreadpool.h"

#include <pthread.h>
#include <sched.h>

#include <functional>
#include <memory>
#include <utility>

#include "lib/concurrentqueue/blockingconcurrentqueue.h"
#include "lib/message.h"

PartitionedThreadPool::PartitionedThreadPool()
    : running_(false), worker_task_mtx_(nullptr), worker_task_cv_(nullptr) {}

PartitionedThreadPool::~PartitionedThreadPool() { Stop(); }

void PartitionedThreadPool::Start(uint64_t num_workers,
                                  uint64_t start_pin_core) {
  num_workers_ = num_workers;
  // worker_task_mtx_ = new std::mutex[num_workers];
  // worker_task_cv_ = new std::condition_variable[num_workers];

  running_ = true;
  for (uint64_t i = 0; i < num_workers; ++i) {
    worker_tasks_.emplace(worker_tasks_.end());
  }
  for (uint64_t i = 0; i < num_workers; ++i) {
    std::thread *t =
        new std::thread(std::bind(&PartitionedThreadPool::RunWorker, this, i));
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i + start_pin_core, &cpuset);
    int rc =
        pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      Panic("Error calling pthread_setaffinity_np: %d", rc);
    }
    workers_.push_back(t);
    // t->detach();
  }
}

void PartitionedThreadPool::Stop() {
  running_ = false;
  for (uint64_t i = 0; i < num_workers_; ++i) {
    // worker_task_cv_[i].notify_one();
    Dispatch(i, []() {});
  }
  for (auto &worker : workers_) {
    worker->join();
    delete worker;
  }

  workers_.clear();
  worker_tasks_.clear();

  if (worker_task_mtx_ != nullptr) {
    // delete [] worker_task_mtx_;
  }
  if (worker_task_cv_ != nullptr) {
    // delete [] worker_task_cv_;
  }
  num_workers_ = 0;
}

void PartitionedThreadPool::Dispatch(Task task) {
  shared_tasks_.push(std::move(task));
  for (uint64_t i = 0; i < num_workers_; ++i) {
    worker_task_cv_[i].notify_one();
  }
}

void PartitionedThreadPool::Dispatch(uint64_t thread_id, Task task) {
  // std::unique_lock<std::mutex> lock(worker_task_mtx_[thread_id]);
  worker_tasks_[thread_id].enqueue(std::move(task));
  // worker_task_cv_[thread_id].notify_one();
}

void PartitionedThreadPool::RunWorker(uint64_t thread_id) {
  Task task;
  while (true) {
    {
      /*std::unique_lock<std::mutex> lock(worker_task_mtx_[thread_id]);
      bool has_shared_task = false;
      while (running_ &&
          !(has_shared_task = shared_tasks_.try_pop(task)) &&
          worker_tasks_[thread_id].size() == 0) {
        worker_task_cv_[thread_id].wait(lock);
      }
      // !running_ || shared_tasks_.size() > 0 ||
      worker_tasks_[thread_id].size() > 0
      */

      worker_tasks_[thread_id].wait_dequeue(task);

      if (!running_) {
        break;
      }

      /*if (!has_shared_task && !shared_tasks_.try_pop(task) &&
          worker_tasks_[thread_id].size() > 0) {
        task = std::move(worker_tasks_[thread_id].front());
        worker_tasks_[thread_id].pop();
      }*/
    }

    task();
  }
}
