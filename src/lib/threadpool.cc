// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lib/threadpool.cc:
 *    Runs tasks on a shared pool of workers.
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

#include "lib/threadpool.h"

#include <bits/stdint-uintn.h>
#include <event2/event.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <thread>  // NOLINT[build/c++11]
#include <type_traits>
#include <utility>

#include "lib/concurrentqueue/blockingconcurrentqueue.h"

struct event;
struct event_base;

// TODO(matthelb): make is so that all but the first core are used.
ThreadPool::ThreadPool() = default;

void ThreadPool::start(int process_id, int total_processes, bool hyperthreading,
                       bool server) {
  // printf("starting threadpool \n");
  // could pre-allocate some Events and EventInfos for a Hotstart
  if (server) {
    fprintf(stderr, "starting server threadpool\n");
    fprintf(stderr, "process_id: %d, total_processes: %d \n", process_id,
            total_processes);
    // TODO(matthelb): add config param for hyperthreading
    // bool hyperthreading = true;
    int num_cpus =
        8;  // std::thread::hardware_concurrency(); ///(2-hyperthreading);
    fprintf(stderr, "Num_cpus: %d \n", num_cpus);
    num_cpus /= total_processes;
    int offset = process_id * num_cpus;
    Debug("num cpus %d", num_cpus);
    uint32_t num_threads = static_cast<uint32_t>(std::max(1, num_cpus));
    // Currently: First CPU = MainThread.
    running = true;
    for (uint32_t i = 1; i < num_threads; i++) {
      // if(i % 2 == 0) continue;
      std::thread* t;
      // Mainthread
      if (i == 1) {
        t = new std::thread([this, i] {
          while (true) {
            std::function<void*()> job;
            {
              // only acquire the lock in this block so that the
              // std::function execution is not holding the lock
              Debug("Thread %d running on CPU %d.", i, sched_getcpu());

              test_main_worklist.wait_dequeue(job);
              // while(!test_main_worklist.try_dequeue(job)) {};

              // std::unique_lock<std::mutex> lock(this->main_worklistMutex);
              // cv_main.wait(lock, [this] { return this->main_worklist.size() >
              // 0 || !running; });
              if (!running) {
                break;
              }
              // if (this->main_worklist.size() == 0) {
              //   continue;
              // }
              // job = std::move(this->main_worklist.front());
              // this->main_worklist.pop_front();
            }
            job();
          }
        });
      } else {
        // Cryptothread
        t = new std::thread([this, i] {
          while (true) {
            std::pair<std::function<void*()>, EventInfo*> job;
            // std::function<void*()> job;
            {
              // only acquire the lock in this block so that the
              // std::function execution is not holding the lock
              Debug("Thread %d running on CPU %d.", i, sched_getcpu());

              // std::unique_lock<std::mutex> lock(this->worklistMutex);
              // //STABLE_VERSION cv.wait(lock, [this] { return
              // this->worklist.size() > 0 || !running; });   //STABLE_VERSION

              test_worklist.wait_dequeue(job);
              // while(!test_worklist.try_dequeue(job)) {};

              // std::shared_lock lock(this->dummyMutex);
              // cv.wait(lock, [this, &job] { return this->testlist.try_pop(job)
              // || !running; }); while(!testlist.try_pop(job) || !running) {}
              Debug("popped job on CPU %d.", i);
              if (!running) {
                break;
              }
              // if (this->worklist.size() == 0) { //STABLE_VERSION
              //   continue;
              // }
              // job = std::move(this->worklist.front()); //STABLE_VERSION
              // this->worklist.pop_front(); //STABLE_VERSION
            }
            // job();

            if (job.second != nullptr) {
              job.second->r = job.first();
              // This _should_ be thread safe
              event_active(job.second->ev, 0, 0);
            } else {
              job.first();
            }
          }
        });
      }
      // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
      // only CPU i as set.
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(i + offset, &cpuset);
      if (i + offset > 7) {
        return;
      }
      std::cerr << "Trying to pin to core: " << i << " + " << offset
                << std::endl;
      int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t),
                                      &cpuset);
      if (rc != 0) {
        Panic("Error calling pthread_setaffinity_np: %d", rc);
      }
      Debug("MainThread running on CPU %d.", sched_getcpu());
      threads.push_back(t);
      t->detach();
    }
  } else {
    fprintf(stderr, "starting client threadpool\n");
    int num_cpus = std::thread::hardware_concurrency();  // /(2-hyperthreading);
    fprintf(stderr, "Num_cpus: %d \n", num_cpus);
    num_cpus /= total_processes;
    num_cpus = 8;  // XXX change back to dynamic
    // int offset = process_id * num_cpus;
    Debug("num cpus %d", num_cpus);
    uint32_t num_threads = static_cast<uint32_t>(std::max(1, num_cpus));
    running = true;
    for (uint32_t i = 0; i < num_threads; i++) {
      std::thread* t;
      t = new std::thread([this, i] {
        while (true) {
          std::pair<std::function<void*()>, EventInfo*> job;
          {
            Debug("Thread %d running on CPU %d.", i, sched_getcpu());

            test_worklist.wait_dequeue(job);
            // while(!test_worklist.try_dequeue(job)) {};

            // std::unique_lock<std::mutex> lock(this->worklistMutex);
            // cv.wait(lock, [this] { return this->worklist.size() > 0 ||
            // !running; });
            if (!running) {
              break;
            }
            // if (this->worklist.size() == 0) {
            //   continue;
            // }
            // job = std::move(this->worklist.front());
            // this->worklist.pop_front();
          }
          if (job.second != nullptr) {
            job.second->r = job.first();
            event_active(job.second->ev, 0, 0);
          } else {
            job.first();
          }
        }
      });
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(i, &cpuset);
      int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpu_set_t),
                                      &cpuset);
      if (rc != 0) {
        Panic("Error calling pthread_setaffinity_np: %d", rc);
      }
      Debug("MainThread running on CPU %d.", sched_getcpu());
      threads.push_back(t);
      t->detach();
    }
  }
}

ThreadPool::~ThreadPool() {
  stop();
  // delete test_worklistMutex;
  // delete test_cv;
}

void ThreadPool::stop() {
  running = false;
  cv.notify_all();
  cv_main.notify_all();
  // for(auto t: threads){
  //    t->join();
  //    delete t;
  // }
}

void ThreadPool::EventCallback(evutil_socket_t fd, int16_t what, void* arg) {
  // we want to run the callback in the main event loop
  auto* info = static_cast<EventInfo*>(arg);
  info->cb(info->r);

  info->tp->FreeEvent(info->ev);
  // event_free(info->ev);
  info->tp->FreeEventInfo(info);
  // delete info;
}

void ThreadPool::dispatch(std::function<void*()> f,
                          std::function<void(void*)> cb,
                          event_base* libeventBase) {
  // EventInfo* info = new EventInfo(this);
  EventInfo* info = GetUnusedEventInfo();
  info->cb = std::move(cb);
  // info->ev = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, nullptr);

  // safe to moveinfo? dont expect it to do anything though, since its just a
  // pointer
  //  std::pair<std::function<void*()>, EventInfo*> job(std::move(f),
  //  std::move(info));

  // std::lock_guard<std::mutex> lk(worklistMutex);
  // //worklist.push_back(std::move(job));
  // worklist.emplace_back(std::move(f), std::move(info));

  test_worklist.enqueue(std::make_pair(std::move(f), info));

  cv.notify_one();
}

void* ThreadPool::combiner(const std::function<void*()>& f,
                           const std::function<void(void*)>& cb) {
  cb(f());
  return nullptr;
}

void ThreadPool::dispatch_local(std::function<void*()> f,
                                std::function<void(void*)> cb) {
  EventInfo* info = nullptr;
  auto combination = [f = std::move(f), cb = std::move(cb)]() {
    cb(f());
    return nullptr;
  };
  // std::function<void*()> combination(std::bind(ThreadPool::combiner,
  // std::move(f), std::move(cb))); std::pair<std::function<void*()>,
  // EventInfo*> job(std::move(combination), info);

  // std::lock_guard<std::mutex> lk(worklistMutex); //STABLE_VERSION
  // worklist.emplace_back(std::move(combination), info); //STABLE_VERSION

  test_worklist.enqueue(std::make_pair(std::move(combination), info));

  // std::pair<std::function<void*()>, EventInfo*> job(std::move(combination),
  // info); testlist.push(job);

  // worklist2.push_back(std::move(combination));
  // worklist.push_back(std::move(job));
  cv.notify_one();
}

void ThreadPool::detatch(std::function<void*()> f) {
  EventInfo* info = nullptr;
  // std::pair<std::function<void*()>, EventInfo*> job(std::move(f), info);

  // std::lock_guard<std::mutex> lk(worklistMutex); //STABLE_VERSION
  // worklist.emplace_back(std::move(f), info); //STABLE_VERSION

  test_worklist.enqueue(std::make_pair(std::move(f), info));

  // std::pair<std::function<void*()>, EventInfo*> job(std::move(f), info);
  // testlist.push(job);

  // worklist2.push_back(std::move(f));
  cv.notify_one();
}

void ThreadPool::detatch_ptr(std::function<void*()>* f) {
  EventInfo* info = nullptr;
  // std::pair<std::function<void*()>, EventInfo*> job(std::move(f), info);

  // std::lock_guard<std::mutex> lk(worklistMutex); //STABLE_VERSION
  // worklist.emplace_back(std::move(*f), info); //STABLE_VERSION

  test_worklist.enqueue(std::make_pair(std::move(*f), info));

  // std::pair<std::function<void*()>, EventInfo*> job(std::move(*f), info);
  // testlist.push(job);

  // worklist2.push_back(std::move(*f));
  cv.notify_one();
}

void ThreadPool::detatch_main(std::function<void*()> f) {
  // EventInfo* info = nullptr;

  // std::lock_guard<std::mutex> lk(main_worklistMutex);
  // main_worklist.push_back(std::move(f));

  test_main_worklist.enqueue(std::move(f));
  // test_worklist.enqueue(std::make_pair(std::move(f), info));

  cv_main.notify_one();
}

////////////////////////////////
// requires transport object to call this... (add to the verifyObj)
// could alternatively use:
// transport->Timer(0, f)   // expects a timer_callback_t though, which is a
// void(void) typedef
// could make f purely void, if I refactored a bunch
// lazy solution:
// transport->Timer(0, [](){f(new bool(true));})
void ThreadPool::issueCallback(std::function<void(void*)> cb, void* arg,
                               event_base* libeventBase) {
  EventInfo* info = GetUnusedEventInfo();  // new EventInfo(this);
  info->cb = std::move(cb);
  info->r = arg;
  // info->ev = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  info->ev = GetUnusedEvent(libeventBase, info);
  event_add(info->ev, nullptr);
  event_active(info->ev, 0, 0);
}

////////////////////////////////////////

ThreadPool::EventInfo* ThreadPool::GetUnusedEventInfo() {
  std::unique_lock<std::mutex> lock(EventInfoMutex);
  EventInfo* info;
  if (!eventInfos.empty()) {
    info = eventInfos.back();
    eventInfos.pop_back();
  } else {
    info = new EventInfo(this);
  }
  return info;
}

void ThreadPool::FreeEventInfo(EventInfo* info) {
  std::unique_lock<std::mutex> lock(EventInfoMutex);
  eventInfos.push_back(info);
}

event* ThreadPool::GetUnusedEvent(event_base* libeventBase, EventInfo* info) {
  std::unique_lock<std::mutex> lock(EventMutex);
  event* event;
  if (!events.empty()) {
    event = events.back();
    events.pop_back();
    event_assign(event, libeventBase, -1, 0, ThreadPool::EventCallback, info);
  } else {
    event = event_new(libeventBase, -1, 0, ThreadPool::EventCallback, info);
  }
  return event;
}

void ThreadPool::FreeEvent(event* event) {
  std::unique_lock<std::mutex> lock(EventMutex);
  event_del(event);
  events.push_back(event);
}
