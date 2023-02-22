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
#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H

#include "tbb/concurrent_queue.h"

template <class T>
class MessageQueue {
 public:
  MessageQueue();
  virtual ~MessageQueue();

  T *Pop();
  void Push(T *msg);

  std::string GetMessageType();

 private:
  tbb::concurrent_queue<T *> queue_{};
};

template <class T>
MessageQueue<T>::MessageQueue() = default;

template <class T>
MessageQueue<T>::~MessageQueue() {
  while (!queue_.empty()) {
    delete Pop();
  }
}

template <class T>
T *MessageQueue<T>::Pop() {
  T *msg;
  if (queue_.try_pop(msg)) {
    msg->Clear();
    return msg;
  }
  return new T();
}

template <class T>
void MessageQueue<T>::Push(T *msg) {
  queue_.push(msg);
}

template <class T>
std::string MessageQueue<T>::GetMessageType() {
  T *msg = Pop();
  std::string type = msg->GetTypeName();
  Push(msg);
  return std::move(type);
}

#endif /* MESSAGE_QUEUE_H */
