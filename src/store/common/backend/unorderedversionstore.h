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
#ifndef _UNORDERED_VERSIONED_KV_STORE_H_
#define _UNORDERED_VERSIONED_KV_STORE_H_

#include <map>
#include <set>
#include <unordered_map>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"

template <class T, class V>
class UnorderedVersionedKVStore {
 public:
  UnorderedVersionedKVStore();
  ~UnorderedVersionedKVStore();

  bool get(const std::string &key, std::pair<T, V> &value);
  void put(const std::string &key, const V &v, const T &t);
  void commitGet(const std::string &key, const T &t);
  bool getLastReads(const std::string &key, std::set<T> &reads);

 private:
  struct VersionedValue {
    T write;
    V value;

    explicit VersionedValue(const T &commit) : write(commit) {}
    VersionedValue(const T &commit, const V &val) : write(commit), value(val) {}

    friend bool operator>(const VersionedValue &v1, const VersionedValue &v2) {
      return v1.write > v2.write;
    }
    friend bool operator<(const VersionedValue &v1, const VersionedValue &v2) {
      return v1.write < v2.write;
    }
  };

  /* Global store which keeps key -> (timestamp, value) list. */
  std::unordered_map<std::string, std::vector<VersionedValue>> store;
  std::unordered_map<std::string, std::set<T>> lastReads;
  bool inStore(const std::string &key);
};

template <class T, class V>
UnorderedVersionedKVStore<T, V>::UnorderedVersionedKVStore() = default;

template <class T, class V>
UnorderedVersionedKVStore<T, V>::~UnorderedVersionedKVStore() = default;

template <class T, class V>
bool UnorderedVersionedKVStore<T, V>::inStore(const std::string &key) {
  return store.find(key) != store.end() && store[key].size() > 0;
}

/* Returns the most recent value and timestamp for given key.
 * Error if key does not exist. */
template <class T, class V>
bool UnorderedVersionedKVStore<T, V>::get(const std::string &key,
                                          std::pair<T, V> &value) {
  // check for existence of key in store
  if (inStore(key)) {
    UnorderedVersionedKVStore<T, V>::VersionedValue v = *(store[key].rbegin());
    value = std::make_pair(v.write, v.value);
    return true;
  }
  return false;
}

template <class T, class V>
void UnorderedVersionedKVStore<T, V>::put(const std::string &key,
                                          const V &value, const T &t) {
  // Key does not exist. Create a list and an entry.
  store[key].push_back(
      UnorderedVersionedKVStore<T, V>::VersionedValue(t, value));
  lastReads.erase(key);
}

template <class T, class V>
void UnorderedVersionedKVStore<T, V>::commitGet(const std::string &key,
                                                const T &t) {
  lastReads[key].insert(t);
}

template <class T, class V>
bool UnorderedVersionedKVStore<T, V>::getLastReads(const std::string &key,
                                                   std::set<T> &reads) {
  if (inStore(key)) {
    reads = lastReads[key];
    return true;
  }
  return false;
}

#endif /* _UNORDERED_VERSIONED_KV_STORE_H_ */
