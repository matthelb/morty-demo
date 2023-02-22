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
#ifndef MORTYV2_VERSION_H
#define MORTYV2_VERSION_H

#include <bits/stdint-uintn.h>

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "store/mortystorev2/morty-proto.pb.h"

namespace mortystorev2 {

namespace proto {
class Version;
}  // namespace proto

class Version {
 public:
  Version();
  Version(uint64_t clientId, uint64_t clientSeq);
  Version(const proto::Version &v);
  Version(const Version &v);
  virtual ~Version();

  void Serialize(proto::Version *version) const;

  inline uint64_t GetClientId() const { return clientId; }
  inline uint64_t GetClientSeq() const { return clientSeq; }

 private:
  friend bool operator==(const Version &a, const Version &b);
  friend bool operator<(const Version &a, const Version &b);

  uint64_t clientId{};
  uint64_t clientSeq{};
};

bool operator==(const Version &a, const Version &b);
bool operator<(const Version &a, const Version &b);

struct VersionHasher {
  size_t operator()(const Version &v) const;
};

typedef std::pair<Version, uint64_t> ExecVersion;
struct ExecVersionHasher {
  size_t operator()(const ExecVersion &v) const;
  VersionHasher vh;
  std::hash<uint64_t> uih;
};

typedef std::unordered_set<Version, VersionHasher> UnorderedVersionSet;

}  // namespace mortystorev2

#endif /* MORTYV2_VERSION_H */
