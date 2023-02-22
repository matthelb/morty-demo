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
#include "store/mortystorev2/version.h"

#include "store/mortystorev2/morty-proto.pb.h"

namespace mortystorev2 {

Version::Version() = default;

Version::Version(uint64_t clientId, uint64_t clientSeq)
    : clientId(clientId), clientSeq(clientSeq) {}

Version::Version(const proto::Version &v)
    : clientId(v.client_id()), clientSeq(v.client_seq()) {}

Version::Version(const Version &v) = default;

Version::~Version() = default;

void Version::Serialize(proto::Version *version) const {
  version->set_client_id(clientId);
  version->set_client_seq(clientSeq);
}

bool operator==(const Version &a, const Version &b) {
  return a.clientId == b.clientId && a.clientSeq == b.clientSeq;
}

bool operator<(const Version &a, const Version &b) {
  return a.clientSeq < b.clientSeq ||
         (a.clientSeq == b.clientSeq && a.clientId < b.clientId);
}

size_t VersionHasher::operator()(const Version &v) const {
  return (v.GetClientSeq() << 14) | (v.GetClientId() & 0b11111111111111);
}

size_t ExecVersionHasher::operator()(const ExecVersion &v) const {
  return vh(v.first) ^ uih(v.second);
}

}  // namespace mortystorev2
