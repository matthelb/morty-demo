// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replica.h:
 *   interface to different vr protocols
 *
 * Copyright 2013-2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                     Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *                     Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef REPLICATION_COMMON_REPLICA_H_
#define REPLICATION_COMMON_REPLICA_H_

#include <string>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "replication/common/log.h"
#include "replication/common/request.pb.h"
#include "replication/common/viewstamp.h"
#include "store/common/timestamp.h"

namespace replication {

class Replica;
class Request;
class UnloggedRequest;

enum ReplicaStatus { STATUS_NORMAL, STATUS_VIEW_CHANGE, STATUS_RECOVERING };

class AppReplica {
 public:
  AppReplica() = default;
  virtual ~AppReplica() = default;
  // Invoke callback on the leader, with the option to replicate on success
  virtual void LeaderUpcall(opnum_t opnum, const std::string &str1,
                            bool *replicate, std::string *str2) {
    *replicate = true;
    *str2 = str1;
  }
  // Invoke callback on all replicas
  virtual void ReplicaUpcall(opnum_t opnum, const std::string &str1,
                             std::string *str2) {}
  // Invoke call back for unreplicated operations run on only one replica
  virtual void UnloggedUpcall(const std::string &str1, std::string *str2) {}
};

class Replica : public TransportReceiver {
 public:
  Replica(const transport::Configuration &configuration, int groupIdx,
          int myIdx, Transport *transport, AppReplica *app);
  ~Replica() override;

 protected:
  void LeaderUpcall(opnum_t opnum, const std::string &op, bool *replicate,
                    std::string *res);
  void ReplicaUpcall(opnum_t opnum, const std::string &op, std::string *res);
  template <class MSG>
  void Execute(opnum_t opnum, const Request &msg, MSG *reply);
  void UnloggedUpcall(const std::string &op, std::string *res);
  template <class MSG>
  void ExecuteUnlogged(const UnloggedRequest &msg, MSG *reply);

 protected:
  transport::Configuration configuration;
  int groupIdx;
  int myIdx;
  Transport *transport;
  AppReplica *app;
  ReplicaStatus status;
};

#include "replication/common/replica-inl.h"

}  // namespace replication

#endif  // REPLICATION_COMMON_REPLICA_H_
