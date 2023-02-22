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
#ifndef MORTYV2_SERVER_H
#define MORTYV2_SERVER_H

#include <bits/stdint-uintn.h>

#include <atomic>
#include <cstdint>
#include <ctime>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/partitionedthreadpool.h"
#include "lib/transport.h"
#include "replication/common/replica.h"
#include "store/common/backend/messageserver.h"
#include "store/common/backend/txnstore.h"
#include "store/common/backend/unorderedversionstore.h"
#include "store/common/stats.h"
#include "store/common/truetime.h"
#include "store/mortystorev2/common.h"
#include "store/mortystorev2/messagequeue.h"
#include "store/mortystorev2/morty-proto.pb.h"
#include "store/mortystorev2/version.h"
#include "store/server.h"
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_unordered_set.h"

class Timestamp;
namespace transport {
class Configuration;
}  // namespace transport

namespace google {
namespace protobuf {
class Message;
}  // namespace protobuf
}  // namespace google

namespace mortystorev2 {

class Server : public TransportReceiver, public ::Server {
 public:
  Server(const transport::Configuration &config, int group_idx, int idx,
         std::vector<Transport *> transport, uint64_t num_workers,
         bool pipeline_commit, bool reexecution_enabled,
         uint64_t gc_watermark_buffer_us, uint64_t num_cores_per_numa_node,
         uint64_t num_numa_nodes);
  ~Server() override;

  void ReceiveMessage(const TransportAddress &remote, std::string *type,
                      std::string *data, void *meta_data) override;

  inline Stats &GetStats() override { return stats; };

  void Load(std::string &&key, std::string &&val,
            Timestamp &&timestamp) override;

 private:
  struct DRecord {
    DRecord()

    {}
    uint64_t view{0};
    uint64_t finalize_view{0};
    proto::Decision finalize_decision{proto::DECISION_NONE};
    proto::Decision decision{proto::DECISION_NONE};
  };

  typedef std::pair<const TransportAddress *, uint64_t> coordinator_t;
  struct ERecord {
    ERecord() {}
    ~ERecord() = default;

    proto::Transaction *txn{nullptr};
    proto::Vote vote{proto::VOTE_NONE};
    DRecord drecord;

    std::unordered_set<Version, VersionHasher> uncommitted_deps{};
    // initially false, set to true the first time that CheckDependencies
    // returns that this ERecord has an uncommitted dep. only used as an
    // optimization to avoid redundant AddDependent calls when checking
    // dependencies.
    bool added_as_dependent{false};

    std::set<coordinator_t> waiting_prepares{};
    std::map<coordinator_t, std::tuple<uint64_t, proto::Decision>>
        waiting_finalizes;
    std::map<coordinator_t, proto::Decision> waiting_decides{};
  };

  struct TRecord {
    TRecord() = default;
    std::unordered_map<uint64_t, ERecord> erecord{};
    std::set<ExecVersion> dependents{};
    std::unordered_set<std::string> all_execs_reads{};
    std::unordered_set<std::string> all_execs_writes{};
  };

  struct Read {
    Read(const Version &version_, const TransportAddress *coordinator_,
         uint64_t req_id_)
        : version(version_), coordinator(coordinator_), req_id(req_id_) {}
    ~Read() { delete coordinator; }
    const Version version;
    const TransportAddress *coordinator;
    const uint64_t req_id;
    Version write_version;
    std::string write_val{};
  };

  struct Write {
    explicit Write(proto::Write *msg)
        : version(msg->txn_version()),
          val(std::move(*msg->release_value())),
          val_id(msg->val_id()) {}
    Write(proto::Transaction *txn, proto::WriteSetEntry &wse)
        : version(txn->version()),
          val(std::move(*wse.release_val())),
          val_id(wse.val_id()) {}
    Write(const Version &version_, const std::string &val_, uint64_t val_id_)
        : version(version_), val(val_), val_id(val_id_) {}
    Write(uint64_t version_client_seq, uint64_t version_client_id,
          std::string &&val_, uint64_t val_id_)
        : version(version_client_seq, version_client_id),
          val(val_),
          val_id(val_id_) {}
    Write(Version &&version_, std::string &&val_, uint64_t val_id_)
        : version(version_), val(val_), val_id(val_id_) {}
    const Version version;
    const std::string val{};
    const uint64_t val_id;
  };

  struct VRecord {
    std::map<Version, Read> reads;
    std::map<Version, Write> writes;
    std::mutex mtx;
    std::map<ExecVersion, Version> prepared_reads;
    std::set<ExecVersion> prepared_writes;
    std::map<ExecVersion, Version> committed_reads;
    std::set<ExecVersion> committed_writes;
    Latency_t *prepared_time;
    Latency_t *conflict_window;
  };

  void HandleRead(const TransportAddress &remote, google::protobuf::Message *m);
  void HandleWrite(const TransportAddress &remote,
                   google::protobuf::Message *m);
  void HandlePrepare(const TransportAddress &remote,
                     google::protobuf::Message *m);
  void HandleFinalize(const TransportAddress &remote,
                      google::protobuf::Message *m);
  void HandleDecide(const TransportAddress &remote,
                    google::protobuf::Message *m);
  void HandlePaxosPrepare(const TransportAddress &remote,
                          google::protobuf::Message *m);

  void ReceiveMessageInternal(const TransportAddress *remote, std::string &type,
                              std::string &data, void *meta_data);
  void HandleReadInternal(const TransportAddress *remote, proto::Read *msg);
  void HandleWriteInternal(const TransportAddress *remote, proto::Write *msg);
  void HandlePrepareInternal(const TransportAddress *remote,
                             proto::Prepare *msg);
  void HandleFinalizeInternal(const TransportAddress *remote,
                              proto::Finalize *msg);
  void HandleDecideInternal(const TransportAddress *remote, proto::Decide *msg);
  void HandlePaxosPrepareInternal(const TransportAddress *remote,
                                  proto::PaxosPrepare *msg);

  void UpdateReadsWithWrite(const std::string &key, VRecord &vrecord,
                            const Write &write);
  void SendReadReply(const std::string &key, Read &read, const Write *write,
                     bool need_dispatch);
  void SendPrepareReply(ERecord &erecord, coordinator_t coordinator);
  void SendDecideReply(ERecord &erecord, coordinator_t coordinator,
                       proto::Decision decision, bool deps_changed_decision);
  void SendFinalizeReply(ERecord &erecord, coordinator_t coordinator);
  bool CheckDependencies(ERecord &erecord, bool &aborted_dep);
  void TryPrepare(ERecord &erecord, coordinator_t coordinator);
  void TryPrepare(ExecVersion exec_version, coordinator_t coordinator);
  void AddDependent(Version version, ExecVersion dependent);
  void AddTrackedRead(Version version, std::string key);
  void AddTrackedWrite(Version version, std::string key);
  void TryFinalize(ERecord *erecord, TRecord *trecord,
                   coordinator_t coordinator, uint64_t view,
                   proto::Decision decision);
  void TryFinalize(ExecVersion exec_version, coordinator_t coordinator,
                   uint64_t view, proto::Decision decision);
  void ProcessFinalize(ERecord *erecord, TRecord *trecord,
                       coordinator_t coordinator, uint64_t view,
                       proto::Decision decision);
  void CheckWaiting(ExecVersion exec_version);
  void TryDecide(ERecord *erecord, TRecord *trecord, coordinator_t coordinator,
                 proto::Decision decision);
  void TryDecide(ExecVersion exec_version, coordinator_t coordinator,
                 proto::Decision decision);
  void ProcessDecide(ERecord *erecord, TRecord *trecord,
                     coordinator_t coordinator, proto::Decision decision,
                     bool deps_changed_decision);

  // must hold lock on vrecord before calling
  const Write *GetReadValue(VRecord &vrecord, const Version &version);
  proto::Vote Validate(ERecord &erecord, coordinator_t coordinator);
  void CleanPreparedTransaction(proto::Transaction *txn);
  void UpdateWatermark();
  ERecord &GetOrCreateERecord(const Version &version, uint64_t exec_id,
                              TRecord *&trecord);
  bool GetERecord(const Version &version, uint64_t exec_id, ERecord *&erecord,
                  TRecord *&trecord);
  VRecord &GetOrCreateVRecord(const std::string &key);

  const transport::Configuration &config_;
  const int group_idx_;
  const int idx_;

  std::vector<Transport *> transport_;
  const uint64_t num_workers_;
  const bool pipeline_commit_;
  const bool reexecution_enabled_;
  // gc_watermark periodically advances to server's local time minus this buffer
  //   (in microseconds)
  const uint64_t gc_watermark_buffer_us_;

  uint64_t round_robin_thread_dispatch_;

  // typedef void(*MessageHandler)(const TransportAddress &,
  // google::protobuf::Message *); std::unordered_map<std::string,
  // std::pair<MessageQueue<google::protobuf::Message> *, MessageHandler>>
  // msg_handlers_; PartitionedThreadPool thread_pool_;
  std::vector<std::thread *> worker_threads_;
  enum MessageType {
    MESSAGE_TYPE_READ = 0,
    MESSAGE_TYPE_WRITE = 1,
    MESSAGE_TYPE_PREPARE = 2,
    MESSAGE_TYPE_FINALIZE = 3,
    MESSAGE_TYPE_DECIDE = 4,
    MESSAGE_TYPE_PAXOS_PREPARE = 5
  };
  std::unordered_map<std::string, MessageType> msg_types_;
  MessageQueue<proto::Read> *read_queue_;
  MessageQueue<proto::Write> *write_queue_;
  MessageQueue<proto::Prepare> *prepare_queue_;
  MessageQueue<proto::Finalize> *finalize_queue_;
  MessageQueue<proto::Decide> *decide_queue_;
  MessageQueue<proto::PaxosPrepare> *paxos_prepare_queue_;

  MessageQueue<proto::ReadReply> read_reply_queue_;
  MessageQueue<proto::PrepareReply> prepare_reply_queue_;
  MessageQueue<proto::FinalizeReply> finalize_reply_queue_;
  MessageQueue<proto::DecideReply> decide_reply_queue_;
  MessageQueue<proto::PaxosPrepareReply> paxos_prepare_reply_queue_;

  tbb::concurrent_unordered_map<Version, TRecord, VersionHasher> trecord_;
  // assume that all concurrent inserts are to existing keys
  tbb::concurrent_unordered_map<std::string, VRecord> vstore_;

  tbb::concurrent_unordered_set<Version, VersionHasher> committed_;
  tbb::concurrent_unordered_set<Version, VersionHasher> aborted_;

  // not thread safe - should only every access from one thread
  TrueTime time_;
  std::atomic<uint64_t> gc_watermark_{};

  Stats stats;
};

}  // namespace mortystorev2

#endif /* MORTYV2_SERVER_H */
