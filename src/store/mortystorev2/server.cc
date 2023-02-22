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
#include "store/mortystorev2/server.h"

#include <google/protobuf/message.h>
#include <google/protobuf/stubs/port.h>
#include <pthread.h>
#include <sched.h>

#include <ctime>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common.h"
#include "store/common/timestamp.h"

namespace mortystorev2 {

Server::Server(const transport::Configuration &config, int group_idx, int idx,
               std::vector<Transport *> transport, uint64_t num_workers,
               bool pipeline_commit, bool reexecution_enabled,
               uint64_t gc_watermark_buffer_us,
               uint64_t num_cores_per_numa_node, uint64_t num_numa_nodes)
    : config_(config),
      group_idx_(group_idx),
      idx_(idx),
      transport_(std::move(std::move(transport))),
      num_workers_(num_workers),
      pipeline_commit_(pipeline_commit),
      reexecution_enabled_(reexecution_enabled),
      gc_watermark_buffer_us_(gc_watermark_buffer_us),
      round_robin_thread_dispatch_(0UL) {
  transport_[0]->Register(this, config_, group_idx_, idx_);

  committed_.insert(
      Version(0, 0));  // manually add initialization txn as committed

  read_queue_ = new MessageQueue<proto::Read>();
  write_queue_ = new MessageQueue<proto::Write>();
  prepare_queue_ = new MessageQueue<proto::Prepare>();
  finalize_queue_ = new MessageQueue<proto::Finalize>();
  decide_queue_ = new MessageQueue<proto::Decide>();
  paxos_prepare_queue_ = new MessageQueue<proto::PaxosPrepare>();
  /*msg_handlers_.insert(std::make_pair(
        read_queue_->GetMessageType(),
        std::make_pair(read_queue_, &Server::HandleRead)));
  msg_handlers_.insert(std::make_pair(
        std::move(write_queue_->GetMessageType()),
        std::make_pair(write_queue_, &Server::HandleWrite)));
  msg_handlers_.insert(std::make_pair(
        std::move(prepare_queue_->GetMessageType()),
        std::make_pair(prepare_queue_, &Server::HandlePrepare)));
  msg_handlers_.insert(std::make_pair(
        std::move(finalize_queue_->GetMessageType()),
        std::make_pair(finalize_queue_, &Server::HandleFinalize)));
  msg_handlers_.insert(std::make_pair(
        std::move(decide_queue_->GetMessageType()),
        std::make_pair(decide_queue_, &Server::HandleDecide)));
  msg_handlers_.insert(std::make_pair(
        std::move(paxos_prepare_queue_->GetMessageType()),
        std::make_pair(paxos_prepare_queue_, &Server::HandlePaxosPrepare)));*/
  msg_types_.insert(
      std::make_pair(read_queue_->GetMessageType(), MESSAGE_TYPE_READ));
  msg_types_.insert(
      std::make_pair(write_queue_->GetMessageType(), MESSAGE_TYPE_WRITE));
  msg_types_.insert(
      std::make_pair(prepare_queue_->GetMessageType(), MESSAGE_TYPE_PREPARE));
  msg_types_.insert(
      std::make_pair(finalize_queue_->GetMessageType(), MESSAGE_TYPE_FINALIZE));
  msg_types_.insert(
      std::make_pair(decide_queue_->GetMessageType(), MESSAGE_TYPE_DECIDE));
  msg_types_.insert(std::make_pair(paxos_prepare_queue_->GetMessageType(),
                                   MESSAGE_TYPE_PAXOS_PREPARE));

  // thread_pool_.Start(num_workers_, 1);
  for (size_t i = 1; i <= num_workers_; ++i) {
    std::thread *worker_thread = new std::thread([this, worker_id = i]() {
      transport_[worker_id]->Register(this, config_, group_idx_, idx_,
                                      worker_id);
      transport_[worker_id]->Run();
    });

    uint64_t numa_node = i % num_numa_nodes;
    uint64_t core = num_cores_per_numa_node * numa_node + (i / num_numa_nodes);
    // Warning("NUMA %lu %lu %lu %lu %lu", numa_node, i, num_numa_nodes, core,
    // num_cores_per_numa_node);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    int rc = pthread_setaffinity_np(worker_thread->native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      Panic("Error calling pthread_setaffinity_np: %d", rc);
    }

    worker_threads_.push_back(worker_thread);
  }

  transport_[0]->Timer(1000, std::bind(&Server::UpdateWatermark, this));
}

Server::~Server() {
  /*for (size_t i = 1; i < num_workers_; ++i) {
    transport_[i]->StopLoop();
    worker_threads_[i - 1]->join();
    delete worker_threads_[i - 1];
  }

  //thread_pool_.Stop();
  delete read_queue_;
  delete write_queue_;
  delete prepare_queue_;
  delete finalize_queue_;
  delete decide_queue_;
  delete paxos_prepare_queue_;
  for (uint64_t i = 0; i <= 10; ++i) {
    std::string key(reinterpret_cast<char *>(&i), sizeof(i));
    if (vstore_.find(key) != vstore_.end()) {
      Notice("Dumping prepare time for key %lu.", i);
      std::unique_lock<std::mutex> lock(vstore_[key].mtx);
      Latency_Dump(vstore_[key].prepared_time);
    }
  }*/
  char wkeyC[5];
  wkeyC[0] = static_cast<char>(0);
  *reinterpret_cast<uint32_t *>(wkeyC + 1) = 1;
  std::string warehouseKey(wkeyC, sizeof(wkeyC));
  if (vstore_.find(warehouseKey) != vstore_.end()) {
    Notice("Dumping prepare time for warehouse 1.");
    std::unique_lock<std::mutex> lock(vstore_[warehouseKey].mtx);
    if (vstore_[warehouseKey].prepared_time != nullptr) {
      Latency_Dump(vstore_[warehouseKey].prepared_time);
    }
    Latency_Dump(vstore_[warehouseKey].conflict_window);
  }
  char dkeyC[9];
  dkeyC[0] = static_cast<char>(1);
  *reinterpret_cast<uint32_t *>(dkeyC + 1) = 1;
  *reinterpret_cast<uint32_t *>(dkeyC + 5) = 1;
  std::string districtKey(dkeyC, sizeof(dkeyC));
  if (vstore_.find(districtKey) != vstore_.end()) {
    Notice("Dumping prepare time for warehouse 1 district 1.");
    std::unique_lock<std::mutex> lock(vstore_[districtKey].mtx);
    Latency_Dump(vstore_[districtKey].prepared_time);
  }
}

void Server::Load(std::string &&key, std::string &&val, Timestamp &&timestamp) {
  /*  if (num_workers_ > 0) {
      thread_pool_.Dispatch([this, key=std::forward<std::string>(key),
          val=std::forward<std::string>(val),
          timestamp=std::forward<Timestamp>(timestamp)](){
        vstore_[key].writes.insert(std::make_pair(Version(timestamp.getTimestamp(),
            timestamp.getID()),
    Write(std::move(Version(timestamp.getTimestamp(), timestamp.getID())),
    std::move(val))));
      });
    } else {*/
  vstore_[key].writes.emplace(
      std::piecewise_construct,
      std::make_tuple(timestamp.getID(), timestamp.getTimestamp()),
      std::make_tuple(timestamp.getTimestamp(), timestamp.getID(),
                      std::move(val), 0UL));
  vstore_[key].prepared_time = nullptr;
  // TODO(matthelb): create flag for measuring prepare time (recoverability
  // window)
  if (*reinterpret_cast<uint64_t *>(&key[0]) <= 10 ||
      (key.size() >= 3 && key[0] == 0 && key[1] == 1) ||
      (key.size() >= 3 && key[0] == 1 && key[1] == 1 && key[5] == 1)) {
    Notice("Creating prepare time tracker for key %s.",
           BytesToHex(key, key.size()).c_str());
    vstore_[key].conflict_window = new Latency_t();
    _Latency_Init(vstore_[key].conflict_window, "conflict_window");
  }
  //}
}

void Server::ReceiveMessage(const TransportAddress &remote, std::string *type,
                            std::string *data, void *meta_data) {
  /*if (num_workers_ > 0) {
    //uint64_t thread_id = round_robin_thread_dispatch_ % num_workers_;
    //round_robin_thread_dispatch_++;
    uint64_t thread_id = (*static_cast<uint64_t *>(meta_data)) % num_workers_;
    Debug("Receive message for thread %lu.", thread_id);
    thread_pool_.Dispatch(thread_id, std::bind(&Server::ReceiveMessageInternal,
          this, &remote, std::move(type), std::move(data), nullptr));
  } else {*/
  ReceiveMessageInternal(&remote, *type, *data, meta_data);
  //}
}

void Server::ReceiveMessageInternal(const TransportAddress *remote,
                                    std::string &type, std::string &data,
                                    void *meta_data) {
  auto msg_types_itr = msg_types_.find(type);
  if (msg_types_itr == msg_types_.end()) {
    Panic("Received unknown message type %s.", type.c_str());
    return;
  }

  // stats.Increment("messages_" + type);

  switch (msg_types_itr->second) {
    case MESSAGE_TYPE_READ: {
      proto::Read *read_msg = read_queue_->Pop();
      read_msg->ParseFromString(data);
      HandleRead(*remote, read_msg);
      break;
    }
    case MESSAGE_TYPE_WRITE: {
      proto::Write *write_msg = write_queue_->Pop();
      write_msg->ParseFromString(data);
      HandleWrite(*remote, write_msg);
      break;
    }
    case MESSAGE_TYPE_PREPARE: {
      proto::Prepare *prepare_msg = prepare_queue_->Pop();
      prepare_msg->ParseFromString(data);
      HandlePrepare(*remote, prepare_msg);
      break;
    }
    case MESSAGE_TYPE_FINALIZE: {
      proto::Finalize *finalize_msg = finalize_queue_->Pop();
      finalize_msg->ParseFromString(data);
      HandleFinalize(*remote, finalize_msg);
      break;
    }
    case MESSAGE_TYPE_DECIDE: {
      proto::Decide *decide_msg = decide_queue_->Pop();
      decide_msg->ParseFromString(data);
      HandleDecide(*remote, decide_msg);
      break;
    }
    case MESSAGE_TYPE_PAXOS_PREPARE: {
      proto::PaxosPrepare *paxos_prepare_msg = paxos_prepare_queue_->Pop();
      paxos_prepare_msg->ParseFromString(data);
      HandlePaxosPrepare(*remote, paxos_prepare_msg);
      break;
    }
    default:
      NOT_REACHABLE();
  }
}

void Server::HandleRead(const TransportAddress &remote,
                        google::protobuf::Message *m) {
  auto *msg = dynamic_cast<proto::Read *>(m);

  Debug("[%lu.%lu][%lu] Received Read for key %s.",
        msg->txn_version().client_id(), msg->txn_version().client_seq(),
        msg->req_id(), BytesToHex(msg->key(), 16).c_str());

  /*if (num_workers_ > 0) {
    // TODO: fix thread pool anonymous dispatch
    //thread_pool_.Dispatch(std::bind(&Server::HandleReadInternal, this,
  &remote,
    //      msg));
    uint64_t thread_id = msg->txn_version().client_seq() % num_workers_;
    //uint64_t thread_id = round_robin_thread_dispatch_ % num_workers_;
    //round_robin_thread_dispatch_++;
    thread_pool_.Dispatch(thread_id, std::bind(&Server::HandleReadInternal,
          this, &remote, msg));
  } else {*/
  HandleReadInternal(&remote, msg);
  //}
}

void Server::HandleWrite(const TransportAddress &remote,
                         google::protobuf::Message *m) {
  auto *msg = dynamic_cast<proto::Write *>(m);

  Debug("[%lu.%lu] Received Write for key %s.", msg->txn_version().client_id(),
        msg->txn_version().client_seq(), BytesToHex(msg->key(), 16).c_str());

  /*if (num_workers_ > 0) {
    // TODO: fix thread pool anonymous dispatch
    //thread_pool_.Dispatch(std::bind(&Server::HandleWriteInternal, this,
  &remote,
    //      msg));
    uint64_t thread_id = msg->txn_version().client_seq() % num_workers_;
    //uint64_t thread_id = round_robin_thread_dispatch_ % num_workers_;
    //round_robin_thread_dispatch_++;
    thread_pool_.Dispatch(thread_id, std::bind(&Server::HandleWriteInternal,
          this, &remote, msg));
  } else {*/
  HandleWriteInternal(&remote, msg);
  //}
}

void Server::HandlePrepare(const TransportAddress &remote,
                           google::protobuf::Message *m) {
  auto *msg = dynamic_cast<proto::Prepare *>(m);

  Debug("[%lu.%lu] Received Prepare.", msg->txn().version().client_id(),
        msg->txn().version().client_seq());

  /*if (num_workers_ > 0) {
    uint64_t thread_id = msg->txn().version().client_seq() % num_workers_;
    thread_pool_.Dispatch(thread_id, std::bind(&Server::HandlePrepareInternal,
          this, &remote, msg));
  } else {*/
  HandlePrepareInternal(&remote, msg);
  //}
}

void Server::HandleFinalize(const TransportAddress &remote,
                            google::protobuf::Message *m) {
  auto *msg = dynamic_cast<proto::Finalize *>(m);

  Debug("[%lu.%lu] Received Finalize.", msg->txn_version().client_id(),
        msg->txn_version().client_seq());

  /*if (num_workers_ > 0) {
    uint64_t thread_id = msg->txn().version().client_seq() % num_workers_;
    thread_pool_.Dispatch(thread_id, std::bind(&Server::HandleFinalizeInternal,
          this, &remote, msg));
  } else {*/
  HandleFinalizeInternal(&remote, msg);
  //}
}

void Server::HandleDecide(const TransportAddress &remote,
                          google::protobuf::Message *m) {
  auto *msg = dynamic_cast<proto::Decide *>(m);

  Debug("[%lu.%lu] Received Decision.", msg->txn_version().client_id(),
        msg->txn_version().client_seq());

  /*if (num_workers_ > 0) {
    uint64_t thread_id = msg->txn().version().client_seq() % num_workers_;
    thread_pool_.Dispatch(thread_id, std::bind(&Server::HandleDecideInternal,
          this, &remote, msg));
  } else {*/
  HandleDecideInternal(&remote, msg);
  //}
}

void Server::HandlePaxosPrepare(const TransportAddress &remote,
                                google::protobuf::Message *m) {
  auto *msg = dynamic_cast<proto::PaxosPrepare *>(m);

  Debug("[%lu.%lu] Received PaxosPrepare.", msg->txn().version().client_id(),
        msg->txn().version().client_seq());

  /*if (num_workers_ > 0) {
    uint64_t thread_id = msg->txn().version().client_seq() % num_workers_;
    thread_pool_.Dispatch(thread_id, std::bind(
          &Server::HandlePaxosPrepareInternal, this, &remote, msg));
  } else {*/
  HandlePaxosPrepareInternal(&remote, msg);
  //}
}

void Server::HandleReadInternal(const TransportAddress *remote,
                                proto::Read *msg) {
  // enforce GC requirement before prepares
  if (msg->txn_version().client_seq() <= gc_watermark_) {
    Debug("[%lu.%lu] Version is below GC watermark %lu.",
          msg->txn_version().client_id(), msg->txn_version().client_seq(),
          gc_watermark_.load());
    proto::ReadReply *read_reply = read_reply_queue_.Pop();
    read_reply->set_req_id(msg->req_id());
    read_reply->set_version_below_watermark(true);

    uint64_t thread_id = msg->txn_version().client_seq() % (num_workers_ + 1);
    transport_[thread_id]->SendMessage(this, *remote, *read_reply);

    read_reply_queue_.Push(read_reply);
    read_queue_->Push(msg);
    return;
  }

  VRecord &vrecord = GetOrCreateVRecord(msg->key());

  {
    std::unique_lock<std::mutex> lock(vrecord.mtx);

    auto reads_itr = vrecord.reads.find(msg->txn_version());
    if (reads_itr != vrecord.reads.end()) {
      vrecord.reads.erase(reads_itr);
    }
    auto reads_insert_itr = vrecord.reads.emplace(
        std::piecewise_construct, std::forward_as_tuple(msg->txn_version()),
        std::forward_as_tuple(Version(msg->txn_version()), remote->clone(),
                              msg->req_id()));
    Read &read = reads_insert_itr.first->second;

    const Write *write = GetReadValue(vrecord, read.version);

    SendReadReply(msg->key(), read, write, false);
  }

  // N.B. for now, we can AddTrackedRead on this same thread because we are
  //   handling all read message for the same transaction on the transaction's
  //   assigned thread. if we ever change to handling reads on different threads
  //   (e.g., round robin or random scheduling), we need to revert to
  //   dispatching the AddTrackedRead job to the correct thread to avoid
  //   concurrent access to the transaction's trecord
  /*if (num_workers_ > 0) {
    uint64_t thread_id = msg->txn_version().client_seq() % (num_workers_ + 1);
    //thread_pool_.Dispatch(thread_id, std::bind(&Server::AddTrackedRead, this,
    //      Version(msg->txn_version()), std::move(*msg->release_key())));
    transport_[thread_id]->Dispatch(std::bind(&Server::AddTrackedRead, this,
          Version(msg->txn_version()), std::move(*msg->release_key())), 1);
  } else {*/
  AddTrackedRead(Version(msg->txn_version()), std::move(*msg->release_key()));
  //}

  read_queue_->Push(msg);
}

void Server::HandleWriteInternal(const TransportAddress *remote,
                                 proto::Write *msg) {
  VRecord &vrecord = GetOrCreateVRecord(msg->key());

  {
    std::unique_lock<std::mutex> lock(vrecord.mtx);

    auto writes_itr = vrecord.writes.find(msg->txn_version());
    if (writes_itr != vrecord.writes.end()) {
      vrecord.writes.erase(writes_itr);
    }
    auto insert_itr =
        vrecord.writes.emplace(std::piecewise_construct,
                               std::make_tuple(msg->txn_version().client_id(),
                                               msg->txn_version().client_seq()),
                               std::make_tuple(msg));
    Write &write = insert_itr.first->second;

    Debug("[%lu.%lu] Inserting write for key %s of val %s.",
          write.version.GetClientId(), write.version.GetClientSeq(),
          BytesToHex(msg->key(), 16).c_str(),
          BytesToHex(write.val, 16).c_str());

    UpdateReadsWithWrite(msg->key(), vrecord, write);
  }

  // N.B. see note for HandleReadInternal
  /*if (num_workers_ > 0) {
    uint64_t thread_id = msg->txn_version().client_seq() % (num_workers_ + 1);
    //thread_pool_.Dispatch(thread_id, std::bind(&Server::AddTrackedWrite, this,
    //      Version(msg->txn_version()), std::move(*msg->release_key())));
    transport_[thread_id]->Dispatch(std::bind(&Server::AddTrackedWrite, this,
          Version(msg->txn_version()), std::move(*msg->release_key())), 1);
  } else {*/
  AddTrackedWrite(Version(msg->txn_version()), std::move(*msg->release_key()));
  //}

  write_queue_->Push(msg);
}

void Server::HandlePrepareInternal(const TransportAddress *remote,
                                   proto::Prepare *msg) {
  TRecord *trecord;
  ERecord &erecord =
      GetOrCreateERecord(msg->txn().version(), msg->txn().exec_id(), trecord);

  erecord.txn = msg->release_txn();

  TryPrepare(erecord, std::make_pair(remote->clone(), msg->req_id()));

  prepare_queue_->Push(msg);
}

void Server::HandleFinalizeInternal(const TransportAddress *remote,
                                    proto::Finalize *msg) {
  Debug("[%lu.%lu] Finalize exec %lu with decision %d in view %lu.",
        msg->txn_version().client_id(), msg->txn_version().client_seq(),
        msg->exec_id(), msg->decision(), msg->view());

  ERecord *erecord;
  TRecord *trecord;

  bool found_record =
      GetERecord(msg->txn_version(), msg->exec_id(), erecord, trecord);
  if (!found_record) {
    // TODO(matthelb): should only happen if TCP connection was preemptively
    // closed by
    //   server and client is in the middle of committing a transaction
    Warning("[%lu.%lu][%lu] Have no ERecord for Finalize.",
            msg->txn_version().client_id(), msg->txn_version().client_seq(),
            msg->exec_id());
    finalize_queue_->Push(msg);
    return;
  }

  TryFinalize(erecord, trecord, std::make_pair(remote->clone(), msg->req_id()),
              msg->view(), msg->decision());

  finalize_queue_->Push(msg);
}

void Server::HandleDecideInternal(const TransportAddress *remote,
                                  proto::Decide *msg) {
  Debug("[%lu.%lu] Decision for exec %lu is %d.",
        msg->txn_version().client_id(), msg->txn_version().client_seq(),
        msg->exec_id(), msg->decision());

  ERecord *erecord;
  TRecord *trecord;

  bool found_record =
      GetERecord(msg->txn_version(), msg->exec_id(), erecord, trecord);
  if (!found_record) {
    // TODO(matthelb): should only happen if TCP connection was preemptively
    // closed by
    //   server and client is in the middle of committing a transaction
    Warning("[%lu.%lu][%lu] Have no ERecord for Finalize.",
            msg->txn_version().client_id(), msg->txn_version().client_seq(),
            msg->exec_id());
    decide_queue_->Push(msg);
    return;
  }

  /*std::stringstream ss;
  ss << std::this_thread::get_id();
  stats.IncrementList("worker_" + ss.str() + "_client_ids",
  msg->txn_version().client_id() % (num_workers_ + 1));*/

  TryDecide(erecord, trecord, std::make_pair(remote->clone(), msg->req_id()),
            msg->decision());

  decide_queue_->Push(msg);
}

void Server::HandlePaxosPrepareInternal(const TransportAddress *remote,
                                        proto::PaxosPrepare *msg) {
  TRecord *trecord;
  // TODO(matthelb): only create ERecord if TRecord decision is DECISION_NONE
  ERecord &erecord =
      GetOrCreateERecord(msg->txn().version(), msg->txn().exec_id(), trecord);

  if (msg->view() > erecord.drecord.view) {
    erecord.drecord.view = msg->view();
  }

  proto::PaxosPrepareReply *paxos_prepare_reply =
      paxos_prepare_reply_queue_.Pop();
  paxos_prepare_reply->set_req_id(msg->req_id());
  paxos_prepare_reply->set_decision(erecord.drecord.decision);
  paxos_prepare_reply->set_vote(erecord.vote);
  paxos_prepare_reply->set_view(erecord.drecord.view);
  paxos_prepare_reply->set_finalize_view(erecord.drecord.finalize_view);
  paxos_prepare_reply->set_finalize_decision(erecord.drecord.finalize_decision);
  uint64_t thread_id = msg->txn().version().client_seq() % (num_workers_ + 1);
  transport_[thread_id]->SendMessage(this, *remote, *paxos_prepare_reply);

  paxos_prepare_reply_queue_.Push(paxos_prepare_reply);
  paxos_prepare_queue_->Push(msg);
}

const Server::Write *Server::GetReadValue(VRecord &vrecord,
                                          const Version &version) {
  const Write *write = nullptr;
  auto writes_itr = vrecord.writes.lower_bound(version);
  if (writes_itr == vrecord.writes.end()) {
    // read version is larger than all write versions, so read should observe
    //   write with largest version
    auto writes_ritr = vrecord.writes.rbegin();
    if (writes_ritr != vrecord.writes.rend()) {
      write = &writes_ritr->second;
    }
  } else {
    // read version is at least as large as a write version. search backward to
    //   find the write with the next smallest version relative to the read.
    while (true) {
      if (writes_itr->second.version < version) {
        write = &writes_itr->second;
        break;
      }

      if (writes_itr == vrecord.writes.begin()) {
        break;
      }

      writes_itr--;
    }
  }

  return write;
}

void Server::SendReadReply(const std::string &key, Read &read,
                           const Write *write, bool need_dispatch) {
  Debug("[%lu.%lu][%lu] Send read reply with write %p.",
        read.version.GetClientId(), read.version.GetClientSeq(), read.req_id,
        write);

  proto::ReadReply *read_reply = read_reply_queue_.Pop();
  read_reply->set_req_id(read.req_id);

  if (write == nullptr) {
    Warning("[%lu.%lu] No write for read %s to observe.",
            read.version.GetClientId(), read.version.GetClientSeq(),
            BytesToHex(key, 16).c_str());
    read_reply->set_key_exists(false);
    stats.Increment("reads_failed");
  } else {
    Debug("[%lu.%lu][%lu] Send read reply for write val %s from txn %lu.%lu.",
          read.version.GetClientId(), read.version.GetClientSeq(), read.req_id,
          BytesToHex(write->val, 16).c_str(), write->version.GetClientId(),
          write->version.GetClientSeq());

    read.write_version = write->version;
    read.write_val = write->val;

    read_reply->set_key_exists(true);
    read_reply->set_committed(committed_.find(write->version) !=
                              committed_.end());
    read.write_version.Serialize(read_reply->mutable_version());
    *read_reply->mutable_val() = read.write_val;
    read_reply->set_val_id(write->val_id);
  }

  uint64_t thread_id = read.version.GetClientSeq() % (num_workers_ + 1);
  Debug("[%lu.%lu][%lu] Replying on core %lu.", read.version.GetClientId(),
        read.version.GetClientSeq(), read.req_id, thread_id);

  if (num_workers_ > 0 && need_dispatch) {
    transport_[thread_id]->Dispatch(
        [this, coord = read.coordinator->clone(), thread_id, read_reply]() {
          transport_[thread_id]->SendMessage(this, *coord, *read_reply);
          delete coord;
          read_reply_queue_.Push(read_reply);
        },
        0);
  } else {
    transport_[thread_id]->SendMessage(this, *read.coordinator, *read_reply);
    read_reply_queue_.Push(read_reply);
  }
}

// must hold lock on vrecord
void Server::UpdateReadsWithWrite(const std::string &key, VRecord &vrecord,
                                  const Write &write) {
  if (!reexecution_enabled_) {
    return;
  }

  auto next_writes_itr = vrecord.writes.upper_bound(write.version);
  auto reads_itr = vrecord.reads.upper_bound(write.version);
  while (reads_itr != vrecord.reads.end()) {
    // check every read that has a version larger than this write
    if (next_writes_itr != vrecord.writes.end() &&
        next_writes_itr->second.version < reads_itr->second.version) {
      // stop checking reads that should observe the next write
      break;
    }

    if (!(reads_itr->second.write_version == write.version) ||
        reads_itr->second.write_val != write.val) {
      // send a new read reply if the read previously observed a different write
      SendReadReply(key, reads_itr->second, &write, true);
    }

    reads_itr++;
  }
}

void Server::SendPrepareReply(ERecord &erecord, coordinator_t coordinator) {
  proto::PrepareReply *prepare_reply = prepare_reply_queue_.Pop();

  Debug("[%lu.%lu] Send PrepareReply for exec %lu with vote %d.",
        erecord.txn->version().client_id(), erecord.txn->version().client_seq(),
        erecord.txn->exec_id(), erecord.vote);

  prepare_reply->set_replica_id(idx_);
  prepare_reply->set_vote(erecord.vote);
  prepare_reply->set_req_id(coordinator.second);
  uint64_t thread_id = erecord.txn->version().client_seq() % (num_workers_ + 1);
  transport_[thread_id]->SendMessage(this, *coordinator.first, *prepare_reply);

  if (!pipeline_commit_) {
    erecord.waiting_prepares.erase(coordinator);
  }

  prepare_reply_queue_.Push(prepare_reply);
}

void Server::SendDecideReply(ERecord &erecord, coordinator_t coordinator,
                             proto::Decision decision,
                             bool deps_changed_decision) {
  proto::DecideReply *decide_reply = decide_reply_queue_.Pop();

  Debug("[%lu.%lu] Send DecideReply for exec %lu with decision %d.",
        erecord.txn->version().client_id(), erecord.txn->version().client_seq(),
        erecord.txn->exec_id(), decision);

  decide_reply->set_replica_id(idx_);
  decide_reply->set_decision(decision);
  decide_reply->set_deps_changed_decision(deps_changed_decision);
  decide_reply->set_req_id(coordinator.second);
  uint64_t thread_id = erecord.txn->version().client_seq() % (num_workers_ + 1);
  transport_[thread_id]->SendMessage(this, *coordinator.first, *decide_reply);

  decide_reply_queue_.Push(decide_reply);
}

void Server::SendFinalizeReply(ERecord &erecord, coordinator_t coordinator) {
  proto::FinalizeReply *finalize_reply = finalize_reply_queue_.Pop();

  Debug("[%lu.%lu] Send FinalizeReply for exec %lu with view %lu.",
        erecord.txn->version().client_id(), erecord.txn->version().client_seq(),
        erecord.txn->exec_id(), erecord.drecord.view);

  finalize_reply->set_replica_id(idx_);
  finalize_reply->set_view(erecord.drecord.view);
  finalize_reply->set_decision(erecord.drecord.finalize_decision);
  finalize_reply->set_req_id(coordinator.second);
  uint64_t thread_id = erecord.txn->version().client_seq() % (num_workers_ + 1);
  transport_[thread_id]->SendMessage(this, *coordinator.first, *finalize_reply);

  finalize_reply_queue_.Push(finalize_reply);
}

void Server::TryPrepare(ExecVersion exec_version, coordinator_t coordinator) {
  TRecord *trecord;
  ERecord *erecord;
  bool found_record =
      GetERecord(exec_version.first, exec_version.second, erecord, trecord);

  if (!found_record) {
    Warning("[%lu.%lu] Trying to Prepare exec %lu but missing ERecord.",
            exec_version.first.GetClientId(), exec_version.first.GetClientSeq(),
            exec_version.second);
    return;
  }

  TryPrepare(*erecord, coordinator);
}

void Server::TryPrepare(ERecord &erecord, coordinator_t coordinator) {
  if (erecord.vote != proto::VOTE_NONE) {
    // duplicate prepare request. for now, let's ignore
    Warning("[%lu.%lu] Already processed Prepare for exec %lu.",
            erecord.txn->version().client_id(),
            erecord.txn->version().client_seq(), erecord.txn->exec_id());
    return;
  }

  if (erecord.drecord.finalize_decision != proto::DECISION_NONE) {
    Debug("[%lu.%lu] Already finalized exec %lu.",
          erecord.txn->version().client_id(),
          erecord.txn->version().client_seq(), erecord.txn->exec_id());
    return;
  }

  if (erecord.drecord.decision != proto::DECISION_NONE) {
    Debug("[%lu.%lu] Already decided exec %lu.",
          erecord.txn->version().client_id(),
          erecord.txn->version().client_seq(), erecord.txn->exec_id());
    return;
  }

  Debug("[%lu.%lu] Trying to prepare exec %lu.",
        erecord.txn->version().client_id(), erecord.txn->version().client_seq(),
        erecord.txn->exec_id());

  bool aborted_dep = false;
  if (pipeline_commit_) {
    for (const auto &dep : erecord.txn->read_set()) {
      if (dep.committed()) {
        continue;
      }
      if (aborted_.find(dep.version()) != aborted_.end()) {
        Debug("[%lu.%lu] Exec %lu has aborted dep %lu.%lu.",
              erecord.txn->version().client_id(),
              erecord.txn->version().client_seq(), erecord.txn->exec_id(),
              dep.version().client_id(), dep.version().client_seq());
        aborted_dep = true;
        break;
      }
    }
  } else {
    bool uncommitted_deps = CheckDependencies(erecord, aborted_dep);
    if (!aborted_dep && uncommitted_deps) {
      Debug("[%lu.%lu] Exec %lu waiting for deps to commit.",
            erecord.txn->version().client_id(),
            erecord.txn->version().client_seq(), erecord.txn->exec_id());

      if (erecord.waiting_prepares.find(coordinator) ==
          erecord.waiting_prepares.end()) {
        erecord.waiting_prepares.insert(coordinator);
      }
      return;
    }
  }

  if (aborted_dep) {
    stats.Increment("abort_dep_aborted");
    erecord.vote = proto::VOTE_ABANDON_FAST;
    SendPrepareReply(erecord, coordinator);
    return;
  }

  erecord.vote = Validate(erecord, coordinator);
  if (erecord.vote != proto::VOTE_COMMIT) {
    CleanPreparedTransaction(erecord.txn);
  }
  SendPrepareReply(erecord, coordinator);
}

bool Server::CheckDependencies(ERecord &erecord, bool &aborted_dep) {
  bool uncommitted_deps = false;
  for (const auto &dep : erecord.txn->read_set()) {
    if (dep.committed()) {
      continue;
    }
    if (aborted_.find(dep.version()) != aborted_.end()) {
      Debug("[%lu.%lu]   Exec %lu has aborted dep %lu.%lu.",
            erecord.txn->version().client_id(),
            erecord.txn->version().client_seq(), erecord.txn->exec_id(),
            dep.version().client_id(), dep.version().client_seq());

      aborted_dep = true;
      return false;
    }
    if (committed_.find(dep.version()) == committed_.end()) {
      Debug("[%lu.%lu]   Exec %lu has uncommitted dep %lu.%lu.",
            erecord.txn->version().client_id(),
            erecord.txn->version().client_seq(), erecord.txn->exec_id(),
            dep.version().client_id(), dep.version().client_seq());
      uncommitted_deps = true;
      if (!erecord.added_as_dependent) {
        if (num_workers_ > 0) {
          uint64_t thread_id = dep.version().client_seq() % (num_workers_ + 1);
          // thread_pool_.Dispatch(thread_id, std::bind(&Server::AddDependent,
          // this,
          //    dep.version(), std::make_pair(erecord.txn->version(),
          //      erecord.txn->exec_id())));
          transport_[thread_id]->Dispatch(
              std::bind(&Server::AddDependent, this, dep.version(),
                        std::make_pair(erecord.txn->version(),
                                       erecord.txn->exec_id())),
              1);
        } else {
          AddDependent(dep.version(), std::make_pair(erecord.txn->version(),
                                                     erecord.txn->exec_id()));
        }
      } else {
        break;
      }
    }
  }

  return uncommitted_deps;
}

void Server::AddDependent(Version version, ExecVersion dependent) {
  if (committed_.find(version) != committed_.end() ||
      aborted_.find(version) != aborted_.end()) {
    if (num_workers_ > 0) {
      uint64_t thread_id = dependent.first.GetClientSeq() % (num_workers_ + 1);
      // thread_pool_.Dispatch(thread_id, std::bind(&Server::CheckWaiting, this,
      //      dependent));
      transport_[thread_id]->Dispatch(
          std::bind(&Server::CheckWaiting, this, dependent), 1);
    } else {
      CheckWaiting(dependent);
    }
    return;
  }

  auto trecord_itr = trecord_.find(version);
  if (trecord_itr == trecord_.end()) {
    // assume that there are never concurrent threads attempting to insert for
    //   same version. this should be true because same thread will handle all
    //   prepares for a given transaction version.
    auto insert_itr = trecord_.insert(std::make_pair(version, TRecord()));
    trecord_itr = insert_itr.first;
  }

  auto prev_dep = trecord_itr->second.dependents.lower_bound(
      ExecVersion(dependent.first, 0));
  if (prev_dep != trecord_itr->second.dependents.end() &&
      prev_dep->first == dependent.first) {
    trecord_itr->second.dependents.erase(prev_dep);
  }
  trecord_itr->second.dependents.insert(dependent);

  Debug("[%lu.%lu] Added %lu.%lu exec %lu as dependent.", version.GetClientId(),
        version.GetClientSeq(), dependent.first.GetClientId(),
        dependent.first.GetClientSeq(), dependent.second);
}

void Server::AddTrackedRead(Version version, std::string key) {
  if (committed_.find(version) != committed_.end() ||
      aborted_.find(version) != aborted_.end()) {
    VRecord &vrecord = GetOrCreateVRecord(key);

    std::unique_lock<std::mutex> lock(vrecord.mtx);

    vrecord.reads.erase(version);
    return;
  }

  auto trecord_itr = trecord_.find(version);
  if (trecord_itr == trecord_.end()) {
    // assume that there are never concurrent threads attempting to insert for
    //   same version. this should be true because same thread will handle all
    //   prepares for a given transaction version.
    auto insert_itr = trecord_.insert(std::make_pair(version, TRecord()));
    trecord_itr = insert_itr.first;
  }

  trecord_itr->second.all_execs_reads.insert(std::move(key));
}

void Server::AddTrackedWrite(Version version, std::string key) {
  if (committed_.find(version) != committed_.end()) {
    // TODO(matthelb): how do we determine if this write is part of the
    // committed
    //    execution versus a stale execution? in the latter case, we need to
    //    make sure it is removed.
    return;
  }
  if (aborted_.find(version) != aborted_.end()) {
    VRecord &vrecord = GetOrCreateVRecord(key);

    std::unique_lock<std::mutex> lock(vrecord.mtx);

    Debug("[%lu.%lu] Removing write to %s from non-committed execution.",
          version.GetClientId(), version.GetClientSeq(),
          BytesToHex(key, 16).c_str());

    vrecord.writes.erase(version);
    return;
  }

  auto trecord_itr = trecord_.find(version);
  if (trecord_itr == trecord_.end()) {
    // assume that there are never concurrent threads attempting to insert for
    //   same version. this should be true because same thread will handle all
    //   prepares for a given transaction version.
    auto insert_itr = trecord_.insert(std::make_pair(version, TRecord()));
    trecord_itr = insert_itr.first;
  }

  Debug("[%lu.%lu] Tracking write for key %s.", version.GetClientId(),
        version.GetClientSeq(), BytesToHex(key, 16).c_str());

  trecord_itr->second.all_execs_writes.insert(std::move(key));
}

void Server::TryFinalize(ERecord *erecord, TRecord *trecord,
                         coordinator_t coordinator, uint64_t view,
                         proto::Decision decision) {
  if (pipeline_commit_ && decision == proto::DECISION_COMMIT) {
    bool aborted_dep = false;
    bool uncommitted_deps = CheckDependencies(*erecord, aborted_dep);
    if (aborted_dep) {
      // finalize decision turns into abort because of aborted dep
      ProcessFinalize(erecord, trecord, coordinator, view,
                      proto::DECISION_ABORT2);
    } else if (!uncommitted_deps) {
      ProcessFinalize(erecord, trecord, coordinator, view,
                      proto::DECISION_COMMIT);
    } else if (erecord->waiting_finalizes.find(coordinator) ==
               erecord->waiting_finalizes.end()) {
      erecord->waiting_finalizes.insert(
          std::make_pair(coordinator, std::make_tuple(view, decision)));
    }
  } else {
    ProcessFinalize(erecord, trecord, coordinator, view, decision);
  }
}

void Server::TryFinalize(ExecVersion exec_version, coordinator_t coordinator,
                         uint64_t view, proto::Decision decision) {
  TRecord *trecord;
  ERecord *erecord;
  bool found_record =
      GetERecord(exec_version.first, exec_version.second, erecord, trecord);

  if (!found_record) {
    Warning("[%lu.%lu] Trying to Finalize exec %lu but missing ERecord.",
            exec_version.first.GetClientId(), exec_version.first.GetClientSeq(),
            exec_version.second);
    return;
  }

  TryFinalize(erecord, trecord, coordinator, view, decision);
}

void Server::ProcessFinalize(ERecord *erecord, TRecord *trecord,
                             coordinator_t coordinator, uint64_t view,
                             proto::Decision decision) {
  if (pipeline_commit_) {
    erecord->waiting_finalizes.clear();
  }

  if (erecord->drecord.view == view) {
    Debug("[%lu.%lu] Finalize for exec %lu in view %lu succeeded.",
          erecord->txn->version().client_id(),
          erecord->txn->version().client_seq(), erecord->txn->exec_id(), view);
    if (erecord->drecord.view == 0UL && decision == proto::DECISION_ABANDON) {
      CleanPreparedTransaction(erecord->txn);
    }
    erecord->drecord.finalize_view = view;
    erecord->drecord.finalize_decision = decision;
  } else {
    Debug(
        "[%lu.%lu] Finalize for exec %lu in view %lu failed because replica in "
        "view %lu.",
        erecord->txn->version().client_id(),
        erecord->txn->version().client_seq(), erecord->txn->exec_id(), view,
        erecord->drecord.view);
  }

  SendFinalizeReply(*erecord, coordinator);

  delete coordinator.first;
}

void Server::TryDecide(ERecord *erecord, TRecord *trecord,
                       coordinator_t coordinator, proto::Decision decision) {
  if (pipeline_commit_ && decision == proto::DECISION_COMMIT) {
    bool aborted_dep = false;
    bool uncommitted_deps = CheckDependencies(*erecord, aborted_dep);
    if (aborted_dep) {
      // finalize decision turns into abort because of aborted dep
      Debug("[%lu.%lu] Exec %lu has aborted dep.",
            erecord->txn->version().client_id(),
            erecord->txn->version().client_seq(), erecord->txn->exec_id());
      ProcessDecide(erecord, trecord, coordinator, proto::DECISION_ABORT2,
                    true);
    } else if (!uncommitted_deps) {
      Debug("[%lu.%lu] All deps for exec %lu are committed.",
            erecord->txn->version().client_id(),
            erecord->txn->version().client_seq(), erecord->txn->exec_id());
      ProcessDecide(erecord, trecord, coordinator, decision, false);
    } else if (erecord->waiting_decides.find(coordinator) ==
               erecord->waiting_decides.end()) {
      Debug("[%lu.%lu] Exec %lu has uncommitted dep.",
            erecord->txn->version().client_id(),
            erecord->txn->version().client_seq(), erecord->txn->exec_id());
      erecord->waiting_decides.insert(std::make_pair(coordinator, decision));
    }
  } else {
    ProcessDecide(erecord, trecord, coordinator, decision, false);
  }
}

void Server::TryDecide(ExecVersion exec_version, coordinator_t coordinator,
                       proto::Decision decision) {
  TRecord *trecord;
  ERecord *erecord;
  bool found_record =
      GetERecord(exec_version.first, exec_version.second, erecord, trecord);

  if (!found_record) {
    Warning("[%lu.%lu] Trying to Finalize exec %lu but missing ERecord.",
            exec_version.first.GetClientId(), exec_version.first.GetClientSeq(),
            exec_version.second);
    return;
  }

  TryDecide(erecord, trecord, coordinator, decision);
}

void Server::ProcessDecide(ERecord *erecord, TRecord *trecord,
                           coordinator_t coordinator, proto::Decision decision,
                           bool deps_changed_decision) {
  if (pipeline_commit_) {
    erecord->waiting_decides.clear();
  }

  if (deps_changed_decision) {
    UW_ASSERT(decision == proto::DECISION_ABORT2);
    SendDecideReply(*erecord, coordinator, proto::DECISION_ABORT2, true);
    return;
  }

  if (decision == proto::DECISION_COMMIT) {
    erecord->drecord.decision = decision;
    committed_.insert(erecord->txn->version());
    for (auto &read : erecord->txn->read_set()) {
      VRecord &vrecord = GetOrCreateVRecord(read.key());

      trecord->all_execs_reads.erase(read.key());

      std::unique_lock<std::mutex> lock(vrecord.mtx);
      vrecord.reads.erase(erecord->txn->version());
      vrecord.prepared_reads.erase(
          std::make_pair(erecord->txn->version(), erecord->txn->exec_id()));
      vrecord.committed_reads.insert(std::make_pair(
          std::make_pair(erecord->txn->version(), erecord->txn->exec_id()),
          read.version()));

      // garbage collection logic
      Version gc_version(gc_watermark_, 0);
      ExecVersion gc_exec_version(gc_version, 0);
      auto gc_committed_reads_itr =
          vrecord.committed_reads.lower_bound(gc_exec_version);
      UW_ASSERT(gc_committed_reads_itr != vrecord.committed_reads.end());
      if (!(gc_committed_reads_itr->first < gc_exec_version) &&
          gc_committed_reads_itr != vrecord.committed_reads.begin()) {
        gc_committed_reads_itr--;
      }
      vrecord.committed_reads.erase(vrecord.committed_reads.begin(),
                                    gc_committed_reads_itr);
    }

    for (auto &write : *erecord->txn->mutable_write_set()) {
      VRecord &vrecord = GetOrCreateVRecord(write.key());

      trecord->all_execs_writes.erase(write.key());

      std::unique_lock<std::mutex> lock(vrecord.mtx);
      if (vrecord.prepared_writes.erase(std::make_pair(
              erecord->txn->version(), erecord->txn->exec_id())) == 1 &&
          vrecord.prepared_time != nullptr) {
        Latency_End(vrecord.prepared_time);
      }
      vrecord.committed_writes.insert(
          std::make_pair(erecord->txn->version(), erecord->txn->exec_id()));
      auto writes_itr = vrecord.writes.find(erecord->txn->version());
      if (writes_itr != vrecord.writes.end()) {
        vrecord.writes.erase(writes_itr);
      }
      vrecord.writes.insert(
          std::make_pair(erecord->txn->version(), Write(erecord->txn, write)));

      // garbage collection logic
      Version gc_version(gc_watermark_, 0);
      ExecVersion gc_exec_version(gc_version, 0);

      auto gc_writes_itr = vrecord.writes.lower_bound(gc_version);
      UW_ASSERT(gc_writes_itr != vrecord.writes.end());
      if (!(gc_writes_itr->first < gc_version) &&
          gc_writes_itr != vrecord.writes.begin()) {
        gc_writes_itr--;
      }
      vrecord.writes.erase(vrecord.writes.begin(), gc_writes_itr);

      auto gc_committed_writes_itr =
          vrecord.committed_writes.lower_bound(gc_exec_version);
      UW_ASSERT(gc_committed_writes_itr != vrecord.committed_writes.end());
      if (!(*gc_committed_writes_itr < gc_exec_version) &&
          gc_committed_writes_itr != vrecord.committed_writes.begin()) {
        gc_committed_writes_itr--;
      }
      vrecord.committed_writes.erase(vrecord.committed_writes.begin(),
                                     gc_committed_writes_itr);
    }
  } else if (decision == proto::DECISION_ABANDON) {
    erecord->drecord.decision = decision;
    CleanPreparedTransaction(erecord->txn);
  } else if (decision == proto::DECISION_ABORT2) {
    aborted_.insert(erecord->txn->version());
    CleanPreparedTransaction(erecord->txn);
  }

  if (decision == proto::DECISION_COMMIT ||
      decision == proto::DECISION_ABORT2) {
    for (const auto &read_key : trecord->all_execs_reads) {
      VRecord &vrecord = GetOrCreateVRecord(read_key);

      std::unique_lock<std::mutex> lock(vrecord.mtx);
      // erase all uncommitted reads from executions with same txn version
      vrecord.reads.erase(erecord->txn->version());
      // erase all prepared_reads from executions with same txn version
      auto prepared_reads_itr = vrecord.prepared_reads.lower_bound(
          std::make_pair(erecord->txn->version(), 0));
      while (prepared_reads_itr != vrecord.prepared_reads.end()) {
        if (prepared_reads_itr->first.first == erecord->txn->version()) {
          prepared_reads_itr = vrecord.prepared_reads.erase(prepared_reads_itr);
        } else {
          break;
        }
      }
    }
    trecord->all_execs_reads.clear();

    for (const auto &write_key : trecord->all_execs_writes) {
      VRecord &vrecord = GetOrCreateVRecord(write_key);

      std::unique_lock<std::mutex> lock(vrecord.mtx);

      Debug("[%lu.%lu] Removing write to %s from non-committed execution.",
            erecord->txn->version().client_id(),
            erecord->txn->version().client_seq(),
            BytesToHex(write_key, 16).c_str());

      // erase all uncommitted writes from executions with same txn version
      vrecord.writes.erase(erecord->txn->version());
      // erase all prepared_writes from executions with same txn version
      auto prepared_writes_itr = vrecord.prepared_writes.lower_bound(
          std::make_pair(erecord->txn->version(), 0));
      while (prepared_writes_itr != vrecord.prepared_writes.end()) {
        if (prepared_writes_itr->first == erecord->txn->version()) {
          prepared_writes_itr =
              vrecord.prepared_writes.erase(prepared_writes_itr);
        } else {
          break;
        }
      }
      if (!vrecord.writes.empty()) {
        auto previous_write_itr =
            vrecord.writes.lower_bound(erecord->txn->version());
        if (previous_write_itr != vrecord.writes.begin()) {
          previous_write_itr--;
          UpdateReadsWithWrite(write_key, vrecord, previous_write_itr->second);
        }
      }
    }
    trecord->all_execs_writes.clear();

    for (const auto &exec_version : trecord->dependents) {
      if (num_workers_ > 0) {
        uint64_t thread_id =
            exec_version.first.GetClientSeq() % (num_workers_ + 1);
        // thread_pool_.Dispatch(thread_id, std::bind(&Server::CheckWaiting,
        // this,
        //      ExecVersion(exec_version)));
        transport_[thread_id]->Dispatch(
            std::bind(&Server::CheckWaiting, this, ExecVersion(exec_version)),
            1);
      } else {
        CheckWaiting(exec_version);
      }
    }
    trecord->dependents.clear();
    trecord->erecord.clear();
  }

  if (pipeline_commit_) {
    SendDecideReply(*erecord, coordinator, erecord->drecord.decision, false);
  }

  delete coordinator.first;
}

void Server::CheckWaiting(ExecVersion exec_version) {
  TRecord *trecord = nullptr;
  ERecord *erecord = nullptr;
  bool found_record =
      GetERecord(exec_version.first, exec_version.second, erecord, trecord);

  if (!found_record) {
    if (trecord != nullptr) {
      Warning(
          "[%lu.%lu] Trying to check waiting for exec %lu but missing ERecord.",
          exec_version.first.GetClientId(), exec_version.first.GetClientSeq(),
          exec_version.second);
    }
    return;
  }

  Debug("[%lu.%lu] Check waiting for exec %lu.",
        exec_version.first.GetClientId(), exec_version.first.GetClientSeq(),
        exec_version.second);

  if (pipeline_commit_) {
    auto waiting_finalizes = erecord->waiting_finalizes;
    for (const auto &coordinator_finalize : waiting_finalizes) {
      TryFinalize(erecord, trecord, coordinator_finalize.first,
                  std::get<0>(coordinator_finalize.second),
                  std::get<1>(coordinator_finalize.second));
    }
    auto waiting_decides = erecord->waiting_decides;
    for (const auto &coordinator_decide : waiting_decides) {
      TryDecide(erecord, trecord, coordinator_decide.first,
                coordinator_decide.second);
    }
  } else {
    auto waiting_prepares = erecord->waiting_prepares;
    for (const auto &coordinator : waiting_prepares) {
      TryPrepare(*erecord, coordinator);
    }
  }
}

proto::Vote Server::Validate(ERecord &erecord, coordinator_t coordinator) {
  proto::Transaction *txn = erecord.txn;

  Debug("[%lu.%lu] Validate.", txn->version().client_id(),
        txn->version().client_seq());

  if (txn->version().client_seq() < gc_watermark_) {
    stats.Increment("abort_gc_watermark");
    return proto::VOTE_ABANDON_SLOW;
  }

  for (const auto &read : txn->read_set()) {
    Debug(
        "[%lu.%lu] Read set contains key %s with version %lu.%lu and val %lu.",
        txn->version().client_id(), txn->version().client_seq(),
        BytesToHex(read.key(), 16).c_str(), read.version().client_id(),
        read.version().client_seq(), read.val_id());

    VRecord &vrecord = GetOrCreateVRecord(read.key());

    std::unique_lock<std::mutex> lock(vrecord.mtx);

    if (reexecution_enabled_) {
      // dirty read check is required for recoverability: this replica will not
      // process the prepare until it has received a commit for every
      // dependency. when the replicas processes these commits, it will store
      // the final write values.
      //
      // so this transaction's reads must exactly match the write values stored
      // at this replica in order for the replica to agree. otherwise (the only
      // other possibility) is that this transaction's reads are outdated.
      auto writes_itr = vrecord.writes.find(read.version());
      if (writes_itr == vrecord.writes.end() ||
          !(writes_itr->second.version == Version(read.version())) ||
          writes_itr->second.val_id != read.val_id()) {
        Debug("[%lu.%lu] Abort due to dirty read %d %d %d %lu %lu.",
              txn->version().client_id(), txn->version().client_seq(),
              writes_itr == vrecord.writes.end(),
              !(writes_itr->second.version == Version(read.version())),
              writes_itr->second.val_id != read.val_id(),
              writes_itr->second.val_id, read.val_id());
        stats.Increment("abort_dirty_read");
        return proto::VOTE_ABANDON_FAST;
      }

      // check if this transaction missed an uncommitted write
      const Write *write = GetReadValue(vrecord, txn->version());
      if (write != nullptr && Version(read.version()) < write->version) {
        Debug("[%lu.%lu] Abort due to uncommitted wr conflict.",
              txn->version().client_id(), txn->version().client_seq());
        stats.Increment("abort_uncommitted_wr_conflict");

        // immediately send read reply to force re-execution
        Read r(txn->version(), coordinator.first->clone(), read.req_id());
        SendReadReply(read.key(), r, write, false);

        // abort fast instead of slow because the reading transaction should be
        //   encouraged to re-execute. aborting slow allows reading transaction
        //   to commit if enough other replicas vote commit
        return proto::VOTE_ABANDON_FAST;
      }
    }

    // check if this transaction missed a prepared write
    //   use exec_id 0 because we want to include any execution of the
    //   transaction whose version we read (read.version())
    //
    //   its ok to use exec_id 0 in < comparison because we only care about the
    //   higher order value (txn->version()). assuming we can't read from
    //   ourselves, prepared_writes version will not be equal to txn->version()
    //
    //   add 1 to lowest order member of Version to ensure that we are searching
    //   for versions strictly greater than the read version
    auto prepared_writes_itr = vrecord.prepared_writes.upper_bound(
        std::make_pair(Version(read.version().client_id() + 1,
                               read.version().client_seq()),
                       0));
    if (prepared_writes_itr != vrecord.prepared_writes.end() &&
        *prepared_writes_itr < std::make_pair(Version(txn->version()), 0UL)) {
      Debug(
          "[%lu.%lu] Abort due to prepared wr conflict: our read set entry "
          "%lu.%lu < prepared write %lu.%lu < our read %lu.%lu.",
          txn->version().client_id(), txn->version().client_seq(),
          read.version().client_id(), read.version().client_seq(),
          prepared_writes_itr->first.GetClientId(),
          prepared_writes_itr->first.GetClientSeq(), txn->version().client_id(),
          txn->version().client_seq());

      stats.Increment("abort_prepared_wr_conflict");

      // abort fast instead of slow because the reading transaction should be
      //   encouraged to re-execute. aborting slow allows reading transaction to
      //   commit if enough other replicas vote commit
      return proto::VOTE_ABANDON_FAST;
    }

    // check if this transaction missed a committed write
    auto committed_writes_itr = vrecord.committed_writes.upper_bound(
        std::make_pair(Version(read.version().client_id() + 1,
                               read.version().client_seq()),
                       0));
    if (committed_writes_itr != vrecord.committed_writes.end() &&
        *committed_writes_itr < std::make_pair(Version(txn->version()), 0UL)) {
      Debug("[%lu.%lu] Abort due to committed wr conflict from txn %lu.%lu.",
            txn->version().client_id(), txn->version().client_seq(),
            committed_writes_itr->first.GetClientId(),
            committed_writes_itr->first.GetClientSeq());
      stats.Increment("abort_committed_wr_conflict");
      return proto::VOTE_ABANDON_FAST;
    }

    // add this transaction to prepared reads, though we may need to undo if
    //    a conflict on another key causes this transaction to abort. we need
    //    to add it now to ensure that the mvtso check and adding this prepared
    //    read happen atomically.
    vrecord.prepared_reads.insert(
        std::make_pair(std::make_pair(Version(txn->version()), txn->exec_id()),
                       read.version()));
  }

  for (const auto &write : txn->write_set()) {
    Debug("[%lu.%lu] Write set contains key %s.", txn->version().client_id(),
          txn->version().client_seq(), BytesToHex(write.key(), 16).c_str());

    VRecord &vrecord = GetOrCreateVRecord(write.key());

    std::unique_lock<std::mutex> lock(vrecord.mtx);

    // check if this transaction would cause a prepared transaction to miss a
    // write
    //   add 1 to lowest order member of Version to ensure that we are searching
    //   for versions strictly greater than the writing transaction's version
    //   (>= is incorrect when the writing transaction also reads the same key)
    auto prepared_reads_itr = vrecord.prepared_reads.upper_bound(std::make_pair(
        Version(txn->version().client_id() + 1, txn->version().client_seq()),
        0UL));
    while (prepared_reads_itr != vrecord.prepared_reads.end()) {
      if (prepared_reads_itr->second < Version(txn->version())) {
        Debug(
            "[%lu.%lu] Abort due to prepared rw conflict: prepared read"
            " %lu.%lu < our write %lu.%lu < prepared txn %lu.%lu.",
            txn->version().client_id(), txn->version().client_seq(),
            prepared_reads_itr->second.GetClientId(),
            prepared_reads_itr->second.GetClientSeq(),
            txn->version().client_id(), txn->version().client_seq(),
            prepared_reads_itr->first.first.GetClientId(),
            prepared_reads_itr->first.first.GetClientSeq());
        stats.Increment("abort_prepared_rw_conflict");
        return proto::VOTE_ABANDON_SLOW;
      }
      prepared_reads_itr++;
    }

    // following check is equivalent to checking that committed_rts < wts
    auto committed_reads_itr = vrecord.committed_reads.upper_bound(
        std::make_pair(Version(txn->version()), 0UL));
    while (committed_reads_itr != vrecord.committed_reads.end()) {
      if (committed_reads_itr->second < Version(txn->version())) {
        Debug("[%lu.%lu] Abort due to committed rw conflict.",
              txn->version().client_id(), txn->version().client_seq());
        stats.Increment("abort_committed_rw_conflict");
        return proto::VOTE_ABANDON_FAST;
      }
      committed_reads_itr++;
    }

    // add this transaction to prepared writes, though we may need to undo if
    //    a conflict on another key causes this transaction to abort. we need
    //    to add it now to ensure that the mvtso check and adding this prepared
    //    write happen atomically.
    vrecord.prepared_writes.insert(
        std::make_pair(txn->version(), txn->exec_id()));

    if (vrecord.prepared_time != nullptr) {
      Latency_Start(vrecord.prepared_time);
    }
  }

  return proto::VOTE_COMMIT;
}

void Server::CleanPreparedTransaction(proto::Transaction *txn) {
  for (const auto &read : txn->read_set()) {
    VRecord &vrecord = GetOrCreateVRecord(read.key());

    std::unique_lock<std::mutex> lock(vrecord.mtx);

    Debug("[%lu.%lu]  Prepared reads for key %s.", txn->version().client_id(),
          txn->version().client_seq(), BytesToHex(read.key(), 16).c_str());
    if (Message_DebugEnabled(__FILE__)) {
      for (const auto &prepared_read : vrecord.prepared_reads) {
        Debug(
            "[%lu.%lu]     Prepared read of %lu.%lu from txn %lu.%lu exec %lu.",
            txn->version().client_id(), txn->version().client_seq(),
            prepared_read.second.GetClientId(),
            prepared_read.second.GetClientSeq(),
            prepared_read.first.first.GetClientId(),
            prepared_read.first.first.GetClientSeq(),
            prepared_read.first.second);
      }
    }
    size_t num_erased = vrecord.prepared_reads.erase(
        std::make_pair(Version(txn->version()), txn->exec_id()));
    Debug("[%lu.%lu]     Erased %lu prepared reads from txn %lu.%lu exec %lu.",
          txn->version().client_id(), txn->version().client_seq(), num_erased,
          txn->version().client_id(), txn->version().client_seq(),
          txn->exec_id());
  }

  for (const auto &write : txn->write_set()) {
    VRecord &vrecord = GetOrCreateVRecord(write.key());

    std::unique_lock<std::mutex> lock(vrecord.mtx);

    Debug("[%lu.%lu]  Prepared writes for key %s.", txn->version().client_id(),
          txn->version().client_seq(), BytesToHex(write.key(), 16).c_str());

    for (const auto &prepared_write : vrecord.prepared_writes) {
      Debug("[%lu.%lu]     Prepared write from txn %lu.%lu exec %lu.",
            txn->version().client_id(), txn->version().client_seq(),
            prepared_write.first.GetClientId(),
            prepared_write.first.GetClientSeq(), prepared_write.second);
    }
    size_t num_erased = vrecord.prepared_writes.erase(
        std::make_pair(txn->version(), txn->exec_id()));
    Debug("[%lu.%lu]     Erased %lu prepared writes from txn %lu.%lu exec %lu.",
          txn->version().client_id(), txn->version().client_seq(), num_erased,
          txn->version().client_id(), txn->version().client_seq(),
          txn->exec_id());
    if (num_erased == 1 && vrecord.prepared_time != nullptr) {
      Latency_End(vrecord.prepared_time);
    }
  }
}

void Server::UpdateWatermark() {
  gc_watermark_ = time_.GetTime();
  uint64_t gc_watermark_s = gc_watermark_ >> 32;
  uint64_t gc_watermark_us = gc_watermark_ & 0xFFFFFFFF;
  if (gc_watermark_us > gc_watermark_buffer_us_) {
    gc_watermark_us -= gc_watermark_buffer_us_;
  } else if (gc_watermark_s > 0) {
    gc_watermark_s -= 1;
    gc_watermark_us = 1000000 + gc_watermark_us - gc_watermark_buffer_us_;
  } else {
    gc_watermark_us = 0;
  }
}

Server::ERecord &Server::GetOrCreateERecord(const Version &version,
                                            uint64_t exec_id,
                                            TRecord *&trecord) {
  auto trecord_itr = trecord_.find(version);
  if (trecord_itr == trecord_.end()) {
    // assume that there are never concurrent threads attempting to insert for
    //   same version. this should be true because same thread will handle all
    //   prepares for a given transaction version.
    auto insert_itr = trecord_.emplace(version, TRecord());
    trecord_itr = insert_itr.first;
  }

  trecord = &trecord_itr->second;

  // assume that there are never concurrent threads attempting to insert for
  //   same version. this should be true because same thread will handle all
  //   prepares for a given transaction version.
  auto erecord_insert_itr = trecord_itr->second.erecord.try_emplace(exec_id);

  return erecord_insert_itr.first->second;
}

bool Server::GetERecord(const Version &version, uint64_t exec_id,
                        ERecord *&erecord, TRecord *&trecord) {
  auto trecord_itr = trecord_.find(version);
  if (trecord_itr == trecord_.end()) {
    Warning("[%lu.%lu][%lu] Could not find TRecord.", version.GetClientId(),
            version.GetClientSeq(), exec_id);
    return false;
  }

  trecord = &trecord_itr->second;

  auto erecord_itr = trecord_itr->second.erecord.find(exec_id);
  if (erecord_itr == trecord_itr->second.erecord.end()) {
    Warning("[%lu.%lu][%lu] Could not find ERecord.", version.GetClientId(),
            version.GetClientSeq(), exec_id);
    return false;
  }

  erecord = &erecord_itr->second;

  return true;
}

Server::VRecord &Server::GetOrCreateVRecord(const std::string &key) {
  /*auto vstore_itr = vstore_.find(key);
  if (vstore_itr == vstore_.end()) {
    // assume that there are never concurrent threads attempting to insert
    //   for same key
    auto insert_itr = vstore_.insert(std::make_pair(key, std::move(VRecord())));
    vstore_itr = insert_itr.first;
  }
  UW_ASSERT(vstore_itr != vstore_.end());

  return vstore_itr->second;*/
  return vstore_[key];
}

}  // namespace mortystorev2
