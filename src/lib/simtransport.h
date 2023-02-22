// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * simtransport.h:
 *   simulated message-passing interface for testing use
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

#ifndef LIB_SIMTRANSPORT_H_
#define LIB_SIMTRANSPORT_H_

#include <bits/std_function.h>
#include <bits/stdint-uintn.h>
#include <netinet/in.h>

#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"

namespace transport {
class Configuration;
}  // namespace transport
template <typename ADDR>
class TransportCommon;

class SimulatedTransportAddress : public TransportAddress {
 public:
  SimulatedTransportAddress *clone() const override;
  explicit SimulatedTransportAddress(int addr);
  int GetAddr() const;
  bool operator==(const SimulatedTransportAddress &other) const;
  inline bool operator!=(const SimulatedTransportAddress &other) const {
    return !(*this == other);
  }

 private:
  sockaddr_in addr{};
  friend class SimulatedTransport;
  friend class TransportCommon<SimulatedTransportAddress>;
};

class SimulatedTransport : public TransportCommon<SimulatedTransportAddress> {
  using filter_t = int;

 public:
  SimulatedTransport();
  ~SimulatedTransport();
  void Register(TransportReceiver *receiver,
                const transport::Configuration &config, int replicaIdx);
  void Register(TransportReceiver *receiver,
                const transport::Configuration &config, int groupIdx,
                int replicaIdx, int portIdx = 0) override;
  void Run();
  void AddFilter(int id, filter_t filter);
  void RemoveFilter(int id);
  int Timer(uint64_t ms, timer_callback_t cb) override;
  int TimerMicro(uint64_t us, timer_callback_t cb) override;
  bool CancelTimer(int id);
  void CancelAllTimers();
  void Stop() override;
  void Close(TransportReceiver *receiver) override;

  void Dispatch(timer_callback_t cb, int priority = 9) override;
  void DispatchTP(std::function<void *()> f, std::function<void(void *)> cb);
  void DispatchTP_local(std::function<void *()> f,
                        std::function<void(void *)> cb);
  void DispatchTP_noCB(std::function<void *()> f);
  void DispatchTP_noCB_ptr(std::function<void *()> *f);
  void DispatchTP_main(std::function<void *()> f);
  void IssueCB(std::function<void(void *)> cb, void *arg);

  bool SendMessageToReplica(TransportReceiver *src, int groupIdx,
                            int replicaIdx, const Message &m,
                            size_t meta_len = 0,
                            void *meta_data = nullptr) override;

  SimulatedTransportAddress LookupAddress(const transport::Configuration &cfg,
                                          int idx);
  SimulatedTransportAddress LookupAddress(const transport::Configuration &cfg,
                                          int groupIdx, int idx);
  const SimulatedTransportAddress *LookupMulticastAddress(
      const transport::Configuration *cfg);
  const SimulatedTransportAddress *LookupFCAddress(
      const transport::Configuration *cfg) override;

 protected:
  bool SendMessageInternal(TransportReceiver *src,
                           const SimulatedTransportAddress &dstAddr,
                           const Message &m, bool multicast);

  bool SendMessageInternal(TransportReceiver *src,
                           const SimulatedTransportAddress &dstAddr,
                           const Message &m, size_t meta_len = 0,
                           void *meta_data = nullptr) override;

 private:
  struct QueuedMessage {
    int dst;
    int src;
    string type{};
    string msg{};
    inline QueuedMessage(int dst, int src, const string &type,
                         const string &msg)
        : dst(dst), src(src), type(type), msg(msg) {}
  };
  struct PendingTimer {
    uint64_t when;
    int id;
    timer_callback_t cb;
  };

  std::deque<QueuedMessage> queue;
  std::map<int, TransportReceiver *> endpoints;
  int lastAddr;
  //    std::map<int,int> replicas;
  std::map<int, int> replicaIdxs;
  std::map<int, std::map<int, int>> g_replicaIdxs;
  std::multimap<int, filter_t> filters;
  std::multimap<uint64_t, PendingTimer> timers;
  int lastTimerId;
  uint64_t vtime;
  bool processTimers;
  int fcAddress{};
  bool running{};

  bool _SendMessageInternal(TransportReceiver *src,
                            const SimulatedTransportAddress &dstAddr,
                            const Message &m, const multistamp_t &stamp);
};

#endif  // LIB_SIMTRANSPORT_H_
