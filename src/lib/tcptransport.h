// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * tcptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef LIB_TCPTRANSPORT_H_
#define LIB_TCPTRANSPORT_H_

#include <bits/std_function.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/util.h>
#include <netinet/in.h>

#include <cstddef>
#include <list>
#include <map>
#include <random>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/threadpool.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"

namespace transport {
class Configuration;
struct ReplicaAddress;
}  // namespace transport
struct bufferevent;
struct event;
struct event_base;

class TCPTransportAddress : public TransportAddress {
 public:
  TCPTransportAddress *clone() const override;
  ~TCPTransportAddress() override = default;
  sockaddr_in addr;
  size_t hash;

 private:
  explicit TCPTransportAddress(const sockaddr_in &addr);

  friend class TCPTransport;
  friend bool operator==(const TCPTransportAddress &a,
                         const TCPTransportAddress &b);
  friend bool operator!=(const TCPTransportAddress &a,
                         const TCPTransportAddress &b);
  friend bool operator<(const TCPTransportAddress &a,
                        const TCPTransportAddress &b);
};

struct TCPTransportAddressReceiverHasher {
  size_t operator()(
      const std::pair<TCPTransportAddress, TransportReceiver *> &p) const;
};

class TCPTransport : public TransportCommon<TCPTransportAddress> {
 public:
  explicit TCPTransport(double dropRate = 0.0, double reogrderRate = 0.0,
                        int dscp = 0, bool handleSignals = true,
                        int process_id = 0, int total_processes = 1,
                        bool hyperthreading = true, bool server = true);
  virtual ~TCPTransport();
  void Register(TransportReceiver *receiver,
                const transport::Configuration &config, int groupIdx,
                int replicaIdx, int portIdx = 0) override;
  bool OrderedMulticast(TransportReceiver *src, const std::vector<int> &groups,
                        const Message &m) override;

  void Run() override;
  void Stop() override;
  void StopLoop() override;
  void Close(TransportReceiver *receiver) override;
  int Timer(uint64_t ms, timer_callback_t cb) override;
  int TimerMicro(uint64_t us, timer_callback_t cb) override;
  void Dispatch(timer_callback_t cb, int priority = 9) override;
  bool CancelTimer(int id) override;
  void CancelAllTimers() override;
  void Flush() override;

  void DispatchTP(std::function<void *()> f, std::function<void(void *)> cb);
  void DispatchTP_local(std::function<void *()> f,
                        std::function<void(void *)> cb);
  void DispatchTP_noCB(std::function<void *()> f);
  void DispatchTP_noCB_ptr(std::function<void *()> *f);
  void DispatchTP_main(std::function<void *()> f);
  void IssueCB(std::function<void(void *)> cb, void *arg);

  TCPTransportAddress LookupAddress(const transport::Configuration &config,
                                    int idx);

  TCPTransportAddress LookupAddress(const transport::Configuration &config,
                                    int groupIdx, int replicaIdx) override;

  TCPTransportAddress LookupAddress(const transport::ReplicaAddress &addr);

 private:
  int TimerInternal(struct timeval *tv, timer_callback_t cb);
  // using recursive mtx because libevent may invoke a callback that uses the
  //   mtx while we have already acquired the mtx on the main thread
  std::shared_mutex mtx;
  const bool server_;
  struct TCPTransportTimerInfo {
    explicit TCPTransportTimerInfo(timer_callback_t &&cb) : cb(std::move(cb)) {}
    timer_callback_t cb{};
    TCPTransport *transport{};
    event *ev{};
    int id{};
  };
  struct TCPTransportDispatchInfo {
    explicit TCPTransportDispatchInfo(timer_callback_t &&cb)
        : cb(std::move(cb)) {}
    timer_callback_t cb{};
    event *ev{};
  };
  struct TCPTransportTCPListener {
    TCPTransport *transport;
    TransportReceiver *receiver;
    int acceptFd;
    int fd;
    int groupIdx;
    int replicaIdx;
    event *acceptEvent;
    std::list<struct bufferevent *> connectionEvents;
  };
  event_base *libeventBase;
  std::vector<event *> listenerEvents;
  std::vector<event *> signalEvents;
  std::map<int, TransportReceiver *> receivers;  // fd -> receiver
  std::map<TransportReceiver *, int> fds;        // receiver -> fd
  int lastTimerId;
  std::unordered_map<int, TCPTransportTimerInfo *> timers;
  std::list<TCPTransportTCPListener *> tcpListeners;
  std::unordered_map<std::pair<TCPTransportAddress, TransportReceiver *>,
                     struct bufferevent *, TCPTransportAddressReceiverHasher>
      tcpOutgoing;
  std::map<struct bufferevent *,
           std::pair<TCPTransportAddress, TransportReceiver *>>
      tcpAddresses;
  Latency_t sockWriteLat{};
  ThreadPool tp;

  bool stopped{};
  std::string serialize_data_;

  bool SendMessageInternal(TransportReceiver *src,
                           const TCPTransportAddress &dst, const Message &m,
                           size_t meta_len, void *meta_data) override;
  const TCPTransportAddress *LookupMulticastAddress(
      const transport::Configuration *config) override {
    return nullptr;
  };

  const TCPTransportAddress *LookupFCAddress(
      const transport::Configuration *cfg) override {
    return nullptr;
  }

  void ConnectTCP(
      const std::pair<TCPTransportAddress, TransportReceiver *> &dstSrc);
  void OnTimer(TCPTransportTimerInfo *info);
  static void TimerCallback(evutil_socket_t fd, int16_t what, void *arg);
  static void DispatchCallback(evutil_socket_t fd, int16_t what, void *arg);
  static void LogCallback(int severity, const char *msg);
  static void FatalCallback(int err);
  static void SignalCallback(evutil_socket_t fd, int16_t what, void *arg);
  static void TCPAcceptCallback(evutil_socket_t fd, int16_t what, void *arg);
  static void TCPReadableCallback(struct bufferevent *bev, void *arg);
  static void TCPEventCallback(struct bufferevent *bev, int16_t what,
                               void *arg);
  static void TCPIncomingEventCallback(struct bufferevent *bev, int16_t what,
                                       void *arg);
  static void TCPOutgoingEventCallback(struct bufferevent *bev, int16_t what,
                                       void *arg);
};

#endif  // LIB_TCPTRANSPORT_H_
