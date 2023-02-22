// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
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

#ifndef LIB_UDPTRANSPORT_H_
#define LIB_UDPTRANSPORT_H_

#include <bits/std_function.h>
#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <event2/event.h>
#include <event2/util.h>
#include <netinet/in.h>
#include <unistd.h>

#include <cstddef>
#include <list>
#include <map>
#include <mutex>  // NOLINT[build/c++11]
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "lib/configuration.h"
#include "lib/threadpool.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"

namespace transport {
class Configuration;
struct ReplicaAddress;
}  // namespace transport
struct event;
struct event_base;

class UDPTransportAddress : public TransportAddress {
 public:
  UDPTransportAddress *clone() const override;
  ~UDPTransportAddress() override = default;
  sockaddr_in addr;

 private:
  explicit UDPTransportAddress(const sockaddr_in &addr);
  friend class UDPTransport;
  friend bool operator==(const UDPTransportAddress &a,
                         const UDPTransportAddress &b);
  friend bool operator!=(const UDPTransportAddress &a,
                         const UDPTransportAddress &b);
  friend bool operator<(const UDPTransportAddress &a,
                        const UDPTransportAddress &b);
};

class UDPTransport : public TransportCommon<UDPTransportAddress> {
 public:
  explicit UDPTransport(double dropRate = 0.0, double reorderRate = 0.0,
                        int dscp = 0, event_base *evbase = nullptr);
  virtual ~UDPTransport();
  void Register(TransportReceiver *receiver,
                const transport::Configuration &config, int groupIdx,
                int replicaIdx, int portIdx = 0) override;
  bool OrderedMulticast(TransportReceiver *src, const std::vector<int> &groups,
                        const Message &m) override;
  void Run() override;
  void Stop() override;
  void Close(TransportReceiver *receiver) override;

  int Timer(uint64_t ms, timer_callback_t cb) override;
  int TimerMicro(uint64_t us, timer_callback_t cb) override;
  void Dispatch(timer_callback_t cb, int priority = 9) override;
  bool CancelTimer(int id) override;
  void CancelAllTimers() override;

  void DispatchTP(std::function<void *()> f,
                  std::function<void(void *)> cb) override;
  void DispatchTP_local(std::function<void *()> f,
                        std::function<void(void *)> cb) override;
  void DispatchTP_noCB(std::function<void *()> f) override;
  void DispatchTP_noCB_ptr(std::function<void *()> *f) override;
  void DispatchTP_main(std::function<void *()> f) override;
  void IssueCB(std::function<void(void *)> cb, void *arg) override;

 private:
  int TimerInternal(struct timeval &tv, timer_callback_t cb);
  struct UDPTransportTimerInfo {
    UDPTransport *transport;
    timer_callback_t cb;
    event *ev;
    int id;
  };

  double dropRate;
  double reorderRate;
  std::uniform_real_distribution<double> uniformDist;
  std::default_random_engine randomEngine;
  struct {
    bool valid;
    UDPTransportAddress *addr;
    string msgType;
    string message;
    int fd;
  } reorderBuffer;
  int dscp;

  event_base *libeventBase;
  std::vector<event *> listenerEvents;
  std::vector<event *> signalEvents;
  std::map<int, TransportReceiver *> receivers;  // fd -> receiver
  std::map<TransportReceiver *, int> fds;        // receiver -> fd
  std::map<const transport::Configuration *, int> multicastFds;
  std::map<int, const transport::Configuration *> multicastConfigs;
  std::set<int> rawFds;
  int lastTimerId;
  std::map<int, UDPTransportTimerInfo *> timers;
  std::mutex timersLock;
  uint64_t lastFragMsgId;
  ThreadPool tp;
  struct UDPTransportFragInfo {
    uint64_t msgId;
    string data;
  };
  std::map<UDPTransportAddress, UDPTransportFragInfo> fragInfo;
  // ThreadPool tp;

  bool _SendMessageInternal(TransportReceiver *src,
                            const UDPTransportAddress &dst, const Message &m,
                            size_t meta_len, void *meta_data);
  bool SendMessageInternal(TransportReceiver *src,
                           const UDPTransportAddress &dst, const Message &m,
                           size_t meta_len, void *meta_data) override;

  UDPTransportAddress LookupAddress(const transport::ReplicaAddress &addr);
  UDPTransportAddress LookupAddress(const transport::Configuration &config,
                                    int groupIdx, int replicaIdx) override;
  const UDPTransportAddress *LookupMulticastAddress(
      const transport::Configuration *config) override;
  const UDPTransportAddress *LookupFCAddress(
      const transport::Configuration *config) override;
  void ListenOnMulticastPort(const transport::Configuration *canonicalConfig,
                             int groupIdx, int replicaIdx);
  void OnReadable(int fd);
  void ProcessPacket(int fd, sockaddr_in sender, socklen_t senderSize,
                     char *buf, ssize_t sz);
  void OnTimer(UDPTransportTimerInfo *info);
  static void SocketCallback(evutil_socket_t fd, int16_t what, void *arg);
  static void TimerCallback(evutil_socket_t fd, int16_t what, void *arg);
  static void LogCallback(int32_t severity, const char *msg);
  static void FatalCallback(int32_t err);
  static void SignalCallback(evutil_socket_t fd, int16_t what, void *arg);
};

#endif  // LIB_UDPTRANSPORT_H_
