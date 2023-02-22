// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 5 -*-
/***********************************************************************
 *
 * tcptransport.cc:
 *   message-passing network interface that uses TCP message delivery
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

#include "lib/tcptransport.h"

#include <arpa/inet.h>
#include <bits/types/struct_timeval.h>
#include <errno.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/thread.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"

struct event;
// #include "lib/threadpool.cc"

const size_t MAX_TCP_SIZE = 100;  // XXX
const uint32_t MAGIC = 0x06121983;
const int SOCKET_BUF_SIZE = 1048576;
const int SOCKET_MAXCONN_BACKLOG = 2048;

using std::pair;

TCPTransportAddress::TCPTransportAddress(const sockaddr_in &addr) : addr(addr) {
  memset(const_cast<unsigned char *>(addr.sin_zero), 0, sizeof(addr.sin_zero));
  // DANGER! NOT PORTABLE
  hash = (static_cast<size_t>(addr.sin_family) << 48) |
         (static_cast<size_t>(addr.sin_port) << 32) | addr.sin_addr.s_addr;
}

TCPTransportAddress *TCPTransportAddress::clone() const {
  auto *c = new TCPTransportAddress(*this);
  return c;
}

bool operator==(const TCPTransportAddress &a, const TCPTransportAddress &b) {
  return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
}

bool operator!=(const TCPTransportAddress &a, const TCPTransportAddress &b) {
  return !(a == b);
}

bool operator<(const TCPTransportAddress &a, const TCPTransportAddress &b) {
  return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
}

size_t TCPTransportAddressReceiverHasher::operator()(
    const std::pair<TCPTransportAddress, TransportReceiver *> &p) const {
  // using boost::hash_combine on p.first and p.second
  return p.first.hash ^ (reinterpret_cast<size_t>(p.second) + 0x9e3779b9 +
                         (p.first.hash << 6) + (p.first.hash >> 2));
}

TCPTransportAddress TCPTransport::LookupAddress(
    const transport::ReplicaAddress &addr) {
  int res;
  struct addrinfo hints {};
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = 0;
  hints.ai_flags = 0;
  struct addrinfo *ai;
  if ((res = getaddrinfo(addr.host.c_str(), addr.port.c_str(), &hints, &ai)) !=
      0) {
    Panic("Failed to resolve %s:%s: %s", addr.host.c_str(), addr.port.c_str(),
          gai_strerror(res));
  }
  if (ai->ai_addr->sa_family != AF_INET) {
    Panic("getaddrinfo returned a non IPv4 address");
  }
  TCPTransportAddress out =
      TCPTransportAddress(*(reinterpret_cast<sockaddr_in *>(ai->ai_addr)));
  freeaddrinfo(ai);
  return out;
}

TCPTransportAddress TCPTransport::LookupAddress(
    const transport::Configuration &config, int idx) {
  return LookupAddress(config, 0, idx);
}

TCPTransportAddress TCPTransport::LookupAddress(
    const transport::Configuration &config, int groupIdx, int replicaIdx) {
  const transport::ReplicaAddress &addr = config.replica(groupIdx, replicaIdx);
  return LookupAddress(addr);
}

static void BindToPort(int fd, const string &host, const string &port) {
  struct sockaddr_in sin {};

  // look up its hostname and port number (which
  // might be a service name)
  struct addrinfo hints {};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = 0;
  hints.ai_flags = AI_PASSIVE;
  struct addrinfo *ai;
  int res;
  if ((res = getaddrinfo(host.c_str(), port.c_str(), &hints, &ai)) != 0) {
    Panic("Failed to resolve host/port %s:%s: %s", host.c_str(), port.c_str(),
          gai_strerror(res));
  }
  UW_ASSERT(ai->ai_family == AF_INET);
  UW_ASSERT(ai->ai_socktype == SOCK_STREAM);
  if (ai->ai_addr->sa_family != AF_INET) {
    Panic("getaddrinfo returned a non IPv4 address");
  }
  sin = *reinterpret_cast<sockaddr_in *>(ai->ai_addr);

  freeaddrinfo(ai);

  Debug("Binding to %s %d TCP", inet_ntoa(sin.sin_addr), htons(sin.sin_port));

  if (bind(fd, reinterpret_cast<sockaddr *>(&sin), sizeof(sin)) < 0) {
    PPanic("Failed to bind to socket: %s:%d", inet_ntoa(sin.sin_addr),
           htons(sin.sin_port));
  }
}

TCPTransport::TCPTransport(double dropRate, double reorderRate, int dscp,
                           bool handleSignals, int process_id,
                           int total_processes, bool hyperthreading,
                           bool server)
    : server_(server) {
  // tp.start(process_id, total_processes, hyperthreading, server);

  lastTimerId = 0;

  // Set up libevent
  evthread_use_pthreads();
  // evthread_enable_lock_debugging();
  event_set_log_callback(LogCallback);
  event_set_fatal_callback(FatalCallback);

  libeventBase = event_base_new();
  Notice("Using Libevent with backend method %s.",
         event_base_get_method(libeventBase));
  /*enum event_method_feature f = event_base_get_features(libeventBase);
  if ((f & EV_FEATURE_ET))
      Notice("  Edge-triggered events are supported.");
  if ((f & EV_FEATURE_O1))
      Notice("  O(1) event notification is supported.");
  if ((f & EV_FEATURE_FDS))
      Notice("  All FD types are supported.");*/

  event_base_priority_init(libeventBase, 10);
  // tp2.emplace(); this works?
  // tp = new ThreadPool(); //change tp to *
  evthread_make_base_notifiable(libeventBase);

  // Set up signal handler
  if (handleSignals) {
    signalEvents.push_back(
        evsignal_new(libeventBase, SIGTERM, SignalCallback, this));
    signalEvents.push_back(
        evsignal_new(libeventBase, SIGINT, SignalCallback, this));
    signalEvents.push_back(
        evsignal_new(libeventBase, SIGPIPE,
                     [](int32_t fd, int16_t what, void *arg) {}, this));

    for (event *x : signalEvents) {
      event_add(x, nullptr);
    }
  }
  _Latency_Init(&sockWriteLat, "sock_write");
}

TCPTransport::~TCPTransport() {
  mtx.lock();
  for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end();) {
    bufferevent_free(itr->second);
    tcpAddresses.erase(itr->second);
    // TCPTransportTCPListener* info = nullptr;
    // bufferevent_getcb(itr->second, nullptr, nullptr, nullptr,
    //    (void **) &info);
    // if (info != nullptr) {
    //  delete info;
    //}
    itr = tcpOutgoing.erase(itr);
  }
  // for (auto kv : timers) {
  //     delete kv.second;
  // }
  Latency_Dump(&sockWriteLat);
  for (const auto info : tcpListeners) {
    delete info;
  }
  mtx.unlock();
  // XXX Shut down libevent?
  event_base_free(libeventBase);
}

void TCPTransport::ConnectTCP(
    const std::pair<TCPTransportAddress, TransportReceiver *> &dstSrc) {
  Debug("Opening new TCP connection to %s:%d",
        inet_ntoa(dstSrc.first.addr.sin_addr),
        htons(dstSrc.first.addr.sin_port));

  // Create socket
  int fd;
  if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    PPanic("Failed to create socket for outgoing TCP connection");
  }

  // Put it in non-blocking mode
  if (fcntl(fd, F_SETFL, O_NONBLOCK, 1) != 0) {
    PWarning("Failed to set O_NONBLOCK on outgoing TCP socket");
  }

  struct linger sl {};
  sl.l_onoff = 1;
  sl.l_linger = 5;
  if (setsockopt(fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl)) < 0) {
    PWarning("Failed to set SOL_SOCKET on socket %d.", fd);
  }

  // Set TCP_NODELAY
  int n = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set TCP_NODELAY on TCP listening socket");
  }

  n = SOCKET_BUF_SIZE;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set SO_RCVBUF on socket");
  }

  if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set SO_SNDBUF on socket");
  }

  /*n = SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_RX_SOFTWARE |
      SOF_TIMESTAMPING_TX_HARDWARE | SOF_TIMESTAMPING_TX_SOFTWARE;
  if (setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPING, (char *) &n, sizeof(n)) < 0) {
    PWarning("Failed to set SO_TIMESTAMPING.");
  }*/

  auto *info = new TCPTransportTCPListener();
  info->transport = this;
  info->acceptFd = 0;
  info->fd = fd;
  info->receiver = dstSrc.second;
  info->replicaIdx = -1;
  info->acceptEvent = nullptr;

  tcpListeners.push_back(info);

  int flags = BEV_OPT_CLOSE_ON_FREE;
  if (server_) {
    flags |= BEV_OPT_THREADSAFE;
  }
  struct bufferevent *bev = bufferevent_socket_new(libeventBase, fd, flags);

  // mtx.lock();
  tcpOutgoing[dstSrc] = bev;
  tcpAddresses.insert(
      std::pair<struct bufferevent *,
                std::pair<TCPTransportAddress, TransportReceiver *>>(bev,
                                                                     dstSrc));
  // mtx.unlock();

  bufferevent_setcb(bev, TCPReadableCallback, nullptr, TCPOutgoingEventCallback,
                    info);

  if (bufferevent_socket_connect(bev, (struct sockaddr *)&(dstSrc.first.addr),
                                 sizeof(dstSrc.first.addr)) < 0) {
    bufferevent_free(bev);

    // mtx.lock();
    tcpOutgoing.erase(dstSrc);
    tcpAddresses.erase(bev);
    // mtx.unlock();

    Warning("Failed to connect to server via TCP");
    return;
  }

  if (bufferevent_enable(bev, EV_READ | EV_WRITE) < 0) {
    Panic("Failed to enable bufferevent");
  }

  // Tell the receiver its address
  struct sockaddr_in sin {};
  socklen_t sinsize = sizeof(sin);
  if (getsockname(fd, reinterpret_cast<sockaddr *>(&sin), &sinsize) < 0) {
    PPanic("Failed to get socket name");
  }

  if (dstSrc.second->GetAddress() == nullptr) {
    auto *addr = new TCPTransportAddress(sin);
    dstSrc.second->SetAddress(addr);
  }

  Debug("Opened TCP connection to %s:%d from %s:%d",
        inet_ntoa(dstSrc.first.addr.sin_addr),
        htons(dstSrc.first.addr.sin_port), inet_ntoa(sin.sin_addr),
        htons(sin.sin_port));
}

void TCPTransport::Register(TransportReceiver *receiver,
                            const transport::Configuration &config,
                            int groupIdx, int replicaIdx, int portIdx) {
  UW_ASSERT(replicaIdx < config.n);
  struct sockaddr_in sin {};

  // const transport::Configuration *canonicalConfig =
  RegisterConfiguration(receiver, config, groupIdx, replicaIdx);

  // Clients don't need to accept TCP connections
  if (replicaIdx == -1) {
    return;
  }

  // Create socket
  int fd;
  if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    PPanic("Failed to create socket to accept TCP connections");
  }

  // Put it in non-blocking mode
  if (fcntl(fd, F_SETFL, O_NONBLOCK, 1) != 0) {
    PWarning("Failed to set O_NONBLOCK");
  }

  // Set SO_REUSEADDR
  int n = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set SO_REUSEADDR on TCP listening socket");
  }

  // Set TCP_NODELAY
  n = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set TCP_NODELAY on TCP listening socket");
  }

  n = SOCKET_BUF_SIZE;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set SO_RCVBUF on socket");
  }

  if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char *>(&n),
                 sizeof(n)) < 0) {
    PWarning("Failed to set SO_SNDBUF on socket");
  }

  /*n = SOF_TIMESTAMPING_RX_HARDWARE | SOF_TIMESTAMPING_RX_SOFTWARE |
      SOF_TIMESTAMPING_TX_HARDWARE | SOF_TIMESTAMPING_TX_SOFTWARE;
  if (setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPING, (char *) &n, sizeof(n)) < 0) {
    PWarning("Failed to set SO_TIMESTAMPING.");
  }*/

  // Registering a replica. Bind socket to the designated
  // host/port
  const string &host = config.replica(groupIdx, replicaIdx).host;
  int port = std::stoi(config.replica(groupIdx, replicaIdx).port) + portIdx;
  string port_str = std::to_string(port);
  BindToPort(fd, host, port_str);

  // Listen for connections
  if (listen(fd, SOCKET_MAXCONN_BACKLOG) < 0) {
    PPanic("Failed to listen for TCP connections");
  }

  // Create event to accept connections
  auto *info = new TCPTransportTCPListener();
  info->transport = this;
  info->acceptFd = fd;
  info->receiver = receiver;
  info->replicaIdx = replicaIdx;
  info->acceptEvent = event_new(libeventBase, fd, EV_READ | EV_PERSIST,
                                TCPAcceptCallback, info);
  event_add(info->acceptEvent, nullptr);
  tcpListeners.push_back(info);

  // Tell the receiver its address
  socklen_t sinsize = sizeof(sin);
  if (getsockname(fd, reinterpret_cast<sockaddr *>(&sin), &sinsize) < 0) {
    PPanic("Failed to get socket name");
  }
  auto *addr = new TCPTransportAddress(sin);
  receiver->SetAddress(addr);

  // Update mappings
  receivers[fd] = receiver;
  fds[receiver] = fd;

  Debug("Accepting connections on TCP port %hu", ntohs(sin.sin_port));
}

bool TCPTransport::OrderedMulticast(TransportReceiver *src,
                                    const std::vector<int> &groups,
                                    const Message &m) {
  Panic("Not implemented :(.");
}

bool TCPTransport::SendMessageInternal(TransportReceiver *src,
                                       const TCPTransportAddress &dst,
                                       const Message &m, size_t meta_len,
                                       void *meta_data) {
  Debug("Sending %s message over TCP to %s:%d", m.GetTypeName().c_str(),
        inet_ntoa(dst.addr.sin_addr), htons(dst.addr.sin_port));
  auto dstSrc = std::make_pair(dst, src);
  mtx.lock();
  auto kv = tcpOutgoing.find(dstSrc);
  // See if we have a connection open
  if (kv == tcpOutgoing.end()) {
    ConnectTCP(dstSrc);
    kv = tcpOutgoing.find(dstSrc);
  }
  struct bufferevent *ev = kv->second;
  mtx.unlock();

  UW_ASSERT(ev != nullptr);

  std::string serialize_data;
  // Serialize message
  UW_ASSERT(m.SerializeToString(&serialize_data));
  string type = m.GetTypeName();
  size_t typeLen = type.length();
  size_t dataLen = serialize_data.length();
  size_t totalLen = meta_len + sizeof(meta_len) + typeLen + sizeof(typeLen) +
                    dataLen + sizeof(dataLen) + sizeof(totalLen) +
                    sizeof(uint32_t);

  Debug("Message is %lu total bytes", totalLen);

  char buf[totalLen];
  char *ptr = buf;

  *(reinterpret_cast<uint32_t *>(ptr)) = MAGIC;
  ptr += sizeof(uint32_t);
  UW_ASSERT((size_t)(ptr - buf) < totalLen);

  *(reinterpret_cast<size_t *>(ptr)) = totalLen;
  ptr += sizeof(size_t);
  UW_ASSERT((size_t)(ptr - buf) < totalLen);

  *(reinterpret_cast<size_t *>(ptr)) = meta_len;
  ptr += sizeof(size_t);
  UW_ASSERT((size_t)(ptr - buf) < totalLen);
  UW_ASSERT((size_t)(ptr + meta_len - buf) < totalLen);
  memcpy(ptr, meta_data, meta_len);
  ptr += meta_len;

  *(reinterpret_cast<size_t *>(ptr)) = typeLen;
  ptr += sizeof(size_t);
  UW_ASSERT((size_t)(ptr - buf) < totalLen);
  UW_ASSERT((size_t)(ptr + typeLen - buf) < totalLen);
  memcpy(ptr, type.c_str(), typeLen);
  ptr += typeLen;

  *(reinterpret_cast<size_t *>(ptr)) = dataLen;
  ptr += sizeof(size_t);
  UW_ASSERT((size_t)(ptr - buf) < totalLen);
  UW_ASSERT((size_t)(ptr + dataLen - buf) == totalLen);
  memcpy(ptr, serialize_data.c_str(), dataLen);
  ptr += dataLen;

  // mtx.lock();
  // evbuffer_lock(ev);
  if (bufferevent_write(ev, buf, totalLen) < 0) {
    Warning("Failed to write to TCP buffer");
    fprintf(stderr, "tcp write failed\n");
    // evbuffer_unlock(ev);
    // mtx.unlock();
    return false;
  }
  // evbuffer_unlock(ev);
  // mtx.unlock();

  /*Latency_Start(&sockWriteLat);
  if (write(ev->ev_write.ev_fd, buf, totalLen) < 0) {
    Warning("Failed to write to TCP buffer");
    return false;
  }
  Latency_End(&sockWriteLat);*/
  return true;
}

void TCPTransport::Flush() { event_base_loop(libeventBase, EVLOOP_NONBLOCK); }

void TCPTransport::Run() {
  int ret = event_base_dispatch(libeventBase);
  Debug("event_base_dispatch returned %d.", ret);
}

void TCPTransport::StopLoop() { event_base_loopbreak(libeventBase); }

void TCPTransport::Stop() {
  // TODO(matthelb): cleaning up TCP connections needs to be done better
  // - We want to close connections from client side when we kill clients so
  // that
  //   server doesn't see many connections in TIME_WAIT and run out of file
  //   descriptors
  // - This is mainly a problem if the client is still running long after it
  // should have
  //   finished (due to abort loops)
  // tp.stop();
  event_base_dump_events(libeventBase, stderr);
  event_base_loopbreak(libeventBase);

  char buf[128];
  for (auto info : tcpListeners) {
    if (shutdown(info->fd, SHUT_WR) < 0) {
      Warning("Failed to shutdown socket %d.", info->fd);
      continue;
    }

    int flags = fcntl(info->fd, F_GETFL, 0);
    if (flags < 0) {
      Warning("Failed to get flags for socket %d.", info->fd);
      continue;
    }

    if (fcntl(info->fd, F_SETFL, flags & ~O_NONBLOCK, 1) < 0) {
      Warning("Failed to set socket %d to blocking.", info->fd);
      continue;
    }

    while (true) {
      ssize_t ret = read(info->fd, buf, sizeof(buf));
      if (ret < 0) {
        Warning("Failed to read from socket %d: %s.", info->fd,
                strerror(errno));
        break;
      }
      if (ret == 0) {
        break;
      }
    }
    if (close(info->fd) != 0) {
      Warning("Failed to close socket %d.", info->fd);
    }
  }
}

void TCPTransport::Close(TransportReceiver *receiver) {
  mtx.lock();
  for (auto itr = tcpOutgoing.begin(); itr != tcpOutgoing.end(); ++itr) {
    if (itr->first.second == receiver) {
      bufferevent_free(itr->second);
      tcpOutgoing.erase(itr);
      tcpAddresses.erase(itr->second);
      break;
    }
  }
  mtx.unlock();
}

int TCPTransport::Timer(uint64_t ms, timer_callback_t cb) {
  struct timeval tv {};
  tv.tv_sec = ms / 1000;
  tv.tv_usec = (ms % 1000) * 1000;

  return TimerInternal(&tv, std::move(cb));
}

int TCPTransport::TimerMicro(uint64_t us, timer_callback_t cb) {
  struct timeval tv {};
  tv.tv_sec = us / 1000000UL;
  tv.tv_usec = us % 1000000UL;

  return TimerInternal(&tv, std::move(cb));
}

int TCPTransport::TimerInternal(struct timeval *tv, timer_callback_t cb) {
  std::unique_lock<std::shared_mutex> lck(mtx);

  auto *info = new TCPTransportTimerInfo(std::move(cb));

  ++lastTimerId;

  info->transport = this;
  info->id = lastTimerId;
  info->ev = event_new(libeventBase, -1, 0, TimerCallback, info);

  timers[info->id] = info;

  event_add(info->ev, tv);

  return info->id;
}

void TCPTransport::Dispatch(timer_callback_t cb, int priority) {
  auto *info = new TCPTransportDispatchInfo(std::move(cb));
  info->ev = event_new(libeventBase, -1, 0, DispatchCallback, info);
  event_priority_set(info->ev, priority);
  event_add(info->ev, nullptr);
  event_active(info->ev, 0, 0);
}

bool TCPTransport::CancelTimer(int id) {
  std::unique_lock<std::shared_mutex> lck(mtx);
  auto info_itr = timers.find(id);

  if (info_itr == timers.end()) {
    return false;
  }

  event_del(info_itr->second->ev);
  event_free(info_itr->second->ev);
  delete info_itr->second;
  timers.erase(info_itr);

  return true;
}

void TCPTransport::CancelAllTimers() {
  mtx.lock();
  while (!timers.empty()) {
    auto kv = timers.begin();
    int id = kv->first;
    mtx.unlock();
    CancelTimer(id);
    mtx.lock();
  }
  mtx.unlock();
}

void TCPTransport::OnTimer(TCPTransportTimerInfo *info) {
  {
    std::unique_lock<std::shared_mutex> lck(mtx);

    timers.erase(info->id);
    event_del(info->ev);
    event_free(info->ev);
  }

  info->cb();

  delete info;
}

void TCPTransport::TimerCallback(evutil_socket_t fd, int16_t what, void *arg) {
  auto *info = static_cast<TCPTransport::TCPTransportTimerInfo *>(arg);

  UW_ASSERT(what & EV_TIMEOUT);

  info->transport->OnTimer(info);
}

void TCPTransport::DispatchCallback(evutil_socket_t fd, int16_t what,
                                    void *arg) {
  auto *info = static_cast<TCPTransport::TCPTransportDispatchInfo *>(arg);
  event_del(info->ev);
  event_free(info->ev);
  info->cb();
  delete info;
}

void TCPTransport::DispatchTP(std::function<void *()> f,
                              std::function<void(void *)> cb) {
  tp.dispatch(std::move(f), std::move(cb), libeventBase);
}

void TCPTransport::DispatchTP_local(std::function<void *()> f,
                                    std::function<void(void *)> cb) {
  tp.dispatch_local(std::move(f), std::move(cb));
}

void TCPTransport::DispatchTP_noCB(std::function<void *()> f) {
  tp.detatch(std::move(f));
}
void TCPTransport::DispatchTP_noCB_ptr(std::function<void *()> *f) {
  tp.detatch_ptr(f);
}
void TCPTransport::DispatchTP_main(std::function<void *()> f) {
  tp.detatch_main(std::move(f));
}
void TCPTransport::IssueCB(std::function<void(void *)> cb, void *arg) {
  // std::lock_guard<std::mutex> lck(mtx);
  tp.issueCallback(std::move(cb), arg, libeventBase);
}

void TCPTransport::LogCallback(int severity, const char *msg) {
  Message_Type msgType;
  switch (severity) {
    case _EVENT_LOG_DEBUG:
      msgType = MSG_DEBUG;
      break;
    case _EVENT_LOG_MSG:
      msgType = MSG_NOTICE;
      break;
    case _EVENT_LOG_WARN:
      msgType = MSG_WARNING;
      break;
    case _EVENT_LOG_ERR:
      msgType = MSG_WARNING;
      break;
    default:
      NOT_REACHABLE();
  }

  _Message(msgType, "libevent", 0, nullptr, "%s", msg);
}

void TCPTransport::FatalCallback(int err) {
  Panic("Fatal libevent error: %d", err);
}

void TCPTransport::SignalCallback(evutil_socket_t fd, int16_t what, void *arg) {
  Debug("Terminating on SIGTERM/SIGINT");
  auto *transport = static_cast<TCPTransport *>(arg);
  event_base_loopbreak(transport->libeventBase);
}

void TCPTransport::TCPAcceptCallback(evutil_socket_t fd, int16_t what,
                                     void *arg) {
  auto *info = static_cast<TCPTransportTCPListener *>(arg);
  TCPTransport *transport = info->transport;

  if ((what & EV_READ) != 0) {
    int newfd;
    struct sockaddr_in sin {};
    socklen_t sinLength = sizeof(sin);
    struct bufferevent *bev;

    // Accept a connection
    if ((newfd = accept(fd, reinterpret_cast<struct sockaddr *>(&sin),
                        &sinLength)) < 0) {
      PWarning("Failed to accept incoming TCP connection");
      return;
    }

    // Put it in non-blocking mode
    if (fcntl(newfd, F_SETFL, O_NONBLOCK, 1) != 0) {
      PWarning("Failed to set O_NONBLOCK");
    }

    // Set TCP_NODELAY
    int n = 1;
    if (setsockopt(newfd, IPPROTO_TCP, TCP_NODELAY,
                   reinterpret_cast<char *>(&n), sizeof(n)) < 0) {
      PWarning("Failed to set TCP_NODELAY on TCP listening socket");
    }

    info->fd = newfd;
    // Create a buffered event
    bev = bufferevent_socket_new(transport->libeventBase, newfd,
                                 BEV_OPT_CLOSE_ON_FREE | BEV_OPT_THREADSAFE);
    bufferevent_setcb(bev, TCPReadableCallback, nullptr,
                      TCPIncomingEventCallback, info);
    if (bufferevent_enable(bev, EV_READ | EV_WRITE) < 0) {
      Panic("Failed to enable bufferevent");
    }
    info->connectionEvents.push_back(bev);
    TCPTransportAddress client = TCPTransportAddress(sin);

    Debug("Opened incoming TCP connection from %s:%d", inet_ntoa(sin.sin_addr),
          htons(sin.sin_port));

    transport->mtx.lock();
    auto dstSrc = std::make_pair(client, info->receiver);
    transport->tcpOutgoing[dstSrc] = bev;
    transport->tcpAddresses.insert(
        pair<struct bufferevent *,
             pair<TCPTransportAddress, TransportReceiver *>>(bev, dstSrc));
    transport->mtx.unlock();

    Debug("Finished adding connection %s:%d to shared data structures.",
          inet_ntoa(sin.sin_addr), htons(sin.sin_port));
  }
}

void TCPTransport::TCPReadableCallback(struct bufferevent *bev, void *arg) {
  auto *info = static_cast<TCPTransportTCPListener *>(arg);
  TCPTransport *transport = info->transport;
  struct evbuffer *evbuf = bufferevent_get_input(bev);

  while (evbuffer_get_length(evbuf) > 0) {
    /* packet format:
     * FRAG_MAGIC + meta_len + meta + type length + type + data length + data
     */
    uint32_t *magic;
    magic =
        reinterpret_cast<uint32_t *>(evbuffer_pullup(evbuf, sizeof(*magic)));
    if (magic == nullptr) {
      Debug("No magic at head of stream.");
      return;
    }
    UW_ASSERT(*magic == MAGIC);

    size_t *sz;
    unsigned char *x = evbuffer_pullup(evbuf, sizeof(*magic) + sizeof(*sz));

    sz = reinterpret_cast<size_t *>(x + sizeof(*magic));
    if (x == nullptr) {
      Debug("No size at head of stream.");
      return;
    }
    size_t totalSize = *sz;
    UW_ASSERT(totalSize < 1073741826);

    size_t evbuf_length = evbuffer_get_length(evbuf);
    if (evbuf_length < totalSize) {
      Debug("Only received %lu bytes of %lu byte message.", evbuf_length,
            totalSize);
      return;
    }
    // Debug("Receiving %ld byte message", totalSize);

    char buf[totalSize];
    size_t copied = evbuffer_remove(evbuf, buf, totalSize);
    UW_ASSERT(copied == totalSize);

    // Parse message
    char *ptr = buf + sizeof(*sz) + sizeof(*magic);

    size_t meta_len = *(reinterpret_cast<size_t *>(ptr));
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr - buf) < totalSize);
    UW_ASSERT((size_t)(ptr + meta_len - buf) < totalSize);
    void *meta_data = ptr;
    ptr += meta_len;

    size_t typeLen = *(reinterpret_cast<size_t *>(ptr));
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr - buf) < totalSize);
    UW_ASSERT((size_t)(ptr + typeLen - buf) < totalSize);
    string msgType(ptr, typeLen);
    ptr += typeLen;

    size_t msgLen = *(reinterpret_cast<size_t *>(ptr));
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr - buf) < totalSize);

    UW_ASSERT((size_t)(ptr + msgLen - buf) <= totalSize);
    string msg(ptr, msgLen);
    ptr += msgLen;

    transport->mtx.lock();
    auto addr_itr = transport->tcpAddresses.find(bev);
    if (addr_itr == transport->tcpAddresses.end()) {
      Warning("Received message for closed connection.");
      transport->mtx.unlock();
    } else {
      // Dispatch
      transport->mtx.unlock();
      Debug("Received %lu bytes %s message from %s:%d", totalSize,
            msgType.c_str(), inet_ntoa(addr_itr->second.first.addr.sin_addr),
            htons(addr_itr->second.first.addr.sin_port));
      info->receiver->ReceiveMessage(addr_itr->second.first, &msgType, &msg,
                                     meta_data);
      // Debug("Done processing large %s message", msgType.c_str());
    }
  }
}

void TCPTransport::TCPIncomingEventCallback(struct bufferevent *bev,
                                            int16_t what, void *arg) {
  auto *info = static_cast<TCPTransportTCPListener *>(arg);
  TCPTransport *transport = info->transport;

  Debug("Incoming event on TCP connection %d", what);

  if ((what & (BEV_EVENT_READING | BEV_EVENT_EOF)) != 0) {
    // properly close socket as requested by client
    //   bufferevent set to CLOSE_ON_FREE

    transport->mtx.lock();
    auto addr_itr = transport->tcpAddresses.find(bev);
    UW_ASSERT(addr_itr != transport->tcpAddresses.end());

    Debug("Closing connection to client %s:%d",
          inet_ntoa(addr_itr->second.first.addr.sin_addr),
          htons(addr_itr->second.first.addr.sin_port));

    transport->tcpOutgoing.erase(addr_itr->second);
    transport->tcpAddresses.erase(addr_itr);
    transport->mtx.unlock();
    bufferevent_free(bev);
    close(info->fd);

  } else if ((what & BEV_EVENT_ERROR) != 0) {
    Panic("Error on incoming TCP connection: %s",
          evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    bufferevent_free(bev);
  }
}

// Note: If ever to make client multithreaded, add mutexes here. (same for
// ConnectTCP)
void TCPTransport::TCPOutgoingEventCallback(struct bufferevent *bev,
                                            int16_t what, void *arg) {
  auto *info = static_cast<TCPTransportTCPListener *>(arg);
  TCPTransport *transport = info->transport;
  transport->mtx.lock();
  auto it = transport->tcpAddresses.find(bev);
  UW_ASSERT(it != transport->tcpAddresses.end());
  TCPTransportAddress addr = it->second.first;
  transport->mtx.unlock();

  if ((what & BEV_EVENT_CONNECTED) != 0) {
    Debug("Established outgoing TCP connection to %s:%d.",
          inet_ntoa(addr.addr.sin_addr), htons(addr.addr.sin_port));
  } else if ((what & BEV_EVENT_ERROR) != 0) {
    Warning("Error on outgoing TCP connection to %s:%d: %s",
            inet_ntoa(addr.addr.sin_addr), htons(addr.addr.sin_port),
            evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR()));
    bufferevent_free(bev);

    transport->mtx.lock();
    auto it2 =
        transport->tcpOutgoing.find(std::make_pair(addr, info->receiver));
    transport->tcpOutgoing.erase(it2);
    transport->tcpAddresses.erase(bev);
    transport->mtx.unlock();

    return;
  } else if ((what & BEV_EVENT_EOF) != 0) {
    Debug("EOF on outgoing TCP connection to %s:%d.",
          inet_ntoa(addr.addr.sin_addr), htons(addr.addr.sin_port));

    bufferevent_free(bev);

    transport->mtx.lock();
    auto it2 =
        transport->tcpOutgoing.find(std::make_pair(addr, info->receiver));
    transport->tcpOutgoing.erase(it2);
    transport->tcpAddresses.erase(bev);
    transport->mtx.unlock();

    return;
  }
}
