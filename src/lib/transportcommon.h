// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport-common.h:
 *   template support for implementing transports
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                Jialin Li        <lijl@cs.washington.edu>
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

#ifndef LIB_TRANSPORTCOMMON_H_
#define LIB_TRANSPORTCOMMON_H_

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <cstddef>
#include <map>
#include <mutex>  // NOLINT[build/c++11]
#include <unordered_map>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"

template <typename ADDR>
class TransportCommon : public Transport {
 public:
  TransportCommon() { replicaAddressesInitialized = false; }

  ~TransportCommon() override {
    for (auto &kv : canonicalConfigs) {
      delete kv.second;
    }
  }

  bool SendMessage(TransportReceiver *src, const TransportAddress &dst,
                   const Message &m) override {
    const auto &dstAddr = dynamic_cast<const ADDR &>(dst);
    return SendMessageInternal(src, dstAddr, m);
  }

  bool SendMessageToReplica(TransportReceiver *src, int replicaIdx,
                            const Message &m) override {
    UW_ASSERT(this->replicaGroups.find(src) != this->replicaGroups.end());
    int groupIdx =
        this->replicaGroups[src] == -1 ? 0 : this->replicaGroups[src];
    return SendMessageToReplica(src, groupIdx, replicaIdx, m);
  }

  bool SendMessageToReplica(TransportReceiver *src, int groupIdx,
                            int replicaIdx, const Message &m,
                            size_t meta_len = 0,
                            void *meta_data = nullptr) override {
    const transport::Configuration *cfg = configurations[src];
    UW_ASSERT(cfg != nullptr);

    // mtx.lock();
    if (!replicaAddressesInitialized) {
      LookupAddresses();
    }
    // mtx.unlock();

    Debug("Send message to replica %d %d.", groupIdx, replicaIdx);

    auto kv = replicaAddresses[cfg][groupIdx].find(replicaIdx);
    UW_ASSERT(kv != replicaAddresses[cfg][groupIdx].end());

    return SendMessageInternal(src, kv->second, m, meta_len, meta_data);
  }

  bool SendMessageToFC(TransportReceiver *src, const Message &m) override {
    const transport::Configuration *cfg = configurations[src];
    UW_ASSERT(cfg != nullptr);

    // mtx.lock();
    if (!replicaAddressesInitialized) {
      LookupAddresses();
    }
    // mtx.unlock();

    auto kv = fcAddresses.find(cfg);
    if (kv == fcAddresses.end()) {
      Panic("Configuration has no failure coordinator address");
    }

    return SendMessageInternal(src, kv->second, m);
  }

  bool SendMessageToAll(TransportReceiver *src, const Message &m) override {
    UW_ASSERT(this->replicaGroups.find(src) != this->replicaGroups.end());
    int groupIdx =
        this->replicaGroups[src] == -1 ? 0 : this->replicaGroups[src];
    const transport::Configuration *cfg = configurations[src];
    UW_ASSERT(cfg != nullptr);

    // mtx.lock();
    if (!replicaAddressesInitialized) {
      LookupAddresses();
    }
    // mtx.unlock();

    return SendMessageToGroup(src, groupIdx, m);
  }

  bool SendMessageToAllGroups(TransportReceiver *src,
                              const Message &m) override {
    const transport::Configuration *cfg = configurations[src];
    UW_ASSERT(cfg != nullptr);

    // mtx.lock();
    if (!replicaAddressesInitialized) {
      LookupAddresses();
    }
    // mtx.unlock();

    const auto *srcAddr = dynamic_cast<const ADDR *>(src->GetAddress());
    for (auto &kv : replicaAddresses[cfg]) {
      for (auto &kv2 : kv.second) {
        if (*srcAddr == kv2.second) {
          continue;
        }
        if (!SendMessageInternal(src, kv2.second, m)) {
          return false;
        }
      }
    }
    return true;
  }

  bool SendMessageToGroup(TransportReceiver *src, int groupIdx,
                          const Message &m, size_t meta_len = 0,
                          void *meta_data = nullptr) override {
    return SendMessageToGroups(src, std::vector<int>{groupIdx}, m, meta_len,
                               meta_data);
  }

  bool SendMessageToGroups(TransportReceiver *src,
                           const std::vector<int> &groups, const Message &m,
                           size_t meta_len, void *meta_data) override {
    const transport::Configuration *cfg = configurations[src];
    UW_ASSERT(cfg != nullptr);

    // mtx.lock();
    if (!replicaAddressesInitialized) {
      LookupAddresses();
    }
    // mtx.unlock();

    int srcGroup = -1;
    auto replicaGroupsItr = replicaGroups.find(src);
    if (replicaGroupsItr != replicaGroups.end()) {
      srcGroup = replicaGroupsItr->second;
    }

    const ADDR *srcAddr;
    if (srcGroup != -1) {
      srcAddr = dynamic_cast<const ADDR *>(src->GetAddress());
      Debug("Source addr %s:%d.", inet_ntoa(srcAddr->addr.sin_addr),
            htons(srcAddr->addr.sin_port));
    }

    for (int groupIdx : groups) {
      for (auto &kv : replicaAddresses[cfg][groupIdx]) {
        if (srcGroup != -1 && *srcAddr == kv.second) {
          continue;
        }
        Debug("Sending message to replica %d in group %d (%s:%d).", kv.first,
              groupIdx, inet_ntoa(kv.second.addr.sin_addr),
              htons(kv.second.addr.sin_port));

        if (!SendMessageInternal(src, kv.second, m, meta_len, meta_data)) {
          return false;
        }
      }
    }
    return true;
  }

  bool OrderedMulticast(TransportReceiver *src, const std::vector<int> &groups,
                        const Message &m) override {
    Panic("Transport implementation does not support OrderedMulticast");
  }

  bool OrderedMulticast(TransportReceiver *src, const Message &m) override {
    std::vector<int> groups = {0};
    return OrderedMulticast(src, groups, m);
  }

 protected:
  std::mutex mtx{};

  virtual bool SendMessageInternal(TransportReceiver *src, const ADDR &dst,
                                   const Message &m, size_t meta_len = 0,
                                   void *meta_data = nullptr) = 0;
  virtual ADDR LookupAddress(const transport::Configuration &cfg, int groupIdx,
                             int replicaIdx) = 0;
  virtual const ADDR *LookupMulticastAddress(
      const transport::Configuration *cfg) = 0;
  virtual const ADDR *LookupFCAddress(const transport::Configuration *cfg) = 0;

  std::unordered_map<transport::Configuration, transport::Configuration *>
      canonicalConfigs{};
  std::map<TransportReceiver *, transport::Configuration *> configurations{};
  std::map<const transport::Configuration *,
           std::map<int, std::map<int, ADDR> > >
      replicaAddresses;  // config->groupid->replicaid->ADDR
  std::map<const transport::Configuration *,
           std::map<int, std::map<int, TransportReceiver *> > >
      replicaReceivers;
  std::map<const transport::Configuration *, ADDR> multicastAddresses{};
  std::map<const transport::Configuration *, ADDR> fcAddresses{};
  std::map<TransportReceiver *, int> replicaGroups{};
  bool replicaAddressesInitialized;

  /* configs is a map of groupIdx to Configuration */
  virtual transport::Configuration *RegisterConfiguration(
      TransportReceiver *receiver, const transport::Configuration &config,
      int groupIdx, int replicaIdx) {
    UW_ASSERT(receiver != nullptr);

    // Have we seen this configuration before? If so, get a
    // pointer to the canonical copy; if not, create one. This
    // allows us to use that pointer as a key in various
    // structures.
    transport::Configuration *canonical = canonicalConfigs[config];
    if (canonical == nullptr) {
      canonical = new transport::Configuration(config);
      canonicalConfigs[config] = canonical;
    }
    // Record configuration
    configurations[receiver] = canonical;

    // If this is a replica, record the receiver
    if (replicaIdx != -1) {
      UW_ASSERT(groupIdx != -1);
      replicaReceivers[canonical][groupIdx][replicaIdx] = receiver;
    }

    // Record which group this receiver belongs to
    replicaGroups[receiver] = groupIdx;

    // Mark replicaAddreses as uninitalized so we'll look up
    // replica addresses again the next time we send a message.
    replicaAddressesInitialized = false;
    LookupAddresses();
    return canonical;
  }

  virtual void LookupAddresses() {
    // Clear any existing list of addresses
    replicaAddresses.clear();
    multicastAddresses.clear();
    fcAddresses.clear();

    // For every configuration, look up all addresses and cache
    // them.
    for (auto &kv : canonicalConfigs) {
      transport::Configuration *cfg = kv.second;

      for (int i = 0; i < cfg->g; i++) {
        for (size_t j = 0; j < cfg->replicas[i].size(); j++) {
          const ADDR addr = LookupAddress(*cfg, i, j);
          replicaAddresses[cfg][i].insert(std::make_pair(j, addr));
        }
      }

      // And check if there's a multicast address
      if (cfg->multicast()) {
        const ADDR *addr = LookupMulticastAddress(cfg);
        if (addr) {
          multicastAddresses.insert(std::make_pair(cfg, *addr));
          delete addr;
        }
      }
      // Failure coordinator address
      if (cfg->fc()) {
        const ADDR *addr = LookupFCAddress(cfg);
        if (addr) {
          fcAddresses.insert(std::make_pair(cfg, *addr));
          delete addr;
        }
      }
    }

    replicaAddressesInitialized = true;
  }
};

#endif  // LIB_TRANSPORTCOMMON_H_
