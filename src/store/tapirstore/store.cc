// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/store.cc:
 *   Key-value store with support for transactions using TAPIR.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#include "store/tapirstore/store.h"

#include <cstdint>
#include <type_traits>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common.h"
#include "store/common/stats.h"

namespace tapirstore {

using namespace std;

Store::Store(bool linearizable) : linearizable(linearizable) {
  char wkeyC[5];
  wkeyC[0] = static_cast<char>(0);
  *reinterpret_cast<uint32_t *>(wkeyC + 1) = 1;
  std::string warehouseKey(wkeyC, sizeof(wkeyC));
  conflict_windows[warehouseKey] = new Latency_t();
  _Latency_Init(conflict_windows[warehouseKey], "warehouse_1_conflict_window");
  Latency_Start(conflict_windows[warehouseKey]);
}

Store::~Store() {
  for (uint64_t i = 0; i <= 10; ++i) {
    std::string key(reinterpret_cast<char *>(&i), sizeof(i));
    if (prepared_time.find(key) != prepared_time.end()) {
      Notice("Dumping prepare time for key %lu.", i);
      Latency_Dump(prepared_time[key]);
      delete prepared_time[key];
    }
  }
  char wkeyC[5];
  wkeyC[0] = static_cast<char>(0);
  *reinterpret_cast<uint32_t *>(wkeyC + 1) = 1;
  std::string warehouseKey(wkeyC, sizeof(wkeyC));
  if (prepared_time.find(warehouseKey) != prepared_time.end()) {
    Notice("Dumping prepare time for warehouse 1.");
    Latency_Dump(prepared_time[warehouseKey]);
    delete prepared_time[warehouseKey];
  }
  if (conflict_windows.find(warehouseKey) != conflict_windows.end()) {
    Notice("Dumping conflict windows for warehouse 1.");
    Latency_Dump(conflict_windows[warehouseKey]);
    delete conflict_windows[warehouseKey];
  }
  char dkeyC[9];
  dkeyC[0] = static_cast<char>(1);
  *reinterpret_cast<uint32_t *>(dkeyC + 1) = 1;
  *reinterpret_cast<uint32_t *>(dkeyC + 5) = 1;
  std::string districtKey(dkeyC, sizeof(dkeyC));
  if (prepared_time.find(districtKey) != prepared_time.end()) {
    Notice("Dumping prepare time for warehouse 1 district 1.");
    Latency_Dump(prepared_time[districtKey]);
    delete prepared_time[districtKey];
  }
}

int Store::Get(uint64_t id, const string &key, pair<Timestamp, string> &value) {
  Debug("[%lu] GET %s", id, BytesToHex(key, 16).c_str());
  bool ret = store.get(key, value);
  if (ret) {
    Debug("Value at %lu.%lu", value.first.getTimestamp(), value.first.getID());
    return REPLY_OK;
  }
  return REPLY_FAIL;
}

int Store::Get(uint64_t id, const string &key, const Timestamp &timestamp,
               pair<Timestamp, string> &value,
               std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] GET %s at %lu.%lu", id, BytesToHex(key, 16).c_str(),
        timestamp.getTimestamp(), timestamp.getID());

  bool ret = store.get(key, timestamp, value);
  if (ret) {
    return REPLY_OK;
  }
  return REPLY_FAIL;
}

int Store::Prepare(uint64_t id, const Transaction &txn,
                   const Timestamp &timestamp, Timestamp &proposedTimestamp) {
  Debug("[%lu] PREPARE with ts %lu.%lu", id, timestamp.getTimestamp(),
        timestamp.getID());

  if (prepared.find(id) != prepared.end()) {
    if (prepared[id].first == timestamp) {
      Warning("[%lu] Already Prepared!", id);
      return REPLY_OK;
    }
    // run the checks again for a new timestamp
    Cleanup(id);
  }

  if (ongoing.find(id) == ongoing.end()) {
    ongoing[id] = const_cast<Transaction &>(txn);
  }

  // do OCC checks
  // unordered_map<string, set<Timestamp>> pReads;
  // GetPreparedReads(pReads);
  // unordered_map<string, set<Timestamp>> preparedWrites;
  // GetPreparedWrites(preparedWrites);

  // check for conflicts with the read set
  for (auto &read : txn.getReadSet()) {
    pair<Timestamp, Timestamp> range;
    bool ret = store.getRange(read.first, read.second, range);

    Debug("Range %s %lu %lu %lu", BytesToHex(read.first, 16).c_str(),
          read.second.getTimestamp(), range.first.getTimestamp(),
          range.second.getTimestamp());

    // if we don't have this key then no conflicts for read
    if (!ret) {
      continue;
    }

    // if we don't have this version then no conflicts for read
    if (range.first != read.second) {
      continue;
    }

    // if the value is still valid
    if (!range.second.isValid()) {
      // check pending writes.
      auto prepared_writes_itr = prepared_writes.find(read.first);
      if (prepared_writes_itr != prepared_writes.end() &&
          (linearizable || prepared_writes_itr->second.upper_bound(timestamp) !=
                               prepared_writes_itr->second.begin())) {
        Debug("[%lu] ABSTAIN wr conflict w/ prepared key: %s", id,
              BytesToHex(read.first, 16).c_str());
        stats.Increment("cc_abstains", 1);
        stats.Increment("cc_abstains_wr_conflict", 1);
        return REPLY_ABSTAIN;
      }
    } else if (linearizable || timestamp > range.second) {
      /* if value is not still valid, if we are running linearizable, then
       * abort. Else check validity range. if proposed timestamp not within
       * validity range, then conflict and abort
       */
      /*if (timestamp <= range.first) {
        Warning("timestamp %lu <= range.first %lu (range.second %lu)",
            timestamp.getTimestamp(), range.first.getTimestamp(),
            range.second.getTimestamp());
      }*/
      // UW_ASSERT(timestamp > range.first);
      /*std::string s = std::to_string((uint32_t) read.first[0]);
      if (read.first.length() >= 5) {
        s += "," + std::to_string(*reinterpret_cast<const uint32_t*>(
            read.first.c_str() + 1));
      }
      if (read.first.length() >= 9) {
        s += "," + std::to_string(*reinterpret_cast<const uint32_t*>(
            read.first.c_str() + 5));
      }
      if (read.first.length() >= 13) {
        s += "," + std::to_string(*reinterpret_cast<const uint32_t*>(
            read.first.c_str() + 9));
      }
      if (read.first.length() >= 17) {
        s += "," + std::to_string(*reinterpret_cast<const uint32_t*>(
            read.first.c_str() + 13));
      }
      stats.Increment("cc_aborts_" + s, 1);*/
      Debug("[%lu] ABORT wr conflict on key %s: %lu > %lu", id,
            BytesToHex(read.first, 16).c_str(), timestamp.getTimestamp(),
            range.second.getTimestamp());
      stats.Increment("cc_aborts", 1);
      stats.Increment("cc_aborts_wr_conflict", 1);
      return REPLY_FAIL;
    } else {
      /* there may be a pending write in the past.  check
       * pending writes again.  If proposed transaction is
       * earlier, abstain
       */
      auto prepared_writes_itr = prepared_writes.find(read.first);
      if (prepared_writes_itr != prepared_writes.end()) {
        for (auto &writeTime : prepared_writes_itr->second) {
          if (writeTime > range.first && writeTime < timestamp) {
            Debug("[%lu] ABSTAIN wr conflict w/ prepared key: %s", id,
                  BytesToHex(read.first, 16).c_str());
            stats.Increment("cc_abstains", 1);
            stats.Increment("cc_abstains_wr_conflict", 1);
            return REPLY_ABSTAIN;
          }
        }
      }
    }
  }

  // check for conflicts with the write set
  for (auto &write : txn.getWriteSet()) {
    pair<Timestamp, string> val;
    // if this key is in the store
    if (store.get(write.first, val)) {
      Timestamp lastRead;
      bool ret;

      // if the last committed write is bigger than the timestamp,
      // then can't accept in linearizable
      if (linearizable && val.first > timestamp) {
        Debug("[%lu] RETRY ww conflict w/ prepared key: %s", id,
              BytesToHex(write.first, 16).c_str());
        proposedTimestamp = val.first;
        stats.Increment("cc_retries", 1);
        stats.Increment("cc_retries_ww_conflict", 1);
        return REPLY_RETRY;
      }

      // if last committed read is bigger than the timestamp, can't
      // accept this transaction, but can propose a retry timestamp

      // if linearizable mode, then we get the timestamp of the last
      // read ever on this object
      if (linearizable) {
        ret = store.getLastRead(write.first, lastRead);
      } else {
        // otherwise, we get the last read for the version that is being written
        ret = store.getLastRead(write.first, timestamp, lastRead);
      }

      // if this key is in the store and has been read before
      if (ret && lastRead > timestamp) {
        Debug("[%lu] RETRY rw conflict w/ prepared key: %s", id,
              BytesToHex(write.first, 16).c_str());
        proposedTimestamp = lastRead;
        stats.Increment("cc_retries", 1);
        stats.Increment("cc_retries_rw_conflict", 1);
        return REPLY_RETRY;
      }
    }

    // if there is a pending write for this key, greater than the
    // proposed timestamp, retry
    if (linearizable) {
      auto prepared_writes_itr = prepared_writes.find(write.first);
      if (prepared_writes_itr != prepared_writes.end()) {
        auto it = prepared_writes_itr->second.upper_bound(timestamp);
        if (it != prepared_writes_itr->second.end()) {
          Debug("[%lu] RETRY ww conflict w/ prepared key: %s", id,
                write.first.c_str());
          proposedTimestamp = *it;
          stats.Increment("cc_retries", 1);
          stats.Increment("cc_retries_ww_conflict", 1);
          return REPLY_RETRY;
        }
      }
    }

    // if there is a pending read for this key, greater than the
    // propsed timestamp, abstain
    auto prepared_reads_itr = prepared_reads.find(write.first);
    if (prepared_reads_itr != prepared_reads.end() &&
        prepared_reads_itr->second.upper_bound(timestamp) !=
            prepared_reads_itr->second.end()) {
      Debug("[%lu] ABSTAIN rw conflict w/ prepared key: %s", id,
            BytesToHex(write.first, 16).c_str());
      stats.Increment("cc_abstains", 1);
      stats.Increment("cc_abstains_rw_conflict", 1);
      return REPLY_ABSTAIN;
    }
  }

  // Otherwise, prepare this transaction for commit
  prepared[id] = make_pair(timestamp, &ongoing[id]);

  for (auto &read : txn.getReadSet()) {
    prepared_reads[read.first].insert(timestamp);
  }
  for (auto &write : txn.getWriteSet()) {
    prepared_writes[write.first].insert(timestamp);
    if (prepared_time.find(write.first) != prepared_time.end()) {
      Latency_Start(prepared_time[write.first]);
    }
  }

  Debug("[%lu] PREPARED TO COMMIT", id);

  return REPLY_OK;
}

void Store::Commit(uint64_t id, const Timestamp &timestamp,
                   std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] COMMIT at ts %lu.%lu", id, timestamp.getTimestamp(),
        timestamp.getID());

  const auto &t = ongoing[id];

  Commit(timestamp, t);

  Cleanup(id);
}

void Store::Commit(const Timestamp &timestamp, const Transaction &txn) {
  // updated timestamp of last committed read for the read set
  for (auto &read : txn.getReadSet()) {
    store.commitGet(read.first,   // key
                    read.second,  // timestamp of read version
                    timestamp);   // commit timestamp
  }

  // insert writes into versioned key-value store
  for (auto &write : txn.getWriteSet()) {
    Debug("Committing write to key %s %lu.%lu.",
          BytesToHex(write.first, 16).c_str(), timestamp.getTimestamp(),
          timestamp.getID());
    store.put(write.first,   // key
              write.second,  // value
              timestamp);    // timestamp
    if (conflict_windows.find(write.first) != conflict_windows.end()) {
      Latency_End(conflict_windows[write.first]);
      Latency_Start(conflict_windows[write.first]);
    }
  }
}

void Store::Abort(uint64_t id, const Transaction &txn,
                  std::unordered_map<uint64_t, int> &statuses) {
  Debug("[%lu] ABORT", id);

  Cleanup(id);
}

void Store::Load(const string &key, const string &value,
                 const Timestamp &timestamp) {
  store.put(key, value, timestamp);
  // const std::string &key_(key);
  /*if (*reinterpret_cast<uint64_t*>(&key_[0]) <= 10 ||
        (key.size() >=3 && key[0] == 0 && key[1] == 1) ||
        (key.size() >=3 && key[0] == 1 && key[1] == 1 && key[5] == 1)) {
    prepared_time[key] = new Latency_t();
    _Latency_Init(prepared_time[key], "prepared_time");
  }*/
}

void Store::GetPreparedWrites(unordered_map<string, set<Timestamp>> &writes) {
  // gather up the set of all writes that are currently prepared
  for (auto &t : prepared) {
    for (auto &write : t.second.second->getWriteSet()) {
      writes[write.first].insert(t.second.first);
    }
  }
}

void Store::GetPreparedReads(unordered_map<string, set<Timestamp>> &reads) {
  debug_counter++;
  if (debug_counter % 5000 == 0) {
    Notice("Prepared size: %lu.", prepared.size());
  }
  // gather up the set of all writes that are currently prepared
  for (auto &t : prepared) {
    for (auto &read : t.second.second->getReadSet()) {
      reads[read.first].insert(t.second.first);
    }
  }
}

void Store::Cleanup(uint64_t txnId) {
  Debug("Removing txn %lu from prepared and ongoing.", txnId);

  auto prepared_itr = prepared.find(txnId);
  if (prepared_itr != prepared.end()) {
    for (auto &read : prepared_itr->second.second->getReadSet()) {
      auto prepared_reads_itr = prepared_reads.find(read.first);
      UW_ASSERT(prepared_reads_itr != prepared_reads.end());
      prepared_reads_itr->second.erase(prepared_itr->second.first);
      if (prepared_reads_itr->second.empty()) {
        prepared_reads.erase(prepared_reads_itr);
      }
    }
    for (auto &write : prepared_itr->second.second->getWriteSet()) {
      auto prepared_writes_itr = prepared_writes.find(write.first);
      UW_ASSERT(prepared_writes_itr != prepared_writes.end());
      prepared_writes_itr->second.erase(prepared_itr->second.first);
      if (prepared_time.find(write.first) != prepared_time.end()) {
        Latency_End(prepared_time[write.first]);
      }
      if (prepared_writes_itr->second.empty()) {
        prepared_writes.erase(prepared_writes_itr);
      }
    }

    prepared.erase(prepared_itr);
  }
  ongoing.erase(txnId);
}

}  // namespace tapirstore
