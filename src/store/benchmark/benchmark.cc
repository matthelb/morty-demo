// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/benchmark.cc
 *   Benchmarking program that measures performance of systems under various
 *   workloads.
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

#include <event2/thread.h>
#include <gflags/gflags.h>
#include <valgrind/callgrind.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <sstream>
#include <thread>  // NOLINT[build/c++11]
#include <vector>

#include "lib/latency.h"
#include "lib/tcptransport.h"
#include "lib/timeval.h"
#include "store/benchmark/bench_client.h"
#include "store/benchmark/common/key_selector.h"
#include "store/benchmark/common/uniform_key_selector.h"
#include "store/benchmark/common/zipf_key_selector.h"
#include "store/benchmark/retwis/retwis_client.h"
#include "store/benchmark/rw/rw_client.h"
#include "store/benchmark/smallbank/smallbank_client.h"
#include "store/benchmark/tpcc/tpcc_client.h"
#include "store/common/frontend/async_adapter_client.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/async_one_shot_adapter_client.h"
#include "store/common/frontend/one_shot_client.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/common/truetime.h"
#include "store/janusstore/client.h"
#include "store/mortystorev2/client.h"
#include "store/spannerstore/client.h"
#include "store/spannerstore/truetime.h"
#include "store/strongstore/client.h"
#include "store/tapirstore/client.h"
#include "store/weakstore/client.h"

enum protomode_t {
  PROTO_UNKNOWN,
  PROTO_TAPIR,
  PROTO_WEAK,
  PROTO_STRONG,
  PROTO_JANUS,
  PROTO_MORTY_CONTEXT,
  PROTO_SPANNER
};

enum benchmode_t {
  BENCH_UNKNOWN,
  BENCH_RETWIS,
  BENCH_TPCC,
  BENCH_SMALLBANK_SYNC,
  BENCH_RW,
  BENCH_TPCC_SYNC
};

enum keysmode_t { KEYS_UNKNOWN, KEYS_UNIFORM, KEYS_ZIPF };

enum transmode_t {
  TRANS_UNKNOWN,
  TRANS_UDP,
  TRANS_TCP,
};

/**
 * System settings.
 */
DEFINE_uint64(client_id, 0, "unique identifier for client");
DEFINE_string(config_path, "", "path to shard configuration file");
DEFINE_uint64(num_shards, 1, "number of shards in the system");
DEFINE_uint64(num_groups, 1, "number of replica groups in the system");
DEFINE_bool(ping_replicas, false, "determine latency to replicas via pings");

DEFINE_bool(strong_unreplicated, false,
            "use unreplicated client/server to"
            " avoid overhead from replication (for StrongStore)");

DEFINE_bool(morty_send_writes, true,
            "send uncommitted writes during execution"
            " (for Morty)");
DEFINE_uint64(morty_backoff, 0,
              "exponential factor for backing off reexecution"
              " (for Morty)");
DEFINE_uint64(morty_max_backoff, 0,
              "maximum backoff for a txn reexecution"
              " (for Morty)");
DEFINE_uint64(morty_spec_wait, 0,
              "milliseconds to delay committing non-speculative"
              " branch (for Morty)");
DEFINE_int64(morty_spec_execution_limit, -1,
             "limit of the number of speculative"
             " executions. -1 indicates no limit (for Morty)");
DEFINE_uint64(morty_prepare_delay_ms, 0,
              "number of milliseconds to delay sending"
              " prepare (for Morty)");
DEFINE_uint64(morty_commit_delay_ms, 0,
              "number of milliseconds to delay sending"
              " commit(for Morty)");
DEFINE_uint64(morty_prepare_timeout_ms, 100,
              "timeout for aborting a txn when not"
              " receiving prepare replies (for Morty)");
DEFINE_bool(morty_commit_single_branch, false,
            "send uncommitted writes during"
            " execution (for Morty)");
DEFINE_bool(morty_logical_timestamps, true,
            "use purely logical clocks for "
            " assigning txn timestamps (for Morty)");
DEFINE_bool(morty_fast_path_enabled, true,
            "allow coordinators to take the fast"
            " path when replicas agree on commit vote (for Morty");
DEFINE_uint64(morty_slow_path_timeout_ms, 500,
              "milliseconds to wait before"
              " invoking commit slow path (for Morty)");
DEFINE_bool(morty_pipeline_commit, false,
            "pipeline prepare-commit process"
            " (for Morty");
DEFINE_bool(morty_reexecution_enabled, true,
            "allow coordinator to reexecute"
            " transactions when learning of new conflicts (for Morty");
DEFINE_uint64(morty_num_workers, 0,
              "number of worker threads to use for"
              " parallel handling of messages");

/**
 * Spanner settings
 */
const char *coord_selection_args[] = {"min-shard", "min-key", "random"};
const spannerstore::CoordinatorSelection coord_selection[]{
    spannerstore::CoordinatorSelection::MIN_SHARD,
    spannerstore::CoordinatorSelection::MIN_KEY,
    spannerstore::CoordinatorSelection::RANDOM};
static bool ValidateCoordinatorSelection(const char *flagname,
                                         const std::string &value) {
  int n = sizeof(coord_selection_args);
  for (int i = 0; i < n; ++i) {
    if (value == coord_selection_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(spanner_coord_select, coord_selection_args[0],
              "how clients select coordinator shard (for Spanner)");
DEFINE_validator(spanner_coord_select, &ValidateCoordinatorSelection);

/**
 * TAPIR settings
 */
DEFINE_bool(tapir_sync_commit, false,
            "wait until commit phase completes before"
            " sending additional transactions (for TAPIR)");

/**
 * Other settings
 */
DEFINE_bool(debug_stats, false, "record stats related to debugging");

const char *trans_args[] = {"udp", "tcp"};

const transmode_t transmodes[]{TRANS_UDP, TRANS_TCP};
static bool ValidateTransMode(const char *flagname, const std::string &value) {
  int n = sizeof(trans_args);
  for (int i = 0; i < n; ++i) {
    if (value == trans_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(trans_protocol, trans_args[0],
              "transport protocol to use for"
              " passing messages");
DEFINE_validator(trans_protocol, &ValidateTransMode);

const char *protocol_args[] = {"txn-l", "txn-s",         "qw",        "occ",
                               "lock",  "span-occ",      "span-lock", "mvtso",
                               "janus", "morty-context", "spanner"};
const protomode_t protomodes[]{PROTO_TAPIR,         PROTO_TAPIR,  PROTO_WEAK,
                               PROTO_STRONG,        PROTO_STRONG, PROTO_STRONG,
                               PROTO_STRONG,        PROTO_STRONG, PROTO_JANUS,
                               PROTO_MORTY_CONTEXT, PROTO_SPANNER};
const strongstore::Mode strongmodes[]{
    strongstore::Mode::MODE_UNKNOWN,   strongstore::Mode::MODE_UNKNOWN,
    strongstore::Mode::MODE_UNKNOWN,   strongstore::Mode::MODE_OCC,
    strongstore::Mode::MODE_LOCK,      strongstore::Mode::MODE_SPAN_OCC,
    strongstore::Mode::MODE_SPAN_LOCK, strongstore::Mode::MODE_MVTSO,
    strongstore::Mode::MODE_UNKNOWN,   strongstore::Mode::MODE_UNKNOWN,
    strongstore::Mode::MODE_UNKNOWN};
static bool ValidateProtocolMode(const char *flagname,
                                 const std::string &value) {
  int n = sizeof(protocol_args);
  for (int i = 0; i < n; ++i) {
    if (value == protocol_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(protocol_mode, protocol_args[0],
              "the mode of the protocol to"
              " use during this experiment");
DEFINE_validator(protocol_mode, &ValidateProtocolMode);

const char *benchmark_args[] = {"retwis", "tpcc", "smallbank", "rw",
                                "tpcc-sync"};
const benchmode_t benchmodes[]{BENCH_RETWIS, BENCH_TPCC, BENCH_SMALLBANK_SYNC,
                               BENCH_RW, BENCH_TPCC_SYNC};
static bool ValidateBenchmark(const char *flagname, const std::string &value) {
  int n = sizeof(benchmark_args);
  for (int i = 0; i < n; ++i) {
    if (value == benchmark_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(benchmark, benchmark_args[0],
              "the mode of the protocol to use"
              " during this experiment");
DEFINE_validator(benchmark, &ValidateBenchmark);

/**
 * Experiment settings.
 */
DEFINE_uint64(exp_duration, 30, "duration (in seconds) of experiment");
DEFINE_uint64(warmup_secs, 5,
              "time (in seconds) to warm up system before"
              " recording stats");
DEFINE_uint64(cooldown_secs, 5,
              "time (in seconds) to cool down system after"
              " recording stats");
DEFINE_uint64(tput_interval, 0,
              "time (in seconds) between throughput"
              " measurements");
DEFINE_uint64(num_clients, 1, "number of clients to run in this process");
DEFINE_uint64(num_client_hosts, 1,
              "number of client processes across all nodes and servers");
DEFINE_uint64(num_requests, -1,
              "number of requests (transactions) per"
              " client");
DEFINE_int32(closest_replica, -1, "index of the replica closest to the client");
DEFINE_string(closest_replicas, "",
              "space-separated list of replica indices in"
              " order of proximity to client(s)");
DEFINE_uint64(delay, 0, "maximum time to wait between client operations");
DEFINE_int32(clock_skew, 0, "difference between real clock and TrueTime");
DEFINE_int32(clock_error, 0, "maximum error for clock");
DEFINE_string(stats_file, "", "path to output stats file.");
DEFINE_uint64(abort_backoff, 100,
              "sleep exponentially increasing amount after abort.");
DEFINE_bool(retry_aborted, true, "retry aborted transactions.");
DEFINE_int64(max_attempts, -1,
             "max number of attempts per transaction (or -1"
             " for unlimited).");
DEFINE_uint64(message_timeout, 10000, "length of timeout for messages in ms.");
DEFINE_uint64(max_backoff, 5000, "max time to sleep after aborting.");

const char *partitioner_args[] = {
    "default",
    "int",
    "warehouse_dist_items",
    "warehouse",
    "warehouse_fine_grain",
};
const partitioner_t parts[]{
    DEFAULT, INT, WAREHOUSE_DIST_ITEMS, WAREHOUSE, WAREHOUSE_FINE_GRAIN,
};
static bool ValidatePartitioner(const char *flagname,
                                const std::string &value) {
  int n = sizeof(partitioner_args);
  for (int i = 0; i < n; ++i) {
    if (value == partitioner_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(partitioner, partitioner_args[0],
              "the partitioner to use during this"
              " experiment");
DEFINE_validator(partitioner, &ValidatePartitioner);

/**
 * Retwis settings.
 */
DEFINE_string(keys_path, "",
              "path to file containing keys in the system"
              " (for retwis)");
DEFINE_uint64(num_keys, 0, "number of keys to generate (for retwis");

const char *keys_args[] = {"uniform", "zipf"};
const keysmode_t keysmodes[]{KEYS_UNIFORM, KEYS_ZIPF};
static bool ValidateKeys(const char *flagname, const std::string &value) {
  int n = sizeof(keys_args);
  for (int i = 0; i < n; ++i) {
    if (value == keys_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(key_selector, keys_args[0],
              "the distribution from which to "
              "select keys.");
DEFINE_validator(key_selector, &ValidateKeys);

DEFINE_double(zipf_coefficient, 0.5,
              "the coefficient of the zipf distribution "
              "for key selection.");
DEFINE_bool(retwis_single_shard, false, "only generate single shard txns.");

/**
 * RW settings.
 */
DEFINE_uint64(num_ops_txn, 1,
              "number of ops in each txn"
              " (for rw)");
// RW benchmark also uses same config parameters as Retwis.

/**
 * TPCC settings.
 */
DEFINE_int32(warehouse_per_shard, 1,
             "number of warehouses per shard"
             " (for tpcc)");
DEFINE_int32(clients_per_warehouse, 1,
             "number of clients per warehouse"
             " (for tpcc)");
DEFINE_int32(remote_item_milli_p, 0, "remote item milli p (for tpcc)");

DEFINE_int32(tpcc_num_warehouses, 1, "number of warehouses (for tpcc)");
DEFINE_int32(tpcc_w_id, 1, "home warehouse id for this client (for tpcc)");
DEFINE_int32(tpcc_C_c_id, 1,
             "C value for NURand() when selecting"
             " random customer id (for tpcc)");
DEFINE_int32(tpcc_C_c_last, 1,
             "C value for NURand() when selecting"
             " random customer last name (for tpcc)");
DEFINE_int32(tpcc_new_order_ratio, 45,
             "ratio of new_order transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_delivery_ratio, 4,
             "ratio of delivery transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_stock_level_ratio, 4,
             "ratio of stock_level transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_payment_ratio, 43,
             "ratio of payment transactions to other"
             " transaction types (for tpcc)");
DEFINE_int32(tpcc_order_status_ratio, 4,
             "ratio of order_status transactions to other"
             " transaction types (for tpcc)");
DEFINE_bool(tpcc_static_w_id, false,
            "force clients to use same w_id for each treansaction (for tpcc)");
DEFINE_bool(tpcc_user_aborts, false,
            "generate new order txns that user abort (for tpcc)");
DEFINE_bool(tpcc_distributed_txns, true,
            "generate txns that access remote warehouses (for tpcc)");

/**
 * Smallbank settings.
 */

DEFINE_int32(balance_ratio, 60,
             "percentage of balance transactions"
             " (for smallbank)");
DEFINE_int32(deposit_checking_ratio, 10,
             "percentage of deposit checking"
             " transactions (for smallbank)");
DEFINE_int32(transact_saving_ratio, 10,
             "percentage of transact saving"
             " transactions (for smallbank)");
DEFINE_int32(amalgamate_ratio, 10,
             "percentage of deposit checking"
             " transactions (for smallbank)");
DEFINE_int32(write_check_ratio, 10,
             "percentage of write check transactions"
             " (for smallbank)");
DEFINE_int32(num_hotspots, 1000, "# of hotspots (for smallbank)");
DEFINE_int32(num_customers, 18000, "# of customers (for smallbank)");
DEFINE_double(hotspot_probability, 0.9, "probability of ending in hotspot");
DEFINE_int32(timeout, 5000, "timeout in ms (for smallbank)");
DEFINE_string(customer_name_file_path, "smallbank_names",
              "path to file"
              " containing names to be loaded (for smallbank)");

DEFINE_LATENCY(op);

std::vector<::AsyncClient *> asyncClients;
std::vector<::SyncClient *> syncClients;
std::vector<::Client *> clients;
std::vector<::OneShotClient *> oneShotClients;
std::vector<::context::Client *> context_clients;
std::vector<::BenchClient *> benchClients;
std::vector<std::thread *> threads;
Transport *tport;
transport::Configuration *config;
Partitioner *part;
std::vector<KeySelector *> keySelectors;

void Signal(int signal);
void Cleanup();
void FlushStats();

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
      "executes transactions from various transactional workload\n"
      "           benchmarks against various distributed replicated "
      "transaction\n"
      "           processing systems.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse transport protocol
  transmode_t trans = TRANS_UNKNOWN;
  int numTransModes = sizeof(trans_args);
  for (int i = 0; i < numTransModes; ++i) {
    if (FLAGS_trans_protocol == trans_args[i]) {
      trans = transmodes[i];
      break;
    }
  }
  if (trans == TRANS_UNKNOWN) {
    std::cerr << "Unknown transport protocol." << std::endl;
    return 1;
  }

  // parse protocol and mode
  protomode_t mode = PROTO_UNKNOWN;
  strongstore::Mode strongmode = strongstore::Mode::MODE_UNKNOWN;
  int numProtoModes = sizeof(protocol_args);
  for (int i = 0; i < numProtoModes; ++i) {
    if (FLAGS_protocol_mode == protocol_args[i]) {
      mode = protomodes[i];
      if (mode == PROTO_STRONG) {
        strongmode = strongmodes[i];
      }
      break;
    }
  }
  if (mode == PROTO_UNKNOWN ||
      (mode == PROTO_STRONG && strongmode == strongstore::Mode::MODE_UNKNOWN)) {
    std::cerr << "Unknown protocol or unknown strongmode." << std::endl;
    return 1;
  }

  // parse benchmark
  benchmode_t benchMode = BENCH_UNKNOWN;
  int numBenchs = sizeof(benchmark_args);
  for (int i = 0; i < numBenchs; ++i) {
    if (FLAGS_benchmark == benchmark_args[i]) {
      benchMode = benchmodes[i];
      break;
    }
  }
  if (benchMode == BENCH_UNKNOWN) {
    std::cerr << "Unknown benchmark." << std::endl;
    return 1;
  }

  // parse partitioner
  partitioner_t partType = DEFAULT;
  int numParts = sizeof(partitioner_args);
  for (int i = 0; i < numParts; ++i) {
    if (FLAGS_partitioner == partitioner_args[i]) {
      partType = parts[i];
      break;
    }
  }

  // parse key selector
  keysmode_t keySelectionMode = KEYS_UNKNOWN;
  int numKeySelectionModes = sizeof(keys_args);
  for (int i = 0; i < numKeySelectionModes; ++i) {
    if (FLAGS_key_selector == keys_args[i]) {
      keySelectionMode = keysmodes[i];
      break;
    }
  }
  if (keySelectionMode == KEYS_UNKNOWN) {
    std::cerr << "Unknown key selector." << std::endl;
    return 1;
  }

  // parse coordinator selection
  spannerstore::CoordinatorSelection coord_select =
      spannerstore::CoordinatorSelection::MIN_SHARD;
  for (std::size_t i = 0; i < sizeof(coord_selection_args); ++i) {
    if (FLAGS_spanner_coord_select == coord_selection_args[i]) {
      coord_select = coord_selection[i];
      break;
    }
  }

  // parse closest replicas
  std::vector<int> closestReplicas;
  std::stringstream iss(FLAGS_closest_replicas);
  int replica;
  iss >> replica;
  while (!iss.fail()) {
    closestReplicas.push_back(replica);
    iss >> replica;
  }

  // parse retwis settings
  std::vector<std::string> keys;
  if (benchMode == BENCH_RETWIS || benchMode == BENCH_RW) {
    if (FLAGS_keys_path.empty()) {
      if (FLAGS_num_keys > 0) {
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
          keys.emplace_back(reinterpret_cast<const char *>(&i),
                            sizeof(uint64_t));
        }
      } else {
        std::cerr << "Specified neither keys file nor number of keys."
                  << std::endl;
        return 1;
      }
    } else {
      std::ifstream in;
      in.open(FLAGS_keys_path);
      if (!in) {
        std::cerr << "Could not read keys from: " << FLAGS_keys_path
                  << std::endl;
        return 1;
      }
      std::string key;
      while (std::getline(in, key)) {
        keys.push_back(key);
      }
      in.close();
    }
  }

  switch (trans) {
    case TRANS_TCP:
      tport = new TCPTransport(0.0, 0.0, 0, false, 0, 1, true, false);
      break;
    case TRANS_UDP:
      tport = new UDPTransport(0.0, 0.0, 0, nullptr);
      break;
    default:
      NOT_REACHABLE();
  }

  std::mt19937 rand(
      FLAGS_client_id);  // TODO(matthelb): is this safe to share rand across
                         //   multiple threads?

  switch (partType) {
    case DEFAULT:
      part = new DefaultPartitioner();
      break;
    case INT:
      part = new IntPartitioner(FLAGS_num_keys);
      break;
    case WAREHOUSE_DIST_ITEMS:
      part = new WarehouseDistItemsPartitioner(FLAGS_tpcc_num_warehouses);
      break;
    case WAREHOUSE:
      part = new WarehousePartitioner(FLAGS_tpcc_num_warehouses, rand);
      break;
    case WAREHOUSE_FINE_GRAIN:
      part = new WarehouseFineGrainPartitioner(FLAGS_tpcc_num_warehouses, rand);
      break;
    default:
      NOT_REACHABLE();
  }

  std::vector<std::vector<std::string>> sharded_keys;
  for (uint64_t i = 0; i < FLAGS_num_shards; ++i) {
    sharded_keys.emplace_back();
  }
  std::vector<int> empty_vec;
  for (const auto &key : keys) {
    auto j = (*part)(key, FLAGS_num_shards, -1, empty_vec);
    sharded_keys[j].push_back(key);
  }

  uint64_t num_key_selectors = 1UL;
  if (keySelectionMode == KEYS_UNIFORM) {
    num_key_selectors = FLAGS_num_shards;
  }
  for (uint64_t i = 0; i < num_key_selectors; ++i) {
    keySelectors.push_back(nullptr);
    switch (keySelectionMode) {
      case KEYS_UNIFORM:
        keySelectors[i] = new UniformKeySelector(sharded_keys[i]);
        break;
      case KEYS_ZIPF:
        keySelectors[i] =
            new ZipfKeySelector(sharded_keys[i], FLAGS_zipf_coefficient);
        break;
      default:
        NOT_REACHABLE();
    }
  }

  std::string latencyFile;
  std::string latencyRawFile;
  std::vector<uint64_t> latencies;
  std::atomic<size_t> clientsDone(0UL);

  bench_done_callback bdcb = [&clientsDone, &latencyFile, &latencyRawFile]() {
    ++clientsDone;
    Debug("%lu clients have finished.", clientsDone.load());
    if (clientsDone == FLAGS_num_clients) {
      Latency_t sum;
      _Latency_Init(&sum, "total");
      for (auto &benchClient : benchClients) {
        Latency_Sum(&sum, &benchClient->latency);
      }

      Latency_Dump(&sum);
      if (!latencyFile.empty()) {
        Latency_FlushTo(latencyFile.c_str());
      }

      latencyRawFile = latencyFile + ".raw";
      std::ofstream rawFile(latencyRawFile.c_str(),
                            std::ios::out | std::ios::binary);
      for (auto x : benchClients) {
        rawFile.write(reinterpret_cast<char *>(&x->latencies[0]),
                      (x->latencies.size() * sizeof(x->latencies[0])));
        if (!rawFile) {
          Warning("Failed to write raw latency output");
        }
      }
      tport->Stop();
    }
  };

  std::ifstream configStream(FLAGS_config_path);
  if (configStream.fail()) {
    std::cerr << "Unable to read configuration file: " << FLAGS_config_path
              << std::endl;
    return -1;
  }
  config = new transport::Configuration(configStream);

  if (!closestReplicas.empty() &&
      closestReplicas.size() != static_cast<size_t>(config->n)) {
    std::cerr << "If specifying closest replicas, must specify all "
              << config->n << "; only specified " << closestReplicas.size()
              << std::endl;
    return 1;
  }

  if (FLAGS_num_clients > (1 << 7)) {
    std::cerr << "Only support up to " << (1 << 7) << " clients in one process."
              << std::endl;
    return 1;
  }

  for (size_t i = 0; i < FLAGS_num_clients; i++) {
    Client *client = nullptr;
    ::context::Client *context_client = nullptr;
    AsyncClient *asyncClient = nullptr;
    SyncClient *syncClient = nullptr;
    OneShotClient *oneShotClient = nullptr;

    uint64_t clientId = FLAGS_client_id + i;
    switch (mode) {
      case PROTO_TAPIR: {
        context_client = new tapirstore::Client(
            config, clientId, FLAGS_num_shards, FLAGS_num_groups,
            FLAGS_closest_replica, tport, part, FLAGS_ping_replicas,
            FLAGS_tapir_sync_commit, trans == TRANS_UDP,
            TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
      }
      case PROTO_SPANNER: {
        spannerstore::TrueTime tt{
            static_cast<std::uint64_t>(FLAGS_clock_error)};
        context_client = new spannerstore::Client(
            config, clientId, FLAGS_num_shards, FLAGS_closest_replica, tport,
            part, tt, coord_select, FLAGS_debug_stats);
        break;
      }
      case PROTO_JANUS: {
        oneShotClient = new janusstore::Client(config, FLAGS_num_shards,
                                               FLAGS_closest_replica, tport);
        asyncClient = new AsyncOneShotAdapterClient(oneShotClient);
        break;
      }
      /*case MODE_WEAK: {
        protoClient = new weakstore::Client(configPath, nshards,
      closestReplica); break;
      }*/
      case PROTO_STRONG: {
        context_client = new strongstore::Client(
            strongmode, config, clientId, FLAGS_num_shards, FLAGS_num_groups,
            FLAGS_closest_replica, tport, part, FLAGS_strong_unreplicated,
            TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
      }
      case PROTO_MORTY_CONTEXT: {
        context_client = new mortystorev2::Client(
            config, clientId, FLAGS_num_shards, FLAGS_num_groups,
            FLAGS_morty_backoff, FLAGS_morty_max_backoff,
            FLAGS_morty_logical_timestamps, FLAGS_morty_fast_path_enabled,
            FLAGS_morty_pipeline_commit, FLAGS_morty_reexecution_enabled,
            FLAGS_morty_slow_path_timeout_ms, FLAGS_morty_num_workers,
            FLAGS_closest_replica, tport, part);
        break;
      }
      default:
        NOT_REACHABLE();
    }

    switch (benchMode) {
      case BENCH_RETWIS:
      case BENCH_TPCC:
      case BENCH_RW:
        if (asyncClient == nullptr && context_client == nullptr) {
          UW_ASSERT(client != nullptr);
          asyncClient = new AsyncAdapterClient(client, FLAGS_message_timeout);
        }
        break;
      case BENCH_SMALLBANK_SYNC:
      case BENCH_TPCC_SYNC:
        if (syncClient == nullptr) {
          UW_ASSERT(client != nullptr);
          syncClient = new SyncClient(client);
        }
        break;
      default:
        NOT_REACHABLE();
    }

    uint32_t seed = FLAGS_client_id + i;
    BenchClient *bench;
    switch (benchMode) {
      case BENCH_RW:
        UW_ASSERT(context_client != nullptr);
        bench = new rw::RWClient(
            keySelectors, FLAGS_num_ops_txn, context_client, *tport, seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff,
            FLAGS_max_attempts);
        break;
      case BENCH_RETWIS:
        UW_ASSERT(context_client != nullptr);
        bench = new retwis::RetwisClient(
            keySelectors, FLAGS_retwis_single_shard, nullptr, context_client,
            *tport, seed, FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff,
            FLAGS_max_attempts);
        break;
      case BENCH_TPCC:
        UW_ASSERT(context_client != nullptr);
        bench = new tpcc::TPCCClient(
            context_client, *tport, seed, FLAGS_num_requests,
            FLAGS_exp_duration, FLAGS_delay, FLAGS_warmup_secs,
            FLAGS_cooldown_secs, FLAGS_tput_interval, FLAGS_tpcc_num_warehouses,
            FLAGS_tpcc_w_id != 0, FLAGS_tpcc_C_c_id, FLAGS_tpcc_C_c_last,
            FLAGS_tpcc_new_order_ratio, FLAGS_tpcc_delivery_ratio,
            FLAGS_tpcc_payment_ratio, FLAGS_tpcc_order_status_ratio,
            FLAGS_tpcc_stock_level_ratio,
            static_cast<uint32_t>(FLAGS_tpcc_static_w_id),
            static_cast<uint32_t>(FLAGS_tpcc_user_aborts),
            static_cast<uint32_t>(FLAGS_tpcc_distributed_txns),
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff != 0u,
            FLAGS_max_attempts != 0);
        break;
      /*case BENCH_SMALLBANK_SYNC:
        UW_ASSERT(syncClient != nullptr);
        bench = new smallbank::SmallbankClient(
            *syncClient, *tport, seed, FLAGS_num_requests, FLAGS_exp_duration,
            FLAGS_delay, FLAGS_warmup_secs, FLAGS_cooldown_secs,
            FLAGS_tput_interval, FLAGS_abort_backoff, FLAGS_retry_aborted,
            FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_timeout,
            FLAGS_balance_ratio, FLAGS_deposit_checking_ratio,
            FLAGS_transact_saving_ratio, FLAGS_amalgamate_ratio,
            FLAGS_num_hotspots, FLAGS_num_customers - FLAGS_num_hotspots,
            FLAGS_hotspot_probability, FLAGS_customer_name_file_path);
        break;*/
      default:
        NOT_REACHABLE();
    }

    switch (benchMode) {
      case BENCH_RETWIS:
      case BENCH_TPCC:
      case BENCH_RW:
        // async benchmarks
        tport->Timer(0, [bench, bdcb]() { bench->Start(bdcb); });
        break;
      /*case BENCH_SMALLBANK_SYNC: {
        auto *syncBench = dynamic_cast<SyncTransactionBenchClient *>(bench);
        UW_ASSERT(syncBench != nullptr);
        threads.push_back(new std::thread([syncBench, bdcb]() {
          syncBench->Start([]() {});
          while (!syncBench->IsFullyDone()) {
            syncBench->StartLatency();
            transaction_status_t result;
            syncBench->SendNext(&result);
            syncBench->IncrementSent(result);
          }
          bdcb();
        }));
        break;
      }*/
      default:
        NOT_REACHABLE();
    }

    if (asyncClient != nullptr) {
      asyncClients.push_back(asyncClient);
    }
    if (client != nullptr) {
      clients.push_back(client);
    }
    if (oneShotClient != nullptr) {
      oneShotClients.push_back(oneShotClient);
    }
    if (context_client != nullptr) {
      context_clients.push_back(context_client);
    }
    benchClients.push_back(bench);
  }

  if (!threads.empty()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  tport->Timer(FLAGS_exp_duration * 1000 - 1000, FlushStats);
  // tport->Timer(FLAGS_exp_duration * 1000, Cleanup);

  std::signal(SIGKILL, Signal);
  std::signal(SIGTERM, Signal);
  std::signal(SIGINT, Signal);

  CALLGRIND_START_INSTRUMENTATION;
  tport->Run();
  CALLGRIND_STOP_INSTRUMENTATION;
  CALLGRIND_DUMP_STATS;

  Notice("Cleaning up after experiment.");

  FlushStats();

  delete config;
  for (auto i : keySelectors) {
    delete i;
  }
  for (auto i : threads) {
    i->join();
    delete i;
  }
  for (auto i : syncClients) {
    delete i;
  }
  for (auto i : oneShotClients) {
    delete i;
  }
  for (auto i : context_clients) {
    delete i;
  }
  for (auto i : asyncClients) {
    delete i;
  }
  for (auto i : clients) {
    delete i;
  }
  for (auto i : benchClients) {
    delete i;
  }

  delete tport;
  delete part;

  return 0;
}

void Signal(int signal) {
  Notice("Gracefully stopping bench clients after signal %d.", signal);
  tport->Stop();
  Cleanup();
}

void Cleanup() {
  // tport->Stop();
}

void FlushStats() {
  if (!FLAGS_stats_file.empty()) {
    Notice("Flushing stats to %s.", FLAGS_stats_file.c_str());
    Stats total;
    for (auto &benchClient : benchClients) {
      total.Merge(benchClient->GetStats());
    }
    for (auto &asyncClient : asyncClients) {
      total.Merge(asyncClient->GetStats());
    }
    for (auto &client : clients) {
      total.Merge(client->GetStats());
    }
    for (auto &context_client : context_clients) {
      total.Merge(context_client->GetStats());
    }

    total.ExportJSON(FLAGS_stats_file);
  }
}
