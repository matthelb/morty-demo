// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
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

#include "store/server.h"

#include <bits/stdint-uintn.h>
#include <bits/types/struct_rusage.h>
#include <bits/types/struct_timespec.h>
#include <bits/types/struct_timeval.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <valgrind/callgrind.h>

#include <csignal>
#include <exception>
#include <iostream>
#include <random>
#include <utility>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/tcptransport.h"
#include "lib/transport.h"
#include "lib/udptransport.h"
#include "replication/common/replica.h"
#include "replication/ir/replica.h"
#include "replication/vr/replica.h"
#include "store/common/backend/loader.h"
#include "store/common/partitioner.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"
#include "store/janusstore/server.h"
#include "store/mortystorev2/server.h"
#include "store/spannerstore/common.h"
#include "store/spannerstore/server.h"
#include "store/spannerstore/truetime.h"
#include "store/strongstore/common.h"
#include "store/strongstore/server.h"
#include "store/strongstore/unreplicatedserver.h"
#include "store/tapirstore/server.h"
#include "store/weakstore/server.h"

enum protocol_t {
  PROTO_UNKNOWN,
  PROTO_TAPIR,
  PROTO_WEAK,
  PROTO_STRONG,
  PROTO_JANUS,
  PROTO_MORTY_CONTEXT,
  PROTO_SPANNER
};

enum transmode_t {
  TRANS_UNKNOWN,
  TRANS_UDP,
  TRANS_TCP,
};

/**
 * System settings.
 */
DEFINE_string(config_path, "", "path to shard configuration file");
DEFINE_string(replica_config_path, "", "path to replica configuration file");
DEFINE_uint64(replica_idx, 0, "index of replica in shard configuration file");
DEFINE_uint64(group_idx, 0, "index of the group to which this replica belongs");
DEFINE_uint64(num_groups, 1, "number of replica groups in the system");
DEFINE_uint64(num_shards, 1, "number of shards in the system");
DEFINE_bool(debug_stats, false, "record stats related to debugging");
DEFINE_uint64(measure_cpu_usage_period, 0,
              "time in between cpu measurements (0"
              " for no measurements)");
DEFINE_uint64(num_cores_per_numa_node, 10, "number of CPU cores per NUMA node");
DEFINE_uint64(num_numa_nodes, 2, "number of NUMA nodes");

DEFINE_bool(rw_or_retwis, true, "true for rw, false for retwis");
const char *protocol_args[] = {"tapir", "weak",          "strong",
                               "janus", "morty-context", "spanner"};
const protocol_t protos[]{PROTO_TAPIR, PROTO_WEAK,          PROTO_STRONG,
                          PROTO_JANUS, PROTO_MORTY_CONTEXT, PROTO_SPANNER};
static bool ValidateProtocol(const char *flagname, const std::string &value) {
  int n = sizeof(protocol_args);
  for (int i = 0; i < n; ++i) {
    if (value == protocol_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(protocol, protocol_args[0],
              "the protocol to use during this"
              " experiment");
DEFINE_validator(protocol, &ValidateProtocol);

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

const char *partitioner_args[] = {"default", "int", "warehouse_dist_items",
                                  "warehouse", "warehouse_fine_grain"};
const partitioner_t parts[]{DEFAULT, INT, WAREHOUSE_DIST_ITEMS, WAREHOUSE,
                            WAREHOUSE_FINE_GRAIN};
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
 * TPCC settings.
 */
DEFINE_int32(tpcc_num_warehouses, 1, "number of warehouses (for tpcc)");

/**
 * TAPIR settings.
 */
DEFINE_bool(tapir_linearizable, true, "run TAPIR in linearizable mode");

/**
 * StrongStore settings.
 */
const char *strongmode_args[] = {"lock", "occ", "span-lock", "span-occ",
                                 "mvtso"};
const strongstore::Mode strongmodes[]{
    strongstore::Mode::MODE_LOCK, strongstore::Mode::MODE_OCC,
    strongstore::Mode::MODE_SPAN_LOCK, strongstore::Mode::MODE_SPAN_OCC,
    strongstore::Mode::MODE_MVTSO};
static bool ValidateStrongMode(const char *flagname, const std::string &value) {
  int n = sizeof(strongmode_args);
  for (int i = 0; i < n; ++i) {
    if (value == strongmode_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(strongmode, strongmode_args[0],
              "the protocol to use during this"
              " experiment");
DEFINE_validator(strongmode, &ValidateStrongMode);
DEFINE_int64(strong_max_dep_depth, -1,
             "maximum length of dependency chain"
             " [-1 is no maximum] (for StrongStore MVTSO)");
DEFINE_bool(strong_unreplicated, false,
            "use unreplicated client/server to"
            " avoid overhead from replication (for StrongStore)");

/**
 * Morty settings.
 */
DEFINE_uint64(prepare_batch_period, 0,
              "length of batches for deterministic prepare message"
              " processing.");
DEFINE_bool(morty_branch, true,
            "send speculative read replies for uncommitted values.");
DEFINE_uint64(morty_prepare_delay_ms, 0,
              "number of milliseconds to delay sending"
              " prepare replies (for Morty)");
DEFINE_uint64(morty_num_workers, 0,
              "number of worker threads to use for"
              " parallel handling of messages");
DEFINE_bool(morty_pipeline_commit, false,
            "pipeline prepares and commits"
            " (for Morty)");
DEFINE_bool(morty_reexecution_enabled, true,
            "allow coordinator to reexecute"
            " transactions when learning of new conflicts (for Morty");
DEFINE_uint64(
    morty_gc_watermark_buffer_us, 200,
    "gc_watermark periodically"
    " advances to server's local time minus this buffer in microseconds (for"
    " Morty)");

/**
 * Experiment settings.
 */
DEFINE_int32(clock_skew, 0, "difference between real clock and TrueTime");
DEFINE_int32(clock_error, 0, "maximum error for clock");
DEFINE_string(stats_file, "", "path to file for server stats");

/**
 * Benchmark settings.
 */
DEFINE_string(keys_path, "", "path to file containing keys in the system");
DEFINE_uint64(num_keys, 0, "number of keys to generate");
DEFINE_string(data_file_path, "",
              "path to file containing key-value pairs to be loaded");
DEFINE_bool(preload_keys, false, "load keys into server if generating keys");
DEFINE_uint64(data_load_multi_thread, 0,
              "load data with multiple"
              " threads");

Server *server = nullptr;
TransportReceiver *replica = nullptr;
std::vector<::Transport *> tport;
Partitioner *part = nullptr;
struct rusage cpu_usage_start;
struct rusage cpu_usage_now;
struct timespec cpu_measure_time_start;
struct timespec cpu_measure_time_now;
struct timespec cpu_process_time_start;
struct timespec cpu_process_time_now;

void Cleanup(int signal);
void Backtrace(int signal);
void GetRUsage();

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
      "runs a replica for a distributed replicated transaction\n"
      "           processing system.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  Notice("Starting server.");

  // parse configuration
  std::ifstream configStream(FLAGS_config_path);
  if (configStream.fail()) {
    std::cerr << "Unable to read configuration file: " << FLAGS_config_path
              << std::endl;
  }

  // parse protocol and mode
  protocol_t proto = PROTO_UNKNOWN;
  int numProtos = sizeof(protocol_args);
  for (int i = 0; i < numProtos; ++i) {
    if (FLAGS_protocol == protocol_args[i]) {
      proto = protos[i];
      break;
    }
  }

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

  transport::Configuration config(configStream);

  if (FLAGS_replica_idx >= static_cast<uint64_t>(config.n)) {
    std::cerr << "Replica index " << FLAGS_replica_idx
              << " is out of bounds"
                 "; only "
              << config.n << " replicas defined" << std::endl;
  }

  if (proto == PROTO_UNKNOWN) {
    std::cerr << "Unknown protocol." << std::endl;
    return 1;
  }

  strongstore::Mode strongMode = strongstore::Mode::MODE_UNKNOWN;
  if (proto == PROTO_STRONG) {
    int numStrongModes = sizeof(strongmode_args);
    for (int i = 0; i < numStrongModes; ++i) {
      if (FLAGS_strongmode == strongmode_args[i]) {
        strongMode = strongmodes[i];
        break;
      }
    }
  }

  size_t num_transports = 1;
  if (proto == PROTO_MORTY_CONTEXT) {
    num_transports = 1 + FLAGS_morty_num_workers;
  }

  for (size_t i = 0; i < num_transports; ++i) {
    switch (trans) {
      case TRANS_TCP:
        tport.push_back(new TCPTransport(0.0, 0.0, 0, false, 0, 1, true, true));
        // TODO(matthelb): add: process_id + total processes (max_grpid/
        // machines (= servers/n))
        break;
      case TRANS_UDP:
        tport.push_back(new UDPTransport(0.0, 0.0, 0, nullptr));
        break;
      default:
        NOT_REACHABLE();
    }
  }

  // parse protocol and mode
  partitioner_t partType = DEFAULT;
  int numParts = sizeof(partitioner_args);
  for (int i = 0; i < numParts; ++i) {
    if (FLAGS_partitioner == partitioner_args[i]) {
      partType = parts[i];
      break;
    }
  }

  std::mt19937 unused;
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
      part = new WarehousePartitioner(FLAGS_tpcc_num_warehouses, unused);
      break;
    case WAREHOUSE_FINE_GRAIN:
      part =
          new WarehouseFineGrainPartitioner(FLAGS_tpcc_num_warehouses, unused);
      break;
    default:
      NOT_REACHABLE();
  }

  switch (proto) {
    case PROTO_TAPIR: {
      server = new tapirstore::Server(FLAGS_tapir_linearizable);
      replica = new replication::ir::IRReplica(
          config, FLAGS_group_idx, FLAGS_replica_idx, tport[0],
          dynamic_cast<replication::ir::IRAppReplica *>(server));
      break;
    }
    case PROTO_SPANNER: {
      // parse replica configuration
      std::ifstream rc_stream{FLAGS_replica_config_path};
      if (rc_stream.fail()) {
        std::cerr << "Unable to read configuration file: "
                  << FLAGS_replica_config_path << std::endl;
      }
      const transport::Configuration replica_config{rc_stream};

      spannerstore::TrueTime tt{static_cast<std::uint64_t>(FLAGS_clock_error)};
      auto server_id = FLAGS_num_shards * FLAGS_group_idx + FLAGS_replica_idx;
      server = new spannerstore::Server(
          spannerstore::Consistency::SS, config, replica_config, server_id,
          FLAGS_group_idx, FLAGS_replica_idx, tport[0], tt, FLAGS_debug_stats);
      replica = new replication::vr::VRReplica(
          replica_config, FLAGS_group_idx, FLAGS_replica_idx, tport[0], 1,
          dynamic_cast<replication::AppReplica *>(server));
      break;
    }
    case PROTO_WEAK: {
      server = new weakstore::Server(config, FLAGS_group_idx, FLAGS_replica_idx,
                                     tport[0]);
      break;
    }
    case PROTO_STRONG: {
      if (FLAGS_strong_unreplicated ||
          strongMode == strongstore::Mode::MODE_MVTSO) {
        server = new strongstore::UnreplicatedServer(
            strongMode, tport[0], config, FLAGS_group_idx,
            FLAGS_strong_max_dep_depth);
      } else {
        server = new strongstore::Server(strongMode, FLAGS_clock_skew,
                                         FLAGS_clock_error);
        replica = new replication::vr::VRReplica(
            config, FLAGS_group_idx, FLAGS_replica_idx, tport[0], 1,
            dynamic_cast<replication::AppReplica *>(server));
      }
      break;
    }
    case PROTO_JANUS: {
      server = new janusstore::Server(config, FLAGS_group_idx,
                                      FLAGS_replica_idx, tport[0]);
      break;
    }
    case PROTO_MORTY_CONTEXT: {
      server = new mortystorev2::Server(
          config, FLAGS_group_idx, FLAGS_replica_idx, tport,
          FLAGS_morty_num_workers, FLAGS_morty_pipeline_commit,
          FLAGS_morty_reexecution_enabled, FLAGS_morty_gc_watermark_buffer_us,
          FLAGS_num_cores_per_numa_node, FLAGS_num_numa_nodes);
      break;
    }
    default: { NOT_REACHABLE(); }
  }

  CALLGRIND_START_INSTRUMENTATION;
  // parse keys
  std::vector<std::string> keys;
  if (FLAGS_data_file_path.empty() && FLAGS_keys_path.empty()) {
    // TODO(matthelb): only do if it is RW workload
    if (FLAGS_num_keys > 0) {
      if (FLAGS_preload_keys) {
        uint64_t zero = 0;
        std::vector<int> txnGroups;
        for (uint64_t i = 0; i < FLAGS_num_keys; ++i) {
          std::string key(reinterpret_cast<const char *>(&i), sizeof(i));
          std::string val(reinterpret_cast<const char *>(&zero), sizeof(zero));
          Timestamp ts(0, 0);
          uint64_t shard =
              (*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups);
          if (shard % FLAGS_num_groups == FLAGS_group_idx) {
            server->Load(std::move(key), std::move(val), std::move(ts));
          }
          if (i % 10000 == 0) {
            Debug("Loaded key %lu to shard %lu", i, shard);
          }
        }
      }
      /*size_t loaded = 0;
size_t stored = 0;
      std::vector<int> txnGroups;
      for (size_t i = 0; i < FLAGS_num_keys; ++i) {
              //TODO add partition. Figure out how client key partitioning is
done.. std::string key; key = std::to_string(i); std::string value;
              if(FLAGS_rw_or_retwis){
                value = std::move(std::string(100, '\0')); //turn the size into
a flag
        }
              else{
                      value = std::to_string(i);
              }

              if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups) %
FLAGS_num_groups == FLAGS_group_idx) { server->Load(key, value, Timestamp());
                      ++stored;
              }
              ++loaded;
      }
      Notice("Created and Stored %lu out of %lu key-value pairs", stored,
loaded);
*/
    }
  } else if (FLAGS_data_file_path.length() > 0 && FLAGS_keys_path.empty()) {
    Loader loader(server, part, FLAGS_num_shards, FLAGS_num_groups,
                  FLAGS_group_idx, FLAGS_data_load_multi_thread);
    if (loader.Load(FLAGS_data_file_path) == 1) {
      std::cerr << "Could not read data from: " << FLAGS_data_file_path
                << std::endl;
      return 1;
    }
  } else {
    std::ifstream in;
    in.open(FLAGS_keys_path);
    if (!in) {
      std::cerr << "Could not read keys from: " << FLAGS_keys_path << std::endl;
      return 1;
    }
    std::string key;
    std::vector<int> txnGroups;
    while (std::getline(in, key)) {
      std::string val;
      if ((*part)(key, FLAGS_num_shards, FLAGS_group_idx, txnGroups) %
              FLAGS_num_groups ==
          FLAGS_group_idx) {
        server->Load(std::move(key), std::move(val),
                     std::move(Timestamp(0, 0)));
      }
    }
    in.close();
  }
  Notice("Done loading server.");

  std::signal(SIGKILL, Cleanup);
  std::signal(SIGTERM, Cleanup);
  std::signal(SIGINT, Cleanup);
  std::signal(SIGSEGV, Backtrace);
  std::set_terminate(Backtrace);

  if (FLAGS_measure_cpu_usage_period > 0) {
    getrusage(RUSAGE_SELF, &cpu_usage_start);
    clock_gettime(CLOCK_REALTIME, &cpu_measure_time_start);
    tport[0]->Timer(FLAGS_measure_cpu_usage_period * 1000, &GetRUsage);
  }

  // event_enable_debug_logging(EVENT_DBG_ALL);

  tport[0]->Run();
  CALLGRIND_STOP_INSTRUMENTATION;
  // CALLGRIND_DUMP_STATS;

  if (!FLAGS_stats_file.empty()) {
    Notice("Exporting stats to %s.", FLAGS_stats_file.c_str());
    server->GetStats().ExportJSON(FLAGS_stats_file);
  }
  delete server;

  return 0;
}

void Cleanup(int signal) {
  Notice("Gracefully exiting after signal %d.", signal);
  tport[0]->StopLoop();
  /*if (FLAGS_stats_file.size() > 0) {
    Notice("Exporting stats to %s.", FLAGS_stats_file.c_str());
    server->GetStats().ExportJSON(FLAGS_stats_file);
  }
  delete server;
  exit(0);*/
}

void Backtrace(int signal) { Panic("SIGSEGV %d.", signal); }

void GetRUsage() {
  int ret = getrusage(RUSAGE_SELF, &cpu_usage_now);
  if (ret != 0) {
    PWarning("Failed to getrusage().");
    return;
  }

  ret = clock_gettime(CLOCK_REALTIME, &cpu_measure_time_now);
  if (ret != 0) {
    PWarning("Failed to clock_gettime().");
    return;
  }

  size_t cpu_measure_time_delta;
  cpu_measure_time_delta =
      cpu_measure_time_now.tv_sec - cpu_measure_time_start.tv_sec;
  cpu_measure_time_delta *= 1000000000LL;
  if (cpu_measure_time_now.tv_nsec < cpu_measure_time_start.tv_nsec) {
    cpu_measure_time_delta -= 1000000000LL;
    cpu_measure_time_delta += (cpu_measure_time_now.tv_nsec + 1000000000LL) -
                              cpu_measure_time_start.tv_nsec;
  } else {
    cpu_measure_time_delta +=
        cpu_measure_time_now.tv_nsec - cpu_measure_time_start.tv_nsec;
  }
  // convert from ns to us
  cpu_measure_time_delta = cpu_measure_time_delta / 1000LL;

  size_t cpu_usage_user_delta;
  cpu_usage_user_delta =
      cpu_usage_now.ru_utime.tv_sec - cpu_usage_start.ru_utime.tv_sec;
  cpu_usage_user_delta *= 1000000LL;
  if (cpu_usage_now.ru_utime.tv_usec < cpu_usage_start.ru_utime.tv_usec) {
    cpu_usage_user_delta -= 1000000LL;
    cpu_usage_user_delta += (cpu_usage_now.ru_utime.tv_usec + 1000000LL) -
                            cpu_usage_start.ru_utime.tv_usec;
  } else {
    cpu_usage_user_delta +=
        cpu_usage_now.ru_utime.tv_usec - cpu_usage_start.ru_utime.tv_usec;
  }

  size_t cpu_usage_sys_delta;
  cpu_usage_sys_delta =
      cpu_usage_now.ru_stime.tv_sec - cpu_usage_start.ru_stime.tv_sec;
  cpu_usage_sys_delta *= 1000000LL;
  if (cpu_usage_now.ru_stime.tv_usec < cpu_usage_start.ru_stime.tv_usec) {
    cpu_usage_sys_delta -= 1000000LL;
    cpu_usage_sys_delta += (cpu_usage_now.ru_stime.tv_usec + 1000000LL) -
                           cpu_usage_start.ru_stime.tv_usec;
  } else {
    cpu_usage_sys_delta +=
        cpu_usage_now.ru_stime.tv_usec - cpu_usage_start.ru_stime.tv_usec;
  }

  server->GetStats().Add("cpu_usage_user",
                         cpu_usage_user_delta * 1000 / cpu_measure_time_delta);
  server->GetStats().Add("cpu_usage_sys",
                         cpu_usage_sys_delta * 1000 / cpu_measure_time_delta);
  server->GetStats().Add("cpu_usage_total",
                         (cpu_usage_user_delta + cpu_usage_sys_delta) * 1000 /
                             cpu_measure_time_delta);

  cpu_measure_time_start = cpu_measure_time_now;
  cpu_usage_start = cpu_usage_now;

  tport[0]->Timer(FLAGS_measure_cpu_usage_period * 1000, &GetRUsage);
}
