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
#include <bits/stdint-uintn.h>
#include <gflags/gflags.h>

#include <string>
#include <utility>

#include "lib/configuration.h"
#include "lib/tcptransport.h"
#include "store/benchmark/bench_client.h"
#include "store/benchmark/tpcc/tpcc_client.h"
#include "store/benchmark/tpcc/trace_client.h"

DEFINE_int32(tpcc_num_warehouses, 10, "number of warehouses (for tpcc)");
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

DEFINE_string(data_file_path, "",
              "path to file containing key-value pairs to be loaded");

DEFINE_uint64(num_txns, 1, "number of txns");
int main(int argc, char *argv[]) {
  gflags::SetUsageMessage("generates a trace of a TPC-C experiment\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  TCPTransport tport(0.0, 0.0, 0, false, 0, 1, false, false);
  TraceClient tclient(&tport, FLAGS_data_file_path);
  BenchClient *bclient = new tpcc::TPCCClient(
      &tclient, tport, 0, FLAGS_num_txns, 0, 0, 0, 0, 0,
      FLAGS_tpcc_num_warehouses, FLAGS_tpcc_w_id != 0, FLAGS_tpcc_C_c_id,
      FLAGS_tpcc_C_c_last, FLAGS_tpcc_new_order_ratio,
      FLAGS_tpcc_delivery_ratio, FLAGS_tpcc_payment_ratio,
      FLAGS_tpcc_order_status_ratio, FLAGS_tpcc_stock_level_ratio,
      static_cast<uint32_t>(FLAGS_tpcc_static_w_id),
      static_cast<uint32_t>(FLAGS_tpcc_user_aborts),
      static_cast<uint32_t>(FLAGS_tpcc_distributed_txns), 0, false, false,
      (-1) != 0, false, "");

  auto bdcb = [&]() { tport.Stop(); };
  tport.Timer(0, [bclient, bdcb]() { bclient->Start(bdcb); });

  tport.Run();
  return 0;
}
