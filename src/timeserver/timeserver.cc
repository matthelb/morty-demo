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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * timeserver/timeserver.cc:
 *   Single TimeStamp Server.
 *
 **********************************************************************/

#include "timeserver/timeserver.h"

#include <bits/getopt_core.h>
#include <stdio.h>
#include <stdlib.h>

#include <iosfwd>

#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/replica.h"

TimeStampServer::TimeStampServer() { ts = 0; }

TimeStampServer::~TimeStampServer() = default;

string TimeStampServer::newTimeStamp() {
  ts++;
  return std::to_string(ts);
}

void TimeStampServer::ReplicaUpcall(opnum_t opnum, const string &str1,
                                    string &str2) {
  Debug("Received Upcall: %lu, %s", opnum, str1.c_str());

  // Get a new timestamp from the TimeStampServer
  str2 = newTimeStamp();
}

static void Usage(const char *progName) {
  fprintf(stderr, "usage: %s -c conf-file -i replica-index\n", progName);
  exit(1);
}

int main(int argc, char **argv) {
  int index = -1;
  const char *configPath = nullptr;
  char *strtolPtr;

  // Parse arguments
  int opt;
  while ((opt = getopt(argc, argv, "c:i:")) != -1) {
    switch (opt) {
      case 'c':
        configPath = optarg;
        break;

      case 'i':
        index = strtoul(optarg, &strtolPtr, 10);
        if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0)) {
          fprintf(stderr, "option -i requires a numeric arg\n");
          Usage(argv[0]);
        }
        break;

      default:
        fprintf(stderr, "Unknown argument %s\n", argv[optind]);
        break;
    }
  }

  if (configPath == nullptr) {
    fprintf(stderr, "option -c is required\n");
    Usage(argv[0]);
  }

  if (index == -1) {
    fprintf(stderr, "option -i is required\n");
    Usage(argv[0]);
  }

  // Load configuration
  std::ifstream configStream(configPath);
  if (configStream.fail()) {
    fprintf(stderr, "unable to read configuration file: %s\n", configPath);
    Usage(argv[0]);
  }
  transport::Configuration config(configStream);

  if (index >= config.n) {
    fprintf(stderr,
            "replica index %d is out of bounds; "
            "only %d replicas defined\n",
            index, config.n);
    Usage(argv[0]);
  }

  UDPTransport transport(0.0, 0.0, 0);

  TimeStampServer server;
  replication::vr::VRReplica replica(config, 0, index, &transport, 1, &server);

  transport.Run();

  return 0;
}
