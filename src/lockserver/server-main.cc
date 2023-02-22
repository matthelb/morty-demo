// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lockserver/server-main.cc:
 *   Main entrypoint for lockserver server.
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

#include <bits/getopt_core.h>
#include <stdio.h>
#include <stdlib.h>

#include <iosfwd>

#include "lib/configuration.h"
#include "lib/udptransport.h"
#include "lockserver/server.h"
#include "replication/ir/replica.h"

int main(int argc, char **argv) {
  int index = -1;
  const char *configPath = nullptr;

  // Parse arguments
  int opt;
  char *strtolPtr;
  while ((opt = getopt(argc, argv, "c:i:")) != -1) {
    switch (opt) {
      case 'c':
        configPath = optarg;
        break;

      case 'i':
        index = strtol(optarg, &strtolPtr, 10);
        if ((*optarg == '\0') || (*strtolPtr != '\0') || (index < 0)) {
          fprintf(stderr, "option -i requires a numeric arg\n");
        }
        break;

      default:
        fprintf(stderr, "Unknown argument %s\n", argv[optind]);
    }
  }

  if (configPath == nullptr) {
    fprintf(stderr, "option -c is required\n");
    return EXIT_FAILURE;
  }

  if (index == -1) {
    fprintf(stderr, "option -i is required\n");
    return EXIT_FAILURE;
  }

  // Load configuration
  std::ifstream configStream(configPath);
  if (configStream.fail()) {
    fprintf(stderr, "unable to read configuration file: %s\n", configPath);
    return EXIT_FAILURE;
  }
  transport::Configuration config(configStream);

  if (index >= config.n) {
    fprintf(stderr,
            "replica index %d is out of bounds; "
            "only %d replicas defined\n",
            index, config.n);
    return EXIT_FAILURE;
  }

  UDPTransport transport(0.0, 0.0, 0);

  lockserver::LockServer server;
  replication::ir::IRReplica replica(config, 0, index, &transport, &server);

  transport.Run();

  return EXIT_SUCCESS;
}
