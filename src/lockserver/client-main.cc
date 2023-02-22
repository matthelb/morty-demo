// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lockserver/client-main.cc:
 *   Main entrypoint for lockserver client.
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
#include <string.h>
#include <strings.h>

#include <iosfwd>
#include <string>
#include <thread>  // NOLINT[build/c++11]

#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "lockserver/client.h"

namespace {

void usage() {
  printf("Unknown command.. Try again!\n");
  printf("Usage: exit | q | lock <key> | unlock <key>\n");
}

}  // namespace

int main(int argc, char **argv) {
  const char *configPath = nullptr;

  // Parse arguments
  int opt;
  while ((opt = getopt(argc, argv, "c:")) != -1) {
    switch (opt) {
      case 'c':
        configPath = optarg;
        break;

      default:
        fprintf(stderr, "Unknown argument %s\n", argv[optind]);
    }
  }

  if (configPath == nullptr) {
    fprintf(stderr, "option -c is required\n");
    return EXIT_FAILURE;
  }

  // Load configuration
  std::ifstream configStream(configPath);
  if (configStream.fail()) {
    Panic("Unable to read configuration file: %s\n", configPath);
  }
  transport::Configuration config(configStream);

  // Create lock client.
  UDPTransport transport(0.0, 0.0, 0);
  lockserver::LockClient locker(&transport, config);
  std::thread run_transport([&transport]() { transport.Run(); });

  char c, cmd[2048], *tok;
  int clen, status;
  string key, value;

  while (true) {
    printf(">> ");
    fflush(stdout);

    clen = 0;
    while ((c = getchar()) != '\n') {
      cmd[clen++] = c;
    }
    cmd[clen] = '\0';

    char *saveptr;
    tok = strtok_r(cmd, " ,.-", &saveptr);
    if (tok == nullptr) {
      continue;
    }

    if (strcasecmp(tok, "exit") == 0 || strcasecmp(tok, "q") == 0) {
      printf("Exiting..\n");
      break;
    }
    if (strcasecmp(tok, "lock") == 0) {
      tok = strtok_r(nullptr, " ,.-", &saveptr);
      if (tok == nullptr) {
        usage();
        continue;
      }
      key = string(tok);
      status = static_cast<int>(locker.lock(key));

      if (status != 0) {
        printf("Lock Successful\n");
      } else {
        printf("Failed to acquire lock..\n");
      }
    } else if (strcasecmp(tok, "unlock") == 0) {
      tok = strtok_r(nullptr, " ,.-", &saveptr);
      if (tok == nullptr) {
        usage();
        continue;
      }
      key = string(tok);
      locker.unlock(key);
      printf("Unlock Successful\n");
    } else {
      usage();
    }
    fflush(stdout);
  }

  transport.Stop();
  run_transport.join();
  return EXIT_SUCCESS;
}
