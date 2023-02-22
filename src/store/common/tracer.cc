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
 * tracer.cc:
 *   latency tracing functions
 *
 **********************************************************************/

#include "store/common/tracer.h"

#include <bits/types/struct_timeval.h>
#include <stdio.h>
#include <sys/time.h>

using namespace std;

void Trace_Init(const string &name, Request_Trace *trace) {
  trace->is_tracing = false;
  trace->start_time = -1;
  trace->curr_stage = -1;
  trace->max_stage = 0;
  for (unsigned int &i : trace->stage) {
    i = 0;
  }
  trace->n_traces = 0;

  // Add to mapping.
  trace_map[name] = trace;
}

void Trace_Flush(const string &name) {
  Request_Trace *trace = trace_map[name];

  fprintf(stderr, "## Latency Stats for %s\n", name.c_str());
  fprintf(stderr, "## Number of samples: %u\n", trace->n_traces);
  fprintf(stderr, "## Number of stages: %u\n", trace->max_stage);
  fprintf(stderr, "## %s Stage Breakdown:", name.c_str());
  for (int i = 0; i < trace->max_stage; i++) {
    trace->stage[i] = trace->stage[i] / trace->n_traces;
    if (i > 0) {
      fprintf(stderr, " %u", trace->stage[i] - trace->stage[i - 1]);
    } else {
      fprintf(stderr, " %u", trace->stage[i]);
    }
  }
  fprintf(stderr, "\n");
}

void Trace_Start(const string &name) {
  Request_Trace *trace = trace_map[name];

  trace->is_tracing = true;

  struct timeval t {};
  gettimeofday(&t, nullptr);
  trace->start_time = t.tv_sec * 1000000 + t.tv_usec;

  trace->curr_stage = 0;
}

void Trace_Save(const string &name, const uint32_t index) {
  Request_Trace *trace = trace_map[name];

  struct timeval t {};
  gettimeofday(&t, nullptr);

  trace->stage[trace->curr_stage] +=
      (t.tv_sec * 1000000 + t.tv_usec) - trace->start_time;

  trace->curr_stage++;

  if (index > 0 && index != trace->curr_stage) {
    fprintf(stderr, "Mismatch index on %u, %u\n", index, trace->curr_stage);
  }
}

void Trace_Stop(const string &name) {
  Request_Trace *trace = trace_map[name];

  struct timeval t {};
  gettimeofday(&t, nullptr);

  trace->stage[trace->curr_stage] +=
      (t.tv_sec * 1000000 + t.tv_usec) - trace->start_time;

  if (trace->curr_stage >= trace->max_stage) {
    trace->max_stage = trace->curr_stage + 1;
  }
  trace->n_traces++;
  trace->is_tracing = false;
}
