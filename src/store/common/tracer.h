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
 * tracer.h:
 *   latency tracing functions
 *
 **********************************************************************/

#ifndef _LIB_TRACER_H_
#define _LIB_TRACER_H_

#include <bits/stdint-uintn.h>
#include <sys/time.h>

#include <map>
#include <string>

/* Trace details of an individual type of request. */
typedef struct Request_Trace {
  bool is_tracing;
  uint64_t start_time;
  uint8_t curr_stage;
  uint8_t max_stage;
  uint32_t stage[16];
  uint32_t n_traces;
} Request_Trace;

/* Mapping of names to request trace data structure. */
static std::map<std::string, Request_Trace *> trace_map;

void Trace_Init(const std::string &name, Request_Trace *trace);

void Trace_Flush(const std::string &name);

void Trace_Start(const std::string &name);

void Trace_Save(const std::string &name, uint32_t index = 0);

void Trace_Stop(const std::string &name);

#endif  // _LIB_TRACER_H_
