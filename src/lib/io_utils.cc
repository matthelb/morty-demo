// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * lib/io_utils.cc:
 *   Reusable functions for basic I/O.
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

#include "lib/io_utils.h"

#include <bits/stdint-uintn.h>

#include <algorithm>

thread_local char _buffer[1024];
thread_local uint32_t _num_bytes;
thread_local uint32_t _amount;

int ReadBytesFromStream(std::istream *is, std::string *value) {
  is->read(reinterpret_cast<char *>(&_num_bytes), sizeof(uint32_t));
  if (*is) {
    while (!is->eof() && _num_bytes > 0) {
      _amount = std::min(_num_bytes, static_cast<uint32_t>(sizeof(_buffer)));
      is->read(_buffer, _amount);
      value->append(_buffer, _amount);
      _num_bytes -= _amount;
    }
  }
  return *is ? 0 : -1;
}

int WriteBytesToStream(std::ostream *os, const std::string &value) {
  uint32_t length = value.length();
  os->write(reinterpret_cast<char *>(&length), sizeof(uint32_t));
  os->write(value.c_str(), value.length());
  return sizeof(uint32_t) + value.length();
}
