// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * message.h:
 *   logging functions
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 * Copyright 2009-2012 Massachusetts Institute of Technology
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

#ifndef LIB_MESSAGE_H_
#define LIB_MESSAGE_H_

#include <atomic>
#include <cstdarg>
#include <cstdio>

enum Message_Type {
  MSG_PANIC = 0,
  MSG_WARNING,
  MSG_NOTICE,
  MSG_DEBUG,
  MSG_NUM_TYPES,
  MSG_PERROR = 1 << 16,
};

#define PanicFlags(flags, msg...)                                   \
  do {                                                              \
    _Message((Message_Type)(MSG_PANIC | flags), __FILE__, __LINE__, \
             static_cast<const char *>(__func__), msg);             \
    _Panic();                                                       \
  } while (0)
#define MessageFlags(flags, msg...) \
  _Message(flags, __FILE__, __LINE__, static_cast<const char *>(__func__), msg)

#define Panic(msg...) PanicFlags(0, msg)
#define Warning(msg...) MessageFlags(MSG_WARNING, msg)
#define Notice(msg...) MessageFlags(MSG_NOTICE, msg)
#define QNotice(msg...) _Message(MSG_NOTICE, NULL, 0, NULL, msg)

#define PPanic(msg...) PanicFlags(MSG_PERROR, msg)
#define PWarning(msg...) \
  MessageFlags((Message_Type)(MSG_WARNING | MSG_PERROR), msg)
#define PNotice(msg...) \
  MessageFlags((Message_Type)(MSG_NOTICE | MSG_PERROR), msg)

void _Message(enum Message_Type type, const char *fname, int line,
              const char *func, const char *fmt, ...)
    __attribute__((format(printf, 5, 6)));
void Message_VA(enum Message_Type type, const char *fname, int line,
                const char *func, const char *fmt, va_list args);

void _Message_VA(enum Message_Type type, FILE *fp, const char *fname, int line,
                 const char *func, const char *fmt, va_list args);

void _Panic() __attribute__((noreturn));
void Backtrace();
bool _Message_DebugEnabled(const char *fname);

const char *Message_DFree(char *buf);
void Message_DoFrees();

void _Message_Hexdump(const void *data, int len);
char *Message_FmtBlob(const void *data, int len);
void PanicOnSignal(int signo);

// This is not a mistake.  We actually want exactly one of these flags
// per file that uses the Debug macro.
static __attribute__((unused)) std::atomic_schar _Message_FileDebugFlag =
    -1;  // signed char

#define Debug(msg...)                                                 \
  do {                                                                \
    if (Message_DebugEnabled(__FILE__)) MessageFlags(MSG_DEBUG, msg); \
  } while (0)
#define Message_Hexdump(data, len)                                   \
  do {                                                               \
    if (Message_DebugEnabled(__FILE__)) _Message_Hexdump(data, len); \
  } while (0)
#ifndef NUW_ASSERT
#define UW_Assert(pred)                                 \
  do {                                                  \
    if (!(pred)) Panic("Assertion `%s' failed", #pred); \
  } while (0)
#else
#define UW_Assert(pred)
#endif

static inline bool Message_DebugEnabled(const char *fname) {
  signed char flag = _Message_FileDebugFlag.load();
  if (flag < 0) {
    _Message_FileDebugFlag.compare_exchange_strong(
        flag, _Message_DebugEnabled(fname));
  }

  return _Message_FileDebugFlag;

  // if (_Message_FileDebugFlag >= 0)
  //         return _Message_FileDebugFlag;
  // _Message_FileDebugFlag = _Message_DebugEnabled(fname);
  // return _Message_FileDebugFlag;
}

#include "lib/hash.h"

#define FMT_BLOB "<%ld %08x>"
#define VA_BLOB(d, l) static_cast<int32_t>(l), hash(d, l, 0)
#define VA_BLOB_STRING(d) \
  static_cast<int32_t>(d.size()), hash(d.c_str(), d.size(), 0)

#define FMT_VBLOB "%s"
#define XVA_VBLOB(d, l) Message_DFree(Message_FmtBlob(d, l))
#define XVA_VBLOB_STRING(d) Message_DFree(Message_FmtBlob(d.c_str(), d.size()))

#endif  // LIB_MESSAGE_H_
