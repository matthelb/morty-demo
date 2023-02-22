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
/**
 * static_block.h
 *
 * An implementation of a Java-style static block, in C++ (and potentially a
 * GCC/clang extension to avoid warnings). Almost, but not quite, valid C.
 * Partially inspired by Andrei Alexandrescu's Scope Guard and
 * discussions on stackoverflow.com
 *
 * By Eyal Rozenberg <eyalroz@technion.ac.il>
 *
 * Licensed under the Apache License v2.0:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 */
#ifndef LIB_STATIC_BLOCK_H_
#define LIB_STATIC_BLOCK_H_

#ifndef CONCATENATE
#define CONCATENATE(s1, s2) s1##s2
#define EXPAND_THEN_CONCATENATE(s1, s2) CONCATENATE(s1, s2)
#endif /* CONCATENATE */

#ifndef UNIQUE_IDENTIFIER
/**
 * This macro expands into a different identifier in every expansion.
 * Note that you _can_ clash with an invocation of UNIQUE_IDENTIFIER
 * by manually using the same identifier elsewhere; or by carefully
 * choosing another prefix etc.
 */
#ifdef __COUNTER__
#define UNIQUE_IDENTIFIER(prefix) EXPAND_THEN_CONCATENATE(prefix, __COUNTER__)
#else
#define UNIQUE_IDENTIFIER(prefix) EXPAND_THEN_CONCATENATE(prefix, __LINE__)
#endif /* COUNTER */
#else
#endif /* UNIQUE_IDENTIFIER */

/**
 * Following is a mechanism for executing code statically.
 *
 * @note Caveats:
 * - Your static block must be surround by curly braces.
 * - No need for a semicolon after the block (but it won't hurt).
 * - Do not put static blocks in files, as it might get compiled multiple
 *   times ane execute multiple times.
 * - A static_block can only be used in file scope - not within any other block
 * etc.
 * - Templated static blocks will probably not work. Avoid them.
 * - No other funny business, this is fragile.
 * - This does not having any threading issues (AFAICT) - as it has no static
 *   initialization order issue. Of course, you have to _keep_ it safe with
 *   your static code.
 * - Execution of the code is guaranteed to occur before main() executes,
 *   but the relative order of statics being initialized is unknown/unclear. So,
 *   do not call any method of an instance of a class which you expect to have
 * been constructed; it may not have been. Instead, you can use a static
 * getInstance() method (look this idiom up on the web, it's safe).
 * - Variables defined within the static block are not global; they will
 *   go out of scope as soon as its execution concludes.
 *
 * Usage example:
 *
 *   static_block {
 *         do_stuff();
 *         std::cout << "in the static block!\n";
 *   }
 *
 */
#define static_block STATIC_BLOCK_IMPL1(UNIQUE_IDENTIFIER(_static_block_))

#define STATIC_BLOCK_IMPL1(prefix) \
  STATIC_BLOCK_IMPL2(CONCATENATE(prefix, _fn), CONCATENATE(prefix, _var))

#define STATIC_BLOCK_IMPL2(function_name, var_name)                 \
  static void function_name();                                      \
  static int var_name __attribute((unused)) = (function_name(), 0); \
  static void function_name()  // NOLINT[readability/fn_size]

#endif  // LIB_STATIC_BLOCK_H_
