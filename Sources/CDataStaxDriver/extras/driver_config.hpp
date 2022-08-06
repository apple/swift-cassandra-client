//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Cassandra Client open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift Cassandra Client project authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of Swift Cassandra Client project authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

#ifndef DATASTAX_INTERNAL_DRIVER_CONFIG_HPP
#define DATASTAX_INTERNAL_DRIVER_CONFIG_HPP

/* #undef HAVE_KERBEROS */
#define HAVE_OPENSSL
#define HAVE_STD_ATOMIC
/* #undef HAVE_BOOST_ATOMIC */

#ifdef __APPLE__
#define HAVE_NOSIGPIPE
#endif

/* #undef HAVE_SIGTIMEDWAIT */
/* #undef HASH_IN_TR1 */
#define HAVE_BUILTIN_BSWAP32
#define HAVE_BUILTIN_BSWAP64
/* #undef HAVE_ARC4RANDOM */
/* #undef HAVE_GETRANDOM */
/* #undef HAVE_TIMERFD */
#undef HAVE_ZLIB

#endif
