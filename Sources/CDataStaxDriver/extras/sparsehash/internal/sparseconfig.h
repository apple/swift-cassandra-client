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

#define HASH_FUN_H  <functional>

/* the namespace of the hash<> function */
#define HASH_NAMESPACE  std

#define HASH_NAME  hash

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H  1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H  1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H  1

/* Define to 1 if the system has the type `long long'. */
#define HAVE_LONG_LONG  1

/* Define to 1 if you have the `memcpy' function. */
#define HAVE_MEMCPY  1

/* Define to 1 if the system has the type `uint16_t'. */
#define HAVE_UINT16_T 1

/* Define to 1 if the system has the type `u_int16_t'. */
#define HAVE_U_INT16_T 1

/* Define to 1 if the system has the type `__uint16'. */
/* #undef HAVE___UINT16 */

/* The system-provided hash function including the namespace. */
#define SPARSEHASH_HASH  HASH_NAMESPACE::HASH_NAME

/* The system-provided hash function, in namespace HASH_NAMESPACE. */
#define SPARSEHASH_HASH_NO_NAMESPACE  HASH_NAME

/* Namespace for Google classes */
#define GOOGLE_NAMESPACE  ::sparsehash

/* Stops putting the code inside the Google namespace */
#define _END_GOOGLE_NAMESPACE_  }

/* Puts following code inside the Google namespace */
#define _START_GOOGLE_NAMESPACE_   namespace sparsehash {
