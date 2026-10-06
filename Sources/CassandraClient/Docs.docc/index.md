# ``CassandraClient``

A Cassandra client in Swift.

## Overview

`CassandraClient` is a Cassandra client in Swift. The client is based on [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver) wrapping it with Swift friendly APIs and data structures.

`CassandraClient` exposes a Swift concurrency based API.

## Getting started

### Creating a client

For work with a bounded scope, ``CassandraClient/withClient(eventLoopGroup:configuration:_:)`` creates a client, runs it
for the duration of the closure, and shuts it down when the closure returns or throws:

```swift
  let configuration = CassandraClient.Configuration(...) // Or use CassandraClient.Configuration(configReader:)
  try await CassandraClient.withClient(configuration: configuration) { client in
    let result = try await client.query(...)
  }
```

To own a client for longer, create it with ``CassandraClient/init(eventLoopGroup:configuration:)`` and run
``CassandraClient/run()``, for example by adding the client to a swift-service-lifecycle `ServiceGroup`. `run()` shuts
the client down on graceful shutdown or when its task is cancelled. A client created this way must be shut down when no
longer needed, either by ending `run()` or with ``CassandraClient/shutdownAsync()``.

The client connects lazily, on its first request, and queries the configured keyspace.

### Querying another keyspace

Qualify the table name with its keyspace:

```swift
  let result = try await client.query("select * from other_keyspace.table ...")
```

If most queries target another keyspace, create a second client with that keyspace configured. Each client keeps its
own connections, so prefer qualified names where they suffice.

### Running result-less commands, e.g. insert, update, delete or DDL

```swift
  try await client.execute("create table ...")
```

### Running queries returning small data-sets that fit in-memory

Returning a model object, having `Model: Codable`:

```swift
  let result: [Model] = try await client.query("select * from table ...")
```

Or using free-form transformations on the row:

```swift
  let values = try await client.query("select * from table ...") { row in
    row.column("column_name").int32
  }
```

### Running queries returning large data-sets that do not fit in-memory

```swift
  // rows is a sequence that one needs to iterate on
  let rows: Rows = try await client.query("select * from table ...")
```

## TLS

TLS is off by default. To turn it on, set `ssl` on the configuration and give it the PEM-encoded certificates to trust:

```swift
  var configuration = CassandraClient.Configuration(...)
  var ssl = CassandraClient.Configuration.SSL()
  ssl.trustedCertificates = [certificate]
  configuration.ssl = ssl
```

### If you are upgrading

As of 0.13.0 the client verifies both the certificate chain and the server's identity, so a configuration that worked before can start failing two ways. A certificate that doesn't name the address the client connects to fails with `sslIdentityMismatch`, "Peer certificate subject name does not match". `trustedCertificates` left unset fails with `sslInvalidPeerCert` and an X509 reason such as "unable to get local issuer certificate". Also, `verifyFlag`'s `.default` case has been removed; use `.noHostnameVerification` (named `.peerCert` before 1.0) for the previous behavior. A configuration file setting `ssl.verifyFlag` to `"default"` now throws when it is read, rather than being accepted.

### What is verified

For every option except `none`, certificates are checked against `trustedCertificates` only. The driver never falls back to the system trust store, so leaving it unset makes verification fail.

By default (`ipAddressVerification`) the client also checks that the certificate belongs to the node it connected to, by matching that node's IP address against an `iPAddress` subject alternative name. For the node reached through a contact point, that is the contact point's resolved address. For nodes discovered from the cluster, it is their `system.peers` `rpc_address`, which need not be a configured contact point.

To match hostnames instead, set `fullVerification` and turn on `hostnameResolution`, which is what lets the driver work out a hostname per node. Note that `ssl` is a struct, so it has to be assigned back to the configuration after being changed:

```swift
  ssl.certificateVerification = .fullVerification
  configuration.ssl = ssl
  configuration.hostnameResolution = true
```

Setting `fullVerification` without `hostnameResolution` throws when the cluster is built, rather than failing later on every connection.

Hostname matching uses the name reverse DNS returns for each node's address, not the hostname configured as a contact point. The driver resolves contact points to addresses before it connects, so the string you configured is never the one matched, and each node's certificate has to carry the name its address reverse-resolves to. A node with no PTR record resolves to its own numeric address, which then fails the subject match and reports the same "Peer certificate subject name does not match" as a certificate naming the wrong address. Check what reverse DNS returns for each node before changing any certificates.

`noHostnameVerification` checks the certificate is valid but not which host it belongs to, and accepts a certificate issued for any host. `none` accepts any certificate at all. The client logs a warning when it connects with either.
