# ``CassandraClient``

A Cassandra client in Swift.

## Overview

`CassandraClient` is a Cassandra client in Swift. The client is based on [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver) wrapping it with Swift friendly APIs and data structures.

`CassandraClient` API currently exposes [SwiftNIO](https://github.com/apple/swift-nio) based futures to simplify integration with SwiftNIO based servers. Swift concurrency based API is also available.

## Getting started

### Creating a client instance

```swift
  let configuration = CassandraClient.Configuration(...) // Or use CassandraClient.Configuration(configReader:)
  let cassandraClient = CassandraClient(configuration: configuration)
```

The client has a default session established (lazily) so that it can be used directly to perform queries on the configured keyspace:

```swift
  cassandraClient.query(...)
```

The client must be explicitly shut down when no longer needed.

```swift
  cassandraClient.shutdown()
```

### Creating a session for a different keyspace

```swift
  let session = cassandraClient.makeSession(keyspace: "the_keyspace")
  session.query(...)
```

The session must be explicitly shut down when no longer needed.

```swift
  session.shutdown()
```

You can also create a session and pass in a closure, which will automatically release the resource when the closure exists:

```swift
  cassandraClient.withSession(keyspace: "<keyspace>") { session in
    session.query(...)
  }
```

### Running result-less commands, e.g. insert, update, delete or DDL

```swift
  cassandraClient.run("create table ...")
```

Or at a session level:

```swift
  session.run("create table ...")
```

### Running queries returning small data-sets that fit in-memory

Returning a model object, having `Model: Codable`:

```swift
  cassandraClient.query("select * from table ...").map { (result: [Model]) in
    ...
  }
```

```swift
  session.query("select * from table ...").map { (result: [Model]) in
    ...
  }
```

Or using free-form transformations on the row:

```swift
  cassandraClient.query("select * from table ...") { row in
    row.column("column_name").int32
  }.map { value in
    ...
  }
```

```swift
  session.query("select * from table ...") { row in
    row.column("column_name").int32
  }.map { value in
    ...
  }
```

### Running queries returning large data-sets that do not fit in-memory

```swift
  cassandraClient.query("select * from table ...").map { (rows: Rows) in
    // rows is a sequence that one needs to iterate on
    rows.map { row in
      ...
    }
  }
```

```swift
  session.query("select * from table ...").map { (rows: Rows) in
    // rows is a sequence that one needs to iterate on
    rows.map { row in
      ...
    }
  }
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

As of 0.13.0 the client verifies both the certificate chain and the server's identity, so a configuration that worked before can start failing two ways. A certificate that doesn't name the address the client connects to fails with `sslIdentityMismatch`, "Peer certificate subject name does not match". `trustedCertificates` left unset fails with `sslInvalidPeerCert` and an X509 reason such as "unable to get local issuer certificate". Also, `verifyFlag`'s `.default` case has been removed; use `.peerCert` for the previous behavior.

### What is verified

For every option except `none`, certificates are checked against `trustedCertificates` only. The driver never falls back to the system trust store, so leaving it unset makes verification fail.

By default (`peerIdentity`) the client also checks that the certificate belongs to the node it connected to, by matching that node's IP address against an `iPAddress` subject alternative name. For the node reached through a contact point, that is the contact point's resolved address. For nodes discovered from the cluster, it is their `system.peers` `rpc_address`, which need not be a configured contact point.

To match hostnames instead, set `peerIdentityDNS` and turn on `hostnameResolution`, which is what lets the driver work out a hostname per node. Note that `ssl` is a struct, so it has to be assigned back to the configuration after being changed:

```swift
  ssl.verifyFlag = .peerIdentityDNS
  configuration.ssl = ssl
  configuration.hostnameResolution = true
```

Setting `peerIdentityDNS` without `hostnameResolution` throws when the cluster is built, rather than failing later on every connection.

Hostname matching uses the name reverse DNS returns for each node's address, not the hostname configured as a contact point. The driver resolves contact points to addresses before it connects, so the string you configured is never the one matched, and each node's certificate has to carry the name its address reverse-resolves to. A node with no PTR record resolves to its own numeric address, which then fails the subject match and reports the same "Peer certificate subject name does not match" as a certificate naming the wrong address. Check what reverse DNS returns for each node before changing any certificates.

`peerCert` checks the certificate is valid but not which host it belongs to, and accepts a certificate issued for any host. `none` accepts any certificate at all. The client logs a warning when it connects with either.
