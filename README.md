# Swift Cassandra Client

CassandraClient is a Cassandra client in Swift. The client is based on [Datastax Cassandra C++ Driver](https://github.com/datastax/cpp-driver) wrapping it with Swift friendly APIs and data structures.

CassandraClient API currently exposes [SwiftNIO](https://github.com/apple/swift-nio) based futures to simplify integration with SwiftNIO based servers. Swift concurrency based API is also available in Swift 5.5 and newer.

## Usage

#### Creating a client instance

```swift
  let configuration = CassandraClient.Configuration(...)
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

#### Creating a session for a different keyspace

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

#### Running result-less commands, e.g. insert, update, delete or DDL

```swift
  cassandraClient.run("create table ...")
```

Or at a session level

```swift
  session.run("create table ...")
```

#### Running queries returning small data-sets that fit in-memory

Returning a model object, having `Model: Codable`

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

Or using free-form transformations on the row

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

#### Running queries returning large data-sets that do not fit in-memory

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

## DataStax Driver and libuv

The library depends on [the DataStax driver](https://github.com/datastax/cpp-driver) and [libuv](https://github.com/libuv/libuv), which are included as git submodules. Both of them have source files that are excluded in `Package.swift`.

### DataStax driver

The git submodule is under `Sources/CDataStaxDriver/datastax-cpp-driver`. To update, do `git fetch` then checkout the desired [tag/release](https://github.com/datastax/cpp-driver/releases). The driver's config files are located in `Sources/CDataStaxDriver/extras`.

### libuv

The git submodule is under `Sources/Clibuv/libuv`. To update, do `git fetch` then checkout the desired [tag/release](https://github.com/libuv/libuv/releases). Note that `include` and `uv.h` in `Sources/Clibuv` are symlinked to the corresponding directory/file in `Sources/Clibuv/libuv`.

## Development Setup

The library's tests require running a Cassandra database.

With docker (takes about 1 minute to be ready to accept connections):

```bash
$ docker run --name cassandra -p 127.0.0.1:9042:9042 -d cassandra:3
```
