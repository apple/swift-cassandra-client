// swift-tools-version:5.2
import class Foundation.FileManager
import PackageDescription

// Compute libuv sources to exclude
var libuvExclude = [
    "./libuv/src/unix/aix-common.c",
    "./libuv/src/unix/aix.c",
    "./libuv/src/unix/bsd-proctitle.c",
    "./libuv/src/unix/cygwin.c",
    "./libuv/src/unix/freebsd.c",
    "./libuv/src/unix/haiku.c",
    "./libuv/src/unix/hurd.c",
    "./libuv/src/unix/ibmi.c",
    "./libuv/src/unix/netbsd.c",
    "./libuv/src/unix/no-fsevents.c",
    "./libuv/src/unix/no-proctitle.c",
    "./libuv/src/unix/openbsd.c",
    "./libuv/src/unix/os390-proctitle.c",
    "./libuv/src/unix/os390-syscalls.c",
    "./libuv/src/unix/os390.c",
    "./libuv/src/unix/posix-hrtime.c",
    "./libuv/src/unix/posix-poll.c",
    "./libuv/src/unix/pthread-fixes.c",
    "./libuv/src/unix/qnx.c",
    "./libuv/src/unix/sunos.c",
    "./libuv/src/unix/sysinfo-loadavg.c",
    "./libuv/src/unix/sysinfo-memory.c",
]

#if !os(macOS)
libuvExclude += [
    "./libuv/src/unix/bsd-ifaddrs.c",
    "./libuv/src/unix/darwin-proctitle.c",
    "./libuv/src/unix/darwin.c",
    "./libuv/src/unix/fsevents.c",
    "./libuv/src/unix/kqueue.c",
    "./libuv/src/unix/random-getentropy.c",
]
#endif

#if !os(Linux)
libuvExclude += [
    "./libuv/src/unix/epoll.c",
    "./libuv/src/unix/linux-core.c",
    "./libuv/src/unix/linux-inotify.c",
    "./libuv/src/unix/linux-syscalls.c",
    "./libuv/src/unix/procfs-exepath.c",
    "./libuv/src/unix/random-getrandom.c",
    "./libuv/src/unix/random-sysctl-linux.c",
]
#endif

var datastaxExclude = [
    "./datastax-cpp-driver/src/gssapi/",
    "./datastax-cpp-driver/src/ssl/ssl_no_impl.cpp",
    "./datastax-cpp-driver/src/ssl/ssl_openssl_impl.cpp", // See ./custom/src/ssl/ssl_openssl_impl.cpp
    "./datastax-cpp-driver/src/third_party/http-parser/bench.c",
    "./datastax-cpp-driver/src/third_party/http-parser/test.c",
    "./datastax-cpp-driver/src/third_party/http-parser/contrib/parsertrace.c",
    "./datastax-cpp-driver/src/third_party/http-parser/contrib/url_parser.c",
    "./datastax-cpp-driver/src/third_party/minizip/iowin32.c",
    "./datastax-cpp-driver/src/third_party/minizip/miniunz.c",
    "./datastax-cpp-driver/src/third_party/minizip/minizip.c",
    "./datastax-cpp-driver/src/third_party/minizip/unzip.c",
    "./datastax-cpp-driver/src/third_party/minizip/zip.c",
    "./datastax-cpp-driver/tests",
    "./datastax-cpp-driver/examples",
]

do {
    if !(try FileManager.default.contentsOfDirectory(atPath: "./Sources/CDataStaxDriver/datastax-cpp-driver/src/third_party/sparsehash/CMakeFiles").isEmpty) {
        datastaxExclude.append("./datastax-cpp-driver/src/third_party/sparsehash/CMakeFiles/")
    }
} catch {
    // Assume CMakeFiles does not exist so no need to exclude it
}

let package = Package(
    name: "swift-cassandra-client",
    products: [
        .library(name: "CassandraClient", targets: ["CassandraClient"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio.git", .upToNextMinor(from: "2.38.0")),
        .package(url: "https://github.com/apple/swift-nio-ssl.git", .upToNextMinor(from: "2.16.1")),
        .package(url: "https://github.com/apple/swift-log.git", .upToNextMajor(from: "1.0.0")),
    ],
    targets: [
        .target(
            name: "Clibuv",
            dependencies: [],
            exclude: libuvExclude,
            sources: [
                "./libuv/src/fs-poll.c",
                "./libuv/src/idna.c",
                "./libuv/src/inet.c",
                "./libuv/src/random.c",
                "./libuv/src/strscpy.c",
                "./libuv/src/strtok.c",
                "./libuv/src/threadpool.c",
                "./libuv/src/timer.c",
                "./libuv/src/uv-common.c",
                "./libuv/src/uv-data-getter-setters.c",
                "./libuv/src/version.c",
                "./libuv/src/unix",
            ],
            cSettings: [
                .headerSearchPath("./libuv/src"),
                .define("_GNU_SOURCE", to: "1"), // required to fix "undefined CPU_COUNT" error
            ]
        ),

        .target(
            name: "CDataStaxDriver",
            dependencies: [
                "Clibuv",
                .product(name: "NIOSSL", package: "swift-nio-ssl"),
            ],
            exclude: datastaxExclude,
            sources: [
                "./datastax-cpp-driver/src",
                "./custom/src",
            ],
            publicHeadersPath: "./datastax-cpp-driver/include",
            cxxSettings: [
                .headerSearchPath("./custom/include"),
                .headerSearchPath("./extras"),
                .headerSearchPath("./datastax-cpp-driver/src"),
                .headerSearchPath("./datastax-cpp-driver/src/third_party/sparsehash/src/"),
                .headerSearchPath("./datastax-cpp-driver/src/third_party/http-parser"),
            ]
        ),

        .target(name: "CassandraClient", dependencies: [
            "CDataStaxDriver",
            .product(name: "NIO", package: "swift-nio"),
            .product(name: "NIOCore", package: "swift-nio"),
            .product(name: "Logging", package: "swift-log"),
        ]),

        .testTarget(name: "CassandraClientTests", dependencies: ["CassandraClient"]),
    ],
    cxxLanguageStandard: .cxx14
)
