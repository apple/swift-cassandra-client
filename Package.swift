// swift-tools-version:5.10

import PackageDescription

import class Foundation.FileManager

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
    "./datastax-cpp-driver/src/CMakeLists.txt",
    "./datastax-cpp-driver/src/fixnl.sh",
    "./datastax-cpp-driver/src/wkt.rl",
    "./datastax-cpp-driver/src/wktgen.sh",
    "./datastax-cpp-driver/src/gssapi",
    "./datastax-cpp-driver/src/ssl/ssl_no_impl.cpp",
    // See ./custom/src/ssl/ssl_openssl_impl.cpp
    "./datastax-cpp-driver/src/ssl/ssl_openssl_impl.cpp",
    "./datastax-cpp-driver/src/third_party/curl/CMakeLists.txt",
    "./datastax-cpp-driver/src/third_party/curl/COPYING",
    "./datastax-cpp-driver/src/third_party/hdr_histogram/CMakeLists.txt",
    "./datastax-cpp-driver/src/third_party/hdr_histogram/LICENSE.txt",
    "./datastax-cpp-driver/src/third_party/http-parser/AUTHORS",
    "./datastax-cpp-driver/src/third_party/http-parser/bench.c",
    "./datastax-cpp-driver/src/third_party/http-parser/CMakeLists.txt",
    "./datastax-cpp-driver/src/third_party/http-parser/http_parser.gyp",
    "./datastax-cpp-driver/src/third_party/http-parser/LICENSE-MIT",
    "./datastax-cpp-driver/src/third_party/http-parser/README",
    "./datastax-cpp-driver/src/third_party/http-parser/README.md",
    "./datastax-cpp-driver/src/third_party/http-parser/test.c",
    "./datastax-cpp-driver/src/third_party/http-parser/contrib/parsertrace.c",
    "./datastax-cpp-driver/src/third_party/http-parser/contrib/url_parser.c",
    "./datastax-cpp-driver/src/third_party/minizip",
    "./datastax-cpp-driver/src/third_party/mt19937_64",
    "./datastax-cpp-driver/src/third_party/rapidjson",
    "./datastax-cpp-driver/src/third_party/sparsehash",
    "./datastax-cpp-driver/tests",
    "./datastax-cpp-driver/examples",
]

do {
    if !(try FileManager.default.contentsOfDirectory(
        atPath: "./Sources/CDataStaxDriver/datastax-cpp-driver/src/third_party/sparsehash/CMakeFiles"
    ).isEmpty) {
        datastaxExclude.append("./datastax-cpp-driver/src/third_party/sparsehash/CMakeFiles/")
    }
} catch {
    // Assume CMakeFiles does not exist so no need to exclude it
}

let package = Package(
    name: "swift-cassandra-client",
    products: [
        .library(name: "CassandraClient", targets: ["CassandraClient"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-nio", .upToNextMajor(from: "2.41.1")),
        .package(url: "https://github.com/apple/swift-nio-ssl", exact: "2.32.0"),
        .package(url: "https://github.com/apple/swift-atomics", from: "1.0.2"),
        .package(url: "https://github.com/apple/swift-log", .upToNextMajor(from: "1.0.0")),
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
                .define("_GNU_SOURCE", to: "1"),  // required to fix "undefined CPU_COUNT" error
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
                // This is available on all modern Linux systems, and is needed for efficient
                // MicroTimer implementation. Otherwise busy waits are used.
                .define("HAVE_TIMERFD", .when(platforms: [.linux])),
                .headerSearchPath("./custom/include"),
                .headerSearchPath("./extras"),
                .headerSearchPath("./datastax-cpp-driver/src"),
                .headerSearchPath("./datastax-cpp-driver/src/third_party/http-parser"),
                .headerSearchPath("./datastax-cpp-driver/src/third_party/sparsehash/src/"),
            ]
        ),

        .target(
            name: "CassandraClient",
            dependencies: [
                "CDataStaxDriver",
                .product(name: "NIO", package: "swift-nio"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "Atomics", package: "swift-atomics"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),

        .testTarget(name: "CassandraClientTests", dependencies: ["CassandraClient"]),
    ],
    cxxLanguageStandard: .cxx17
)

// ---    STANDARD CROSS-REPO SETTINGS DO NOT EDIT   --- //
for target in package.targets {
    switch target.type {
    case .regular, .test, .executable:
        var settings = target.swiftSettings ?? []
        // https://github.com/swiftlang/swift-evolution/blob/main/proposals/0444-member-import-visibility.md
        settings.append(.enableUpcomingFeature("MemberImportVisibility"))
        target.swiftSettings = settings
    case .macro, .plugin, .system, .binary:
        ()  // not applicable
    @unknown default:
        ()  // we don't know what to do here, do nothing
    }
}
// --- END: STANDARD CROSS-REPO SETTINGS DO NOT EDIT --- //
