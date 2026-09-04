# NormFS 🚀

[![Crates.io](https://img.shields.io/crates/v/normfs.svg)](https://crates.io/crates/normfs)

**High-performance persistent queue storage for robotics and embedded systems**

Storage engine with automatic data lifecycle management across memory, disk, and cloud. Built for high-frequency sensor data ingestion. Available as embeddable library or standalone server.

## 📊 Latency

![Fanout Scaling](images/fanout-scaling.jpg)

**Fanout Latency**: Time for a message to propagate from write to all N concurrent subscribers over TCP. Measures the server's ability to efficiently distribute messages to multiple clients simultaneously - critical for real-time multi-sensor coordination in robotics and distributed systems.

**TCP Fanout Benchmarks** (1KB message):

| Clients | P50 | P95 | P99 |
|---------|-----|-----|-----|
| 1 | 49µs | 65µs | 88µs |
| 2 | 59µs | 78µs | 97µs |
| 4 | 81µs | 109µs | 145µs |
| 8 | 146µs | 183µs | 224µs |
| 16 | 243µs | 305µs | 347µs |
| 32 | 357µs | 436µs | 508µs |
| 64 | 549µs | 656µs | 809µs |
| 128 | 956µs | 1.1ms | 1.5ms |
| 256 | 1.8ms | 2.0ms | 3.0ms |
| 512 | 3.7ms | 5.0ms | 6.5ms |
| 1024 | 7.0ms | 8.2ms | 19ms |
| 2048 | 15.0ms | 17.9ms | 37.1ms |
| 4096 | 35.9ms | 40.3ms | 78.5ms |
| 8192 | 792ms | 1.08s | 1.26s |

*Benchmarked on Apple M3 Max MacBook Pro. Embedded library performance is significantly faster.*

📈 **[Full TCP benchmarks →](normfs_go/bench/README.md)**

## 📈 Throughput and Readers

![Device throughput and reader cost](images/device-and-readers.png)

Two machines, one picture: what a board's SD card holds while writing, and what
concurrent tail readers cost on a laptop.

**On the board** — rover-alpha, aarch64, class-10 SD card, zstd + AES-GCM,
120 s per size:

| Record | Records/s | MB/s in | MB/s to card | Amplification |
|--------|-----------|---------|--------------|---------------|
| 50 B | 138,676 | 6.93 | 12.95 | 2.16× |
| 8 KiB | 1,659 | 13.59 | **21.73** | 1.89× |
| 100 KiB | 124 | 12.65 | 20.23 | 1.89× |
| 450 KiB | 23 | 10.69 | 17.44 | 1.95× |
| 2 MiB | 5 | 10.69 | 18.78 | 1.93× |

The card itself does 18–20 MB/s under `dd` at any block size from 4 KiB to
1 MiB, so from 8 KiB records upward NormFS is running it at its limit. The
amplification is framing, compression and encryption — the bytes that reach the
card are not the bytes the caller wrote.

Replaying three camera streams faster than real time finds the *board's* limit
rather than the card's: at 1 Hz per stream all three keep up at 100 %, at 2 Hz
they hold 42 % and at 3 Hz 24 %, while input stays near 2 MB/s and the card
idles at 20. Compression is the ceiling there, not I/O.

**On the laptop** — MacBook Pro, Apple M4 Pro (8P + 4E cores, 24 GB), N clients
each polling `ShiftFromTail(0)` every 1 ms, 500 publishes per point, median of
two rounds:

| Readers | Read p50 | Read p99 | Publish→last reader p50 | p99 |
|---------|----------|----------|-------------------------|-----|
| 1 | 6µs | 29µs | 2.4ms | 4.9ms |
| 10 | 4µs | 44µs | 4.3ms | 5.1ms |
| 100 | 5µs | 152µs | 4.2ms | 6.0ms |
| 500 | 7µs | 152µs | 4.1ms | 6.7ms |
| 1000 | 4µs | 95µs | 4.1ms | 7.1ms |
| 2000 | 5µs | 94µs | 6.5ms | 11.1ms |
| 4000 | 5µs | 97µs | **11.9ms** | 18.3ms |

An individual read stays flat from 1 to 4000 readers because it is a page
lookup and the page is borrowed, not copied. Propagation is flat to 1000
readers; past that the fan-out itself is the cost. Both propagation figures
include each reader's own 1 ms poll interval.

## ✨ Features

- 🗄️ **Tiered Storage**: Memory → WAL → Compressed Store → Cloud archival
- ⚡ **High Performance**: <100μs latency (p99)
- 🤖 **Multi-System Isolation**: Unique instance IDs for safe fleet-wide cloud syncing
- 🔒 **Security**: AES-256-GCM encryption + Ed25519 signatures
- 📖 **Flexible Reads**: Absolute/relative positioning, tail reads, subscriptions, step queries
- ⏱️ **Time Sync**: Nanosecond-precision timestamps for distributed coordination
- 🔄 **Crash Recovery**: Write-ahead logging with automatic replay
- ✅ **Proven Codecs**: The WAL entry and header codecs are C, verified with Frama-C WP and run in CI — a corrupt entry is detected, not decoded

## 🏗️ Architecture

```
Memory → WAL (Disk) → Store (Disk) → Cloud (S3-compatible)
  ↓         ↓             ↓              ↓
Fast    Durable    Compressed      Archival
```

## 🎯 Use Cases

🤖 **Robotics**: High-frequency sensor logging (IMU, lidar, GPS), multi-sensor sync, black box recording, simulation replay, fleet data aggregation

💾 **Embedded Systems**: Time-series data, event sourcing, audit logs, edge computing with cloud sync

🌐 **IoT & Edge**: Local-first storage with automatic cloud archival, multi-device coordination

## 🌍 Language Support

### 🦀 Rust (Core Implementation + Embedded Library)

**Embedded Library**: Zero external dependencies, runs in-process with your Rust application. No separate database or server process required.

**Standalone Server**: TCP/WebSocket server for multi-language client access.

📖 **Rust Documentation** - Coming soon

### 🐹 Go (Client Library)

Native Go client for connecting to NormFS servers.

📖 **[Go Documentation →](normfs_go/README.md)**

### 🐍 Python · 🟨 TypeScript

Coming soon. Protocol specification available for implementing additional clients.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/norma-core/normfs.git
cd normfs
```

### Run as Server

```bash
# Build server
cargo build --release --features server-bin --bin normfs-server

# Run server
./target/release/normfs-server --data-dir /tmp/normfs-data --addr 0.0.0.0:8888
```

### Client Libraries

See language-specific documentation:
- 🐹 **Go**: [normfs_go/README.md](normfs_go/README.md)
- 🦀 **Rust**: Available (documentation coming soon)

### Cross-Compilation

Build the server for multiple platforms using [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild):

```bash
# Install cargo-zigbuild
cargo install cargo-zigbuild

# Build server for different platforms
cargo zigbuild --release --features server-bin --bin normfs-server --target x86_64-unknown-linux-gnu
cargo zigbuild --release --features server-bin --bin normfs-server --target aarch64-unknown-linux-gnu
cargo zigbuild --release --features server-bin --bin normfs-server --target aarch64-apple-darwin
cargo zigbuild --release --features server-bin --bin normfs-server --target x86_64-apple-darwin
cargo zigbuild --release --features server-bin --bin normfs-server --target x86_64-unknown-freebsd

# Binaries will be at: target/<target-triple>/release/normfs-server
```

## 💻 Platform Support

| Platform | Arch | Status |
|----------|------|--------|
| Linux | x86_64, aarch64 | ✅ |
| macOS | x86_64, aarch64 | ✅ |
| FreeBSD | x86_64 | ✅ |

The WAL checksums with the CPU's CRC32 instruction, so x86_64 needs SSE4.2
(Nehalem, Bulldozer and later) and aarch64 needs the CRC extension. A CPU
without it is not supported and faults rather than falling back.

## 📦 Components

- **normfs**: Core Rust library and server
- **normfs-wal**: Write-ahead log, with a formally verified C entry codec
- **normfs-store**: Compressed/encrypted persistent storage
- **normfs-cloud**: S3-compatible cloud integration
- **normfs-crypto**: Encryption and signing
- **normfs_go**: Go client library
- **uintn**: Variable-width integers with infinite scaling

## 📊 Status

**v0.3.0** - Active development, API may change before 1.0

WAL files written by 0.1 are read by 0.2 unchanged. 0.2 writes a smaller entry
format that 0.1 cannot read, so a downgrade needs the queue drained first. 0.3
writes the same format as 0.2 and needs no migration in either direction.

## 📄 License

[MIT](LICENSE)
