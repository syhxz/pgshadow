# pgshadow

PostgreSQL/Greenplum passive traffic capture and replay tool. Captures SQL traffic from the source database without any connection or modification to it, then replays against a target database for load testing, migration validation, or performance benchmarking.

## Architecture

```
Source DB (untouched)
    |
    | TCP traffic (passive capture via pcap/af_packet/ebpf)
    v
[Capture] -> [Protocol Parser] -> [Filter] -> [Queue] -> [Replayer] -> Target DB
```

- **Zero impact on source**: passive network capture only, never connects to the source database
- **Protocol-aware**: full PostgreSQL wire protocol parsing including Extended Query (Parse/Bind/Execute) with faithful parameterized replay, and COPY FROM STDIN
- **Greenplum support**: autocommit-per-statement dialect, transaction control filtering
- **Concurrency modes**: bounded (default), faithful (1:1 connection mapping), serial
- **Session affinity**: same source connection replays on same target connection
- **Pacing**: reproduce original timing, speed up (2x, 10x), or replay ASAP

## Quick Start

```bash
# 1. Configure
cp config.example.yaml config.yaml
vim config.yaml  # set interface, target_host, target_database, target_user

# 2. Set target database password (must match password_env in config.yaml)
export PGSHADOW_DB_PASSWORD="your_password"

# 3. Run (requires CAP_NET_RAW for packet capture, or root for eBPF)
sudo -E ./pgshadow -config config.yaml

# Or grant capability without sudo (pcap/af_packet mode only):
sudo setcap cap_net_raw=eip ./pgshadow
./pgshadow -config config.yaml
```

## Configuration

See `config.example.yaml` for all options. Key settings:

### Capture

| Field | Default | Description |
|-------|---------|-------------|
| `interface` | (required) | Network interface to capture (e.g., `eth0`, `lo`) |
| `port` | 5432 | PostgreSQL port to filter |
| `source_host` | — | Source DB IP/hostname — only capture traffic to/from this host |
| `source_port` | same as `port` | Source DB port (for cross-host capture) |
| `mode` | pcap | `pcap` / `af_packet` / `ebpf` / `ebpf_ssl` |
| `bidirectional` | true | Capture both directions (enables pacing, exec time metrics) |
| `target_pid` | 0 | (ebpf_ssl only) Capture specific PG process; 0 = all |
| `libssl_path` | auto | (ebpf_ssl only) Path to libssl.so; auto-detected if empty |

### Replayer

| Field | Default | Description |
|-------|---------|-------------|
| `target_host` | (required) | Target database host |
| `target_port` | 5432 | Target database port |
| `target_database` | (required) | Target database name |
| `target_user` | (required) | Target database user |
| `password_env` | `PGSHADOW_DB_PASSWORD` | Env var name for target password |
| `workers` | 32 | Worker goroutines |
| `speed_factor` | 1.0 | `1.0`=original, `2.0`=2x faster, `0`=ASAP |
| `session_affinity` | true | Same source ConnID -> same target connection |
| `error_policy` | skip | `skip` / `retry` / `abort` |
| `replay_concurrency_mode` | bounded | `bounded` / `faithful` / `serial` |
| `target_dialect` | postgresql | `postgresql` / `greenplum` |
| `pool_max_conns` | 100 | Connection pool max size |

### Queue

| Field | Default | Description |
|-------|---------|-------------|
| `type` | ring | `ring` (in-memory) / `file` (on-disk WAL) / `kafka` (external Kafka cluster) |
| `capacity` | 100000 | Max buffered events (ring/file only) |
| `overflow` | drop_oldest | `drop_oldest` / `drop_newest` / `block` (ring/file only) |
| `kafka.brokers` | — | Kafka broker addresses (kafka mode) |
| `kafka.topic` | pgshadow | Kafka topic name (kafka mode) |

### Metrics

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | true | Enable Prometheus metrics endpoint |
| `port` | 9090 | Metrics HTTP port |

## Metrics

When enabled, Prometheus metrics are exposed at `http://<host>:9090/metrics`:

- `pgshadow_queue_depth` — current buffered event count
- `pgshadow_queue_overflow_total` — events discarded due to overflow
- `pgshadow_replay_exec_time` — target execution time (p50/p95/p99)
- `pgshadow_replay_errors_total` — target execution errors by type

## Graceful Shutdown

Send `SIGINT` or `SIGTERM`:

1. Capture stops immediately
2. Queue drains remaining buffered events to the replayer
3. Replayer finishes in-flight statements
4. Connections released, process exits

## Requirements

- Linux (packet capture requires `CAP_NET_RAW`)
- Network visibility to source database traffic (same host, mirror port, or network tap)
- Network connectivity to target database

### Platform Support

| Platform | pcap | af_packet | ebpf (XDP) | ebpf_ssl (uprobe) |
|----------|------|-----------|------------|-------------------|
| Linux x86_64 (amd64) | ✅ | ✅ | ✅ | ✅ |
| Linux arm64 (Graviton) | ✅ | ✅ | ✅ | ✅ |
| Ubuntu 20.04+ | ✅ | ✅ | ✅ | ✅ |
| Amazon Linux 2023 | ✅ | ✅ | ✅ | ✅ |
| RHEL/CentOS 8+ | ✅ | ✅ | ✅ | ✅ |
| Debian 11+ | ✅ | ✅ | ✅ | ✅ |
| macOS / Windows | ❌ | ❌ | ❌ | ❌ |

**Kernel requirements by mode:**

| Mode | Minimum Kernel | Required Capabilities |
|------|---------------|----------------------|
| `pcap` | Any | `CAP_NET_RAW` |
| `af_packet` | 3.x | `CAP_NET_RAW` |
| `ebpf` | 5.8+ (ring buffer) | `CAP_BPF` + `CAP_NET_ADMIN` |
| `ebpf_ssl` | 5.5+ (uprobe + CO-RE) | `CAP_BPF` + `CAP_SYS_ADMIN` |

**Multi-architecture BPF build:**

eBPF programs contain architecture-specific code (register access for uprobes). Pre-compiled BPF objects for arm64 are included. To build for x86_64:

```bash
# On an x86_64 machine:
./pkg/capture/bpf/generate_bpf.sh

# This generates:
#   pgshadowxdp_amd64_bpfel.{go,o}
#   pgshadowssl_amd64_bpfel.{go,o}
# Commit these to the repo for x86_64 support.
```

The Go binary itself cross-compiles normally — only the BPF `.o` objects need to be generated on the target architecture (once, then committed to the repo).

## Building from Source

```bash
# Standard build (no capture, for testing/CI)
go build -o pgshadow ./cmd/pgshadow/

# With pcap capture (requires libpcap-dev)
sudo apt-get install -y libpcap-dev
go build -tags pcap -o pgshadow ./cmd/pgshadow/

# With eBPF capture (requires clang, Linux kernel ≥ 5.8)
sudo apt-get install -y clang llvm libbpf-dev
go build -tags "pcap ebpf" -o pgshadow ./cmd/pgshadow/

# Cross-compile: Linux amd64
GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -tags pcap -o pgshadow-linux-amd64 ./cmd/pgshadow/

# Cross-compile: Linux arm64 (Graviton)
GOOS=linux GOARCH=arm64 CGO_ENABLED=1 go build -tags pcap -o pgshadow-linux-arm64 ./cmd/pgshadow/
```

## eBPF Capture Mode

eBPF/XDP mode provides kernel-level packet capture with zero-copy ring buffer delivery, offering lower overhead than pcap/af_packet for high-throughput scenarios.

### Prerequisites

- Linux kernel ≥ 5.8 (ring buffer support)
- BTF enabled (`/sys/kernel/btf/vmlinux` exists)
- `CAP_BPF` + `CAP_NET_ADMIN` capabilities (or root)
- clang/llvm (for compiling BPF programs, build-time only)

### Building with eBPF Support

```bash
# Install build dependencies (Ubuntu/Debian)
sudo apt-get install -y clang llvm libbpf-dev

# Build with eBPF tag
GOOS=linux go build -tags "pcap ebpf" -o pgshadow ./cmd/pgshadow/

# Or build eBPF-only (no libpcap dependency)
GOOS=linux go build -tags ebpf -o pgshadow ./cmd/pgshadow/
```

### eBPF Configuration

```yaml
capture:
  interface: "eth0"    # Network interface to attach XDP program
  port: 5432           # PostgreSQL port to filter
  mode: "ebpf"         # Use eBPF/XDP capture
  bidirectional: true   # Capture both client→server and server→client
```

### Running

```bash
# eBPF requires root or CAP_BPF + CAP_NET_ADMIN
sudo ./pgshadow -config config.yaml

# Or grant capabilities:
sudo setcap cap_bpf,cap_net_admin,cap_net_raw=eip ./pgshadow
./pgshadow -config config.yaml
```

### Verifying eBPF is Active

```bash
# Check attached XDP program
sudo bpftool prog show name pgshadow_xdp

# Check packet stats (per-CPU counters)
sudo bpftool map dump name stats

# Check ring buffer
sudo bpftool map show name pkt_ring
```

### Notes

- On loopback (`lo`), XDP runs in generic/SKB mode automatically
- On physical NICs with driver support, XDP runs in native mode (higher performance)
- The XDP program always returns `XDP_PASS` — packets are mirrored, never dropped (read-only, zero impact on source traffic)
- Ring buffer default size: 64 MiB (configurable via `buffer_size` in capture config)

---

## eBPF SSL Capture Mode (TLS Support)

When the source PostgreSQL has SSL/TLS enabled, network-level capture (pcap/af_packet/ebpf) can only see encrypted ciphertext. The `ebpf_ssl` mode solves this by hooking OpenSSL's `SSL_read`/`SSL_write` functions via eBPF uprobes, reading plaintext data directly from the PostgreSQL process memory — **without decryption keys, without MITM proxy, fully passive**.

Full Extended Query protocol support (Parse/Bind/Execute) is maintained: per-connection TCP sequence numbers are tracked to ensure the reassembler correctly orders multiple SSL records from the same connection.

```
Client → [TLS 1.3 encrypted] → PostgreSQL process
                                       |
                                 SSL_read() / SSL_write()
                                       |
                                 eBPF uprobe captures plaintext
                                       |
                                 ring buffer → pgshadow
```

### Prerequisites

- Linux kernel ≥ 5.5 (uprobe + ring buffer)
- BTF enabled (`/sys/kernel/btf/vmlinux`)
- `CAP_BPF` + `CAP_SYS_ADMIN` (or root)
- `libssl.so` accessible on the system (auto-detected from PostgreSQL process)

### Building

The `ebpf_ssl` mode requires the `ebpf` build tag:

```bash
go build -tags ebpf -o pgshadow ./cmd/pgshadow/
```

Without this tag, `mode: "ebpf_ssl"` will fail at startup with: `ebpf_ssl mode is unavailable in this build; rebuild with -tags ebpf`.

### Configuration

```yaml
capture:
  mode: "ebpf_ssl"        # Hook SSL_read/SSL_write via uprobe
  port: 5432
  bidirectional: true
  # target_pid: 12345     # Optional: only capture specific PG process
  # libssl_path: "/usr/lib/aarch64-linux-gnu/libssl.so.3"  # Optional: auto-detected
```

### How It Works

1. pgshadow finds `libssl.so` loaded by the PostgreSQL process (via `/proc/PID/maps`)
2. Attaches eBPF uprobes to `SSL_read` entry + return, and `SSL_write` entry + return
3. On function entry: saves the buffer pointer (where plaintext will be written)
4. On function return: reads the plaintext from that buffer, sends to ring buffer
5. Userspace reconstructs PG wire protocol packets and feeds them into the normal pipeline

### Supported TLS Versions

| TLS Version | Cipher Suite | Status |
|-------------|-------------|--------|
| TLS 1.3 | AES-256-GCM-SHA384 | ✅ Tested |
| TLS 1.3 | AES-128-GCM-SHA256 | ✅ Supported |
| TLS 1.2 | ECDHE-RSA-AES256 | ✅ Supported |
| TLS 1.2 | Any cipher | ✅ Supported |

### Verifying

```bash
# Confirm PostgreSQL SSL is on
psql -h 127.0.0.1 -U postgres -c "SHOW ssl;"

# Confirm connection is encrypted
psql "host=127.0.0.1 sslmode=require" -c \
  "SELECT * FROM pg_stat_ssl WHERE pid = pg_backend_pid();"

# Check uprobe programs loaded
sudo bpftool prog list | grep ssl

# Check captured events
sudo bpftool map dump name ssl_stats
```

### Capture Mode Comparison

| Mode | Network Capture | SSL Support | Performance | Use Case |
|------|----------------|-------------|-------------|----------|
| `pcap` | libpcap | ❌ ciphertext only | Good | General, portable |
| `af_packet` | kernel bypass | ❌ ciphertext only | Better | High throughput |
| `ebpf` | XDP/ring buffer | ❌ ciphertext only | Best | Highest throughput, no libpcap |
| **`ebpf_ssl`** | **uprobe hook** | **✅ full TLS support** | Good | **SSL-enabled PostgreSQL** |

### Notes

- Works with both OpenSSL 1.1.x and OpenSSL 3.x (`libssl.so.1.1` / `libssl.so.3`)
- Does not require PostgreSQL source modification or restart
- Can filter by specific PID to avoid capturing unrelated SSL traffic
- If PostgreSQL uses a non-standard libssl path, specify it via `libssl_path`
- Fully passive: never modifies SSL sessions, never injects data
- Extended Query (Parse/Bind/Execute) works correctly: each SSL_read/SSL_write maps to a properly sequenced synthetic TCP segment for faithful reassembly

---

## Kafka Queue Mode

Kafka provides durable, distributed buffering between the capture layer and the replayer. It supports cross-host deployments where capture and replay run on different machines.

### Starting Kafka with Docker

```bash
# Start Kafka in KRaft mode (no Zookeeper required)
sudo docker run -d --name kafka \
  --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  apache/kafka:latest

# Wait for Kafka to be ready
until nc -z localhost 9092; do sleep 1; done
echo "Kafka is ready"

# Verify
sudo docker logs kafka | tail -5
```

For production multi-broker clusters, adjust `KAFKA_ADVERTISED_LISTENERS` and replication factor accordingly.

### Kafka Configuration

```yaml
queue:
  type: "kafka"
  kafka:
    brokers: ["localhost:9092"]        # Comma-separated broker list
    topic: "pgshadow"                  # Topic name (auto-created if needed)
```

### Connecting to Managed Kafka Services

pgshadow supports SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and TLS, enabling connectivity to any managed or self-hosted Kafka cluster.

**AWS MSK (SASL/SCRAM + TLS)**

```yaml
queue:
  type: "kafka"
  kafka:
    brokers:
      - "b-1.your-msk-cluster.kafka.us-east-1.amazonaws.com:9096"
      - "b-2.your-msk-cluster.kafka.us-east-1.amazonaws.com:9096"
    topic: "pgshadow"
    sasl_mechanism: "scram-sha-512"
    sasl_username: "your-msk-user"
    sasl_password_env: "KAFKA_SASL_PASSWORD"   # read from env var
    tls_enabled: true
```

**Confluent Cloud (SASL/PLAIN + TLS)**

```yaml
queue:
  type: "kafka"
  kafka:
    brokers: ["pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"]
    topic: "pgshadow"
    sasl_mechanism: "plain"
    sasl_username: "YOUR_API_KEY"
    sasl_password_env: "CONFLUENT_API_SECRET"
    tls_enabled: true
```

**Self-hosted Kafka with mTLS (mutual TLS)**

```yaml
queue:
  type: "kafka"
  kafka:
    brokers: ["kafka1.internal:9093", "kafka2.internal:9093"]
    topic: "pgshadow"
    tls_enabled: true
    tls_ca_file: "/etc/pgshadow/ca.pem"
    tls_cert_file: "/etc/pgshadow/client-cert.pem"
    tls_key_file: "/etc/pgshadow/client-key.pem"
```

**Self-hosted Kafka without authentication (plaintext)**

```yaml
queue:
  type: "kafka"
  kafka:
    brokers: ["kafka1:9092", "kafka2:9092", "kafka3:9092"]
    topic: "pgshadow"
```

#### SASL/TLS Configuration Reference

| Field | Description |
|-------|-------------|
| `sasl_mechanism` | `plain` / `scram-sha-256` / `scram-sha-512` |
| `sasl_username` | SASL username |
| `sasl_password` | SASL password (plaintext in config) |
| `sasl_password_env` | Env var name to read password from (preferred over `sasl_password`) |
| `tls_enabled` | Enable TLS encryption (`true`/`false`) |
| `tls_skip_verify` | Skip server cert verification (testing only) |
| `tls_ca_file` | Path to CA certificate PEM file |
| `tls_cert_file` | Path to client certificate (for mTLS) |
| `tls_key_file` | Path to client private key (for mTLS) |

### Full eBPF + Kafka Example (`config.yaml`)

```yaml
capture:
  interface: "eth0"
  port: 5432
  mode: "ebpf"
  bidirectional: true

parser:
  max_sql_length: 1048576
  timeout_idle_conn: 300
  extended_query: true

filter:
  mode: "all"

queue:
  type: "kafka"
  kafka:
    brokers: ["localhost:9092"]
    topic: "pgshadow"

replayer:
  target_host: "your-target-db.rds.amazonaws.com"
  target_port: 5432
  target_database: "postgres"
  target_user: "dbmgr"
  password_env: "PGSHADOW_DB_PASSWORD"
  workers: 128
  speed_factor: 0               # 0 = replay ASAP (no pacing)
  session_affinity: true
  error_policy: "skip"
  replay_concurrency_mode: "bounded"
  target_dialect: "postgresql"
  pool_max_conns: 128
  pool_min_conns: 32

metrics:
  enabled: true
  port: 9091
```

### Running the Full Pipeline

```bash
# 1. Start Kafka
sudo docker run -d --name kafka --network host \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  apache/kafka:latest

# 2. Wait for Kafka
until nc -z localhost 9092; do sleep 1; done

# 3. Set target password (must match password_env in config.yaml)
export PGSHADOW_DB_PASSWORD="your_password"

# 4. Start pgshadow (eBPF + Kafka)
sudo -E ./pgshadow -config config.yaml

# 5. Monitor via Prometheus metrics
curl http://localhost:9091/metrics | grep pgshadow
```

### Kafka Operational Notes

- **Topic auto-creation**: pgshadow auto-creates the topic on first produce; no manual `kafka-topics` needed
- **Ordering guarantee**: Messages are keyed by ConnID (source connection), so all SQL from one connection lands in the same partition and preserves order
- **Consumer group**: `pgshadow-replayer` (fixed); Kafka tracks offsets for at-least-once delivery
- **Backpressure**: If Kafka is unreachable for extended periods, the local 200K-message buffer absorbs bursts; overflow is reported via `pgshadow_queue_overflow_total` metric
- **Startup fail-fast**: If any configured broker is unreachable at startup, pgshadow exits immediately with an error naming the unreachable broker(s)

### Stopping

```bash
# Stop pgshadow
sudo pkill -SIGTERM pgshadow

# Stop Kafka
sudo docker stop kafka && sudo docker rm kafka
```

---

## Kubernetes / Greenplum Deployment Guide

### Deployment Scenario: pgshadow on the GP Master Host

In a Kubernetes environment, GP Master runs inside a Pod. Client traffic passes through the host's virtual bridge to reach the Pod. Deploy pgshadow on the host node to passively capture all SQL traffic to/from GP.

#### Step 1: Identify the Network Interface

```bash
# Get GP Master Pod IP
kubectl get pod -n <namespace> -l app=greenplum-master -o wide
# Example output: NAME          IP            NODE
#                 gp-master-0   <GP_POD_IP>   worker-node-1

# Determine which interface carries traffic (varies by CNI)
ip route get <GP_POD_IP>
# Output: <GP_POD_IP> dev cali1234abcd src <HOST_IP>

# Common CNI interface names:
#   Calico:  cali*
#   Flannel: flannel.1, cni0
#   Cilium:  cilium_host, lxc*
#   Bridge:  cbr0, docker0

# Verify GP traffic is visible
sudo tcpdump -i cali1234abcd -nn host <GP_POD_IP> and port 5432 -c 5
```

#### Step 2: Configure pgshadow

**Filter by Pod IP (precise):**

```yaml
capture:
  interface: "cali1234abcd"   # veth interface connected to GP Pod
  port: 5432
  source_host: "<GP_POD_IP>"  # GP Master Pod IP
  source_port: 5432           # GP listen port inside the Pod
  mode: "ebpf"
  bidirectional: true

replayer:
  target_host: "target-gp-master.example.com"
  target_port: 5432
  target_dialect: "greenplum"
  workers: 64
  speed_factor: 0
  session_affinity: true
  error_policy: "skip"
  pool_max_conns: 64
```

**Filter by port only (no config update needed when Pod IP changes):**

```yaml
capture:
  interface: "cni0"           # bridge interface
  port: 5432                  # works if only GP uses port 5432 on this host
  mode: "ebpf"               # recommended: falls back to generic mode on veth/bridge
  bidirectional: true
```

#### Step 3: Run

```bash
export PGSHADOW_DB_PASSWORD="target_db_password"
sudo -E ./pgshadow -config config.yaml
```

#### Notes

| Topic | Details |
|-------|---------|
| Pod IP changes | Pod restart assigns new IP; update `source_host` or omit it to filter by port only |
| NodePort mapping | Client connects to `Node:30432`; after DNAT, veth sees `PodIP:5432` |
| SSL/TLS | K8s internal traffic is usually unencrypted (use plaintext capture); if SSL is enabled use `ebpf_ssl` mode |
| eBPF vs pcap | eBPF XDP requires a specific interface; on veth/bridge it auto-falls back to generic mode (still faster than pcap); pcap supports `interface: "any"` |
| Permissions | eBPF requires `CAP_BPF + CAP_NET_ADMIN`; pcap requires `CAP_NET_RAW` |

---

### Greenplum Dialect (`target_dialect: "greenplum"`)

When `target_dialect: "greenplum"` is set, the replayer automatically adapts to GP execution semantics:

| Behavior | Description |
|----------|-------------|
| Skip transaction control | BEGIN / COMMIT / ROLLBACK / END / ABORT / START TRANSACTION are not sent to the target |
| Autocommit mode | Each SQL statement executes independently, no explicit transactions, avoids holding distributed locks |
| Master-only connection | Replay connects only to the target GP Master; no segment fan-out |
| Compatibility | Based on PG wire protocol; compatible with GP 6.x / 7.x |

**Why skip transaction control?**

In Greenplum, an explicit BEGIN opens a distributed transaction on the Master and holds locks across all involved Segments until COMMIT/ROLLBACK. For replay scenarios, this causes unnecessary cross-segment lock contention. Skipping transaction control lets each statement execute in autocommit mode, consistent with GP's recommended high-throughput execution pattern.

**Full GP replay configuration example:**

```yaml
capture:
  interface: "eth0"
  port: 5432
  source_host: "<GP_POD_IP>"
  mode: "ebpf"
  bidirectional: true

filter:
  mode: "all"                 # or "write_only" to replay writes only

queue:
  type: "kafka"
  kafka:
    brokers: ["kafka:9092"]
    topic: "pgshadow-gp"

replayer:
  target_host: "target-gp-master.internal"
  target_port: 5432
  target_database: "gpdb"
  target_user: "gpadmin"
  password_env: "PGSHADOW_DB_PASSWORD"
  target_dialect: "greenplum"
  workers: 128
  speed_factor: 0             # ASAP replay
  session_affinity: true
  error_policy: "skip"
  pool_max_conns: 128

metrics:
  enabled: true
  port: 9090
```

---

## Extended Query Protocol Support

pgshadow fully supports the PostgreSQL Extended Query sub-protocol (Parse/Bind/Execute), which is used by most modern drivers and ORMs (psycopg3, pgx, JDBC, GORM, SQLAlchemy, etc.).

### Capture Side

1. **Parse ('P')** — Records `{stmtName, SQL, paramOIDs}` via `BindCorrelator`
2. **Bind ('B')** — Correlates with the matching Parse, extracts parameter values and format codes (text/binary), produces a complete `SQLEvent{SQL, Params, Extended: true}`
3. **Execute ('E')** — Not processed separately; the SQL event is emitted at Bind time

### Replay Side

When replaying an Extended Query event with parameters, pgshadow uses `pgconn.ExecParams` to send the statement with its original:
- Parameter OIDs (type information from Parse)
- Parameter values (raw bytes from Bind)
- Parameter format codes (text=0 / binary=1, preserved per-parameter)

This ensures the target database receives an identical parameterized query — no string interpolation, no type coercion, no placeholder issues.

```go
// Replay path (simplified):
if ev.Extended && len(ev.Params) > 0 {
    paramOIDs, paramValues, paramFormats := splitParams(ev.Params)
    c.Conn().PgConn().ExecParams(ctx, ev.SQL, paramValues, paramOIDs, paramFormats, nil)
} else {
    c.Exec(ctx, ev.SQL)
}
```

### Supported Across All Capture Modes

| Mode | Extended Query | Notes |
|------|--------------|-------|
| `pcap` | ✅ | Requires plaintext PG traffic |
| `af_packet` | ✅ | Requires plaintext PG traffic |
| `ebpf` | ✅ | Requires plaintext PG traffic |
| `ebpf_ssl` | ✅ | Works with SSL-encrypted connections |

---

## Limitations

- COPY FROM STDIN: single COPY operation capped at 256 MiB in-memory buffering; larger bulk loads are skipped
- Single-goroutine TCP reassembly: may hit single-core bottleneck above 10 Gbps
- Greenplum: replays to Master only, no segment fan-out
- eBPF mode: requires Linux kernel ≥ 5.8 with BTF; on loopback interface runs in generic/SKB mode (slightly lower performance than native driver mode on physical NICs)
