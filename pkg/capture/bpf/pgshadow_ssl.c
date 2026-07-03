// SPDX-License-Identifier: GPL-2.0
// pgshadow eBPF SSL uprobe program: hooks SSL_read/SSL_write return to capture
// plaintext data from TLS-encrypted PostgreSQL connections.
//
// Architecture:
//   Client → [TLS encrypted] → PostgreSQL process
//                                     |
//                               SSL_read() / SSL_write()
//                                     |
//                               uprobe/uretprobe captures plaintext
//                                     |
//                               ring buffer → userspace (pgshadow)
//
// This approach is fully passive — it never modifies traffic or interferes with
// the TLS session. It hooks the *return* of SSL_read/SSL_write to read the
// plaintext buffer after the OpenSSL function has completed.

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_tracing.h>
#include <bpf/bpf_core_read.h>

#define MAX_DATA_LEN 4096
#define MAX_ENTRIES  10240

// Direction: 0 = read (client→server from PG's perspective), 1 = write (server→client)
#define DIR_READ  0
#define DIR_WRITE 1

// Event passed to userspace via ring buffer.
struct ssl_event {
    __u32 pid;
    __u32 tid;
    __u32 fd;           // socket file descriptor
    __u32 data_len;     // actual bytes captured
    __u8  direction;    // DIR_READ or DIR_WRITE
    __u8  _pad[3];
    __u64 timestamp_ns;
    __u8  data[MAX_DATA_LEN];
};

// Ring buffer for SSL events.
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024); // 64 MiB
} ssl_events SEC(".maps");

// Per-thread state: save SSL_read/SSL_write arguments on entry so we can
// read the buffer on return. Key: tid (thread ID).
struct ssl_args {
    __u64 buf_ptr;  // pointer to the plaintext buffer
    __u32 fd;       // SSL object's underlying fd (obtained via SSL_get_fd)
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, __u64);   // pid_tgid
    __type(value, struct ssl_args);
    __uint(max_entries, MAX_ENTRIES);
} ssl_read_args SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, __u64);
    __type(value, struct ssl_args);
    __uint(max_entries, MAX_ENTRIES);
} ssl_write_args SEC(".maps");

// Optional: filter by target PID. If 0, capture all processes.
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u32);
    __uint(max_entries, 1);
} target_pid SEC(".maps");

// Per-CPU stats: [0] = events captured, [1] = events dropped
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, __u64);
    __uint(max_entries, 2);
} ssl_stats SEC(".maps");

#define STATS_CAPTURED 0
#define STATS_DROPPED  1

static __always_inline int should_trace() {
    __u32 key = 0;
    __u32 *filter_pid = bpf_map_lookup_elem(&target_pid, &key);
    if (filter_pid && *filter_pid != 0) {
        __u32 pid = bpf_get_current_pid_tgid() >> 32;
        if (pid != *filter_pid)
            return 0;
    }
    return 1;
}

// SSL_read entry: int SSL_read(SSL *ssl, void *buf, int num)
// Save the buffer pointer so we can read from it on return.
SEC("uprobe/ssl_read")
int uprobe_ssl_read_enter(struct pt_regs *ctx) {
    if (!should_trace())
        return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct ssl_args args = {};

    // Use CORE macros for architecture portability (arm64/x86_64)
    args.buf_ptr = PT_REGS_PARM2_CORE(ctx);
    args.fd = 0;

    bpf_map_update_elem(&ssl_read_args, &pid_tgid, &args, BPF_ANY);
    return 0;
}

// SSL_read return: return value = bytes read (or <=0 on error)
SEC("uretprobe/ssl_read")
int uretprobe_ssl_read_exit(struct pt_regs *ctx) {
    if (!should_trace())
        return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct ssl_args *args = bpf_map_lookup_elem(&ssl_read_args, &pid_tgid);
    if (!args)
        return 0;

    int ret = (int)PT_REGS_RC_CORE(ctx);
    bpf_map_delete_elem(&ssl_read_args, &pid_tgid);

    if (ret <= 0)
        return 0;

    __u32 data_len = (__u32)ret;
    if (data_len > MAX_DATA_LEN)
        data_len = MAX_DATA_LEN;

    // Reserve ring buffer entry
    struct ssl_event *event = bpf_ringbuf_reserve(&ssl_events, sizeof(struct ssl_event), 0);
    if (!event) {
        __u32 key = STATS_DROPPED;
        __u64 *cnt = bpf_map_lookup_elem(&ssl_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    event->pid = pid_tgid >> 32;
    event->tid = (__u32)pid_tgid;
    event->fd = args->fd;
    event->data_len = data_len;
    event->direction = DIR_READ;
    event->timestamp_ns = bpf_ktime_get_ns();

    // Read plaintext from the user-space buffer
    bpf_probe_read_user(event->data, data_len & (MAX_DATA_LEN - 1), (void *)args->buf_ptr);

    bpf_ringbuf_submit(event, 0);

    __u32 key = STATS_CAPTURED;
    __u64 *cnt = bpf_map_lookup_elem(&ssl_stats, &key);
    if (cnt) __sync_fetch_and_add(cnt, 1);

    return 0;
}

// SSL_write entry: int SSL_write(SSL *ssl, const void *buf, int num)
SEC("uprobe/ssl_write")
int uprobe_ssl_write_enter(struct pt_regs *ctx) {
    if (!should_trace())
        return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct ssl_args args = {};
    args.buf_ptr = PT_REGS_PARM2_CORE(ctx);
    args.fd = 0;

    bpf_map_update_elem(&ssl_write_args, &pid_tgid, &args, BPF_ANY);
    return 0;
}

// SSL_write return: return value = bytes written (or <=0 on error)
SEC("uretprobe/ssl_write")
int uretprobe_ssl_write_exit(struct pt_regs *ctx) {
    if (!should_trace())
        return 0;

    __u64 pid_tgid = bpf_get_current_pid_tgid();
    struct ssl_args *args = bpf_map_lookup_elem(&ssl_write_args, &pid_tgid);
    if (!args)
        return 0;

    int ret = (int)PT_REGS_RC_CORE(ctx);
    bpf_map_delete_elem(&ssl_write_args, &pid_tgid);

    if (ret <= 0)
        return 0;

    __u32 data_len = (__u32)ret;
    if (data_len > MAX_DATA_LEN)
        data_len = MAX_DATA_LEN;

    struct ssl_event *event = bpf_ringbuf_reserve(&ssl_events, sizeof(struct ssl_event), 0);
    if (!event) {
        __u32 key = STATS_DROPPED;
        __u64 *cnt = bpf_map_lookup_elem(&ssl_stats, &key);
        if (cnt) __sync_fetch_and_add(cnt, 1);
        return 0;
    }

    event->pid = pid_tgid >> 32;
    event->tid = (__u32)pid_tgid;
    event->fd = args->fd;
    event->data_len = data_len;
    event->direction = DIR_WRITE;
    event->timestamp_ns = bpf_ktime_get_ns();

    bpf_probe_read_user(event->data, data_len & (MAX_DATA_LEN - 1), (void *)args->buf_ptr);

    bpf_ringbuf_submit(event, 0);

    __u32 key = STATS_CAPTURED;
    __u64 *cnt = bpf_map_lookup_elem(&ssl_stats, &key);
    if (cnt) __sync_fetch_and_add(cnt, 1);

    return 0;
}

char _license[] SEC("license") = "GPL";
