// SPDX-License-Identifier: GPL-2.0
// pgshadow XDP program: mirrors TCP packets matching the configured port into
// a BPF ring buffer for userspace consumption. Always returns XDP_PASS so the
// kernel delivers the packet normally (read-only mirror, R1.6).

#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

// Maximum capture size per packet.
#define FIXED_SNAP_LEN 4096

// Ring buffer for delivering mirrored packet data to userspace.
struct {
    __uint(type, BPF_MAP_TYPE_RINGBUF);
    __uint(max_entries, 64 * 1024 * 1024); // 64 MiB
} pkt_ring SEC(".maps");

// Per-CPU stats array: [0] = received (matched), [1] = dropped (ring full)
struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, __u64);
    __uint(max_entries, 2);
} stats SEC(".maps");

// Configuration map: [0] = target port (network byte order)
struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, __u16);
    __uint(max_entries, 1);
} cfg_port SEC(".maps");

#define STATS_RECEIVED 0
#define STATS_DROPPED  1

// Packet record in the ring buffer.
struct pkt_record {
    __u32 len;                    // actual frame length
    __u32 cap_len;                // actual captured bytes (≤ FIXED_SNAP_LEN)
    __u8  data[FIXED_SNAP_LEN];  // packet data (only cap_len bytes are valid)
};

SEC("xdp")
int pgshadow_xdp(struct xdp_md *ctx) {
    void *data     = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    // Parse Ethernet header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    __u16 eth_proto = bpf_ntohs(eth->h_proto);

    struct tcphdr *tcp = NULL;

    if (eth_proto == 0x0800) { // IPv4
        struct iphdr *ip = (void *)(eth + 1);
        if ((void *)(ip + 1) > data_end)
            return XDP_PASS;
        if (ip->protocol != 6)
            return XDP_PASS;
        __u32 ip_hlen = ip->ihl * 4;
        if (ip_hlen < 20 || ip_hlen > 60)
            return XDP_PASS;
        tcp = (void *)((char *)ip + ip_hlen);
        if ((void *)(tcp + 1) > data_end)
            return XDP_PASS;
    } else if (eth_proto == 0x86DD) { // IPv6
        struct ipv6hdr *ip6 = (void *)(eth + 1);
        if ((void *)(ip6 + 1) > data_end)
            return XDP_PASS;
        if (ip6->nexthdr != 6)
            return XDP_PASS;
        tcp = (void *)(ip6 + 1);
        if ((void *)(tcp + 1) > data_end)
            return XDP_PASS;
    } else {
        return XDP_PASS;
    }

    // Read target port from config map
    __u32 key = 0;
    __u16 *target_port = bpf_map_lookup_elem(&cfg_port, &key);
    if (!target_port)
        return XDP_PASS;

    // Match source OR destination port (bidirectional capture)
    if (tcp->source != *target_port && tcp->dest != *target_port)
        return XDP_PASS;

    // Calculate frame length
    __u32 frame_len = (__u32)(data_end - data);
    if (frame_len == 0)
        return XDP_PASS;

    // Cap to snap length
    __u32 cap_len = frame_len;
    if (cap_len > FIXED_SNAP_LEN)
        cap_len = FIXED_SNAP_LEN;

    // Reserve fixed-size slot in ring buffer (verifier requires constant size).
    struct pkt_record *rec = bpf_ringbuf_reserve(&pkt_ring, sizeof(struct pkt_record), 0);
    if (!rec) {
        __u32 drop_key = STATS_DROPPED;
        __u64 *drop_cnt = bpf_map_lookup_elem(&stats, &drop_key);
        if (drop_cnt)
            __sync_fetch_and_add(drop_cnt, 1);
        return XDP_PASS;
    }

    rec->len = frame_len;
    rec->cap_len = cap_len;

    // Direct memory copy from packet data. The verifier can prove safety
    // because we bound cap_len ≤ FIXED_SNAP_LEN and check data + cap_len ≤ data_end.
    if (data + cap_len > data_end) {
        // Should not happen given frame_len calculation, but satisfies verifier.
        bpf_ringbuf_discard(rec, 0);
        return XDP_PASS;
    }

    // Use bpf_probe_read_kernel to copy from XDP packet memory.
    // This is safe for XDP context since the packet lives in kernel memory.
    long ret = bpf_probe_read_kernel(rec->data, cap_len & (FIXED_SNAP_LEN - 1), data);
    if (ret < 0) {
        bpf_ringbuf_discard(rec, 0);
        return XDP_PASS;
    }

    bpf_ringbuf_submit(rec, 0);

    // Increment received counter
    __u32 recv_key = STATS_RECEIVED;
    __u64 *recv_cnt = bpf_map_lookup_elem(&stats, &recv_key);
    if (recv_cnt)
        __sync_fetch_and_add(recv_cnt, 1);

    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
