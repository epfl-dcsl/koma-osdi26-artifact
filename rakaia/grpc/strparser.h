/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * Stream Parser
 *
 * Copyright (c) 2016 Tom Herbert <tom@herbertland.com>
 */

#ifndef __RAKAIA_STRPARSER_H_
#define __RAKAIA_STRPARSER_H_

#include <linux/skbuff.h>
#include <net/sock.h>
#include <linux/hashtable.h>
#include <linux/slab.h>

#define STREAM_HASH_BITS 4

#define STREAM_HASH_SIZE (1 << STREAM_HASH_BITS)
#define STREAM_HASH_MASK (STREAM_HASH_SIZE - 1)

// Frame types
#define FRAME_TYPE_DATA 0x0
#define FRAME_TYPE_HEADERS 0x1
#define FRAME_TYPE_PRIORITY 0x2
#define FRAME_TYPE_RST_STREAM 0x3
#define FRAME_TYPE_SETTINGS 0x4
#define FRAME_TYPE_PUSH_PROMISE 0x5
#define FRAME_TYPE_PING 0x6
#define FRAME_TYPE_GOAWAY 0x7
#define FRAME_TYPE_WINDOW_UPDATE 0x8
#define FRAME_TYPE_CONTINUATION 0x9

// Common flags (used per frame-type where valid)
#define FLAG_END_STREAM 0x1 // For DATA and HEADERS
#define FLAG_ACK 0x1 // For SETTINGS and PING
#define FLAG_END_HEADERS 0x4 // For HEADERS, PUSH_PROMISE, CONTINUATION
#define FLAG_PADDED 0x8 // For DATA, HEADERS, PUSH_PROMISE
#define FLAG_PRIORITY 0x20 // For HEADERS

#define STRP_STATS_ADD(stat, count) ((stat) += (count))
#define STRP_STATS_INCR(stat) ((stat)++)

// struct h2_header {
//     u32 stream_id;
//     u32 length;
//     u16 type : 8;
//     u16 flag : 8;
// };

struct h2_stream_buffer {
    uint32_t stream_id;
    struct sk_buff *skb;
    bool complete;
    struct hlist_node hnode; // For linking into the hashtable
};

struct strp_stats {
    unsigned long long msgs;
    unsigned long long bytes;
    unsigned int mem_fail;
    unsigned int need_more_hdr;
    unsigned int msg_too_big;
    unsigned int msg_timeouts;
    unsigned int bad_hdr_len;
};

struct strp_aggr_stats {
    unsigned long long msgs;
    unsigned long long bytes;
    unsigned int mem_fail;
    unsigned int need_more_hdr;
    unsigned int msg_too_big;
    unsigned int msg_timeouts;
    unsigned int bad_hdr_len;
    unsigned int aborts;
    unsigned int interrupted;
    unsigned int unrecov_intr;
};

struct strparser;

/* Callbacks are called with lock held for the attached socket */
struct strp_callbacks {
    int (*parse_msg)(struct strparser *strp, struct sk_buff *skb);
    void (*rcv_msg)(struct strparser *strp, struct sk_buff *skb);
    int (*read_sock_done)(struct strparser *strp, int err);
    void (*abort_parser)(struct strparser *strp, int err);
    void (*lock)(struct strparser *strp);
    void (*unlock)(struct strparser *strp);
};

struct strp_msg {
    int full_len;
    int offset;
    u32 stream_id;
    // u32 length;
    u16 type : 8;
    u16 flag : 8;
};

struct _strp_msg {
    /* Internal cb structure. struct strp_msg must be first for passing
     *      * to upper layer.
     *           */
    struct strp_msg strp;
    int accum_len;
};

struct sk_skb_cb {
    unsigned char data[16];
    /* align strp on cache line boundary within skb->cb[] */
    struct _strp_msg strp;

    /* strp users' data follows */
    struct tls_msg {
        u8 control;
    } tls;
    /* temp_reg is a temporary register used for bpf_convert_data_end_access
     *      * when dst_reg == src_reg.
     *           */
    struct sk_buff *next;
    // u64 temp_reg;
};

static inline struct strp_msg *strp_msg(struct sk_buff *skb)
{
    return (struct strp_msg *)((void *)skb->cb +
                               offsetof(struct sk_skb_cb, strp));
}

static inline struct sk_skb_cb *STRP_SKB_CB(struct sk_buff *skb)
{
    return (struct sk_skb_cb *)skb->cb;
}

#define strp_next(skb) (STRP_SKB_CB(skb)->next)


/* Structure for an attached lower socket */
struct strparser {
    struct sock *sk;

    u32 stopped : 1;
    u32 paused : 1;
    u32 aborted : 1;
    u32 interrupted : 1;
    u32 unrecov_intr : 1;
    u32 http2 : 1;

    struct sk_buff **skb_nextp;
    struct sk_buff *skb_head;
    unsigned int need_bytes;
    struct delayed_work msg_timer_work;
    struct work_struct work;
    struct strp_stats stats;
    struct strp_callbacks cb;
    DECLARE_HASHTABLE(stream_storage, STREAM_HASH_BITS);
};

/* Must be called with lock held for attached socket */
static inline void strp_pause(struct strparser *strp)
{
    strp->paused = 1;
}

/* May be called without holding lock for attached socket */
void strp_unpause(struct strparser *strp);
/* Must be called with process lock held (lock_sock) */
void __strp_unpause(struct strparser *strp);

static inline void save_strp_stats(struct strparser *strp,
                                   struct strp_aggr_stats *agg_stats)
{
    /* Save psock statistics in the mux when psock is being unattached. */

#define SAVE_PSOCK_STATS(_stat) (agg_stats->_stat += strp->stats._stat)
    SAVE_PSOCK_STATS(msgs);
    SAVE_PSOCK_STATS(bytes);
    SAVE_PSOCK_STATS(mem_fail);
    SAVE_PSOCK_STATS(need_more_hdr);
    SAVE_PSOCK_STATS(msg_too_big);
    SAVE_PSOCK_STATS(msg_timeouts);
    SAVE_PSOCK_STATS(bad_hdr_len);
#undef SAVE_PSOCK_STATS

    if (strp->aborted)
        agg_stats->aborts++;
    if (strp->interrupted)
        agg_stats->interrupted++;
    if (strp->unrecov_intr)
        agg_stats->unrecov_intr++;
}

static inline void aggregate_strp_stats(struct strp_aggr_stats *stats,
                                        struct strp_aggr_stats *agg_stats)
{
#define SAVE_PSOCK_STATS(_stat) (agg_stats->_stat += stats->_stat)
    SAVE_PSOCK_STATS(msgs);
    SAVE_PSOCK_STATS(bytes);
    SAVE_PSOCK_STATS(mem_fail);
    SAVE_PSOCK_STATS(need_more_hdr);
    SAVE_PSOCK_STATS(msg_too_big);
    SAVE_PSOCK_STATS(msg_timeouts);
    SAVE_PSOCK_STATS(bad_hdr_len);
    SAVE_PSOCK_STATS(aborts);
    SAVE_PSOCK_STATS(interrupted);
    SAVE_PSOCK_STATS(unrecov_intr);
#undef SAVE_PSOCK_STATS
}

void strp_done(struct strparser *strp);
void strp_stop(struct strparser *strp);
void strp_check_rcv(struct strparser *strp);
int strp_init(struct strparser *strp, struct sock *sk,
              const struct strp_callbacks *cb);
void strp_data_ready(struct strparser *strp);
int strp_process(struct strparser *strp, struct sk_buff *orig_skb,
                 unsigned int orig_offset, size_t orig_len, size_t max_msg_size,
                 long timeo);

#endif /* __RAKAIA_STRPARSER_H_ */
