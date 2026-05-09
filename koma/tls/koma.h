/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * Kernel Connection Multiplexor
 *
 * Copyright (c) 2016 Tom Herbert <tom@herbertland.com>
 */

#ifndef __NET_KOMA_H_
#define __NET_KOMA_H_

#include "linux/types.h"
#include <linux/skbuff.h>
#include <net/sock.h>
#include <uapi/linux/koma.h>
#include "strparser.h"

extern unsigned int koma_net_id;
#define NUM_KOMA_SOCKETS 20 // TODO: dynamically allocate it?
#define NUM_BUCKETS 1024

#define KOMA_STATS_ADD(stat, count) ((stat) += (count))
#define KOMA_STATS_INCR(stat) ((stat)++)

#define CMD_GET 0x00
#define CMD_GETK 0x0c
#define CMD_SET 0x01

struct __packed binary_header_t {
    u8 magic;
    u8 opcode;
    u32 id;
    u16 key_len;
    u8 extra_len;
    u8 data_type;
    union {
        u16 vbucket;
        u16 status;
    };
    u32 body_len;
    u32 opaque;
    u64 version;
};

struct koma_tx_msg {
    unsigned int sent;
    unsigned int frag_offset;
    unsigned int msg_flags;
    bool started_tx;
    struct koma_sock *ksk; // koma socket the msg was written from;
    struct koma_psock *psock; // cached psock for fast TX routing
    struct sk_buff *frag_skb;
    struct sk_buff *last_skb;
};

/* Socket structure for KCM client sockets */
struct koma_sock {
    /** @sock: generic socket data; must be the first field */
    struct sock sk;

    /* Koma specific fields  */

    /**
     * @lock: borrowed from Homa, which is supposed to be different from
     * sk->sk_lock,
     * TODO: figure out the diff. The lock must be held when modifying fields
     * such as interests and lists RPCs.
     */
    struct spinlock lock;

    struct koma_net *knet;

    /** @ksk_waiting_list: the list of koma available to receive new messages */
    struct list_head ksk_waiting_list;
    int index;

    /**
     * @slow_requests: Contains the messages which requires long service time.
     * The head is the oldest.
     */
    struct sk_buff_head slow_requests;

    /**
     * @fast_requests: Contains the messages which requires long service time.
     * The head is the oldest.
     */
    struct sk_buff_head fast_requests;

    /* kcm legacy fields */
    u32 done : 1;

    /* Transmit */
    struct sk_buff *seq_skb;
    u32 tx_stopped : 1;

    /* Don't use bit fields here, these are set under different locks */
    bool tx_wait;
    bool tx_wait_more;

    /* Receive */
    struct koma_psock *psock;
    u32 rx_disabled : 1;
    struct koma_psock *last_psock;

    /* for work stealing */
    struct sk_buff_head rx_q;
    size_t steal_cnt;
};

struct bpf_prog;

/* Structure for an attached lower socket */
struct koma_psock {
    struct list_head koma_psock_list;
    struct rcu_head rcu;
    struct koma_net *knet;
    struct sock *sk;
    struct strparser strp;
    int index;

    u32 tx_stopped : 1;
    u32 done : 1;
    u32 unattaching : 1;

    void (*save_state_change)(struct sock *sk);
    void (*save_data_ready)(struct sock *sk);
    void (*save_write_space)(struct sock *sk);

    struct list_head psock_list;

    struct bpf_prog *bpf_prog;

    /* From the previous koma_mux */
    spinlock_t lock ____cacheline_aligned_in_smp; /* mux locking */
    spinlock_t rx_lock ____cacheline_aligned_in_smp;
    spinlock_t tx_lock ____cacheline_aligned_in_smp; /* mux locking */

    /* New fields for koma */
    u32 hash_id; /* hash value based on the 5-tuple of tcp socket */

    /**
     * @hash_links: Used to link this object into a hash bucket for
     * koma->koma_psock_buckets.
     */
    struct hlist_node hash_links;

    /** @tx_in_use: Flag suggesting if the tx path is in usage (send_msg on)*/
    bool tx_in_use;

    /** @tx_wait_queue: the list of skbuffs waiting to get transmitted vis
     * sock_sendmsg() */
    struct sk_buff_head tx_wait_queue;

    /** @tx_work: work queue job called upon psock_write_space, writing messages
     * to tcp socket*/
    struct work_struct tx_work;
};

/* Per net psock list */
struct koma_net {
    struct mutex mutex;
    struct list_head psock_list;
    int count;
};

struct msghdr_tx_node {
    struct msghdr msg;
    /** @wait_tx_list: the linked list pointer to koma_psock->tx_wait_queue */
    struct list_head wait_tx_list;
};

struct koma_psock_bucket {
    /**
     * @lock: serves as a lock both for this bucket (e.g. when adding and
     * removing psocks) and also for all the psocks in the bucket. Must be held
     * whenever manipulating an psock in this bucket.
     */
    struct spinlock lock;
    struct hlist_head psocks;

    /**
     * @id: identifier for this bucket, used in error messages etc.
     * It is the index of the bucket within its hash table bucket array.
     */
    int id;
};

/**
 * struct koma - Global information about koma, including all koma sockets.
 *
 * There will typically only exist one of these at a time, except during
 * unit tests.
 */
struct koma {
    /**
     * @koma_socks: Contains all koma sockets in the network .
     */
    struct koma_sock *koma_socks[NUM_KOMA_SOCKETS];

    /* work-stealing */
    spinlock_t waiters_lock ____cacheline_aligned_in_smp;
    struct list_head ksk_waiters;
    DECLARE_BITMAP(waiters_bm, NUM_KOMA_SOCKETS);   // bitmap for koma sockets which are idle;
    DECLARE_BITMAP(rx_nonempty_bm, NUM_KOMA_SOCKETS); // bitmap for per-core message queues which have waiting messages.


    int koma_socks_cnt;

    spinlock_t lock ____cacheline_aligned_in_smp;

    /**
     * @koma_psock_buckets: Hash table for fast lookup of koma multiplexors.
     */
    struct koma_psock_bucket koma_psock_buckets[NUM_BUCKETS];
    int koma_psock_cnt;

    /**
     * @all_msgs: the skb linked list of all messsages. TODO: allocate a
     * specific single lock for it?
     */
    struct sk_buff_head all_msgs;
    size_t push_cnt;
    size_t pull_cnt;
    size_t all_steal_cnt;
};

static inline int koma_proc_init(void)
{
    return 0;
}
static inline void koma_proc_exit(void)
{
}

#endif /* __NET_KOMA_H_ */
