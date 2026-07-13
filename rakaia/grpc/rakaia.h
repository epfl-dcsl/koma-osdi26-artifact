/* SPDX-License-Identifier: GPL-2.0-only */
/*
 * Kernel Connection Multiplexor
 *
 * Copyright (c) 2016 Tom Herbert <tom@herbertland.com>
 */

#ifndef __NET_RAKAIA_H_
#define __NET_RAKAIA_H_

#include "linux/types.h"
#include <linux/skbuff.h>
#include <net/sock.h>
#include <uapi/linux/rakaia.h>
#include "strparser.h"

extern unsigned int rakaia_net_id;
#define NUM_RAKAIA_SOCKETS 20 // TODO: dynamically allocate it?
#define NUM_BUCKETS 1024

#define RAKAIA_ATTACH_EXTENDED_BPF_FD (-2)
#define HTTP2_DEFAULT_CONNECTION_WINDOW 65535
#define INITIAL_WINDOW_SIZE 126553500
// #define INITIAL_WINDOW_SIZE 65535

#define RAKAIA_STATS_ADD(stat, count) ((stat) += (count))
#define RAKAIA_STATS_INCR(stat) ((stat)++)

/*
 * Keep attach ioctl parsing private to the module so we can accept an
 * extended userspace payload without depending on the installed UAPI header.
 */
struct rakaia_attach_user_legacy {
    int fd;
    int bpf_fd;
};

struct rakaia_attach_user {
    int fd;
    int bpf_fd;
    int initial_conn_window;
};

/* Socket structure for KCM client sockets */
struct rakaia_sock {
    /** @sock: generic socket data; must be the first field */
    struct sock sk;

    /* Rakaia specific fields  */

    /**
     * @lock: borrowed from Homa, which is supposed to be different from
     * sk->sk_lock,
     * TODO: figure out the diff. The lock must be held when modifying fields
     * such as interests and lists RPCs.
     */
    struct spinlock lock;

    struct rakaia_net *knet;

    /** @ksk_waiting_list: the list of rakaia available to receive new messages */
    struct list_head ksk_waiting_list;
    int index;

    /* kcm legacy fields */
    u32 done : 1;

    /* Transmit */
    struct sk_buff *seq_skb;
    u32 tx_stopped : 1;

    /* Don't use bit fields here, these are set under different locks */
    bool tx_wait;
    bool tx_wait_more;

    /* Receive */
    struct rakaia_psock *psock;
    u32 rx_disabled : 1;

    /* for debugging */
    size_t msg_cnt;
    size_t accumu_cnt;
    
    struct rakaia_psock *last_psock;

    /* for work stealing */
    struct sk_buff_head rx_q;
    size_t steal_cnt;

};

/* Structure for an attached lower socket */
struct rakaia_psock {
    struct list_head rakaia_psock_list;
    struct rcu_head rcu;
    struct rakaia_net *knet;
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

    /* From the previous rakaia_mux */
    spinlock_t lock ____cacheline_aligned_in_smp; /* mux locking */
    spinlock_t rx_lock ____cacheline_aligned_in_smp;
    spinlock_t tx_lock ____cacheline_aligned_in_smp; /* mux locking */

    /* New fields for rakaia */
    u32 hash_id; /* hash value based on the 5-tuple of tcp socket */

    /** @dport: Port number on the client at the other end point (network)
     * order) */
    u16 dport;
    /** @daddr: IP address of the client at the other end point (network order)
     */
    u32 daddr;

    /**
     * @hash_links: Used to link this object into a hash bucket for
     * rakaia->rakaia_psock_buckets.
     */
    struct hlist_node hash_links;

    /** @tx_in_use: Flag suggesting if the tx path is in usage (send_msg on)*/
    atomic_t tx_in_use;

    /** @tx_wait_queue: the list of skbuffs being accumulated, which are waiting
     * to be added into the write queue. protected with psock->tx_lock */
    struct sk_buff_head tx_wait_queue;

    /**
     * @wait_queue_waiters: thread (in process context) waiting to acquire the
     * exclusive access of psock->tx_wait_queue;
     */
    int wait_queue_waiters;

    /** @tx_in_use: Flag suggesting if the tx_wait_queue is in usage
     * (other thread currently accumulating msgs in the queue)*/
    bool wait_queue_in_use;

    /** @tx_work: work queue job called upon psock_write_space, writing messages
     * to tcp socket*/
    struct work_struct tx_work;

    // tmp for accumulation test
    int accumu;

    // for process_backlog in tcp_sendmsg_locked
    int process_backlog;

    // gRPC specific: local receive-side connection flow-control window
    int recv_conn_window;
    int unacked; // bytes received but not acked with a window_update

    // gRPC specific: for tx connection-level flow control, note that grabbing
    // tx lock is necessary for changing the
    int send_quota;
    bool flow_stalled;
};

/* Per net psock list */
struct rakaia_net {
    struct spinlock lock;
    struct list_head psock_list;
    int count;
};

struct msghdr_tx_node {
    struct msghdr msg;
    /** @wait_tx_list: the linked list pointer to rakaia_psock->tx_wait_queue */
    struct list_head wait_tx_list;
};

struct rakaia_psock_bucket {
    struct hlist_head psocks;

    /**
     * @id: identifier for this bucket, used in error messages etc.
     * It is the index of the bucket within its hash table bucket array.
     */
    int id;
};

/**
 * struct rakaia - Global information about rakaia, including all rakaia sockets.
 *
 * There will typically only exist one of these at a time, except during
 * unit tests.
 */
struct rakaia {
    /**
     * @rakaia_socks: Contains all rakaia sockets in the network .
     */
    struct rakaia_sock *rakaia_socks[NUM_RAKAIA_SOCKETS];


    int rakaia_socks_cnt;

    spinlock_t lock ____cacheline_aligned_in_smp;

    /* work-stealing */
    spinlock_t waiters_lock ____cacheline_aligned_in_smp;
    struct list_head ksk_waiters;
    DECLARE_BITMAP(waiters_bm, NUM_RAKAIA_SOCKETS);   // bitmap for rakaia sockets which are idle;
    DECLARE_BITMAP(rx_nonempty_bm, NUM_RAKAIA_SOCKETS); // bitmap for per-core message queues which have waiting messages.

    /**
     * @rakaia_psock_buckets: Hash table for fast lookup of rakaia multiplexors.
     */
    struct rakaia_psock_bucket rakaia_psock_buckets[NUM_BUCKETS];

    /**
     * @all_msgs: the skb linked list of all messsages. TODO: allocate a
     * specific single lock for it?
     */
    struct sk_buff_head all_msgs;
    size_t push_cnt;
    size_t pull_cnt;

    /* for debugging */
    size_t all_msg_rcv_cnt;
    size_t snd_cnt;
    size_t accumu_cnt;
    size_t all_msg_cnt;
    size_t all_steal_cnt;

};

static inline int rakaia_proc_init(void)
{
    return 0;
}
static inline void rakaia_proc_exit(void)
{
}

void rakaia_schedule_tx(struct rakaia_psock *psock);
void try_send_from_psock(int ksk_index, struct rakaia_psock *psock, int flags,
                         int mss_now, int size_goal, u32 mark);
#endif /* __NET_RAKAIA_H_ */
