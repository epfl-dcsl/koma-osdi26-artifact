// SPDX-License-Identifier: GPL-2.0-only
/*
 * Kernel Connection Multiplexor
 *
 * Copyright (c) 2016 Tom Herbert <tom@herbertland.com>
 */

#include "linux/printk.h"
#include "linux/spinlock.h"
#include "strparser.h"
#include <linux/bpf.h>
#include <linux/errno.h>
#include <linux/errqueue.h>
#include <linux/file.h>
#include <linux/filter.h>
#include <linux/in.h>
#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/net.h>
#include <linux/netdevice.h>
#include <linux/poll.h>
#include <linux/rculist.h>
#include <linux/sched/signal.h>
#include <linux/skbuff.h>
#include <linux/socket.h>
#include <linux/syscalls.h>
#include <linux/uaccess.h>
#include <linux/workqueue.h>
#include <linux/jhash.h>

#include <net/tcp.h>
#include <net/netns/generic.h>
#include <net/sock.h>
#include <trace/events/sock.h>
#include <uapi/linux/koma.h> //TODO: add the header file in the specified path

#include "koma.h"
#include "tcp_send.h"
#include "logging.h"

unsigned int koma_net_id;
extern struct workqueue_struct *strp_wq;
static struct kmem_cache *koma_psockp __read_mostly;
extern struct kmem_cache *h2_stream_bufferp;
static struct workqueue_struct *koma_wq;

/* Global data for koma, which allows */
struct koma koma_data = { 0 };
struct koma *koma = &koma_data;

static inline struct koma_sock *koma_sk(const struct sock *sk)
{
    return (struct koma_sock *)sk;
}

static void report_csk_error(struct sock *csk, int err)
{
    csk->sk_err = EPIPE;
    sk_error_report(csk);
}

static int koma_pull(struct socket *sock);

static int copy_koma_attach_from_user(struct koma_attach_user *info,
                                      void __user *arg)
{
    struct koma_attach_user_legacy legacy;

    memset(info, 0, sizeof(*info));
    if (copy_from_user(&legacy, arg, sizeof(legacy)))
        return -EFAULT;

    info->fd = legacy.fd;
    info->bpf_fd = legacy.bpf_fd;
    if (legacy.bpf_fd != KOMA_ATTACH_EXTENDED_BPF_FD)
        return 0;

    if (copy_from_user(info, arg, sizeof(*info)))
        return -EFAULT;

    return 0;
}

/**
 * hash_tcp_tuple() - Impkements the five-tuple hash function for a given tcp
 * socket.
 * @saddr:   the remote ip addr.
 * @sport:   the remote port.
 * Return:  the 32bit hash value.
 */
static u32 hash_tcp_tuple(u32 saddr, u16 sport)
{
    return jhash_2words(saddr, sport, 0);
}

// Function to get the size of a linked list
size_t get_list_size(const struct sk_buff_head *list)
{
    size_t count = 0;
    struct sk_buff *skb;

    // Use the skb_queue_walk macro to traverse the list
    skb_queue_walk(list, skb)
    {
        count++;
    }

    return count;
}

static void koma_free_strp_chain(struct sk_buff *head)
{
    struct sk_buff *skb = head;
    while (skb) {
        struct sk_buff *next = strp_next(skb);
        strp_next(skb) = NULL;   // defensive: break chain
        kfree_skb(skb);
        skb = next;
    }
}

static void skb_queue_purge_koma(struct sk_buff_head *list)
{
    struct sk_buff *skb, *iter, *next;
    while ((skb = __skb_dequeue(list)) != NULL) {
            koma_free_strp_chain(skb);
    }
}

static int koma_queue_rcv_skb(struct koma_sock *ksk, struct sk_buff *skb);

/* Pick a uniformly random *set* bit from mask */
static inline int pick_random_set_bit(unsigned long mask)
{
    int total;
    int k;
    int bit;

    if (mask == 0)
        return -1;

    total = hweight_long(mask);          // number of set bits
    k = get_random_u8() % total;          // pick index in [0, total-1]

    for_each_set_bit(bit, &mask, BITS_PER_LONG) {
        if (k-- == 0)
            return bit;
    }

    return -1;  // unreachable
}

/**
 * koma_steal_skb - try to steal one skb from another koma socket.
 */
static struct sk_buff *koma_steal_skb(struct koma_sock *ksk)
{
    unsigned long mask;
    int v1, v2, victim;
    struct koma_sock *s1, *s2;
    struct sk_buff_head *list;
    struct sk_buff *skb;

retry:
    /* Fast global liveness check */
    mask = READ_ONCE(koma->rx_nonempty_bm[0]);
    if (unlikely(!mask))
        return NULL;

    /* === Power-of-Two victim selection (load balancing) === */
    v1 = pick_random_set_bit(mask);
    if (unlikely(v1 < 0))
        return NULL;

    v2 = v1;
    if (hweight_long(mask) > 1) {
        do {
            v2 = pick_random_set_bit(mask);
        } while (v2 == v1);
    }

    s1 = koma->koma_socks[v1];
    s2 = koma->koma_socks[v2];

    if (READ_ONCE(s2->rx_q.qlen) > READ_ONCE(s1->rx_q.qlen))
        victim = v2;
    else
        victim = v1;

    list = &koma->koma_socks[victim]->rx_q;

    spin_lock_bh(&list->lock);
    skb = __skb_dequeue(list);

    if (skb) {
        if (list->qlen == 0)
            clear_bit(victim, koma->rx_nonempty_bm);

        ksk->steal_cnt++;
        spin_unlock_bh(&list->lock);
        return skb;
    }
    spin_unlock_bh(&list->lock);

    goto retry;
}

/**
 * koma_get_avail_sk() - pop one idle koma socket from the waiter list. 
 * must only be called with waiters_lock held. 
 * Return:  the available koma socket.
 */
static struct koma_sock *koma_get_avail_sk(struct koma_sock *local_ksk)
{
    struct koma_sock *ksk = NULL;

    if (list_empty(&koma->ksk_waiters)){
        return NULL;
    }

    /* Prefer local koma_sock first */
    if (local_ksk && test_bit(local_ksk->index, koma->waiters_bm)){
        // local koma_sock is idle
        ksk = local_ksk;
    } else {
        ksk = list_first_entry(&koma->ksk_waiters, struct koma_sock,
                               ksk_waiting_list);
    }
    list_del_init(&ksk->ksk_waiting_list);
    clear_bit(ksk->index, koma->waiters_bm);
    return ksk;
}

/**
 * koma_push_skb_single_queue() - the central scheduler pushes the skb to an
 * available koma socket.
 * @skb:    the skb to be pushed to a koma socket.
 * @psock:  psock connected to the tcp sock where the skb is from.
 * Return:  0
 */
static int koma_push_skb_single_queue(struct sk_buff *skb,
                                      struct koma_psock *psock)
{

    struct koma_sock *ksk1, *ksk2, *chosen, *new_ksk;
    int num1, num2;
    struct sk_buff_head *list;

    skb->dev = NULL;
    skb_orphan(skb);
    *(struct koma_psock**)&skb->cb[0] = psock;

    num1 = get_random_u8() % NUM_KOMA_SOCKETS;
    num2 = get_random_u8() % NUM_KOMA_SOCKETS;

    ksk1 = koma->koma_socks[num1];
    ksk2 = koma->koma_socks[num2];

    if (READ_ONCE(ksk1->rx_q.qlen) <= READ_ONCE(ksk2->rx_q.qlen))
        chosen = ksk1;
    else
        chosen = ksk2;
    
    list = &chosen->rx_q;

    trace_record("%u rx: push skb to the central queue\n", skb->mark);

    spin_lock_bh(&list->lock);
    if (list->qlen >= 1){
        __skb_queue_tail(list,skb);
       spin_unlock_bh(&list->lock); 
       return 0;
    }

    spin_lock_bh(&koma->waiters_lock);
    __skb_queue_tail(list, skb);
    new_ksk = koma_get_avail_sk(chosen);
    spin_unlock_bh(&koma->waiters_lock);

    if (!new_ksk) {
        set_bit(chosen->index, koma->rx_nonempty_bm);
        spin_unlock_bh(&list->lock);
        return 0;
    }

    skb = __skb_dequeue(list);
    if (list->qlen >= 1) 
        set_bit(chosen->index, koma->rx_nonempty_bm);
    else 
        clear_bit(chosen->index, koma->rx_nonempty_bm);
    spin_unlock_bh(&list->lock);
    koma->push_cnt++;
    return koma_queue_rcv_skb(new_ksk, skb);
}

/**
 * koma_rfree() - callback function triggered when a skb is destructed.
 * The context for this function is before the skb is queued to a koma socket.
 * @skb:    the skb which is destructed.
 */
static void koma_rfree(struct sk_buff *skb) { }

/**
 * koma_queue_rcv_skb() - queue a skb to a specific koma socket.
 * @ksk:    the koma socket where the skb is to be queued at.
 * @skb:    the skb to be queue at the koma socket.
 * Return:  0 on success.
 */
static int koma_queue_rcv_skb(struct koma_sock *ksk, struct sk_buff *skb)
{
    struct sock *sk = &ksk->sk;
    struct sk_buff_head *list = &sk->sk_receive_queue;

    // pr_info("[strp] koma_queue_rcv_skb %p, strp_next %p\n", skb,
    // strp_next(skb));
    skb->sk = &ksk->sk;
    skb->destructor = koma_rfree;

    skb_queue_tail(list, skb);

    // /* after enqueue */
    // if (skb->prev == NULL || skb->next == NULL) {
    //     pr_info(
    //         "[dbg] enqueue: post tail link invalid: skb=%p prev=%p next=%p list=%p head(prev=%p next=%p)\n",
    //         skb, skb->prev, skb->next, list, list ? list->prev : NULL,
    //         list ? list->next : NULL);
    // }

    if (!sock_flag(sk, SOCK_DEAD))
        sk->sk_data_ready(sk); // wake up application thread.
    return 0;
}

static void koma_done(struct koma_sock *koma);

/**
 * psock_data_ready() - callback function substituting the original
 * tcp_data_ready function. Used to link to strparser and call strp_data_ready
 * to parse requests from tcp data streams.
 * @ksk:    the koma socket where the skb is to be queued at.
 * @skb:    the skb to be queue at the koma socket.
 * Return:  0 on success.
 */
static void psock_data_ready(struct sock *sk)
{
    struct koma_psock *psock;

    trace_sk_data_ready(sk);

    read_lock_bh(&sk->sk_callback_lock);

    psock = (struct koma_psock *)sk->sk_user_data;

    if (likely(psock))
        strp_data_ready(&psock->strp);

    read_unlock_bh(&sk->sk_callback_lock);
}

/**
 * koma_rcv_strparser() - callback function when strparser gets a full message.
 * Called with lower sock held.
 * @strp:   strparser instance.
 * @skb:    the skb to be queued & contains the message.
 */
static void koma_rcv_strparser(struct strparser *strp, struct sk_buff *skb)
{
    koma->all_msg_rcv_cnt++;
    struct koma_psock *psock = container_of(strp, struct koma_psock, strp);
    // pr_info("[strp] koma_rcv_strparser %p, strp_next %p\n", skb,
    // strp_next(skb));
    koma_push_skb_single_queue(skb, psock);
}

static void psock_state_change(struct sock *sk)
{
    /* TCP only does a EPOLLIN for a half close. Do a EPOLLHUP here
     * since application will normally not poll with EPOLLIN
     * on the TCP sockets.
     */

    report_csk_error(sk, EPIPE);
}

static void koma_tx_work(struct work_struct *w)
{
    struct koma_psock *psock = container_of(w, struct koma_psock, tx_work);
    struct sock *csk = psock->sk;
    struct tcp_sock *tp;
    int size_goal;
    int mss_now;

    if (!csk)
        return;

    tp = tcp_sk(csk);

    /* Best-effort MSS/size_goal; use nonblocking flags */
    mss_now = tcp_send_mss(csk, &size_goal, MSG_DONTWAIT);

    /* No associated koma_sock here for stats; pass -1 as index */
    try_send_from_psock(0, psock, MSG_DONTWAIT, mss_now, size_goal, 0);
}

void koma_schedule_tx(struct koma_psock *psock)
{
    if (unlikely(!psock || !psock->sk))
        return;

    queue_work(koma_wq, &psock->tx_work);
}

static void psock_write_space(struct sock *sk)
{
    struct koma_psock *psock;
    struct sk_buff *head;
    read_lock_bh(&sk->sk_callback_lock);

    psock = (struct koma_psock *)sk->sk_user_data;
    // spin_lock_bh(&psock->lock);
    // head = skb_peek(&psock->tx_wait_queue);
    // if (head)
    //     pr_info("%u peek skb in psock_write_space", head->mark);
    // psock->tx_in_use = false;
    // spin_unlock_bh(&psock->lock);
    if (likely(psock))
        koma_schedule_tx(psock);
    read_unlock_bh(&sk->sk_callback_lock);
}

void __release_sock(struct sock *sk)
	__releases(&sk->sk_lock.slock)
	__acquires(&sk->sk_lock.slock)
{
	struct sk_buff *skb, *next;

	while ((skb = sk->sk_backlog.head) != NULL) {
		sk->sk_backlog.head = sk->sk_backlog.tail = NULL;

		spin_unlock_bh(&sk->sk_lock.slock);

		do {
			next = skb->next;
			prefetch(next);
			DEBUG_NET_WARN_ON_ONCE(skb_dst_is_noref(skb));
			skb_mark_not_on_list(skb);
			tcp_v4_do_rcv(sk, skb);

			cond_resched();

			skb = next;
		} while (skb != NULL);

		spin_lock_bh(&sk->sk_lock.slock);
	}

	/*
	 * Doing the zeroing here guarantee we can not loop forever
	 * while a wild producer attempts to flood us.
	 */
	sk->sk_backlog.len = 0;
}

/**
 * try_send_from_psock() - try to send messages from the psock->tx_wait_queue.
 * It involves two phrases: i) move the skbs from psock->tx_wait_queue to
 * sk_write_queue; ii) try to call tcp_send() to send existing skbs in he
 * sk_write_queue. Note that this function should only be called when exclusive
 * access to psock->tx_wait_queue is guaranteed.
 * @psock:      the psock which has the accumulation queue.
 * @flags:      the flags of the message (msg->msg_flags)
 */
void try_send_from_psock(int ksk_index, struct koma_psock *psock, int flags,
                         int mss_now, int size_goal, u32 mark)
{
    struct sock *csk = psock->sk;
    struct tcp_sock *tp = tcp_sk(csk);
    struct sk_buff *head = NULL, *skb = NULL;
    unsigned long sflags;
    struct sockcm_cookie sockc;

    if (atomic_cmpxchg(&psock->tx_in_use, 0, 1)){
        return;
    }
    lock_sock(csk);

    // /* The sk_lock has mutex_lock() semantics here. */
    // mutex_acquire(&csk->sk_lock.dep_map, 0, 0, _RET_IP_);

    // might_sleep();
    // spin_lock_bh(&csk->sk_lock.slock);
    // if (sock_owned_by_user_nocheck(csk)) {
    //     __releases(&csk->sk_lock.slock) __acquires(&csk->sk_lock.slock)
    //         spin_unlock_bh(&csk->sk_lock.slock);
    //     return;
    //     // {
    //     //     DEFINE_WAIT(wait);

    //     //     for (;;) {
    //     //         prepare_to_wait_exclusive(&csk->sk_lock.wq, &wait,
    //     //                                   TASK_UNINTERRUPTIBLE);
    //     //         spin_unlock_bh(&csk->sk_lock.slock);
    //     //         schedule();
    //     //         spin_lock_bh(&csk->sk_lock.slock);
    //     //         if (!sock_owned_by_user(csk))
    //     //             break;
    //     //     }
    //     //     finish_wait(&csk->sk_lock.wq, &wait);
    //     // }
    // }
    // csk->sk_lock.owned = 1;
    // spin_unlock_bh(&csk->sk_lock.slock);

    koma->snd_cnt++;

    while (true) {
        // move all skbs from tmp queue to to tcp tx queue.
        spin_lock_bh(&psock->tx_lock);
        if (skb_queue_empty(&psock->tx_wait_queue)) {
            spin_unlock_bh(&psock->tx_lock);
            break;
        }
        head = (&psock->tx_wait_queue)->next;
        skb_queue_splice_tail_init(&(psock->tx_wait_queue),
                                   &(csk->sk_write_queue));
        spin_unlock_bh(&psock->tx_lock);

        for (skb = head; skb != (struct sk_buff *)(&csk->sk_write_queue);
             skb = skb->next) {
            trace_record("%u tx: move message to send_queue\n", skb->mark);
            struct tcp_skb_cb *tcb = TCP_SKB_CB(skb);
            tcb->end_seq = tp->write_seq + TCP_SKB_CB(skb)->seq;
            tcb->seq = tp->write_seq;
            tp->write_seq = tcb->end_seq;
        }

        // TODO: synchronization for accessing psock->process_backlog
        if (READ_ONCE(psock->process_backlog) >= 16) {
            WRITE_ONCE(psock->process_backlog, 0);
            sk_flush_backlog(csk);
        }

        sk_clear_bit(SOCKWQ_ASYNC_NOSPACE, csk);
        tcp_tx_timestamp(csk, sockc.tsflags);

        skb_queue_walk(&csk->sk_write_queue, head)
        {
            trace_record("%u tx: start send message through tcp_send\n",
                         head->mark);
        }
        // pr_info_id("%d call tcp_push!\n", ksk_index);
        tcp_push(csk, flags, mss_now, tp->nonagle, size_goal);
    }

    atomic_set(&psock->tx_in_use, 0);

    // customized release_sock
    spin_lock_bh(&csk->sk_lock.slock);
	if (csk->sk_backlog.tail)
		__release_sock(csk);

	if (csk->sk_prot->release_cb)
		INDIRECT_CALL_INET_1(csk->sk_prot->release_cb,
				     tcp_release_cb, csk);

	sock_release_ownership(csk);
	if (waitqueue_active(&csk->sk_lock.wq))
		wake_up(&csk->sk_lock.wq);
    pr_info_id("%d release lock_sock!\n", ksk_index);
	spin_unlock_bh(&csk->sk_lock.slock);

}

int koma_sendmsg(struct socket *sock, struct msghdr *msg, size_t len)
{
    struct sock *sk = sock->sk;
    struct sockaddr_in *in4 = msg->msg_name;
    struct koma_psock *psock = NULL;
    struct koma_psock_bucket *bucket;
    struct cmsghdr *cmsg;
    struct sock *csk;
    struct tcp_sock *tp;
    struct sk_buff *skb = NULL, *skb_full = NULL;
    struct sk_buff_head cur_msg;
    struct koma_sock *ksk = koma_sk(sk);
    unsigned long sflags;

    u32 mark;
    int copied = 0;
    int mss_now = 0, size_goal;
    long timeo;

    // keep sendmsg counter.
    ksk->msg_cnt++;

    // Parse the control message, to retreive the identifier(mark) for tt.
    for (cmsg = CMSG_FIRSTHDR(msg); cmsg != NULL;
         cmsg = CMSG_NXTHDR(msg, cmsg)) {
        if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SO_MARK) {
            // Extract the mark value
            memcpy(&mark, CMSG_DATA(cmsg), sizeof(mark));
            break;
        }
    }

    trace_record("%u tx: enter koma_sendmsg\n", mark);

    // @Rui: set msg to nonblocking mode;
    // TODO: set koma socket to nonblocking directly.
    msg->msg_flags |= MSG_DONTWAIT;

    // @Rui: MSG_MORE indicates that there should be more data coming, while
    // MSG_EOR indicates that the final part of a message/unit part. SOCK_DGRAM
    // is a socket type where data is sent as discrete packets/datagrams (e.g.,
    // udp). Here koma uses sock_dgram. so if msg->msg_flags has MSG_EOR, eor
    // = 1. if msg->msg_flags = MSG_MORE, eor = 0. The usage of eor here is to
    // determine if saving the messages for later processing (MSG_MORE), or
    // should be sent out immediately (MSG_EOR). TODO: To be imlemented.

    // int eor = (sock->type == SOCK_DGRAM) ? !(msg->msg_flags & MSG_MORE) :
    //                                        !!(msg->msg_flags & MSG_EOR);
    int err = -EPIPE;

    if (sk->sk_err || (sk->sk_shutdown & SEND_SHUTDOWN))
        goto out_error;

    // check if there is data left to process in the current msg
    if (!msg_data_left(msg)) {
        goto out_error;
    }

    psock = ksk->last_psock;
    if (!psock) {
        // pr_info_log("psock does not exist any more!:\n", psock_index);
        goto out_error;
    }

    csk = psock->sk;
    tp = tcp_sk(csk);
    mss_now = tcp_send_mss(csk, &size_goal, msg->msg_flags);
    timeo = sock_sndtimeo(csk, msg->msg_flags & MSG_DONTWAIT);

    // pr_info_id("start accumu!\n");
    trace_record("%u tx: start accumulation\n", mark);

    skb_queue_head_init(&cur_msg);

    while (msg_data_left(msg)) {
        ssize_t copy = 0;
        skb = skb_peek(&cur_msg);
        // check if the last skb in the list can fit more data
        if (skb)
            copy = size_goal - skb->len;
        if (copy <= 0 || !tcp_skb_can_collapse_to(skb)) {
        new_segment:
            // Use koma socket memory allocation budget here.
            if (!sk_stream_memory_free(sk)) {
                pr_info_id(
                    "sk_stream_memory_free returns 0! sndbuf size is %d\n",
                    READ_ONCE(sk->sk_wmem_queued));
                goto wait_for_space;
            }
            // pr_info_id("sndbuf size is %d\n", READ_ONCE(sk->sk_wmem_queued));
            // alloc skb for tcp
            skb = alloc_skb_fclone(MAX_TCP_HEADER, sk->sk_allocation);
            if (likely(skb)) {
                bool mem_scheduled;
                skb->truesize = SKB_TRUESIZE(skb_end_offset(skb));
                mem_scheduled = sk_wmem_schedule(sk, skb->truesize);
                if (likely(mem_scheduled)) {
                    skb_reserve(skb, MAX_TCP_HEADER);
                    skb->ip_summed = CHECKSUM_PARTIAL;
                    INIT_LIST_HEAD(&skb->tcp_tsorted_anchor);
                }
            }
            if (!skb) {
                pr_info_id("couldnot alloc skb!\n");
                goto wait_for_space;
            }

            // no lock protection; to be added;
            psock->process_backlog++;
            trace_record("%u tx: finish skb allocation\n", mark);
            // TODO: note here we add this skb to the wait_queue, including
            // initializing the new skb for tcp. adopted from
            // https://elixir.bootlin.com/linux/v6.12.6/source/net/ipv4/tcp.c#L675

            skb->mark = mark;
            struct tcp_skb_cb *tcb = TCP_SKB_CB(skb);
            tcb->seq = 0;
            tcb->tcp_flags = TCPHDR_ACK;
            __skb_header_release(skb);

            skb_queue_tail(&cur_msg, skb);

            sk_wmem_queued_add(sk, skb->truesize);
            // pr_info_id("add %u tp wmem_queued, which currently is %u\n",
            // skb->truesize, READ_ONCE(sk->sk_wmem_queued));
            sk_mem_charge(sk, skb->truesize);

            if (tp->nonagle & TCP_NAGLE_PUSH)
                tp->nonagle &= ~TCP_NAGLE_PUSH;
            // TODO: export the symbols for functions used by
            // tcp_slow_start_after_idle_check
            /*tcp_slow_start_after_idle_check(csk);*/
            copy = size_goal;
        }

        /* try to append data to the end of the skb */
        if (copy > msg_data_left(msg))
            copy = msg_data_left(msg);

        bool merge = true;
        int i = skb_shinfo(skb)->nr_frags;
        struct page_frag *pfrag = sk_page_frag(sk);

        // check if this pgrag has sufficient space to hold new data.
        if (!sk_page_frag_refill(sk, pfrag)) {
            pr_info_id("couldnot refill frag!\n");
            goto wait_for_space;
        }

        // check if data can be appended to the current fragment
        if (!skb_can_coalesce(skb, i, pfrag->page, pfrag->offset)) {
            if (i >= MAX_SKB_FRAGS) {
                // when i >= MAX_SKB_FRAGS, it means that this skb cannot
                // acccomodate more data. so tcp_mark_push is used to mark the
                // push flag of tcp for this skb, to signal the underlying send
                // tcp stack that the skb is ready to be trasmitted even when
                // the sendbuf is not full. of the current socket.
                tcp_mark_push(tp, skb);
                goto new_segment;
            }
            merge = false;
        }

        // how many data can be copied into the current pfrag.
        copy = min_t(int, copy, pfrag->size - pfrag->offset);

        // check if there is enough space in the sk quota forward_alloc.

        if (!copy) {
            pr_info_id("couldnot wmem schedule\n");
            goto wait_for_space;
        }

        if (!likely(sk_wmem_schedule(sk, copy))) {
            goto wait_for_space;
        }

        err = skb_copy_to_page_nocache(sk, &msg->msg_iter, skb, pfrag->page,
                                       pfrag->offset, copy);
        if (err)
            goto do_error;

        trace_record("%u tx: finish data copy\n", mark);

        if (merge) {
            skb_frag_size_add(&skb_shinfo(skb)->frags[i - 1], copy);
        } else {
            skb_fill_page_desc(skb, i, pfrag->page, pfrag->offset, copy);
            page_ref_inc(pfrag->page);
        }
        pfrag->offset += copy;

        if (!copied)
            TCP_SKB_CB(skb)->tcp_flags &= ~TCPHDR_PSH;

        // WRITE_ONCE(tp->write_seq, tp->write_seq + copy);
        // TCP_SKB_CB(skb)->end_seq += copy;
        tcp_skb_pcount_set(skb, 0);

        copied += copy;
        TCP_SKB_CB(skb)->seq += copy;

        // full skb, mark it to avoid iterating through tx_wait_queue for
        // relinking.
        if (skb->len >= size_goal) {
            skb_full = skb;
        }

        if (!msg_data_left(msg)) {
            trace_record("%u tx: finish copying full message\n", mark);
            skb_queue_walk(&cur_msg, skb)
            {
                sk_wmem_queued_add(sk, -skb->truesize);
                sk_mem_uncharge(sk, skb->truesize);
            }

            spin_lock_bh(&psock->tx_lock);
            skb_queue_walk(&cur_msg, skb)
            {
                sk_wmem_queued_add(csk, skb->truesize);
                sk_mem_charge(csk, skb->truesize);
            }

            skb_queue_splice_tail(&cur_msg, &(psock->tx_wait_queue));
            trace_record("%u tx: move message to wait_queue\n", mark);
            spin_unlock_bh(&psock->tx_lock);
            goto out;
        }

        // TODO: add the logic where msg still have leftover but skb is full.
        continue;

    wait_for_space:
        pr_info("wait_for_space!\n");
        set_bit(SOCK_NOSPACE, &sk->sk_socket->flags);
        // pr_info_id("%d enter sk_stream_wait_memory %ld", ksk->index, timeo);
        err = sk_stream_wait_memory(csk, &timeo);
        // pr_info_id("%d sk_stream_wait_memory returns %d", ksk->index, err);
        if (err != 0)
            goto do_error;
    }

out:
    ksk->accumu_cnt++;
    // pr_info_id("In out, try to call try_send_from_psock\n");
    try_send_from_psock(ksk->index, psock, msg->msg_flags, mss_now, size_goal,
                        mark);
    koma_pull(sock);
    return copied;

do_error:
    tcp_remove_empty_skb(csk);
    if (copied) {
        skb_queue_walk(&cur_msg, skb)
        {
            sk_wmem_queued_add(sk, -skb->truesize);
            sk_mem_uncharge(sk, skb->truesize);
            __kfree_skb(skb);
        }
    }

out_error:
    err = sk_stream_error(sk, msg->msg_flags, err);
    pr_info("out_error returns %d", err);
    return err;
}

static void koma_splice_eof(struct socket *sock)
{
    // TODO: to be implemented
    return;
}

static size_t simple_copy_to_iter(const void *addr, size_t bytes,
                                  void *data __always_unused,
                                  struct iov_iter *i)
{
    return copy_to_iter(addr, bytes, i);
}

static int __skb_datagram_iter(const struct sk_buff *skb, int offset,
                               struct iov_iter *to, int len, bool fault_short,
                               size_t (*cb)(const void *, size_t, void *,
                                            struct iov_iter *),
                               void *data)
{
    int start = skb_headlen(skb);
    int i, copy = start - offset, start_off = offset, n;
    struct sk_buff *frag_iter;

    /* Copy header. */
    if (copy > 0) {
        if (copy > len)
            copy = len;
        n = INDIRECT_CALL_1(cb, simple_copy_to_iter, skb->data + offset, copy,
                            data, to);
        offset += n;
        if (n != copy)
            goto short_copy;
        if ((len -= copy) == 0)
            return 0;
    }

    /* Copy paged appendix. Hmm... why does this look so complicated? */
    for (i = 0; i < skb_shinfo(skb)->nr_frags; i++) {
        int end;
        const skb_frag_t *frag = &skb_shinfo(skb)->frags[i];

        WARN_ON(start > offset + len);

        end = start + skb_frag_size(frag);
        if ((copy = end - offset) > 0) {
            struct page *page = skb_frag_page(frag);
            u8 *vaddr = kmap(page);

            if (copy > len)
                copy = len;
            n = INDIRECT_CALL_1(cb, simple_copy_to_iter,
                                vaddr + skb_frag_off(frag) + offset - start,
                                copy, data, to);
            kunmap(page);
            offset += n;
            if (n != copy)
                goto short_copy;
            if (!(len -= copy))
                return 0;
        }
        start = end;
    }

    skb_walk_frags(skb, frag_iter)
    {
        int end;

        WARN_ON(start > offset + len);

        end = start + frag_iter->len;
        if ((copy = end - offset) > 0) {
            if (copy > len)
                copy = len;
            if (__skb_datagram_iter(frag_iter, offset - start, to, copy,
                                    fault_short, cb, data))
                goto fault;
            if ((len -= copy) == 0)
                return 0;
            offset += copy;
        }
        start = end;
    }
    if (!len)
        return 0;

    pr_info("reached end of skb but %d bytes left to copy\n", len);
    /* This is not really a user copy fault, but rather someone
     * gave us a bogus length on the skb.  We should probably
     * print a warning here as it may indicate a kernel bug.
     */

fault:
    iov_iter_revert(to, offset - start_off);
    return -EFAULT;

short_copy:
    if (fault_short || iov_iter_count(to))
        goto fault;

    return 0;
}

int skb_copy_datagram_msg_http2(struct sk_buff *from, struct msghdr *msg,
                                int *len)
{
    struct sk_buff *iter = from;
    int res;
    pr_info_log("skb_copy_datagram_msg_http2: starting skb %p\n", iter);
    while (iter) {
        pr_info_log(
            "skb_copy_datagram_msg_http2: skb %p; size %d; iter offset %d\n",
            iter, strp_msg(iter)->full_len, strp_msg(iter)->offset);
        res = __skb_datagram_iter(iter, strp_msg(iter)->offset, &msg->msg_iter,
                                  strp_msg(iter)->full_len, false,
                                  simple_copy_to_iter, NULL);
        *len += strp_msg(iter)->full_len;
        iter = strp_next(iter);
    }
    return res;
}

static int koma_recvmsg(struct socket *sock, struct msghdr *msg, size_t len,
                        int flags)
{
    // return 0;
    flags &= ~MSG_DONTWAIT;
    if (sock->file)
        sock->file->f_flags &= ~O_NONBLOCK;
        
    struct sock *sk = sock->sk;
    struct koma_sock *ksk = koma_sk(sk);
    int err = 0;
    struct strp_msg *stm;
    int copied = 0;
    struct sk_buff *skb;

    int off = 0;

    pr_info_log("[recvmsg] enter koma_recvmsg\n");

    skb = skb_recv_datagram(sk, flags, &err);
    if (!skb) {
        pr_info_log("[recvmsg] skb_recv_datagram returns null\n");
        goto out;
    }

    ksk->last_psock = *(struct koma_psock**)&skb->cb[0];
    /*if (*skb->data != 0x80)*/

    // pr_info("[recvmsg] the first skb of the messge is %p\n", skb);
    /* Okay, have a message on the receive queue */

    stm = strp_msg(skb);

    /*err = skb_copy_datagram_msg(skb, stm->offset, msg, len);*/
    err = skb_copy_datagram_msg_http2(skb, msg, &copied);
    if (err < 0) {
        pr_info_log("[recvmsg] skb_copy_datagram_msg_http2 returns err %d\n",
                    err);
        goto out;
    }

    pr_info_log("[recvmsg] finish copying %d bytes\n", copied);

    msg->msg_namelen = 0;

    // pr_info("[koma_recvmsg] the remote port and addresses are %04X %08X!:\n",
    //         in4->sin_port, in4->sin_addr.s_addr);

    if (len > copied)
        len = copied;
    if (likely(!(flags & MSG_PEEK))) {
        if (len < copied) {
            if (sock->type == SOCK_DGRAM) {
                /* Truncated message */
                msg->msg_flags |= MSG_TRUNC;
                goto msg_finished;
            }
            // TODO (RUI): handle the case where the message is larger than len.
            // does not work with http2 yet. TO be fixed. stm->offset += copied;
            // stm->full_len -= copied;
        } else {
        msg_finished:
            /* Finished with message */
            msg->msg_flags |= MSG_EOR;
            /*KOMA_STATS_INCR(ksk->stats.rx_msgs);*/
        }
    }

out:
    // if (ksk->index == 0)
    //     pr_info("[%lld] [Latency] start freeing skb from koma socket
    //     index 0\n", ktime_get_ns() / 1000);
    if (skb) {
        // free all skbs chained via strp_next()
        struct sk_buff *iter = strp_next(skb);
        koma_free_strp_chain(iter);
        strp_next(skb) = NULL;
        kfree_skb(skb);
    }
    return copied ?: err;
}

static ssize_t koma_splice_read(struct socket *sock, loff_t *ppos,
                                struct pipe_inode_info *pipe, size_t len,
                                unsigned int flags)
{
    struct sock *sk = sock->sk;
    struct koma_sock *koma = koma_sk(sk);
    struct strp_msg *stm;
    int err = 0;
    ssize_t copied;
    struct sk_buff *skb;

    /* Only support splice for SOCKSEQPACKET */

    skb = skb_recv_datagram(sk, flags, &err);
    if (!skb)
        goto err_out;

    /* Okay, have a message on the receive queue */

    stm = strp_msg(skb);

    if (len > stm->full_len)
        len = stm->full_len;

    copied = skb_splice_bits(skb, sk, stm->offset, pipe, len, flags);
    if (copied < 0) {
        err = copied;
        goto err_out;
    }

    stm->offset += copied;
    stm->full_len -= copied;

    /* We have no way to return MSG_EOR. If all the bytes have been
     * read we still leave the message in the receive socket buffer.
     * A subsequent recvmsg needs to be done to return MSG_EOR and
     * finish reading the message.
     */

    skb_free_datagram(sk, skb);
    return copied;

err_out:
    skb_free_datagram(sk, skb);
    return err;
}

static int koma_setsockopt(struct socket *sock, int level, int optname,
                           sockptr_t optval, unsigned int optlen)
{
    int val, valbool;
    int err = 0;

    if (level != SOL_KOMA)
        return -ENOPROTOOPT;

    if (optlen < sizeof(int))
        return -EINVAL;

    if (copy_from_sockptr(&val, optval, sizeof(int)))
        return -EFAULT;

    valbool = val ? 1 : 0;

    switch (optname) {
    default:
        err = -ENOPROTOOPT;
    }

    return err;
}

static int koma_getsockopt(struct socket *sock, int level, int optname,
                           char __user *optval, int __user *optlen)
{
    int val, len;

    if (level != SOL_KOMA)
        return -ENOPROTOOPT;

    if (get_user(len, optlen))
        return -EFAULT;

    if (len < 0)
        return -EINVAL;

    len = min_t(unsigned int, len, sizeof(int));

    switch (optname) {
    default:
        return -ENOPROTOOPT;
    }

    if (put_user(len, optlen))
        return -EFAULT;
    if (copy_to_user(optval, &val, len))
        return -EFAULT;
    return 0;
}

static int default_parse_msg(struct strparser *strp, struct sk_buff *skb)
{
    return 0; // or some safe default
}

/**
 * koma_attach() - Implements the ioctl koma_attach for a given koma socket.
 * In the original KCM design, when a kcm socket was created, a multiplexor
 * was also created automatically. The tcp socket will be attached to the
 * multiplexor through this call. However, in the Koma design, we dont need
 * a multiplexor attached to any specific set of koma sockets. To achieve
 * this, we abort the abstraction of multiplexor and only have a psock
 * attached to the tcp socket. Note that here we use the thread-local koma
 * socket to do the ioctl, even there is no mapping relationship between
 * this koma socket and the tcp socket.
 * @sock:      Koma socket on which the ioctl call was invoked.
 * @csock:     TCP socket to be attached to the Koma attached.
 * @initial_conn_window: Connection-level receive window supplied by userspace.
 * Return:     0 on success, otherwise a negative errno.
 */
static int koma_attach(struct socket *sock, struct socket *csock,
                       int initial_conn_window)
{
    struct koma_sock *ksk = koma_sk(sock->sk);
    struct koma_net *knet = ksk->knet;
    struct koma_psock_bucket *bucket;
    struct koma_psock *psock = NULL;

    /* Create psock */
    psock = kmem_cache_zalloc(koma_psockp, GFP_KERNEL);
    if (!psock) {
        return -ENOMEM;
    }

    psock->knet = knet;

    struct sock *csk;
    static const struct strp_callbacks cb = { .rcv_msg = koma_rcv_strparser,
                                              .parse_msg = default_parse_msg };
    int err = 0;
    int conn_window = initial_conn_window > 0 ? initial_conn_window
                                              : INITIAL_WINDOW_SIZE;

    csk = csock->sk;
    if (!csk)
        return -EINVAL;

    lock_sock(csk);

    /* create a hash id for the psock and record the addr*/
    psock->hash_id = hash_tcp_tuple(csk->sk_daddr, csk->sk_dport);
    psock->dport = csk->sk_dport;
    psock->daddr = csk->sk_daddr;

    pr_info_log(
        "Created koma psock %d with bucket idx % d for new connection %08X %04X!\n",
        psock->hash_id, psock->hash_id % NUM_BUCKETS, ntohl(csk->sk_daddr),
        ntohs(csk->sk_dport));

    /* Only allow TCP sockets to be attached for now */
    if ((csk->sk_family != AF_INET && csk->sk_family != AF_INET6) ||
        csk->sk_protocol != IPPROTO_TCP) {
        err = -EOPNOTSUPP;
        goto out;
    }

    /* Don't allow listeners or closed sockets */
    if (csk->sk_state == TCP_LISTEN || csk->sk_state == TCP_CLOSE) {
        err = -EOPNOTSUPP;
        goto out;
    }

    psock->sk = csk;

    skb_queue_head_init(&psock->tx_wait_queue);

    spin_lock_init(&psock->lock);
    spin_lock_init(&psock->rx_lock);
    spin_lock_init(&psock->tx_lock);

    write_lock_bh(&csk->sk_callback_lock);

    /* Check if sk_user_data is already by KOMA or someone else.
     * Must be done under lock to prevent race conditions.
     */
    if (csk->sk_user_data) {
        write_unlock_bh(&csk->sk_callback_lock);
        kmem_cache_free(koma_psockp, psock);
        err = -EALREADY;
        goto out;
    }

    err = strp_init(&psock->strp, csk, &cb);
    if (err) {
        write_unlock_bh(&csk->sk_callback_lock);
        kmem_cache_free(koma_psockp, psock);
        goto out;
    }

    psock->save_data_ready = csk->sk_data_ready;
    psock->save_write_space = csk->sk_write_space;
    psock->save_state_change = csk->sk_state_change;
    csk->sk_user_data = psock;
    csk->sk_data_ready = psock_data_ready;
    // not manipulate sk_write_space because we do not use koma tx.
    // csk->sk_write_space = psock_write_space;
    csk->sk_state_change = psock_state_change;

    write_unlock_bh(&csk->sk_callback_lock);

    sock_hold(csk);

    atomic_set(&psock->tx_in_use, 0);
    psock->accumu = 0;
    psock->process_backlog = 0;
    psock->recv_conn_window = conn_window;
    psock->send_quota = HTTP2_DEFAULT_CONNECTION_WINDOW;
    psock->flow_stalled = false;
    psock->unacked = 0;

    /* Add new psock to the list */
    spin_lock_bh(&knet->lock);
    list_add_rcu(&psock->koma_psock_list, &knet->psock_list);
    knet->count++;
    spin_unlock_bh(&knet->lock);

    /* Add the new psock to the koma->koma_psock_buckets */
    bucket = &koma->koma_psock_buckets[psock->hash_id % NUM_BUCKETS];

    hlist_add_head_rcu(&psock->hash_links, &bucket->psocks);

    INIT_WORK(&psock->tx_work, koma_tx_work);
out:
    release_sock(csk);

    return err;
}

static int koma_attach_ioctl(struct socket *sock, struct koma_attach_user *info)
{
    struct socket *csock;
    int err;

    csock = sockfd_lookup(info->fd, &err);
    if (!csock)
        return -ENOENT;

    // prog = bpf_prog_get_type(info->bpf_fd, BPF_PROG_TYPE_SK_SKB);
    // if (IS_ERR(prog)) {
    //     err = PTR_ERR(prog);
    //     goto out;
    // }

    err = koma_attach(sock, csock, info->initial_conn_window);
//     if (err) {
//         goto out;
//     }

//     /* Keep reference on file also */
//     return 0;
// out:
    sockfd_put(csock);
    return err;
}

static int koma_pull(struct socket *sock)
{
    struct koma_sock *ksk = koma_sk(sock->sk);
    struct sk_buff_head *list = &ksk->rx_q;
    struct sk_buff *skb = NULL;

again:
    // first pull from local queue.
    spin_lock_bh(&list->lock);
    skb = __skb_dequeue(list);
    if (skb) {
        if (list->qlen == 0)
            clear_bit(ksk->index, koma->rx_nonempty_bm);
        spin_unlock_bh(&list->lock);
        return koma_queue_rcv_skb(ksk, skb);
    }
    spin_unlock_bh(&list->lock);

    // // local queue is empty, steal from other queues.
    // ksk->steal_cnt++;
    skb = koma_steal_skb(ksk);
    if (skb)
        return koma_queue_rcv_skb(ksk, skb);

    // whole system empty, mark self as idle
    spin_lock_bh(&koma->waiters_lock);
    if (READ_ONCE(koma->rx_nonempty_bm[0]) != 0){
        spin_unlock_bh(&koma->waiters_lock);
        goto again;
    }
    if (!test_bit(ksk->index, koma->waiters_bm)) {
        set_bit(ksk->index, koma->waiters_bm);
        list_add_tail(&ksk->ksk_waiting_list, &koma->ksk_waiters);
    }
    spin_unlock_bh(&koma->waiters_lock);
    return 0;
}

/* containing functions to define the protocol operations for protocol.
 * for example, function on how to establish a connection
 * since KCM does not need a protocol implementation (heavy lifting done by
 * tcp), it is more or less a place holder. Note that it is lower-level than
 * `struct proto_ops`
 */
static struct proto koma_proto = {
    .name = "KOMA",
    .owner = THIS_MODULE,
    .obj_size = sizeof(struct koma_sock),
};

static int koma_ioctl(struct socket *sock, unsigned int cmd, unsigned long arg)
{
    int err;

    switch (cmd) {
    case SIOCKOMAATTACH: {
        struct koma_attach_user info;

        err = copy_koma_attach_from_user(&info, (void __user *)arg);
        if (err)
            return err;

        err = koma_attach_ioctl(sock, &info);

        break;
    }
    case SIOCKOMAPULL: {
        err = koma_pull(sock);
        break;
    }

    default:
        err = -ENOIOCTLCMD;
        break;
    }

    return err;
}

static void free_psock(struct rcu_head *rcu)
{
    struct koma_psock *psock = container_of(rcu, struct koma_psock, rcu);

    pr_info_log("[free_psock] Start freeing psock for %d", psock->hash_id);
    kmem_cache_free(koma_psockp, psock);
    pr_info_log("[free_psock] Freed psock for %d", psock->hash_id);
}

static void release_psock(struct koma_psock *psock)
{
    struct koma_net *knet = psock->knet;
    struct sock *csk = psock->sk;

    spin_lock_bh(&knet->lock);
    list_del_rcu(&psock->koma_psock_list);
    knet->count--;
    spin_unlock_bh(&knet->lock);

    if (!csk) {
        call_rcu(&psock->rcu, free_psock);
        return;
    }

    // clean the tcp socket before freeing psock, cannot run it in call_rcu,
    // because call_rcu runs in softirq, which cannot sleep, and lock_sock may
    // involves sleeping
    lock_sock(csk);
    write_lock_bh(&csk->sk_callback_lock);
    csk->sk_user_data = NULL;
    csk->sk_data_ready = psock->save_data_ready;
    csk->sk_write_space = psock->save_write_space;
    csk->sk_state_change = psock->save_state_change;
    strp_stop(&psock->strp);
    write_unlock_bh(&csk->sk_callback_lock);

    strp_done(&psock->strp);
    release_sock(csk);
    sock_put(csk);

    call_rcu(&psock->rcu, free_psock);
}

static void koma_done(struct koma_sock *ksk)
{
    sock_put(&ksk->sk);
}

/**
 * koma_release() - called to close a koma socket. If this is the last koma
 * socket, destroy all psocks.
 * TODO: theoretically, psock should only be freed on the unattachment or
 * terminating of a tcp connection. Should implement that instead.
 * @sock:   the socket data structure for the closing koma socket.
 * Return:  0 on success
 */
static int koma_release(struct socket *sock)
{
    struct sock *sk = sock->sk;
    struct koma_sock *ksk;
    struct koma_psock *tpsock;
    struct koma_psock_bucket *bucket;
    struct hlist_node *tmp;
    int cnt;

    if (!sk)
        return 0;

    ksk = koma_sk(sk);
    pr_info("Releasing koma socket %d", ksk->index);

    lock_sock(sk);
    sock_orphan(sk);
    kfree_skb(ksk->seq_skb);

    /* Purge/Eempty a queue under lock to avoid race condition with tx_work
     * trying to act when queue is nonempty. If tx_work runs after this
     * point it will just return.
     */
    skb_queue_purge_koma(&sk->sk_receive_queue);

    release_sock(sk);

    /* Remove koma socks from the array and decrease the count
     * by one
     * TODO: it will cause a sparse problem. Need to fix it later.
     */
    spin_lock_bh(&koma->lock);
    if (ksk->index <= NUM_KOMA_SOCKETS) {
        koma->koma_socks[ksk->index] = NULL;
        koma->koma_socks_cnt--;
        koma->all_msg_cnt += ksk->msg_cnt;
        koma->accumu_cnt += ksk->accumu_cnt;
        pr_info_log(
            "[koma_release] Setting %d koma socket to null and decrease koma_socks_cnt to %d!\n",
            ksk->index, koma->koma_socks_cnt);
    } else {
        pr_info_log("[koma_release] found invalid koma socket with index %d!",
                    ksk->index);
    }
    cnt = koma->koma_socks_cnt;
    spin_unlock_bh(&koma->lock);

    if (cnt == 0) {
        pr_info_log("[koma_release] koma_socks_cnt reaches 0, clean psocks\n");
        pr_info("#messages received is %zu", koma->all_msg_rcv_cnt);
        pr_info("#messages to send is %zu", koma->all_msg_cnt);
        pr_info("#planned tcp_send is %zu", koma->accumu_cnt);
        pr_info("#actual tcp_send is %zu", koma->snd_cnt);
        /* All koma sockets does not exist any more. remove all psocks
         */
        for (int i = 0; i < NUM_BUCKETS; i++) {
            bucket = &koma->koma_psock_buckets[i];
            if (!bucket)
                continue;
            hlist_for_each_entry_safe(tpsock, tmp, &bucket->psocks, hash_links)
            {
                if (tpsock) {
                    pr_info_log("[koma_release] Starting releasing psock\n");
                    // spin_lock_bh(&tpsock->tx_lock);
                    // skb_queue_purge(&tpsock->tx_wait_queue);
                    // spin_unlock_bh(&tpsock->tx_lock);
                    release_psock(tpsock);
                    hlist_del_rcu(&tpsock->hash_links);
                }
            }
        }
        spin_lock_bh(&koma->lock);
        // skb_queue_purge_koma(&koma->all_msgs);
        spin_unlock_bh(&koma->lock);
    }
    /* Cancel work. After this point there should be no outside references
     * to the koma socket.
     */
    /*cancel_work_sync(&ksk->tx_work);*/

    sock->sk = NULL;

    koma_done(ksk);
    return 0;
}

/* the two structure dfines functions that handle various opreations on kcm
 * sockets. they are relatively generic -- called to implement top-level
 * system calls.
 */
static const struct proto_ops koma_dgram_ops = {
    .family = PF_KOMA,
    .owner = THIS_MODULE,
    .release = koma_release,
    .bind = sock_no_bind,
    .connect = sock_no_connect,
    .socketpair = sock_no_socketpair,
    .accept = sock_no_accept,
    .getname = sock_no_getname,
    .poll = datagram_poll,
    .ioctl = koma_ioctl,
    .listen = sock_no_listen,
    .shutdown = sock_no_shutdown,
    .setsockopt = koma_setsockopt,
    .getsockopt = koma_getsockopt,
    .sendmsg = koma_sendmsg,
    .recvmsg = koma_recvmsg,
    .mmap = sock_no_mmap,
    .splice_eof = koma_splice_eof,
};

static const struct proto_ops koma_seqpacket_ops = {
    .family = PF_KOMA,
    .owner = THIS_MODULE,
    .release = koma_release,
    .bind = sock_no_bind,
    .connect = sock_no_connect,
    .socketpair = sock_no_socketpair,
    .accept = sock_no_accept,
    .getname = sock_no_getname,
    .poll = datagram_poll,
    .ioctl = koma_ioctl,
    .listen = sock_no_listen,
    .shutdown = sock_no_shutdown,
    .setsockopt = koma_setsockopt,
    .getsockopt = koma_getsockopt,
    .sendmsg = koma_sendmsg,
    .recvmsg = koma_recvmsg,
    .mmap = sock_no_mmap,
    .splice_eof = koma_splice_eof,
    .splice_read = koma_splice_read,
};

/* Create proto operation for koma sockets */
static int koma_create(struct net *net, struct socket *sock, int protocol,
                       int kern)
{
    struct koma_net *knet = net_generic(net, koma_net_id);
    struct sock *sk;
    struct koma_sock *ksk;

    switch (sock->type) {
    case SOCK_DGRAM:
        sock->ops = &koma_dgram_ops;
        break;
    case SOCK_SEQPACKET:
        sock->ops = &koma_seqpacket_ops;
        break;
    default:
        return -ESOCKTNOSUPPORT;
    }

    if (protocol != KOMAPROTO_CONNECTED)
        return -EPROTONOSUPPORT;

    /* Create koma_sock */
    sk = sk_alloc(net, PF_KOMA, GFP_KERNEL, &koma_proto, kern);
    if (!sk)
        return -ENOMEM;
    ksk = koma_sk(sk);

    /* Add koma socket to struct koma, and increase the count
     * for #koma_sockets.
     */
    spin_lock_bh(&koma->lock);
    /* check if the number of koma sockets exceeds the allocated number */
    if (koma->koma_socks_cnt >= NUM_KOMA_SOCKETS) {
        sk_free(sk);
        spin_unlock_bh(&koma->lock);
        return -ENOMEM;
    }
    koma->koma_socks[koma->koma_socks_cnt] = ksk;
    ksk->index = koma->koma_socks_cnt;
    skb_queue_head_init(&ksk->rx_q);

    koma->koma_socks_cnt++;

    ksk->knet = knet;

    pr_info_log(
        "koma socket created successfully, with the number of koma sockets as %d!\n",
        koma->koma_socks_cnt);

    /* Init KOMA socket */
    sock_init_data(sock, sk);

    /* For SOCK_SEQPACKET sock type, datagram_poll checks the sk_state, so
     * we set sk_state, otherwise epoll_wait always returns right away with
     * EPOLLHUP
     */
    ksk->sk.sk_state = TCP_ESTABLISHED;
    ksk->msg_cnt = 0;
    ksk->accumu_cnt = 0;
    set_bit(ksk->index, koma->waiters_bm);
    list_add_tail(&ksk->ksk_waiting_list, &koma->ksk_waiters);
    spin_unlock_bh(&koma->lock);

    return 0;
}

/* structure to define a protocol family.
 * For example: the `AF_INET` protocol family includes the IPv4, TCP, and
 * UDP protocols. The `AF_INET6` includes IPv6. It contains i) the protocol
 * family identifier (AF_INET, here is PF_KCM). ii) the pointer to function
 * to create a socket, whic is called when a user application calls
 * `socket()`
 */
static const struct net_proto_family koma_family_ops = {
    .family = PF_KOMA,
    .create = koma_create,
    .owner = THIS_MODULE,
};

static __net_init int koma_init_net(struct net *net)
{
    struct koma_net *knet = net_generic(net, koma_net_id);

    INIT_LIST_HEAD_RCU(&knet->psock_list);
    spin_lock_init(&knet->lock);

    return 0;
}

static __net_exit void koma_exit_net(struct net *net)
{
    struct koma_net *knet = net_generic(net, koma_net_id);

    /* All KOMA sockets should be closed at this point, which should mean
     * that all multiplexors and psocks have been destroyed.
     */
    WARN_ON(!list_empty(&knet->psock_list));
}

static struct pernet_operations koma_net_ops = {
    .init = koma_init_net,
    .exit = koma_exit_net,
    .id = &koma_net_id,
    .size = sizeof(struct koma_net),
};

static int __init koma_init(void)
{
    int err = -ENOMEM;

    /* strparser part */
    BUILD_BUG_ON(sizeof(struct sk_skb_cb) > sizeof_field(struct sk_buff, cb));
    strp_wq = create_singlethread_workqueue("kstrp");
    if (unlikely(!strp_wq))
        return -ENOMEM;

    koma_psockp = KMEM_CACHE(koma_psock, SLAB_HWCACHE_ALIGN);
    if (!koma_psockp)
        goto fail;

    h2_stream_bufferp = KMEM_CACHE(h2_stream_buffer, SLAB_HWCACHE_ALIGN);
    if (!h2_stream_bufferp)
        goto fail;

    koma_wq = create_singlethread_workqueue("kkomad");
    if (!koma_wq)
        goto fail;

    err = proto_register(&koma_proto, 1);
    if (err)
        goto fail;

    err = register_pernet_device(&koma_net_ops);
    if (err)
        goto net_ops_fail;

    err = sock_register(&koma_family_ops);
    if (err)
        goto sock_register_fail;

    err = koma_proc_init();
    if (err)
        goto proc_init_fail;

    /* Initialize struct koma, including its hash table, and linked lists */
    INIT_LIST_HEAD(&koma->ksk_waiters);
    bitmap_zero(koma->waiters_bm, NUM_KOMA_SOCKETS);
    bitmap_zero(koma->rx_nonempty_bm, NUM_KOMA_SOCKETS);

    spin_lock_init(&koma->lock);
    spin_lock_init(&koma->waiters_lock);

    for (int i = 0; i < NUM_BUCKETS; i++) {
        struct koma_psock_bucket *bucket = &koma->koma_psock_buckets[i];
        INIT_HLIST_HEAD(&bucket->psocks);
        bucket->id = i;
    }
    skb_queue_head_init(&koma->all_msgs);
    /*pr_info_id("Koma is successfully loaded into the kernel!\n");*/
    pr_info_log("PF_MAX is %d\n", PF_MAX);

    return 0;

proc_init_fail:
    sock_unregister(PF_KOMA);

sock_register_fail:
    unregister_pernet_device(&koma_net_ops);

net_ops_fail:
    proto_unregister(&koma_proto);

fail:
    pr_info_log("Koma fails to be loaded into the kernel %d\n", err);
    kmem_cache_destroy(koma_psockp);
    kmem_cache_destroy(h2_stream_bufferp);
    if (koma_wq)
        destroy_workqueue(koma_wq);

    return err;
}

static void __exit koma_exit(void)
{
    koma_proc_exit();
    sock_unregister(PF_KOMA);
    unregister_pernet_device(&koma_net_ops);
    proto_unregister(&koma_proto);
    destroy_workqueue(koma_wq);

    kmem_cache_destroy(koma_psockp);
    kmem_cache_destroy(h2_stream_bufferp);
}

module_init(koma_init);
module_exit(koma_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("KOMA sockets");
MODULE_ALIAS_NETPROTO(PF_KOMA);
