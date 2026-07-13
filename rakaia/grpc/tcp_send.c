// SPDX-License-Identifier: GPL-2.0-only
/*
 * rakaia
 *
 * Copyright (c) 2024 Rui Yang <rui.yang@epfl.ch>
 */

#include "linux/spinlock.h"
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
#include <linux/syscalls.h>
#include <linux/uaccess.h>
#include <linux/workqueue.h>
#include <linux/jhash.h>
#include <net/tcp.h>

#include <net/netns/generic.h>
#include <net/sock.h>
#include <trace/events/sock.h>
#include <uapi/linux/rakaia.h> //TODO: add the header file in the specified path

#include "rakaia.h"
#include "tcp_send.h"

void tcp_tx_timestamp(struct sock *sk, u16 tsflags)
{
    struct sk_buff *skb = tcp_write_queue_tail(sk);

    if (tsflags && skb) {
        struct skb_shared_info *shinfo = skb_shinfo(skb);
        struct tcp_skb_cb *tcb = TCP_SKB_CB(skb);

        sock_tx_timestamp(sk, tsflags, &shinfo->tx_flags);
        if (tsflags & SOF_TIMESTAMPING_TX_ACK)
            tcb->txstamp_ack = 1;
        if (tsflags & SOF_TIMESTAMPING_TX_RECORD_MASK)
            shinfo->tskey = TCP_SKB_CB(skb)->seq + skb->len - 1;
    }
}

int tcp_wmem_schedule(struct sock *sk, int copy)
{
    int left;

    if (likely(sk_wmem_schedule(sk, copy)))
        return copy;

    /* We could be in trouble if we have nothing queued.
     *   * Use whatever is left in sk->sk_forward_alloc and tcp_wmem[0]
     *       * to guarantee some progress.
     *           */
    left =
        READ_ONCE(sock_net(sk)->ipv4.sysctl_tcp_wmem[0]) - sk->sk_wmem_queued;
    if (left > 0)
        sk_forced_mem_schedule(sk, min(left, copy));
    return min(copy, sk->sk_forward_alloc);
}

static unsigned int tcp_xmit_size_goal(struct sock *sk, u32 mss_now,
                                       int large_allowed)
{
    struct tcp_sock *tp = tcp_sk(sk);
    u32 new_size_goal, size_goal;

    if (!large_allowed)
        return mss_now;

    /* Note : tcp_tso_autosize() will eventually split this later */
    new_size_goal = tcp_bound_to_half_wnd(tp, sk->sk_gso_max_size);

    /* We try hard to avoid divides here */
    size_goal = tp->gso_segs * mss_now;
    if (unlikely(new_size_goal < size_goal ||
                 new_size_goal >= size_goal + mss_now)) {
        tp->gso_segs = min_t(u16, new_size_goal / mss_now, sk->sk_gso_max_segs);
        size_goal = tp->gso_segs * mss_now;
    }

    return max(size_goal, mss_now);
}

int tcp_send_mss(struct sock *sk, int *size_goal, int flags)
{
    int mss_now;

    mss_now = tcp_current_mss(sk);
    *size_goal = tcp_xmit_size_goal(sk, mss_now, !(flags & MSG_OOB));

    return mss_now;
}

struct sk_buff *tcp_stream_alloc_skb(struct sock *sk, gfp_t gfp,
                                     bool force_schedule)
{
    struct sk_buff *skb;

    skb = alloc_skb_fclone(MAX_TCP_HEADER, gfp);
    if (likely(skb)) {
        bool mem_scheduled;

        skb->truesize = SKB_TRUESIZE(skb_end_offset(skb));
        if (force_schedule) {
            mem_scheduled = true;
            sk_forced_mem_schedule(sk, skb->truesize);
        } else {
            mem_scheduled = sk_wmem_schedule(sk, skb->truesize);
        }
        if (likely(mem_scheduled)) {
            skb_reserve(skb, MAX_TCP_HEADER);
            skb->ip_summed = CHECKSUM_PARTIAL;
            INIT_LIST_HEAD(&skb->tcp_tsorted_anchor);
            return skb;
        }
        __kfree_skb(skb);
    } else {
        sk->sk_prot->enter_memory_pressure(sk);
        sk_stream_moderate_sndbuf(sk);
    }
    return NULL;
}

static inline bool forced_push(const struct tcp_sock *tp)
{
    return after(tp->write_seq, tp->pushed_seq + (tp->max_window >> 1));
}

static inline void tcp_mark_urg(struct tcp_sock *tp, int flags)
{
    if (flags & MSG_OOB)
        tp->snd_up = tp->write_seq;
}

/* If a not yet filled skb is pushed, do not send it if
 * we have data packets in Qdisc or NIC queues :
 * Because TX completion will happen shortly, it gives a chance
 * to coalesce future sendmsg() payload into this skb, without
 * need for a timer, and with no latency trade off.
 * As packets containing data payload have a bigger truesize
 * than pure acks (dataless) packets, the last checks prevent
 * autocorking if we only have an ACK in Qdisc/NIC queues,
 * or if TX completion was delayed after we processed ACK packet.
 */
static bool tcp_should_autocork(struct sock *sk, struct sk_buff *skb,
                                int size_goal)
{
    return skb->len < size_goal &&
           READ_ONCE(sock_net(sk)->ipv4.sysctl_tcp_autocorking) &&
           !tcp_rtx_queue_empty(sk) &&
           refcount_read(&sk->sk_wmem_alloc) > skb->truesize &&
           tcp_skb_can_collapse_to(skb);
}

void tcp_push(struct sock *sk, int flags, int mss_now, int nonagle,
              int size_goal)
{
    struct tcp_sock *tp = tcp_sk(sk);
    struct sk_buff *skb;

    skb = tcp_write_queue_tail(sk);
    if (!skb)
        return;
    if (!(flags & MSG_MORE) || forced_push(tp))
        tcp_mark_push(tp, skb);

    tcp_mark_urg(tp, flags);

    if (tcp_should_autocork(sk, skb, size_goal)) {
        /* avoid atomic op if TSQ_THROTTLED bit is already set */
        if (!test_bit(TSQ_THROTTLED, &sk->sk_tsq_flags)) {
            NET_INC_STATS(sock_net(sk), LINUX_MIB_TCPAUTOCORKING);
            set_bit(TSQ_THROTTLED, &sk->sk_tsq_flags);
            smp_mb__after_atomic();
        }
        /* It is possible TX completion already happened
         *       * before we set TSQ_THROTTLED.
         *               */
        if (refcount_read(&sk->sk_wmem_alloc) > skb->truesize)
            return;
    }

    if (flags & MSG_MORE)
        nonagle = TCP_NAGLE_CORK;

    __tcp_push_pending_frames(sk, mss_now, nonagle);
}

/* In some cases, sendmsg() could have added an skb to the write queue,
 * but failed adding payload on it. We need to remove it to consume less
 * memory, but more importantly be able to generate EPOLLOUT for Edge Trigger
 * epoll() users. Another reason is that tcp_write_xmit() does not like
 * finding an empty skb in the write queue.
 */
void tcp_remove_empty_skb(struct sock *sk)
{
    struct sk_buff *skb = tcp_write_queue_tail(sk);

    if (skb && TCP_SKB_CB(skb)->seq == TCP_SKB_CB(skb)->end_seq) {
        tcp_unlink_write_queue(skb, sk);
        if (tcp_write_queue_empty(sk))
            tcp_chrono_stop(sk, TCP_CHRONO_BUSY);
        tcp_wmem_free_skb(sk, skb);
    }
}

/**
 * tcp_send() - sends prepared skb (allocated and data copied into) to the
 * underlying layers. essentially adopted from tcp_sendmsg() in
 * https://elixir.bootlin.com/linux/v4.9.39/source/net/ipv4/tcp.c#L1106.
 * @sk:             the tcp socket from which the data is to be sent.
 * @flag:           the flag for the msg passed from userspace.
 * Return:
 */
int tcp_send(struct sock *sk, int mflags)
{
    struct tcp_sock *tp = tcp_sk(sk);
    long timeo;
    int mss_now = 0, size_goal;
    int err = 0;
    struct sockcm_cookie sockc;

    // TODO: add fast open logic, we assume MSG_DONTWAIT is set to make it
    // nonblocking. Need to examine the correctness and necessity.
    timeo = sock_sndtimeo(sk, mflags & MSG_DONTWAIT);

    tcp_rate_check_app_limited(sk); /* is sending application-limited? */

    /* Wait for a connection to finish. One exception is TCP Fast Open
     * (passive side) where data is allowed to be sent before a connection
     * is fully established.
     */
    if (((1 << sk->sk_state) & ~(TCPF_ESTABLISHED | TCPF_CLOSE_WAIT)) &&
        !tcp_passive_fastopen(sk)) {
        err = sk_stream_wait_connect(sk, &timeo);
        if (err != 0)
            goto do_error;
    }

    // TODO: control msg is ignored for now (see attached code snippet from
    // tcp_sendmsg). to check later.
    //  sockc.tsflags = sk->sk_tsflags;
    //      if (msg->msg_controllen) {
    //              err = sock_cmsg_send(sk, msg, &sockc);
    //                      if (unlikely(err)) {
    //                                  err = -EINVAL;
    //                                              goto out_err;
    //                                                      }
    //                                                          }
    //                      }
    //      }
    sk_clear_bit(SOCKWQ_ASYNC_NOSPACE, sk);

    mss_now = tcp_send_mss(sk, &size_goal, mflags);

    err = -EPIPE;
    if (sk->sk_err || (sk->sk_shutdown & SEND_SHUTDOWN))
        goto do_error;

    // TODO: should we put process_backlog here? In the kernel, it is only
    // checked before skb_alloc. But I believe process_backlog (to-be-checked)
    // needs the socket lock aquired, which we cannot have outside this
    // function.
    sk_flush_backlog(sk);
    tcp_tx_timestamp(sk, sockc.tsflags);
    tcp_push(sk, mflags, mss_now, tp->nonagle, size_goal);
    return 0;

do_error:
    return err;
}
