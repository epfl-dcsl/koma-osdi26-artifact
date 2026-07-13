// SPDX-License-Identifier: GPL-2.0-only
/*
 * Kernel Connection Multiplexor
 *
 * Copyright (c) 2016 Tom Herbert <tom@herbertland.com>
 */

#include "linux/printk.h"
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
#include <linux/socket.h>
#include <linux/syscalls.h>
#include <linux/uaccess.h>
#include <linux/workqueue.h>
#include <linux/jhash.h>

#include <net/tcp.h>
#include <net/netns/generic.h>
#include <net/sock.h>
#include <trace/events/sock.h>
#include <uapi/linux/rakaia.h> //TODO: add the header file in the specified path

#define ENABLE_LOGGING 0 // Set to 1 to enable logging, 0 to disable
#define ENABLE_TIME_TRACE 0  // Set to 1 to enable time tracing
#define SIOCRAKAIAPULL (SIOCPROTOPRIVATE + 3)

#if ENABLE_LOGGING
    // #define pr_info_log(fmt, ...) pr_info(fmt, ##__VA_ARGS__)
    #define pr_info_log(fmt, ...) /* No logging */
    #define pr_info_id(fmt, ...) pr_info("[CPU %d] " fmt, raw_smp_processor_id(), ##__VA_ARGS__)
#else
    #define pr_info_log(fmt, ...) /* No logging */
    #define pr_info_id(fmt, ...) /* No logging */
#endif

// #define trace_record(fmt, ...) trace_printk(fmt, ##__VA_ARGS__)
#define trace_record(fmt, ...) 

#include "rakaia.h"
#include "tcp_send.h"
#include "tls.h"
 
 unsigned int rakaia_net_id;
 
 extern struct workqueue_struct *strp_wq;
 static struct kmem_cache *rakaia_psockp __read_mostly;
 static struct workqueue_struct *rakaia_wq;
 
 /* Global data for rakaia, which allows */
 struct rakaia rakaia_data = { 0 };
 struct rakaia *rakaia = &rakaia_data;
 
 static inline struct rakaia_sock *rakaia_sk(const struct sock *sk)
 {
     return (struct rakaia_sock *)sk;
 }
 
 static inline struct rakaia_tx_msg *rakaia_tx_msg(struct sk_buff *skb)
 {
     return (struct rakaia_tx_msg *)skb->cb;
 }
 
 static void report_csk_error(struct sock *csk, int err)
 {
     csk->sk_err = EPIPE;
     sk_error_report(csk);
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
 
 static int rakaia_queue_rcv_skb(struct rakaia_sock *ksk, struct sk_buff *skb);
 
 static int write_to_csock(struct rakaia_psock *psock);
 
static int rakaia_pull(struct socket *sock);

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
 * rakaia_steal_skb - try to steal one skb from another rakaia socket.
 */
static struct sk_buff *rakaia_steal_skb(struct rakaia_sock *ksk)
{
    unsigned long mask;
    int v1, v2, victim;
    struct rakaia_sock *s1, *s2;
    struct sk_buff_head *list;
    struct sk_buff *skb;

retry:
    /* Fast global liveness check */
    mask = READ_ONCE(rakaia->rx_nonempty_bm[0]);
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

    s1 = rakaia->rakaia_socks[v1];
    s2 = rakaia->rakaia_socks[v2];

    if (READ_ONCE(s2->rx_q.qlen) > READ_ONCE(s1->rx_q.qlen))
        victim = v2;
    else
        victim = v1;

    list = &rakaia->rakaia_socks[victim]->rx_q;

    spin_lock_bh(&list->lock);
    skb = __skb_dequeue(list);

    if (skb) {
        if (list->qlen == 0)
            clear_bit(victim, rakaia->rx_nonempty_bm);

        ksk->steal_cnt++;
        spin_unlock_bh(&list->lock);
        return skb;
    }
    spin_unlock_bh(&list->lock);

    goto retry;
}

/**
 * rakaia_get_avail_sk() - pop one idle rakaia socket from the waiter list. 
 * must only be called with waiters_lock held. 
 * Return:  the available rakaia socket.
 */
static struct rakaia_sock *rakaia_get_avail_sk(struct rakaia_sock *local_ksk)
{
    struct rakaia_sock *ksk = NULL;

    if (list_empty(&rakaia->ksk_waiters)){
        return NULL;
    }

    /* Prefer local rakaia_sock first */
    if (local_ksk && test_bit(local_ksk->index, rakaia->waiters_bm)){
        // local rakaia_sock is idle
        ksk = local_ksk;
    } else {
        ksk = list_first_entry(&rakaia->ksk_waiters, struct rakaia_sock,
                               ksk_waiting_list);
    }
    list_del_init(&ksk->ksk_waiting_list);
    clear_bit(ksk->index, rakaia->waiters_bm);
    return ksk;
}

/**
 * rakaia_push_skb_single_queue() - the central scheduler pushes the skb to an
 * available rakaia socket.
 * @skb:    the skb to be pushed to a rakaia socket.
 * @psock:  psock connected to the tcp sock where the skb is from.
 * Return:  0
 */
static int rakaia_push_skb_single_queue(struct sk_buff *skb,
                                      struct rakaia_psock *psock)
{

    struct rakaia_sock *ksk1, *ksk2, *chosen, *new_ksk;
    int num1, num2;
    struct sk_buff_head *list;

    skb->dev = NULL;
    skb_orphan(skb);
    *(struct rakaia_psock **)&skb->cb[0] = psock;

    num1 = get_random_u8() % NUM_RAKAIA_SOCKETS;
    num2 = get_random_u8() % NUM_RAKAIA_SOCKETS;

    ksk1 = rakaia->rakaia_socks[num1];
    ksk2 = rakaia->rakaia_socks[num2];

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

    spin_lock_bh(&rakaia->waiters_lock);
    __skb_queue_tail(list, skb);
    new_ksk = rakaia_get_avail_sk(chosen);
    spin_unlock_bh(&rakaia->waiters_lock);

    if (!new_ksk) {
        set_bit(chosen->index, rakaia->rx_nonempty_bm);
        spin_unlock_bh(&list->lock);
        return 0;
    }

    skb = __skb_dequeue(list);
    if (list->qlen >= 1) 
        set_bit(chosen->index, rakaia->rx_nonempty_bm);
    else 
        clear_bit(chosen->index, rakaia->rx_nonempty_bm);
    spin_unlock_bh(&list->lock);
    rakaia->push_cnt++;
    return rakaia_queue_rcv_skb(new_ksk, skb);
}

 /**
  * rakaia_rfree() - callback function triggered when a skb is destructed.
  * The context for this function is before the skb is queued to a rakaia socket.
  * @skb:    the skb which is destructed.
  */
 static void rakaia_rfree(struct sk_buff *skb)
 {
     struct sock *sk = skb->sk;
     unsigned int len = skb->truesize;
 
     sk_mem_uncharge(sk, len);
     atomic_sub(len, &sk->sk_rmem_alloc);
     smp_mb__after_atomic();
 }
 
/**
 * rakaia_queue_rcv_skb() - queue a skb to a specific rakaia socket.
 * @ksk:    the rakaia socket where the skb is to be queued at.
 * @skb:    the skb to be queue at the rakaia socket.
 * Return:  0 on success.
 */
static int rakaia_queue_rcv_skb(struct rakaia_sock *ksk, struct sk_buff *skb)
{
    struct sock *sk = &ksk->sk;
    struct sk_buff_head *list = &sk->sk_receive_queue;

    if (atomic_read(&sk->sk_rmem_alloc) >= sk->sk_rcvbuf)
        return -ENOMEM;

    if (!sk_rmem_schedule(sk, skb, skb->truesize))
        return -ENOBUFS;

    skb->sk = &ksk->sk;
    skb->destructor = rakaia_rfree;
    atomic_add(skb->truesize, &sk->sk_rmem_alloc);
    sk_mem_charge(sk, skb->truesize);

    skb_queue_tail(list, skb);

    if (!sock_flag(sk, SOCK_DEAD))
        sk->sk_data_ready(sk); // wake up application thread.
    return 0;
}

 
 static void rakaia_done(struct rakaia_sock *rakaia);
 
 /**
  * psock_data_ready() - callback function substituting the original
  * tcp_data_ready function. Used to link to strparser and call strp_data_ready
  * to parse requests from tcp data streams.
  * @ksk:    the rakaia socket where the skb is to be queued at.
  * @skb:    the skb to be queue at the rakaia socket.
  * Return:  0 on success.
  */
 static void psock_data_ready(struct sock *sk)
 {
     struct rakaia_psock *psock;
 
     trace_sk_data_ready(sk);
 
     read_lock_bh(&sk->sk_callback_lock);
 
     psock = (struct rakaia_psock *)sk->sk_user_data;
     pr_info_log("receive data at psock %d", psock->hash_id);
 
     if (likely(psock))
         strp_data_ready(&psock->strp);
 
     read_unlock_bh(&sk->sk_callback_lock);
 }
 
 /**
  * rakaia_rcv_strparser() - callback function when strparser gets a full message.
  * Called with lower sock held.
  * @strp:   strparser instance.
  * @skb:    the skb to be queued & contains the message.
  */
 static void rakaia_rcv_strparser(struct strparser *strp, struct sk_buff *skb)
 {
     struct rakaia_psock *psock = container_of(strp, struct rakaia_psock, strp);
     if (unlikely(!psock || !psock->sk)) {
         kfree_skb(skb);
         return;
     }

     rakaia_push_skb_single_queue(skb, psock);
 }
 
 /**
  * rakaia_parse_func_strparser() - strparser callback function for parsing tcp
  * data. This function calls the ebpf program for parsing messages.
  * @strp:   strparser instance.
  * @skb:    the skb to be parsed.
  */
 static int rakaia_parse_func_strparser(struct strparser *strp,
                                      struct sk_buff *skb)
 {
     struct binary_header_t hdr;
     int skb_offset = *((int *)skb->cb);

     struct binary_header_t *phdr =
         skb_header_pointer(skb, skb_offset, sizeof(hdr), &hdr);

     if (!phdr)
         return -EINVAL;

     /* Extract fields (converted to host byte order) */
     u8 opcode = phdr->opcode;
     u16 key_len = be16_to_cpu(phdr->key_len);
     u8 extra_len = phdr->extra_len;
     u32 body_len = be32_to_cpu(phdr->body_len);
     u32 header_len = sizeof(struct binary_header_t);

     /*
      * Return the total size of the full message,
      * exactly like your BPF function.
      */
     if (opcode == CMD_SET)
         return header_len + body_len;

     return header_len + key_len + extra_len;
  }
 
 static void psock_state_change(struct sock *sk)
 {
     /* TCP only does a EPOLLIN for a half close. Do a EPOLLHUP here
      * since application will normally not poll with EPOLLIN
      * on the TCP sockets.
      */
 
     report_csk_error(sk, EPIPE);
 }
 

static void rakaia_tx_work(struct work_struct *w)
{
    struct rakaia_psock *psock = container_of(w, struct rakaia_psock, tx_work);
    struct sock *csk = psock->sk;
    struct tcp_sock *tp;
    int size_goal;
    int mss_now;

    if (!csk)
        return;

    // tp = tcp_sk(csk);

    /* Best-effort MSS/size_goal; use nonblocking flags */
    // mss_now = tcp_send_mss(csk, &size_goal, MSG_DONTWAIT);

    /* No associated rakaia_sock here for stats; pass -1 as index */
    // try_send_from_psock(0, psock, MSG_DONTWAIT, mss_now, size_goal, 0);
}

static void psock_write_space(struct sock *sk)
{
    struct rakaia_psock *psock;
    struct sk_buff *head;
    // read_lock_bh(&sk->sk_callback_lock);

    // psock = (struct rakaia_psock *)sk->sk_user_data;
    // spin_lock_bh(&psock->lock);
    // head = skb_peek(&psock->tx_wait_queue);
    // if (head)
    //     pr_info("%u peek skb in psock_write_space", head->mark);
    // psock->tx_in_use = false;
    // spin_unlock_bh(&psock->lock);
    // if (likely(psock)) {
    //     queue_work(rakaia_wq, &psock->tx_work);
    // }
    // read_unlock_bh(&sk->sk_callback_lock);
}

 
 
 /**
  * write_to_csock() - writing tx messages accumulated at psock->tx_wait_queue to
  * the underlying tcp sockets.
  * @psock:  psock instance
  */
 static int write_to_csock(struct rakaia_psock *psock)
 {
     int ret;
     unsigned int total_sent = 0;
     struct sk_buff *head;
     while (1) {
         spin_lock_bh(&psock->lock);
         head = skb_peek(&psock->tx_wait_queue);
         spin_unlock_bh(&psock->lock);
         if (!head) {
             break;
         }
 
         struct msghdr msg = {
             .msg_flags = MSG_DONTWAIT | MSG_SPLICE_PAGES,
         };
         struct rakaia_tx_msg *txm = rakaia_tx_msg(head);
         struct sk_buff *skb;
         unsigned int msize = 0;
         int i = 0;
         if (!txm->started_tx) {
             skb = head;
             txm->frag_offset = 0;
             txm->sent = 0;
             txm->started_tx = true;
         } else {
             if (WARN_ON(!psock)) {
                 ret = -EINVAL;
                 return ret;
             }
             skb = txm->frag_skb;
         }
         if (WARN_ON(!skb_shinfo(skb)->nr_frags) ||
             WARN_ON_ONCE(!skb_frag_page(&skb_shinfo(skb)->frags[0]))) {
             ret = -EINVAL;
             return ret;
         }
         for (i = 0; i < skb_shinfo(skb)->nr_frags; i++)
             msize += skb_frag_size(&skb_shinfo(skb)->frags[i]);
 
         iov_iter_bvec(&msg.msg_iter, ITER_SOURCE,
                       (const struct bio_vec *)skb_shinfo(skb)->frags,
                       skb_shinfo(skb)->nr_frags, msize);
         iov_iter_advance(&msg.msg_iter, txm->frag_offset);
 
         do {
             ret = sock_sendmsg(psock->sk->sk_socket, &msg);
             if (ret <= 0) {
                 if (ret == -EAGAIN) {
                     /* Save state to try again when there's
                      * write space on the socket
                      */
                     // pr_info("sock_sendmsg returns EAGAIN");
                     txm->frag_skb = skb;
                     ret = 0;
                     return ret;
                 }
 
                 /* Hard failure in sending message, abort this
                  * psock since it has lost framing
                  * synchronization and retry sending the
                  * message from the beginning.
                  */
 
                 /* TODO: disable this psock so that no more
                  * same issue for invalid psock (usually because
                  * the tcp connection is closed) */
                 pr_info("sock_sendmsg returns hard failure %d", ret);
                //  txm->started_tx = false;
                 return ret;
             }
             txm->sent += ret;
             txm->frag_offset += ret;
         } while (msg.msg_iter.count > 0);
 
         if (skb == head) {
             if (skb_has_frag_list(skb)) {
                 txm->frag_skb = skb_shinfo(skb)->frag_list;
                 txm->frag_offset = 0;
                 continue;
             }
         } else if (skb->next) {
             txm->frag_skb = skb->next;
             txm->frag_offset = 0;
             continue;
         }
 
         /* Successfully sent the whole packet or saved it to the tx_wait_queue,
          * account for it. */
         txm->ksk->sk.sk_wmem_queued -= txm->sent;
         total_sent += txm->sent;
 
         spin_lock_bh(&psock->lock);
         skb_dequeue(&psock->tx_wait_queue);
         spin_unlock_bh(&psock->lock);
         kfree_skb(head);
     }
     return total_sent;
 }
 
 /**
  * rakaia_write_msgs() - write messages ready to be sent on the rakaia socket.
  * Called with rakaia sock lock held.
  * @ksk: rakaia socket
  * Return: number of bytes actually sent or error.
  */
 static int rakaia_write_msgs(struct rakaia_sock *ksk)
 {
     unsigned int total_sent = 0;
     struct sock *sk = &ksk->sk;
     struct rakaia_psock *psock = NULL;
     struct sk_buff *head;
     int ret = 0;
 
     ksk->tx_wait_more = false;
     while ((head = skb_dequeue(&sk->sk_write_queue))) {
         struct rakaia_tx_msg *txm = rakaia_tx_msg(head);

         psock = txm->psock;
         if (!psock) {
             pr_info_log("psock does not exist any more!\n");
             return 0;
         }

         spin_lock_bh(&psock->lock);
 
         // queue message at the head of a sk_buff queue.
         skb_queue_tail(&psock->tx_wait_queue, head);
         if (psock->tx_in_use == true) {
             spin_unlock_bh(&psock->lock);
             continue;
         } else {
             psock->tx_in_use = true;
         }
         spin_unlock_bh(&psock->lock);
 
         ret = write_to_csock(psock);
         if (ret <= 0) {
             // hard failure or EAGAIN, in both cases stop sending to the 
             // underlying tcp socket. 
             spin_lock_bh(&psock->lock);
             psock->tx_in_use = true;
             spin_unlock_bh(&psock->lock);
             goto out;
         } else {
             total_sent += ret;
         }
 
         spin_lock_bh(&psock->lock);
         psock->tx_in_use = false;
         spin_unlock_bh(&psock->lock);
     }
 out:
     if (!head) {
         /* Done with all queued messages. */
         WARN_ON(!skb_queue_empty(&sk->sk_write_queue));
     }
     /* Check if write space is available */
     sk->sk_write_space(sk);
     return total_sent ?: ret;
 }
 
 static void rakaia_push(struct rakaia_sock *ksk)
 {
     if (ksk->tx_wait_more)
         rakaia_write_msgs(ksk);
 }
 
 static int rakaia_sendmsg(struct socket *sock, struct msghdr *msg, size_t len)
 {
     struct sock *sk = sock->sk;
     struct rakaia_sock *ksk = rakaia_sk(sk);
     struct sk_buff *skb = NULL, *head = NULL;
     size_t copy, copied = 0;
     long timeo = sock_sndtimeo(sk, msg->msg_flags & MSG_DONTWAIT);
     struct rakaia_tx_msg *txm = rakaia_tx_msg(head);

     int eor = (sock->type == SOCK_DGRAM) ? !(msg->msg_flags & MSG_MORE) :
                                            !!(msg->msg_flags & MSG_EOR);
     int err = -EPIPE;
 
    //  lock_sock(sk);
 
     /* Per tcp_sendmsg this should be in poll */
     sk_clear_bit(SOCKWQ_ASYNC_NOSPACE, sk); // clear flag to indicate this send
                                             // buffer has available space again
     if (sk->sk_err)
         goto out_error;
 
     /* Check previously opened message. If exists, starts from there */
     if (ksk->seq_skb) {
         head = ksk->seq_skb;
         skb = rakaia_tx_msg(head)->last_skb;
         goto start;
     }
 
     /* Check if there is free space in the socket's send buffer */
     if (!sk_stream_memory_free(sk)) {
         rakaia_push(ksk);
         set_bit(SOCK_NOSPACE, &sk->sk_socket->flags); // set nospace bit
 
         // blocking until there is more memory in the socket send buffer.
         err = sk_stream_wait_memory(sk, &timeo);
         if (err)
             goto out_error;
     }
 
     // check if there is data left to process in the current msg
     if (msg_data_left(msg)) {
         /* New message, alloc head skb */
         pr_info_log("New message, alloc head skb!\n");
         head = alloc_skb(0, sk->sk_allocation);
         while (!head) {
             rakaia_push(ksk);
             err = sk_stream_wait_memory(sk, &timeo);
             if (err)
                 goto out_error;
 
             head = alloc_skb(0, sk->sk_allocation);
         }
 
         skb = head;

         /* Set ip_summed to CHECKSUM_UNNECESSARY to avoid calling
          * csum_and_copy_from_iter from skb_do_copy_data_nocache.
          */
         skb->ip_summed = CHECKSUM_UNNECESSARY;
     }
 
 start:
     while (msg_data_left(msg)) {
         bool merge = true;
         int i = skb_shinfo(skb)->nr_frags;
         struct page_frag *pfrag = sk_page_frag(sk);
 
         // check if there is space in the current page fragment
         if (!sk_page_frag_refill(sk, pfrag))
             goto wait_for_memory;
 
         // check if data can be appended to the current fragment
         if (!skb_can_coalesce(skb, i, pfrag->page, pfrag->offset)) {
             if (i == MAX_SKB_FRAGS) {
                 struct sk_buff *tskb;
 
                 tskb = alloc_skb(0, sk->sk_allocation);
                 if (!tskb)
                     goto wait_for_memory;
 
                 if (head == skb)
                     skb_shinfo(head)->frag_list = tskb;
                 else
                     skb->next = tskb;
 
                 skb = tskb;
                 skb->ip_summed = CHECKSUM_UNNECESSARY;
                 continue;
             }
             merge = false;
         }
 
         // Handle zero-copy transmission using MSG_SPLICE_PAGES.
         if (msg->msg_flags & MSG_SPLICE_PAGES) {
             copy = msg_data_left(msg);
             if (!sk_wmem_schedule(sk, copy))
                 goto wait_for_memory;
 
             err = skb_splice_from_iter(skb, &msg->msg_iter, copy,
                                        sk->sk_allocation);
             if (err < 0) {
                 if (err == -EMSGSIZE)
                     goto wait_for_memory;
                 goto out_error;
             }
 
             copy = err;
             skb_shinfo(skb)->flags |= SKBFL_SHARED_FRAG;
             sk_wmem_queued_add(sk, copy);
             sk_mem_charge(sk, copy);
 
             if (head != skb)
                 head->truesize += copy;
         } else {
             // Copy data into skb fragments when MSG_SPLICE_PAGES is not used.
             copy = min_t(int, msg_data_left(msg), pfrag->size - pfrag->offset);
             if (!sk_wmem_schedule(sk, copy))
                 goto wait_for_memory;
 
             // data copying
             err = skb_copy_to_page_nocache(sk, &msg->msg_iter, skb, pfrag->page,
                                            pfrag->offset, copy);
             if (err)
                 goto out_error;
 
             /* Update the skb. */
             if (merge) {
                 skb_frag_size_add(&skb_shinfo(skb)->frags[i - 1], copy);
             } else {
                 skb_fill_page_desc(skb, i, pfrag->page, pfrag->offset, copy);
                 get_page(pfrag->page);
             }
 
             pfrag->offset += copy;
         }
 
         copied += copy;
         if (head != skb) {
             head->len += copy;
             head->data_len += copy;
         }
 
         continue;
 
     wait_for_memory:
         rakaia_push(ksk);
         err = sk_stream_wait_memory(sk, &timeo);
         if (err)
             goto out_error;
     }
 
     if (eor) {
         bool not_busy = skb_queue_empty(&sk->sk_write_queue);
 
         if (head) {
             /* Message complete, queue it on send buffer */
             txm = rakaia_tx_msg(head);
             txm->ksk = ksk;
             txm->psock = ksk->last_psock;
             __skb_queue_tail(&sk->sk_write_queue, head);
             ksk->seq_skb = NULL;
         }
 
         if (msg->msg_flags & MSG_BATCH) {
             ksk->tx_wait_more = true;
         } else if (ksk->tx_wait_more || not_busy) {
             /* TODO: to optimize the logic here */
             pr_info_log("[rakaia_sendmsg] Calling rakaia_write_msgs %d %d\n",
                         ksk->tx_wait_more, not_busy);
             err = rakaia_write_msgs(ksk);
             if (err < 0) {
                 /* We got a hard error in write_msgs but have
                  * already queued this message. Report an error
                  * in the socket, but don't affect return value
                  * from sendmsg
                  */
                 pr_warn("RAKAIA: Hard failure on rakaia_write_msgs\n");
                 /*report_csk_error(&ksk->sk, -err);*/
             }
         }
     } else {
         /* Message not complete, save state */
     partial_message:
         if (head) {
             ksk->seq_skb = head;
             rakaia_tx_msg(head)->last_skb = skb;
         }
     }
 
     /* TODO: Figure out the locking here */
     if (eor)
         rakaia_pull(sock);

    //  release_sock(sk);
     return copied;
 
 out_error:
     rakaia_push(ksk);
 
     if (sock->type == SOCK_SEQPACKET) {
         /* Wrote some bytes before encountering an
          */
         if (copied)
             goto partial_message;
         if (head != ksk->seq_skb)
             kfree_skb(head);
     } else {
         kfree_skb(head);
         ksk->seq_skb = NULL;
     }
 
     err = sk_stream_error(sk, msg->msg_flags, err);
 
     /* make sure we wake any epoll edge trigger waiter */
     if (unlikely(skb_queue_len(&sk->sk_write_queue) == 0 && err == -EAGAIN))
         sk->sk_write_space(sk);
 
     release_sock(sk);
     return err;
 }
 
 static void rakaia_splice_eof(struct socket *sock)
 {
     struct sock *sk = sock->sk;
     struct rakaia_sock *rakaia = rakaia_sk(sk);
 
     if (skb_queue_empty_lockless(&sk->sk_write_queue))
         return;
 
     lock_sock(sk);
     rakaia_write_msgs(rakaia);
     release_sock(sk);
 }
 
 static int rakaia_recvmsg(struct socket *sock, struct msghdr *msg, size_t len,
                         int flags)
 {
     struct sock *sk = sock->sk;
     struct rakaia_sock *ksk = rakaia_sk(sk);
     int err = 0;
     struct strp_msg *stm;
     int copied = 0;
     struct sk_buff *skb;
 
     /* TODO: if no skb should try slow requests. */
     pr_info_log("start processing recvmsg\n");
 
     /*skb = __skb_recv_datagram(sk, &sk->sk_receive_queue, flags, 0,
      * &err);*/
     skb = skb_recv_datagram(sk, flags, &err);
     if (!skb)
         goto out;
 
     /*if (*skb->data != 0x80)*/
     pr_info_log("[recvmsg] the first byte is 0x%x\n", *skb->data);
     /* Okay, have a message on the receive queue */

     struct rakaia_psock *psock = *(struct rakaia_psock **)&skb->cb[0];
     ksk->last_psock = psock;

     stm = strp_msg(skb);

     if (len > stm->full_len)
         len = stm->full_len;

     err = skb_copy_datagram_msg(skb, stm->offset, msg, len);
     if (err < 0)
         goto out;

     copied = len;
     if (likely(!(flags & MSG_PEEK))) {
         if (copied < stm->full_len) {
             if (sock->type == SOCK_DGRAM) {
                 /* Truncated message */
                 msg->msg_flags |= MSG_TRUNC;
                 goto msg_finished;
             }
             stm->offset += copied;
             stm->full_len -= copied;
         } else {
         msg_finished:
             /* Finished with message */
             msg->msg_flags |= MSG_EOR;
             /*RAKAIA_STATS_INCR(ksk->stats.rx_msgs);*/
         }
     }
 
 out:
     // if (ksk->index == 0)
     //     pr_info("[%lld] [Latency] start freeing skb from rakaia socket
     //     index 0\n", ktime_get_ns() / 1000);
     skb_free_datagram(sk, skb);
     return copied ?: err;
 }
 
 static ssize_t rakaia_splice_read(struct socket *sock, loff_t *ppos,
                                 struct pipe_inode_info *pipe, size_t len,
                                 unsigned int flags)
 {
     struct sock *sk = sock->sk;
     struct rakaia_sock *rakaia = rakaia_sk(sk);
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
 
 static int rakaia_setsockopt(struct socket *sock, int level, int optname,
                            sockptr_t optval, unsigned int optlen)
 {
     int val, valbool;
     int err = 0;
 
     if (level != SOL_RAKAIA)
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
 
 static int rakaia_getsockopt(struct socket *sock, int level, int optname,
                            char __user *optval, int __user *optlen)
 {
     int val, len;
 
     if (level != SOL_RAKAIA)
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
 
 /**
  * rakaia_attach() - Implements the ioctl rakaia_attach for a given rakaia socket.
  * In the original KCM design, when a kcm socket was created, a multiplexor
  * was also created automatically. The tcp socket will be attached to the
  * multiplexor through this call. However, in the Rakaia design, we dont need
  * a multiplexor attached to any specific set of rakaia sockets. To achieve
  * this, we abort the abstraction of multiplexor and only have a psock
  * attached to the tcp socket. Note that here we use the thread-local rakaia
  * socket to do the ioctl, even there is no mapping relationship between
  * this rakaia socket and the tcp socket.
  * @sock:      Rakaia socket on which the ioctl call was invoked.
  * @csock:     TCP socket to be attached to the Rakaia attached.
  * @prog:      BPF program to be attached to the psock for message parsing.
  * Return:     0 on success, otherwise a negative errno.
  */
 static int rakaia_attach(struct socket *sock, struct socket *csock)
 {
     struct rakaia_sock *ksk = rakaia_sk(sock->sk);
     struct rakaia_net *knet = ksk->knet;
     struct rakaia_psock_bucket *bucket;
     struct rakaia_psock *psock = NULL;

     /* Create psock */
     psock = kmem_cache_zalloc(rakaia_psockp, GFP_KERNEL);
     if (!psock) {
         return -ENOMEM;
     }
 
     psock->knet = knet;
 
     /* Add new psock to the list */
     mutex_lock(&knet->mutex);
     list_add_rcu(&psock->rakaia_psock_list, &knet->psock_list);
     knet->count++;
     mutex_unlock(&knet->mutex);
 
     struct sock *csk;
     static const struct strp_callbacks cb = {
         .rcv_msg = rakaia_rcv_strparser,
         .parse_msg = rakaia_parse_func_strparser,
     };
     int err = 0;
 
     csk = csock->sk;
     if (!csk)
         return -EINVAL;
 
     lock_sock(csk);
 
     /* create a hash id for the psock */
     psock->hash_id = hash_tcp_tuple(csk->sk_daddr, csk->sk_dport);
 
     pr_info_log(
         "Created rakaia psock %d with bucket idx % d for new connection %08X %04X!\n",
         psock->hash_id, psock->hash_id % NUM_BUCKETS, ntohl(csk->sk_daddr),
         ntohs(csk->sk_dport));
 
     /* Add the new psock to the rakaia->rakaia_psock_buckets */
     bucket = &rakaia->rakaia_psock_buckets[psock->hash_id % NUM_BUCKETS];
 
     spin_lock_bh(&bucket->lock);
     hlist_add_head_rcu(&psock->hash_links, &bucket->psocks);
     spin_unlock_bh(&bucket->lock);
 
     spin_lock_bh(&rakaia->lock);
     rakaia->rakaia_psock_cnt++;
     spin_unlock_bh(&rakaia->lock);
 
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
 
     /* Check if sk_user_data is already by RAKAIA or someone else.
      * Must be done under lock to prevent race conditions.
      */
     if (csk->sk_user_data) {
         write_unlock_bh(&csk->sk_callback_lock);
         kmem_cache_free(rakaia_psockp, psock);
         err = -EALREADY;
         goto out;
     }
 
     err = strp_init(&psock->strp, csk, &cb);
     if (err) {
         write_unlock_bh(&csk->sk_callback_lock);
         kmem_cache_free(rakaia_psockp, psock);
         goto out;
     }

     struct tls_context *tls_ctx = tls_get_ctx(csk);
     struct tls_sw_context_rx *ctx = tls_sw_ctx_rx(tls_ctx); 
 
     psock->save_data_ready = ctx->saved_data_ready;
     psock->save_write_space = csk->sk_write_space;
     psock->save_state_change = csk->sk_state_change;
     csk->sk_user_data = psock;
     ctx->saved_data_ready = psock_data_ready;
    //  csk->sk_data_ready = psock_data_ready;
    //  csk->sk_write_space = psock_write_space;
    //  csk->sk_state_change = psock_state_change;
 
     write_unlock_bh(&csk->sk_callback_lock);
 
     sock_hold(csk);
 
     psock->tx_in_use = false;
     INIT_WORK(&psock->tx_work, rakaia_tx_work);
 out:
     release_sock(csk);
 
     return err;
 }
 
 static int rakaia_attach_ioctl(struct socket *sock, struct rakaia_attach *info)
 {
     struct socket *csock;
     struct bpf_prog *prog;
     int err;
 
     csock = sockfd_lookup(info->fd, &err);
     if (!csock)
         return -ENOENT;
 
     err = rakaia_attach(sock, csock);
     if (err) {
         goto out;
     }
 
     /* Keep reference on file also */
     return 0;
 out:
     sockfd_put(csock);
     return err;
 }
 
static int rakaia_pull(struct socket *sock)
{
    struct rakaia_sock *ksk = rakaia_sk(sock->sk);
    struct sk_buff_head *list = &ksk->rx_q;
    struct sk_buff *skb = NULL;

again:
    // first pull from local queue.
    spin_lock_bh(&list->lock);
    skb = __skb_dequeue(list);
    if (skb) {
        if (list->qlen == 0)
            clear_bit(ksk->index, rakaia->rx_nonempty_bm);
        spin_unlock_bh(&list->lock);
        return rakaia_queue_rcv_skb(ksk, skb);
    }
    spin_unlock_bh(&list->lock);

    // // local queue is empty, steal from other queues.
    // ksk->steal_cnt++;
    skb = rakaia_steal_skb(ksk);
    if (skb)
        return rakaia_queue_rcv_skb(ksk, skb);

    // whole system empty, mark self as idle
    spin_lock_bh(&rakaia->waiters_lock);
    if (READ_ONCE(rakaia->rx_nonempty_bm[0]) != 0){
        spin_unlock_bh(&rakaia->waiters_lock);
        goto again;
    }
    if (!test_bit(ksk->index, rakaia->waiters_bm)) {
        set_bit(ksk->index, rakaia->waiters_bm);
        list_add_tail(&ksk->ksk_waiting_list, &rakaia->ksk_waiters);
    }
    spin_unlock_bh(&rakaia->waiters_lock);
    return 0;
}

 
 
 /* containing functions to define the protocol operations for protocol.
  * for example, function on how to establish a connection
  * since KCM does not need a protocol implementation (heavy lifting done by
  * tcp), it is more or less a place holder. Note that it is lower-level than
  * `struct proto_ops`
  */
 static struct proto rakaia_proto = {
     .name = "RAKAIA",
     .owner = THIS_MODULE,
     .obj_size = sizeof(struct rakaia_sock),
 };
 
 static int rakaia_ioctl(struct socket *sock, unsigned int cmd, unsigned long arg)
 {
     int err;
 
     switch (cmd) {
     case SIOCRAKAIAATTACH: {
         struct rakaia_attach info;
 
         if (copy_from_user(&info, (void __user *)arg, sizeof(info)))
             return -EFAULT;
 
         err = rakaia_attach_ioctl(sock, &info);
 
         break;
     }
     case SIOCRAKAIAPULL: {
         err = rakaia_pull(sock);
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
     struct rakaia_psock *psock = container_of(rcu, struct rakaia_psock, rcu);
     struct sock *csk = psock->sk;
 
     // clean the tcp socket before freeing psock
     lock_sock(csk);
     write_lock_bh(&csk->sk_callback_lock);
     csk->sk_user_data = NULL;
     csk->sk_data_ready = psock->save_data_ready;
     csk->sk_write_space = psock->save_write_space;
     csk->sk_state_change = psock->save_state_change;
     strp_stop(&psock->strp);
     write_unlock_bh(&csk->sk_callback_lock);
     release_sock(csk);
     
     cancel_work_sync(&psock->strp.work);
     strp_done(&psock->strp);
 
     pr_info_log("[free_psock] Start freeing psock for %d", psock->hash_id);
     kmem_cache_free(rakaia_psockp, psock);
     pr_info_log("[free_psock] Freed psock for %d", psock->hash_id);
 }
 
 static void release_psock(struct rakaia_psock *psock)
 {
     struct rakaia_net *knet = psock->knet;
 
     mutex_lock(&knet->mutex);
     list_del_rcu(&psock->rakaia_psock_list);
     knet->count--;
     mutex_unlock(&knet->mutex);
 
     /*call_rcu(&mux->rcu, free_mux);*/
    call_rcu(&psock->rcu, free_psock);
 }
 
 static void rakaia_done(struct rakaia_sock *ksk)
 {
     sock_put(&ksk->sk);
 }
 
 /**
  * rakaia_release() - called to close a rakaia socket. If this is the last rakaia
  * socket, destroy all psocks.
  * TODO: theoretically, psock should only be freed on the unattachment or
  * terminating of a tcp connection. Should implement that instead.
  * @sock:   the socket data structure for the closing rakaia socket.
  * Return:  0 on success
  */
 static int rakaia_release(struct socket *sock)
 {
     struct sock *sk = sock->sk;
     struct rakaia_sock *ksk;
     struct rakaia_psock *tpsock;
     struct rakaia_psock_bucket *bucket;
     struct hlist_node *tmp;
 
     if (!sk)
         return 0;
 
     ksk = rakaia_sk(sk);
     pr_info("Releasing rakaia socket %d", ksk->index);
 
     lock_sock(sk);
     sock_orphan(sk);
     kfree_skb(ksk->seq_skb);
 
     /* Purge/Eempty a queue under lock to avoid race condition with tx_work
      * trying to act when queue is nonempty. If tx_work runs after this
      * point it will just return.
      */
     __skb_queue_purge(&sk->sk_write_queue);
 
     /* Set tx_stopped. This is checked when psock is bound to a rakaia and we
      * get a writespace callback. This prevents further work being queued
      * from the callback (unbinding the psock occurs after canceling work.
      */
     ksk->tx_stopped = 1;
     release_sock(sk);
 
     /* Remove rakaia socks from the array and decrease the count
      * by one
      * TODO: it will cause a sparse problem. Need to fix it later.
      */
     spin_lock_bh(&rakaia->lock);
     if (ksk->index <= NUM_RAKAIA_SOCKETS) {
         rakaia->rakaia_socks[ksk->index] = NULL;
         rakaia->rakaia_socks_cnt--;
         pr_info_log(
             "[rakaia_release] Setting %d rakaia socket to null and decrease rakaia_socks_cnt to %d!\n",
             ksk->index, rakaia->rakaia_socks_cnt);
     } else {
         pr_info_log("[rakaia_release] found invalid rakaia socket with index %d!",
                     ksk->index);
     }
 
     if (rakaia->rakaia_socks_cnt == 0) {
         pr_info_log("[rakaia_release] rakaia_socks_cnt reaches 0, clean psocks\n");
         // pr_info("pull_cnt and push_cnt are %zu and %zu\n", rakaia->pull_cnt
         // - rakaia->push_cnt, rakaia->push_cnt);
         /* All rakaia sockets does not exist any more. remove all psocks
          */
         for (int i = 0; i < NUM_BUCKETS; i++) {
             bucket = &rakaia->rakaia_psock_buckets[i];
             if (bucket) {
                 hlist_for_each_entry_safe(tpsock, tmp, &bucket->psocks,
                                           hash_links)
                 {
                     if (tpsock) {
                         pr_info_log(
                             "[rakaia_release] Starting releasing psock\n");
 
                         release_psock(tpsock);
                         rakaia->rakaia_psock_cnt--;
                         hlist_del(&tpsock->hash_links);
                     }
                 }
             }
         }
     }
     spin_unlock_bh(&rakaia->lock);
 
     /* Cancel work. After this point there should be no outside references
      * to the rakaia socket.
      */
     /*cancel_work_sync(&ksk->tx_work);*/
 
     WARN_ON(ksk->tx_wait);
 
     sock->sk = NULL;
 
     rakaia_done(ksk);
 
     return 0;
 }
 
 /* the two structure dfines functions that handle various opreations on kcm
  * sockets. they are relatively generic -- called to implement top-level
  * system calls.
  */
 static const struct proto_ops rakaia_dgram_ops = {
     .family = PF_RAKAIA,
     .owner = THIS_MODULE,
     .release = rakaia_release,
     .bind = sock_no_bind,
     .connect = sock_no_connect,
     .socketpair = sock_no_socketpair,
     .accept = sock_no_accept,
     .getname = sock_no_getname,
     .poll = datagram_poll,
     .ioctl = rakaia_ioctl,
     .listen = sock_no_listen,
     .shutdown = sock_no_shutdown,
     .setsockopt = rakaia_setsockopt,
     .getsockopt = rakaia_getsockopt,
     .sendmsg = rakaia_sendmsg,
     .recvmsg = rakaia_recvmsg,
     .mmap = sock_no_mmap,
     .splice_eof = rakaia_splice_eof,
 };
 
 static const struct proto_ops rakaia_seqpacket_ops = {
     .family = PF_RAKAIA,
     .owner = THIS_MODULE,
     .release = rakaia_release,
     .bind = sock_no_bind,
     .connect = sock_no_connect,
     .socketpair = sock_no_socketpair,
     .accept = sock_no_accept,
     .getname = sock_no_getname,
     .poll = datagram_poll,
     .ioctl = rakaia_ioctl,
     .listen = sock_no_listen,
     .shutdown = sock_no_shutdown,
     .setsockopt = rakaia_setsockopt,
     .getsockopt = rakaia_getsockopt,
     .sendmsg = rakaia_sendmsg,
     .recvmsg = rakaia_recvmsg,
     .mmap = sock_no_mmap,
     .splice_eof = rakaia_splice_eof,
     .splice_read = rakaia_splice_read,
 };
 
 /* Create proto operation for rakaia sockets */
 static int rakaia_create(struct net *net, struct socket *sock, int protocol,
                        int kern)
 {
     struct rakaia_net *knet = net_generic(net, rakaia_net_id);
     struct sock *sk;
     struct rakaia_sock *ksk;
 
     switch (sock->type) {
     case SOCK_DGRAM:
         sock->ops = &rakaia_dgram_ops;
         break;
     case SOCK_SEQPACKET:
         sock->ops = &rakaia_seqpacket_ops;
         break;
     default:
         return -ESOCKTNOSUPPORT;
     }
 
     if (protocol != RAKAIAPROTO_CONNECTED)
         return -EPROTONOSUPPORT;
 
     /* Create rakaia_sock */
     sk = sk_alloc(net, PF_RAKAIA, GFP_KERNEL, &rakaia_proto, kern);
     if (!sk)
         return -ENOMEM;
     ksk = rakaia_sk(sk);
     /* Add rakaia socket to struct rakaia, and increase the count
      * for #rakaia_sockets.
      */
     spin_lock_bh(&rakaia->lock);
     /* check if the number of rakaia sockets exceeds the allocated number */
     if (rakaia->rakaia_socks_cnt >= NUM_RAKAIA_SOCKETS)
         return -ENOMEM;
     rakaia->rakaia_socks[rakaia->rakaia_socks_cnt] = ksk;
     ksk->index = rakaia->rakaia_socks_cnt;
     skb_queue_head_init(&ksk->rx_q);

     rakaia->rakaia_socks_cnt++;
 
     ksk->knet = knet;
     skb_queue_head_init(&ksk->fast_requests);
 
     pr_info_log(
         "rakaia socket created successfully, with the number of rakaia sockets as %d!\n",
         rakaia->rakaia_socks_cnt);
 
     /* Init RAKAIA socket */
     sock_init_data(sock, sk);
 
     /* For SOCK_SEQPACKET sock type, datagram_poll checks the sk_state, so
      * we set sk_state, otherwise epoll_wait always returns right away with
      * EPOLLHUP
      */
     ksk->sk.sk_state = TCP_ESTABLISHED;
     set_bit(ksk->index, rakaia->waiters_bm);
     list_add_tail(&ksk->ksk_waiting_list, &rakaia->ksk_waiters);
     spin_unlock_bh(&rakaia->lock);
 
     return 0;
 }
 
 /* structure to define a protocol family.
  * For example: the `AF_INET` protocol family includes the IPv4, TCP, and
  * UDP protocols. The `AF_INET6` includes IPv6. It contains i) the protocol
  * family identifier (AF_INET, here is PF_KCM). ii) the pointer to function
  * to create a socket, whic is called when a user application calls
  * `socket()`
  */
 static const struct net_proto_family rakaia_family_ops = {
     .family = PF_RAKAIA,
     .create = rakaia_create,
     .owner = THIS_MODULE,
 };
 
 static __net_init int rakaia_init_net(struct net *net)
 {
     struct rakaia_net *knet = net_generic(net, rakaia_net_id);
 
     INIT_LIST_HEAD_RCU(&knet->psock_list);
     mutex_init(&knet->mutex);
 
     return 0;
 }
 
 static __net_exit void rakaia_exit_net(struct net *net)
 {
     struct rakaia_net *knet = net_generic(net, rakaia_net_id);
 
     /* All RAKAIA sockets should be closed at this point, which should mean
      * that all multiplexors and psocks have been destroyed.
      */
     WARN_ON(!list_empty(&knet->psock_list));
 
     mutex_destroy(&knet->mutex);
 }
 
 static struct pernet_operations rakaia_net_ops = {
     .init = rakaia_init_net,
     .exit = rakaia_exit_net,
     .id = &rakaia_net_id,
     .size = sizeof(struct rakaia_net),
 };
 
 static int __init rakaia_init(void)
 {
     int err = -ENOMEM;
 
     /* strparser part */
     BUILD_BUG_ON(sizeof(struct sk_skb_cb) > sizeof_field(struct sk_buff, cb));
    //  strp_wq = create_singlethread_workqueue("kstrp");
    //  if (unlikely(!strp_wq))
    //      return -ENOMEM;

    strp_wq = alloc_workqueue("kstrp", WQ_UNBOUND |  WQ_HIGHPRI | WQ_MEM_RECLAIM, 0);
    if (unlikely(!strp_wq))
        return -ENOMEM;

    rakaia_psockp = KMEM_CACHE(rakaia_psock, SLAB_HWCACHE_ALIGN);
    if (!rakaia_psockp)
        goto fail;

    rakaia_wq = create_singlethread_workqueue("krakaiad");
    if (!rakaia_wq)
        goto fail;

    err = proto_register(&rakaia_proto, 1);
    if (err)
        goto fail;

    err = register_pernet_device(&rakaia_net_ops);
    if (err)
        goto net_ops_fail;

    err = sock_register(&rakaia_family_ops);
    if (err)
        goto sock_register_fail;

    err = rakaia_proc_init();
    if (err)
        goto proc_init_fail;

    /* Initialize struct rakaia, including its hash table, and linked lists */
    INIT_LIST_HEAD(&rakaia->ksk_waiters);

    bitmap_zero(rakaia->waiters_bm, NUM_RAKAIA_SOCKETS);
    bitmap_zero(rakaia->rx_nonempty_bm, NUM_RAKAIA_SOCKETS);

    spin_lock_init(&rakaia->lock);
    spin_lock_init(&rakaia->waiters_lock);

    for (int i = 0; i < NUM_BUCKETS; i++) {
        struct rakaia_psock_bucket *bucket = &rakaia->rakaia_psock_buckets[i];
        spin_lock_init(&bucket->lock);
        INIT_HLIST_HEAD(&bucket->psocks);
        bucket->id = i;
    }
     skb_queue_head_init(&rakaia->all_msgs);
     pr_info_log("Rakaia is successfully loaded into the kernel!\n");
     pr_info_log("PF_MAX is %d\n", PF_MAX);
 
     return 0;
 
 proc_init_fail:
     sock_unregister(PF_RAKAIA);
 
 sock_register_fail:
     unregister_pernet_device(&rakaia_net_ops);
 
 net_ops_fail:
     proto_unregister(&rakaia_proto);
 
 fail:
     pr_info_log("Rakaia fails to be loaded into the kernel %d\n", err);
     kmem_cache_destroy(rakaia_psockp);
     if (rakaia_wq)
         destroy_workqueue(rakaia_wq);
 
     return err;
 }
 
 static void __exit rakaia_exit(void)
 {
     rakaia_proc_exit();
     sock_unregister(PF_RAKAIA);
     unregister_pernet_device(&rakaia_net_ops);
     proto_unregister(&rakaia_proto);
     destroy_workqueue(rakaia_wq);
 
     kmem_cache_destroy(rakaia_psockp);
 }
 
 module_init(rakaia_init);
 module_exit(rakaia_exit);
 
 MODULE_LICENSE("GPL");
 MODULE_DESCRIPTION("RAKAIA (Kernel Connection Multiplexor & Homa) sockets");
 MODULE_ALIAS_NETPROTO(PF_RAKAIA);
