// SPDX-License-Identifier: GPL-2.0-only
/*
 * Stream Parser
 *
 * Copyright (c) 2016 Tom Herbert <tom@herbertland.com>
 */

#include <linux/bpf.h>
#include <linux/errno.h>
#include <linux/errqueue.h>
#include <linux/file.h>
#include <linux/in.h>
#include <linux/kernel.h>
#include <linux/export.h>
#include <linux/init.h>
#include <net/tcp.h>
#include <linux/net.h>
#include <linux/netdevice.h>
#include <linux/poll.h>
#include <linux/rculist.h>
#include <linux/skbuff.h>
#include <linux/socket.h>
#include <linux/uaccess.h>
#include <linux/workqueue.h>
#include <net/netns/generic.h>
#include <net/sock.h>
#include "strparser.h"
#include "logging.h"
#include "koma.h"

// Define the HTTP/2 connection preface as a macro
#define HTTP2_PREFACE                                                          \
    "\x50\x52\x49\x20\x2A\x20\x48\x54"                                         \
    "\x54\x50\x2F\x32\x2E\x30\x0D\x0A"                                         \
    "\x0D\x0A\x53\x4D\x0D\x0A\x0D\x0A"

#define HTTP2_PREFACE_LEN 24

struct workqueue_struct *strp_wq;
struct kmem_cache *h2_stream_bufferp;

static inline struct _strp_msg *_strp_msg(struct sk_buff *skb)
{
    return (struct _strp_msg *)((void *)skb->cb +
                                offsetof(struct sk_skb_cb, strp));
}

#include <linux/skbuff.h>
#include <linux/netdevice.h> // for skb->dev (optional)

void print_skb_fraglist(struct sk_buff *head, const char *tag)
{
    unsigned int len;
    int count = 0;

    struct sk_buff *frag_iter;

    // Iterate over the sk_buff queue
    /*pr_info("[%s] Queue contents:\n", tag);*/
    /*pr_info("[%s] Queue length: %u\n", tag, skb_queue_len(list));*/
    /*pr_info("[%s] Dumping skb queue at %p\n", tag, list);*/

    // Walk through each sk_buff in the queue
    skb_walk_frags(head, frag_iter)
    {
        // Get the length of the current sk_buff
        len = frag_iter->len;

        // Print the details of the sk_buff
        // pr_info("[%s] [%d] skb: %p, len: %u, data: %p, tail: %p, end: %p\n",
        //         tag, count, frag_iter, frag_iter->len, frag_iter->data,
        //         skb_tail_pointer(frag_iter), skb_end_pointer(frag_iter));
        count++;
    }
    // pr_info("[%s] End of skb queue\n", tag);
}

/* Lower lock held */
static void strp_abort_strp(struct strparser *strp, int err)
{
    /* Unrecoverable error in receive */

    cancel_delayed_work(&strp->msg_timer_work);

    if (strp->stopped)
        return;

    strp->stopped = 1;

    if (strp->sk) {
        struct sock *sk = strp->sk;

        /* Report an error on the lower socket */
        sk->sk_err = -err;
        sk_error_report(sk);
    }
}

static void strp_start_timer(struct strparser *strp, long timeo)
{
    if (timeo && timeo != LONG_MAX)
        mod_delayed_work(strp_wq, &strp->msg_timer_work, timeo);
}

/* Lower lock held */
static void strp_parser_err(struct strparser *strp, int err,
                            read_descriptor_t *desc)
{
    desc->error = err;
    kfree_skb(strp->skb_head);
    strp->skb_head = NULL;
    strp->cb.abort_parser(strp, err);
}

static inline int strp_peek_len(struct strparser *strp)
{
    if (strp->sk) {
        struct socket *sock = strp->sk->sk_socket;

        return sock->ops->peek_len(sock);
    }

    /* If we don't have an associated socket there's nothing to peek.
     * Return int max to avoid stopping the strparser.
     */

    return INT_MAX;
}

// TODO: get rid of the h2_header, as it should be part of the strp_msg
static inline int parse_msg_http2(struct strparser *strp, struct sk_buff *skb)
{
    u8 buf[9];
    const u8 *hdr;
    int skb_offset = _strp_msg(skb)->strp.offset;
    struct _strp_msg *stm = _strp_msg(skb);

    // TODO (Rui): peek 9bytes from the pointer, without any copying.
    hdr = skb_header_pointer(skb, skb_offset, 9, buf);
    if (!hdr)
        return -EFAULT;

    stm->strp.full_len = ((hdr[0] << 16) | (hdr[1] << 8) | hdr[2]) + 9;
    stm->strp.type = hdr[3];
    stm->strp.flag = hdr[4];
    stm->strp.stream_id =
        ((hdr[5] << 24) | (hdr[6] << 16) | (hdr[7] << 8) | hdr[8]) & 0x7FFFFFFF;

    return 0;
    // /* Start Debug*/
    // static const char *const type_names[] = {
    //     [0x0] = "DATA",         [0x1] = "HEADERS",  [0x2] = "PRIORITY",
    //     [0x3] = "RST_STREAM",   [0x4] = "SETTINGS", [0x5] = "PUSH_PROMISE",
    //     [0x6] = "PING",         [0x7] = "GOAWAY",   [0x8] = "WINDOW_UPDATE",
    //     [0x9] = "CONTINUATION",
    // };

    // const char *type_str =
    //     (header.type < ARRAY_SIZE(type_names) && type_names[header.type]) ?
    //         type_names[header.type] :
    //         "UNKNOWN";

    // pr_info("[strp] HTTP/2 Frame Header:\n");
    // pr_info("[strp]   Length    : %u bytes\n", header.length);
    // pr_info("[strp]   Type      : 0x%02x (%s)\n", header.type, type_str);
    // pr_info("[strp]   Flags     : 0x%02x\n", header.flag);
    // pr_info("[strp]   Stream ID : %u\n", header.stream_id);
    // /* End debug*/
}

void add_skbs_to_head(uint32_t sid, struct sk_buff *head, struct sk_buff *skb)
{
    struct sk_buff *frag;
    struct skb_shared_info *shinfo;
    struct _strp_msg *head_stm, *skb_stm;

    if (!head || !skb)
        return;

    struct sk_buff *last = head;
    while (strp_next(last)) {
        // pr_info("[add_skbs_to_head %d] the strp_next of head %p exists %p\n",
        // sid, head, last);
        last = strp_next(last);
    }
    strp_next(last) = skb;
    strp_next(skb) = NULL;

    // pr_info("[add_skbs_to_head %d] set the strp_next of head %p to %p\n",
    // sid, head, skb);
    /* Update accounting in the head skb */
    head->len += skb->len;
    head->data_len += skb->len;
    head->truesize += skb->truesize;

    /* Clear next pointer to avoid confusion */
    // skb->next = NULL;
}

int create_stream_head(uint32_t stream_id, struct sk_buff *buffer,
                       struct strparser *strp)
{
    struct h2_stream_buffer *stream;

    stream = kmem_cache_zalloc(h2_stream_bufferp, GFP_KERNEL);
    if (!stream)
        return -ENOMEM;

    stream->stream_id = stream_id;

    stream->skb = buffer;
    strp_next(buffer) = NULL;

    // pr_info("[strp] the address of head at init is %p\n", stream->skb);
    // TODO: chained multiple skb add it
    // pr_info("[strp] the address of head after first add is %p\n",
    // stream->skb);
    stream->complete = false;
    hash_add(strp->stream_storage, &stream->hnode, stream_id);
    pr_info_log("[strp] Stream %u created\n", stream_id);
    return 0;
}

struct h2_stream_buffer *stream_lookup(uint32_t stream_id,
                                       struct strparser *strp)
{
    struct h2_stream_buffer *stream;
    hash_for_each_possible(strp->stream_storage, stream, hnode, stream_id)
    {
        if (stream->stream_id == stream_id)
            return stream;
    }

    return NULL;
}

int delete_stream(uint32_t stream_id, struct strparser *strp)
{
    struct h2_stream_buffer *stream = stream_lookup(stream_id, strp);

    if (stream) {
        //! should we purge the skb list?
        /*skb_queue_purge(&stream->skb_list);*/
        hash_del(&stream->hnode);
        kmem_cache_free(h2_stream_bufferp, stream);
        return 0;
    }
    return -ENOENT;
}

int flush_stream(uint32_t stream_id, struct strparser *strp)
{
    //! ounce we flush what should we do with our skb_list ?
    struct h2_stream_buffer *stream = stream_lookup(stream_id, strp);

    if (stream) {
        stream->complete = true;
        // for debug
        if (strp_next(stream->skb) == NULL) {
            pr_info_log("[strp] Stream %u has only one skb %p\n", stream_id,
                        stream->skb);
        }

        // TODO:is this the correct pointer?
        strp->cb.rcv_msg(strp, stream->skb);
        // pr_info("[strp] Stream %u is complete\n", stream_id);
        // TODO: should delete the stream here? this purges the skb list not
        // just removing from head
        // after copying it to userspace!!!
        return delete_stream(stream_id, strp);
    }
    // pr_info("[strp] Stream %u not found for flushing\n", stream_id);
    return -ENOENT;
}

int add_skb_to_stream(uint32_t stream_id, struct sk_buff *skb,
                      struct strparser *strp)
{
    // pr_info("[strp] adding HTTP/2 Frame Header:\n");
    // skb_orphan(skb);
    struct h2_stream_buffer *stream = stream_lookup(stream_id, strp);
    if (stream) {
        pr_info_log(
            "[add_skbs_to_head %d] To call add_skbs_to_head, head %p, head->frag_list %p, skb %p\n",
            stream_id, stream->skb, skb_shinfo(stream->skb)->frag_list, skb);

        add_skbs_to_head(stream_id, stream->skb, skb);
        return 0;
    }
    return create_stream_head(stream_id, skb, strp);
}

/* Create a small linear skb with TCP headroom and copy ctrl bytes in. */
static void send_pingack(struct koma_psock *psock, struct sk_buff *cloned_skb)
{
    const u8 *p;
    u8 buf[8];
    struct _strp_msg *stm = _strp_msg(cloned_skb);
    struct sk_buff *skb;
    struct sock *csk = psock->sk;

    p = skb_header_pointer(cloned_skb, stm->strp.offset + 9, 8, buf);
    if (!p) {
        pr_info("cannot get the payload of PING frame\n");
        kfree_skb(cloned_skb);
    }

    // int mss_now = 0, size_goal;
    u8 *d;

    // TODO (Rui): use MAX_TCP_HEADER for now, since we only use ipv4
    skb = alloc_skb_fclone(MAX_TCP_HEADER + 32, csk->sk_allocation);
    if (unlikely(!skb)) {
        pr_info("alloc_skb fails in make_ctrl_skb\n");
        return;
    }

    skb->truesize = SKB_TRUESIZE(skb_end_offset(skb));
    // if (unlikely(!sk_wmem_schedule(csk, skb->truesize))) {
    //     __kfree_skb(skb);
    //     return NULL;
    // }

    skb_reserve(skb, MAX_TCP_HEADER);
    skb->ip_summed = CHECKSUM_PARTIAL;
    INIT_LIST_HEAD(&skb->tcp_tsorted_anchor);
    TCP_SKB_CB(skb)->seq = 17;
    TCP_SKB_CB(skb)->tcp_flags = TCPHDR_ACK;

    /* (Optional) sanity: ensure we have space for 17 bytes. */
    if (unlikely(skb_tailroom(skb) < 17)) {
        pr_err("[koma] PINGACK: insufficient tailroom %d\n", skb_tailroom(skb));
        __kfree_skb(skb);
        return;
    }

    /* Write HTTP/2 Ping ack directly into skb's linear data */
    d = skb_put(skb, 17);
    d[0] = 0;
    d[1] = 0;
    d[2] = 8; /* Length = 8 */
    d[3] = 0x06; /* Type = PING */
    d[4] = 0x01; /* Flags = ACK */
    d[5] = d[6] = d[7] = d[8] = 0; /* Stream ID = 0 */
    memcpy(d + 9, buf, 8); /* Copy opaque payload */

    spin_lock_bh(&psock->tx_lock);
    sk_wmem_queued_add(csk, skb->truesize);
    sk_mem_charge(csk, skb->truesize);
    skb_queue_tail(&(psock->tx_wait_queue), skb);
    spin_unlock_bh(&psock->tx_lock);

    kfree_skb(cloned_skb);
    koma_schedule_tx(psock);
    // mss_now = tcp_send_mss(csk, &size_goal, MSG_DONTWAIT);
    // try_send_from_psock(0, psock, MSG_DONTWAIT, mss_now, size_goal, 0);
}

/* Create a small linear skb with TCP headroom and copy ctrl bytes in. */
static void send_window_update(struct koma_psock *psock, int inc)
{
    struct sk_buff *skb;
    struct sock *csk = psock->sk;
    // int mss_now = 0, size_goal;
    u8 *d;

    // TODO (Rui): use MAX_TCP_HEADER for now, since we only use ipv4
    skb = alloc_skb_fclone(MAX_TCP_HEADER + 32, csk->sk_allocation);
    if (unlikely(!skb)) {
        pr_info("alloc_skb fails in make_ctrl_skb\n");
        return;
    }

    skb->truesize = SKB_TRUESIZE(skb_end_offset(skb));
    // if (unlikely(!sk_wmem_schedule(csk, skb->truesize))) {
    //     __kfree_skb(skb);
    //     return NULL;
    // }

    skb_reserve(skb, MAX_TCP_HEADER);
    skb->ip_summed = CHECKSUM_PARTIAL;
    INIT_LIST_HEAD(&skb->tcp_tsorted_anchor);
    TCP_SKB_CB(skb)->seq = 13;
    TCP_SKB_CB(skb)->tcp_flags = TCPHDR_ACK;

    /* (Optional) sanity: ensure we have space for 17 bytes. */
    if (unlikely(skb_tailroom(skb) < 13)) {
        pr_err("[koma] PINGACK: insufficient tailroom %d\n", skb_tailroom(skb));
        __kfree_skb(skb);
        return;
    }

    /* Write HTTP/2 Ping ack directly into skb's linear data */
    d = skb_put(skb, 13);
    d[0] = 0;
    d[1] = 0;
    d[2] = 4; /* Length = 4 */
    d[3] = 0x08; /* Type = WINDOW_UPDATE */
    d[4] = 0x00; /* Flags = 0 */
    d[5] = d[6] = d[7] = d[8] = 0; /* Stream ID = 0 */

    /* Payload: Window Size Increment (31 bits) */
    d[9] = (inc >> 24) & 0x7F; /* mask top bit (reserved) */
    d[10] = (inc >> 16) & 0xFF;
    d[11] = (inc >> 8) & 0xFF;
    d[12] = (inc) & 0xFF;

    spin_lock_bh(&psock->tx_lock);
    sk_wmem_queued_add(csk, skb->truesize);
    sk_mem_charge(csk, skb->truesize);
    skb_queue_tail(&(psock->tx_wait_queue), skb);
    spin_unlock_bh(&psock->tx_lock);

    koma_schedule_tx(psock);
    // mss_now = tcp_send_mss(csk, &size_goal, MSG_DONTWAIT);
    // try_send_from_psock(0, psock, MSG_DONTWAIT, mss_now, size_goal, 0);
}

/* Create a small linear skb with TCP headroom and copy ctrl bytes in. */
static void send_settings_ack(struct koma_psock *psock)
{
    struct sk_buff *skb;
    struct sock *csk = psock->sk;
    u8 *d;

    skb = alloc_skb_fclone(MAX_TCP_HEADER + 32, csk->sk_allocation);
    if (unlikely(!skb)) {
        pr_info("alloc_skb fails in send_settings_ack\n");
        return;
    }

    skb->truesize = SKB_TRUESIZE(skb_end_offset(skb));
    skb_reserve(skb, MAX_TCP_HEADER);
    skb->ip_summed = CHECKSUM_PARTIAL;
    INIT_LIST_HEAD(&skb->tcp_tsorted_anchor);
    TCP_SKB_CB(skb)->seq = 9;
    TCP_SKB_CB(skb)->tcp_flags = TCPHDR_ACK;

    if (unlikely(skb_tailroom(skb) < 9)) {
        pr_err("[koma] SETTINGS ACK: insufficient tailroom %d\n",
               skb_tailroom(skb));
        __kfree_skb(skb);
        return;
    }

    d = skb_put(skb, 9);
    d[0] = 0;
    d[1] = 0;
    d[2] = 0; /* Length = 0 */
    d[3] = FRAME_TYPE_SETTINGS;
    d[4] = FLAG_ACK;
    d[5] = d[6] = d[7] = d[8] = 0; /* Stream ID = 0 */

    spin_lock_bh(&psock->tx_lock);
    sk_wmem_queued_add(csk, skb->truesize);
    sk_mem_charge(csk, skb->truesize);
    skb_queue_tail(&(psock->tx_wait_queue), skb);
    spin_unlock_bh(&psock->tx_lock);

    koma_schedule_tx(psock);
}

static void process_incoming_window_update(struct koma_psock *psock,
                                           struct sk_buff *cloned_skb)
{
    u8 buf[4];
    const u8 *p;
    int inc;
    bool should_schedule = false;

    struct _strp_msg *stm = _strp_msg(cloned_skb);
    if (stm->strp.stream_id != 0)
        goto err;
    // TODO (Rui): stream level FC later

    // read window_update frame
    p = skb_header_pointer(cloned_skb, stm->strp.offset + 9, 4, buf);
    if (!p)
        goto err;

    // Decode 31-bit increment (MSB reserved)
    inc =
        ((u32)p[0] << 24) | ((u32)p[1] << 16) | ((u32)p[2] << 8) | ((u32)p[3]);
    inc &= 0x7fffffff;
    if (inc == 0)
        goto err;

    spin_lock_bh(&psock->tx_lock);
    pr_info_log("send quota increased by %d", inc);
    psock->send_quota += inc;
    if (psock->flow_stalled && psock->send_quota > 0) {
        psock->flow_stalled = false;
        should_schedule = true;
    }
    spin_unlock_bh(&psock->tx_lock);
    if (should_schedule)
        koma_schedule_tx(psock);
err:
    kfree_skb(cloned_skb);
    return;
}

// we handle the skb that has the h2 frame, and we need it to be cloned
// beforehand
int handle_h2_frame(struct sk_buff *cloned_skb, struct strparser *strp)
{
    struct h2_stream_buffer *stream;
    struct _strp_msg *stm = _strp_msg(cloned_skb);
    uint32_t stream_id = stm->strp.stream_id;
    struct koma_psock *psock;

    // TODO: skip bucket if not data or headers and continuation

    psock = strp->sk->sk_user_data;

    // TODO: this is only for debugging
    stream = stream_lookup(stream_id, strp);
    if (stream) {
        // pr_info("[strp] Stream %u found; skb is %p, frag_list is %p\n",
        // stream_id, cloned_skb, skb_shinfo(cloned_skb)->frag_list);
        print_skb_fraglist(stream->skb, "stream");
    } else {
        // pr_info("[strp] Stream %u not found; skb is %p, frag_list is %p\n",
        // stream_id, cloned_skb, skb_shinfo(cloned_skb)->frag_list);
    }

    // concatenate the type and flags to a single var
    switch (stm->strp.type) {
    case FRAME_TYPE_RST_STREAM:
    case FRAME_TYPE_DATA:
        // flow control bookkeeping
        psock->unacked += stm->strp.full_len - 9;
        if (psock->unacked >= psock->recv_conn_window / 4) {
            send_window_update(psock, psock->unacked);
            psock->unacked = 0;
        }
    case FRAME_TYPE_HEADERS:
    case FRAME_TYPE_CONTINUATION: {
        pr_info_log("[strp] stream %d type %d arrive, len %d\n", stream_id,
                    stm->strp.type, stm->strp.full_len);
        int res = add_skb_to_stream(stream_id, cloned_skb, strp);
        if (res) {
            // pr_info("[strp] Failed to add skb to stream %u\n", stream_id);
            return res;
        }
        if ((stm->strp.flag & FLAG_END_STREAM) ||
            stm->strp.type == FRAME_TYPE_RST_STREAM) {
            res = flush_stream(stream_id, strp);
            if (res) {
                // pr_info("[strp] Failed to flush stream %u\n", stream_id);
                return res;
            }
            // pr_info("[strp] End of stream flag set on stream %u\n",
            // stream_id);
        }
        break;
    }
    case FRAME_TYPE_PING:
        // receives a ping frame, ack directly in the kernel
        send_pingack(psock, cloned_skb);
        break;
    case FRAME_TYPE_WINDOW_UPDATE:
        // incoming window update:
        process_incoming_window_update(psock, cloned_skb);
        break;
    case FRAME_TYPE_SETTINGS:
        if (stream_id == 0 && !(stm->strp.flag & FLAG_ACK))
            send_settings_ack(psock);
        kfree_skb(cloned_skb);
        break;
    default:
        // Not a data-carrying frame: flush immediately
        pr_info_log(
            "[strp] Non-data frame type %d on stream %u; flushing immediately\n",
            stm->strp.type, stream_id);
        kfree_skb(cloned_skb);
        /*strp->cb.rcv_msg(strp, cloned_skb);*/
        break;
    }

    return 0;
}

/* Lower socket lock held */
static int __strp_recv(read_descriptor_t *desc, struct sk_buff *orig_skb,
                       unsigned int orig_offset, size_t orig_len,
                       size_t max_msg_size, long timeo)
{
    struct strparser *strp = (struct strparser *)desc->arg.data;
    struct _strp_msg *stm;
    struct sk_buff *head, *skb;
    size_t eaten = 0, cand_len;
    ssize_t extra;
    int err;
    bool cloned_orig = false;

    if (strp->paused) {
        return 0;
    }

    head = strp->skb_head;
    if (head) {
        /* Message already in progress */
        // pr_info("message already in progress!!!\n");
        if (unlikely(orig_offset)) {
            /* Getting data with a non-zero offset when a message is
             * in progress is not expected. If it does happen, we
             * need to clone and pull since we can't deal with
             * offsets in the skbs for a message expect in the head.
             */
            orig_skb = skb_clone(orig_skb, GFP_ATOMIC);
            if (!orig_skb) {
                STRP_STATS_INCR(strp->stats.mem_fail);
                desc->error = -ENOMEM;
                return 0;
            }
            if (!pskb_pull(orig_skb, orig_offset)) {
                STRP_STATS_INCR(strp->stats.mem_fail);
                kfree_skb(orig_skb);
                desc->error = -ENOMEM;
                return 0;
            }
            cloned_orig = true;
            orig_offset = 0;
        }

        if (!strp->skb_nextp) {
            /* We are going to append to the frags_list of head.
             * Need to unshare the frag_list.
             */
            err = skb_unclone(head, GFP_ATOMIC);
            if (err) {
                STRP_STATS_INCR(strp->stats.mem_fail);
                desc->error = err;
                return 0;
            }

            if (unlikely(skb_shinfo(head)->frag_list)) {
                /* We can't append to an sk_buff that already
                 * has a frag_list. We create a new head, point
                 * the frag_list of that to the old head, and
                 * then are able to use the old head->next for
                 * appending to the message.
                 */
                if (WARN_ON(head->next)) {
                    desc->error = -EINVAL;
                    return 0;
                }

                skb = alloc_skb_for_msg(head);
                if (!skb) {
                    STRP_STATS_INCR(strp->stats.mem_fail);
                    desc->error = -ENOMEM;
                    return 0;
                }

                // pr_info("set strp->skb_nextp!!!\n");
                strp->skb_nextp = &head->next;
                strp->skb_head = skb;
                head = skb;
            } else {
                strp->skb_nextp = &skb_shinfo(head)->frag_list;
            }
        }
    }

    while (eaten < orig_len) {
        /* Always clone since we will consume something */
        skb = skb_clone(orig_skb, GFP_ATOMIC);
        if (!skb) {
            STRP_STATS_INCR(strp->stats.mem_fail);
            desc->error = -ENOMEM;
            break;
        }

        cand_len = orig_len - eaten;

        head = strp->skb_head;
        if (!head) {
            head = skb;
            strp->skb_head = head;
            /* Will set skb_nextp on next packet if needed */
            strp->skb_nextp = NULL;
            stm = _strp_msg(head);
            memset(stm, 0, sizeof(*stm));
            stm->strp.offset = orig_offset + eaten;
        } else {
            /* Unclone if we are appending to an skb that we
             * already share a frag_list with.
             */
            // pr_info("append to an skb that we already share a frag_list
            // with!!!\n");
            if (skb_has_frag_list(skb)) {
                err = skb_unclone(skb, GFP_ATOMIC);
                if (err) {
                    STRP_STATS_INCR(strp->stats.mem_fail);
                    desc->error = err;
                    break;
                }
            }

            stm = _strp_msg(head);
            *strp->skb_nextp = skb;
            strp->skb_nextp = &skb->next;
            head->data_len += skb->len;
            head->len += skb->len;
            head->truesize += skb->truesize;
        }

        if (!stm->strp.full_len) {
            ssize_t len;
            if (!parse_msg_http2(strp, head)) {
                len = stm->strp.full_len;
            } else {
                len = 0;
            }

            pr_info_log("[strp] Message header parsed, stream %u\n",
                        stm->strp.stream_id);
            // else {
            //     len = (*strp->cb.parse_msg)(strp, head);
            // }
            // pr_info(
            //     "[strp] the address of head is %p, eaten bytes is %d, length
            //     of msg is %d, full length is %d\n", head, stm->strp.offset,
            //     len, orig_len);

            if (!len) {
                /* Need more header to determine length */
                if (!stm->accum_len) {
                    /* Start RX timer for new message */
                    strp_start_timer(strp, timeo);
                }
                stm->accum_len += cand_len;
                eaten += cand_len;
                STRP_STATS_INCR(strp->stats.need_more_hdr);
                WARN_ON(eaten != orig_len);
                break;
            } else if (len < 0) {
                if (len == -ESTRPIPE && stm->accum_len) {
                    len = -ENODATA;
                    strp->unrecov_intr = 1;
                } else {
                    strp->interrupted = 1;
                }
                strp_parser_err(strp, len, desc);
                break;
            } else if (len > max_msg_size) {
                /* Message length exceeds maximum allowed */
                STRP_STATS_INCR(strp->stats.msg_too_big);
                strp_parser_err(strp, -EMSGSIZE, desc);
                break;
            } else if (len <=
                       (ssize_t)head->len - skb->len - stm->strp.offset) {
                /* Length must be into new skb (and also
                 * greater than zero)
                 */
                STRP_STATS_INCR(strp->stats.bad_hdr_len);
                strp_parser_err(strp, -EPROTO, desc);
                break;
            }

            // for (int i = 0; i < 48; i++) {
            // Print each byte in hexadecimal format
            //     pr_info("[strp] Byte %d: 0x%02x\n", i, head->cb[i] & 0xFF);
            // }

        } else {
            pr_info_log(
                "[strp] Message %u len decided already; full %d, accumu %d\n",
                stm->strp.stream_id, stm->strp.full_len, stm->accum_len);
            pr_info_log(
                "[strp] Message %u head_skb is %p, cur_skb is %p, head->frag_list is %p\n",
                stm->strp.stream_id, head, skb, skb_shinfo(head)->frag_list);
        }

        extra = (ssize_t)(stm->accum_len + cand_len) - stm->strp.full_len;

        if (extra < 0) {
            pr_info_log("[strp] Message not completed, stream %u\n",
                        stm->strp.stream_id);
            /* Message not complete yet. */
            if (stm->strp.full_len - stm->accum_len > strp_peek_len(strp)) {
                /* Don't have the whole message in the socket
                 * buffer. Set strp->need_bytes to wait for
                 * the rest of the message. Also, set "early
                 * eaten" since we've already buffered the skb
                 * but don't consume yet per strp_read_sock.
                 */

                if (!stm->accum_len) {
                    /* Start RX timer for new message */
                    strp_start_timer(strp, timeo);
                }

                stm->accum_len += cand_len;
                eaten += cand_len;
                strp->need_bytes = stm->strp.full_len - stm->accum_len;
                STRP_STATS_ADD(strp->stats.bytes, cand_len);
                desc->count = 0; /* Stop reading socket */
                break;
            }
            stm->accum_len += cand_len;
            eaten += cand_len;
            WARN_ON(eaten != orig_len);
            break;
        }

        /* Positive extra indicates more bytes than needed for the
         * message
         */

        WARN_ON(extra > cand_len);

        eaten += (cand_len - extra);

        /* Hurray, we have a new message! */
        cancel_delayed_work(&strp->msg_timer_work);
        strp->skb_head = NULL;
        strp->need_bytes = 0;
        STRP_STATS_INCR(strp->stats.msgs);

        pr_info_log("[strp] Message completed, stream %u\n",
                    stm->strp.stream_id);

        /*if (stm->strp.stream_id != 0 || stm->strp.type == FRAME_TYPE_PING ||*/
        /*stm->strp.type == FRAME_TYPE_WINDOW_UPDATE) {*/
        // if (stm->strp.stream_id != 0 ) {
        handle_h2_frame(head, strp);
        /*} else {*/
        /*[> Give skb to upper layer <]*/
        /*strp_next(head) = NULL;*/
        /*pr_info_log(*/
        /*"[strp] give skb to upper layer directly %p, strp_next %p\n, stream
         * %u",*/
        /*head, strp_next(head), stm->strp.stream_id);*/
        /*strp->cb.rcv_msg(strp, head);*/
        /*}*/

        if (unlikely(strp->paused)) {
            /* Upper layer paused strp */
            break;
        }
    }

    if (cloned_orig)
        kfree_skb(orig_skb);

    STRP_STATS_ADD(strp->stats.bytes, eaten);

    // pr_info("[strp] returning\n");

    return eaten;
}

int strp_process(struct strparser *strp, struct sk_buff *orig_skb,
                 unsigned int orig_offset, size_t orig_len, size_t max_msg_size,
                 long timeo)
{
    read_descriptor_t desc; /* Dummy arg to strp_recv */

    desc.arg.data = strp;

    return __strp_recv(&desc, orig_skb, orig_offset, orig_len, max_msg_size,
                       timeo);
}

static int strp_recv(read_descriptor_t *desc, struct sk_buff *orig_skb,
                     unsigned int orig_offset, size_t orig_len)
{
    struct strparser *strp = (struct strparser *)desc->arg.data;

    return __strp_recv(desc, orig_skb, orig_offset, orig_len,
                       strp->sk->sk_rcvbuf, strp->sk->sk_rcvtimeo);
}

static int default_read_sock_done(struct strparser *strp, int err)
{
    return err;
}

/* Called with lock held on lower socket */
static int strp_read_sock(struct strparser *strp)
{
    struct socket *sock = strp->sk->sk_socket;
    read_descriptor_t desc;

    if (unlikely(!sock || !sock->ops || !sock->ops->read_sock))
        return -EBUSY;

    desc.arg.data = strp;
    desc.error = 0;
    desc.count = 1; /* give more than one skb per call */

    /* sk should be locked here, so okay to do read_sock */
    sock->ops->read_sock(strp->sk, &desc, strp_recv);

    desc.error = strp->cb.read_sock_done(strp, desc.error);

    return desc.error;
}

/* Lower sock lock held */
void strp_data_ready(struct strparser *strp)
{
    if (unlikely(strp->stopped) || strp->paused)
        return;

    /* This check is needed to synchronize with do_strp_work.
     * do_strp_work acquires a process lock (lock_sock) whereas
     * the lock held here is bh_lock_sock. The two locks can be
     * held by different threads at the same time, but bh_lock_sock
     * allows a thread in BH context to safely check if the process
     * lock is held. In this case, if the lock is held, queue work.
     */
    if (sock_owned_by_user_nocheck(strp->sk)) {
        queue_work(strp_wq, &strp->work);
        return;
    }

    if (strp->need_bytes) {
        if (strp_peek_len(strp) < strp->need_bytes)
            return;
    }

    if (strp_read_sock(strp) == -ENOMEM)
        queue_work(strp_wq, &strp->work);
}

static void do_strp_work(struct strparser *strp)
{
    /* We need the read lock to synchronize with strp_data_ready. We
     * need the socket lock for calling strp_read_sock.
     */
    strp->cb.lock(strp);

    if (unlikely(strp->stopped))
        goto out;

    if (strp->paused)
        goto out;

    if (strp_read_sock(strp) == -ENOMEM)
        queue_work(strp_wq, &strp->work);

out:
    strp->cb.unlock(strp);
}

static void strp_work(struct work_struct *w)
{
    do_strp_work(container_of(w, struct strparser, work));
}

static void strp_msg_timeout(struct work_struct *w)
{
    struct strparser *strp =
        container_of(w, struct strparser, msg_timer_work.work);

    /* Message assembly timed out */
    STRP_STATS_INCR(strp->stats.msg_timeouts);
    strp->cb.lock(strp);
    strp->cb.abort_parser(strp, -ETIMEDOUT);
    strp->cb.unlock(strp);
}

static void strp_sock_lock(struct strparser *strp)
{
    lock_sock(strp->sk);
}

static void strp_sock_unlock(struct strparser *strp)
{
    release_sock(strp->sk);
}

int strp_init(struct strparser *strp, struct sock *sk,
              const struct strp_callbacks *cb)
{
    if (!cb || !cb->rcv_msg || !cb->parse_msg)
        return -EINVAL;

    /* The sk (sock) arg determines the mode of the stream parser.
     *
     * If the sock is set then the strparser is in receive callback mode.
     * The upper layer calls strp_data_ready to kick receive processing
     * and strparser calls the read_sock function on the socket to
     * get packets.
     *
     * If the sock is not set then the strparser is in general mode.
     * The upper layer calls strp_process for each skb to be parsed.
     */

    if (!sk) {
        if (!cb->lock || !cb->unlock)
            return -EINVAL;
    }

    memset(strp, 0, sizeof(*strp));

    strp->sk = sk;

    strp->cb.lock = cb->lock ?: strp_sock_lock;
    strp->cb.unlock = cb->unlock ?: strp_sock_unlock;
    strp->cb.rcv_msg = cb->rcv_msg;
    strp->cb.parse_msg = cb->parse_msg;
    strp->cb.read_sock_done = cb->read_sock_done ?: default_read_sock_done;
    strp->cb.abort_parser = cb->abort_parser ?: strp_abort_strp;

    INIT_DELAYED_WORK(&strp->msg_timer_work, strp_msg_timeout);
    INIT_WORK(&strp->work, strp_work);
    strp->http2 = 1; // default to http2 for koma
    hash_init(strp->stream_storage);
    return 0;
}

/* Sock process lock held (lock_sock) */
void __strp_unpause(struct strparser *strp)
{
    strp->paused = 0;

    if (strp->need_bytes) {
        if (strp_peek_len(strp) < strp->need_bytes)
            return;
    }
    strp_read_sock(strp);
}

void strp_unpause(struct strparser *strp)
{
    strp->paused = 0;

    /* Sync setting paused with RX work */
    smp_mb();

    queue_work(strp_wq, &strp->work);
}

void strp_cleanup_streams(struct strparser *strp)
{
    struct h2_stream_buffer *stream;
    struct hlist_node *tmp;
    struct sk_buff *iter, *next;
    int bkt;

    hash_for_each_safe(strp->stream_storage, bkt, tmp, stream, hnode)
    {
        hash_del(&stream->hnode);
        /* free any skb chains or payload inside the stream if needed */
        if (stream->skb) {
            iter = strp_next(stream->skb);
            while (iter) {
                next = strp_next(iter);
                kfree_skb(iter);
                iter = next;
            }
            kfree_skb(stream->skb);
        }
        /* finally free the stream structure itself */
        kmem_cache_free(h2_stream_bufferp, stream);
    }
}

/* strp must already be stopped so that strp_recv will no longer be called.
 * Note that strp_done is not called with the lower socket held.
 */
void strp_done(struct strparser *strp)
{
    WARN_ON(!strp->stopped);

    cancel_delayed_work_sync(&strp->msg_timer_work);
    cancel_work_sync(&strp->work);

    if (strp->skb_head) {
        kfree_skb(strp->skb_head);
        strp->skb_head = NULL;
    }
    strp_cleanup_streams(strp);
}

void strp_stop(struct strparser *strp)
{
    strp->stopped = 1;
}

void strp_check_rcv(struct strparser *strp)
{
    queue_work(strp_wq, &strp->work);
}
