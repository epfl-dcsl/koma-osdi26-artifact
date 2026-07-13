// SPDX-License-Identifier: GPL-2.0-only
/*
 * rakaia
 *
 * Copyright (c) 2024 Rui Yang <rui.yang@epfl.ch>
 */

#include <linux/socket.h>
int tcp_send(struct sock *sk, int mflags);
void tcp_tx_timestamp(struct sock *sk, u16 tsflags);
void tcp_push(struct sock *sk, int flags, int mss_now, int nonagle,
              int size_goal);