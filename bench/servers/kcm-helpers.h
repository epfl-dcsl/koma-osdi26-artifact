#pragma once

#include <linux/kcm.h>
//#ifndef AF_KCM
//[> From linux/socket.h <]
//#define AF_KCM 41 [> Kernel Connection Multiplexor<]
//#endif

//#ifndef KCMPROTO_CONNECTED
//[> From linux/kcm.h <]
//#define KCMPROTO_CONNECTED 0
//#endif

int kcm_init(void);
int kcm_clone(int kcmfd);
int kcm_attach(int kcmfd, int csock, int bpf_prog_fd);
