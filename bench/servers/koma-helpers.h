#pragma once

#include <linux/kcm.h>
#ifndef AF_KOMA
/* From linux/socket.h */
#define AF_KOMA 46 /* Kernel Connection Multiplexor*/
#endif

#ifndef KOMAPROTO_CONNECTED
/* From linux/kcm.h */
#define KOMAPROTO_CONNECTED 0
#endif

#ifndef SIOCKOMAATTACH
#define SIOCKOMAATTACH (SIOCPROTOPRIVATE + 0)
#endif

#ifndef SIOCKOMAPULL
#define SIOCKOMAPULL (SIOCPROTOPRIVATE + 3)
#endif

int koma_init(void);
int koma_attach(int komafd, int csock);
int koma_pull(int komafd);
