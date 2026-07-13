#pragma once

#include <linux/kcm.h>
#ifndef AF_RAKAIA
/* From linux/socket.h */
#define AF_RAKAIA 46 /* Kernel Connection Multiplexor*/
#endif

#ifndef RAKAIAPROTO_CONNECTED
/* From linux/kcm.h */
#define RAKAIAPROTO_CONNECTED 0
#endif

#ifndef SIOCRAKAIAATTACH
#define SIOCRAKAIAATTACH (SIOCPROTOPRIVATE + 0)
#endif

#ifndef SIOCRAKAIAPULL
#define SIOCRAKAIAPULL (SIOCPROTOPRIVATE + 3)
#endif

int rakaia_init(void);
int rakaia_attach(int rakaiafd, int csock);
int rakaia_pull(int rakaiafd);
