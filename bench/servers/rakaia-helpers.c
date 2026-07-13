#define _GNU_SOURCE

#include <errno.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>

#include "rakaia-helpers.h"

int rakaia_init(void) {
  int rakaiafd;
  rakaiafd = socket(AF_RAKAIA, SOCK_DGRAM, RAKAIAPROTO_CONNECTED);
  if (rakaiafd == -1)
    perror("rakaia Failure: socket(AF_RAKAIA)");
  return rakaiafd;
}

int rakaia_attach(int rakaiafd, int csock) {
  int error;
  struct kcm_attach attach_info;

  memset(&attach_info, 0, sizeof(attach_info));
  attach_info.fd = csock;
  attach_info.bpf_fd = 0;

  error = ioctl(rakaiafd, SIOCRAKAIAATTACH, &attach_info);
  if (error == -1)
    perror("IOCTL ERROR: ioctl(SIOCRAKAIAATTACH)");
  return error;
}

int rakaia_pull(int rakaiafd) {
  int error;
  error = ioctl(rakaiafd, SIOCRAKAIAPULL);
  if (error == -1)
    perror("IOCTL ERROR: ioctl(SIOCRAKAIAPULL)");
  return error;
}
