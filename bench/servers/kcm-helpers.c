#define _GNU_SOURCE

#include <errno.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>

#include "kcm-helpers.h"

int kcm_init(void) {
  int kcmfd;
  kcmfd = socket(AF_KCM, SOCK_DGRAM, KCMPROTO_CONNECTED);
  if (kcmfd == -1)
    perror("KCM Failure: socket(AF_KCM)");
  return kcmfd;
}

int kcm_clone(int kcmfd) {
  int error;
  struct kcm_clone clone_info;

  memset(&clone_info, 0, sizeof(clone_info));
  error = ioctl(kcmfd, SIOCKCMCLONE, &clone_info);
  if (error == -1)
    perror("IOCTL ERROR: ioctl(SIOCKCMCLONE)");

  return clone_info.fd;
}

int kcm_attach(int kcmfd, int csock, int bpf_prog_fd) {
  int error;
  struct kcm_attach attach_info;

  memset(&attach_info, 0, sizeof(attach_info));
  attach_info.fd = csock;
  attach_info.bpf_fd = bpf_prog_fd;

  error = ioctl(kcmfd, SIOCKCMATTACH, &attach_info);
  if (error == -1)
    perror("IOCTL ERROR: ioctl(SIOCKCMATTACH)");
  return error;
}
