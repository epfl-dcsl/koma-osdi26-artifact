#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>

/* libbcc */
#include <arpa/inet.h>

#include "common.h"
#include "config.h"
#include "koma-helpers.h"
#include "memcached.h"

#define BUFSIZE 2048
#define BACKLOG 8192
#define MAX_THREADS 64

static int epollfd[MAX_THREADS];
__thread int thread_no;
int nr_cpu;

struct koma_msg_ctx {
  struct {
    union {
      binary_header_t bin_header;
      bid_header_t id_header;
    } header;
    unsigned char data[BUFSIZE];
  } request;
  union {
    binary_header_t bin_header;
    bid_header_t id_header;
  } response;
  struct iovec rx_iov;
  struct iovec tx_iov;
  struct msghdr rx_msg;
  struct msghdr tx_msg;
};

const char *service_proto;
void (*protocol_dm)(int fd);
static __thread struct koma_msg_ctx msg_ctx;

// Signal handler function
void signal_handler(int signal) {
  printf("Caught signal %d\n", signal);
  exit(0);
}

static void epoll_ctl_add(int fd, int thread_num) {
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLERR;
#if CONFIG_USE_EPOLLEXCLUSIVE
  ev.events |= EPOLLEXCLUSIVE;
#endif
  ev.data.fd = fd;
  /*ev.data.ptr = arg;*/
  if (epoll_ctl(epollfd[thread_num], EPOLL_CTL_ADD, fd, &ev) == -1) {
    perror("epoll_ctl: EPOLL_CTL_ADD");
    exit(EXIT_FAILURE);
  }
}

static void setnonblocking(int fd) {
  int flags;
  flags = fcntl(fd, F_GETFL, 0);
  assert(flags >= 0);
  flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  assert(flags >= 0);
}

static int handle_ret(int fd, ssize_t ret, int line) {
  // printf("KOMA recvmsg does not return 0, handle ret %zd !\n", ret);
  if (ret == 0) {
    close(fd);
    /* TODO: should also free conn */
    return 1;
  } else if (ret == -1) {
    switch (errno) {
    case EAGAIN:
      // printf("EAGAIN error at line %d\n", errno, line);
    case EBADF:
      return 1;
    case EPIPE:
    case ECONNRESET:
      close(fd);
      /* TODO: should also free conn */
      return 1;
    default:
      fprintf(stderr, "Unexpected errno %d at line %d\n", errno, line);
    }
  }
  return 0;
}

static void init_msg_ctx(size_t header_len) {
  memset(&msg_ctx, 0, sizeof(msg_ctx));

  msg_ctx.rx_iov.iov_base = &msg_ctx.request;
  msg_ctx.rx_iov.iov_len = sizeof(msg_ctx.request);
  msg_ctx.rx_msg.msg_iov = &msg_ctx.rx_iov;
  msg_ctx.rx_msg.msg_iovlen = 1;

  msg_ctx.tx_iov.iov_base = &msg_ctx.response;
  msg_ctx.tx_iov.iov_len = header_len;
  msg_ctx.tx_msg.msg_iov = &msg_ctx.tx_iov;
  msg_ctx.tx_msg.msg_iovlen = 1;
}

static void init_memcached_id_ctx(void) {
  init_msg_ctx(sizeof(bid_header_t));
  msg_ctx.response.id_header.magic = 0x81;
  msg_ctx.response.id_header.status = __builtin_bswap16(1);
  msg_ctx.response.id_header.body_len = 0;
}

static void init_memcached_bin_ctx(void) {
  init_msg_ctx(sizeof(binary_header_t));
  msg_ctx.response.bin_header.magic = 0x81;
  msg_ctx.response.bin_header.status = __builtin_bswap16(1);
  msg_ctx.response.bin_header.body_len = 0;
}

static size_t memcached_request_len(size_t header_len, uint8_t opcode,
                                    uint16_t key_len, uint8_t extra_len,
                                    uint32_t body_len) {
  if (opcode == CMD_SET)
    return header_len + __builtin_bswap32(body_len);
  return header_len + __builtin_bswap16(key_len) + extra_len;
}

static void memcached_id_drive_machine(int fd) {
  ssize_t ret;
  bid_header_t *request = &msg_ctx.request.header.id_header;
  bid_header_t *response = &msg_ctx.response.id_header;

  msg_ctx.rx_msg.msg_flags = 0;
  ret = recvmsg(fd, &msg_ctx.rx_msg, 0);
  if (handle_ret(fd, ret, __LINE__))
    return;

  assert(ret >= (ssize_t)sizeof(*request));
  if (request->magic != 0x80)
    printf("Received message %d\n", request->magic);

  // start with driver drive_machine
  assert(request->magic == 0x80);
  assert(request->opcode == CMD_SET ||
         request->opcode == CMD_GET ||
         request->opcode == CMD_GETK);
  assert(ret == (ssize_t)memcached_request_len(sizeof(*request),
                                               request->opcode,
                                               request->key_len,
                                               request->extra_len,
                                               request->body_len));
  process_request();

  response->id = request->id;

  msg_ctx.tx_msg.msg_flags = 0;
  ret = sendmsg(fd, &msg_ctx.tx_msg, 0);
  if (handle_ret(fd, ret, __LINE__))
    return;
}

static void memcached_bin_drive_machine(int fd) {
  ssize_t ret;
  binary_header_t *request = &msg_ctx.request.header.bin_header;

  msg_ctx.rx_msg.msg_flags = 0;
  ret = recvmsg(fd, &msg_ctx.rx_msg, 0);
  if (handle_ret(fd, ret, __LINE__))
    return;

  assert(ret >= (ssize_t)sizeof(*request));

  if (request->magic != 0x80)
    printf("Received message %d\n", request->magic);

  // start with driver drive_machine
  assert(request->magic == 0x80);
  assert(request->opcode == CMD_SET ||
         request->opcode == CMD_GET ||
         request->opcode == CMD_GETK);
  assert(ret == (ssize_t)memcached_request_len(sizeof(*request),
                                               request->opcode,
                                               request->key_len,
                                               request->extra_len,
                                               request->body_len));
  process_request();

  msg_ctx.tx_msg.msg_flags = 0;
  ret = sendmsg(fd, &msg_ctx.tx_msg, 0);
  if (handle_ret(fd, ret, __LINE__))
    return;
}

static void *tcp_thread_main(void *arg) {
  struct sockaddr_in sin;
  int sock;
  int one;
  int ret, i, nfds, conn_sock;
  int koma_pull_ret;
  int koma_fd0, koma_fd_cur;
  struct epoll_event ev, events[CONFIG_MAX_EVENTS];
  /*struct conn *conn;*/

  if (strcmp(service_proto, "memcache-bin") == 0) {
    protocol_dm = &memcached_bin_drive_machine;
    init_memcached_bin_ctx();
  } else if (strcmp(service_proto, "memcache-id") == 0) {
    protocol_dm = &memcached_id_drive_machine;
    init_memcached_id_ctx();
  } else {
    printf("Unknown protocol: %s\n", service_proto);
    exit(1);
  }

  sock = socket(AF_INET, SOCK_STREAM, 0);
  if (!sock) {
    perror("socket");
    exit(1);
  }
  setnonblocking(sock);

  one = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, (void *)&one, sizeof(one))) {
    perror("setsockopt(SO_REUSEPORT)");
    exit(1);
  }

  one = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void *)&one, sizeof(one))) {
    perror("setsockopt(SO_REUSEADDR)");
    exit(1);
  }
  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = htonl(0);
  sin.sin_port = htons(40001);

  if (bind(sock, (struct sockaddr *)&sin, sizeof(sin))) {
    perror("bind");
    exit(1);
  }

  if (listen(sock, BACKLOG)) {
    perror("listen");
    exit(1);
  }

  thread_no = (long)arg;
  init_thread();

  epollfd[thread_no] = epoll_create1(0);
  ev.events = EPOLLIN;
  ev.data.u32 = 0;
  ret = epoll_ctl(epollfd[thread_no], EPOLL_CTL_ADD, sock, &ev);
  assert(!ret);

  // The ONLY koma socket.
  koma_fd0 = koma_init();
  epoll_ctl_add(koma_fd0, thread_no);

  koma_pull_ret = koma_pull(koma_fd0);
  while (1) {
    nfds = epoll_wait(epollfd[thread_no], events, CONFIG_MAX_EVENTS, -1);
    assert(nfds > 0);

    for (i = 0; i < nfds; i++) {
      if (events[i].data.u32 == 0) {
        conn_sock = accept(sock, NULL, NULL);
        if (conn_sock == -1) {
          perror("accept");
          exit(EXIT_FAILURE);
        }
        // not sure if the below is needed or should do the same for kcm socket,
        // put here for now
        setnonblocking(conn_sock);
        if (setsockopt(conn_sock, IPPROTO_TCP, TCP_NODELAY, (void *)&one,
                       sizeof(one))) {
          perror("setsockopt(TCP_NODELAY)");
          exit(1);
        }
        // attach the tcp sock to the koma system.
        koma_attach(koma_fd0, conn_sock);
      } else {
        if (events[i].events & (EPOLLHUP | EPOLLERR)) {
          close(events[i].data.fd);
        } else {
          (*protocol_dm)(events[i].data.fd);
        }
      }
    }
  }
}

/* does not really pin threads to different cores individually, but rather pin
 * some threads to some cores which is specified in the numactl in our code
 */
void init_koma(void) {
  srand48(mytime());

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  nr_cpu = CPU_COUNT(&cpuset);
}

void start_koma_server(const char *str) {
  signal(SIGPIPE, signal_handler);
  signal(SIGTERM, signal_handler);
  /*printf("starting to state_header\n");*/
  int i;
  pthread_t tid;
  service_proto = str;
  for (i = 1; i < nr_cpu; i++) {
    if (pthread_create(&tid, NULL, tcp_thread_main, (void *)(long)i)) {
      fprintf(stderr, "failed to spawn thread %d\n", i);
      exit(-1);
    }
  }

  tcp_thread_main(0);
}
