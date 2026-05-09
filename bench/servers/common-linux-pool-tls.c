#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <semaphore.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "common.h"
#include "memcached.h"

#define BUFSIZE 2048
#define BACKLOG 8192
#define MAX_THREADS 64
#define MAX_EVENTS 64
#define EPOLL_TOKEN_LISTENER 0ULL

struct conn {
  /* IO-thread-only — protected by io_owned, never written by worker threads */
  int fd;
  int buf_head;
  int buf_tail;
  bool retired;
  enum conn_state state;
  bid_header_t header;
  size_t pending_bytes;
  struct pool_completion *resp_head;
  struct pool_completion *resp_tail;
  struct conn *retired_next;

  /* Cross-thread — accessed by both IO and worker threads.
   * Aligned to a dedicated cache line to prevent false sharing. */
  __attribute__((aligned(64)))
  int refcnt;
  int io_owned;
  int io_pending;
  int closed;
  int epollout_armed;
  struct pool_completion *inbox;
  char _pad_cross[32]; /* push buf to next cache line */

  unsigned char buf[BUFSIZE];
  SSL *ssl;
};

struct pool_job {
  struct conn *conn;
  uint32_t req_id;
};

struct pool_completion {
  struct pool_completion *resp_next;
  struct conn *conn;
  uint32_t req_id;
  int worker_id;
  bid_header_t response;
  size_t sent;
};

/*
 * Vyukov MPMC ring — one CAS per push, one CAS per pop.
 * Large enough to absorb short overload bursts without pinning IO threads
 * in the producer wait loop.
 */
#define RING_CAPACITY 65536  /* power of 2 */

struct job_ring_cell {
  uint64_t seq;   /* accessed via __atomic_* builtins */
  struct pool_job job;
};

struct job_ring {
  uint64_t head;          /* accessed via __atomic_* builtins */
  char     _pad0[56];     /* head on its own cache line */
  uint64_t tail;          /* accessed via __atomic_* builtins */
  char     _pad1[56];     /* tail on its own cache line */
  sem_t    sem;           /* counts enqueued items; blocks idle workers */
  uint32_t mask;
  struct job_ring_cell cells[RING_CAPACITY];
};

struct io_thread_ctx {
  int id;
  pthread_t tid;
  struct conn *retired_head;
};

static struct io_thread_ctx io_threads[MAX_THREADS];
static pthread_t worker_tids[MAX_THREADS];
static int worker_ids[MAX_THREADS];
static struct job_ring job_ring;
static struct pool_completion *completion_cache[MAX_THREADS];
static int shared_epollfd;
static int listenfd;
static int io_thread_count;
static int worker_thread_count;
static SSL_CTX *ssl_ctx;
const char *service_proto;
__thread int thread_no;
static __thread struct pool_completion *local_completion_cache;
int nr_cpu;

static void init_openssl(void) {
  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();
}

static void configure_context(SSL_CTX *ctx) {
  if (SSL_CTX_use_certificate_file(ctx, "cert.pem", SSL_FILETYPE_PEM) <= 0) {
    ERR_print_errors_fp(stderr);
    exit(EXIT_FAILURE);
  }

  if (SSL_CTX_use_PrivateKey_file(ctx, "key.pem", SSL_FILETYPE_PEM) <= 0) {
    ERR_print_errors_fp(stderr);
    exit(EXIT_FAILURE);
  }

  if (SSL_CTX_set_cipher_list(ctx, "AES128-GCM-SHA256") != 1) {
    ERR_print_errors_fp(stderr);
    exit(EXIT_FAILURE);
  }
}

static SSL_CTX *create_tls_context(void) {
  const SSL_METHOD *method;
  SSL_CTX *ctx;

  init_openssl();
  method = TLS_server_method();
  ctx = SSL_CTX_new(method);
  if (!ctx) {
    fprintf(stderr, "Unable to create SSL context\n");
    ERR_print_errors_fp(stderr);
    exit(EXIT_FAILURE);
  }

  configure_context(ctx);
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);
  SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS);
  SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
  SSL_CTX_set_verify_depth(ctx, 0);

  return ctx;
}

static inline void cpu_relax(void) {
  asm volatile("pause" ::: "memory");
}

static void job_ring_init(struct job_ring *r) {
  uint32_t i;
  __atomic_store_n(&r->head, 0, __ATOMIC_RELAXED);
  __atomic_store_n(&r->tail, 0, __ATOMIC_RELAXED);
  sem_init(&r->sem, 0, 0);
  r->mask = RING_CAPACITY - 1;
  for (i = 0; i < RING_CAPACITY; i++) {
    __atomic_store_n(&r->cells[i].seq, (uint64_t)i, __ATOMIC_RELAXED);
  }
}

static void job_ring_push(struct job_ring *r, const struct pool_job *job) {
  uint64_t pos = __atomic_fetch_add(&r->head, 1, __ATOMIC_RELAXED);
  struct job_ring_cell *cell = &r->cells[pos & r->mask];
  unsigned int spins = 0;

  while (__atomic_load_n(&cell->seq, __ATOMIC_ACQUIRE) != pos) {
    cpu_relax();
    if ((++spins & 0x3ff) == 0) {
      sched_yield();
    }
  }

  cell->job = *job;
  __atomic_store_n(&cell->seq, pos + 1, __ATOMIC_RELEASE);
  sem_post(&r->sem);
}

static void job_ring_pop_wait(struct job_ring *r, struct pool_job *job) {
  uint64_t pos;
  struct job_ring_cell *cell;

  sem_wait(&r->sem);  /* block until an item is committed */

  pos = __atomic_fetch_add(&r->tail, 1, __ATOMIC_RELAXED);
  cell = &r->cells[pos & r->mask];

  /* A producer claimed this head slot before us but may not have written
   * data yet — spin the (extremely short) gap. */
  while (__atomic_load_n(&cell->seq, __ATOMIC_ACQUIRE) != pos + 1) {
    cpu_relax();
  }

  *job = cell->job;
  __atomic_store_n(&cell->seq, pos + RING_CAPACITY, __ATOMIC_RELEASE);
}

static void conn_retain(struct conn *conn) {
  __atomic_add_fetch(&conn->refcnt, 1, __ATOMIC_RELAXED);
}

static void conn_release(struct conn *conn) {
  if (__atomic_sub_fetch(&conn->refcnt, 1, __ATOMIC_ACQ_REL) == 0) {
    free(conn);
  }
}

static size_t buffered_bytes(const struct conn *conn) {
  return (size_t)(conn->buf_tail - conn->buf_head);
}

static void compact_buffer(struct conn *conn) {
  size_t bytes = buffered_bytes(conn);
  if (conn->buf_head == 0) {
    return;
  }
  if (bytes) {
    memmove(conn->buf, &conn->buf[conn->buf_head], bytes);
  }
  conn->buf_head = 0;
  conn->buf_tail = (int)bytes;
}

static void setnonblocking(int fd) {
  int flags;

  flags = fcntl(fd, F_GETFL, 0);
  assert(flags >= 0);
  flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  assert(flags >= 0);
}

static int read_more_into_buffer(struct conn *conn) {
  ssize_t ret;
  bool read_any = false;

  compact_buffer(conn);
  while (conn->buf_tail < BUFSIZE) {
    ret = recv(conn->fd, &conn->buf[conn->buf_tail], BUFSIZE - conn->buf_tail, 0);
    if (ret > 0) {
      read_any = true;
      conn->buf_tail += ret;
      if (conn->buf_tail == BUFSIZE) {
        return 1;
      }
      continue;
    }
    if (ret == 0) {
      return -1;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return read_any ? 1 : 0;
    }
    return -1;
  }

  return 1;
}

static int read_header(struct conn *conn) {
  size_t header_len = sizeof(conn->header);

  while (buffered_bytes(conn) < header_len) {
    int ret = read_more_into_buffer(conn);
    if (ret <= 0) {
      return ret;
    }
  }

  memcpy(&conn->header, &conn->buf[conn->buf_head], header_len);
  conn->buf_head += (int)header_len;
  return 1;
}

static int consume_pending_bytes(struct conn *conn) {
  char scratch[2048];

  while (conn->pending_bytes > 0) {
    size_t available = buffered_bytes(conn);
    if (available > 0) {
      size_t take = available < conn->pending_bytes ? available : conn->pending_bytes;
      conn->buf_head += (int)take;
      conn->pending_bytes -= take;
      continue;
    }

    while (conn->pending_bytes > 0) {
      size_t chunk = conn->pending_bytes < sizeof(scratch) ? conn->pending_bytes : sizeof(scratch);
      ssize_t ret = recv(conn->fd, scratch, chunk, 0);
      if (ret > 0) {
        conn->pending_bytes -= (size_t)ret;
        if (ret < (ssize_t)chunk) {
          break;
        }
        continue;
      }
      if (ret == 0) {
        return -1;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return 0;
      }
      return -1;
    }
  }

  return 1;
}

static void retire_conn(struct io_thread_ctx *ctx, struct conn *conn) {
  if (conn->retired) {
    return;
  }
  conn->retired = true;
  conn->retired_next = ctx->retired_head;
  ctx->retired_head = conn;
}

static struct pool_completion *alloc_completion(int worker_id) {
  struct pool_completion *c;

  if (!local_completion_cache) {
    local_completion_cache =
        __atomic_exchange_n(&completion_cache[worker_id], NULL, __ATOMIC_ACQUIRE);
  }

  c = local_completion_cache;
  if (c) {
    local_completion_cache = c->resp_next;
    return c;
  }

  return malloc(sizeof(struct pool_completion));
}

static void free_completion(struct pool_completion *c) {
  struct pool_completion *head;
  int worker_id = c->worker_id;

  do {
    head = __atomic_load_n(&completion_cache[worker_id], __ATOMIC_RELAXED);
    c->resp_next = head;
  } while (!__atomic_compare_exchange_n(&completion_cache[worker_id], &head, c, false,
                                         __ATOMIC_RELEASE, __ATOMIC_RELAXED));
}

static void release_response(struct pool_completion *completion) {
  conn_release(completion->conn);
  free_completion(completion);
}

static void queue_conn_response(struct conn *conn, struct pool_completion *completion) {
  completion->resp_next = NULL;
  if (conn->resp_tail) {
    conn->resp_tail->resp_next = completion;
  } else {
    conn->resp_head = completion;
  }
  conn->resp_tail = completion;
}

static void inbox_push(struct conn *conn, struct pool_completion *c) {
  struct pool_completion *old;
  do {
    old = __atomic_load_n(&conn->inbox, __ATOMIC_RELAXED);
    c->resp_next = old;
  } while (!__atomic_compare_exchange_n(&conn->inbox, &old, c, false,
                                         __ATOMIC_RELEASE, __ATOMIC_RELAXED));
}

static void drain_inbox(struct conn *conn) {
  struct pool_completion *stack, *reversed, *next;

  stack = __atomic_exchange_n(&conn->inbox, NULL, __ATOMIC_ACQUIRE);
  if (!stack)
    return;

  reversed = NULL;
  while (stack) {
    next = stack->resp_next;
    stack->resp_next = reversed;
    reversed = stack;
    stack = next;
  }
  while (reversed) {
    next = reversed->resp_next;
    queue_conn_response(conn, reversed);
    reversed = next;
  }
}

static void discard_conn_responses(struct conn *conn) {
  struct pool_completion *completion;

  while (conn->resp_head) {
    completion = conn->resp_head;
    conn->resp_head = completion->resp_next;
    release_response(completion);
  }
  conn->resp_tail = NULL;

  completion = __atomic_exchange_n(&conn->inbox, NULL, __ATOMIC_ACQUIRE);
  while (completion) {
    struct pool_completion *next = completion->resp_next;
    release_response(completion);
    completion = next;
  }
}

static void close_conn(struct io_thread_ctx *ctx, struct conn *conn) {
  int fd = __atomic_load_n(&conn->fd, __ATOMIC_ACQUIRE);

  if (fd < 0) {
    return;
  }
  /* Set closed before touching the fd so worker threads see it and skip epoll_ctl. */
  __atomic_store_n(&conn->closed, 1, __ATOMIC_RELEASE);
  discard_conn_responses(conn);
  epoll_ctl(shared_epollfd, EPOLL_CTL_DEL, fd, NULL);
  if (conn->ssl) {
    SSL_free(conn->ssl);
    conn->ssl = NULL;
  }
  close(fd);
  __atomic_store_n(&conn->fd, -1, __ATOMIC_RELEASE);
  if (ctx) {
    retire_conn(ctx, conn);
  }
}

static bool try_acquire_conn_io(struct conn *conn) {
  return __atomic_compare_exchange_n(&conn->io_owned, &(int){0}, 1, false,
                                     __ATOMIC_ACQUIRE, __ATOMIC_RELAXED);
}

static void release_conn_io(struct conn *conn) {
  __atomic_store_n(&conn->io_owned, 0, __ATOMIC_RELEASE);
}

static void update_conn_events(struct conn *conn, bool want_epollout) {
  struct epoll_event ev;
  int fd;

  /* Called from both IO threads (under io_owned) and worker threads, so all
   * shared fields must be accessed atomically. */
  if (__atomic_load_n(&conn->closed, __ATOMIC_ACQUIRE)) {
    return;
  }
  if (__atomic_load_n(&conn->epollout_armed, __ATOMIC_RELAXED) == (int)want_epollout) {
    return;
  }
  fd = __atomic_load_n(&conn->fd, __ATOMIC_ACQUIRE);
  if (fd < 0) {
    return;
  }

  memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
  if (want_epollout) {
    ev.events |= EPOLLOUT;
  }
  ev.data.u64 = (uint64_t)(uintptr_t)conn;
  __atomic_store_n(&conn->epollout_armed, (int)want_epollout, __ATOMIC_RELAXED);
  /* Ignore errors: the conn may have been closed concurrently. */
  epoll_ctl(shared_epollfd, EPOLL_CTL_MOD, fd, &ev);
}

static void release_retired_conns(struct io_thread_ctx *ctx) {
  struct conn *conn = ctx->retired_head;
  ctx->retired_head = NULL;

  if (io_thread_count > 1) {
    /*
     * Shared epoll can still hand other I/O threads stale conn pointers after
     * EPOLL_CTL_DEL. Keeping the accept-time reference avoids freeing conn
     * storage while those threads finish draining already-returned events.
     *
     * This intentionally leaks one conn object per closed connection in the
     * multi-I/O-thread configuration, which is acceptable for the benchmark's
     * short-lived runs and much safer than a sporadic use-after-free.
     */
    return;
  }

  while (conn) {
    struct conn *next = conn->retired_next;
    conn->retired_next = NULL;
    conn_release(conn);
    conn = next;
  }
}

static int parse_positive_arg(const char *name, const char *value) {
  char *end = NULL;
  long parsed;

  errno = 0;
  parsed = strtol(value, &end, 10);
  if (errno != 0 || end == value || *end != '\0' || parsed <= 0 || parsed > INT_MAX) {
    fprintf(stderr, "Invalid value for %s: %s\n", name, value);
    exit(EXIT_FAILURE);
  }

  return (int)parsed;
}

static int parse_positive_env(const char *name, int default_value) {
  const char *value = getenv(name);

  if (!value || !value[0]) {
    return default_value;
  }

  return parse_positive_arg(name, value);
}

static int default_io_thread_count(void) {
  // int suggested = nr_cpu / 5;
  int suggested = 4;
  if (suggested < 1) {
    suggested = 1;
  }
  if (suggested > MAX_THREADS) {
    suggested = MAX_THREADS;
  }
  return suggested;
}

static void enqueue_job(struct conn *conn) {
  struct pool_job job;

  conn_retain(conn);
  job.conn = conn;
  job.req_id = conn->header.id;
  job_ring_push(&job_ring, &job);
}

static bool valid_opcode(uint8_t opcode) {
  return opcode == CMD_SET || opcode == CMD_GET || opcode == CMD_GETK;
}

/*
 * Drain as many complete requests as are currently readable from conn and
 * enqueue one job per request. Returns true if at least one job was enqueued.
 * Returns false if more data is needed before the next request completes.
 * On error the conn is closed and false is returned.
 */
static bool process_conn_reads(struct io_thread_ctx *ctx, struct conn *conn) {
  bool queued_any = false;

  while (1) {
    int ret;

    switch (conn->state) {
    case STATE_HEADER:
      ret = read_header(conn);
      if (ret == 0) {
        return queued_any;
      }
      if (ret < 0) {
        close_conn(ctx, conn);
        return false;
      }
      if (conn->header.magic != 0x80 || !valid_opcode(conn->header.opcode)) {
        close_conn(ctx, conn);
        return false;
      }
      conn->pending_bytes = conn->header.extra_len;
      conn->state = STATE_EXTRA;
      break;
    case STATE_EXTRA:
      ret = consume_pending_bytes(conn);
      if (ret == 0) {
        return queued_any;
      }
      if (ret < 0) {
        close_conn(ctx, conn);
        return false;
      }
      conn->pending_bytes = ntohs(conn->header.key_len);
      conn->state = STATE_KEY;
      break;
    case STATE_KEY:
      ret = consume_pending_bytes(conn);
      if (ret == 0) {
        return queued_any;
      }
      if (ret < 0) {
        close_conn(ctx, conn);
        return false;
      }
      if (conn->header.opcode == CMD_SET) {
        uint32_t body_len = ntohl(conn->header.body_len);
        uint16_t key_len = ntohs(conn->header.key_len);
        if (body_len < (uint32_t)(key_len + conn->header.extra_len)) {
          close_conn(ctx, conn);
          return false;
        }
        conn->pending_bytes = body_len - key_len - conn->header.extra_len;
      } else {
        conn->pending_bytes = 0;
      }
      conn->state = STATE_VALUE;
      break;
    case STATE_VALUE:
      ret = consume_pending_bytes(conn);
      if (ret == 0) {
        return queued_any;
      }
      if (ret < 0) {
        close_conn(ctx, conn);
        return false;
      }
      enqueue_job(conn);
      queued_any = true;
      conn->state = STATE_HEADER;
      conn->pending_bytes = 0;
      break;
    default:
      assert(0);
    }
  }

  return queued_any;
}

static int flush_conn_responses(struct io_thread_ctx *ctx, struct conn *conn) {
  while (conn->resp_head) {
    struct pool_completion *completion = conn->resp_head;
    const char *buf = (const char *)&completion->response;
    size_t total = sizeof(completion->response);

    while (completion->sent < total) {
      ssize_t ret = send(conn->fd, buf + completion->sent, total - completion->sent, MSG_NOSIGNAL);
      if (ret > 0) {
        completion->sent += (size_t)ret;
        continue;
      }
      if (ret == 0) {
        close_conn(ctx, conn);
        return -1;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        update_conn_events(conn, true);
        return 0;
      }
      close_conn(ctx, conn);
      return -1;
    }

    conn->resp_head = completion->resp_next;
    if (!conn->resp_head) {
      conn->resp_tail = NULL;
    }
    release_response(completion);
  }

  update_conn_events(conn, false);
  return 1;
}

static void drive_conn(struct io_thread_ctx *ctx, struct conn *conn) {
  while (1) {
    int flush_ret;

    __atomic_store_n(&conn->io_pending, 0, __ATOMIC_RELAXED);

    drain_inbox(conn);
    flush_ret = flush_conn_responses(ctx, conn);
    if (flush_ret < 0) {
      return;
    }

    process_conn_reads(ctx, conn);
    if (conn->fd < 0) {
      return;
    }

    if (!__atomic_exchange_n(&conn->io_pending, 0, __ATOMIC_ACQ_REL)) {
      return;
    }
  }
}

static void drive_owned_conn_until_idle(struct io_thread_ctx *ctx, struct conn *conn) {
  while (1) {
    drive_conn(ctx, conn);
    release_conn_io(conn);
    if (!__atomic_exchange_n(&conn->io_pending, 0, __ATOMIC_ACQ_REL)) {
      return;
    }
    if (!try_acquire_conn_io(conn)) {
      return;
    }
  }
}

static void flush_owned_conn_from_worker(struct conn *conn) {
  __atomic_store_n(&conn->io_pending, 0, __ATOMIC_RELAXED);
  drain_inbox(conn);
  flush_conn_responses(NULL, conn);
  release_conn_io(conn);

  if (!__atomic_exchange_n(&conn->io_pending, 0, __ATOMIC_ACQ_REL)) {
    return;
  }
  if (try_acquire_conn_io(conn)) {
    drive_owned_conn_until_idle(NULL, conn);
  } else {
    update_conn_events(conn, true);
  }
}

static void schedule_conn_drive(struct io_thread_ctx *ctx, struct conn *conn) {
  __atomic_store_n(&conn->io_pending, 1, __ATOMIC_RELEASE);
  if (!try_acquire_conn_io(conn)) {
    return;
  }
  drive_owned_conn_until_idle(ctx, conn);
}

static int open_listen_socket(void) {
  struct sockaddr_in sin;
  int one = 1;
  int sock = socket(AF_INET, SOCK_STREAM, 0);

  if (sock < 0) {
    perror("socket");
    exit(EXIT_FAILURE);
  }

  setnonblocking(sock);
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, (void *)&one, sizeof(one))) {
    perror("setsockopt(SO_REUSEPORT)");
    exit(EXIT_FAILURE);
  }
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void *)&one, sizeof(one))) {
    perror("setsockopt(SO_REUSEADDR)");
    exit(EXIT_FAILURE);
  }

  memset(&sin, 0, sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = htonl(0);
  sin.sin_port = htons(40002);

  if (bind(sock, (struct sockaddr *)&sin, sizeof(sin))) {
    perror("bind");
    exit(EXIT_FAILURE);
  }
  if (listen(sock, BACKLOG)) {
    perror("listen");
    exit(EXIT_FAILURE);
  }

  return sock;
}

static void accept_new_connections(struct io_thread_ctx *ctx) {
  (void)ctx;

  while (1) {
    struct conn *conn;
    struct epoll_event ev;
    int conn_sock;
    int one = 1;
    SSL *ssl;

    conn_sock = accept(listenfd, NULL, NULL);
    if (conn_sock == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      perror("accept");
      exit(EXIT_FAILURE);
    }

    if (setsockopt(conn_sock, IPPROTO_TCP, TCP_NODELAY, (void *)&one, sizeof(one))) {
      perror("setsockopt(TCP_NODELAY)");
      exit(EXIT_FAILURE);
    }

    ssl = SSL_new(ssl_ctx);
    if (!ssl) {
      ERR_print_errors_fp(stderr);
      close(conn_sock);
      continue;
    }
    SSL_set_fd(ssl, conn_sock);
    if (SSL_accept(ssl) <= 0) {
      ERR_print_errors_fp(stderr);
      SSL_free(ssl);
      close(conn_sock);
      continue;
    }

    setnonblocking(conn_sock);

    conn = calloc(1, sizeof(*conn));
    if (!conn) {
      perror("calloc");
      SSL_free(ssl);
      close(conn_sock);
      exit(EXIT_FAILURE);
    }
    conn->fd = conn_sock;
    conn->refcnt = 1;
    conn->state = STATE_HEADER;
    conn->ssl = ssl;

    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN | EPOLLERR | EPOLLHUP;
    ev.data.u64 = (uint64_t)(uintptr_t)conn;
    if (epoll_ctl(shared_epollfd, EPOLL_CTL_ADD, conn_sock, &ev) == -1) {
      perror("epoll_ctl");
      exit(EXIT_FAILURE);
    }
  }
}

static void *worker_thread_main(void *arg) {
  int worker_id = *(int *)arg;

  thread_no = worker_id;
  init_thread();
  while (1) {
    struct pool_job job;
    struct pool_completion *completion;

    job_ring_pop_wait(&job_ring, &job);
    completion = alloc_completion(worker_id);

    if (!completion) {
      perror("malloc");
      exit(EXIT_FAILURE);
    }

    process_request();

    memset(completion, 0, sizeof(*completion));
    completion->conn = job.conn;
    completion->req_id = job.req_id;
    completion->worker_id = worker_id;
    completion->response.magic = 0x81;
    completion->response.status = htons(1);
    completion->response.body_len = 0;
    completion->response.id = job.req_id;

    inbox_push(job.conn, completion);

    if (__atomic_load_n(&job.conn->closed, __ATOMIC_ACQUIRE)) {
      /* The connection was closed and its inbox was already discarded.
       * Drain whatever we (or other workers) pushed to prevent a leak.
       * The __atomic_exchange_n ensures only one winner; others get NULL. */
      struct pool_completion *stale =
          __atomic_exchange_n(&job.conn->inbox, NULL, __ATOMIC_ACQUIRE);
      while (stale) {
        struct pool_completion *next = stale->resp_next;
        release_response(stale);
        stale = next;
      }
    } else {
      __atomic_store_n(&job.conn->io_pending, 1, __ATOMIC_RELEASE);
      if (try_acquire_conn_io(job.conn)) {
        flush_owned_conn_from_worker(job.conn);
      } else {
        update_conn_events(job.conn, true);
      }
    }
  }

  return NULL;
}

static void *io_thread_main(void *arg) {
  struct io_thread_ctx *ctx = arg;
  struct epoll_event events[MAX_EVENTS];
  int i;

  thread_no = ctx->id;

  while (1) {
    int nfds = epoll_wait(shared_epollfd, events, MAX_EVENTS, -1);
    if (nfds < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("epoll_wait");
      exit(EXIT_FAILURE);
    }

    for (i = 0; i < nfds; i++) {
      uint64_t token = events[i].data.u64;
      if (token == EPOLL_TOKEN_LISTENER) {
        accept_new_connections(ctx);
        continue;
      }

      {
        struct conn *conn = (struct conn *)(uintptr_t)token;
        if (__atomic_load_n(&conn->fd, __ATOMIC_ACQUIRE) < 0) {
          continue;
        }
        if (events[i].events & (EPOLLERR | EPOLLHUP)) {
          if (try_acquire_conn_io(conn)) {
            close_conn(ctx, conn);
            release_conn_io(conn);
          } else {
            __atomic_store_n(&conn->io_pending, 1, __ATOMIC_RELEASE);
          }
          continue;
        }
        schedule_conn_drive(ctx, conn);
      }
    }

    release_retired_conns(ctx);
  }

  return NULL;
}

void init_linux(void) {
  srand48(mytime());

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  nr_cpu = CPU_COUNT(&cpuset);
}

void start_linux_pool_server(const char *str, const char *io_threads_arg,
                             const char *worker_threads_arg) {
  struct epoll_event ev;
  int i;

  if (strcmp(str, "memcache-id") != 0) {
    fprintf(stderr, "spin-linux-pool-tls only supports memcache-id, got %s\n", str);
    exit(EXIT_FAILURE);
  }

  service_proto = str;
  job_ring_init(&job_ring);
  ssl_ctx = create_tls_context();

  io_thread_count = io_threads_arg
    ? parse_positive_arg("pool I/O thread count", io_threads_arg)
    : parse_positive_env("TCP_POOL_IO_THREADS", default_io_thread_count());
  if (io_thread_count > nr_cpu) {
    io_thread_count = nr_cpu;
  }
  if (io_thread_count > MAX_THREADS) {
    io_thread_count = MAX_THREADS;
  }

  worker_thread_count = worker_threads_arg
    ? parse_positive_arg("pool worker thread count", worker_threads_arg)
    : parse_positive_env("TCP_POOL_WORKERS", nr_cpu > 0 ? nr_cpu : 1);
  if (worker_thread_count > MAX_THREADS) {
    worker_thread_count = MAX_THREADS;
  }

  fprintf(stderr, "pool I/O threads=%d, worker threads=%d\n",
          io_thread_count, worker_thread_count);

  shared_epollfd = epoll_create1(0);
  if (shared_epollfd == -1) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  listenfd = open_listen_socket();
  memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLIN | EPOLLEXCLUSIVE;
  ev.data.u64 = EPOLL_TOKEN_LISTENER;
  if (epoll_ctl(shared_epollfd, EPOLL_CTL_ADD, listenfd, &ev) == -1) {
    perror("epoll_ctl listenfd");
    exit(EXIT_FAILURE);
  }

  for (i = 0; i < io_thread_count; i++) {
    io_threads[i].id = i;
    io_threads[i].retired_head = NULL;
  }

  for (i = 0; i < worker_thread_count; i++) {
    worker_ids[i] = i;
    if (pthread_create(&worker_tids[i], NULL, worker_thread_main, &worker_ids[i])) {
      fprintf(stderr, "failed to spawn worker thread %d\n", i);
      exit(EXIT_FAILURE);
    }
  }

  for (i = 1; i < io_thread_count; i++) {
    if (pthread_create(&io_threads[i].tid, NULL, io_thread_main, &io_threads[i])) {
      fprintf(stderr, "failed to spawn I/O thread %d\n", i);
      exit(EXIT_FAILURE);
    }
  }

  io_thread_main(&io_threads[0]);
}
