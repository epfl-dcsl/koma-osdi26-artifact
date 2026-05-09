#if 0
#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <signal.h>

#include "config.h"

#include "./silo/benchmarks/dcsl.h"

/* libbcc */
#include <arpa/inet.h>
#include <bcc/bcc_common.h>
#include <bcc/libbpf.h>
#include <bpf/bpf.h>

#include "config.h"
#include "kcm-helpers.h"

#define CMD_GET 0x00
#define CMD_GETK 0x0c
#define CMD_SET 0x01

#define BUFSIZE 2048
#define BACKLOG 8192
#define MAX_THREADS 64
#define EPOLLEXCLUSIVE (1 << 28)

typedef struct __attribute__ ((__packed__)) {
	uint8_t magic;
	uint8_t opcode;
	uint32_t id;
	uint16_t key_len;
	uint8_t extra_len;
	uint8_t data_type;
	union {
		uint16_t vbucket; // request use
		uint16_t status;  // response use
	};
	uint32_t body_len;
	uint32_t opaque;
	uint64_t version;
} bid_header_t;

struct memcached_request {
	bid_header_t id_header;
	unsigned char data[BUFSIZE];
};

static int epollfd[MAX_THREADS];
static __thread int thread_no;
static int nr_threads;
int bpf_prog_fd;

int bpf_init(void)
{
	int fd = 0;
	char bpf_prog[50];
	void *mod;
	strcpy(bpf_prog, "memcache-id_kern.c");

	mod = bpf_module_create_c(bpf_prog, 0, NULL, 0, 0, NULL);
	fd = bcc_prog_load(BPF_PROG_TYPE_SK_SKB, "memcached_koma",
					   bpf_function_start(mod, "memcached_koma"),
					   bpf_function_size(mod, "memcached_koma"),
					   bpf_module_license(mod), bpf_module_kern_version(mod), 0,
					   NULL, 0);
	/*printf("fd of kcm ebpf file is: %d\n", fd);*/
	if (fd == -1)
		exit(1);
	return fd;
}

// Signal handler function
void signal_handler(int signal)
{
	printf("Caught signal %d\n", signal);
	exit(0);
}

/**
 * min - picks the minimum of two expressions
 *
 * Arguments @a and @b are evaluated exactly once
 */
#define min(a, b) \
	({typeof(a) _a = (a); \
	  typeof(b) _b = (b); \
	  _a < _b ? _a : _b; })


static long mytime(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000000 + tv.tv_usec;
}

static int handle_ret(int fd, ssize_t ret, int line)
{
	if (ret == 0) {
		close(fd);
		/* TODO: should also free conn */
		return 1;
	} else if (ret == -1) {
		switch (errno) {
		case EAGAIN:
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

	// assert(ret == 1);

	return 0;
}

static void drive_machine(int worker_id, int fd)
{
	ssize_t ret;
	struct memcached_request my_msg = {0};
	bid_header_t response;
	struct msghdr msg;
	struct iovec iov;
	struct sockaddr_in client_addr;
	char cmsgbuf[CMSG_SPACE(sizeof(uint32_t))];
	struct cmsghdr *cmsg;
	uint32_t mark = 0;

	iov.iov_base = &my_msg;
	iov.iov_len = sizeof(my_msg);
	memset(&msg, 0, sizeof(msg));
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	memset(&client_addr, 0, sizeof(client_addr));
	msg.msg_name = &client_addr;
	msg.msg_namelen = sizeof(client_addr);
	msg.msg_control = cmsgbuf;
	msg.msg_controllen = sizeof(cmsgbuf);

	ret = recvmsg(fd, &msg, 0);
	if (handle_ret(fd, ret, __LINE__))
		return;

	// Extract client IP and port
	/*char client_ip[INET_ADDRSTRLEN];*/
	/*inet_ntop(AF_INET, &client_addr.sin_addr.s_addr, client_ip,*/
	/*sizeof(client_ip));*/
	/*int client_port = ntohs(client_addr.sin_port);*/

	// // debug mark
	// for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL;
	// 	 cmsg = CMSG_NXTHDR(&msg, cmsg))
	// {
	// 	if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SO_MARK)
	// 	{
	// 		memcpy(&mark, CMSG_DATA(cmsg), sizeof(mark));
	// 		break;
	// 	}
	// }

	// printf("Received message from IP family Mark: %u\n", mark);

	if (my_msg.id_header.magic != 0x80)
		printf("Received message %d\n", my_msg.id_header.magic);

	// start with driver drive_machine
	assert(my_msg.id_header.magic == 0x80);
	assert(my_msg.id_header.opcode == CMD_SET ||
		   my_msg.id_header.opcode == CMD_GET ||
		   my_msg.id_header.opcode == CMD_GETK);

	dcsl_exec_rd_trans(worker_id);


	response.magic = 0x81;
	response.status = __builtin_bswap16(1);
	response.body_len = 0;
	response.id = my_msg.id_header.id;
	iov.iov_base = &response;
	iov.iov_len = sizeof(bid_header_t);

	ret = sendmsg(fd, &msg, 0);
	if (handle_ret(fd, ret, __LINE__))
		return;
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

static void setnonblocking(int fd)
{
	int flags;

	flags = fcntl(fd, F_GETFL, 0);
	assert(flags >= 0);
	flags = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	assert(flags >= 0);
}


static void *tcp_thread_main(void *arg)
{
	struct sockaddr_in sin;
	int sock;
	int one;
	int ret, i, nfds, conn_sock;
    int kcm_fd0, kcm_fd_cur;
	struct epoll_event ev, events[CONFIG_MAX_EVENTS];
	struct conn *conn;

	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (!sock) {
		perror("socket");
		exit(1);
	}

	setnonblocking(sock);

	one = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, (void *) &one, sizeof(one))) {
		perror("setsockopt(SO_REUSEPORT)");
		exit(1);
	}

	one = 1;
	if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (void *) &one, sizeof(one))) {
		perror("setsockopt(SO_REUSEADDR)");
		exit(1);
	}

	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(0);
	sin.sin_port = htons(40001);

	if (bind(sock, (struct sockaddr*)&sin, sizeof(sin))) {
		perror("bind");
		exit(1);
	}

	if (listen(sock, BACKLOG)) {
		perror("listen");
		exit(1);
	}

	thread_no = (long) arg;

	dcsl_init_worker(thread_no);

	epollfd[thread_no] = epoll_create1(0);
	ev.events = EPOLLIN;
	ev.data.u32 = 0;
	ret = epoll_ctl(epollfd[thread_no], EPOLL_CTL_ADD, sock, &ev);
	assert(!ret);

	// The ONLY koma socket.
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
				setnonblocking(conn_sock);
				if (setsockopt(conn_sock, IPPROTO_TCP, TCP_NODELAY, (void *) &one, sizeof(one))) {
					perror("setsockopt(TCP_NODELAY)");
					exit(1);
				}
				// attach the tcp sock to the koma system.
                kcm_fd0 = kcm_init();
                kcm_attach(kcm_fd0, conn_sock, bpf_prog_fd);
                epoll_ctl_add(kcm_fd0, thread_no);
                for (int j = 0; j < nr_threads; j++) {
                    if (j != thread_no) {
                      kcm_fd_cur = kcm_clone(kcm_fd0);
                      printf("thread %d is cloning kcm socket for fd %d\n", j, conn_sock);
                      epoll_ctl_add(kcm_fd_cur, j);
                    }
                  }
			} else {
				if (events[i].events & (EPOLLHUP | EPOLLERR)) {
					close(events[i].data.fd);
					/* TODO: should also free conn */
				} else {
					drive_machine(thread_no, events[i].data.fd);
				}
			}
		}
	}

	return NULL;
}


int main(int argc, char *argv[])
{
	int i;
	pthread_t tid;

	srand48(mytime());

	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
	nr_threads = CPU_COUNT(&cpuset);

	signal(SIGPIPE, signal_handler);
	signal(SIGTERM, signal_handler);
	signal(SIGINT, signal_handler);

	bpf_prog_fd = bpf_init();

	/*The Silo init calls.*/
	dcsl_init_db();
	fprintf(stderr, "Done initiliazing database.\n");
	dcsl_init_globals(nr_threads);
	fprintf(stderr, "Done initiliazing globals.\n");
	dcsl_make_loaders();
	fprintf(stderr, "Done initiliazing loaders.\n");
	dcsl_make_workers();
	fprintf(stderr, "nrc cpu is %d\n", nr_threads);

	for (i = 1; i < nr_threads; i++) {
		if (pthread_create(&tid, NULL, tcp_thread_main, (void *) (long) i)) {
			fprintf(stderr, "failed to spawn thread %d\n", i);
			exit(-1);
		}
	}

	tcp_thread_main(0);
	close(bpf_prog_fd);
	return 0;
}
#endif

#include "common.h"
#include "silo-workload.h"

int main(void) {
	init_kcm();
	silo_workload_init();
	start_kcm_server("memcache-id");

	return 0;
}
