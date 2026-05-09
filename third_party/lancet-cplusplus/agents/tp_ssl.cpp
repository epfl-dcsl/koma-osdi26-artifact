/*
 * MIT License
 *
 * Copyright (c) 2019-2021 Ecole Polytechnique Federale Lausanne (EPFL)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#include <iostream>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <cstdlib>
#include <cassert>
#include <ctime>

#include <lancet/error.hpp>
#include <lancet/misc.hpp>
#include <lancet/timestamping.hpp>
#include <lancet/tp_proto.hpp>

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

static SSL_CTX *ssl_ctx;
static thread_local std::vector<tls_connection> connections;
static thread_local int epoll_fd;
static thread_local std::vector<pending_tx_timestamps> per_conn_tx_timestamps;
static thread_local std::vector<std::unordered_map<uint32_t, timestamp_info>>
	per_conn_tx_timestamps_map;

static inline struct tls_connection *pick_conn()
{
	int idx;
	struct tls_connection *c;

	// FIXME: Consider picking connection round robin
	// idx = rand() % (get_conn_count() / get_thread_count()) ;
	idx = rand() % (get_conn_count() / get_thread_count());
	c = &connections[idx];
	if ((c->conn.pending_reqs < get_max_pending_reqs()) && (!c->conn.closed))
		return c;

	return NULL;
}


static int ssl_init(void)
{
	/* Load encryption & hashing algorithms for the SSL program */
	SSL_library_init();

	/* Load the error strings for SSL & CRYPTO APIs */
	SSL_load_error_strings();

	// SSL context for the process. All connections will share one
	// process level context.
	ssl_ctx = SSL_CTX_new(TLS_client_method());
	if (!ssl_ctx)
		return -1;

	SSL_CTX_set_min_proto_version(ssl_ctx, TLS1_2_VERSION);
	// SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv3); 

	/* Don't verify the certificate */
	SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_NONE, NULL);

	/* Don't use session caching */
	SSL_CTX_set_session_cache_mode(ssl_ctx, SSL_SESS_CACHE_OFF);

	return 0;
}

static int ssl_init_connection(struct tls_connection *tls_conn)
{
	tls_conn->ssl = SSL_new(ssl_ctx);
	assert(tls_conn->ssl);

	int err = SSL_set_fd(tls_conn->ssl, tls_conn->conn.fd);
	assert(err == 1);

	/*
	 * Assume that connection is in blocking mode
	 */
	err = SSL_connect(tls_conn->ssl);
	if (err <= 0) {
		int ssl_error = SSL_get_error(tls_conn->ssl, err);
		lancet_fprintf(std::cerr, "Failed to SSL connect: %d\n", ssl_error);
		switch (ssl_error) {
			case SSL_ERROR_SSL:
				lancet_fprintf(std::cerr, "SSL error: %s\n", ERR_error_string(ERR_get_error(), NULL));
				break;
			case SSL_ERROR_SYSCALL:
				perror("System call error");
				break;
			case SSL_ERROR_WANT_READ:
			case SSL_ERROR_WANT_WRITE:
				lancet_fprintf(std::cerr, "Non-blocking operation, try again later\n");
				break;
			case SSL_ERROR_ZERO_RETURN:
				lancet_fprintf(std::cerr, "Connection closed gracefully\n");
				break;
			default:
				lancet_fprintf(std::cerr, "Unknown SSL error\n");
				break;
		}
		return -1;
	}


	err = SSL_is_init_finished(tls_conn->ssl);
	assert(err == 1);

	return 0;
}


static int throughput_open_connections(void)
{
	/*init epoll*/
	struct sockaddr_in addr;
	int i, efd, ret, sock, per_thread_conn, dest_idx, n;
	int one = 1;
	struct epoll_event event;
	struct linger linger;
	std::vector<host_tuple> targets;

	addr.sin_family = AF_INET;
	efd = epoll_create(1);
	if (efd < 0) {
		lancet_perror("epoll_create error");
		return -1;
	}

	per_thread_conn = get_conn_count() / get_thread_count();
	connections.resize(per_thread_conn);
	assert(connections.size());
	if ((get_agent_type() == SYMMETRIC_NIC_TIMESTAMP_AGENT) ||
		(get_agent_type() == SYMMETRIC_AGENT)) {
		per_conn_tx_timestamps.resize(per_thread_conn);
		assert(per_conn_tx_timestamps.size());
		for (i = 0; i < per_thread_conn; i++) {
			per_conn_tx_timestamps[i].pending.resize(get_max_pending_reqs());
			assert(per_conn_tx_timestamps[i].pending.size());
		}
	}
	if ((get_agent_type() == SYMMETRIC_MAPPING_AGENT)) {
		per_conn_tx_timestamps_map.resize(per_thread_conn);
		assert(per_conn_tx_timestamps_map.size());
	}
	targets = get_targets();

	for (i = 0; i < per_thread_conn; i++) {
		sock = socket(AF_INET, SOCK_STREAM, 0);
		if (sock == -1) {
			lancet_perror("Error creating socket");
			return -1;
		}
		dest_idx = i % get_target_count();
		addr.sin_port = htons(targets[dest_idx].port);
		addr.sin_addr.s_addr = targets[dest_idx].ip;
		ret = connect(sock, (struct sockaddr *)&addr, sizeof(addr));
		if (ret) {
			lancet_perror("Error connecting");
			return -1;
		}

		connections[i].conn.fd = sock;
		connections[i].conn.pending_reqs = 0;
		connections[i].conn.idx = i;
		connections[i].conn.buffer_idx = 0;
		connections[i].conn.closed = 0;

		/* Init connection in blocking mode */
		if (ssl_init_connection(&connections[i]))
			return -1;

		ret = fcntl(sock, F_SETFL, O_NONBLOCK);
		if (ret == -1) {
			lancet_perror("Error while setting nonblocking");
			return -1;
		}

		n = 524288;
		ret = setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &n, sizeof(n));
		if (ret) {
			lancet_perror("Error setsockopt");
			return -1;
		}
		n = 524288;
		ret = setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &n, sizeof(n));
		if (ret) {
			lancet_perror("Error setsockopt");
			return -1;
		}

		if (get_agent_type() == SYMMETRIC_NIC_TIMESTAMP_AGENT) {
			if (setsockopt(sock, SOL_SOCKET, SO_BINDTODEVICE, get_if_name().c_str(),
						   get_if_name().length())) {
				lancet_perror("setsockopt SO_BINDTODEVICE");
				return -1;
			}
			ret = sock_enable_timestamping(sock);
			if (ret) {
				lancet_fprintf(std::cerr, "sock enable timestamping failed\n");
				return -1;
			}
		}

		/* Disable Nagle's algorithm */
		ret = setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
		if (ret) {
			lancet_perror("Error setsockopt");
			return -1;
		}

		/* Close with RST not FIN */
		linger.l_onoff = 1;
		linger.l_linger = 0;
		if (setsockopt(sock, SOL_SOCKET, SO_LINGER, (void *)&linger,
					   sizeof(linger))) {
			perror("setsockopt(SO_LINGER)");
			exit(1);
		}

		event.events = EPOLLIN;
		event.data.u32 = i;
		ret = epoll_ctl(efd, EPOLL_CTL_ADD, sock, &event);
		if (ret) {
			lancet_perror("Error while adding to epoll group");
			return -1;
		}
	}
	epoll_fd = efd;
	return 0;
}

static void send_request(struct request *to_send, uint32_t fd)
{
	struct msghdr hdr;
	int ret, bytes_to_send, i, current_iov_cnt;
	current_iov_cnt = to_send->iov_cnt;

	struct iovec *current_iovs = to_send->iovs;

	int cumulative_bytes;

	do {
		bzero(&hdr, sizeof(hdr));
		hdr.msg_iov = current_iovs;
		hdr.msg_iovlen = current_iov_cnt;

		bytes_to_send = 0;
		for (i = 0; i < current_iov_cnt; i++)
			bytes_to_send += current_iovs[i].iov_len;

		ret = sendmsg(fd, &hdr, 0);
		if ((ret < 0) && (errno != EWOULDBLOCK)) {
			lancet_perror("Unknown connection error write\n");
			return;
		}
		if (ret < bytes_to_send) {
			i = 0;
			cumulative_bytes = 0;
			while (cumulative_bytes < ret) {
				assert(i < current_iov_cnt);
				assert(cumulative_bytes <= ret); // should never exceed ret

				if (cumulative_bytes + current_iovs[i].iov_len > ret) {
					// found, but too much
					// set remaining bytes
					current_iovs[i].iov_len =
						(cumulative_bytes + current_iovs[i].iov_len) - ret;
					current_iovs[i].iov_base =
						current_iovs[i].iov_base + (ret - cumulative_bytes);
					current_iovs = &current_iovs[i];
					current_iov_cnt -= i;
					cumulative_bytes = ret;
				} else if (cumulative_bytes + current_iovs[i].iov_len == ret) {
					current_iovs = &current_iovs[i + 1];
					current_iov_cnt -= (i + 1);
					cumulative_bytes = ret;
				} else {
					cumulative_bytes += current_iovs[i].iov_len;
					i++;
				}
			}
		}
	} while (ret < bytes_to_send);
}

static void symmetric_ssl_main(int thread_idx)
{
	int ready, idx, i, j, conn_per_thread, ret, bytes_to_send;
	long next_tx, copied;
	std::vector<epoll_event> events;
	struct tls_connection *conn;
	struct request *to_send;
	struct consume_resp_pair read_res;
	struct byte_req_pair send_res;
	struct timespec tx_timestamp, rx_timestamp, latency;
	struct timestamp_info *pending_tx;
	char *wbuf;
	uint64_t wbuf_size = 512;

	wbuf = (char *)malloc(wbuf_size);
	if (throughput_open_connections())
		return;

	/*Initializations*/
	conn_per_thread = get_conn_count() / get_thread_count();
	events.resize(conn_per_thread);

	pthread_barrier_wait(&conn_open_barrier);
	set_conn_open(1);

	next_tx = time_ns();
	while (1) {
		if (!should_load()) {
			next_tx = time_ns();
			continue;
		}
		while (time_ns() >= next_tx) {
			conn = pick_conn();
			if (!conn)
				goto REP_PROC;
			to_send = prepare_request();

            bytes_to_send = 0;
			for (i = 0; i < to_send->iov_cnt; i++)
				bytes_to_send += to_send->iovs[i].iov_len;

			if (bytes_to_send > wbuf_size) {
				free(wbuf);
				wbuf = (char *)malloc(bytes_to_send);
				wbuf_size = bytes_to_send;
			}

			copied = 0;
			for (i = 0; i < to_send->iov_cnt; i++) {
				memcpy(&wbuf[copied], to_send->iovs[i].iov_base,
					   to_send->iovs[i].iov_len);
				copied += to_send->iovs[i].iov_len;
			}
			// assert(copied == bytes_to_send);
			int ret = SSL_write(conn->ssl, wbuf, bytes_to_send);

			// assert(ret == bytes_to_send);

			time_ns_to_ts(&tx_timestamp);
			push_complete_tx_timestamp(&per_conn_tx_timestamps[conn->conn.idx],
									   &tx_timestamp);
			conn->conn.pending_reqs++;

			/*BookKeeping*/
			send_res.bytes = ret;
			send_res.reqs = 1;
			add_throughput_tx_sample(send_res);

			/*Schedule next*/
			next_tx += get_ia();
		}
	REP_PROC:
		/* process responses */
		ready = epoll_wait(epoll_fd, events.data(), conn_per_thread, 0);
		for (i = 0; i < ready; i++) {
			idx = events[i].data.u32;
			conn = &connections[idx];
			/* Handle incoming packet */
			if (events[i].events & EPOLLIN) {
				// read into the connection buffer
                ret = SSL_read(conn->ssl, &conn->conn.buffer[conn->conn.buffer_idx],
                    MAX_PAYLOAD - conn->conn.buffer_idx);
				if (ret <= 0) {
					int ssl_err = SSL_get_error(conn->ssl, ret);
					if (ssl_err == SSL_ERROR_WANT_READ)
						continue;

					if (ret == 0) {
						SSL_shutdown(conn->ssl);
						SSL_free(conn->ssl);
						close(conn->conn.fd);
						conn->conn.closed = 1;
						continue;
					}

					lancet_fprintf(std::cerr, "Unexpected SSL error %d\n",
								   ssl_err);
					return;
				}

				time_ns_to_ts(&rx_timestamp);
				conn->conn.buffer_idx += ret;
				read_res = handle_response(&conn->conn);
				if (read_res.reqs == 0)
					continue;

				// No need for assert because it's uint64
				conn->conn.pending_reqs -= read_res.reqs;
				/*
				 * Assume only the last request will have an rx timestamp!
				 */
				for (j = 0; j < read_res.reqs; j++) {
					pending_tx = pop_pending_tx_timestamps(
						&per_conn_tx_timestamps[conn->conn.idx]);
					// if (!pending_tx) {
					// 	ret = get_tx_timestamp(
					// 		conn->conn.fd, &per_conn_tx_timestamps[conn->conn.idx]);
					// 	while (ret != 1)
					// 		ret = get_tx_timestamp(
					// 			conn->conn.fd, &per_conn_tx_timestamps[conn->conn.idx]);
					// 	assert(ret == 1);
					// 	assert(per_conn_tx_timestamps[conn->conn.idx].consumed <
					// 		   per_conn_tx_timestamps[conn->conn.idx].tail);
					// 	pending_tx = pop_pending_tx_timestamps(
					// 		&per_conn_tx_timestamps[conn->conn.idx]);
					// 	assert(pending_tx);
					// }
				}
				ret = timespec_diff(&latency, &rx_timestamp, &pending_tx->time);
				assert(ret == 0);
				long diff = latency.tv_nsec + latency.tv_sec * 1e9;
				add_latency_sample(diff, &pending_tx->time);

				/* Bookkeeping */
				struct byte_req_pair byte_res = {read_res.bytes, read_res.reqs};
				add_throughput_rx_sample(byte_res);
			} 
		}
	}
}

static void symmetric_mapping_ssl_main(int thread_idx)
{
	int ready, idx, i, j, conn_per_thread, ret, bytes_to_send;
	long next_tx, copied;
	std::vector<epoll_event> events;
	struct tls_connection *conn;
	struct request *to_send;
	struct consume_resp_pair read_res;
	struct byte_req_pair send_res;
	struct timespec tx_timestamp, rx_timestamp, latency;
	struct timestamp_info *pending_tx;
    char *wbuf;
	uint64_t wbuf_size = 512;

	wbuf = (char *)malloc(wbuf_size);
	if (throughput_open_connections())
		return;

	/*Initializations*/
	conn_per_thread = get_conn_count() / get_thread_count();
	events.resize(conn_per_thread);

	pthread_barrier_wait(&conn_open_barrier);
	set_conn_open(1);

	next_tx = time_ns();
	while (1) {
		if (!should_load()) {
			next_tx = time_ns();
			continue;
		}
		while (time_ns() >= next_tx) {
			conn = pick_conn();
			if (!conn)
				goto REP_PROC;
			to_send = prepare_request_id(conn->conn.rpc_id);
			conn->conn.rpc_id++;

            bytes_to_send = 0;
			for (i = 0; i < to_send->iov_cnt; i++)
				bytes_to_send += to_send->iovs[i].iov_len;

			if (bytes_to_send > wbuf_size) {
				free(wbuf);
				wbuf = (char *)malloc(bytes_to_send);
				wbuf_size = bytes_to_send;
			}

			copied = 0;
			for (i = 0; i < to_send->iov_cnt; i++) {
				memcpy(&wbuf[copied], to_send->iovs[i].iov_base,
					   to_send->iovs[i].iov_len);
				copied += to_send->iovs[i].iov_len;
			}
			// assert(copied == bytes_to_send);

			int ret = SSL_write(conn->ssl, wbuf, bytes_to_send);

			time_ns_to_ts(&tx_timestamp);
			push_tx_timestamp_map(to_send->id,
								  per_conn_tx_timestamps_map[conn->conn.idx],
								  &tx_timestamp);

			conn->conn.pending_reqs++;

			/*BookKeeping*/
			send_res.bytes = ret;
			send_res.reqs = 1;
			add_throughput_tx_sample(send_res);

			/*Schedule next*/
			next_tx += get_ia();
		}
	REP_PROC:
		/* process responses */
		ready = epoll_wait(epoll_fd, events.data(), conn_per_thread, 0);
		// lancet_fprintf(std::cerr, "ready for REP_PROC %d\n", ready);
		for (i = 0; i < ready; i++) {
			idx = events[i].data.u32;
			conn = &connections[idx];
			/* Handle incoming packet */
			if (events[i].events & EPOLLIN) {
				// lancet_fprintf(std::cerr, "read into the connection buffer\n", ready);
				// read into the connection buffer
				ret = SSL_read(conn->ssl,
							   &conn->conn.buffer[conn->conn.buffer_idx],
							   MAX_PAYLOAD - conn->conn.buffer_idx);
				if (ret <= 0) {
					int ssl_err = SSL_get_error(conn->ssl, ret);
					if (ssl_err == SSL_ERROR_WANT_READ)
						continue;

					if (ret == 0) {
						SSL_shutdown(conn->ssl);
						SSL_free(conn->ssl);
						close(conn->conn.fd);
						conn->conn.closed = 1;
						continue;
					}

					lancet_fprintf(std::cerr, "Unexpected SSL error %d\n",
								   ssl_err);
					return;
				}

				// record cur time for reponse receiving
				time_ns_to_ts(&rx_timestamp);

				conn->conn.buffer_idx += ret;
				read_res = handle_response(&conn->conn);
				if (read_res.reqs == 0)
					continue;

				// No need for assert because it's uint64
				conn->conn.pending_reqs -= read_res.reqs;
				for (j = 0; j < read_res.reqs; j++) {
					pending_tx = pop_pending_tx_timestamps_map(
						read_res.ids->at(j),
						per_conn_tx_timestamps_map[conn->conn.idx]);

					// lancet_fprintf(
					// std::cerr,
					// "receiving response with memcache-id is %u\n",
					// read_res.ids->at(j));
					// lancet_fprintf(std::cerr, "response at %ld, %ld\n",
					// rx_timestamp.tv_sec, rx_timestamp.tv_nsec);
					// lancet_fprintf(std::cerr, "response at %ld, %ld\n",
					// rx_timestamp.tv_sec, rx_timestamp.tv_nsec);

					ret = timespec_diff(&latency, &rx_timestamp,
										&pending_tx->time);
					assert(ret == 0);
					long diff = latency.tv_nsec + latency.tv_sec * 1e9;
					if (diff < 0) {
						lancet_fprintf(std::cerr, "request %u at %ld, %ld\n",
									   read_res.ids->at(j),
									   pending_tx->time.tv_sec,
									   pending_tx->time.tv_nsec);
						lancet_fprintf(std::cerr, "response %u at %ld, %ld\n",
									   read_res.ids->at(j), rx_timestamp.tv_sec,
									   rx_timestamp.tv_nsec);
					}
					add_latency_sample(diff, &pending_tx->time);
				}

				/* Bookkeeping */
				struct byte_req_pair byte_res = {read_res.bytes, read_res.reqs};
				add_throughput_rx_sample(byte_res);

				// free read_res->ids
				delete read_res.ids;
				read_res.ids = nullptr;
			} else
				assert(0);
		}
	}
}

static void throughput_ssl_main(int thread_idx)
{
	lancet_fprintf(std::cerr, "throughput_ssl_main not implemented\n");
	assert(0);
}

static void latency_ssl_main(int thread_idx)
{
	lancet_fprintf(std::cerr, "latency_ssl_main not implemented\n");
	assert(0);
}

static void symmetric_nic_ssl_main(int thread_idx)
{
	lancet_fprintf(std::cerr, "symmetric_nic_ssl_main not implemented\n");
	assert(0);
}

struct transport_protocol *init_tls(void)
{
	struct transport_protocol *tp;
	tp = new struct transport_protocol;
	lancet_fprintf(std::cerr, "Successfully alloc transport protocol\n");
	if (!tp) {
		lancet_fprintf(std::cerr, "Failed to alloc transport_protocol\n");
		return NULL;
	}

	tp->tp_main[THROUGHPUT_AGENT] = throughput_ssl_main;
	tp->tp_main[LATENCY_AGENT] = latency_ssl_main;
	tp->tp_main[SYMMETRIC_NIC_TIMESTAMP_AGENT] = symmetric_nic_ssl_main;
	tp->tp_main[SYMMETRIC_AGENT] = symmetric_ssl_main;
	tp->tp_main[SYMMETRIC_MAPPING_AGENT] = symmetric_mapping_ssl_main;

	if (ssl_init()) {
		lancet_fprintf(std::cerr, "Failed to initate TLS\n");
		return NULL;
	}

	return tp;
}
