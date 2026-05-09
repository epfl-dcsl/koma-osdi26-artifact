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
#include <arpa/inet.h>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <lancet/error.hpp>
#include <lancet/misc.hpp>
#include <lancet/timestamping.hpp>
#include <lancet/tp_proto.hpp>

// extern thread_local union stats *thread_stats;
thread_local std::vector<grpc_connection> connections;
// thread_local std::vector<pending_tx_timestamps> per_conn_tx_timestamps;

EchoClient::EchoClient(std::shared_ptr<Channel> channel, int idx,
					   CompletionQueue *cq = nullptr)
	: stub_(Echo::NewStub(channel)), idx_(idx), cq_(cq)
{
}

struct EchoClient::AsyncClientCall {
	// Container for the data we expect from the server.
	EchoResponse reply;

	// Context for the client. It could be used to convey extra information to
	// the server and/or tweak certain RPC behaviors.
	ClientContext context;
	int client_idx;
	// Storage for the status of the RPC upon completion.
	Status status;
	std::unique_ptr<ClientAsyncResponseReader<EchoResponse>> response_reader;
};

void EchoClient::SendEcho(const std::string &user)
{
	// Data we are sending to the server.
	EchoRequest request;
	request.set_message(user);

	// Call object to store rpc data
	AsyncClientCall *call = new AsyncClientCall;

	// stub_->PrepareAsyncSayHello() creates an RPC object, returning
	// an instance to store in "call" but does not actually start the RPC
	// Because we are using the asynchronous API, we need to hold on to
	// the "call" instance in order to get updates on the ongoing RPC.
	call->response_reader =
		stub_->PrepareAsyncSendEcho(&call->context, request, cq_);

	call->client_idx = idx_;
	// StartCall initiates the RPC call
	call->response_reader->StartCall();
	// Request that, upon completion of the RPC, "reply" be updated with the
	// server's response; "status" with the indication of whether the operation
	// was successful. Tag the request with the memory address of the call
	// object.
	call->response_reader->Finish(&call->reply, &call->status, (void *)call);
}

// sync echo request
std::string EchoClient::SyncSendEcho(const std::string &user)
{
	// Data we are sending to the server.
	EchoRequest request;
	request.set_message(user);

	// Container for the data we expect from the server.
	EchoResponse reply;

	// Context for the client. It could be used to convey extra information to
	// the server and/or tweak certain RPC behaviors.
	ClientContext context;

	// The actual RPC.
	Status status = stub_->SendEcho(&context, request, &reply);

	// Act upon its status.
	if (status.ok()) {
		return reply.message();
	} else {
		std::cout << status.error_code() << ": " << status.error_message()
				  << std::endl;
		return "RPC failed";
	}
}

// Loop while listening for completed responses.
// Prints out the response from the server.
void EchoClient::AsyncCompleteRpc(CompletionQueue *cq,
								  union stats *t_stats, struct grpc_throu_pending *throu_pending)
{
	void *got_tag;
	bool ok = false;
	struct byte_req_pair read_res;

	// Block until the next result is available in the completion queue "cq".
	while ((*cq).Next(&got_tag, &ok)) {
		// The tag in this example is the memory location of the call object
		AsyncClientCall *call = static_cast<AsyncClientCall *>(got_tag);

		// Verify that the request was completed successfully. Note that "ok"
		// corresponds solely to the request for updates introduced by Finish().
		// CHECK(ok);

		if (call->status.ok()) {
			// lancet_fprintf(std::cerr, "Greeter Received: %s from client
			// %d\n", call->reply.message(), call->client_idx); add lock
			throu_pending->reqs[call->client_idx].fetch_sub(1);

			read_res.bytes = call->reply.message().length();
			read_res.reqs = 1;
			if (!should_measure())
				continue;
			t_stats->th_s.rx.bytes += read_res.bytes;
			t_stats->th_s.rx.reqs += read_res.reqs;
			// add_throughput_rx_sample(read_res);
		} else
			std::cout << "RPC failed" << std::endl;

		// Once we're complete, deallocate the call object.
		delete call;
	}
}

static inline struct grpc_connection *pick_conn()
{
	int idx;
	struct grpc_connection *c;

	// FIXME: Consider picking connection round robin
	// idx = rand() % (get_conn_count() / get_thread_count()) ;
	idx = rand() % (get_conn_count() / get_thread_count());
	c = &connections[idx];
	if ((c->pending_reqs < get_max_pending_reqs()) && (!c->closed)) {
		// lancet_fprintf(std::cerr, "return grpc connection!  \
		// pending rquest num is (%d), connect closed state is (%d)\n",
		// 		   c->pending_reqs, c->closed);
		return c;
	}
	// lancet_fprintf(std::cerr, "could not return grpc connection!  \
	// 	pending rquest num is (%d), connect closed state is (%d)\n",
	// 			   c->pending_reqs, c->closed);
	return NULL;
}


static inline struct grpc_connection *throu_pick_conn(std::atomic<uint16_t> *throu_pending_reqs)
{
	int idx;
	struct grpc_connection *c;

	// FIXME: Consider picking connection round robin
	// idx = rand() % (get_conn_count() / get_thread_count()) ;
	idx = rand() % (get_conn_count() / get_thread_count());
	c = &connections[idx];
	if ((throu_pending_reqs[idx] < get_max_pending_reqs()) && (!c->closed)) {
		// lancet_fprintf(std::cerr, "return grpc connection!  \
		// pending rquest num is (%d), connect closed state is (%d)\n",
		// 		   c->pending_reqs, c->closed);
		return c;
	}
	// lancet_fprintf(std::cerr, "could not return grpc connection!  \
	// 	pending rquest num is (%d), connect closed state is (%d)\n",
	// 			   c->pending_reqs, c->closed);
	return NULL;
}


static int latency_open_connections()
{
	int i, ret, sock, per_thread_conn, million = 1e6, one = 1, dest_idx;
	std::vector<host_tuple> targets;

	per_thread_conn = get_conn_count() / get_thread_count();
	connections.resize(per_thread_conn);
	targets = get_targets();
	for (i = 0; i < per_thread_conn; i++) {
		dest_idx = i % get_target_count();
		// Convert the code to create a gRPC channel
		std::string address =
			targets[dest_idx].ip == 0
				? "localhost"
				: inet_ntoa(*(struct in_addr *)&targets[dest_idx].ip);
		std::string port_str = std::to_string(targets[dest_idx].port);
		std::string target_address = address + ":" + port_str;
		// here create grpc channel
		connections[i].greeter = new EchoClient(
			grpc::CreateChannel(target_address,
								grpc::InsecureChannelCredentials()),
			i);
		connections[i].idx = i;
		connections[i].buffer_idx = 0;
		connections[i].closed = 0;

		// TODO: Need to find a way to enable busy polling
	}
	return 0;
}

static int throughput_open_connections(CompletionQueue *cq)
{
	/*init epoll*/
	struct sockaddr_in addr;
	int i, efd, ret, sock, per_thread_conn, dest_idx, n;
	std::vector<host_tuple> targets;

	efd = epoll_create(1);
	if (efd < 0) {
		lancet_perror("epoll_create error");
		return -1;
	}

	per_thread_conn = get_conn_count() / get_thread_count();
	connections.resize(per_thread_conn);

	assert(connections.size());

	targets = get_targets();

	grpc::ChannelArguments args;  
	args.SetInt(GRPC_ARG_HTTP2_HPACK_TABLE_SIZE_DECODER, 0);  
	args.SetInt(GRPC_ARG_HTTP2_HPACK_TABLE_SIZE_ENCODER, 0);  

	for (i = 0; i < per_thread_conn; i++) {
		dest_idx = i % get_target_count();
		// Convert the code to create a gRPC channel
		std::string address =
			targets[dest_idx].ip == 0
				? "localhost"
				: inet_ntoa(*(struct in_addr *)&targets[dest_idx].ip);
		std::string port_str = std::to_string(targets[dest_idx].port);
		std::string target_address = address + ":" + port_str;

		// here create grpc channel
		connections[i].greeter = new EchoClient(
			grpc::CreateCustomChannel(target_address,
								grpc::InsecureChannelCredentials(), args),
			i, cq);
		connections[i].idx = i;
		connections[i].buffer_idx = 0;
		connections[i].closed = 0;
	}
	return 0;
}

static void latency_grpc_main(int thread_idx)
{
	int i, ret, bytes_to_send, offset;
	long start_time, end_time, next_tx;
	struct grpc_connection *conn;
	struct request *to_send;
	struct byte_req_pair read_res;
	struct byte_req_pair send_res;

	if (latency_open_connections())
		exit(-1);
	lancet_fprintf(std::cerr, "start latency grpc main\n");
	next_tx = time_ns();
	while (1) {
		if (!should_load()) {
			// lancet_fprintf(std::cerr, "Shouldnt load\n");
			next_tx = time_ns();
			continue;
		}
		if (time_ns() < next_tx) {
			// lancet_fprintf(std::cerr, "time next tx\n");
			continue;
		}
		conn = pick_conn();
		if (!conn) {
			// lancet_fprintf(std::cerr, "pick no conn\n");
			continue;
		}
		// lancet_fprintf(std::cerr, "Preparing sending latency grpc\n");
		// construct echo request
		to_send = prepare_request();
		bytes_to_send = 0;
		offset = 0;
		for (i = 0; i < to_send->iov_cnt; i++) {
			bytes_to_send += to_send->iovs[i].iov_len;
		}
		std::vector<char> request_buf(bytes_to_send + 1);
		for (i = 0; i < to_send->iov_cnt; i++) {
			memcpy(request_buf.data() + offset, to_send->iovs[i].iov_base,
				   to_send->iovs[i].iov_len);
			offset += to_send->iovs[i].iov_len;
		}
		request_buf[bytes_to_send] = '\0';
		std::string request_str(request_buf.data());
		start_time = time_ns();
		// Send echo request;
		send_res.bytes = request_str.length();
		send_res.reqs = 1;
		add_throughput_tx_sample(send_res);
		// lancet_fprintf(std::cerr, "Sending echo request(%s)\n", request_str);
		std::string reply = conn->greeter->SyncSendEcho(request_str);

		end_time = time_ns();
		/*BookKeeping*/
		read_res.bytes = reply.length();
		read_res.reqs = 1;
		add_throughput_rx_sample(read_res);
		add_latency_sample((end_time - start_time), NULL);

		/*Schedule next*/
		next_tx += get_ia();
	}
}

static void throughput_grpc_main(int thread_idx)
{
	int ready, idx, i, conn_per_thread, ret, bytes_to_send, offset;
	long next_tx;
	std::vector<struct epoll_event> events;
	struct grpc_connection *conn;
	struct request *to_send;
	struct byte_req_pair read_res;
	struct byte_req_pair send_res;
	struct timespec tx_timestamp;
	int start_iov;

	struct grpc_throu_pending throu_pending;

	CompletionQueue cq;
	// std::vector<uint16_t> throu_pending_reqs;
	extern thread_local union stats *thread_stats;

	lancet_fprintf(std::cerr, "start throughput_grpc_main\n");
	if (throughput_open_connections(&cq))
		return;
	conn_per_thread = get_conn_count() / get_thread_count();
	
	throu_pending.reqs = new std::atomic<uint16_t>[conn_per_thread];
	for (size_t i = 0; i < conn_per_thread; ++i) {
		throu_pending.reqs[i].store(0);
	}
	pthread_barrier_wait(&conn_open_barrier);
	set_conn_open(1);
	next_tx = time_ns();

	// create thread for polling responses
	std::thread thread_ =
		std::thread(&EchoClient::AsyncCompleteRpc, &cq, thread_stats, &throu_pending);
	while (1) {
		if (!should_load()) {
			next_tx = time_ns();
			// lancet_fprintf(std::cerr, "Shouldnt load\n");
			continue;
		}
		while (time_ns() >= next_tx) {
			conn = throu_pick_conn(throu_pending.reqs);
			if (!conn) {
				// lancet_fprintf(std::cerr, "pick no conn\n");
				continue;
			}

			// construct echo request
			to_send = prepare_request();
			bytes_to_send = 0;
			offset = 0;
			for (i = 0; i < to_send->iov_cnt; i++) {
				bytes_to_send += to_send->iovs[i].iov_len;
			}
			std::vector<char> request_buf(bytes_to_send + 1);
			for (i = 0; i < to_send->iov_cnt; i++) {
				memcpy(request_buf.data() + offset, to_send->iovs[i].iov_base,
					   to_send->iovs[i].iov_len);
				offset += to_send->iovs[i].iov_len;
			}
			request_buf[bytes_to_send] = '\0';
			std::string request_str(request_buf.data());

			// Send echo request;

			// lancet_fprintf(std::cerr, "start sending greeter from client
			// %d\n", 			   conn->idx);
			conn->greeter->SendEcho(request_str);

			throu_pending.reqs[conn->idx].fetch_add(1);

			time_ns_to_ts(&tx_timestamp);
			add_tx_timestamp(&tx_timestamp);

			/*BookKeeping*/
			send_res.bytes = bytes_to_send;
			send_res.reqs = 1;
			add_throughput_tx_sample(send_res);

			/*Schedule next*/
			next_tx += get_ia();
		}
	}
	thread_.join(); // blocks forever
	return;
}

struct transport_protocol *init_grpc(void)
{

	struct transport_protocol *tp;
	tp = new struct transport_protocol;
	lancet_fprintf(std::cerr, "Successfully alloc transport protocol\n");
	if (!tp) {
		lancet_fprintf(std::cerr, "Failed to alloc transport_protocol\n");
		return NULL;
	}
	tp->tp_main[THROUGHPUT_AGENT] = throughput_grpc_main;
	tp->tp_main[LATENCY_AGENT] = latency_grpc_main;
	// tp->tp_main[SYMMETRIC_NIC_TIMESTAMP_AGENT] = symmetric_nic_grpc_main;
	// tp->tp_main[SYMMETRIC_AGENT] = symmetric_grpc_main;

	return tp;
}
