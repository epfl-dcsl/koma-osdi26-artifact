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
/*
 * Interface for the communication protocol e.g. TCP, UDP, R2P2 etc
 */
#pragma once

#include <lancet/agent.hpp>
#include <lancet/stats.hpp>
#include <openssl/ssl.h>
#include <cstdlib>
#include <time.h>
#include <mutex>

#include <grpc/support/log.h>
#include <grpcpp/grpcpp.h>
#include "echo.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using echo::Echo;
using echo::EchoRequest;
using echo::EchoResponse;

struct transport_protocol {
	void (*tp_main[AGENT_NR])(int);
};

struct transport_protocol *init_tcp(void);
struct transport_protocol *init_grpc(void);
struct transport_protocol *init_grpc_go(void);
struct transport_protocol *init_tls(void);
/*
 * TCP specific
 */
#define MAX_PAYLOAD 16384
struct tcp_connection {
	uint32_t fd;
	uint16_t idx;
	uint16_t closed;
	uint16_t pending_reqs;
	uint16_t buffer_idx;
	char buffer[MAX_PAYLOAD];
	uint32_t rpc_id; // id of rpc (either request/response)
};
struct consume_resp_pair handle_response(struct tcp_connection *conn);

/*
 * gRPC specific
 */

class EchoClient
{
  public:
	explicit EchoClient(std::shared_ptr<grpc::Channel> channel, int idx,
						CompletionQueue *cq);
	~EchoClient(); // Destructor to ensure proper cleanup

	void SendEcho(const std::string &user);
	std::string SyncSendEcho(const std::string &user);
	static void AsyncCompleteRpc(CompletionQueue *cq, union stats *thread_stats,
								 struct grpc_throu_pending *throu_pending);

  private:
	// Nested struct for keeping state and data information
	struct AsyncClientCall;

	// Unique pointer to the gRPC stub
	std::unique_ptr<Echo::Stub> stub_;
	int idx_;
	CompletionQueue *cq_;
};

struct grpc_connection {
	EchoClient *greeter;
	uint16_t idx;
	uint16_t closed;
	uint16_t pending_reqs; // only used by latency
	uint16_t buffer_idx;
};

// only for grpc throughput pending requests
struct grpc_throu_pending {
	std::atomic<uint16_t>* reqs;
};

/*
 * TLS specific
 */
struct tls_connection {
	struct tcp_connection conn;
	SSL *ssl;
};
