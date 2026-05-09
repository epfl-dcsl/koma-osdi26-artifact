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
#include <cstring>
#include <arpa/inet.h>
#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <lancet/error.hpp>
#include <lancet/libecho.h>
#include <lancet/tp_proto.hpp>

thread_local std::uint32_t thread_idx = 0;

static std::string grpc_go_app_proto_name()
{
	switch (get_app_proto()->type) {
	case PROTO_ECHO:
		return "echo";
	case PROTO_ETCD:
		return "etcd";
	default:
		return "";
	}
}

static std::string grpc_go_targets_csv()
{
	std::ostringstream joined;
	std::vector<host_tuple> targets = get_targets();

	for (int i = 0; i < get_target_count(); i++) {
		if (i)
			joined << ",";
		std::string address =
			targets[i].ip == 0 ? "localhost"
							   : inet_ntoa(*(struct in_addr *)&targets[i].ip);
		joined << address << ":" << targets[i].port;
	}
	return joined.str();
}

static int grpc_go_init_thread_context(void)
{
	int per_thread_conn;
	union stats *thread_stats;
	struct tx_samples *tx_samples;
	struct agent_control_block *acb;
	std::string targets_csv;
	std::string app_proto_name;
	std::string workload;

	per_thread_conn = get_conn_count() / get_thread_count();
	assert(per_thread_conn > 0);

	targets_csv = grpc_go_targets_csv();
	app_proto_name = grpc_go_app_proto_name();
	workload = get_workload();

	get_thread_stats(&thread_stats, &tx_samples);
	get_acb(&acb);

	if (app_proto_name.empty()) {
		lancet_fprintf(std::cerr,
					   "GRPC_GO transport does not support this app proto yet\n");
		return -1;
	}

	lancet_fprintf(std::cerr, "start initThreadContext %d\n", thread_idx);
	if (initThreadContext(thread_idx, per_thread_conn,
						  (char *)targets_csv.c_str(),
						  (char *)app_proto_name.c_str(),
						  (char *)workload.c_str(),
						  (uintptr_t)thread_stats, (uintptr_t)tx_samples,
						  (uintptr_t)acb, get_max_pending_reqs(),
						  get_echo_msg_size(), get_agent_type())) {
		lancet_fprintf(std::cerr, "initThreadContext failed %d\n", thread_idx);
		return -1;
	}
	lancet_fprintf(std::cerr, "end initThreadContext %d\n", thread_idx);
	return 0;
}

static void symmetric_grpc_go_main(int tid)
{
	lancet_fprintf(std::cerr, "start symmetric_grpc_go_main %d\n", tid);
	thread_idx = tid;
	if (grpc_go_init_thread_context())
		return;
	lancet_fprintf(std::cerr, "end grpc_go_init_thread_context %d\n", tid);
	pthread_barrier_wait(&conn_open_barrier);
	set_conn_open(1);
	lancet_fprintf(std::cerr, "start symmetricGRPCGOMain %d\n", tid);
	symmetricGRPCGoMain(thread_idx);
}

static void throughput_grpc_go_main(int tid)
{
	lancet_fprintf(std::cerr, "start throughput_grpc_go_main %d\n", tid);
	thread_idx = tid;
	if (grpc_go_init_thread_context())
		return;
	pthread_barrier_wait(&conn_open_barrier);
	set_conn_open(1);
	throughputGRPCGoMain(thread_idx);
}

static void latency_grpc_go_main(int tid)
{
	lancet_fprintf(std::cerr, "start latency_grpc_go_main %d\n", tid);
	thread_idx = tid;
	if (grpc_go_init_thread_context())
		return;
	pthread_barrier_wait(&conn_open_barrier);
	set_conn_open(1);
	latencyGRPCGoMain(thread_idx);
}

struct transport_protocol *init_grpc_go(void)
{
	struct transport_protocol *tp;
	tp = new struct transport_protocol;
	lancet_fprintf(std::cerr, "Successfully alloc transport protocol\n");
	if (!tp) {
		lancet_fprintf(std::cerr, "Failed to alloc transport_protocol\n");
		return NULL;
	}
	tp->tp_main[THROUGHPUT_AGENT] = throughput_grpc_go_main;
	tp->tp_main[LATENCY_AGENT] = latency_grpc_go_main;
	tp->tp_main[SYMMETRIC_AGENT] = symmetric_grpc_go_main;
	return tp;
}
