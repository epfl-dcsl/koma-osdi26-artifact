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
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <lancet/app_proto.hpp>
#include <lancet/rand_gen.hpp>

#define MAX_THREADS 16

struct host_tuple {
	std::uint32_t ip;
	std::uint16_t port;
};

enum agent_type {
	THROUGHPUT_AGENT,
	LATENCY_AGENT,
	SYMMETRIC_NIC_TIMESTAMP_AGENT,
	SYMMETRIC_AGENT,
	SYMMETRIC_MAPPING_AGENT,
	AGENT_NR,
};

enum transport_protocol_type {
	TCP,
	R2P2,
	UDP,
	TLS,
	GRPC,
	GRPC_GO,
};

struct agent_config {
	std::int32_t thread_count;
	std::int32_t conn_count;
	std::vector<host_tuple> targets;
	std::int32_t target_count;
	agent_type atype;
	transport_protocol_type tp_type;
	struct transport_protocol *tp;
	struct rand_gen *idist;
	struct application_protocol *app_proto;
	std::string workload;
	std::string if_name;
	std::int32_t per_conn_reqs;
};

struct __attribute__((packed)) agent_control_block {
	struct rand_gen idist; // it should be the first field
	std::int32_t should_load;
	std::int32_t should_measure;
	std::int32_t thread_count;
	agent_type atype;
	std::uint32_t per_thread_samples;
	double sampling;
	std::int32_t conn_open;
};

void get_acb(struct agent_control_block **out_acb);
int should_load(void);
int should_measure(void);
struct agent_config *parse_arguments(int argc, char **argv);
std::int32_t get_conn_count();
std::int32_t get_thread_count();
std::int32_t get_target_count();
struct application_protocol *get_app_proto(void);
std::vector<host_tuple> get_targets(void);
long get_ia(void);
agent_type get_agent_type(void);
std::int32_t get_agent_tid(void);
std::uint32_t get_per_thread_samples(void);
double get_sampling_rate(void);
std::string get_if_name(void);
std::string get_workload(void);
std::int32_t get_max_pending_reqs(void);
void set_conn_open(int val);
int get_echo_msg_size(void);
struct request *prepare_request(void);
struct request *prepare_request_id(std::uint32_t rpc_id);
struct consume_resp_pair process_response(char *buf, int size);

extern pthread_barrier_t conn_open_barrier;
