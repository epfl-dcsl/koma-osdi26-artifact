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
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

#include <bits/getopt_core.h>
#include <lancet/agent.hpp>
#include <lancet/app_proto.hpp>
#include <lancet/error.hpp>
#include <lancet/rand_gen.hpp>
#include <lancet/tp_proto.hpp>

static struct transport_protocol *
init_transport_protocol(enum transport_protocol_type tp_type)
{
	struct transport_protocol *res;

	switch (tp_type) {
	case TCP:
		res = init_tcp();
		break;
		// 	case UDP:
		// 		res = init_udp();
		// 		break;
		// 	case TLS:
		// 		res = init_tls();
		// 		break;
		// #ifdef ENABLE_R2P2
		// 	case R2P2:
		// 		res = init_r2p2();
		// 		break;
		// #endif
	case GRPC:
		res = init_grpc();
		break;
	case GRPC_GO:
		res = init_grpc_go();
		break;
		//
	case TLS:
		res = init_tls();
		break;
	default:
		res = NULL;
		break;
	}
	return res;
}

struct agent_config *parse_arguments(int argc, char **argv)
{
	int c, agent_type;
	struct agent_config *cfg;
	char *token1, *token2;
	struct sockaddr_in sa;
	// char proto[128];

	cfg = new struct agent_config;
	cfg->targets.resize(8192); // Resize the vector to have a size of 8192
	cfg->target_count = 0;	   // Initialize target_count to zero

	if (!cfg) {
		lancet_fprintf(std::cerr, "Failed to allocate cfg\n");
		return NULL;
	}

	while ((c = getopt(argc, argv, "t:s:c:a:p:i:r:n:o:w:")) != -1) {
		switch (c) {
		case 't':
			// Thread count
			cfg->thread_count = std::atoi(optarg);
			break;
		case 's':
			// Targets ip:port,ip:port
			token1 = strtok_r(optarg, ",", &optarg);
			while (token1) {
				/* Prepare the target */
				token2 = strtok_r(token1, ":", &token1);
				inet_pton(AF_INET, token2, &(sa.sin_addr));
				cfg->targets[cfg->target_count].ip = sa.sin_addr.s_addr;
				token2 = strtok_r(token1, ":", &token1);
				cfg->targets[cfg->target_count++].port = atoi(token2);

				assert(cfg->target_count < 64);
				token1 = strtok_r(optarg, ",", &optarg);
			}

			break;
		case 'c':
			// Connection count
			cfg->conn_count = std::atoi(optarg);
			break;
		case 'a':
			// Agent type
			agent_type = atoi(optarg);
			if (agent_type == THROUGHPUT_AGENT)
				cfg->atype = THROUGHPUT_AGENT;
			else if (agent_type == LATENCY_AGENT)
				cfg->atype = LATENCY_AGENT;
			else if (agent_type == SYMMETRIC_NIC_TIMESTAMP_AGENT)
				cfg->atype = SYMMETRIC_NIC_TIMESTAMP_AGENT;
			else if (agent_type == SYMMETRIC_AGENT)
				cfg->atype = SYMMETRIC_AGENT;
			else if (agent_type == SYMMETRIC_MAPPING_AGENT)
				cfg->atype = SYMMETRIC_MAPPING_AGENT;

			else {
				lancet_fprintf(std::cerr, "Unknown agent type\n");
				return NULL;
			}
			break;
		case 'p':
			// Communication protocol
			if (std::strcmp(optarg, "TCP") == 0)
				cfg->tp_type = TCP;
			else if (std::strcmp(optarg, "GRPC") == 0)
				cfg->tp_type = GRPC;
			// #ifdef ENABLE_R2P2
			// 			else if (strcmp(optarg, "R2P2") == 0)
			// 				cfg->tp_type = R2P2;
			// #endif
			// 			else if (strcmp(optarg, "UDP") == 0)
			// 				cfg->tp_type = UDP;
			else if (std::strcmp(optarg, "GRPC_GO") == 0)
				cfg->tp_type = GRPC_GO;
			else if (strcmp(optarg, "TLS") == 0)
				cfg->tp_type = TLS;
			else {
				lancet_fprintf(std::cerr, "Unknown transport protocol\n");
				return NULL;
			}
			break;
		case 'i':
			// Interarrival distribution
			cfg->idist = init_rand(optarg);
			if (!cfg->idist) {
				lancet_fprintf(std::cerr, "Failed to create iadist\n");
				return NULL;
			}
			break;
		case 'r':
			// Application protocol (request response types)
			cfg->app_proto = init_app_proto(optarg);
			if (!cfg->app_proto) {
				lancet_fprintf(std::cerr, "Failed to create app proto\n");
				return NULL;
			}
			break;
		case 'n':
			cfg->if_name.assign(optarg, 0, 64);
			break;
		case 'o':
			cfg->per_conn_reqs = std::atoi(optarg);
			break;
		case 'w':
			cfg->workload = optarg;
			break;
		default:
			lancet_fprintf(std::cerr, "Unknown argument\n");
			abort();
		}
	}

	if (cfg->app_proto && cfg->app_proto->type == PROTO_ETCD) {
		if (cfg->tp_type != GRPC_GO) {
			lancet_fprintf(std::cerr,
						   "Application protocol etcd requires transport GRPC_GO\n");
			return NULL;
		}
		if (cfg->workload.empty())
			cfg->workload = "ycsb:workloada";
	} else if (!cfg->workload.empty()) {
		lancet_fprintf(std::cerr,
					   "Workload selection is only supported with application protocol etcd\n");
		return NULL;
	}

	if (!cfg->workload.empty()) {
		if (cfg->workload.rfind("ycsb:", 0) != 0) {
			lancet_fprintf(std::cerr, "Workload must use ycsb:<name> syntax\n");
			return NULL;
		}
		std::string workloadName = cfg->workload.substr(5);
		if (workloadName.empty()) {
			lancet_fprintf(std::cerr, "Workload name cannot be empty\n");
			return NULL;
		}
		if (workloadName.find_first_of("/\\") != std::string::npos) {
			lancet_fprintf(std::cerr,
						   "Workload must be a named built-in workload, not a path\n");
			return NULL;
		}
	}
#ifdef ENABLE_R2P2
	// Generators interfacing with R2P2 must use host endianness (except
	// latency)
	if (cfg->tp_type == R2P2 && cfg->atype != LATENCY_AGENT) {
		for (int i = 0; i < cfg->target_count; i++)
			cfg->targets[i].ip = ntohl(cfg->targets[i].ip);
	}
#endif
	cfg->tp = init_transport_protocol(cfg->tp_type);
	if (!cfg->tp) {
		lancet_fprintf(std::cerr, "Failed to init transport\n");
		return NULL;
	}
	return cfg;
}
