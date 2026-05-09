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

#include <cassert>
#include <cstdint>
#include <vector>
#include <memory>

#include <lancet/rand_gen.hpp>

#define MAX_PER_THREAD_SAMPLES 131072
#define MAX_PER_THREAD_TX_SAMPLES 4096

struct byte_req_pair {
	std::uint64_t bytes;
	std::uint64_t reqs;
};

struct consume_resp_pair {
	std::uint64_t bytes;
	std::uint64_t reqs;
	std::vector<uint32_t> *ids;
};

struct tx_samples {
	std::uint32_t count;
	struct timespec samples[MAX_PER_THREAD_SAMPLES];
};

struct throughput_stats {
	struct byte_req_pair rx;
	struct byte_req_pair tx;
};

struct lat_sample {
	std::uint64_t nsec;
	struct timespec tx; // used for iid-ness checks
};

struct latency_stats {
	struct throughput_stats th_s;
	std::uint32_t inc_idx; // increment idx
	struct lat_sample samples[MAX_PER_THREAD_SAMPLES];
};

union stats {
	struct throughput_stats th_s;
	struct latency_stats lt_s;
};

int init_per_thread_stats(void);
int add_throughput_tx_sample(struct byte_req_pair tx_p);
int add_throughput_rx_sample(struct byte_req_pair rx_p);
int add_tx_timestamp(struct timespec *tx_ts);
int add_latency_sample(long diff, struct timespec *tx);
int get_thread_stats(union stats **stats, struct tx_samples **tx_samples);