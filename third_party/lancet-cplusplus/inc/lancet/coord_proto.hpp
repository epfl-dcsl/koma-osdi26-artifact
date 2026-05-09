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

/*
 * Message types
 */
enum {
	START_LOAD = 0,
	START_MEASURE,
	REPORT_REQ,
	REPLY,
	TERMINATE,
	CONN_OPEN,
};

/*
 * Types of REPORT_REQ messages
 */
enum {
	REPORT_THROUGHPUT = 0,
	REPORT_LATENCY,
};

/*
 * Types of REPLY messages
 */
enum {
	REPLY_ACK = 0,
	REPLY_STATS_THROUGHPUT,
	REPLY_STATS_LATENCY,
	REPLY_CONVERGENCE,
	REPLY_IA_COMP,
	REPLY_IID,
	// REPLY_KV_STATS etc...
};

struct __attribute__((__packed__)) msg_hdr {
	std::uint32_t MessageType;
	std::uint32_t MessageLength;
};

struct __attribute__((__packed__)) msg1 {
	struct msg_hdr Hdr;
	std::uint32_t Info;
};

struct __attribute__((__packed__)) msg2 {
	struct msg_hdr Hdr;
	std::uint32_t Info1;
	std::uint32_t Info2;
};

// the reply payload starts with the reply type which is a uint32_t
struct __attribute__((__packed__)) throughput_reply {
	std::uint64_t Rx_bytes;
	std::uint64_t Tx_bytes;
	std::uint64_t Req_count;
	std::uint64_t Duration;
	std::uint64_t CorrectIAD; // to avoid padding
};

struct __attribute__((__packed__)) latency_reply {
	struct throughput_reply Th_data;
	std::uint64_t Avg_lat;
	std::uint64_t P50_i;
	std::uint64_t P50;
	std::uint64_t P50_k;
	std::uint64_t P90_i;
	std::uint64_t P90;
	std::uint64_t P90_k;
	std::uint64_t P95_i;
	std::uint64_t P95;
	std::uint64_t P95_k;
	std::uint64_t P99_i;
	std::uint64_t P99;
	std::uint64_t P99_k;
        std::uint64_t P999_i;
	std::uint64_t P999;
	std::uint64_t P999_k;
        std::uint64_t P9999_i;
	std::uint64_t P9999;
	std::uint64_t P9999_k;
        std::uint64_t P99999_i;
	std::uint64_t P99999;
	std::uint64_t P99999_k;
        std::uint64_t P999999_i;
	std::uint64_t P999999;
	std::uint64_t P999999_k;
	std::uint32_t ToReduceSampling;
	std::uint8_t IsIid;
	std::uint8_t IsStationary;
};
