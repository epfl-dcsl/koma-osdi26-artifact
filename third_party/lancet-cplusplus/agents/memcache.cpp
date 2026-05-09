/*
 * MIT License
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
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "lancet/stats.hpp"
#include <arpa/inet.h>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <random>

#include <lancet/app_proto.hpp>
#include <lancet/error.hpp>
#include <lancet/key_gen.hpp>
#include <lancet/memcache_bin.hpp>
#include <lancet/rand_gen.hpp>

thread_local struct bmc_header header;
thread_local struct bmc_id_header id_header;
thread_local uint64_t extras;
thread_local char val_len_str[64];
static const char get_cmd[] = "get ";
static const char set_cmd[] = "set ";
static const char rn[] = "\r\n";
static const char set_zeros[] = " 0 0 ";

enum { WAIT_FOR_HEADER = 0, WAIT_FOR_BODY, FINISHED };

static char *strchnth(char *s, char c, int occ)
{
	char *ptr = s - 1;
	for (int i = 0; i < occ; i++) {
		ptr = strchr(ptr + 1, c);
		if (!ptr)
			return nullptr;
	}
	return ptr;
}

static struct consume_resp_pair
memcache_ascii_consume_response(struct application_protocol *proto,
								struct iovec *resp)
{
	struct consume_resp_pair res {
		0, 0, nullptr
	};
	int bytes_to_process = resp->iov_len;
	char *buf = static_cast<char *>(resp->iov_base);

	while (bytes_to_process) {
		if (bytes_to_process < 5) // minimum reply is END\r\n
			break;
		if (strncmp(&buf[resp->iov_len - bytes_to_process], "END\r\n", 5) ==
			0) {
			// key not found
			res.bytes += 5;
			res.reqs += 1;
			bytes_to_process -= 5;
			continue;
		}
		if (bytes_to_process < 8) // try STORED\r\n
			break;
		if (strncmp(&buf[resp->iov_len - bytes_to_process], "STORED\r\n", 8) ==
			0) {
			// successful set
			res.bytes += 8;
			res.reqs += 1;
			bytes_to_process -= 8;
			continue;
		}
		// try for get reply - look for 3 \n
		char *ptr = strchnth(&buf[resp->iov_len - bytes_to_process], '\n', 3);
		if (!ptr)
			break;
		int get_rep_size = ptr - &buf[resp->iov_len - bytes_to_process] + 1;
		bytes_to_process -= get_rep_size;
		res.bytes += get_rep_size;
		res.reqs += 1;
	}

	return res;
}

static int memcache_ascii_create_request(struct application_protocol *proto,
										 struct request *req)
{
	struct kv_info *info = static_cast<struct kv_info *>(proto->arg);
	int key_idx = generate(info->key_sel);
	struct iovec *key = &info->key->keys[key_idx];
	long val_len;

	if (drand48() > info->get_ratio) {
		// set
		val_len = std::lround(generate(info->val_len));
		assert(val_len <= MAX_VAL_SIZE);
		snprintf(val_len_str, 64, "%ld", val_len);

		req->iovs[0].iov_base = const_cast<char *>(set_cmd);
		req->iovs[0].iov_len = 4;
		req->iovs[1].iov_base = key->iov_base;
		req->iovs[1].iov_len = key->iov_len;
		req->iovs[2].iov_base = const_cast<char *>(set_zeros);
		req->iovs[2].iov_len = 5;
		req->iovs[3].iov_base = val_len_str;
		req->iovs[3].iov_len = strlen(val_len_str);
		req->iovs[4].iov_base = const_cast<char *>(rn);
		req->iovs[4].iov_len = 2;
		req->iovs[5].iov_base = random_char;
		req->iovs[5].iov_len = val_len;
		req->iovs[6].iov_base = const_cast<char *>(rn);
		req->iovs[6].iov_len = 2;

		req->iov_cnt = 7;
	} else {
		// get
		req->iovs[0].iov_base = const_cast<char *>(get_cmd);
		req->iovs[0].iov_len = 4;
		req->iovs[1].iov_base = key->iov_base;
		req->iovs[1].iov_len = key->iov_len;
		req->iovs[2].iov_base = const_cast<char *>(rn);
		req->iovs[2].iov_len = 2;

		req->iov_cnt = 3;
	}

	return 0;
}

static struct consume_resp_pair
memcache_bin_consume_response(struct application_protocol *proto,
							  struct iovec *resp)
{
	struct consume_resp_pair res {
		0, 0, nullptr
	};
	int bytes_to_process = resp->iov_len;
	char *buf = static_cast<char *>(resp->iov_base);
	int state = WAIT_FOR_HEADER;

	while (bytes_to_process) {
		switch (state) {
		case WAIT_FOR_HEADER: {
			if (bytes_to_process < sizeof(struct bmc_header))
				return res;
			struct bmc_header *bmc_header =
				reinterpret_cast<struct bmc_header *>(
					&buf[resp->iov_len - bytes_to_process]);
			long body_len = ntohl(bmc_header->body_len);
			state = WAIT_FOR_BODY;
			break;
		}
		case WAIT_FOR_BODY: {
			struct bmc_header *bmc_header =
				reinterpret_cast<struct bmc_header *>(
					&buf[resp->iov_len - bytes_to_process]);
			long body_len = ntohl(bmc_header->body_len);
			if (bytes_to_process < (body_len + sizeof(struct bmc_header)))
				return res;
			state = FINISHED;
			break;
		}
		case FINISHED: {
			struct bmc_header *bmc_header =
				reinterpret_cast<struct bmc_header *>(
					&buf[resp->iov_len - bytes_to_process]);
			long body_len = ntohl(bmc_header->body_len);
			bytes_to_process -= (body_len + sizeof(struct bmc_header));
			res.reqs += 1;
			res.bytes += (sizeof(struct bmc_header) + body_len);
			state = WAIT_FOR_HEADER;
			break;
		}
		}
	}

	return res;
}

static int memcache_bin_create_request(struct application_protocol *proto,
									   struct request *req)
{
	struct kv_info *info = static_cast<struct kv_info *>(proto->arg);
	int key_idx = generate(info->key_sel);
	struct iovec *key = &info->key->keys[key_idx];
	long val_len;

	memset(&header, 0, sizeof(struct bmc_header));
	extras = 0;

	header.magic = 0x80;
	header.key_len = htons(key->iov_len);
	header.data_type = 0x00;
	header.vbucket = 0x00;
	assert(key != nullptr);

	if (drand48() > info->get_ratio) {
		// set
		val_len = std::lround(generate(info->val_len));
		assert(val_len <= MAX_VAL_SIZE);

		header.opcode = CMD_SET;
		header.extra_len = 0x08; // sets have extras for flags and expiration
		header.body_len = htonl(key->iov_len + val_len + header.extra_len);

		req->iovs[0].iov_base = &header;
		req->iovs[0].iov_len = sizeof(struct bmc_header);
		req->iovs[1].iov_base = &extras;
		req->iovs[1].iov_len = sizeof(uint64_t);
		req->iovs[2].iov_base = key->iov_base;
		req->iovs[2].iov_len = key->iov_len;
		req->iovs[3].iov_base = random_char;
		req->iovs[3].iov_len = val_len;

		req->iov_cnt = 4;
	} else {
		// get
		header.opcode = CMD_GETK;
		header.extra_len = 0x00;
		header.body_len = htonl(key->iov_len);

		req->iovs[0].iov_base = &header;
		req->iovs[0].iov_len = sizeof(struct bmc_header);
		req->iovs[1].iov_base = key->iov_base;
		req->iovs[1].iov_len = key->iov_len;

		req->iov_cnt = 2;
	}

	return 0;
}

static struct consume_resp_pair
memcache_id_consume_response(struct application_protocol *proto,
							 struct iovec *resp)
{
	struct consume_resp_pair res {
		0, 0, new std::vector<uint32_t>
	};
	int bytes_to_process = resp->iov_len;
	char *buf = static_cast<char *>(resp->iov_base);
	int state = WAIT_FOR_HEADER;

	while (bytes_to_process) {
		switch (state) {
		case WAIT_FOR_HEADER: {
			if (bytes_to_process < sizeof(struct bmc_id_header))
				return res;
			struct bmc_id_header *id_header =
				reinterpret_cast<struct bmc_id_header *>(
					&buf[resp->iov_len - bytes_to_process]);
			long body_len = ntohl(id_header->body_len);
			state = WAIT_FOR_BODY;
			break;
		}
		case WAIT_FOR_BODY: {
			struct bmc_id_header *id_header =
				reinterpret_cast<struct bmc_id_header *>(
					&buf[resp->iov_len - bytes_to_process]);
			long body_len = ntohl(id_header->body_len);
			if (bytes_to_process < (body_len + sizeof(struct bmc_id_header)))
				return res;
			state = FINISHED;
			break;
		}
		case FINISHED: {
			struct bmc_id_header *id_header =
				reinterpret_cast<struct bmc_id_header *>(
					&buf[resp->iov_len - bytes_to_process]);
			long body_len = ntohl(id_header->body_len);
			bytes_to_process -= (body_len + sizeof(struct bmc_id_header));
			res.reqs += 1;
			res.bytes += (sizeof(struct bmc_id_header) + body_len);
			res.ids->push_back(id_header->id);
			state = WAIT_FOR_HEADER;
			break;
		}
		}
	}
	return res;
}

static int memcache_id_create_request(struct application_protocol *proto,
									  struct request *req)
{
	struct kv_info *info = static_cast<struct kv_info *>(proto->arg);
	int key_idx = generate(info->key_sel);
	struct iovec *key = &info->key->keys[key_idx];
	long val_len;

	memset(&id_header, 0, sizeof(struct bmc_id_header));
	extras = 0;

	id_header.magic = 0x80;
	id_header.key_len = htons(key->iov_len);
	id_header.data_type = 0x00;
	id_header.vbucket = 0x00;
	id_header.id = req->id;
	// id_header.id = std::random_device{}();

	assert(key != nullptr);

	if (drand48() > info->get_ratio) {
		// set
		val_len = std::lround(generate(info->val_len));
		assert(val_len <= MAX_VAL_SIZE);

		id_header.opcode = CMD_SET;
		id_header.extra_len = 0x08; // sets have extras for flags and expiration
		id_header.body_len =
			htonl(key->iov_len + val_len + id_header.extra_len);

		req->iovs[0].iov_base = &id_header;
		req->iovs[0].iov_len = sizeof(struct bmc_id_header);
		req->iovs[1].iov_base = &extras;
		req->iovs[1].iov_len = sizeof(uint64_t);
		req->iovs[2].iov_base = key->iov_base;
		req->iovs[2].iov_len = key->iov_len;
		req->iovs[3].iov_base = random_char;
		req->iovs[3].iov_len = val_len;

		req->iov_cnt = 4;
		// req->id = id_header.id;
	} else {
		// get
		id_header.opcode = CMD_GETK;
		id_header.extra_len = 0x00;
		id_header.body_len = htonl(key->iov_len);

		req->iovs[0].iov_base = &id_header;
		req->iovs[0].iov_len = sizeof(struct bmc_id_header);
		req->iovs[1].iov_base = key->iov_base;
		req->iovs[1].iov_len = key->iov_len;

		req->iov_cnt = 2;
	}

	return 0;
}

int memcache_init(char *proto, struct application_protocol *app_proto)
{
	std::unique_ptr<kv_info> data = std::make_unique<kv_info>();
	char *token;
	char *key_dist;
	int key_count;
	char *saveptr;
	char key_sel[64];

	assert(data != nullptr);
	assert(strncmp("memcache-", proto, 9) == 0);

	/* key size dist */
	strtok_r(const_cast<char *>(proto), "_", &saveptr);

	key_dist = strtok_r(nullptr, "_", &saveptr);

	token = strtok_r(nullptr, "_", &saveptr);

	/* value length dist */
	data->val_len = init_rand(token);
	assert(data->val_len != nullptr);

	/* key count */
	token = strtok_r(nullptr, "_", &saveptr);
	key_count = std::atoi(token);
	/* init key generator */
	data->key = init_key_gen(key_dist, key_count);
	assert(data->key != nullptr);

	/* get request ratio */
	token = strtok_r(nullptr, "_", &saveptr);
	data->get_ratio = std::strtod(token, nullptr);

	/* key selector distribution */
	token = strtok_r(nullptr, "_", &saveptr);
	std::snprintf(key_sel, 64, "%s:%d\n", token, key_count);
	data->key_sel = init_rand(key_sel);
	assert(data->key_sel != nullptr);

	app_proto->arg = data.release();
	if (strncmp("memcache-bin", proto, 12) == 0) {
		app_proto->type = PROTO_MEMCACHED_BIN;
		app_proto->consume_response = memcache_bin_consume_response;
		app_proto->create_request = memcache_bin_create_request;
	} else if (strncmp("memcache-ascii", proto, 14) == 0) {
		app_proto->type = PROTO_MEMCACHED_ASCII;
		app_proto->consume_response = memcache_ascii_consume_response;
		app_proto->create_request = memcache_ascii_create_request;
	} else if (strncmp("memcache-id", proto, 11) == 0) {
		app_proto->type = PROTO_MEMCACHED_ID;
		app_proto->consume_response = memcache_id_consume_response;
		app_proto->create_request = memcache_id_create_request;
	} else {
		std::cerr << "Wrong memcached protocol" << std::endl;
		return -1;
	}
	lancet_fprintf(std::cerr, "Finish initializing memcache protocols\n");
	return 0;
}
