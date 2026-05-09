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

#define CMD_GET 0x00
#define CMD_GETK 0x0c
#define CMD_SET 0x01

struct __attribute__((__packed__)) bmc_header {
	std::uint8_t magic;
	std::uint8_t opcode;
	std::int16_t key_len;

	std::uint8_t extra_len;
	std::uint8_t data_type;
	union {
		uint16_t vbucket; // request use
		uint16_t status;  // response use
	};

	std::uint32_t body_len;
	std::uint32_t opaque;
	std::uint64_t version;
};

struct __attribute__((__packed__)) bmc_id_header {
	std::uint8_t magic;
	std::uint8_t opcode;
	std::uint32_t id; // id used to match request with response
	std::int16_t key_len;

	std::uint8_t extra_len;
	std::uint8_t data_type;
	union {
		uint16_t vbucket; // request use
		uint16_t status;  // response use
	};

	std::uint32_t body_len;
	std::uint32_t opaque;
	std::uint64_t version;
};
