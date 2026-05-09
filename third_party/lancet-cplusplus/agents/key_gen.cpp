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
#include <cmath>
#include <iostream>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <vector>

#include <lancet/error.hpp>
#include <lancet/key_gen.hpp>
#include <lancet/rand_gen.hpp>

static struct iovec *uniform_get_key(struct key_gen *kg)
{
	long int idx = rand();
	return &kg->keys[idx % kg->key_count];
}

static void generate_keys(struct key_gen *kg)
{

	int i;
	long key_size;
	// i starts from 1 to avoid log(0) in exp mode.
	for (i = 1; i <= kg->key_count; i++) {
		key_size = lround(
			kg->key_size_gen->inv_cdf(kg->key_size_gen, static_cast<double>(i) / kg->key_count));
		if (key_size == 0)
			key_size += 1;
		kg->keys[i-1].iov_base = calloc(key_size + 1, sizeof(char));
		kg->keys[i-1].iov_len = key_size;
		std::snprintf(static_cast<char*>(kg->keys[i-1].iov_base), static_cast<int>(key_size + 1), "%0*d", static_cast<int>(key_size),
				 i-1);
		// lancet_fprintf(std::cerr, "Formatted Output: %s\n",
		// 			   static_cast<char *>(kg->keys[i - 1].iov_base));
	}
}

struct key_gen *init_key_gen(char *type, int key_count)
{
	struct key_gen *kg = new struct key_gen;
	struct rand_gen *size_gen;

	size_gen = init_rand(type);
	kg->key_count = key_count;
	kg->get_key = uniform_get_key;
	kg->key_size_gen = size_gen;
	kg->keys = static_cast<struct iovec*>(malloc(key_count * sizeof(struct iovec)));
	generate_keys(kg);
	return kg;
}
