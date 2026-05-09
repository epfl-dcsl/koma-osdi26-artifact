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

#include <iostream>
#include <iomanip>
#include <cstring>
#include <unistd.h>

#define lancet_fprintf(f, fmt, ...)                                                \
    {                                                                          \
        char hostname[64];                                                    \
        gethostname(hostname, 64);                                            \
        std::ostream& lancet_output_stream = (f);                            \
        lancet_output_stream << "[" << hostname << "] ";                     \
        lancet_output_stream << std::setw(4) << std::left << "INFO: ";        \
        lancet_output_stream << std::setw(4) << std::right;                   \
        char lancet_formatted_message[256];                                   \
        snprintf(lancet_formatted_message, sizeof(lancet_formatted_message), \
                 fmt, ##__VA_ARGS__);                                         \
        lancet_output_stream << lancet_formatted_message;                                                    \
    }

#define lancet_perror(...)                                                    \
    {                                                                         \
        char hostname[64];                                                    \
        gethostname(hostname, 64);                                            \
        std::cerr << "[" << hostname << "] ERROR: ";                         \
        perror(__VA_ARGS__);                                                  \
    }