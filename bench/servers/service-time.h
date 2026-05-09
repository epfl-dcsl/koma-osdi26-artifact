#pragma once

#include <errno.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"

#define SERVICE_FIXED_PREFIX "fixed:"
#define SERVICE_EXPONENTIAL_PREFIX "exponential:"
#define SERVICE_BIMODAL_PREFIX "bimodal:"
#define PREFIX_LEN(prefix) (sizeof(prefix) - 1)

/* SplitMix64 is used only to turn nearby thread/time seeds into independent
 * xorshift states. These are the standard SplitMix64 mixing constants. */
#define SPLITMIX64_GAMMA 0x9e3779b97f4a7c15ULL
#define SPLITMIX64_MIX1 0xbf58476d1ce4e5b9ULL
#define SPLITMIX64_MIX2 0x94d049bb133111ebULL
#define SPLITMIX64_SHIFT1 30
#define SPLITMIX64_SHIFT2 27
#define SPLITMIX64_SHIFT3 31

/* Marsaglia/Vigna xorshift64* constants. The generator is not cryptographic;
 * it is a cheap per-thread uniform RNG for service-time sampling. */
#define XORSHIFT64STAR_SHIFT1 12
#define XORSHIFT64STAR_SHIFT2 25
#define XORSHIFT64STAR_SHIFT3 27
#define XORSHIFT64STAR_MULTIPLIER 2685821657736338717ULL

#define DOUBLE_MANTISSA_BITS 53
#define DOUBLE_RANDOM_SHIFT (64 - DOUBLE_MANTISSA_BITS)
#define DOUBLE_UNIT_SCALE (1.0 / ((double)(1ULL << DOUBLE_MANTISSA_BITS) + 1.0))

enum service_time_mode {
  SERVICE_TIME_FALLBACK,
  SERVICE_TIME_FIXED,
  SERVICE_TIME_EXPONENTIAL,
  SERVICE_TIME_BIMODAL,
};

static enum service_time_mode service_time_mode = SERVICE_TIME_FALLBACK;
static double fixed_service_time;
static double exponential_lambda;
static double bimodal_ratio;
static double bimodal_v1;
static double bimodal_v2;
static __thread uint64_t service_time_rng_state;

static uint64_t service_time_splitmix64_next(uint64_t *state) {
  uint64_t z;

  *state += SPLITMIX64_GAMMA;
  z = *state;
  z = (z ^ (z >> SPLITMIX64_SHIFT1)) * SPLITMIX64_MIX1;
  z = (z ^ (z >> SPLITMIX64_SHIFT2)) * SPLITMIX64_MIX2;
  return z ^ (z >> SPLITMIX64_SHIFT3);
}

static void service_time_init_thread(void) {
  uint64_t seed = (uint64_t)mytime();

  seed ^= (uint64_t)(uintptr_t)pthread_self();
  service_time_rng_state = service_time_splitmix64_next(&seed);
  if (service_time_rng_state == 0) {
    service_time_rng_state = SPLITMIX64_GAMMA;
  }
}

static uint64_t service_time_rng_u64(void) {
  uint64_t x;

  if (service_time_rng_state == 0) {
    service_time_init_thread();
  }

  x = service_time_rng_state;
  x ^= x >> XORSHIFT64STAR_SHIFT1;
  x ^= x << XORSHIFT64STAR_SHIFT2;
  x ^= x >> XORSHIFT64STAR_SHIFT3;
  service_time_rng_state = x;
  return x * XORSHIFT64STAR_MULTIPLIER;
}

static double service_time_uniform_open(void) {
  return ((service_time_rng_u64() >> DOUBLE_RANDOM_SHIFT) + 1.0) * DOUBLE_UNIT_SCALE;
}

static bool service_time_parse_double(const char **cursor, double *value, char delim) {
  char *end;
  double parsed;

  errno = 0;
  parsed = strtod(*cursor, &end);
  if (errno != 0 || end == *cursor) {
    return false;
  }
  if ((delim && *end != delim) || (!delim && *end != '\0')) {
    return false;
  }

  *value = parsed;
  *cursor = delim ? end + 1 : end;
  return true;
}

static void service_time_configure(const char *config) {
  const char *cursor;
  double a, b, c;

  cursor = config;
  if (service_time_parse_double(&cursor, &a, '\0')) {
    fixed_service_time = a;
    service_time_mode = SERVICE_TIME_FIXED;
    return;
  }

  if (strncmp(config, SERVICE_FIXED_PREFIX, PREFIX_LEN(SERVICE_FIXED_PREFIX)) == 0) {
    cursor = config + PREFIX_LEN(SERVICE_FIXED_PREFIX);
    if (service_time_parse_double(&cursor, &a, '\0')) {
      fixed_service_time = a;
      service_time_mode = SERVICE_TIME_FIXED;
      return;
    }
  } else if (strncmp(config, SERVICE_EXPONENTIAL_PREFIX,
                     PREFIX_LEN(SERVICE_EXPONENTIAL_PREFIX)) == 0) {
    cursor = config + PREFIX_LEN(SERVICE_EXPONENTIAL_PREFIX);
    if (service_time_parse_double(&cursor, &a, '\0')) {
      exponential_lambda = a;
      service_time_mode = SERVICE_TIME_EXPONENTIAL;
      return;
    }
  } else if (strncmp(config, SERVICE_BIMODAL_PREFIX,
                     PREFIX_LEN(SERVICE_BIMODAL_PREFIX)) == 0) {
    cursor = config + PREFIX_LEN(SERVICE_BIMODAL_PREFIX);
    if (service_time_parse_double(&cursor, &a, ',') &&
        service_time_parse_double(&cursor, &b, ',') &&
        service_time_parse_double(&cursor, &c, '\0')) {
      bimodal_ratio = a;
      bimodal_v1 = b;
      bimodal_v2 = c;
      service_time_mode = SERVICE_TIME_BIMODAL;
      return;
    }
  }

  service_time_mode = SERVICE_TIME_FALLBACK;
}

static int service_time_generate(void) {
  double u;

  switch (service_time_mode) {
  case SERVICE_TIME_FIXED:
    return (int)fixed_service_time;
  case SERVICE_TIME_EXPONENTIAL:
    if (exponential_lambda <= 0.0) {
      return 0;
    }
    return (int)(-log(service_time_uniform_open()) / exponential_lambda);
  case SERVICE_TIME_BIMODAL:
    u = service_time_uniform_open();
    return (int)(u > bimodal_ratio ? bimodal_v2 : bimodal_v1);
  case SERVICE_TIME_FALLBACK:
  default:
    fprintf(stderr,
            "service-time: unrecognized distribution; expected "
            "fixed:<v>, exponential:<lambda>, or bimodal:<r>,<v1>,<v2>\n");
    abort();
  }
}
