#pragma once

#include <sys/time.h>
#include <unistd.h>

void init_linux(void);
void init_thread(void);
void process_request(void);
void start_linux_server(const char *str);
void start_linux_pool_server(const char *str, const char *io_threads_arg,
                             const char *worker_threads_arg);

// kcm related
void init_kcm(void);
void start_kcm_server(const char *);

// koma related
void init_koma(void);
void start_koma_server(const char *);

extern __thread int thread_no;
extern int nr_cpu;

static inline long mytime(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000 + tv.tv_usec;
}
