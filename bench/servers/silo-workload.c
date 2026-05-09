#include <stddef.h>
#include <stdio.h>

#include "common.h"
#include "silo-workload.h"

#include "./silo/benchmarks/dcsl.h"

static __thread int worker_initialized;

void silo_workload_init(void) {
  dcsl_init_db();
  fprintf(stderr, "Done initiliazing database.\n");
  dcsl_init_globals(nr_cpu);
  fprintf(stderr, "Done initiliazing globals.\n");
  dcsl_make_loaders();
  fprintf(stderr, "Done initiliazing loaders.\n");
  dcsl_make_workers();
  fprintf(stderr, "nrc cpu is %d\n", nr_cpu);
}

void init_thread(void) {
  if (worker_initialized) {
    return;
  }

  dcsl_init_worker(thread_no);
  worker_initialized = 1;
}

void process_request(void) {
  init_thread();
  dcsl_exec_rd_trans(thread_no);
}
