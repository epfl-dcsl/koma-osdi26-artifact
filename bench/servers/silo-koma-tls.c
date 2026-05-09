#include "common.h"
#include "silo-workload.h"

int main(void) {
  init_koma();
  silo_workload_init();
  start_koma_server("memcache-id");

  return 0;
}
