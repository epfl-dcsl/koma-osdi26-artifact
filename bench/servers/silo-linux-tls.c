#include "common.h"
#include "silo-workload.h"

int main(void) {
  init_linux();
  silo_workload_init();
  start_linux_server("memcache-id");

  return 0;
}
