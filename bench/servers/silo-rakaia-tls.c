#include "common.h"
#include "silo-workload.h"

int main(void) {
  init_rakaia();
  silo_workload_init();
  start_rakaia_server("memcache-id");

  return 0;
}
