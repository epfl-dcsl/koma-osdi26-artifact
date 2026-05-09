#include <stddef.h>

#include "common.h"
#include "silo-workload.h"

int main(void) {
  init_linux();
  silo_workload_init();
  start_linux_pool_server("memcache-id", NULL, NULL);

  return 0;
}
