#include <stdio.h>
#include <string.h>

#include "common.h"
#include "service-time.h"

/* spin for usecs to imitate the service time*/
static void spin(int usecs) {
  long start;

  start = mytime();
  while (mytime() < start + usecs)
    asm volatile("pause");
}

/* generate service time according to the specified distribution */
void process_request(void) {
  int svc_time;
  svc_time = service_time_generate();
  /*printf("to spin for %d microseconds\n", svc_time);*/
  spin(svc_time);
}

void init_thread(void) {
  service_time_init_thread();
}

static void help(const char *prgname) {
  printf("Usage: %s [--udp] service-time-distribution service-protocol\n"
         "\n"
         "Distributions are specified by <distribution>[:<param1>[,...]].\n"
         "Parameters are not required.  The following distributions are "
         "supported:\n"
         "\n"
         "   [fixed:]<value>              Always generates <value>.\n"
         "   uniform:<max>                Uniform distribution between 0 and "
         "<max>.\n"
         "   normal:<mean>,<sd>           Normal distribution.\n"
         "   exponential:<lambda>         Exponential distribution.\n"
         "   pareto:<loc>,<scale>,<shape> Generalized Pareto distribution.\n"
         "   gev:<loc>,<scale>,<shape>    Generalized Extreme Value "
         "distribution.\n"
         "   bimodal:<ratio>,<v1>,<v2>    Bimodal distribution P(v1)=ratio, "
         "P(v2)=1-ratio.\n"
         "   file:<path>                  Draws random latencies from file.\n"
         "\n"

         "Service protocol are specified by <proto_name>.\n"
         "The following protocols are supported:"
         "\n"
         "   echo:<value>                 plain echo, the number of bytes is "
         "<value>\n"
         "   memcache-bin                    binary memcached\n"
         "   memcache-id                    binary memcached with identifier "
         "embeded in each request/response\n"
         "\n",
         prgname);
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    help(argv[0]);
    return -1;
  }

  init_koma();
  service_time_configure(argv[1]);
  start_koma_server(argv[2]);

  return 0;
}
