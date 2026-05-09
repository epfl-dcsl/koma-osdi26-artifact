#ifndef __NET_LOGGING_H_
#define __NET_LOGGING_H_

#define ENABLE_LOGGING 0 // Set to 1 to enable logging, 0 to disable
#define ENABLE_TIME_TRACE 0 // Set to 1 to enable time tracing
#define SIOCKOMAPULL (SIOCPROTOPRIVATE + 3)

#if ENABLE_LOGGING
// #define pr_info_log(fmt, ...) pr_info(fmt, ##__VA_ARGS__)
#define pr_info_log(fmt, ...) /* No logging */
#define pr_info_id(fmt, ...)                                                   \
    pr_info("[CPU %d] " fmt, raw_smp_processor_id(), ##__VA_ARGS__)
#else
#define pr_info_log(fmt, ...) /* No logging */
#define pr_info_id(fmt, ...) /* No logging */
#endif

// #define trace_record(fmt, ...) trace_printk(fmt, ##__VA_ARGS__)
#define trace_record(fmt, ...) /* No logging */

#endif /* __NET_LOGGING_H_ */
