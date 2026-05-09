#ifndef _DCSL_H
#define _DCSL_H

#ifdef __cplusplus

extern int verbose;
extern int enable_parallel_loading;

extern "C" 
{

#endif 
void dcsl_test(int);
int dcsl_init_db();
int dcsl_init_globals(size_t number_threads);
int dcsl_make_loaders();
int dcsl_make_workers();
int dcsl_exec_trans(int worker_id, int trans_idx);
int dcsl_exec_rd_trans(int worker_id);
void dcsl_print_stats(void);
void dcsl_init_worker(int worker_id);

#ifdef __cplusplus
}
#endif

#endif