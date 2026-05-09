#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <set>

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>

#include "dcsl.h"
#include "../allocator.h"
#include "../stats_server.h"
#include "bench.h"
#include "bdb_wrapper.h"
#include "ndb_wrapper.h"
#include "ndb_wrapper_impl.h"
#include "kvdb_wrapper.h"
#include "kvdb_wrapper_impl.h"

using namespace std;
using namespace util;

/* XXX(aghosn) keeping the variables here.*/
size_t nthreads = 1;
int verbose = 1;

/* For initialization etc.*/
spin_barrier *barrier_a;
spin_barrier *barrier_b;
bench_runner *runner;
std::map<std::string, abstract_ordered_index *> open_tables;
std::map<string, vector<abstract_ordered_index *>> partitions;

/* The actual worker for the transaction*/
std::vector<bench_worker *> workers;

abstract_db *db = NULL;
void (*test_fn)(abstract_db *, int argc, char **argv) = NULL;
string bench_type = "tpcc";
string db_type = "ndb-proto2";
char *curdir = get_current_dir_name();
string basedir = curdir;
string bench_opts;
size_t numa_memory = 0;
int saw_run_spec = 0;
int nofsync = 0;
int do_compress = 0;
int fake_writes = 0;
int disable_gc = 0;
int disable_snapshots = 0;
vector<string> logfiles;
vector<vector<unsigned>> assignments;
string stats_server_sockfile;
/*XXX(aghosn) done.*/

extern "C" void dcsl_test(int i)
{
	cout << "Test DCSL C++ function " << endl;
}

/*	Copied from dbtest.cc*/
extern "C" int dcsl_init_db()
{
  db = new ndb_wrapper<transaction_proto2>(
        logfiles, assignments, !nofsync, do_compress, fake_writes);
  ALWAYS_ASSERT(!transaction_proto2_static::get_hack_status());
  return 1;
}

extern void print_tpcc_settings(void);

/*	Initialization required.*/
extern "C" int dcsl_init_globals(size_t number_threads)
{
	//From bench_runner constructor.
	nthreads = number_threads;
	scale_factor = number_threads;

	runtime = 20;
	pin_cpus = 1;
	long numa_memory = 20l * (1 << 30);
	const size_t maxpercpu = util::iceil(numa_memory / nthreads, ::allocator::GetHugepageSize());
	std::cerr << "xxxxx " << numa_memory << "\n";
	std::cerr << "xxxxx " << numa_memory / nthreads << "\n";
	std::cerr << "xxxxx " << maxpercpu << "\n";
	::allocator::Initialize(nthreads, maxpercpu);

	barrier_a = new spin_barrier(nthreads);
	barrier_b = new spin_barrier(1);
	//TODO set the open_tables as well.
	dcsl_create_runner_partitions(db, runner, open_tables, partitions);

  if (verbose) {
    const unsigned long ncpus = coreid::num_cpus_online();
    cerr << "Database Benchmark:"                           << endl;
    cerr << "  pid: " << getpid()                           << endl;
    cerr << "settings:"                                     << endl;
    cerr << "  par-loading : " << enable_parallel_loading   << endl;
    cerr << "  pin-cpus    : " << pin_cpus                  << endl;
    cerr << "  slow-exit   : " << slow_exit                 << endl;
    cerr << "  retry-txns  : " << retry_aborted_transaction << endl;
    cerr << "  backoff-txns: " << backoff_aborted_transaction << endl;
    cerr << "  bench       : " << bench_type                << endl;
    cerr << "  scale       : " << scale_factor              << endl;
    cerr << "  num-cpus    : " << ncpus                     << endl;
    cerr << "  num-threads : " << nthreads                  << endl;
    cerr << "  db-type     : " << db_type                   << endl;
    cerr << "  basedir     : " << basedir                   << endl;
    cerr << "  txn-flags   : " << hexify(txn_flags)         << endl;
    if (run_mode == RUNMODE_TIME)
      cerr << "  runtime     : " << runtime                 << endl;
    else
      cerr << "  ops/worker  : " << ops_per_worker          << endl;
#ifdef USE_VARINT_ENCODING
    cerr << "  var-encode  : yes"                           << endl;
#else
    cerr << "  var-encode  : no"                            << endl;
#endif

#ifdef USE_JEMALLOC
    cerr << "  allocator   : jemalloc"                      << endl;
#elif defined USE_TCMALLOC
    cerr << "  allocator   : tcmalloc"                      << endl;
#elif defined USE_FLOW
    cerr << "  allocator   : flow"                          << endl;
#else
    cerr << "  allocator   : libc"                          << endl;
#endif
    if (numa_memory > 0) {
      cerr << "  numa-memory : " << numa_memory             << endl;
    } else {
      cerr << "  numa-memory : disabled"                    << endl;
    }
    cerr << "  logfiles : " << logfiles                     << endl;
    cerr << "  assignments : " << assignments               << endl;
    cerr << "  disable-gc : " << disable_gc                 << endl;
    cerr << "  disable-snapshots : " << disable_snapshots   << endl;
    cerr << "  stats-server-sockfile: " << stats_server_sockfile << endl;

    cerr << "system properties:" << endl;
    cerr << "  btree_internal_node_size: " << concurrent_btree::InternalNodeSize() << endl;
    cerr << "  btree_leaf_node_size    : " << concurrent_btree::LeafNodeSize() << endl;

#ifdef TUPLE_PREFETCH
    cerr << "  tuple_prefetch          : yes" << endl;
#else
    cerr << "  tuple_prefetch          : no" << endl;
#endif

#ifdef BTREE_NODE_PREFETCH
    cerr << "  btree_node_prefetch     : yes" << endl;
#else
    cerr << "  btree_node_prefetch     : no" << endl;
#endif
  cerr << "The db type" << db_type << endl;
  print_tpcc_settings();

  }


	return 1;
}

extern pair<uint64_t, uint64_t> get_system_memory_info();

static pair<uint64_t, uint64_t> mem_info_before;
static map<string, size_t> table_sizes_before;
static timer *t, *t_nosync;

/*TODO probably missing partitions and need to check if open_tables is modified*/
extern "C" int dcsl_make_loaders()
{
	vector<bench_loader *> loaders;
	//dcsl_init_mloaders(db, open_tables, partitions, loaders);
	dcsl_init_mloaders(runner, loaders);
	cerr << "Finished init mloaders" << endl;
	spin_barrier b(loaders.size());
  	
    const pair<uint64_t, uint64_t> mem_info_before_ = get_system_memory_info();
    {
      scoped_timer t("dataloading", verbose);
      for (vector<bench_loader *>::const_iterator it = loaders.begin();
          it != loaders.end(); ++it) {
        (*it)->set_barrier(b);
        (*it)->start();
      }
      for (vector<bench_loader *>::const_iterator it = loaders.begin();
          it != loaders.end(); ++it)
        (*it)->join();
    }
    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before_.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    if (verbose)
      cerr << "DB size: " << delta_mb << " MB" << endl;

  	db->do_txn_epoch_sync();
  	{
  		const auto persisted_info = db->get_ntxn_persisted();
  		if (get<0>(persisted_info) != get<1>(persisted_info))
  			cerr << "ERROR: " << persisted_info << endl;
  	}
  	
  	db->reset_ntxn_persisted();
	if (!no_reset_counters) {
		event_counter::reset_all_counters(); // XXX: for now - we really should have a before/after loading
		PERF_EXPR(scopedperf::perfsum_base::resetall());
	}
	{
		const auto persisted_info = db->get_ntxn_persisted();
		if (get<0>(persisted_info) != 0 ||
			get<1>(persisted_info) != 0 ||
			get<2>(persisted_info) != 0.0) {
			cerr << persisted_info << endl;
			ALWAYS_ASSERT(false);
		}
	}

  {
    const auto persisted_info = db->get_ntxn_persisted();
    if (get<0>(persisted_info) != get<1>(persisted_info))
      cerr << "ERROR: " << persisted_info << endl;
    //ALWAYS_ASSERT(get<0>(persisted_info) == get<1>(persisted_info));
    if (verbose)
      cerr << persisted_info << " txns persisted in loading phase" << endl;
  }
  if (verbose) {
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      scoped_rcu_region guard;
      const size_t s = it->second->size();
      cerr << "table " << it->first << " size " << s << endl;
      table_sizes_before[it->first] = s;
    }
    cerr << "starting benchmark..." << endl;
  }

  mem_info_before = get_system_memory_info();

	cerr << "Finished initializing and running loaders..." << endl;
	return 1;
}

/* TODO might be better to say make_work, we try not to have a thread.
 * Maybe pass as an argument a call back function for the wait...
 * Or take as argument something.*/
extern "C" int dcsl_make_workers()
{
	//dcsl_init_mworkers(workers, nthreads, db, open_tables, partitions,
	//	*barrier_a, *barrier_b);
	dcsl_init_mworkers(workers, runner);

	cerr << "Finished initializing workers..." << endl;
	t = new timer();
	t_nosync = new timer();
	return 1;
}

extern "C" int dcsl_exec_trans(int worker_id, int trans_idx)
{
	//Error.
	if (trans_idx < 0 || trans_idx > 4) {
		cerr << "Wrong id for transaction." << endl;
		return -1;
	}

	//probably do a static_cast.
	dcsl_exec(worker_id, trans_idx, &workers);

	return 1;
}

extern "C" int dcsl_exec_rd_trans(int worker_id)
{
	dcsl_exec_rd(worker_id, &workers);
	return 1;
}

template <typename K, typename V>
static void
map_agg(map<K, V> &agg, const map<K, V> &m)
{
  for (typename map<K, V>::const_iterator it = m.begin();
       it != m.end(); ++it)
    agg[it->first] += it->second;
}

extern "C" void dcsl_print_stats(void)
{
  const unsigned long elapsed_nosync = t_nosync->lap();
  db->do_txn_finish(); // waits for all worker txns to persist
  size_t n_commits = 0;
  size_t n_aborts = 0;
  uint64_t latency_numer_us = 0;
  for (size_t i = 0; i < nthreads; i++) {
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
    latency_numer_us += workers[i]->get_latency_numer_us();
  }
  const auto persisted_info = db->get_ntxn_persisted();

  const unsigned long elapsed = t->lap(); // lap() must come after do_txn_finish(),
                                         // because do_txn_finish() potentially
                                         // waits a bit

  const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
  const double agg_nosync_throughput = double(n_commits) / elapsed_nosync_sec;
  const double avg_nosync_per_core_throughput = agg_nosync_throughput / double(workers.size());

  const double elapsed_sec = double(elapsed) / 1000000.0;
  const double agg_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_throughput = agg_throughput / double(workers.size());

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate = agg_abort_rate / double(workers.size());

  // we can use n_commits here, because we explicitly wait for all txns
  // run to be durable
  const double agg_persist_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_persist_throughput =
    agg_persist_throughput / double(workers.size());

  // XXX(stephentu): latency currently doesn't account for read-only txns
  const double avg_latency_us =
    double(latency_numer_us) / double(n_commits);
  const double avg_latency_ms = avg_latency_us / 1000.0;
  const double avg_persist_latency_ms =
    get<2>(persisted_info) / 1000.0;

  if (verbose) {
    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    map<string, size_t> agg_txn_counts = workers[0]->get_txn_counts();
    ssize_t size_delta = workers[0]->get_size_delta();
    for (size_t i = 1; i < workers.size(); i++) {
      map_agg(agg_txn_counts, workers[i]->get_txn_counts());
      size_delta += workers[i]->get_size_delta();
    }
    const double size_delta_mb = double(size_delta)/1048576.0;
    map<string, counter_data> ctrs = event_counter::get_all_counters();

    cerr << "--- table statistics ---" << endl;
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      scoped_rcu_region guard;
      const size_t s = it->second->size();
      const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
      cerr << "table " << it->first << " size " << it->second->size();
      if (delta < 0)
        cerr << " (" << delta << " records)" << endl;
      else
        cerr << " (+" << delta << " records)" << endl;
    }
#ifdef ENABLE_BENCH_TXN_COUNTERS
    cerr << "--- txn counter statistics ---" << endl;
    {
      // take from thread 0 for now
      abstract_db::txn_counter_map agg = workers[0]->get_local_txn_counters();
      for (auto &p : agg) {
        cerr << p.first << ":" << endl;
        for (auto &q : p.second)
          cerr << "  " << q.first << " : " << q.second << endl;
      }
    }
#endif
    cerr << "--- benchmark statistics ---" << endl;
    cerr << "runtime: " << elapsed_sec << " sec" << endl;
    cerr << "memory delta: " << delta_mb  << " MB" << endl;
    cerr << "memory delta rate: " << (delta_mb / elapsed_sec)  << " MB/sec" << endl;
    cerr << "logical memory delta: " << size_delta_mb << " MB" << endl;
    cerr << "logical memory delta rate: " << (size_delta_mb / elapsed_sec) << " MB/sec" << endl;
    cerr << "agg_nosync_throughput: " << agg_nosync_throughput << " ops/sec" << endl;
    cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_persist_throughput: " << agg_persist_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_persist_throughput: " << avg_per_core_persist_throughput << " ops/sec/core" << endl;
    cerr << "avg_latency: " << avg_latency_ms << " ms" << endl;
    cerr << "avg_persist_latency: " << avg_persist_latency_ms << " ms" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
    // cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(), agg_txn_counts.end()) << endl;
    cerr << "--- system counters (for benchmark) ---" << endl;
    for (map<string, counter_data>::iterator it = ctrs.begin();
         it != ctrs.end(); ++it)
      cerr << it->first << ": " << it->second << endl;
    cerr << "--- perf counters (if enabled, for benchmark) ---" << endl;
    PERF_EXPR(scopedperf::perfsum_base::printall());
    cerr << "--- allocator stats ---" << endl;
    ::allocator::DumpStats();
    cerr << "---------------------------------------" << endl;

#ifdef USE_JEMALLOC
    cerr << "dumping heap profile..." << endl;
    //mallctl("prof.dump", NULL, NULL, NULL, 0);
    cerr << "printing jemalloc stats..." << endl;
    //malloc_stats_print(write_cb, NULL, "");
#endif
#ifdef USE_TCMALLOC
    HeapProfilerDump("before-exit");
#endif
  }
}

extern "C" void dcsl_init_worker(int worker_id)
{
  __dcsl_init_worker(worker_id, &workers);
}
