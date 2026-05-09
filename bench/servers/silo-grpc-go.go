//go:build silo

package main

/*
#cgo CFLAGS: -I./silo/benchmarks
#cgo LDFLAGS: ./silo/out-perf.masstree/allocator.o ./silo/out-perf.masstree/btree.o ./silo/out-perf.masstree/compiler.o ./silo/out-perf.masstree/core.o ./silo/out-perf.masstree/counter.o ./silo/out-perf.masstree/json.o ./silo/out-perf.masstree/memory.o ./silo/out-perf.masstree/rcu.o ./silo/out-perf.masstree/stats_server.o ./silo/out-perf.masstree/straccum.o ./silo/out-perf.masstree/string.o ./silo/out-perf.masstree/str.o ./silo/out-perf.masstree/thread.o ./silo/out-perf.masstree/ticker.o ./silo/out-perf.masstree/tuple.o ./silo/out-perf.masstree/txn_btree.o ./silo/out-perf.masstree/txn.o ./silo/out-perf.masstree/txn_proto2_impl.o ./silo/out-perf.masstree/varint.o ./silo/out-perf.masstree/benchmarks/bench.o ./silo/out-perf.masstree/benchmarks/bid.o ./silo/out-perf.masstree/benchmarks/dcsl.o ./silo/out-perf.masstree/benchmarks/tpcc.o ./silo/out-perf.masstree/benchmarks/ycsb.o ./silo/third-party/lz4/lz4.o -lpthread -lnuma -lm -lstdc++


#include "dcsl.h"

// Thin wrappers to avoid cgo type noise.
static inline void dcsl_init_db_wrap(void) {
	dcsl_init_db();
}

static inline void dcsl_init_globals_wrap(int nworkers) {
	dcsl_init_globals(nworkers);
}

static inline void dcsl_make_loaders_wrap(void) {
	dcsl_make_loaders();
}
static inline void dcsl_make_workers_wrap(void) {
	dcsl_make_workers();
}
static inline void dcsl_init_worker_wrap(int wid) {
	dcsl_init_worker(wid);
}

static inline void dcsl_exec_rd_trans_wrap(int wid) {
	dcsl_exec_rd_trans(wid);
}
*/
import "C"

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"os"
	// "runtime"
	"runtime/debug"
	"strings"

	proto "main/proto.com"
)

// ============================================================
// Config
// ============================================================

const (
	port       = 40001
	NumWorkers = 24
)

// ============================================================
// Silo worker + free-worker pool
// ============================================================

type siloJob struct {
	done chan struct{}
}

type siloWorker struct {
	wid  int
	jobs chan siloJob
}

// Global pool of currently FREE workers
var freeWorkers chan *siloWorker

func resolveNumWorkers(defaultWorkers uint32) uint32 {
	cores := os.Getenv("GRPC_KOMA_CORES")
	if cores == "" {
		return defaultWorkers
	}

	var count uint32
	for _, core := range strings.Split(cores, ",") {
		if strings.TrimSpace(core) != "" {
			count++
		}
	}
	if count == 0 {
		return defaultWorkers
	}
	return count
}

// ============================================================
// Start Silo workers (like silo-linux, but via Go)
// ============================================================

func startSiloWorkers(numWorkers int) {
	freeWorkers = make(chan *siloWorker, numWorkers)

	for wid := 0; wid < numWorkers; wid++ {
		w := &siloWorker{
			wid:  wid,
			jobs: make(chan siloJob),
		}

		go func(w *siloWorker) {
			// Each worker owns ONE OS thread forever (like pthread)
			// runtime.LockOSThread()
			// defer runtime.UnlockOSThread()

			log.Printf("[SILO] Worker %d: init", w.wid)
			C.dcsl_init_worker_wrap(C.int(w.wid))

			// Mark worker as initially FREE
			freeWorkers <- w

			for job := range w.jobs {
				// Execute exactly ONE Silo transaction
				C.dcsl_exec_rd_trans_wrap(C.int(w.wid))

				// Notify RPC handler
				close(job.done)

				// Return worker to FREE pool
				freeWorkers <- w
			}
		}(w)
	}
}

// ============================================================
// Silo initialization (MATCHES silo-linux semantics)
// ============================================================

func initSilo(numWorkers int) {
	log.Printf("[SILO] Initializing DB")
	C.dcsl_init_db_wrap()

	log.Printf("[SILO] Initializing globals with %d workers", numWorkers)
	C.dcsl_init_globals_wrap(C.int(numWorkers))

	log.Printf("[SILO] Making loaders")
	C.dcsl_make_loaders_wrap()

	// IMPORTANT:
	// We DO NOT call dcsl_make_workers().
	// Go replaces Silo's pthread worker pool.
	C.dcsl_make_workers_wrap()

	log.Printf("[SILO] Starting %d Go-pinned workers", numWorkers)
	startSiloWorkers(numWorkers)
}

// ============================================================
// gRPC server
// ============================================================

type siloServer struct {
	proto.UnimplementedEchoServer
}

func (s *siloServer) SendEcho(ctx context.Context, in *proto.EchoRequest) (*proto.EchoResponse, error) {
	var w *siloWorker

	// Get ANY free worker (work-conserving)
	select {
	case w = <-freeWorkers:
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "no free Silo worker")
	}

	job := siloJob{done: make(chan struct{})}

	// Send job to the worker
	select {
	case w.jobs <- job:
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "enqueue cancelled")
	}

	// Wait for completion
	select {
	case <-job.done:
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "execution cancelled")
	}

	return &proto.EchoResponse{
		Message: in.GetMessage(),
	}, nil
}

// ============================================================
// main
// ============================================================

func main() {
	debug.SetGCPercent(-1)

	initSilo(NumWorkers)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	numWorkers := resolveNumWorkers(NumWorkers)
	maxStreams := uint32(10000000)

	s := grpc.NewServer(
		grpc.NumStreamWorkers(numWorkers),
		grpc.MaxConcurrentStreams(maxStreams),
		grpc.InitialConnWindowSize(126553500),
	)

	proto.RegisterEchoServer(s, &siloServer{})

	log.Printf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
