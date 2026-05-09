//go:build preloadcli

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	workload := flag.String("workload", "ycsb:workloada", "built-in workload name, e.g. ycsb:workloada")
	targetsCSV := flag.String("targets", "", "comma-separated etcd endpoints")
	concurrency := flag.Int("concurrency", 1, "number of parallel preload workers")
	dialTimeout := flag.Duration("dial-timeout", 2*time.Second, "etcd dial timeout")
	requestTimeout := flag.Duration("request-timeout", 5*time.Second, "timeout per preload request")
	flag.Parse()

	if *targetsCSV == "" {
		fmt.Fprintln(os.Stderr, "missing required -targets")
		os.Exit(2)
	}

	if err := runNativeEtcdPreload(context.Background(), nativeEtcdPreloadConfig{
		Targets:        parseTargetsCSV(*targetsCSV),
		Workload:       *workload,
		Concurrency:    *concurrency,
		DialTimeout:    *dialTimeout,
		RequestTimeout: *requestTimeout,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "native etcd preload failed: %v\n", err)
		os.Exit(1)
	}
}
