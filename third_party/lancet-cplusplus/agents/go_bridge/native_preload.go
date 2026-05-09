package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type nativeEtcdPreloadConfig struct {
	Targets        []string
	Workload       string
	Concurrency    int
	DialTimeout    time.Duration
	RequestTimeout time.Duration
}

func runNativeEtcdPreload(ctx context.Context, cfg nativeEtcdPreloadConfig) error {
	if len(cfg.Targets) == 0 {
		return fmt.Errorf("at least one etcd target is required")
	}
	if cfg.Workload == "" {
		cfg.Workload = "ycsb:workloada"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 2 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Second
	}

	workloadCfg, err := parseBuiltinYCSBWorkload(cfg.Workload)
	if err != nil {
		return err
	}

	shared := newNativeYCSBSharedState(workloadCfg)
	clients := make([]*clientv3.Client, 0, cfg.Concurrency)
	for i := 0; i < cfg.Concurrency; i++ {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   cfg.Targets,
			DialTimeout: cfg.DialTimeout,
		})
		if err != nil {
			closeEtcdClients(clients)
			return err
		}
		clients = append(clients, client)
	}
	defer closeEtcdClients(clients)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var nextKey atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup

	for workerID := 0; workerID < cfg.Concurrency; workerID++ {
		wg.Add(1)
		threadState := newNativeYCSBThreadState(shared, workerID)
		client := clients[workerID]
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}

				keyNum := nextKey.Add(1) - 1
				if keyNum >= shared.cfg.RecordCount {
					return
				}

				putValue, err := threadState.buildAllFieldValues()
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}

				reqCtx, reqCancel := context.WithTimeout(ctx, cfg.RequestTimeout)
				err = etcdPut(reqCtx, client, threadState.rowKeyForKeyNum(keyNum), putValue)
				reqCancel()
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func closeEtcdClients(clients []*clientv3.Client) {
	for _, client := range clients {
		if client != nil {
			_ = client.Close()
		}
	}
}
