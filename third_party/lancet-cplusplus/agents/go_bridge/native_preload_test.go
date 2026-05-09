package main

import (
	"context"
	"testing"
)

func TestRunNativeEtcdPreloadRequiresTargets(t *testing.T) {
	err := runNativeEtcdPreload(context.Background(), nativeEtcdPreloadConfig{
		Workload: "ycsb:workloada",
	})
	if err == nil {
		t.Fatal("expected missing targets to fail")
	}
}

func TestRunNativeEtcdPreloadRejectsUnknownWorkload(t *testing.T) {
	err := runNativeEtcdPreload(context.Background(), nativeEtcdPreloadConfig{
		Targets:  []string{"127.0.0.1:2379"},
		Workload: "ycsb:does-not-exist",
	})
	if err == nil {
		t.Fatal("expected unknown workload to fail")
	}
}
