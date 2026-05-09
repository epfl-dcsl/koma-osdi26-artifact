package main

import (
	"strings"
	"testing"
)

func TestLoadBuiltinYCSBWorkload(t *testing.T) {
	workload, err := loadBuiltinYCSBWorkload("ycsb:workloada")
	if err != nil {
		t.Fatalf("loadBuiltinYCSBWorkload: %v", err)
	}
	if !strings.Contains(workload, "workload=core") {
		t.Fatalf("expected core workload contents, got %q", workload)
	}
}

func TestLoadBuiltinYCSBWorkloadSupportsWorkloadF(t *testing.T) {
	workload, err := loadBuiltinYCSBWorkload("ycsb:workloadf")
	if err != nil {
		t.Fatalf("loadBuiltinYCSBWorkload: %v", err)
	}
	if !strings.Contains(workload, "readmodifywriteproportion=0.5") {
		t.Fatalf("expected workloadf contents, got %q", workload)
	}
}

func TestLoadBuiltinYCSBWorkloadRejectsPathLikeNames(t *testing.T) {
	if _, err := loadBuiltinYCSBWorkload("ycsb:../../workloada"); err == nil {
		t.Fatal("expected path-like workload to be rejected")
	}
}

func TestLoadBuiltinYCSBWorkloadRejectsUnknownPrefix(t *testing.T) {
	if _, err := loadBuiltinYCSBWorkload("file:workloada"); err == nil {
		t.Fatal("expected unsupported workload prefix to be rejected")
	}
}

func TestParseBuiltinYCSBWorkloadDefaults(t *testing.T) {
	cfg, err := parseBuiltinYCSBWorkload("ycsb:workloada")
	if err != nil {
		t.Fatalf("parseBuiltinYCSBWorkload: %v", err)
	}
	if cfg.RecordCount != 1000 {
		t.Fatalf("unexpected record count: %d", cfg.RecordCount)
	}
	if cfg.FieldCount != 10 {
		t.Fatalf("unexpected field count: %d", cfg.FieldCount)
	}
	if cfg.FieldLength != 100 {
		t.Fatalf("unexpected field length: %d", cfg.FieldLength)
	}
	if cfg.RequestDistribution != "uniform" {
		t.Fatalf("unexpected request distribution: %q", cfg.RequestDistribution)
	}
	if cfg.InsertOrder != "hashed" {
		t.Fatalf("unexpected insert order: %q", cfg.InsertOrder)
	}
}

func TestParseBuiltinYCSBWorkloadSupportsReadModifyWrite(t *testing.T) {
	cfg, err := parseBuiltinYCSBWorkload("ycsb:workloadf")
	if err != nil {
		t.Fatalf("parseBuiltinYCSBWorkload: %v", err)
	}
	if cfg.ReadModifyWriteProportion != 0.5 {
		t.Fatalf("unexpected read-modify-write proportion: %f", cfg.ReadModifyWriteProportion)
	}
}
