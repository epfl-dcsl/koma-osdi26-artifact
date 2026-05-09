package main

import "testing"

func TestBuildYCSBKeyNameMatchesExpectedHashing(t *testing.T) {
	cfg := &ycsbWorkloadConfig{
		KeyPrefix:   "user",
		ZeroPadding: 1,
		InsertOrder: "hashed",
	}

	if got := buildYCSBKeyName(cfg, 0); got != "user6284781860667377211" {
		t.Fatalf("unexpected hashed key name: %q", got)
	}
}

func TestNativeYCSBInsertStartsAfterRecordCount(t *testing.T) {
	cfg := &ycsbWorkloadConfig{
		Table:          "usertable",
		RecordCount:    1000,
		FieldCount:     10,
		FieldLength:    100,
		KeyPrefix:      "user",
		ZeroPadding:    1,
		InsertOrder:    "hashed",
		WriteAllFields: true,
	}

	shared := newNativeYCSBSharedState(cfg)
	thread := newNativeYCSBThreadState(shared, 0)
	plan, err := thread.buildInsertPlan()
	if err != nil {
		t.Fatalf("buildInsertPlan: %v", err)
	}
	if plan.insertedKey != 1000 {
		t.Fatalf("unexpected inserted key number: %d", plan.insertedKey)
	}
	if plan.rowKey == "" || len(plan.putValue) == 0 {
		t.Fatalf("expected populated insert plan, got %#v", plan)
	}
}
