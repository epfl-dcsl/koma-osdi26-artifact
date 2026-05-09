package main

import (
	"embed"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	defaultYCSBTable                   = "usertable"
	defaultYCSBFieldCount              = 10
	defaultYCSBFieldLength             = 100
	defaultYCSBFieldLengthDistribution = "constant"
	defaultYCSBRequestDistribution     = "uniform"
	defaultYCSBScanLengthDistribution  = "uniform"
	defaultYCSBInsertOrder             = "hashed"
	defaultYCSBKeyPrefix               = "user"
	defaultYCSBZeroPadding             = 1
	defaultYCSBReadAllFields           = true
	defaultYCSBWriteAllFields          = false
	defaultYCSBMinScanLength           = 1
	defaultYCSBMaxScanLength           = 1000
)

type ycsbWorkloadConfig struct {
	Name                      string
	Table                     string
	RecordCount               int64
	FieldCount                int
	FieldLength               int
	FieldLengthDistribution   string
	ReadAllFields             bool
	WriteAllFields            bool
	ReadProportion            float64
	UpdateProportion          float64
	InsertProportion          float64
	ScanProportion            float64
	ReadModifyWriteProportion float64
	RequestDistribution       string
	MinScanLength             int64
	MaxScanLength             int64
	ScanLengthDistribution    string
	InsertOrder               string
	KeyPrefix                 string
	ZeroPadding               int
}

//go:embed workloads/workload*
var builtinYCSBWorkloads embed.FS

func resolveYCSBWorkloadName(spec string) (string, error) {
	name, ok := strings.CutPrefix(spec, "ycsb:")
	if !ok || name == "" {
		return "", fmt.Errorf("unsupported workload spec: %s", spec)
	}
	if strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return "", fmt.Errorf("workload name must be a built-in identifier, got %s", spec)
	}
	return name, nil
}

func loadBuiltinYCSBWorkload(spec string) (string, error) {
	name, err := resolveYCSBWorkloadName(spec)
	if err != nil {
		return "", err
	}

	data, err := builtinYCSBWorkloads.ReadFile("workloads/" + name)
	if err != nil {
		return "", fmt.Errorf("unsupported built-in ycsb workload: %s", spec)
	}

	return string(data), nil
}

func parseBuiltinYCSBWorkload(spec string) (*ycsbWorkloadConfig, error) {
	raw, err := loadBuiltinYCSBWorkload(spec)
	if err != nil {
		return nil, err
	}

	name, err := resolveYCSBWorkloadName(spec)
	if err != nil {
		return nil, err
	}

	props := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("invalid workload line %q", line)
		}
		props[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	if workloadType := props["workload"]; workloadType != "" && workloadType != "core" {
		return nil, fmt.Errorf("unsupported workload type %q", workloadType)
	}

	cfg := &ycsbWorkloadConfig{
		Name:                      name,
		Table:                     getStringProp(props, "table", defaultYCSBTable),
		RecordCount:               getInt64Prop(props, "recordcount", math.MaxInt32),
		FieldCount:                int(getInt64Prop(props, "fieldcount", defaultYCSBFieldCount)),
		FieldLength:               int(getInt64Prop(props, "fieldlength", defaultYCSBFieldLength)),
		FieldLengthDistribution:   strings.ToLower(getStringProp(props, "fieldlengthdistribution", defaultYCSBFieldLengthDistribution)),
		ReadAllFields:             getBoolProp(props, "readallfields", defaultYCSBReadAllFields),
		WriteAllFields:            getBoolProp(props, "writeallfields", defaultYCSBWriteAllFields),
		ReadProportion:            getFloat64Prop(props, "readproportion", 0.95),
		UpdateProportion:          getFloat64Prop(props, "updateproportion", 0.05),
		InsertProportion:          getFloat64Prop(props, "insertproportion", 0.0),
		ScanProportion:            getFloat64Prop(props, "scanproportion", 0.0),
		ReadModifyWriteProportion: getFloat64Prop(props, "readmodifywriteproportion", 0.0),
		RequestDistribution:       strings.ToLower(getStringProp(props, "requestdistribution", defaultYCSBRequestDistribution)),
		MinScanLength:             getInt64Prop(props, "minscanlength", defaultYCSBMinScanLength),
		MaxScanLength:             getInt64Prop(props, "maxscanlength", defaultYCSBMaxScanLength),
		ScanLengthDistribution:    strings.ToLower(getStringProp(props, "scanlengthdistribution", defaultYCSBScanLengthDistribution)),
		InsertOrder:               strings.ToLower(getStringProp(props, "insertorder", defaultYCSBInsertOrder)),
		KeyPrefix:                 getStringProp(props, "keyprefix", defaultYCSBKeyPrefix),
		ZeroPadding:               int(getInt64Prop(props, "zeropadding", defaultYCSBZeroPadding)),
	}

	if cfg.RecordCount <= 0 {
		return nil, fmt.Errorf("recordcount must be positive, got %d", cfg.RecordCount)
	}
	if cfg.FieldCount <= 0 {
		return nil, fmt.Errorf("fieldcount must be positive, got %d", cfg.FieldCount)
	}
	if cfg.FieldLength <= 0 {
		return nil, fmt.Errorf("fieldlength must be positive, got %d", cfg.FieldLength)
	}
	if cfg.FieldLengthDistribution != "constant" {
		return nil, fmt.Errorf("unsupported fieldlengthdistribution %q", cfg.FieldLengthDistribution)
	}
	if cfg.RequestDistribution != "uniform" && cfg.RequestDistribution != "latest" {
		return nil, fmt.Errorf("unsupported requestdistribution %q", cfg.RequestDistribution)
	}
	if cfg.ScanLengthDistribution != "uniform" {
		return nil, fmt.Errorf("unsupported scanlengthdistribution %q", cfg.ScanLengthDistribution)
	}
	if cfg.InsertOrder != "hashed" && cfg.InsertOrder != "ordered" {
		return nil, fmt.Errorf("unsupported insertorder %q", cfg.InsertOrder)
	}
	if cfg.MinScanLength <= 0 || cfg.MaxScanLength < cfg.MinScanLength {
		return nil, fmt.Errorf("invalid scan length range [%d, %d]", cfg.MinScanLength, cfg.MaxScanLength)
	}

	total := cfg.ReadProportion + cfg.UpdateProportion + cfg.InsertProportion +
		cfg.ScanProportion + cfg.ReadModifyWriteProportion
	if total <= 0 {
		return nil, fmt.Errorf("workload %s defines no operations", spec)
	}

	return cfg, nil
}

func getStringProp(props map[string]string, key string, defaultValue string) string {
	if value, ok := props[key]; ok {
		return value
	}
	return defaultValue
}

func getInt64Prop(props map[string]string, key string, defaultValue int64) int64 {
	if value, ok := props[key]; ok {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getFloat64Prop(props map[string]string, key string, defaultValue float64) float64 {
	if value, ok := props[key]; ok {
		parsed, err := strconv.ParseFloat(value, 64)
		if err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getBoolProp(props map[string]string, key string, defaultValue bool) bool {
	if value, ok := props[key]; ok {
		parsed, err := strconv.ParseBool(value)
		if err == nil {
			return parsed
		}
	}
	return defaultValue
}
