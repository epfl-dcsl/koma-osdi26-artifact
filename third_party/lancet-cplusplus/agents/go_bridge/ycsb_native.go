package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

const nativeZipfianConstant = 0.99

type nativeYCSBOperation int

const (
	nativeRead nativeYCSBOperation = iota
	nativeUpdate
	nativeInsert
	nativeScan
	nativeReadModifyWrite
)

type weightedOperation struct {
	kind       nativeYCSBOperation
	cumulative float64
}

type nativeYCSBSharedState struct {
	cfg               *ycsbWorkloadConfig
	nextInsertKey     atomic.Int64
	latestReadableKey atomic.Int64
}

type nativeYCSBThreadState struct {
	shared     *nativeYCSBSharedState
	rng        *rand.Rand
	operations []weightedOperation
	fieldNames []string
	latestDist *localZipfian
}

type etcdOperationPlan struct {
	kind         nativeYCSBOperation
	rowKey       string
	putValue     []byte
	scanCount    int64
	requestBytes uint64
	insertedKey  int64
}

type localZipfian struct {
	theta      float64
	alpha      float64
	zetan      float64
	eta        float64
	zeta2Theta float64
	count      int64
}

func newNativeYCSBSharedState(cfg *ycsbWorkloadConfig) *nativeYCSBSharedState {
	shared := &nativeYCSBSharedState{cfg: cfg}
	shared.nextInsertKey.Store(cfg.RecordCount)
	shared.latestReadableKey.Store(cfg.RecordCount - 1)
	return shared
}

func newNativeYCSBThreadState(shared *nativeYCSBSharedState, threadID int) *nativeYCSBThreadState {
	seed := time.Now().UnixNano() + int64(threadID+1)*7919
	fieldNames := make([]string, shared.cfg.FieldCount)
	for i := range fieldNames {
		fieldNames[i] = fmt.Sprintf("field%d", i)
	}

	return &nativeYCSBThreadState{
		shared:     shared,
		rng:        rand.New(rand.NewSource(seed)),
		operations: buildOperationChooser(shared.cfg),
		fieldNames: fieldNames,
		latestDist: newLocalZipfian(shared.cfg.RecordCount),
	}
}

func buildOperationChooser(cfg *ycsbWorkloadConfig) []weightedOperation {
	weights := []struct {
		kind   nativeYCSBOperation
		weight float64
	}{
		{kind: nativeRead, weight: cfg.ReadProportion},
		{kind: nativeUpdate, weight: cfg.UpdateProportion},
		{kind: nativeInsert, weight: cfg.InsertProportion},
		{kind: nativeScan, weight: cfg.ScanProportion},
		{kind: nativeReadModifyWrite, weight: cfg.ReadModifyWriteProportion},
	}

	total := 0.0
	for _, entry := range weights {
		total += entry.weight
	}
	if total <= 0 {
		return nil
	}

	ops := make([]weightedOperation, 0, len(weights))
	cumulative := 0.0
	for _, entry := range weights {
		if entry.weight <= 0 {
			continue
		}
		cumulative += entry.weight / total
		ops = append(ops, weightedOperation{
			kind:       entry.kind,
			cumulative: cumulative,
		})
	}
	ops[len(ops)-1].cumulative = 1.0
	return ops
}

func (s *nativeYCSBThreadState) nextOperation() (etcdOperationPlan, error) {
	switch s.chooseOperation() {
	case nativeRead:
		return s.buildReadPlan()
	case nativeUpdate:
		return s.buildUpdatePlan()
	case nativeInsert:
		return s.buildInsertPlan()
	case nativeScan:
		return s.buildScanPlan()
	default:
		return s.buildReadModifyWritePlan()
	}
}

func (s *nativeYCSBThreadState) chooseOperation() nativeYCSBOperation {
	if len(s.operations) == 0 {
		return nativeRead
	}

	draw := s.rng.Float64()
	for _, entry := range s.operations {
		if draw <= entry.cumulative {
			return entry.kind
		}
	}
	return s.operations[len(s.operations)-1].kind
}

func (s *nativeYCSBThreadState) buildReadPlan() (etcdOperationPlan, error) {
	rowKey := s.rowKeyForKeyNum(s.chooseExistingKeyNum())
	return etcdOperationPlan{
		kind:         nativeRead,
		rowKey:       rowKey,
		requestBytes: uint64(len(rowKey)),
	}, nil
}

func (s *nativeYCSBThreadState) buildUpdatePlan() (etcdOperationPlan, error) {
	rowKey := s.rowKeyForKeyNum(s.chooseExistingKeyNum())
	putValue, err := s.buildEncodedValues()
	if err != nil {
		return etcdOperationPlan{}, err
	}
	return etcdOperationPlan{
		kind:         nativeUpdate,
		rowKey:       rowKey,
		putValue:     putValue,
		requestBytes: uint64(len(rowKey) + len(putValue)),
	}, nil
}

func (s *nativeYCSBThreadState) buildInsertPlan() (etcdOperationPlan, error) {
	keyNum := s.shared.nextInsertKey.Add(1) - 1
	rowKey := s.rowKeyForKeyNum(keyNum)
	putValue, err := s.buildAllFieldValues()
	if err != nil {
		return etcdOperationPlan{}, err
	}
	return etcdOperationPlan{
		kind:         nativeInsert,
		rowKey:       rowKey,
		putValue:     putValue,
		requestBytes: uint64(len(rowKey) + len(putValue)),
		insertedKey:  keyNum,
	}, nil
}

func (s *nativeYCSBThreadState) buildScanPlan() (etcdOperationPlan, error) {
	rowKey := s.rowKeyForKeyNum(s.chooseExistingKeyNum())
	scanCount := s.chooseScanLength()
	return etcdOperationPlan{
		kind:         nativeScan,
		rowKey:       rowKey,
		scanCount:    scanCount,
		requestBytes: uint64(len(rowKey) + 8),
	}, nil
}

func (s *nativeYCSBThreadState) buildReadModifyWritePlan() (etcdOperationPlan, error) {
	rowKey := s.rowKeyForKeyNum(s.chooseExistingKeyNum())
	putValue, err := s.buildEncodedValues()
	if err != nil {
		return etcdOperationPlan{}, err
	}
	return etcdOperationPlan{
		kind:         nativeReadModifyWrite,
		rowKey:       rowKey,
		putValue:     putValue,
		requestBytes: uint64((2 * len(rowKey)) + len(putValue)),
	}, nil
}

func (s *nativeYCSBThreadState) chooseExistingKeyNum() int64 {
	switch s.shared.cfg.RequestDistribution {
	case "latest":
		maxKey := s.shared.latestReadableKey.Load()
		if maxKey <= 0 {
			return 0
		}
		offset := s.latestDist.Next(s.rng, maxKey+1)
		if offset > maxKey {
			offset = maxKey
		}
		return maxKey - offset
	default:
		return s.rng.Int63n(s.shared.cfg.RecordCount)
	}
}

func (s *nativeYCSBThreadState) chooseScanLength() int64 {
	if s.shared.cfg.MinScanLength == s.shared.cfg.MaxScanLength {
		return s.shared.cfg.MinScanLength
	}
	span := s.shared.cfg.MaxScanLength - s.shared.cfg.MinScanLength + 1
	return s.shared.cfg.MinScanLength + s.rng.Int63n(span)
}

func (s *nativeYCSBThreadState) buildEncodedValues() ([]byte, error) {
	if s.shared.cfg.WriteAllFields {
		return s.buildAllFieldValues()
	}
	return s.buildSingleFieldValue()
}

func (s *nativeYCSBThreadState) buildAllFieldValues() ([]byte, error) {
	values := make(map[string][]byte, len(s.fieldNames))
	for _, fieldName := range s.fieldNames {
		values[fieldName] = s.randomValue()
	}
	return json.Marshal(values)
}

func (s *nativeYCSBThreadState) buildSingleFieldValue() ([]byte, error) {
	fieldName := s.fieldNames[s.rng.Intn(len(s.fieldNames))]
	values := map[string][]byte{
		fieldName: s.randomValue(),
	}
	return json.Marshal(values)
}

func (s *nativeYCSBThreadState) randomValue() []byte {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	buf := make([]byte, s.shared.cfg.FieldLength)
	for i := range buf {
		buf[i] = alphabet[s.rng.Intn(len(alphabet))]
	}
	return buf
}

func (s *nativeYCSBThreadState) rowKeyForKeyNum(keyNum int64) string {
	keyName := buildYCSBKeyName(s.shared.cfg, keyNum)
	return s.shared.cfg.Table + ":" + keyName
}

func buildYCSBKeyName(cfg *ycsbWorkloadConfig, keyNum int64) string {
	if cfg.InsertOrder != "ordered" {
		keyNum = hash64(keyNum)
	}
	return fmt.Sprintf("%s%0*d", cfg.KeyPrefix, cfg.ZeroPadding, keyNum)
}

func hash64(n int64) int64 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	hash := fnv.New64a()
	_, _ = hash.Write(b[:])
	value := int64(hash.Sum64())
	if value < 0 {
		return -value
	}
	return value
}

func newLocalZipfian(items int64) *localZipfian {
	if items < 1 {
		items = 1
	}

	z := &localZipfian{
		theta: nativeZipfianConstant,
		count: items,
	}
	z.zeta2Theta = zetaStatic(0, 2, z.theta, 0)
	z.alpha = 1.0 / (1.0 - z.theta)
	z.zetan = zetaStatic(0, items, z.theta, 0)
	z.eta = computeZipfianEta(items, z.theta, z.zeta2Theta, z.zetan)
	return z
}

func (z *localZipfian) Next(r *rand.Rand, itemCount int64) int64 {
	if itemCount <= 1 {
		return 0
	}

	if itemCount != z.count {
		if itemCount > z.count {
			z.zetan = zetaStatic(z.count, itemCount, z.theta, z.zetan)
		} else {
			z.zetan = zetaStatic(0, itemCount, z.theta, 0)
		}
		z.count = itemCount
		z.eta = computeZipfianEta(itemCount, z.theta, z.zeta2Theta, z.zetan)
	}

	u := r.Float64()
	uz := u * z.zetan
	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+math.Pow(0.5, z.theta) {
		return 1
	}

	value := int64(float64(itemCount) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	if value >= itemCount {
		return itemCount - 1
	}
	if value < 0 {
		return 0
	}
	return value
}

func computeZipfianEta(items int64, theta float64, zeta2Theta float64, zetan float64) float64 {
	return (1 - math.Pow(2.0/float64(items), 1-theta)) / (1 - zeta2Theta/zetan)
}

func zetaStatic(start int64, n int64, theta float64, initial float64) float64 {
	sum := initial
	for i := start; i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta)
	}
	return sum
}
