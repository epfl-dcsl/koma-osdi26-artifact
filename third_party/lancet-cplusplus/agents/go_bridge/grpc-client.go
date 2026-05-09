// grpc-client.go provides a small Go bridge that Lancet can call via cgo.
// It supports the existing echo flow and a native etcd + YCSB-style path
// that keeps request generation thread-local while Lancet remains in control
// of pacing and runtime.

package main

/*
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"
	proto "main/proto.com"

	"google.golang.org/grpc"
	"runtime/debug"
)

const (
	MaxPerThreadSamples      = 131072
	txTimestampSamplingEvery = 100

	throughputAgent = 0
	latencyAgent    = 1
)

type Timespec struct {
	Tv_sec  int64
	Tv_nsec int64
}

type byte_req_pair struct {
	bytes uint64
	reqs  uint64
}

type TxSamples struct {
	Count   uint32
	Samples [MaxPerThreadSamples]Timespec
}

type LatSample struct {
	nsec uint64
	tx   Timespec
}

type throughputStats struct {
	rx byte_req_pair
	tx byte_req_pair
}

type LatencyStats struct {
	th_s    throughputStats
	inc_idx uint32
	samples [MaxPerThreadSamples]LatSample
}

type agent_control_block struct {
	idist              [52]byte
	should_load        int32
	should_measure     int32
	thread_count       int32
	agent_type         int32
	per_thread_samples int32
	sampling           float64
	conn_open          int32
}

type EchoClientConn struct {
	conn   *grpc.ClientConn
	client proto.EchoClient
}

type ThreadContext struct {
	threadID    int
	appProto    string
	workload    string
	agentType   int32
	syncErrLogs uint32
	targets     []string
	echoClients []*EchoClientConn
	etcdClients []*clientv3.Client
	etcdYCSB    *nativeYCSBThreadState

	throughputStats *throughputStats
	latencyStats    *LatencyStats
	txSamples       *TxSamples

	txSampleSelector    uint32
	txTimestampSelector uint32
	prevTxTimestamp     Timespec

	connIdx uint32
	pending []uint32
	payload []byte
	iaRand  *rand.Rand
}

var (
	threadTable    = make(map[int]*ThreadContext)
	threadTableMu  sync.Mutex
	global_acb     *agent_control_block
	MaxPendingReqs uint32

	nativeWorkloadMu   sync.Mutex
	nativeWorkloadSpec string
	nativeWorkload     *nativeYCSBSharedState
)

func parseTargetsCSV(targetsCSV string) []string {
	targets := make([]string, 0)
	for _, target := range strings.Split(targetsCSV, ",") {
		if target == "" {
			continue
		}
		targets = append(targets, target)
	}
	return targets
}

func getOrCreateNativeWorkload(spec string) (*nativeYCSBSharedState, error) {
	nativeWorkloadMu.Lock()
	defer nativeWorkloadMu.Unlock()

	if nativeWorkload != nil {
		if nativeWorkloadSpec != spec {
			return nil, fmt.Errorf("multiple workload specs in one process are unsupported: %s vs %s", nativeWorkloadSpec, spec)
		}
		return nativeWorkload, nil
	}

	cfg, err := parseBuiltinYCSBWorkload(spec)
	if err != nil {
		return nil, err
	}

	nativeWorkloadSpec = spec
	nativeWorkload = newNativeYCSBSharedState(cfg)
	return nativeWorkload, nil
}

func initEchoClients(tc *ThreadContext, targets []string, connPerThread int) error {
	for i := 0; i < connPerThread; i++ {
		addr := targets[i%len(targets)]
		conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithInitialConnWindowSize(126553500))
		if err != nil {
			return err
		}

		tc.echoClients = append(tc.echoClients, &EchoClientConn{
			conn:   conn,
			client: proto.NewEchoClient(conn),
		})
	}
	return nil
}

func initEtcdClients(tc *ThreadContext, targets []string, connPerThread int) error {
	workloadSpec := tc.workload
	if workloadSpec == "" {
		workloadSpec = "ycsb:workloada"
	}

	shared, err := getOrCreateNativeWorkload(workloadSpec)
	if err != nil {
		return err
	}
	tc.etcdYCSB = newNativeYCSBThreadState(shared, tc.threadID)

	for i := 0; i < connPerThread; i++ {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{targets[i%len(targets)]},
			DialTimeout: 2 * time.Second,
		})
		if err != nil {
			return err
		}
		tc.etcdClients = append(tc.etcdClients, client)
	}
	return nil
}

func (tc *ThreadContext) shouldMeasure() bool {
	return global_acb != nil && atomic.LoadInt32(&global_acb.should_measure) != 0
}

func (tc *ThreadContext) shouldLoad() bool {
	return global_acb != nil && atomic.LoadInt32(&global_acb.should_load) != 0
}

func (tc *ThreadContext) add_throughput_tx_sample(p byte_req_pair) {
	if !tc.shouldMeasure() || tc.throughputStats == nil {
		return
	}
	atomic.AddUint64(&tc.throughputStats.tx.bytes, p.bytes)
	atomic.AddUint64(&tc.throughputStats.tx.reqs, p.reqs)
}

func (tc *ThreadContext) add_throughput_rx_sample(p byte_req_pair) {
	if !tc.shouldMeasure() || tc.throughputStats == nil {
		return
	}
	atomic.AddUint64(&tc.throughputStats.rx.bytes, p.bytes)
	atomic.AddUint64(&tc.throughputStats.rx.reqs, p.reqs)
}

func (tc *ThreadContext) AddLatencySample(diff uint64) {
	if diff == 0 || !tc.shouldMeasure() || tc.latencyStats == nil {
		return
	}

	sel := atomic.AddUint32(&tc.txSampleSelector, 1) - 1
	if sel%uint32(math.Round(1.0/global_acb.sampling)) != 0 {
		return
	}

	idx := atomic.AddUint32(&tc.latencyStats.inc_idx, 1) - 1
	slot := &tc.latencyStats.samples[idx%MaxPerThreadSamples]
	slot.nsec = diff
}

func (tc *ThreadContext) addTxTimestamp(now Timespec) {
	if !tc.shouldMeasure() || tc.txSamples == nil {
		tc.prevTxTimestamp = now
		return
	}

	sel := atomic.AddUint32(&tc.txTimestampSelector, 1) - 1
	if sel%txTimestampSamplingEvery != 0 {
		tc.prevTxTimestamp = now
		return
	}

	diff, ok := diffTimespec(now, tc.prevTxTimestamp)
	tc.prevTxTimestamp = now
	if !ok {
		return
	}

	idx := atomic.AddUint32(&tc.txSamples.Count, 1) - 1
	tc.txSamples.Samples[idx%MaxPerThreadSamples] = diff
}

func diffTimespec(a Timespec, b Timespec) (Timespec, bool) {
	if a.Tv_sec == 0 || b.Tv_sec == 0 {
		return Timespec{}, false
	}

	const billion int64 = 1_000_000_000
	if a.Tv_nsec < b.Tv_nsec {
		return Timespec{
			Tv_sec:  a.Tv_sec - 1 - b.Tv_sec,
			Tv_nsec: billion - (b.Tv_nsec - a.Tv_nsec),
		}, true
	}

	return Timespec{
		Tv_sec:  a.Tv_sec - b.Tv_sec,
		Tv_nsec: a.Tv_nsec - b.Tv_nsec,
	}, true
}

func lookupThreadContext(threadID int) *ThreadContext {
	threadTableMu.Lock()
	defer threadTableMu.Unlock()
	return threadTable[threadID]
}

func getNowTimespec() Timespec {
	now := time.Now()
	return Timespec{
		Tv_sec:  now.Unix(),
		Tv_nsec: int64(now.Nanosecond()),
	}
}

func (tc *ThreadContext) getIA() int64 {
	y := tc.iaRand.Float64()
	aBits := binary.LittleEndian.Uint64(global_acb.idist[28:36])
	a := math.Float64frombits(aBits)
	val := -math.Log(y) / a
	return int64(math.Round(val * 1000))
}

func (tc *ThreadContext) pickConnectionIndex() int {
	n := len(tc.pending)
	if n == 0 {
		return -1
	}

	idx := atomic.AddUint32(&tc.connIdx, 1) - 1
	connIdx := int(idx % uint32(n))
	if atomic.LoadUint32(&tc.pending[connIdx]) >= MaxPendingReqs {
		return -1
	}
	return connIdx
}

func (tc *ThreadContext) nextConnectionIndex() int {
	switch tc.appProto {
	case "etcd":
		if len(tc.etcdClients) == 0 {
			return -1
		}
		return int(atomic.AddUint32(&tc.connIdx, 1)-1) % len(tc.etcdClients)
	default:
		if len(tc.echoClients) == 0 {
			return -1
		}
		return int(atomic.AddUint32(&tc.connIdx, 1)-1) % len(tc.echoClients)
	}
}

func (tc *ThreadContext) sendEchoAsync(connID int) {
	if connID < 0 || connID >= len(tc.echoClients) {
		return
	}

	atomic.AddUint32(&tc.pending[connID], 1)
	tc.add_throughput_tx_sample(byte_req_pair{
		bytes: uint64(len(tc.payload)),
		reqs:  1,
	})
	tc.addTxTimestamp(getNowTimespec())

	cl := tc.echoClients[connID]
	go func() {
		defer atomic.AddUint32(&tc.pending[connID], ^uint32(0))

		start := time.Now()
		response, err := cl.client.SendEcho(context.Background(), &proto.EchoRequest{
			Message: string(tc.payload),
		})
		elapsed := time.Since(start).Nanoseconds()
		if err != nil || response == nil {
			return
		}

		tc.add_throughput_rx_sample(byte_req_pair{
			bytes: uint64(len(response.Message)),
			reqs:  1,
		})
		tc.AddLatencySample(uint64(elapsed))
	}()
}

func executeEtcdOperation(ctx context.Context, client *clientv3.Client, shared *nativeYCSBSharedState, plan etcdOperationPlan) (uint64, error) {
	switch plan.kind {
	case nativeRead:
		return etcdReadBytes(ctx, client, plan.rowKey)
	case nativeUpdate:
		return 0, etcdPut(ctx, client, plan.rowKey, plan.putValue)
	case nativeInsert:
		if err := etcdPut(ctx, client, plan.rowKey, plan.putValue); err != nil {
			return 0, err
		}
		publishLatestReadable(shared, plan.insertedKey)
		return 0, nil
	case nativeScan:
		return etcdScanBytes(ctx, client, plan.rowKey, plan.scanCount)
	case nativeReadModifyWrite:
		readBytes, err := etcdReadBytes(ctx, client, plan.rowKey)
		if err != nil {
			return 0, err
		}
		if err := etcdPut(ctx, client, plan.rowKey, plan.putValue); err != nil {
			return 0, err
		}
		return readBytes, nil
	default:
		return 0, fmt.Errorf("unsupported etcd operation %d", plan.kind)
	}
}

func etcdReadBytes(ctx context.Context, client *clientv3.Client, rowKey string) (uint64, error) {
	resp, err := client.Get(ctx, rowKey, clientv3.WithSerializable())
	if err != nil {
		return 0, err
	}
	if resp.Count == 0 {
		return 0, fmt.Errorf("could not find value for key [%s]", rowKey)
	}
	return uint64(len(resp.Kvs[0].Value)), nil
}

func etcdScanBytes(ctx context.Context, client *clientv3.Client, rowKey string, count int64) (uint64, error) {
	resp, err := client.Get(
		ctx,
		rowKey,
		clientv3.WithFromKey(),
		clientv3.WithLimit(count),
		clientv3.WithSerializable(),
	)
	if err != nil {
		return 0, err
	}
	if resp.Count != count {
		return 0, fmt.Errorf("unexpected number of result for key [%s], expected %d but was %d", rowKey, count, resp.Count)
	}

	var total uint64
	for _, kv := range resp.Kvs {
		total += uint64(len(kv.Value))
	}
	return total, nil
}

func etcdPut(ctx context.Context, client *clientv3.Client, rowKey string, value []byte) error {
	_, err := client.Put(ctx, rowKey, string(value))
	return err
}

func publishLatestReadable(shared *nativeYCSBSharedState, key int64) {
	for {
		current := shared.latestReadableKey.Load()
		if key <= current {
			return
		}
		if shared.latestReadableKey.CompareAndSwap(current, key) {
			return
		}
	}
}

func (tc *ThreadContext) sendEtcdAsync(connID int, plan etcdOperationPlan) {
	if connID < 0 || connID >= len(tc.etcdClients) {
		return
	}

	atomic.AddUint32(&tc.pending[connID], 1)
	tc.add_throughput_tx_sample(byte_req_pair{
		bytes: plan.requestBytes,
		reqs:  1,
	})
	tc.addTxTimestamp(getNowTimespec())

	client := tc.etcdClients[connID]
	go func() {
		defer atomic.AddUint32(&tc.pending[connID], ^uint32(0))

		start := time.Now()
		readBytes, err := executeEtcdOperation(context.Background(), client, tc.etcdYCSB.shared, plan)
		elapsed := time.Since(start).Nanoseconds()
		if err != nil {
			return
		}

		tc.add_throughput_rx_sample(byte_req_pair{
			bytes: readBytes,
			reqs:  1,
		})
		tc.AddLatencySample(uint64(elapsed))
	}()
}

func (tc *ThreadContext) doEchoSync(connID int) {
	if connID < 0 || connID >= len(tc.echoClients) {
		return
	}

	reqBytes := uint64(len(tc.payload))
	tc.add_throughput_tx_sample(byte_req_pair{bytes: reqBytes, reqs: 1})
	tc.addTxTimestamp(getNowTimespec())

	start := time.Now()
	reply, err := tc.echoClients[connID].client.SendEcho(context.Background(), &proto.EchoRequest{
		Message: string(tc.payload),
	})
	elapsed := time.Since(start).Nanoseconds()
	if err != nil {
		if atomic.AddUint32(&tc.syncErrLogs, 1) <= 10 {
			fmt.Printf("sync echo RPC failed on thread %d conn %d after %d ns: %v\n", tc.threadID, connID, elapsed, err)
		}
		return
	}
	if reply == nil {
		if atomic.AddUint32(&tc.syncErrLogs, 1) <= 10 {
			fmt.Printf("sync echo RPC returned nil reply on thread %d conn %d after %d ns\n", tc.threadID, connID, elapsed)
		}
		return
	}

	tc.add_throughput_rx_sample(byte_req_pair{bytes: uint64(len(reply.Message)), reqs: 1})
	tc.AddLatencySample(uint64(elapsed))
}

func (tc *ThreadContext) doEtcdSync(connID int, plan etcdOperationPlan) {
	if connID < 0 || connID >= len(tc.etcdClients) {
		return
	}

	tc.add_throughput_tx_sample(byte_req_pair{bytes: plan.requestBytes, reqs: 1})
	tc.addTxTimestamp(getNowTimespec())

	start := time.Now()
	readBytes, err := executeEtcdOperation(context.Background(), tc.etcdClients[connID], tc.etcdYCSB.shared, plan)
	elapsed := time.Since(start).Nanoseconds()
	if err != nil {
		if atomic.AddUint32(&tc.syncErrLogs, 1) <= 10 {
			fmt.Printf("sync etcd RPC failed on thread %d conn %d after %d ns: %v\n", tc.threadID, connID, elapsed, err)
		}
		return
	}

	tc.add_throughput_rx_sample(byte_req_pair{bytes: readBytes, reqs: 1})
	tc.AddLatencySample(uint64(elapsed))
}

func (tc *ThreadContext) runThroughputLoop() {
	nextTx := time.Now().UnixNano()

	for {
		if !tc.shouldLoad() {
			nextTx = time.Now().UnixNano()
			continue
		}

		now := time.Now().UnixNano()
		for now >= nextTx {
			connIdx := tc.pickConnectionIndex()
			if connIdx < 0 {
				break
			}

			switch tc.appProto {
			case "echo":
				tc.sendEchoAsync(connIdx)
			case "etcd":
				plan, err := tc.etcdYCSB.nextOperation()
				if err != nil {
					fmt.Printf("failed to generate etcd operation: %v\n", err)
					break
				}
				tc.sendEtcdAsync(connIdx, plan)
			}

			nextTx += tc.getIA()
			now = time.Now().UnixNano()
		}
	}
}

func (tc *ThreadContext) runLatencyLoop() {
	nextTx := time.Now().UnixNano()

	for {
		if !tc.shouldLoad() {
			nextTx = time.Now().UnixNano()
			continue
		}

		if time.Now().UnixNano() < nextTx {
			continue
		}

		connIdx := tc.nextConnectionIndex()
		if connIdx < 0 {
			continue
		}

		switch tc.appProto {
		case "echo":
			tc.doEchoSync(connIdx)
		case "etcd":
			plan, err := tc.etcdYCSB.nextOperation()
			if err != nil {
				fmt.Printf("failed to generate etcd operation: %v\n", err)
				continue
			}
			tc.doEtcdSync(connIdx, plan)
		}

		nextTx += tc.getIA()
	}
}

//export initThreadContext
func initThreadContext(threadID C.int, connPerThread C.int, targetsCSV *C.char,
	appProto *C.char, workload *C.char,
	threadStatsPtr C.uintptr_t, txSamplesPtr C.uintptr_t, acb C.uintptr_t,
	maxpendingreqs C.int, payloadSize C.int, agentType C.int) (ret C.int) {
	appProtoName := C.GoString(appProto)
	targets := parseTargetsCSV(C.GoString(targetsCSV))
	threadTableMu.Lock()
	defer threadTableMu.Unlock()

	if _, ok := threadTable[int(threadID)]; ok {
		return 0
	}
	if len(targets) == 0 {
		fmt.Printf("no GRPC_GO targets provided\n")
		return -1
	}

	global_acb = (*agent_control_block)(unsafe.Pointer(uintptr(acb)))

	tc := &ThreadContext{
		threadID:  int(threadID),
		appProto:  appProtoName,
		workload:  C.GoString(workload),
		agentType: int32(agentType),
		targets:   targets,
		txSamples: (*TxSamples)(unsafe.Pointer(uintptr(txSamplesPtr))),
		pending:   make([]uint32, int(connPerThread)),
		payload:   make([]byte, int(payloadSize)),
		iaRand:    rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID+1)*104729)),
	}

	if int32(agentType) == throughputAgent {
		tc.throughputStats = (*throughputStats)(unsafe.Pointer(uintptr(threadStatsPtr)))
	} else {
		tc.latencyStats = (*LatencyStats)(unsafe.Pointer(uintptr(threadStatsPtr)))
		tc.throughputStats = &tc.latencyStats.th_s
	}

	MaxPendingReqs = uint32(maxpendingreqs)

	if len(tc.payload) > 0 {
		tc.payload[0] = 'A'
		for i := 1; i < len(tc.payload); i++ {
			tc.payload[i] = '#'
		}
	}

	switch appProtoName {
	case "echo":
		if err := initEchoClients(tc, targets, int(connPerThread)); err != nil {
			fmt.Printf("failed to initialize echo GRPC_GO clients: %v\n", err)
			return -1
		}
	case "etcd":
		if err := initEtcdClients(tc, targets, int(connPerThread)); err != nil {
			fmt.Printf("failed to initialize etcd GRPC_GO clients: %v\n", err)
			return -1
		}
	default:
		fmt.Printf("unsupported GRPC_GO app proto in current bridge: %s\n", appProtoName)
		return -1
	}

	threadTable[int(threadID)] = tc
	return 0
}

//export SendEchoAsync
func SendEchoAsync(threadID int, connID int) {
	tc := lookupThreadContext(threadID)
	if tc == nil {
		return
	}
	tc.sendEchoAsync(connID)
}

//export throughputGRPCGoMain
func throughputGRPCGoMain(threadID int) {
	tc := lookupThreadContext(threadID)
	if tc == nil {
		return
	}
	tc.runThroughputLoop()
}

//export latencyGRPCGoMain
func latencyGRPCGoMain(threadID int) {
	tc := lookupThreadContext(threadID)
	if tc == nil {
		return
	}
	tc.runLatencyLoop()
}

//export symmetricGRPCGoMain
func symmetricGRPCGoMain(threadID int) {
	tc := lookupThreadContext(threadID)
	if tc == nil {
		return
	}
	tc.runThroughputLoop()
}

func init() {
	debug.SetGCPercent(-1)
}
