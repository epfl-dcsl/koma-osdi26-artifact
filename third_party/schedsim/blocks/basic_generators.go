package blocks

import (
	"container/heap"
	"math/rand"
	"time"

	"gitlab.epfl.ch/ryang/schedsim/engine"
)

// Generator interface describes how a generator behaves when describing
// a topology
type Generator interface {
	engine.ActorInterface
	SetCreator(ReqCreator)
	SetConnHeapPtr(*engine.ConnHeap)
}

type genericGenerator struct {
	engine.Actor
	Creator     ReqCreator
	ServiceTime randDist
	WaitTime    randDist
	ConnHeapPtr *engine.ConnHeap
}

func (g *genericGenerator) SetCreator(rc ReqCreator) {
	g.Creator = rc
}

func (g *genericGenerator) SetConnHeapPtr(p *engine.ConnHeap) {
	g.ConnHeapPtr = p
}

// TODO: create a new gerator type for connheap?
type randGenerator struct {
	genericGenerator
}

// random
func (g *randGenerator) Run() {
	for {
		req := g.Creator.NewRequest(g.ServiceTime.GetRand())
		qIdx := rand.Intn(g.GetOutQueueCount())

		// Check if the request is a ConnReq
		if connReq, ok := req.(*ConnReq); ok {
			connReq.ConnID = qIdx
			if g.GetOutQueueLen(connReq.ConnID) == 0 && !g.GetOutQueueinProcess(connReq.ConnID) {
				// add the req info to the min-heap
				hItem := &engine.ConnHeapItem{ConnID: connReq.ConnID, InitTime: connReq.InitTime}
				heap.Push(g.ConnHeapPtr, hItem)
			}
		}

		if monitorReq, ok := req.(*MonitorReq); ok {
			monitorReq.initLength = g.GetAllOutQueueLens()[qIdx]
		}
		g.WriteOutQueueI(req, qIdx)
		waitT := g.WaitTime.GetRand()
		g.Wait(waitT)
	}
}

type rRGenerator struct {
	genericGenerator
}

// round robin
func (g *rRGenerator) Run() {
	for count := 0; ; count++ {
		req := g.Creator.NewRequest(g.ServiceTime.GetRand())
		qIdx := count % g.GetOutQueueCount()
		// Check if the request is a ConnReq
		if connReq, ok := req.(*ConnReq); ok {
			connReq.ConnID = qIdx
			if g.GetOutQueueLen(connReq.ConnID) == 0 && !g.GetOutQueueinProcess(connReq.ConnID) {
				// add the req info to the min-heap
				hItem := &engine.ConnHeapItem{ConnID: connReq.ConnID, InitTime: connReq.InitTime}
				heap.Push(g.ConnHeapPtr, hItem)
			}
		}
		g.WriteOutQueueI(req, qIdx)
		g.Wait(g.WaitTime.GetRand())
	}
}

type connGenerator struct {
	genericGenerator
	connNum int
}

// generator for specific connection_num. Note this generator is only used for
// conn_multi_queue model but not conn_single_queue model. The reason is
// because in conn_multi_queue model, we already need to put it in a specific
// queue (1-to-1 mapped with the processor)
func (g *connGenerator) Run() {
	for count := 0; ; count++ {
		req := g.Creator.NewRequest(g.ServiceTime.GetRand())
		qIdx := rand.Intn(g.connNum) % g.GetOutQueueCount()
		// Check if the request is a ConnReq
		if connReq, ok := req.(*ConnReq); ok {
			connReq.ConnID = qIdx
			if g.GetOutQueueLen(connReq.ConnID) == 0 && !g.GetOutQueueinProcess(connReq.ConnID) {
				// add the req info to the min-heap
				hItem := &engine.ConnHeapItem{ConnID: connReq.ConnID, InitTime: connReq.InitTime}
				heap.Push(g.ConnHeapPtr, hItem)
			}
		}
		g.WriteOutQueueI(req, qIdx)
		g.Wait(g.WaitTime.GetRand())
	}
}

// DDGenerator is a fixed waiting time generator that produces fixed service time requests
type DDGenerator struct {
	rRGenerator
}

// NewDDGenerator returns a DDGenerator
func NewDDGenerator(waitTime, serviceTime float64) *DDGenerator {
	g := &DDGenerator{}
	g.ServiceTime = newDeterministicDistr(serviceTime)
	g.WaitTime = newDeterministicDistr(waitTime)
	return g
}

// MDGenerator is a exponential waiting time generator that produces fixed service time requests
// If multiple queues they are fed round robin
type MDGenerator struct {
	rRGenerator
}

// NewMDGenerator returns a MDGenerator
func NewMDGenerator(waitLambda float64, serviceTime float64) *MDGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MDGenerator{}
	g.ServiceTime = newDeterministicDistr(serviceTime)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MDRandGenerator is a exponential waiting time generator that produces fixed service time requests
// If multiple queues they are fed randomly
type MDRandGenerator struct {
	randGenerator
}

// NewMDRandGenerator returns a MDRandGenerator
func NewMDRandGenerator(waitLambda float64, serviceTime float64) *MDRandGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MDRandGenerator{}
	g.WaitTime = newExponDistr(waitLambda)
	g.ServiceTime = newDeterministicDistr(serviceTime)
	return g
}

// MDConnGenerator is a exponential waiting time generator that produces fixed service time requests
type MDConnGenerator struct {
	connGenerator
}

// NewMDRandGenerator returns a MDRandGenerator
func NewMDConnGenerator(waitLambda float64, serviceTime float64, connNum int) *MDConnGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MDConnGenerator{}
	g.connNum = connNum
	g.WaitTime = newExponDistr(waitLambda)
	g.ServiceTime = newDeterministicDistr(serviceTime)
	return g
}

// MMGenerator is a exponential waiting time generator that produces exponential service time requests
// If multiple queues they are fed round robin
type MMGenerator struct {
	rRGenerator
}

// NewMMGenerator returns a MMGenerator
func NewMMGenerator(waitLambda float64, serviceMu float64) *MMGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MMGenerator{}
	g.ServiceTime = newExponDistr(serviceMu)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MMRandGenerator is a exponential waiting time generator that produces exponential service time requests
// If multiple queues they are fed randomly
type MMRandGenerator struct {
	randGenerator
}

// NewMMRandGenerator returns a MMRandGenerator
func NewMMRandGenerator(waitLambda float64, serviceMu float64) *MMRandGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MMRandGenerator{}
	g.ServiceTime = newExponDistr(serviceMu)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MMConnGenerator is a exponential waiting time generator that produces exponential service time requests
// If multiple queues they are fed randomly
type MMConnGenerator struct {
	connGenerator
}

// NewMMRandGenerator returns a MMRandGenerator
func NewMMConnGenerator(waitLambda float64, serviceMu float64, connNum int) *MMConnGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MMConnGenerator{}
	g.connNum = connNum
	g.ServiceTime = newExponDistr(serviceMu)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MLNGenerator is exponential waiting time lognormal service time generator
// If multiple queues they are fed round robin
type MLNGenerator struct {
	rRGenerator
}

// NewMLNGenerator returns an MLNGenerator
func NewMLNGenerator(waitLambda, mu, sigma float64) *MLNGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MLNGenerator{}
	g.ServiceTime = newLGDistr(mu, sigma)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MBGenerator is a poisson interarrival generator with
// requests with bimodal service times (2 values)
// If multiple queues they are fed roundrobin
type MBGenerator struct {
	rRGenerator
}

// NewMBGenerator returns a MBGenerator
func NewMBGenerator(waitLambda, peak1, peak2, ratio float64) *MBGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MBGenerator{}
	g.ServiceTime = newBiDistr(peak1, peak2, ratio)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MBRandGenerator is a poisson interarrival generator with
// requests with bimodal service times (2 values)
// If multiple queues they are fed randomly
type MBRandGenerator struct {
	randGenerator
}

// NewMBRandGenerator returns a new MBRandGenerator
func NewMBRandGenerator(waitLambda, peak1, peak2, ratio float64) *MBRandGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MBRandGenerator{}
	g.ServiceTime = newBiDistr(peak1, peak2, ratio)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}

// MBConnGenerator is a poisson interarrival generator with
// requests with bimodal service times (2 values)
type MBConnGenerator struct {
	connGenerator
}

// NewMBRandGenerator returns a new MBRandGenerator
func NewMBConnGenerator(waitLambda, peak1, peak2, ratio float64, connNum int) *MBConnGenerator {
	// Seed with time
	rand.Seed(time.Now().UTC().UnixNano())

	g := &MBConnGenerator{}
	g.connNum = connNum
	g.ServiceTime = newBiDistr(peak1, peak2, ratio)
	g.WaitTime = newExponDistr(waitLambda)
	return g
}
