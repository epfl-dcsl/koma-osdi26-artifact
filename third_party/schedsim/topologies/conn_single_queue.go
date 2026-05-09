package topologies

import (
	"container/heap"
	"fmt"

	"gitlab.epfl.ch/ryang/schedsim/blocks"
	"gitlab.epfl.ch/ryang/schedsim/engine"
)

// ConnSingleQueue described a topology where there are multiple processors, multiple connection-based request stream, and a single queue abstraction. Each processor has access to each connection-based stream, and process all requests from different connections in FIFO manner. The constraint here is that only one processor can process requests from a given connection at one time.
func ConnSingleQueue(lambda, mu, duration float64, genType, procType, connNum int, cores int) {
	engine.InitSim()

	// Init the statistics
	// stats := blocks.NewBookKeeper()
	stats := &blocks.AllKeeper{}
	stats.SetName("Main Stats")
	engine.InitStats(stats)

	// Create min-heap
	h := &engine.ConnHeap{}
	heap.Init(h)

	// Add generator
	var g blocks.Generator
	switch genType {
	case 0:
		g = blocks.NewMMRandGenerator(lambda, mu)
	case 1:
		g = blocks.NewMDRandGenerator(lambda, 1/mu)
	case 2:
		g = blocks.NewMBRandGenerator(lambda, 0.5*(1/mu), 5.5*(1/mu), 0.9)
	case 3:
		g = blocks.NewMBRandGenerator(lambda, 0.5*(1/mu), 500*(1/mu), 0.999)
	}

	g.SetCreator(&blocks.ConnReqCreator{ConnNum: connNum})
	g.SetConnHeapPtr(h)

	// Create queues, which has the same number of connections (connNum)
	fastQueues := make([]engine.QueueInterface, connNum)
	for i := range fastQueues {
		fastQueues[i] = blocks.NewQueue()
	}

	// Create processors
	processors := make([]blocks.Processor, cores)

	// first the slow cores
	for i := 0; i < cores; i++ {
		switch procType {
		case 0:
			processors[i] = &blocks.ConnProcessor{Index: i, ConnHeapPtr: h}
		case 1:
			// TODO: not implemented yet!
			processors[i] = blocks.NewPSProcessor()
		}
	}

	// Connect the fast queues
	for _, q := range fastQueues {
		// add out queues to the request generator
		g.AddOutQueue(q)

		// add in queues to the processors (every inqueue is added to
		// every processor)
		for _, p := range processors {
			p.AddInQueue(q)
		}
	}

	// Add the stats and register processors as actors
	for _, p := range processors {
		p.SetReqDrain(stats)
		engine.RegisterActor(p)
	}

	// Register the generator as an actor
	engine.RegisterConnHeap(h)
	engine.RegisterActor(g)

	fmt.Printf("Cores:%v\tservice_rate:%v\tinterarrival_rate:%v\n", cores, mu, lambda)
	engine.ConnHeapRun(duration)
}
