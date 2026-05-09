package topologies

import (
	"fmt"

	"gitlab.epfl.ch/ryang/schedsim/blocks"
	"gitlab.epfl.ch/ryang/schedsim/engine"
)

// ConnMultiQueue described a topology where there are multiple processors,
// multiple connection-based request streams. Each processor has access to its
// own pre-allocated request streams, and process requests from these streams/
// connections in FIFO manner.
func ConnMultiQueue(lambda, mu, duration float64, genType, procType, connNum int, cores int) {
	engine.InitSim()

	// Init the statistics
	// stats := blocks.NewBookKeeper()
	stats := &blocks.AllKeeper{}
	stats.SetName("Main Stats")
	engine.InitStats(stats)

	// Add generator
	var g blocks.Generator
	switch genType {
	case 0:
		g = blocks.NewMMConnGenerator(lambda, mu, connNum)
	case 1:
		g = blocks.NewMDConnGenerator(lambda, 1/mu, connNum)
	case 2:
		g = blocks.NewMBConnGenerator(lambda, 0.5*(1/mu), 5.5*(1/mu), 0.9, connNum)
	case 3:
		g = blocks.NewMBConnGenerator(lambda, 0.5*(1/mu), 500*(1/mu), 0.999, connNum)
	}

	g.SetCreator(&blocks.SimpleReqCreator{})

	// Create queues, which has the same number of cores
	fastQueues := make([]engine.QueueInterface, cores)
	for i := range fastQueues {
		fastQueues[i] = blocks.NewQueue()
	}

	// Create processors
	processors := make([]blocks.Processor, cores)

	// first the slow cores
	for i := 0; i < cores; i++ {
		switch procType {
		case 0:
			processors[i] = &blocks.RTCProcessor{Index: i}
		case 1:
			// TODO: not implemented yet!
			processors[i] = blocks.NewPSProcessor()
		}
	}

	// Connect the fast queues
	for i, q := range fastQueues {
		// add out queues to the request generator
		g.AddOutQueue(q)

		// add in queues to the processors
		processors[i].AddInQueue(q)
	}

	// Add the stats and register processors as actors
	for _, p := range processors {
		p.SetReqDrain(stats)
		engine.RegisterActor(p)
	}

	// Register the generator as an actor
	engine.RegisterActor(g)

	fmt.Printf("Cores:%v\tservice_rate:%v\tinterarrival_rate:%v\n", cores, mu, lambda)
	engine.Run(duration)
}
