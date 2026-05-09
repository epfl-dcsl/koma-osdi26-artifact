package topologies

import (
	"fmt"

	"gitlab.epfl.ch/ryang/schedsim/blocks"
	"gitlab.epfl.ch/ryang/schedsim/engine"
)

// SingleQueue implement a single-generator-multiprocessor topology with a single
// queue. Each processor just dequeues from this queue
func SingleQueue(lambda, mu, duration float64, genType, procType int, cores int) {
	engine.InitSim()

	// Init the statistics
	stats := &blocks.AllKeeper{}
	stats.SetName("Main Stats")
	engine.InitStats(stats)

	// Add generator
	var g blocks.Generator
	if genType == 0 {
		g = blocks.NewMMRandGenerator(lambda, mu)
	} else if genType == 1 {
		g = blocks.NewMDRandGenerator(lambda, 1/mu)
	} else if genType == 2 {
		g = blocks.NewMBRandGenerator(lambda, 0.5*(1/mu), 5.5*(1/mu), 0.9)
	} else if genType == 3 {
		g = blocks.NewMBRandGenerator(lambda, 0.5*(1/mu), 500*(1/mu), 0.999)
	}

	g.SetCreator(&blocks.SimpleReqCreator{})

	// Create queues
	q := blocks.NewQueue()

	// Create processors

	if procType == 0 {
		for i := 0; i < cores; i++ {
			p := &blocks.RTCProcessor{}
			p.AddInQueue(q)
			p.SetReqDrain(stats)
			engine.RegisterActor(p)
		}
	} else if procType == 1 {
		p := blocks.NewPSProcessor()
		p.SetWorkerCount(cores)
		p.AddInQueue(q)
		p.SetReqDrain(stats)
		engine.RegisterActor(p)
	}

	g.AddOutQueue(q)

	// Register the generator
	engine.RegisterActor(g)

	fmt.Printf("Cores:%v\tservice_rate:%v\tinterarrival_rate:%v\n", cores, mu, lambda)
	engine.Run(duration)
}
