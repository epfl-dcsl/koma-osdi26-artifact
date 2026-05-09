package main

import (
	"flag"
	"fmt"

	"gitlab.epfl.ch/ryang/schedsim/topologies"
)

func main() {
	topo := flag.Int("topo", 0, "topology selector")
	mu := flag.Float64("mu", 0.02, "mu service rate") // default 50usec
	lambda := flag.Float64("lambda", 0.005, "lambda poisson interarrival")
	genType := flag.Int("genType", 0, "type of generator")
	procType := flag.Int("procType", 0, "type of processor")
	duration := flag.Float64("duration", 10000000, "experiment duration")
	bufferSize := flag.Int("buffersize", 1, "size of the bounded buffer")
	connNum := flag.Int("connNum", 1, "Number of connections")
	cores := flag.Int("cores", 1, "Number of cores")

	flag.Parse()
	fmt.Printf("Selected topology: %v\n", *topo)

	switch *topo {
	case 0:
		topologies.SingleQueue(*lambda, *mu, *duration, *genType, *procType, *cores)
	case 1:
		topologies.MultiQueue(*lambda, *mu, *duration, *genType, *procType, *cores)
	case 2:
		topologies.BoundedQueue(*lambda, *mu, *duration, *bufferSize, *cores)
	case 3:
		topologies.ConnSingleQueue(*lambda, *mu, *duration, *genType, *procType, *connNum, *cores)
	case 4:
		topologies.ConnMultiQueue(*lambda, *mu, *duration, *genType, *procType, *connNum, *cores)

	default:
		panic("Unknown topology")
	}
}
