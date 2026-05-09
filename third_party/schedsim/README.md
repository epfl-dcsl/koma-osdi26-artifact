# SchedSim

SchedSim is a discrete event simulator designed to model queuing and record latency distribution given a static number of queues, connections, and processors. It allows two scheduling policies: i) FIFO, and ii) Processor Sharing. 

## Running a single simulation

`go build`

`./schedsim [OPTION...]`

### Options
* --topo: single queue (0), multi queue (1), bounded queue (2), connection-based single queue (3), connection-based multi queue (4).
* --mu: service rate per core [reqs/us]
* --lambda: arrival rate [reqs/us]
* --genType: MM (0), MD (1), MB[90-10] (2),  MB[99.9-0.1] (3)
* --procType: FIFO processing - number of cores from common.go (0), Processor sharing (1)
* --cores: the number of cores for simulation
* --connNum: the number of connections/streams; only applicable when topo=3.

#### Examples
`./schedsim --topo=0 --mu=0.1 --lambda=0.005 --genType=2 --procType=0 --cores=8`

`./schedsim --topo=3 --mu=0.1 --lambda=0.005 --genType=2 --procType=0 --cores=8 --connNum=2`

## genType Notation
[Kendall’s notation](https://en.wikipedia.org/wiki/Kendall%27s_notation):

A/S/c
* A denotes the time between arrivals to the queue
    * M: Poisson
    * D: fixed inter-arrival time
* S the service time distribution
    * M: Exponential
    * D: Fixed
    * L: Lognormal
    * B: Bimodal
* c the number of service channels open at the node

## testing
schedsim includes end-to-end tests to verify the behavior of the entire system under different configurations. Currently schedsim only supports testing connection-based single queue.  

To run the end-to-end tests, execute the following in the schedsim folder:

`go test ./test/e2e/conn_single_queue -v`


## Running for multiple arrival rates and configs

Add schedsim to path:

`export PATH="$PATH:${PWD}"` (from where schedsim is)

Example: 

`./scripts/run_new.py "single_queue"`

### Running for multiple arrival rates (partial implementation)
`python3 ./scripts/run_many.py run --topo=0 --mu=0.1 --gen_type=1 --proc_type=0 --num_cores=10`

`python3 ./scripts/run_many.py csv`
