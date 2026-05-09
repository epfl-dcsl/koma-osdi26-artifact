#!/usr/bin/python3

'''
Core author: Marios Kogias

Updated for Python3
'''

import os
import sys
import numpy as np
from subprocess import run
from multiprocessing import Process
import argparse


# GENTYPES = ['m', 'd', 'b', 'b2']  # exponential, deterministic, bimodal{1-2}
GENTYPES = ['m', 'd', 'b']  # exponential, deterministic, bimodal{1-2}
# GENTYPES = [ 'b']  # exponential, deterministic, bimodal{1-2}
FIFO = 0


def execute_topology(topo, mu, lambdas, g, p, path, threshold=None):
    pathname = "{}/{}_{}_{}_{}_{}.dat".format(path, "FIFO", s_time, n_workers, connNum, GENTYPES[g])
    csv_name = "{}/{}_{}_{}_{}_{}.csv".format(path, "FIFO", s_time, n_workers, connNum, GENTYPES[g])
    cmd = ["schedsim", "--mu={}".format(mu), "--topo={}".format(topo), "--genType={}".format(g), "--cores={}".format(n_workers), "--connNum={}".format(connNum)]
    if p:
        cmd.append("--procType={}".format(p))
    if threshold:
        cmd.append("--threshold={}".format(threshold))

    with open(pathname, 'w') as f:
        for l in lambdas:
            exec_cmd = cmd + ["--lambda={}".format(l)]
            run(exec_cmd, stdout=f, stderr=f, shell=False)
    with open(csv_name, 'w') as csv_f, open(pathname, 'r') as dat_f:
        csv_f.write("Arrival_Rate (req/us), AVG (us), STDDev, 50th (us), 90th (us), 95th (us), 99th (us), Req/time_unit\n")
        lines = dat_f.readlines()
        for idx, l in enumerate(lines):
            if l.startswith("Cores:"):
                arr_rate = l.split("\n")[0].split("interarrival_rate:")[1]
                res = lines[idx + 3].split()[2:]
                csv_f.write("{}, {}, {}, {}, {}, {}, {}, {}\n".format(arr_rate, *res))
            else:
                continue


def parallel_exec(args):
    '''
        args = [(topo, mus, lambdas, genType, procType, path, <optinal threshold>)...]
    '''
    pids = []
    for run in args:
        p = Process(target=execute_topology, args=run)
        p.start()
        pids.append(p)
    for p in pids:
        p.join()


def setup_arguments(mu, lambda_list, queue_abstraction):
    directory = "data/{}".format(queue_abstraction)
    if not os.path.exists(directory):
        os.makedirs(directory)

    genTypes = list(range(len(GENTYPES)))
    parallel_num = len(genTypes)
    pTypes = [FIFO] * parallel_num
    lambdas = [lambda_list] * parallel_num

    if queue_abstraction == "single_queue":
        topologies = [0] * parallel_num
    elif queue_abstraction == "multi_queue":
        topologies = [1] * parallel_num
    elif queue_abstraction == "conn_single_queue":
        topologies = [3] * parallel_num
    elif queue_abstraction == "conn_multi_queue":
        topologies = [4] * parallel_num


    else:
        print("Unknown queue abstraction")
        exit(1)

    mus = [mu] * parallel_num
    names = [directory] * parallel_num
    parallel_exec(zip(topologies, mus, lambdas, genTypes, pTypes, names))


    
def setup_experiment(args):
    global n_workers, s_time, connNum
    s_time = args.service_time
    n_workers = args.num_workers
    connNum = args.connNum
    mu = 1 / s_time
    max_lambda = n_workers * mu
    lambda_list: list[float] = np.arange(max_lambda/50, max_lambda * 1.06, max_lambda/50)
    setup_arguments(mu, lambda_list, args.queue_abstraction)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="increase output verbosity")
    parser.add_argument("-s", "--service_time", type=int, help="the service time of workser in microseconds")
    parser.add_argument("-n", "--num_workers", type=int, help="the number of workers")
    parser.add_argument("-c", "--connNum", type=int, help="the number of connections")
    parser.add_argument("-q", "--queue_abstraction", type=str, help="the queue abstraction (single_queue or multiple_queue)")
    args = parser.parse_args()

    setup_experiment(args)
    
if __name__ == "__main__":
    main()

