#!/usr/bin/python
# Author: Rui Yang (DCSL at EPFL)

from fab_experiments import *
from fab_config import *
import re
from fabric import task
from itertools import product 


def project_dirs(conn):
    return ProjDir.from_connection(conn)


def single_host_config(conn, port=SERVER_PORT):
    host = getattr(conn, "host", None) or SERVER[0] or "localhost"
    return {
        'coordinator': [host],
        'client': [host],
        'server': [host],
        'port': port,
        'lancet_config': None,
    }


@task
def bench_setup(conn):
    proj_dir = project_dirs(conn)
    exp_config = ExperimentConfig(proj_dir=proj_dir, conn=conn, exp_config=setupConfig) 
    exp_run = ExperimentRun(exp_config=exp_config) 
    print("start setup experiments")
    exp_run.setup_experiment()
    exp_run.deploy_experiment()

@task
def bench_update(conn):
    proj_dir = project_dirs(conn)
    exp_config = ExperimentConfig(proj_dir=proj_dir, conn=conn, exp_config=setupConfig) 
    exp_run = ExperimentRun(exp_config=exp_config) 
    exp_run.update_experiment()
    exp_run.deploy_experiment()

def run_experiment(conn, dist_config, server_config, exp_name, exp_config, duration):
    proj_dir = project_dirs(conn)

    res_head = "dist_config, lancet_load, n_repetition"
    for i in server_config:
        res_head += ", load_{}(QPS)".format(i)
    for i in server_config:
        res_head += ", latency_{}".format(i)
    res_head += "\n"

    exp_config = ExperimentConfig(proj_dir=proj_dir, conn=conn, 
                                  exp_config=exp_config, server_config=server_config, 
                                  dist_config=dist_config, exp_type=exp_name,
                                  res_head=res_head)

    exp_run = ExperimentRun(exp_config=exp_config)
    if any("silo" in config for config in server_config):
        exp_run.ensure_silo_hugepages()
    exp_load = range(exp_config.run_config['start_load'], exp_config.run_config['end_load'], exp_config.run_config['step'])
    exp_repetitions = range(exp_config.run_config['n_repetitions'])
    exp_permutations = product(dist_config, exp_load, exp_repetitions)

    load = {i: [] for i in server_config}
    lat = {i: [] for i in server_config}

    build_targets = []
    if any(config in {"grpc-go", "grpc-rakaia-go"} for config in server_config):
        build_targets.append("spin-grpc-go")
    if any(config in {"silo-grpc-go", "silo-grpc-rakaia-go"} for config in server_config):
        build_targets.append("silo-grpc-go")
    with exp_run.server_conn.cd(exp_run.dirs.server_dir):
        exp_run.server_conn.run("make clean")
        if not build_targets:
            exp_run.server_conn.run("make")

    for d_config, cur_load, cur_rep in exp_permutations:
        exp_config.run_config['load'] = cur_load
        if cur_rep == 0:
            load = {i: [] for i in server_config}
            lat = {i: [] for i in server_config}

        # change the load pattern
        exp_config.run_config['loadPattern'] = re.sub("^(.*):([^:]*):([^:]*)$", rf"\1:{exp_config.run_config['load']}:{duration*exp_config.run_config['load']}", exp_config.run_config['loadPattern']) 
        # exp_config.run_config['loadPattern'] = re.sub("^(.*):([^:]*):([^:]*)$", rf"\1:{exp_config.run_config['load']}:{5000}", exp_config.run_config['loadPattern']) 
        exp_config.logger.print("[Epoch{}]; service time at the server is: {}\n".format(
            cur_rep, exp_config.run_config.get('service_time', 'external')))

        # apply linux config to the server and make 
        for s_config in exp_config.server_config:
            res_load, res_tail_lat = exp_run.run_experiment(s_config, d_config)
            # parse, check and save Lancet Result
            load[s_config].append(res_load)
            lat[s_config].append(res_tail_lat)


        with open(exp_config.raw_res, 'a') as out_file:
            out_file.write("{}, {}, {}".format(d_config, exp_config.run_config['load'], cur_rep))
            for type in load: 
                out_file.write(", {}".format(load[type][-1]))
            for type in lat: 
                out_file.write(", {}".format(lat[type][-1]))
            out_file.write("\n")

        if cur_rep == exp_config.run_config['n_repetitions'] - 1:
            with open(exp_config.raw_res, 'a') as out_file1, open(exp_config.sanitized_res, 'a') as out_file2:
                line_to_write = "{}, {}, {}".format(d_config, exp_config.run_config['load'], 'avg')
                for key in load:
                    # line_to_write += ", {}".format(lowest_val(load[key]) if load[key] else 0)
                    line_to_write += ", {}".format(mean_val(load[key]) if load[key] else 0)
                for key in lat:
                    # line_to_write += ", {}".format(lowest_val(lat[key]) if lat[key] else 0)
                    line_to_write += ", {}".format(mean_val(lat[key]) if lat[key] else 0)
                line_to_write += "\n"
                out_file1.write(line_to_write)
                out_file2.write(line_to_write)


@task
def cloudlab_patch_rakaia_kernel(conn):
    proj_dir = project_dirs(conn)
    exp_config = ExperimentConfig(
        proj_dir=proj_dir,
        conn=conn,
        exp_config=single_host_config(conn),
    )
    exp_run = ExperimentRun(exp_config=exp_config)
    exp_run.patch_cloudlab_rakaia_kernel()


@task
def cloudlab_verify_ktls_module(conn):
    proj_dir = project_dirs(conn)
    exp_config = ExperimentConfig(
        proj_dir=proj_dir,
        conn=conn,
        exp_config=single_host_config(conn),
    )
    exp_run = ExperimentRun(exp_config=exp_config)
    verification = exp_run.verify_cloudlab_ktls_module()
    print(
        "kTLS verified on {}: kernel {}, srcversion {}, module {}".format(
            exp_run.server_conn.host,
            verification["kernel_release"],
            verification["loaded_srcversion"],
            verification["installed_module"],
        )
    )


@task
def gRPC20_Conn24(conn):
    server_config = ["grpc-go", "grpc-rakaia-go"]
    dist_config = ["exponential"]

    run_experiment(conn, dist_config, server_config, ExperimentType.GRPC20_Conn24.name, grpcBench, 2)

@task
def gRPC20_Conn5000(conn):
    server_config = ["grpc-go", "grpc-rakaia-go"]
    dist_config = ["exponential"]
    run_experiment(conn, dist_config, server_config, ExperimentType.GRPC20_Conn5000.name, grpcBench, 2)

@task
def Silo_GRPC_Conn24(conn):
    server_config = ["silo-grpc-go", "silo-grpc-rakaia-go"]
    dist_config = ["silo"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Silo_GRPC_Conn24.name, siloGrpcBench, 2)

@task
def Silo_GRPC_Conn5000(conn):
    server_config = ["silo-grpc-go", "silo-grpc-rakaia-go"]
    dist_config = ["silo"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Silo_GRPC_Conn5000.name, siloGrpcBench, 2)


@task
def vanilla100_Conn20(conn):
    server_config = ["rakaia", "partition", "floating", "kcm_floating", "pool"]
    dist_config = ["fixed", "exponential", "bimodal"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Vanilla100_Conn20.name, vanillaBench, 1)

@task
def vanilla100_Conn80(conn):
    server_config = ["rakaia", "partition", "floating", "kcm_floating", "pool"]
    dist_config = ["fixed", "exponential", "bimodal"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Vanilla100_Conn80.name, vanillaBench, 1)


@task
def vanilla20_Conn80(conn):
    server_config = ["rakaia", "partition", "floating", "kcm_floating", "pool"]
    dist_config = ["fixed", "exponential", "bimodal"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Vanilla20_Conn80.name, vanillaBench, 1)


@task
def vanilla100_Conn5000(conn):
    server_config = ["rakaia", "partition", "floating", "kcm_floating", "pool"]
    dist_config = ["fixed", "exponential", "bimodal"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Vanilla100_Conn5000.name, vanillaBench, 1)

@task
def TLS100_Conn80(conn):
    server_config = ["floating-tls", "rakaia-tls", "pool-tls"]
    dist_config = ["fixed", "exponential", "bimodal"]
    run_experiment(conn, dist_config, server_config, ExperimentType.TLS100_Conn80.name, vanillaTLSBench, 1)

@task
def TLS20_Conn80(conn):
    server_config = ["floating-tls", "rakaia-tls", "pool-tls"]
    dist_config = ["fixed", "exponential", "bimodal"]
    run_experiment(conn, dist_config, server_config, ExperimentType.TLS20_Conn80.name, vanillaTLSBench, 1)

@task
def silo_vanilla_80(conn):
    server_config = ["silo-rakaia", "silo-partition", "silo-floating", "silo-kcm_floating", "silo-pool"]
    dist_config = ["silo"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Silo_Conn80.name, vanillaBench, 1)

@task
def silo_vanilla_tls_80(conn):
    server_config = ["silo-rakaia-tls",  "silo-floating-tls", "silo-pool-tls"]
    dist_config = ["silo"]
    run_experiment(conn, dist_config, server_config, ExperimentType.Silo_Conn80_TLS.name, vanillaTLSBench, 1)
