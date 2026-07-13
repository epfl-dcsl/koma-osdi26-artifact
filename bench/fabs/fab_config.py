#!/usr/bin/python
# Author: Rui Yang (DCSL at EPFL)

import os
from typing import TypedDict

"""
Configuration for running benchmarks, specifying:
i) the physical machines running as clients, lb, and servers, 
ii) the listening port
"""


def _parse_hosts(env_vars: str | list[str], default: list[str]) -> list[str]:
    if isinstance(env_vars, str):
        env_vars = [env_vars]
    for env_var in env_vars:
        env_value = os.environ.get(env_var)
        if env_value is None:
            continue
        hosts = [host.strip() for host in env_value.split(",") if host.strip()]
        return hosts or default
    return default


def _parse_bool(env_var: str, default: bool) -> bool:
    env_value = os.environ.get(env_var)
    if env_value is None:
        return default
    return env_value.strip().lower() in {"1", "true", "yes", "on"}


######### config for the servers' setup #########
DEFAULT_REMOTE_USER = os.environ.get("RAKAIA_OSDI_REMOTE_USER", "raina96")
DEFAULT_SSH_PRIVATE_KEY = os.environ.get("RAKAIA_OSDI_SSH_KEY", "~/.ssh/id_rsa")
ENABLE_SSH_AGENT_FORWARDING = _parse_bool("RAKAIA_OSDI_FORWARD_AGENT", True)

CLOUDLAB_COORDINATOR = "hp171.utah.cloudlab.us"
CLOUDLAB_CLIENTS = [
    "hp171.utah.cloudlab.us",
    "hp081.utah.cloudlab.us",
    "hp197.utah.cloudlab.us",
    "hp163.utah.cloudlab.us",
    "hp175.utah.cloudlab.us",
    "hp172.utah.cloudlab.us",
    "hp083.utah.cloudlab.us",
    "hp192.utah.cloudlab.us",
    "hp115.utah.cloudlab.us",
    "hp112.utah.cloudlab.us",
]

CLOUDLAB_GRPC_CLIENTS = [
    "hp171.utah.cloudlab.us",
    "hp081.utah.cloudlab.us",
    "hp197.utah.cloudlab.us",
    "hp163.utah.cloudlab.us",
    "hp175.utah.cloudlab.us",
    "hp172.utah.cloudlab.us",
    "hp083.utah.cloudlab.us",
    "hp192.utah.cloudlab.us",
    "hp115.utah.cloudlab.us",
    "hp112.utah.cloudlab.us",
    "hp167.utah.cloudlab.us",
    "hp164.utah.cloudlab.us",
]

CLOUDLAB_SERVER = "hp161.utah.cloudlab.us"


COORDINATOR = _parse_hosts("RAKAIA_OSDI_COORDINATOR", [CLOUDLAB_COORDINATOR])
SYM_CLIENTS = _parse_hosts("RAKAIA_OSDI_SYM_CLIENTS", CLOUDLAB_CLIENTS)
ASYM_CLIENTS = _parse_hosts("RAKAIA_OSDI_ASYM_CLIENTS", CLOUDLAB_GRPC_CLIENTS)
SERVER = _parse_hosts("RAKAIA_OSDI_SERVER", [CLOUDLAB_SERVER])
SERVER_PORT = 40001

TLS_SERVER = _parse_hosts("RAKAIA_OSDI_TLS_SERVER", [CLOUDLAB_SERVER])
TLS_SERVER_PORT = 40002

######### Configuration Classes #########
class LancetConfig(TypedDict, total=False):
    transport_proto: str
    idist: str
    app_proto: str
    nicTS: str
    symAgents: str
    workload: str
    preload: bool

class MachinesConfig(TypedDict):
    coordinator: list[str]
    client: list[str]
    server: list[str]
    port: int
    lancet_config: LancetConfig

########## config for server setup #########
setupConfig: MachinesConfig = {
    'coordinator': COORDINATOR,
    'client': SYM_CLIENTS,
    'server': SERVER,
    'port': SERVER_PORT,
    'lancet_config': None
}

########## config for the gRPC vs Rakaia+gRPC benchmark SW experiments (without TLS) #########
grpcConfig: LancetConfig = {
    'transport_proto': 'GRPC_GO',
    'idist': 'exp',
    'app_proto': 'echo:2048',
    'nicTS': "False",
    'symAgents': "False"
}

grpcBench: MachinesConfig = {
    'coordinator' : COORDINATOR,
    'client': ASYM_CLIENTS,
    'server': SERVER,
    'port': SERVER_PORT,
    'lancet_config': grpcConfig
}


########## config for the SILO gRPC vs Rakaia+gRPC benchmark SW experiments (without TLS) #########
siloGrpcConfig: LancetConfig = {
    'transport_proto': 'GRPC_GO',
    'idist': 'exp',
    'app_proto': 'echo:2048',
    'nicTS': "False",
    'symAgents': "False"
}

siloGrpcBench: MachinesConfig = {
    'coordinator' : COORDINATOR,
    'client': ASYM_CLIENTS,
    'server': SERVER,
    'port': SERVER_PORT,
    'lancet_config': siloGrpcConfig
}

########## config for Vanilla Rakaia, TCP partition & floating, and KCM #########
vanillaConfig: LancetConfig = {
   'transport_proto': 'TCP',
   'idist': 'exp',
   'app_proto': 'memcache-id', 
   'nicTS': "False",
   'symAgents': "True"
}

vanillaBench: MachinesConfig = {
    'coordinator' : COORDINATOR,
    'client': SYM_CLIENTS,
    'server': SERVER,
    'port': SERVER_PORT,
    'lancet_config': vanillaConfig
}

########## config for Rakaia, TCP floating with TLS #########
vanillaTLSConfig: LancetConfig = {
   'transport_proto': 'TLS',
   'idist': 'exp',
   'app_proto': 'memcache-id',
   'nicTS': "False",
   'symAgents': "True"
}

vanillaTLSBench: MachinesConfig = {
    'coordinator' : COORDINATOR,
    'client': SYM_CLIENTS,
    'server': TLS_SERVER,
    'port': TLS_SERVER_PORT,
    'lancet_config': vanillaTLSConfig
}
