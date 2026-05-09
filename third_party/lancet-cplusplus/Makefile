# MIT License
#
# Copyright (c) 2019-2021 Ecole Polytechnique Federale Lausanne (EPFL)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
CXXFLAGS += -std=c++14
GO = /usr/local/go/bin/go

.PHONY: coordinator agents native_etcd_preload style manager manager_r2p2 all clean

.DEFAULT_GOAL := all
all: agents
	${MAKE} manager
	${MAKE} coordinator

current_dir= $(shell pwd)

manager:
	${MAKE} -C ${current_dir}/agent-manager

coordinator:
	${MAKE} -C coordinator/

agents: clean
	cd agents/go_bridge; ${GO} build -o ../../inc/lancet/libecho.so -buildmode=c-shared .
	mkdir agents/build
	cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_BUILD_TYPE=ReleaseAssert -S . -B agents/build
	cmake	--build agents/build

native_etcd_preload:
	mkdir -p bin
	cd agents/go_bridge; ${GO} build -tags preloadcli -o ../../bin/native-etcd-preload .

agents_r2p2: clean
	mkdir agents/build
	cmake -DBUILD_R2P2=ON -DR2P2_ROOT=${R2P2} -S . -B agents/build
	cmake	--build agents/build

agents_r2p2_nic_ts: clean
	mkdir agents/build
	cmake -DBUILD_R2P2=ON -DR2P2_ROOT=${R2P2} -DR2P2_NIC_TS=ON -S . -B agents/build
	cmake	--build agents/build

deploy: clean_remote
	scripts/deploy_lancet.sh ${HOSTS} ${DIR}

prepare_clients:
	scripts/prepare_clients.sh ${HOSTS}

clean_remote:
	scripts/clean_up.sh ${HOSTS} ${DIR}

clean:
	rm -rf agents/build

style:
	clang-format -i -style=file agents/*.c
	clang-format -i -style=file inc/lancet/*.h
