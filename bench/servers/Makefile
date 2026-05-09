CPPFLAGS = -Wall -O3 -g -MD
CXXFLAGS = -std=c++11

GO = /usr/local/go/bin/go

SILO_PATH_DBG = ./silo/out-perf.debug.check.masstree
SILO_PATH = ./silo/out-perf.masstree

SILO_OBJ_NAMES = allocator.o \
	btree.o \
	core.o \
	counter.o \
	memory.o \
	rcu.o \
	stats_server.o \
	thread.o \
	ticker.o \
	tuple.o \
	txn_btree.o \
	txn.o \
	txn_proto2_impl.o \
	varint.o \
	compiler.o \
	str.o \
	string.o \
	straccum.o \
	json.o

SILO_BENCH_OBJ_NAMES = bench.o \
	bid.o \
	dcsl.o \
	tpcc.o \
	ycsb.o

SILO_OBJS := $(addprefix $(SILO_PATH)/,$(SILO_OBJ_NAMES))
OBJFILES := $(addprefix $(SILO_PATH)/benchmarks/,$(SILO_BENCH_OBJ_NAMES))
SILO_LZ4_OBJ := ./silo/third-party/lz4/lz4.o
SILO_LINK_OBJS := $(SILO_OBJS) $(OBJFILES) $(SILO_LZ4_OBJ)
SILO_LDLIBS := -pthread -lnuma

.PHONY: all silo-objs clean

all: spin-linux spin-koma spin-linux-tls spin-koma-tls spin-linux-pool spin-linux-pool-tls spin-kcm silo-linux silo-koma silo-kcm silo-linux-tls silo-linux-pool silo-linux-pool-tls silo-koma-tls spin-grpc-go silo-grpc-go
# all: spin-linux spin-kcm spin-koma silo-linux silo-koma silo-kcm spin-grpc-go silo-grpc-go spin-linux-tls spin-koma-tls 

spin-grpc-go: spin-grpc-go.go go.mod go.sum
	$(GO) build -tags spin -o ./spin-grpc-go ./spin-grpc-go.go

silo-grpc-go: silo-grpc-go.go go.mod go.sum
	$(GO) build -tags silo -o ./silo-grpc-go ./silo-grpc-go.go

spin-linux: spin-linux.o common-linux.o
	$(CXX) -o $@ $^ -pthread -lm

spin-linux-pool: spin-linux-pool.o common-linux-pool.o
	$(CXX) -o $@ $^ -pthread -lm

spin-linux-pool-tls: spin-linux-pool-tls.o common-linux-pool-tls.o
	$(CXX) -o $@ $^ -pthread -lm -lssl -lcrypto

spin-linux-tls: spin-linux-tls.o common-linux-tls.o
	$(CXX) -o $@ $^ -pthread -lm -lssl -lcrypto

spin-koma-tls: spin-koma-tls.o common-koma-tls.o koma-helpers.o
	$(CXX) -o $@ $^ -pthread -lm -lssl -lcrypto

spin-kcm: spin-kcm.o common-kcm.o kcm-helpers.o
	$(CXX) -o $@ $^ -pthread -lm -lbcc

spin-koma: spin-koma.o common-koma.o koma-helpers.o
	$(CXX) -o $@ $^ -pthread -lm

silo-objs:
	$(MAKE) -C silo $(patsubst ./silo/%,%,$(SILO_OBJS) $(OBJFILES))
	$(MAKE) -C silo/third-party/lz4 lz4.o

silo-linux: silo-linux.o common-linux.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS)

silo-linux-tls: silo-linux-tls.o common-linux-tls.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS) -lssl -lcrypto

silo-linux-pool: silo-linux-pool.o common-linux-pool.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS)

silo-linux-pool-tls: silo-linux-pool-tls.o common-linux-pool-tls.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS) -lssl -lcrypto

silo-koma: silo-koma.o common-koma.o koma-helpers.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS)

silo-koma-tls: silo-koma-tls.o common-koma-tls.o koma-helpers.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS) -lssl -lcrypto

silo-kcm: silo-kcm.o common-kcm.o kcm-helpers.o silo-workload.o | silo-objs
	$(CXX) -o $@ $^ $(SILO_LINK_OBJS) $(SILO_LDLIBS) -lbcc

clean:
	rm -f *.o *.d spin-linux spin-linux-pool spin-linux-pool-tls spin-kcm spin-koma silo-linux silo-koma silo-kcm spin-grpc-go spin-linux-tls spin-koma-tls silo-linux-tls silo-linux-pool silo-linux-pool-tls silo-koma-tls spin-grpc-go silo-grpc-go
	rm -rf cmake/build

-include *.d
