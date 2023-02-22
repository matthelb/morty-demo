d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), benchmark.cc bench_client.cc)

OBJS-all-store-clients := $(OBJS-strong-client) $(OBJS-weak-client) \
		$(LIB-tapir-client) $(LIB-janus-client) \
		$(LIB-morty-client-v2) \
		$(LIB-spanner-client)

LIB-bench-client := $(o)bench_client.o

OBJS-all-bench-clients := $(LIB-retwis) $(LIB-tpcc) $(LIB-sync-tpcc) $(LIB-async-tpcc) \
	$(LIB-smallbank) $(LIB-rw)  

$(d)benchmark: $(o)benchmark.o $(LIB-key-selector) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(OBJS-all-store-clients) $(OBJS-all-bench-clients) $(LIB-bench-client) $(LIB-store-common) $(LIB-io-utils)

store/benchmark/tpcc/tpcc_trace: $(LIB-tpcc-trace) \
	$(LIB-io-utils) $(LIB-tpcc) \
	$(LIB-store-common) $(LIB-bench-client) $(LIB-tcptransport) $(LIB-latency) \
	$(LIB-tpcc) $(LIB-message) $(LIB-bench-client) $(LIB-key-selector) \
	$(OBJS-all-store-clients)


BINS +=  $(d)benchmark store/benchmark/tpcc/tpcc_trace

SRCS += $(addprefix $(d), ping_server.cc ping_client.cc)

PROTOS += $(addprefix $(d), ping-proto.proto)

$(d)ping_client: $(o)ping_client.o $(LIB-tcptransport) $(o)ping-proto.o $(LIB-latency)

$(d)ping_server: $(o)ping_server.o $(LIB-tcptransport) $(o)ping-proto.o $(LIB-latency)

BINS += $(d)ping_server $(d)ping_client 

