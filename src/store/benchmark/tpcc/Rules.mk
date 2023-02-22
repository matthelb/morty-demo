d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), tpcc_client.cc new_order.cc tpcc_generator.cc tpcc_utils.cc payment.cc order_status.cc stock_level.cc delivery.cc tpcc_trace.cc trace_client.cc)

PROTOS += $(addprefix $(d), tpcc-proto.proto)

OBJ-tpcc-transaction := $(LIB-store-frontend)

OBJ-tpcc-client := $(o)tpcc_client.o

LIB-tpcc := $(OBJ-tpcc-client) $(OBJ-tpcc-transaction) $(o)new_order.o \
	$(o)tpcc-proto.o $(o)tpcc_utils.o $(o)payment.o $(o)order_status.o \
	$(o)stock_level.o $(o)delivery.o

LIB-tpcc-trace := $(o)tpcc_trace.o $(o)trace_client.o

$(d)tpcc_generator: $(LIB-io-utils) $(LIB-lib-message) $(o)tpcc-proto.o $(o)tpcc_generator.o $(o)tpcc_utils.o

BINS += $(d)tpcc_generator
