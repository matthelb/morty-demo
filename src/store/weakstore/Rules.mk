d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc store.cc server.cc)

PROTOS += $(addprefix $(d), weak-proto.proto)

LIB-store-weakstore-server := $(o)server.o $(o)store.o $(o)weak-proto.o \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-transport) \
	$(LIB-lib-udptransport) $(LIB-store-common-stats) \
	$(LIB-store-common-timestamp)
