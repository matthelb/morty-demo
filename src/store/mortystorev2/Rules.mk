d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc server.cc shardclient.cc common.cc \
				version.cc)

PROTOS += $(addprefix $(d), morty-proto.proto)

LIB-store-mortystorev2-server := $(o)server.o $(o)version.o $(o)morty-proto.o \
	$(LIB-lib-configuration) \
	$(LIB-lib-latency) $(LIB-lib-message) $(LIB-lib-partitionedthreadpool) \
	$(LIB-lib-transport) $(LIB-replication-common-replica) \
	$(LIB-store-common-backened-messageserver) \
	$(LIB-store-common-backened-txnstore) \
	$(LIB-store-common-common) \
	$(LIB-store-common-stats) \
	$(LIB-store-common-timestamp) \
	$(LIB-store-common-truetime)
