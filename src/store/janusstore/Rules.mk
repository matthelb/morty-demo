d := $(dir $(lastword $(MAKEFILE_LIST)))

# TODO replace with janus-specific stuff
SRCS += $(addprefix $(d), client.cc shardclient.cc transaction.cc server.cc \
				store.cc)

PROTOS += $(addprefix $(d), janus-proto.proto)

LIB-store-janusstore-server := $(o)server.o $(o)janus-proto.o $(o)store.o \
	$(o)transaction.o $(LIB-lib-configuration) \
	$(LIB-lib-message) $(LIB-lib-tcptransport) $(LIB-lib-transport) \
	$(LIB-replication-common-replica) $(LIB-replication-ir-ir-proto) \
	$(LIB-store-common-stats) $(LIB-store-common-truetime) \
	$(LIB-store-common-timestamp) $(LIB-replication-common-request)

include $(d)tests/Rules.mk
