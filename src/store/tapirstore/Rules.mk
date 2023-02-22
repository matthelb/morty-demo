d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc shardclient.cc server.cc store.cc \
	irtransport.cc)

PROTOS += $(addprefix $(d), tapir-proto.proto)

LIB-store-tapirstore-server := $(o)server.o $(o)store.o $(o)tapir-proto.o \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-replication-ir-record) \
	$(LIB-replication-ir-replica) $(LIB-store-common-backend-txnstore) \
	$(LIB-store-common-common-proto) $(LIB-store-common-timestamp) \
	$(LIB-store-common-transaction) $(LIB-store-common-truetime)
	
