d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc locktable.cc preparedtransaction.cc \
	replicaclient.cc server.cc shardclient.cc transactionstore.cc truetime.cc \
	woundwait.cc)

PROTOS += $(addprefix $(d), spanner-proto.proto)

LIB-store-spannerstore-locktable := $(o)locktable.o $(o)woundwait.o \
	$(LIB-lib-message) \
	$(LIB-store-common-timestamp) $(LIB-store-common-transaction)
LIB-store-spannerstore-preparedtransaction := $(o)preparedtransaction.o \
	$(o)spanner-proto.o $(LIB-store-common-timestamp) \
	$(LIB-store-common-common)
LIB-store-spannerstore-shardclient := $(o)shardclient.o $(o)spanner-proto.o \
	$(LIB-store-spannerstore-preparedtransaction) \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-transport) \
	$(LIB-store-common-promise) $(LIB-store-common-timestamp) \
	$(LIB-store-common-transaction) $(LIB-store-common-common-proto)
LIB-store-spannerstore-server := $(o)server.o $(o)replicaclient.o \
	$(LIB-store-spannerstore-shardclient) $(o)spanner-proto.o \
	$(o)transactionstore.o \
	$(o)truetime.o $(LIB-store-spannerstore-locktable) $(LIB-lib-configuration) \
	$(LIB-lib-latency) \
	$(LIB-lib-message) $(LIB-lib-transport) $(LIB-replication-common-replica)  \
	$(LIB-replication-vr-client) $(LIB-replication-vr-replica) \
	$(LIB-store-common-backend-pingserver) \
	$(LIB-store-common-frontend-client) \
	$(LIB-store-common-common-proto) $(LIB-store-common-stats) \
	$(LIB-store-common-timestamp) $(LIB-store-common-transaction)

LIB-store-spannerstore-truetime := $(o)truetime.o $(LIB-lib-message)

include $(d)tests/Rules.mk
