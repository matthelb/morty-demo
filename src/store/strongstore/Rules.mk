d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), mvtsostore.cc occstore.cc lockstore.cc server.cc \
		client.cc shardclient.cc unreplicatedserver.cc unreplicatedshardclient.cc)

PROTOS += $(addprefix $(d), strong-proto.proto)

LIB-store-strongstore-lockstore := $(o)lockstore.o $(LIB-lib-message) \
	$(LIB-store-common-backend-kvstore) $(LIB-store-common-backend-lockserver) \
	$(LIB-store-common-backend-txnstore) $(LIB-store-common-timestamp) \
	$(LIB-store-common-transaction)
LIB-store-strongstore-occstore := $(o)occstore.o
LIB-store-strongstore-server := $(o)server.o $(o)strong-proto.o \
	$(LIB-store-strongstore-lockstore) $(LIB-store-strongstore-occstore) \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-udptransport) \
	$(LIB-replication-common-replica) $(LIB-replication-vr-replica) \
	$(LIB-store-common-backend-txnstore) $(LIB-store-common-common-proto) \
	$(LIB-store-common-stats) $(LIB-store-common-timestamp) \
	$(LIB-store-common-transaction) $(LIB-store-common-truetime)
LIB-store-strongstore-unreplicatedserver := $(o)unreplicatedserver.o \
	$(o)mvtsostore.o $(o)occstore.o $(o)strong-proto.o \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-transport) \
	$(LIB-store-common-backend-messageserver) \
	$(LIB-store-common-backend-txnstore) $(LIB-store-common-timestamp) \
	$(LIB-store-common-transaction)
