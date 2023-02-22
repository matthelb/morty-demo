d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), pingserver.cc kvstore.cc lockserver.cc txnstore.cc \
				messageserver.cc loader.cc)

LIB-store-common-backend-lockserver := $(o)lockserver.o $(LIB-lib-message)
LIB-store-common-backend-kvstore := $(o)kvstore.o $(LIB-lib-message)
LIB-store-common-backend-loader := $(o)loader.o $(LIB-lib-io-utils) \
	$(LIB-lib-latency) $(LIB-lib-message) $(LIB-store-common-common) \
	$(LIB-store-common-partitioner) $(LIB-store-common-timestamp) \
	$(LIB-store-server)
LIB-store-common-backend-messageserver := $(o)messageserver.o \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-transport) \
	$(LIB-store-common-stats)
LIB-store-common-backend-txnstore := $(o)txnstore.o $(LIB-lib-message) \
	$(LIB-store-common-stats) $(LIB-store-common-timestamp) \
	$(LIB-store-common-transaction)
LIB-store-common-backend-pingserver := $(o)pingserver.o $(LIB-lib-transport) \
	$(LIB-store-common-common-proto)

include $(d)tests/Rules.mk
