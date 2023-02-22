d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), client.cc replica.cc log.cc)

PROTOS += $(addprefix $(d), request.proto)

LIB-replication-common-log := $(o)log.o
LIB-replication-common-request := $(o)request.o
LIB-replication-common-replica := $(o)replica.o $(LIB-replication-common-log) \
	$(LIB-replication-common-request) $(LIB-lib-configuration) \
	$(LIB-lib-transport) $(LIB-lib-message) $(LIB-store-common-timestamp)
LIB-replication-common-client := $(o)client.o $(LIB-lib-configuration) \
	$(LIB-lib-message) $(LIB-lib-transport) $(LIB-replication-common-request)
include $(d)tests/Rules.mk
