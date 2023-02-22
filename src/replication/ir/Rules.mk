d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), record.cc client.cc replica.cc)

PROTOS += $(addprefix $(d), ir-proto.proto)

LIB-replication-ir-ir-proto := $(o)ir-proto.o
LIB-replication-ir-record := $(o)record.o $(LIB-lib-configuration) \
	$(LIB-lib-message) $(LIB-lib-transport) $(LIB-replication-common-request) \
	$(LIB-replication-ir-ir-proto)
LIB-replication-ir-replica := $(o)replica.o $(LIB-replication-ir-record) \
	$(LIB-replication-ir-ir-proto) \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-persistent-register) \
	$(LIB-lib-transport) $(LIB-lib-udptransport) \
	$(LIB-replication-common-replica) $(LIB-store-common-timestamp)

include $(d)tests/Rules.mk
