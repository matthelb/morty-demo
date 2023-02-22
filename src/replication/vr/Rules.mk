d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), replica.cc client.cc)

PROTOS += $(addprefix $(d), vr-proto.proto)

LIB-replication-vr-replica := $(o)replica.o $(o)vr-proto.o \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-transport) \
	$(LIB-replication-common-log) \
	$(LIB-replication-common-replica) $(LIB-replication-common-request)

LIB-replication-vr-client := $(o)client.o $(o)vr-proto.o \
	$(LIB-lib-configuration) $(LIB-lib-message) $(LIB-lib-transport) \
	$(LIB-replication-common-client) $(LIB-replication-common-request) \


include $(d)tests/Rules.mk
