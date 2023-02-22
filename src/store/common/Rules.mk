d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), promise.cc timestamp.cc tracer.cc transaction.cc \
				truetime.cc stats.cc partitioner.cc pinginitiator.cc common.cc \
				partialorder.cc)

PROTOS += $(addprefix $(d), common-proto.proto)

LIB-store-common-common := $(o)common.o
LIB-store-common-partitioner := $(o)partitioner.o
LIB-store-common-truetime := $(o)truetime.o
LIB-store-common-common-proto := $(o)common-proto.o
LIB-store-common-stats := $(o)stats.o $(LIB-lib-message)
LIB-store-common-timestamp := $(o)timestamp.o	$(LIB-lib-message)
LIB-store-common-transaction := $(o)transaction.o $(LIB-lib-message) \
	$(LIB-store-common-common-proto) $(LIB-store-common-timestamp)

include $(d)backend/Rules.mk $(d)frontend/Rules.mk
