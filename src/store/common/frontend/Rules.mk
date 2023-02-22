d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), bufferclient.cc async_adapter_client.cc \
				transaction_utils.cc sync_client.cc async_one_shot_adapter_client.cc \
				one_shot_transaction.cc client.cc async_client.cc txncontext.cc \
				appcontext.cc)

LIB-store-common-frontend-client := $(o)client.o $(o)appcontext.o \
	$(LIB-lib-latency) $(LIB-lib-message) $(LIB-store-common-partitioner) \
	$(LIB-store-common-stats) $(LIB-store-common-timestamp)
