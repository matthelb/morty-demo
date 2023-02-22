d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), smallbank_transaction.cc utils.cc smallbank_client.cc smallbank_generator.cc smallbank_generator_main.cc bal.cc write_check.cc amalgamate.cc transact.cc deposit.cc)

PROTOS += $(addprefix $(d), smallbank-proto.proto)

LIB-smallbank := $(o)smallbank_transaction.o $(o)utils.o $(o)smallbank-proto.o $(o)smallbank_client.o $(o)bal.o $(o)write_check.o $(o)amalgamate.o $(o)transact.o $(o)deposit.o

$(d)smallbank_generator_main: $(LIB-store-common) $(LIB-io-utils) $(o)smallbank-proto.o $(o)smallbank_generator_main.o $(o)smallbank_generator.o $(o)utils.o

BINS += $(addprefix $(d), smallbank_generator_main)

include $(d)tests/Rules.mk
