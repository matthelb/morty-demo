d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), lookup3.cc message.cc memory.cc latency.cc \
	configuration.cc transport.cc udptransport.cc tcptransport.cc \
  simtransport.cc repltransport.cc persistent_register.cc io_utils.cc \
	threadpool.cc timer.cc partitionedthreadpool.cc)

PROTOS += $(addprefix $(d), latency-format.proto)

LIB-lib-message := $(o)message.o
LIB-lib-configuration := $(o)configuration.o $(LIB-lib-message)
LIB-lib-latency := $(o)latency.o $(o)latency-format.o $(LIB-lib-message)
LIB-lib-threadpool := $(o)threadpool.o $(LIB-lib-message)
LIB-lib-transport := $(o)transport.o
LIB-lib-tcptransport := $(o)tcptransport.o $(LIB-lib-configuration) \
	$(LIB-lib-message) $(LIB-lib-latency) $(LIB-lib-threadpool) \
	$(LIB-lib-transport)
LIB-lib-udptransport := $(o)udptransport.o $(LIB-lib-configuration) \
	$(LIB-lib-message) $(LIB-lib-threadpool) $(LIB-lib-transport)
LIB-lib-persistent-register := $(o)persistent_register.o $(LIB-lib-message)
LIB-lib-io-utils := $(o)io_utils.o
LIB-lib-partitionedthreadpool := $(o)partitionedthreadpool.o $(LIB-lib-message)

include $(d)tests/Rules.mk
