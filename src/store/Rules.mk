d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), server.cc)

$(d)server: $(o)server.o \
	$(LIB-lib-configuration) \
	$(LIB-lib-message) \
	$(LIB-lib-tcptransport) \
	$(LIB-lib-transport) \
	$(LIB-lib-udptransport) \
	$(LIB-replication-common-replica) \
	$(LIB-replication-ir-replica) \
	$(LIB-replication-vr-replica) \
	$(LIB-store-common-backend-loader) \
	$(LIB-store-common-partitioner) \
	$(LIB-store-common-stats) \
  $(LIB-store-common-timestamp) \
  $(LIB-store-janusstore-server) \
  $(LIB-store-mortystorev2-server) \
  $(LIB-store-spannerstore-server) \
  $(LIB-store-spannerstore-truetime) \
  $(LIB-store-strongstore-server) \
  $(LIB-store-strongstore-unreplicatedserver) \
  $(LIB-store-tapirstore-server) \
  $(LIB-store-weakstore-server)

BINS += $(d)server
