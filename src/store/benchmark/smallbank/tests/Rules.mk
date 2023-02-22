d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(addprefix $(d), amalgamate_test.cc bal_test.cc deposit_test.cc transact_test.cc write_check_test.cc utils_test.cc smallbank_generator_test.cc smallbank_client_test.cc)

$(d)smallbank_generator_test: $(o)smallbank_generator_test.o \
	$(LIB-io-utils) $(o)../smallbank_generator.o $(o)../smallbank-proto.o $(o)../utils.o \
	$(GTEST_MAIN)
$(d)smallbank_client_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)smallbank_client_test.o $(GTEST_MAIN)
$(d)smallbank_utils_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)utils_test.o $(GTEST_MAIN) $(GMOCK) $(GTEST)
$(d)amalgamate_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)amalgamate_test.o $(GTEST_MAIN) $(GMOCK) $(GTEST)
$(d)bal_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)bal_test.o $(GTEST_MAIN) $(GMOCK) $(GTEST)
$(d)deposit_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)deposit_test.o $(GTEST_MAIN) $(GMOCK) $(GTEST)
$(d)transact_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)transact_test.o $(GTEST_MAIN) $(GMOCK) $(GTEST)
$(d)write_check_test: $(LIB-io-utils) $(LIB-bench-client) $(LIB-store-frontend) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(LIB-smallbank) $(o)write_check_test.o $(GTEST_MAIN) $(GMOCK) $(GTEST)


TEST_BINS +=  $(addprefix $(d), smallbank_generator_test smallbank_utils_test amalgamate_test bal_test deposit_test transact_test write_check_test)
