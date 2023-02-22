d := $(dir $(lastword $(MAKEFILE_LIST)))

GTEST_SRCS += $(d)woundwait-test.cc

$(d)spanner-test: $(LIB-wound-wait) $(LIB-store-common) $(o)woundwait-test.o \
	$(GTEST_MAIN)

TEST_BINS += $(d)spanner-test
