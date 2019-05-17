MKDIR_P = mkdir -p

O = priv
S = c_src
T = test
ODPI_LIB_DIR = $(S)/odpi/lib

OBJS = $(O)/dpi_nif.o $(O)/dpiContext_nif.o $(O)/dpiConn_nif.o $(O)/dpiStmt_nif.o $(O)/dpiData_nif.o $(O)/dpiQueryInfo_nif.o $(O)/dpiVar_nif.o $(O)/dpiObjectType_nif.o
TARGETS = $(O)/dpi_nif.so

TEST_OBJS = test/odpi_test.o
TEST_TARGETS = test/odpi_test

BASECXXFLAGS = -ggdb -Wall -fPIC -shared -I$(S) -I$(ERL_INTERFACE_DIR)/include -I"$(OTP_ERTS_DIR)/include" -I"$(S)/odpi/include"
LINKDIRS = -L$(ERL_INTERFACE_DIR)/lib -L$(ODPI_LIB_DIR)
BASELINKFLAGS = -lerl_interface -lei -lodpic

ifeq ($(shell uname -s), Darwin)
	CXXFLAGS = $(BASECXXFLAGS) -dynamiclib 
	LINKFLAGS = $(BASELINKFLAGS) -flat_namespace -undefined suppress
else
	CXXFLAGS = $(BASECXXFLAGS)
	LINKFLAGS = $(BASELINKFLAGS)
endif

#all: priv $(TEST_TARGETS) $(TARGETS) cleanup
all: priv $(TARGETS)

ORANIF_SRCS = $(OBJS:$(O)/%.o=$(S)/%.c)

#$(TARGETS): odpi $(OBJS)
$(TARGETS): odpi $(ORANIF_SRCS)
	gcc -o $@ $(CXXFLAGS) $(LINKDIRS) $(LINKFLAGS) $(ORANIF_SRCS)

#$(TEST_TARGETS): odpi $(TEST_OBJS)

priv:
	$(MKDIR_P) $(O)

odpi:
	@if [ ! -d "c_src/odpi" ] ; then cd c_src && (git clone -b v3.0.0 --single-branch https://github.com/K2InformaticsGmbH/odpi) fi
	cd c_src/odpi && make

clean:
	rm -rf priv
