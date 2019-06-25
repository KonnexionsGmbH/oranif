MKDIR_P = mkdir -p

O = priv
S = c_src
T = test
ODPI_LIB_DIR = $(S)/odpi/lib

ifndef OTP_ERTS_DIR
$(error OTP_ERTS_DIR is not set)
endif

ORANIF_SRCS = $(wildcard  $(S)/*.c)

TARGETS = $(O)/dpi_nif.so

BASECXXFLAGS = -ggdb -Wall -fPIC -shared -std=c11
INCLUDEDIRS = -I$(S) -I"$(OTP_ERTS_DIR)/include" -I"$(S)/odpi/include"
LINKDIRS = -L$(ODPI_LIB_DIR)
BASELINKFLAGS = -lodpic

ifeq ($(shell uname -s), Darwin)
	CXXFLAGS = $(BASECXXFLAGS) -dynamiclib 
	LINKFLAGS = $(BASELINKFLAGS) -flat_namespace -undefined suppress
else
	CXXFLAGS = $(BASECXXFLAGS)
	LINKFLAGS = $(BASELINKFLAGS)
endif

all: priv $(TARGETS)

$(TARGETS): odpi $(ORANIF_SRCS)
	gcc -o $@ $(CXXFLAGS) $(INCLUDEDIRS) $(LINKDIRS) $(ORANIF_SRCS) $(LINKFLAGS)

priv:
	$(MKDIR_P) $(O)

odpi:
	@if [ ! -d "c_src/odpi" ] ;\
	then \
		cd c_src && (git clone -b v3.0.0 --single-branch https://github.com/K2InformaticsGmbH/odpi) \
	fi
	cd c_src/odpi && make

clean:
	rm -rf priv
