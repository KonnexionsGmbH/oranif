O = priv
S = c_src
T = test

ifndef OTP_ERTS_DIR
$(error OTP_ERTS_DIR is not set)
endif

SRCS = $(wildcard $(S)/*.c)
ODPIROOT = $(S)/odpi
ODPI_LIB_DIR = $(S)/odpi/lib

TARGET = $(O)/dpi_nif.so

INCLUDEDIRS = -I$(S) -I"$(OTP_ERTS_DIR)/include" -I"$(S)/odpi/include"
CFLAGS = $(INCLUDEDIRS) -O2 -ggdb -Wall -fPIC -std=c11
LINKDIRS = -L$(ODPI_LIB_DIR)
LDFLAGS  = $(LINKDIRS) -shared -lodpic

ifeq ($(shell uname -s), Darwin)
	CFLAGS += -dynamiclib 
	LDFLAGS += -flat_namespace -undefined suppress
endif

all: priv odpi $(TARGET)

$(TARGET): $(SRCS)
	gcc -o $@ $(CFLAGS) $(SRCS) $(LDFLAGS)

priv:
	mkdir -p $(O)

odpi:
	@if [ ! -d "$(ODPIROOT)" ] ;\
	then \
		cd $(S) && (git clone -b v3.0.0 --single-branch https://github.com/K2InformaticsGmbH/odpi) \
	fi
	cd $(ODPIROOT) && make

clean:
	rm -rf $(O)

clean_all: clean
	rm -rf $(ODPIROOT)
