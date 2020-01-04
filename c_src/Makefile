O = priv
S = c_src
ODPI_REPO = https://github.com/oracle/odpi
ERTS_DIR ?= $(shell erl -noshell -s init stop -eval "io:format(\"~ts/erts-~ts/\", [code:root_dir(), erlang:system_info(version)]).")

SRCS = $(wildcard $(S)/*.c)
ODPIROOT = $(S)/odpi
ODPI_LIB_DIR = $(S)/odpi/lib

TARGET = $(O)/dpi_nif.so

INCLUDEDIRS = -I$(S) -I"$(ERTS_DIR)/include" -I"$(S)/odpi/include"
CFLAGS = $(INCLUDEDIRS) -O2 -ggdb -Wall -fPIC -std=c11
LDFLAGS = -shared

ifdef LINKODPI
LDFLAGS = -L$(ODPI_LIB_DIR) -shared -lodpic
else
CFLAGS += -DEMBED
endif

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
	@if [ ! -d "$(ODPIROOT)" ]; then \
		cd $(S) && (git clone -b v3.0.0 --single-branch $(ODPI_REPO)) \
	fi
	@if [ ! -z "${LINKODPI}" -a ! -d "$(ODPI_LIB_DIR)" ]; then \
		cd $(ODPIROOT) && (make) \
	fi

clean:
	rm -rf $(O)

clean_all: clean
	rm -rf $(ODPIROOT)

test: CFLAGS += --coverage
test: clean
test: all
