MODULE_big = parquet_fdw
OBJS = parquet_impl.o parquet_fdw.o \
	   src/Error.o \
	   src/Misc.o \
	   src/ParquetFdwReader.o \
	   src/ParquetFdwExecutionState.o \
	   src/FilterPushdown.o \
	   src/functions/ConvertCsvToParquet.o

PGFILEDESC = "parquet_fdw - foreign data wrapper for parquet"

SHLIB_LINK = -lm -lstdc++ -lparquet -larrow -lstdc++fs

EXTENSION = parquet_fdw
DATA = parquet_fdw--0.1.sql \
	   parquet_fdw--0.1--0.2.sql \
	   parquet_fdw--0.2--0.3.sql

REGRESS = basic invalid files_func multifile advanced import

EXTRA_CLEAN = sql/parquet_fdw.sql expected/parquet_fdw.out

# PG_CONFIG ?= pg_config
PG_CONFIG = /usr/pgsql-12/bin/pg_config

# parquet_impl.cpp requires C++ 11.
override PG_CXXFLAGS += -std=c++17 -Wall -Werror -Wfatal-errors -Wno-ignored-attributes

override CFLAGS := $(filter-out -fstack-clash-protection,$(CFLAGS))
override CFLAGS += -Wno-ignored-attributes

PGXS := $(shell $(PG_CONFIG) --pgxs)

override CC = clang
override CXX = clang++

# pass CCFLAGS (when defined) to both C and C++ compilers.
ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

ifdef DEBUG
	override PG_CXXFLAGS += -O0 -g
	override PG_CFLAGS += -O0 -g
endif

include $(PGXS)

# XXX: PostgreSQL below 11 does not automatically add -fPIC or equivalent to C++
# flags when building a shared library, have to do it here explicitely.
ifeq ($(shell test $(VERSION_NUM) -lt 110000; echo $$?), 0)
	override CXXFLAGS += $(CFLAGS_SL)
endif

# PostgreSQL uses link time optimization option which may break compilation
# (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

# XXX: a hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<
