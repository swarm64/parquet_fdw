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

PG_CONFIG = pg_config
PG_CXXFLAGS += -std=c++17 -Wall -Werror -Wfatal-errors

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

override CFLAGS := $(filter-out -fstack-clash-protection,$(CFLAGS))

ifdef DEBUG
	DEBUG_FLAGS = -O0 -g
	override CPPFLAGS := $(filter-out -O2,$(CPPFLAGS)) $(DEBUG_FLAGS)
	override CXXFLAGS := $(filter-out -O2,$(CXXFLAGS)) $(DEBUG_FLAGS)
	override BITCODE_CXXFLAGS := $(filter-out -O2,$(BITCODE_CXXFLAGS)) $(DEBUG_FLAGS)
	override CFLAGS := $(filter-out -O2,$(CFLAGS)) $(DEBUG_FLAGS)
endif

# XXX: PostgreSQL below 11 does not automatically add -fPIC or equivalent to C++
# flags when building a shared library, have to do it here explicitely.
ifeq ($(shell test $(VERSION_NUM) -lt 110000; echo $$?), 0)
	override CXXFLAGS += $(CFLAGS_SL)
endif

# # PostgreSQL uses link time optimization option which may break compilation
# # (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c
# 
# # XXX: a hurdle to use common compiler flags when building bytecode from C++
# # files. should be not unnecessary, but src/Makefile.global omits passing those
# # flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) -o $@ $<
