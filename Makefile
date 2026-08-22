# Building without Bazel: the multiplexer, the C++ library and the Python
# package from the system's compiler, protobuf, Boost and pybind11. See
# docs/building.md. The source lists in make/sources.mk are generated from
# the BUILD files by ./format.sh, so the two builds never disagree about
# which files exist; everything else about the build is in this file.
#
#   make -j            build/bin/mxcontrol, build/libmultiplexer.a, build/python/
#   make check         the C++ and Python unit tests, against what was built
#   make wheel         build/dist/multiplexer-<VERSION>-*.whl, for pip
#   make install       mxcontrol, the library and the headers under PREFIX
#   make RULES=your.rules ...   generate the constants from your rules file
#
# Debian and Ubuntu packages: g++ make protobuf-compiler libprotobuf-dev
# libboost-dev libboost-program-options-dev python3-dev python3-protobuf
# pybind11-dev python3-pybind11; libgtest-dev for `make check`;
# python3-pip python3-setuptools python3-wheel for `make wheel`.

RULES ?= multiplexer.rules
BUILD ?= build
PREFIX ?= /usr/local
PYTHON ?= python3
PROTOC ?= protoc
CXX ?= g++
AR ?= ar
VERSION ?= 0.1.0
# The optimisation and debug flags; the rest is what the code needs.
CXXFLAGS ?= -O2 -g -DNDEBUG
GTEST_LIBS ?= -lgtest -lgtest_main

GEN := $(BUILD)/gen
OBJ := $(BUILD)/obj
PY := $(BUILD)/python

ALL_CXXFLAGS := $(CXXFLAGS) -std=c++17 -Wall -Wextra -fPIC -pthread
ALL_CPPFLAGS := -I. -I$(GEN) $(CPPFLAGS)
ALL_LDLIBS := -lprotobuf -lboost_program_options -pthread $(LDLIBS)

include make/sources.mk

# Generated code: protocol buffers, the constants from the rules file, the
# log type ids, and where the rules file is for the testing package.
PROTO_CC := $(patsubst %.proto,$(GEN)/%.pb.cc,$(PROTOS))
PROTO_H := $(PROTO_CC:.cc=.h)
PROTO_PY := $(patsubst %.proto,$(GEN)/%_pb2.py,$(PROTOS))
CONSTANTS_H := $(GEN)/multiplexer/multiplexer.constants.h
TYPE_IDS_H := $(GEN)/multiplexer/mxlog/type_id_constants.h
GEN_H := $(PROTO_H) $(TYPE_IDS_H) $(CONSTANTS_H)
GEN_PY := $(PROTO_PY) $(GEN)/multiplexer/multiplexer_constants.py $(GEN)/multiplexer/mxlog/type_id_constants.py \
          $(GEN)/multiplexer/testing/rules_path.py

LIB_OBJS := $(patsubst %.cc,$(OBJ)/%.o,$(LIB_SRCS)) $(patsubst $(GEN)/%.cc,$(OBJ)/%.o,$(PROTO_CC))
MXCONTROL_OBJS := $(patsubst %.cc,$(OBJ)/%.o,$(MXCONTROL_SRCS))
GENERATE_CONSTANTS_OBJS := $(patsubst %.cc,$(OBJ)/%.o,$(GENERATE_CONSTANTS_SRCS)) \
                           $(patsubst $(GEN)/%.cc,$(OBJ)/%.o,$(PROTO_CC))
NATIVE_OBJ := $(OBJ)/multiplexer/_native.o
CC_TEST_BINS := $(patsubst %.cc,$(BUILD)/tests/%,$(CC_TEST_SRCS))

LIBRARY := $(BUILD)/libmultiplexer.a
MXCONTROL := $(BUILD)/bin/mxcontrol
GENERATE_CONSTANTS := $(BUILD)/bin/generate_constants

# The Python package: the sources, the generated modules, the extension,
# and __init__.py where Bazel needed none.
PY_PACKAGE := $(patsubst %,$(PY)/%,$(PY_FILES)) $(patsubst $(GEN)/%,$(PY)/%,$(GEN_PY)) \
              $(PY)/multiplexer/__init__.py $(PY)/multiplexer/util/__init__.py \
              $(PY)/lib/__init__.py $(PY)/lib/logging/__init__.py $(PY)/multiplexer/_native.so
PY_TESTS := $(patsubst %,$(PY)/%,$(PY_TEST_FILES))

.PHONY: all python check check-cc check-py wheel install clean
.DELETE_ON_ERROR:
.SECONDARY:

all: $(MXCONTROL) $(LIBRARY) python

python: $(PY_PACKAGE)

# Generated sources.

$(GEN)/%.pb.cc $(GEN)/%.pb.h $(GEN)/%_pb2.py: %.proto
	@mkdir -p $(GEN)
	$(PROTOC) -I. --cpp_out=$(GEN) --python_out=$(GEN) $<

$(TYPE_IDS_H): multiplexer/mxlog/type_id_constants.txt multiplexer/mxlog/gen_type_id_constants.py
	@mkdir -p $(dir $@)
	$(PYTHON) multiplexer/mxlog/gen_type_id_constants.py $< $@

$(GEN)/multiplexer/mxlog/type_id_constants.py: multiplexer/mxlog/type_id_constants.txt
	@mkdir -p $(dir $@)
	cp $< $@

$(CONSTANTS_H): $(RULES) $(GENERATE_CONSTANTS)
	@mkdir -p $(dir $@)
	$(GENERATE_CONSTANTS) $< $@

$(GEN)/multiplexer/multiplexer_constants.py: $(RULES) $(GENERATE_CONSTANTS)
	@mkdir -p $(dir $@)
	$(GENERATE_CONSTANTS) $< $@

$(GEN)/multiplexer/testing/rules_path.py: $(RULES)
	@mkdir -p $(dir $@)
	@echo '"""Generated: the rules file this build used."""' > $@
	@echo 'RULES = "$(abspath $(RULES))"' >> $@

# Objects. Every object of the library and the tool waits for the
# generated headers it may include; the tool's own objects only for the
# ones that do not come from the tool, or nothing could be built first.
# After the first build the .d files know the real includes.

$(OBJ)/%.o: %.cc
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) $(ALL_CPPFLAGS) -MMD -MP -c $< -o $@

$(OBJ)/%.o: $(GEN)/%.cc
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) $(ALL_CPPFLAGS) -MMD -MP -c $< -o $@

$(GENERATE_CONSTANTS_OBJS): | $(PROTO_H) $(TYPE_IDS_H)
$(filter-out $(GENERATE_CONSTANTS_OBJS),$(LIB_OBJS) $(MXCONTROL_OBJS) $(NATIVE_OBJ)): | $(GEN_H)

$(NATIVE_OBJ): ALL_CPPFLAGS += $(shell $(PYTHON) -m pybind11 --includes)

-include $(LIB_OBJS:.o=.d) $(MXCONTROL_OBJS:.o=.d) $(GENERATE_CONSTANTS_OBJS:.o=.d) $(NATIVE_OBJ:.o=.d)

# Binaries and the library.

$(GENERATE_CONSTANTS): $(GENERATE_CONSTANTS_OBJS)
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) -o $@ $^ $(ALL_LDLIBS)

$(LIBRARY): $(LIB_OBJS)
	@mkdir -p $(dir $@)
	$(AR) rcs $@ $^

# The subcommands register themselves from static initializers, so they
# are linked as objects, not from the library, which would drop them.
$(MXCONTROL): $(MXCONTROL_OBJS) $(LIBRARY)
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) -o $@ $(MXCONTROL_OBJS) $(LIBRARY) $(ALL_LDLIBS)

$(PY)/multiplexer/_native.so: $(NATIVE_OBJ) $(LIBRARY)
	@mkdir -p $(dir $@)
	$(CXX) -shared $(ALL_CXXFLAGS) -o $@ $(NATIVE_OBJ) $(LIBRARY) $(ALL_LDLIBS)

# The Python package.

$(PY)/%.py: %.py
	@mkdir -p $(dir $@)
	cp $< $@

$(PY)/%.py: $(GEN)/%.py
	@mkdir -p $(dir $@)
	cp $< $@

$(PY)/multiplexer/__init__.py $(PY)/multiplexer/util/__init__.py $(PY)/lib/__init__.py $(PY)/lib/logging/__init__.py:
	@mkdir -p $(dir $@)
	touch $@

# Tests.

$(BUILD)/tests/%: %.cc $(LIBRARY)
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) $(ALL_CPPFLAGS) -o $@ $< $(LIBRARY) $(GTEST_LIBS) $(ALL_LDLIBS)

$(CC_TEST_BINS): | $(GEN_H)

check: check-cc check-py

# Each test's output goes to a .log next to its binary; a failure prints it.
check-cc: $(CC_TEST_BINS)
	@for test in $(CC_TEST_BINS); do \
	    if $$test > $$test.log 2>&1; then echo "passed: $$test"; else cat $$test.log; echo "FAILED: $$test"; exit 1; fi; \
	done

# The Python tests find the multiplexer and the rules file the way they do
# under Bazel, through TEST_SRCDIR, pointed at a runfiles tree made of links.
RUNFILES := $(BUILD)/runfiles
check-py: python $(PY_TESTS) $(MXCONTROL)
	@rm -rf $(RUNFILES) && mkdir -p $(RUNFILES)/mx/mxcontrol $(BUILD)/tmp
	@ln -s $(abspath $(MXCONTROL)) $(RUNFILES)/mx/mxcontrol/mxcontrol
	@ln -s $(abspath $(RULES)) $(RUNFILES)/mx/multiplexer.rules
	cd $(PY) && PYTHONPATH=. MXCONTROL=$(abspath $(MXCONTROL)) TEST_SRCDIR=$(abspath $(RUNFILES)) TEST_WORKSPACE=mx \
	    TEST_TMPDIR=$(abspath $(BUILD)/tmp) $(PYTHON) -m unittest $(subst /,.,$(patsubst %.py,%,$(PY_TEST_FILES)))

# The wheel: the package without the tests, with setup.py from make/.

wheel: python
	rm -rf $(BUILD)/wheel && mkdir -p $(BUILD)/wheel $(BUILD)/dist
	cp -r $(PY)/multiplexer $(PY)/lib $(BUILD)/wheel/
	find $(BUILD)/wheel -name '*_test.py' -delete
	sed 's/@VERSION@/$(VERSION)/' make/setup.py > $(BUILD)/wheel/setup.py
	cd $(BUILD)/wheel && $(PYTHON) -m pip wheel --no-deps --no-build-isolation -q -w ../dist .
	@ls $(BUILD)/dist/*.whl

install: $(MXCONTROL) $(LIBRARY)
	install -d $(DESTDIR)$(PREFIX)/bin $(DESTDIR)$(PREFIX)/lib
	install -m 755 $(MXCONTROL) $(DESTDIR)$(PREFIX)/bin/
	install -m 644 $(LIBRARY) $(DESTDIR)$(PREFIX)/lib/
	@for header in $(HEADERS); do install -D -m 644 $$header $(DESTDIR)$(PREFIX)/include/mx/$$header; done
	@for header in $(GEN_H); do install -D -m 644 $$header $(DESTDIR)$(PREFIX)/include/mx/$${header#$(GEN)/}; done

clean:
	rm -rf $(BUILD)
