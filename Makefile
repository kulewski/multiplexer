# Building without Bazel: the multiplexer, the C++ library and the Python
# package from the system's compiler, protobuf, Asio and pybind11. See
# docs/building.md. The source lists in make/sources.mk are generated from
# the BUILD files by ./format.sh, so the two builds never disagree about
# which files exist; everything else about the build is in this file.
#
#   make -j            build/bin/mxcontrol, build/libmultiplexer.a, build/python/
#   make check         the C++ and Python unit tests, against a build of their own
#                      whose constants come from tests/testing.rules
#   make wheel         build/dist/mx_multiplexer-<VERSION>-*.whl, for pip, with mxcontrol
#                      inside, stripped
#   make install       mxcontrol, generate_constants, the library, the headers and
#                      a pkg-config file under PREFIX
#   make RULES=your.rules ...   generate the constants from your rules file
#
# Debian and Ubuntu packages: g++ make protobuf-compiler libprotobuf-dev
# libasio-dev python3-dev python3-protobuf
# pybind11-dev python3-pybind11; libgtest-dev for `make check`;
# python3-pip python3-setuptools python3-wheel for `make wheel`.

RULES ?= multiplexer.rules
BUILD ?= build
PREFIX ?= /usr/local
PYTHON ?= python3
PROTOC ?= protoc
CXX ?= g++
AR ?= ar
STRIP ?= strip
VERSION ?= 2.3.1
# A version given as its tag, v2.3.1, as the release workflow passes it in
# the environment, is taken without the v: the .pc file carries the number,
# which pkg-config compares.
override VERSION := $(VERSION:v%=%)
# The optimisation and debug flags; the rest is what the code needs.
CXXFLAGS ?= -O2 -g -DNDEBUG
GTEST_LIBS ?= -lgtest -lgtest_main

GEN := $(BUILD)/gen
OBJ := $(BUILD)/obj
PY := $(BUILD)/python
# _native.pyi needs pybind11-stubgen in $(PYTHON); without it the package has no stub for the extension.
NATIVE_PYI := $(if $(shell $(PYTHON) -c "import pybind11_stubgen" 2>/dev/null && echo yes),$(PY)/multiplexer/_native.pyi,)

ALL_CXXFLAGS := $(CXXFLAGS) -std=c++17 -Wall -Wextra -fPIC -pthread -DASIO_STANDALONE
ALL_CPPFLAGS := -I. -I$(GEN) $(CPPFLAGS)
ALL_LDLIBS := -lprotobuf -pthread $(LDLIBS)

include make/sources.mk

# Generated code: protocol buffers, the constants from the rules file, the
# log type ids, and where the rules file is for the testing package.
PROTO_CC := $(patsubst %.proto,$(GEN)/%.pb.cc,$(PROTOS))
PROTO_H := $(PROTO_CC:.cc=.h)
PROTO_PY := $(patsubst %.proto,$(GEN)/%_pb2.py,$(PROTOS))
# protoc writes the stubs of the generated modules (--pyi_out) from 3.20 on;
# an older one, Ubuntu 22.04's 3.12, builds the package without them.
PROTOC_HAS_PYI := $(if $(shell printf '%s\n' 3.20 "$$($(PROTOC) --version | sed 's/.* //')" | sort -V | head -1 | grep -qx 3.20 && echo yes),yes,)
# The protobuf runtime the wheel asks for, from the same protoc: its code
# from 3.20 on needs the runtime's builder module, new in 3.20, and from
# 3.19 that release; an older protoc's, Ubuntu 22.04's 3.12 say, needs a
# runtime as new as itself and one older than 4, which refuses that code
# at import. packaging/wheels.sh's pinned protoc makes it protobuf>=3.20.
PROTOC_VERSION := $(shell $(PROTOC) --version | sed 's/.* //')
PROTOC_HAS_319 := $(if $(shell printf '%s\n' 3.19 "$(PROTOC_VERSION)" | sort -V | head -1 | grep -qx 3.19 && echo yes),yes,)
comma := ,
PROTOBUF_REQUIREMENT := $(if $(PROTOC_HAS_PYI),protobuf>=3.20,$(if $(PROTOC_HAS_319),protobuf>=3.19,protobuf>=$(PROTOC_VERSION)$(comma)<4))
PYI_OUT := $(if $(PROTOC_HAS_PYI),--pyi_out=$(GEN),)
PROTO_PYI := $(if $(PROTOC_HAS_PYI),$(PROTO_PY:.py=.pyi),)
CONSTANTS_H := $(GEN)/multiplexer/multiplexer.constants.h
TYPE_IDS_H := $(GEN)/multiplexer/mxlog/type_id_constants.h
GEN_H := $(PROTO_H) $(TYPE_IDS_H) $(CONSTANTS_H)
GEN_PY := $(PROTO_PY) $(GEN)/multiplexer/multiplexer_constants.py $(GEN)/multiplexer/type_id_constants.py
# The stubs type checkers read, one per generated module; the package
# carries them with a py.typed marker (PEP 561).
GEN_PYI := $(PROTO_PYI) $(GEN)/multiplexer/multiplexer_constants.pyi $(GEN)/multiplexer/type_id_constants.pyi

LIB_OBJS := $(patsubst %.cc,$(OBJ)/%.o,$(LIB_SRCS)) $(patsubst $(GEN)/%.cc,$(OBJ)/%.o,$(PROTO_CC))
# The system rules compiled into mxcontrol for generate_rules: a generated
# source, listed here by hand, as PROTO_CC is.
SYSTEM_RULES_CC := $(GEN)/mxcontrol/system_rules_text.cc
MXCONTROL_OBJS := $(patsubst %.cc,$(OBJ)/%.o,$(MXCONTROL_SRCS)) $(OBJ)/mxcontrol/system_rules_text.o
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
              $(patsubst $(GEN)/%,$(PY)/%,$(GEN_PYI)) $(PY)/multiplexer/py.typed $(NATIVE_PYI) \
              $(PY)/multiplexer/__init__.py $(PY)/multiplexer/util/__init__.py $(PY)/multiplexer/_native.so \
              $(PY)/multiplexer/bin/mxcontrol
PY_TESTS := $(patsubst %,$(PY)/%,$(PY_TEST_FILES))

.PHONY: all python check check-cc check-py wheel install clean
.DELETE_ON_ERROR:
.SECONDARY:

all: $(MXCONTROL) $(LIBRARY) python

python: $(PY_PACKAGE)

# Generated sources.

$(GEN)/%.pb.cc $(GEN)/%.pb.h $(GEN)/%_pb2.py $(GEN)/%_pb2.pyi: %.proto
	@mkdir -p $(GEN)
	$(PROTOC) -I. --cpp_out=$(GEN) --python_out=$(GEN) $(PYI_OUT) $<

# multiplexer/protocolbuffers.py keeps `from_`, a read-only alias of `sender`, the field named `from` up
# to 2.3.1.
$(GEN)/multiplexer/Multiplexer_pb2.pyi: multiplexer/Multiplexer.proto
	@mkdir -p $(GEN)
	$(PROTOC) -I. --cpp_out=$(GEN) --python_out=$(GEN) $(PYI_OUT) $<
	sed -i 's/^class MultiplexerMessage(.*/&\n    @property\n    def from_(self) -> int: ...  # deprecated: the field is sender/' $@

$(TYPE_IDS_H): multiplexer/mxlog/type_id_constants.txt multiplexer/mxlog/gen_type_id_constants.py
	@mkdir -p $(dir $@)
	$(PYTHON) multiplexer/mxlog/gen_type_id_constants.py $< $@

$(GEN)/multiplexer/type_id_constants.py: multiplexer/mxlog/type_id_constants.txt
	@mkdir -p $(dir $@)
	cp $< $@

$(GEN)/multiplexer/type_id_constants.pyi: multiplexer/mxlog/type_id_constants.txt multiplexer/mxlog/gen_type_id_constants.py
	@mkdir -p $(dir $@)
	$(PYTHON) multiplexer/mxlog/gen_type_id_constants.py $< $@

$(CONSTANTS_H): $(RULES) $(GENERATE_CONSTANTS)
	@mkdir -p $(dir $@)
	$(GENERATE_CONSTANTS) $< $@

$(GEN)/multiplexer/multiplexer_constants.py $(GEN)/multiplexer/multiplexer_constants.pyi: $(RULES) $(GENERATE_CONSTANTS)
	@mkdir -p $(dir $@)
	$(GENERATE_CONSTANTS) $< $@

# Always the system rules, whichever RULES the constants come from.
$(SYSTEM_RULES_CC): multiplexer.rules mxcontrol/embed_rules.sh
	@mkdir -p $(dir $@)
	sh mxcontrol/embed_rules.sh multiplexer.rules $@

# The package's mxcontrol, where multiplexer/mxcontrol.py looks for it;
# executable, or pip would install it as data.
$(PY)/multiplexer/bin/mxcontrol: $(MXCONTROL)
	install -D -m 755 $< $@

$(PY)/multiplexer/py.typed:
	@mkdir -p $(dir $@)
	@touch $@

# The stub of the extension, from the built module with pybind11-stubgen
# (tools/native_stub.py); built when that tool is importable, as it is where
# the wheels are made, and skipped with a note otherwise.
$(PY)/multiplexer/_native.pyi: $(PY)/multiplexer/_native.so tools/native_stub.py
	$(PYTHON) tools/native_stub.py $< $@

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
	$(CXX) $(ALL_CXXFLAGS) $(LDFLAGS) -o $@ $^ $(ALL_LDLIBS)

$(LIBRARY): $(LIB_OBJS)
	@mkdir -p $(dir $@)
	$(AR) rcs $@ $^

# The subcommands register themselves from static initializers, so they
# are linked as objects, not from the library, which would drop them.
$(MXCONTROL): $(MXCONTROL_OBJS) $(LIBRARY)
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) $(LDFLAGS) -o $@ $(MXCONTROL_OBJS) $(LIBRARY) $(ALL_LDLIBS)

$(PY)/multiplexer/_native.so: $(NATIVE_OBJ) $(LIBRARY)
	@mkdir -p $(dir $@)
	$(CXX) -shared $(ALL_CXXFLAGS) $(LDFLAGS) -o $@ $(NATIVE_OBJ) $(LIBRARY) $(ALL_LDLIBS)

# The Python package.

$(PY)/%.py: %.py
	@mkdir -p $(dir $@)
	cp $< $@

$(PY)/%.py: $(GEN)/%.py
	@mkdir -p $(dir $@)
	cp $< $@

$(PY)/%.pyi: $(GEN)/%.pyi
	@mkdir -p $(dir $@)
	cp $< $@

$(PY)/multiplexer/__init__.py $(PY)/multiplexer/util/__init__.py:
	@mkdir -p $(dir $@)
	touch $@

# Tests.

$(BUILD)/tests/%: %.cc $(LIBRARY)
	@mkdir -p $(dir $@)
	$(CXX) $(ALL_CXXFLAGS) $(ALL_CPPFLAGS) -o $@ $< $(LIBRARY) $(GTEST_LIBS) $(ALL_LDLIBS)

$(CC_TEST_BINS): | $(GEN_H)

# The tests use types of their own, which the system rules do not have: they
# run against a build of their own, in $(BUILD)/check, whose constants come
# from tests/testing.rules, as the repository's Bazel builds' do, so that
# nothing built for installing or packaging carries them. One make in that
# directory, whichever of the three is asked for.
ifeq ($(CHECK_BUILD),)
check check-cc check-py:
	$(MAKE) BUILD=$(BUILD)/check RULES=tests/testing.rules CHECK_BUILD=1 $@
else
check: check-cc check-py

# Each test's output goes to a .log next to its binary; a failure prints it.
check-cc: $(CC_TEST_BINS)
	@for test in $(CC_TEST_BINS); do \
	    if $$test > $$test.log 2>&1; then echo "passed: $$test"; else cat $$test.log; echo "FAILED: $$test"; exit 1; fi; \
	done

# The Python tests find the multiplexer and the rules files the way they do
# under Bazel, through TEST_SRCDIR, pointed at a runfiles tree made of links.
RUNFILES := $(BUILD)/runfiles
check-py: python $(PY_TESTS) $(MXCONTROL)
	@rm -rf $(RUNFILES) $(BUILD)/tmp && mkdir -p $(RUNFILES)/mx/mxcontrol $(RUNFILES)/mx/tests $(BUILD)/tmp
	@ln -s $(abspath $(MXCONTROL)) $(RUNFILES)/mx/mxcontrol/mxcontrol
	@ln -s $(abspath multiplexer.rules) $(RUNFILES)/mx/multiplexer.rules
	@ln -s $(abspath tests/testing.rules) $(RUNFILES)/mx/tests/testing.rules
	cd $(PY) && PYTHONPATH=. MXCONTROL=$(abspath $(MXCONTROL)) TEST_SRCDIR=$(abspath $(RUNFILES)) TEST_WORKSPACE=mx \
	    TEST_TMPDIR=$(abspath $(BUILD)/tmp) $(PYTHON) -m unittest $(subst /,.,$(patsubst %.py,%,$(PY_TEST_FILES)))
endif

# The wheel: the package without the tests, with setup.py and
# pyproject.toml from make/ and the README, which setup.py turns into the
# PyPI page. Its extension and its mxcontrol are stripped; build/python
# keeps the symbols.

wheel: python
	rm -rf $(BUILD)/wheel && mkdir -p $(BUILD)/wheel $(BUILD)/dist
	cp -r $(PY)/multiplexer $(BUILD)/wheel/
	find $(BUILD)/wheel -name '*_test.py' -delete
	$(STRIP) $(BUILD)/wheel/multiplexer/_native.so $(BUILD)/wheel/multiplexer/bin/mxcontrol
	sed -e 's/@VERSION@/$(VERSION)/' -e 's/@PROTOBUF_REQUIREMENT@/$(PROTOBUF_REQUIREMENT)/' make/setup.py \
	    > $(BUILD)/wheel/setup.py
	cp make/pyproject.toml README.md $(BUILD)/wheel/
	cd $(BUILD)/wheel && $(PYTHON) -m pip wheel --no-deps --no-build-isolation -q -w ../dist .
	@ls $(BUILD)/dist/*.whl

install: $(MXCONTROL) $(LIBRARY) $(GENERATE_CONSTANTS)
	install -d $(DESTDIR)$(PREFIX)/bin $(DESTDIR)$(PREFIX)/lib/pkgconfig
	install -m 755 $(MXCONTROL) $(GENERATE_CONSTANTS) $(DESTDIR)$(PREFIX)/bin/
	install -m 644 $(LIBRARY) $(DESTDIR)$(PREFIX)/lib/
	sed -e 's|@PREFIX@|$(PREFIX)|' -e 's/@VERSION@/$(VERSION)/' make/multiplexer.pc.in \
	    > $(DESTDIR)$(PREFIX)/lib/pkgconfig/multiplexer.pc
	@for header in $(HEADERS); do install -D -m 644 $$header $(DESTDIR)$(PREFIX)/include/mx/$$header; done
	@for header in $(GEN_H); do install -D -m 644 $$header $(DESTDIR)$(PREFIX)/include/mx/$${header#$(GEN)/}; done

clean:
	rm -rf $(BUILD)
