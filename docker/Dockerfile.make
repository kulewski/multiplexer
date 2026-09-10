# The build without Bazel on a clean distribution: the Makefile with the
# packages docs/building.md lists, then the unit tests, the wheel installed
# into a virtual environment and used. BASE picks the distribution.
#
#   docker build -f docker/Dockerfile.make --build-arg BASE=ubuntu:24.04 .
ARG BASE=debian:12
FROM ${BASE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
        g++ \
        make \
        protobuf-compiler \
        libprotobuf-dev \
        libboost-dev \
        libboost-program-options-dev \
        python3-dev \
        python3-protobuf \
        pybind11-dev \
        python3-pybind11 \
        libgtest-dev \
        python3-pip \
        python3-setuptools \
        python3-wheel \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY . /work
CMD make -j"$(nproc)" all check wheel \
    && python3 -m venv --system-site-packages /venv \
    && /venv/bin/pip install -q build/dist/*.whl \
    && MXCONTROL=/work/build/bin/mxcontrol /venv/bin/python make/wheel_smoke.py
