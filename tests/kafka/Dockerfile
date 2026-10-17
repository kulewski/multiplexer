# Every process of the Kafka comparison but the broker (docs/kafka.md): the
# multiplexer and the C++ library built with the Makefile and installed,
# their constants from tests/kafka/bench.rules; mx_bench on that library,
# found with pkg-config as a program of yours would find it; kafka_bench on
# librdkafka from the distribution. tests/kafka/run.sh builds it from the
# working tree and runs each process of a run in a container of its own, on
# the host's network.
ARG BASE=debian:13
FROM ${BASE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
        g++ \
        make \
        pkg-config \
        protobuf-compiler \
        libprotobuf-dev \
        libasio-dev \
        libre2-dev \
        python3 \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . /src
RUN make -j"$(nproc)" RULES=tests/kafka/bench.rules install PREFIX=/usr/local
# The installed headers first, then the tree's for common.h; both programs
# with the warnings this repository's own code is held to.
RUN g++ -std=c++17 -O2 -DNDEBUG -Wall -Wextra -Werror $(pkg-config --cflags multiplexer) -I. \
        tests/kafka/mx_bench.cc $(pkg-config --static --libs multiplexer) -o /usr/local/bin/mx_bench \
    && g++ -std=c++17 -O2 -DNDEBUG -Wall -Wextra -Werror -I. \
        tests/kafka/kafka_bench.cc $(pkg-config --libs rdkafka++) -pthread -o /usr/local/bin/kafka_bench
