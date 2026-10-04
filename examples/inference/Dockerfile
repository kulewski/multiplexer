# The web app and the workers of the inference example, for compose.yaml:
# Python, the requirements (torch from its CPU index: a 200 MB download and
# about 800 MB installed, against several gigabytes for the CUDA build; the
# image is about 1.2 GB), and this directory. amd64: the wheels on PyPI are
# x86_64, so on arm64 Docker builds and runs it under emulation.
FROM --platform=linux/amd64 python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -q -r requirements.txt
COPY . .
