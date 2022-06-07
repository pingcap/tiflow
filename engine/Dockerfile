FROM golang:1.18-alpine as builder

#build
RUN apk add --no-cache \
    make \
    bash \
    gcc \
    git \
    binutils-gold \
    musl-dev

RUN mkdir -p /dataflow-engine
WORKDIR /dataflow-engine

COPY . .
RUN make engine

FROM alpine:3.16

COPY --from=builder /dataflow-engine/bin/df-master /df-master
COPY --from=builder /dataflow-engine/bin/df-executor /df-executor
COPY --from=builder /dataflow-engine/bin/df-master-client /df-master-client
COPY --from=builder /dataflow-engine/bin/df-demoserver /df-demoserver
