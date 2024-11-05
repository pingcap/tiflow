FROM golang:1.23-alpine as builder

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

FROM ghcr.io/pingcap-qe/bases/tools-base:v1.9.2

COPY --from=builder /dataflow-engine/bin/tiflow /tiflow

RUN /tiflow -v