FROM golang:1.21-alpine as builder

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
ARG GOPROXY="https://proxy.golang.org,direct"
ENV GOPROXY=$GOPROXY

RUN make engine

FROM gcr.io/pingcap-public/pingcap/alpine:alpine-3.14.6

COPY --from=builder /dataflow-engine/bin/tiflow /tiflow
