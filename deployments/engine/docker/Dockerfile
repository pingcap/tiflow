<<<<<<< HEAD
FROM golang:1.25-alpine as builder
=======
FROM golang:1.27.3-alpine as builder
>>>>>>> 4ff802a6ab (*: upgrade go version to 1.27.3 (#12904))

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
