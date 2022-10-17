FROM golang:1.19-alpine as builder

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

COPY --from=builder /dataflow-engine/bin/tiflow /tiflow
COPY --from=builder /dataflow-engine/bin/tiflow-demoserver /tiflow-demoserver
