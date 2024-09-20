FROM golang:1.23-alpine as builder
MAINTAINER siddontang

RUN apk add --no-cache git make

RUN mkdir -p /go/src/github.com/pingcap/dm
WORKDIR /go/src/github.com/pingcap/dm

# Cache dependencies
COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN apk update && apk add bash

RUN make dm-worker dm-master dmctl

FROM alpine:3.15

# keep compatibility
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-worker /dm-worker
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-master /dm-master
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dmctl /dmctl

COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-worker /bin/dm-worker
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dm-master /bin/dm-master
COPY --from=builder /go/src/github.com/pingcap/dm/bin/dmctl /bin/dmctl

EXPOSE 8261 8262 8291

