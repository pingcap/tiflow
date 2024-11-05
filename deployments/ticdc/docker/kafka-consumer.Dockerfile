FROM golang:1.23.2-alpine as builder
RUN apk add --no-cache make bash git
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN make kafka_consumer

FROM ghcr.io/pingcap-qe/bases/tools-base:v1.9.2
COPY --from=builder  /go/src/github.com/pingcap/tiflow/bin/cdc_kafka_consumer /cdc_kafka_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]
