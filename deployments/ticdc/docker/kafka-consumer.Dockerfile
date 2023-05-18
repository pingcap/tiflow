FROM golang:1.19.1-alpine3.15 as builder
RUN apk add --no-cache make bash git
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN make kafka_consumer

FROM alpine:3.15
COPY --from=builder  /go/src/github.com/pingcap/tiflow/bin/cdc_kafka_consumer /cdc_kafka_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]
