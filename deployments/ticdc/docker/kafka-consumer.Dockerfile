FROM golang:1.20-alpine as builder
RUN apk add --no-cache make bash git
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=type=cache,target=/root/.cache/go-build make kafka_consumer

FROM alpine:3.15
COPY --from=builder  /go/src/github.com/pingcap/tiflow/bin/cdc_kafka_consumer /cdc_kafka_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]
