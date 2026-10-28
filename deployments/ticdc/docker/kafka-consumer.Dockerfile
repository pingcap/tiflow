<<<<<<< HEAD
FROM golang:1.25-alpine AS builder
=======
FROM golang:1.27.3-alpine AS builder
>>>>>>> 4ff802a6ab (*: upgrade go version to 1.27.3 (#12904))
RUN apk add --no-cache make bash git build-base
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=type=cache,target=/root/.cache/go-build make kafka_consumer

FROM alpine:3.15

RUN apk update && apk add tzdata

ENV TZ=Asia/Shanghai

COPY --from=builder  /go/src/github.com/pingcap/tiflow/bin/cdc_kafka_consumer /cdc_kafka_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]
