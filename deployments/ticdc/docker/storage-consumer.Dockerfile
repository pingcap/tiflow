FROM golang:1.20-alpine as builder
RUN apk add --no-cache make bash
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN make storage_consumer

FROM alpine:3.15
COPY --from=builder  /go/src/github.com/pingcap/tiflow/bin/cdc_storage_consumer /cdc_storage_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]
