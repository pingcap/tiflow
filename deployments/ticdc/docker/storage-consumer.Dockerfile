FROM golang:1.23-alpine as builder
RUN apk add --no-cache make bash git
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN make storage_consumer

FROM ghcr.io/pingcap-qe/bases/tools-base:v1.9.2
COPY --from=builder  /go/src/github.com/pingcap/tiflow/bin/cdc_storage_consumer /cdc_storage_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]
