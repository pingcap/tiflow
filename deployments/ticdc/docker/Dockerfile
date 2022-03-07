FROM golang:1.16-alpine as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make

FROM alpine:3.12
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/pingcap/tiflow/bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]
