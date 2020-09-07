FROM golang:1.14-alpine as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make

FROM alpine:3.12
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/pingcap/ticdc/bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]
