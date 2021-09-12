FROM golang:1.16-alpine
RUN apk add --no-cache git make bash gcc musl-dev
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make

ENTRYPOINT ./test.sh
