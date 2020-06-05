FROM golang:1.13 as builder
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .
RUN go mod download
RUN make

FROM alpine:3.11
RUN apk add --no-cache tzdata
COPY --from=builder /go/src/github.com/pingcap/ticdc/bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]
