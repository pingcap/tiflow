<<<<<<< HEAD
FROM golang:1.25-alpine as builder
=======
FROM golang:1.27.3-alpine as builder
>>>>>>> 4ff802a6ab (*: upgrade go version to 1.27.3 (#12904))
RUN apk add --no-cache git make bash findutils
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .

RUN --mount=type=cache,target=/root/.cache/go-build,target=/go/pkg/mod make build-cdc-with-failpoint

FROM alpine:3.15
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/pingcap/tiflow/bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]

