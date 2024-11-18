FROM golang:1.23-alpine as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/pingcap/tiflow
COPY . .
ENV CDC_ENABLE_VENDOR=0
RUN make cdc

FROM ghcr.io/pingcap-qe/bases/tools-base:v1.9.2
# RUN apk add --no-cache tzdata bash curl socat
RUN --mount=type=cache,target=/var/cache/dnf \
    dnf upgrade-minimal -y && \
    dnf install -y tzdata bash curl socat
COPY --from=builder /go/src/github.com/pingcap/tiflow/bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]
