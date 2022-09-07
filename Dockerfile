FROM golang:1.18-alpine as builder

#build
RUN apk add --no-cache \
    make \
    bash \
    gcc \
    bind-tools \
    git \
    binutils-gold \
    musl-dev

RUN mkdir -p /dataflow-engine
WORKDIR /dataflow-engine

COPY . .
RUN make engine

FROM alpine:3.16
COPY --from=builder /bin/nslookup /bin/


COPY --from=builder /dataflow-engine/bin/tiflow /tiflow
COPY --from=builder /dataflow-engine/engine/deployments/docker-compose/config/master.toml  /master.toml
COPY --from=builder /dataflow-engine/engine/deployments/docker-compose/config/executor.toml  /executor.toml
COPY --from=builder /dataflow-engine/bin/tiflow-demoserver /tiflow-demoserver
