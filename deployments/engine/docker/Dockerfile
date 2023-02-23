FROM golang:1.19 as builder

RUN mkdir -p /dataflow-engine
WORKDIR /dataflow-engine

COPY . .
RUN make engine

FROM rockylinux:9.0

COPY --from=builder /dataflow-engine/bin/tiflow /tiflow
