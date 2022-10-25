FROM gcr.io/pingcap-public/pingcap/alpine:alpine-3.14.6 as builder

RUN apk add --no-cache curl dumb-init

COPY ./bin/tiflow /
COPY ./bin/tiflow-demoserver /tiflow-demoserver
COPY ./bin/tiflow-chaos-case /tiflow-chaos-case

ENTRYPOINT ["/usr/bin/dumb-init"]
