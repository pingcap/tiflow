FROM alpine:3.16 as builder

RUN apk add --no-cache curl dumb-init

COPY ./bin/tiflow /
COPY ./bin/tiflow-demoserver /tiflow-demoserver
COPY ./bin/tiflow-chaos-case /tiflow-chaos-case

ENTRYPOINT ["/usr/bin/dumb-init"]
