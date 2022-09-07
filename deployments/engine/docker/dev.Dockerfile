FROM alpine:3.16 as builder

ENV http_proxy=http://10.2.7.60:8899
ENV https_proxy=http://10.2.7.60:8899
RUN apk add --no-cache curl dumb-init
ENV http_proxy=
ENV https_proxy=

COPY ./bin/tiflow /
COPY ./bin/tiflow-demoserver /tiflow-demoserver
COPY ./bin/tiflow-chaos-case /tiflow-chaos-case

ENTRYPOINT ["/usr/bin/dumb-init"]
