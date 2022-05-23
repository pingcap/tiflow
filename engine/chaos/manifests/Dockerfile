FROM alpine:3.14

ADD master /df-server-master
ADD executor /df-executor
ADD df-chaos-case /df-chaos-case
ADD conf /conf

RUN chmod a+x /df-server-master /df-executor /df-chaos-case

WORKDIR /

EXPOSE 10239 10240 10241
