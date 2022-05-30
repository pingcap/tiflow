FROM alpine:3.14

ADD df-master /df-master
ADD df-executor /df-executor
ADD df-chaos-case /df-chaos-case
ADD conf /conf

RUN chmod a+x /df-master /df-executor /df-chaos-case

WORKDIR /

EXPOSE 10239 10240 10241
