FROM alpine:3.10
RUN apk add --no-cache tzdata
ADD dm-master /dm-master
ADD dm-worker /dm-worker
ADD dmctl /dmctl
ADD chaos-case /chaos-case
ADD conf /conf

RUN chmod +x /dm-master
RUN chmod +x /dm-worker
RUN chmod +x /dmctl
RUN chmod +x /chaos-case

WORKDIR /

EXPOSE 8291 8261 8262
