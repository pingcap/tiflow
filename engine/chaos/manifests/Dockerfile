FROM gcr.io/pingcap-public/pingcap/alpine:alpine-3.14.6

ADD tiflow /tiflow
ADD tiflow-chaos-case /tiflow-chaos-case
ADD engine-conf /engine-conf

RUN chmod a+x /tiflow /tiflow-chaos-case

WORKDIR /

EXPOSE 10239 10240 10241
