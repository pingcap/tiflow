FROM alpine:3.14

ADD tiflow /tiflow
ADD tiflow-chaos-case /tiflow-chaos-case
ADD conf /conf

RUN chmod a+x /tiflow /tiflow-chaos-case

WORKDIR /

EXPOSE 10239 10240 10241
