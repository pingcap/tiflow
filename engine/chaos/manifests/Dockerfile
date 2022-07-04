FROM alpine:3.14

ADD tiflow /tiflow
ADD df-chaos-case /df-chaos-case
ADD conf /conf

RUN chmod a+x /tiflow /df-chaos-case

WORKDIR /

EXPOSE 10239 10240 10241
