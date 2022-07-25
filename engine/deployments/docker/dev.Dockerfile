FROM alpine:3.16 as builder

RUN apk add --no-cache gcompat

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 && \
    chmod +x /usr/local/bin/dumb-init

COPY ./bin/tiflow /

ENTRYPOINT ["/usr/local/bin/dumb-init"]
