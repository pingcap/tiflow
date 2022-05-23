FROM golang:1.18-alpine as builder

#build
RUN apk add --no-cache \
    make \
    bash \
    gcc \
    git \
    binutils-gold \
    musl-dev

RUN mkdir -p /dataflow-engine
WORKDIR /dataflow-engine

COPY . .
RUN make engine

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 && \
    chmod +x /usr/local/bin/dumb-init

ENTRYPOINT ["/usr/local/bin/dumb-init"]
