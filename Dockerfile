FROM golang:1.18-alpine as builder

#build
RUN apk add --no-cache \
    make \
    bash \
    gcc \
    git \
    binutils-gold \
    musl-dev

RUN mkdir -p /data-engine
WORKDIR /data-engine

COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

# Protoc setup
RUN apk add --no-cache \
    curl \
    gcompat \
    unzip \
    wget

ENV PROTOC_VERSION "3.8.0"
RUN curl -L https://github.com/google/protobuf/releases/download/v$PROTOC_VERSION/protoc-$PROTOC_VERSION-linux-x86_64.zip -o protoc.zip && \
    unzip protoc.zip -d protoc && \
    mv protoc/bin/protoc /usr/bin/ && \
    chmod +x /usr/bin/protoc

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 && \
    chmod +x /usr/local/bin/dumb-init

COPY . .

RUN make tools_setup
RUN make

ENTRYPOINT ["/usr/local/bin/dumb-init"]
