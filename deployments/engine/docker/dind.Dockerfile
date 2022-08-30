FROM golang:1.18-alpine as builder

RUN apk update && apk add --no-cache docker-cli \
    make bash curl git \
    jq mariadb-client

RUN wget -O /usr/bin/docker-compose https://github.com/docker/compose/releases/download/v2.10.2/docker-compose-linux-x86_64 && \
    chmod +x /usr/bin/docker-compose
