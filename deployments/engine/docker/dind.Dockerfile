# In this file, we build an image with `docker-cli in docker` for engine integration tests.
# For `dockerd in docker`, please refer to https://hub.docker.com/_/docker/tags?page=1&name=dind.
FROM golang:1.19-alpine as builder

# If you add a new command dependency to engine integration test, add here and 
# rebuild this image as well
RUN apk update && apk add --no-cache docker-cli \
    make bash curl git \
    jq mariadb-client openssl

# For better compatibility, use `docker-compose` directly instead of the subcommand 'docker compose'
RUN wget -O /usr/bin/docker-compose https://github.com/docker/compose/releases/download/v2.10.2/docker-compose-linux-x86_64 && \
    chmod +x /usr/bin/docker-compose

# mc for minio related tests \
RUN wget -O /usr/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x /usr/bin/mc
