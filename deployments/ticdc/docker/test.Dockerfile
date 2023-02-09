FROM alpine:3.15
RUN apk add --no-cache tzdata bash curl socat
COPY ./bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]

