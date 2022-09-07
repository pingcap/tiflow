#!/bin/bash

docker build --platform=linux/amd64 -f ./Dockerfile_local -t hub.pingcap.net/dataflow/engine:maxtest .
docker image push hub.pingcap.net/dataflow/engine:maxtest
