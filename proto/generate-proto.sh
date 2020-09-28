#!/usr/bin/env bash

echo "generate canal protocol code..."

[ ! -d ./canal ] && mkdir ./canal
[ ! -d ./cdclog ] && mkdir ./cdclog

protoc --gogofast_out=./ ./canal/EntryProtocol.proto
protoc --gogofast_out=./ ./canal/CanalProtocol.proto
