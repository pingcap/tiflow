#!/usr/bin/env bash

echo "generate canal protocol code..."

[ ! -d ./canal ] && mkdir ./canal
[ ! -d ./cdclog ] && mkdir ./cdclog

protoc --gofast_out=./canal EntryProtocol.proto
protoc --gofast_out=./canal CanalProtocol.proto
