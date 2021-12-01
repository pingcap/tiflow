#!/usr/bin/env bash

echo "generate canal & craft benchmark protocol code..."

[ ! -d ./canal ] && mkdir ./canal
[ ! -d ./benchmark ] && mkdir ./benchmark

protoc --gofast_out=./canal EntryProtocol.proto
protoc --gofast_out=./canal CanalProtocol.proto
protoc --gofast_out=./benchmark CraftBenchmark.proto
protoc --gofast_out=plugins=grpc:./p2p CDCPeerToPeer.proto
