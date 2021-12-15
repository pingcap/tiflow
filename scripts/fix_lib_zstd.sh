#!/bin/bash

GOPATH=$(go env GOPATH)
module="github.com/apache/pulsar-client-go@v0.1.1"

GO111MODULE=on go mod download ${module}
# In CI environment, the gopath contains multiple dirs, choose the first one
cd $(echo ${GOPATH} | awk -F':' '{print $1}')/pkg/mod/${module}/pulsar/internal/compression
sudo rm zstd_cgo.go
sudo sed -i '/build !cgo/d' zstd.go
