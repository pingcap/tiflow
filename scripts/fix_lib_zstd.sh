#!/bin/bash

user=$(whoami)
GOOS=$(go env GOOS)
GOARCH=$(go env GOARCH)
module="github.com/valyala/gozstd@v1.7.0"

GO111MODULE=on go mod download ${module}
# In CI environment, the gopath contains multiple dirs, choose the first one
cd $(echo $GOPATH|awk -F':' '{print $1}')/pkg/mod/${module}
sudo MOREFLAGS=-fPIC make clean libzstd.a

sudo which go
if [[ $? != 0 ]]; then
    lib_name=libzstd_${GOOS}_${GOARCH}.a
    echo "mv libzstd__.a ${lib_name}"
    sudo mv libzstd__.a ${lib_name}
    sudo chown ${user}:${user} ${lib_name}
fi
