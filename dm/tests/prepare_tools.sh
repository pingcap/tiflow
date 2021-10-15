#!/bin/bash

CUR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

mkdir -p $CUR/bin
cd $CUR
for file in "_dmctl_tools"/*; do
	bin_name=$(echo $file | awk -F"/" '{print $(NF)}' | awk -F"." '{print $1}')
	GO111MODULE=on go build -o bin/$bin_name $file
done
cd -
