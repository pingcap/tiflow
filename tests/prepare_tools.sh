#!/bin/bash

CUR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

DEST=$CUR/../bin
mkdir -p $DEST
cd $CUR
for file in "_cli_tools"/*; do
    bin_name=$(echo $file|awk -F"/" '{print $(NF)}'|awk -F"." '{print $1}')
    echo "build $file to $DEST/$bin_name"
    GO111MODULE=on go build -o $DEST/$bin_name $file
done
cd -
