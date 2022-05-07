#!/bin/bash

current_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

docker build -f $current_dir/../Dockerfile -t dataflow:test $current_dir/..
