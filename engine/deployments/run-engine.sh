#!/bin/bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

$CUR_DIR/../test/utils/run_engine.sh $*
