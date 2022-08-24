#!/bin/bash

set -e

cd "$(dirname "$0")/../.."
./engine/test/utils/run_engine.sh "$@"
