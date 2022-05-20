#!/bin/bash

docker run --network="sample_default" --rm -it dataflow:test ./bin/master-client $@
