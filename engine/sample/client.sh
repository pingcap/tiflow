#!/bin/bash

docker run --network="sample_default" --rm -it dataflow:test ./bin/df-master-client $@
